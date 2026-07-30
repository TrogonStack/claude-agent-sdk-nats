import {
  JetStreamApiCodes,
  JetStreamApiError,
  type JetStreamClient,
  type JetStreamManager,
} from '@nats-io/jetstream';
import type { SessionKey, SessionStore, SessionStoreEntry } from '@anthropic-ai/claude-agent-sdk';

export type NatsSessionStoreOptions = {
  /**
   * Pre-configured JetStream client. Caller controls the connection, auth,
   * and JetStream options (domain, apiPrefix, timeout).
   */
  js: JetStreamClient;
  /**
   * Pre-configured JetStream manager. Defaults to one derived from `js`
   * with the same JetStream options.
   */
  jsm?: JetStreamManager;
  /** Name of a pre-provisioned JetStream stream. Default 'CLAUDE_SESSIONS'. */
  stream?: string;
  /** Subject prefix. Trailing '.' is normalized. Default 'claude.sessions'. */
  prefix?: string;
};

const MAIN = 'main';
const SUB = 'sub';

function isStreamNotFound(err: unknown): boolean {
  return err instanceof JetStreamApiError && err.code === JetStreamApiCodes.StreamNotFound;
}

/**
 * NATS JetStream-backed SessionStore.
 *
 * Subject scheme ('.' separator; projectKey/sessionId/subpath are opaque
 * strings, so each is base64url-encoded into a single subject token):
 *   {prefix}.{enc(projectKey)}.{enc(sessionId)}.main             stream messages, one per JSON entry
 *   {prefix}.{enc(projectKey)}.{enc(sessionId)}.sub.{enc(subpath)}  subagent transcript entries
 *
 * There is no separate index subject for sessions or subkeys: `listSessions`
 * and `listSubkeys` are derived from the stream's subject listing
 * (`jsm.streams.info` with `subjects_filter`), and `mtime` comes from the
 * last stored message's timestamp on each `.main` subject.
 *
 * The stream is an operations-owned resource: this adapter never creates,
 * updates, or deletes stream configuration. The named stream must exist and
 * cover `{prefix}.>` before use; see the README's stream provisioning
 * section for the required and recommended configuration. The adapter never
 * expires messages on its own beyond what `delete()` explicitly purges.
 */
export class NatsSessionStore implements SessionStore {
  private readonly stream: string;
  private readonly prefix: string;
  private readonly js: JetStreamClient;
  private jsmPromise: Promise<JetStreamManager> | undefined;

  constructor(options: NatsSessionStoreOptions) {
    this.stream = options.stream ?? 'CLAUDE_SESSIONS';
    this.prefix = (options.prefix ?? 'claude.sessions').replace(/\.+$/, '');
    this.js = options.js;
    if (options.jsm) this.jsmPromise = Promise.resolve(options.jsm);
  }

  private jsm(): Promise<JetStreamManager> {
    if (!this.jsmPromise) {
      this.jsmPromise = this.js.jetstreamManager();
    }
    return this.jsmPromise;
  }

  private mainSubject(key: SessionKey): string {
    return `${this.prefix}.${enc(key.projectKey)}.${enc(key.sessionId)}.${MAIN}`;
  }

  private subSubject(key: SessionKey & { subpath: string }): string {
    return `${this.prefix}.${enc(key.projectKey)}.${enc(key.sessionId)}.${SUB}.${enc(key.subpath)}`;
  }

  private subject(key: SessionKey): string {
    return key.subpath !== undefined
      ? this.subSubject({ ...key, subpath: key.subpath })
      : this.mainSubject(key);
  }

  async append(key: SessionKey, entries: SessionStoreEntry[]): Promise<void> {
    if (entries.length === 0) return;
    const subject = this.subject(key);
    for (const entry of entries) {
      // msgID enables server-side dedupe (via duplicate_window) for the SDK's
      // at-least-once append retries; entries without a string uuid must
      // never be deduped, since the conformance suite appends identical
      // uuid-less entries that are expected to land as separate messages.
      const msgID = typeof entry.uuid === 'string' ? entry.uuid : undefined;
      await this.js.publish(subject, JSON.stringify(entry), { msgID });
    }
  }

  async load(key: SessionKey): Promise<SessionStoreEntry[] | null> {
    const subject = this.subject(key);
    const consumer = await this.js.consumers.get(this.stream, {
      filter_subjects: [subject],
    });
    const info = await consumer.info();
    const numPending = info.num_pending;
    if (numPending === 0) return null;

    const out: SessionStoreEntry[] = [];
    const msgs = await consumer.fetch({ max_messages: numPending });
    for await (const m of msgs) {
      try {
        out.push(JSON.parse(m.string()));
      } catch {
        // Skip malformed entries (parity with Redis/S3 adapters).
      }
    }
    return out.length > 0 ? out : null;
  }

  async listSessions(projectKey: string): Promise<Array<{ sessionId: string; mtime: number }>> {
    const jsm = await this.jsm();
    const filter = `${this.prefix}.${enc(projectKey)}.*.${MAIN}`;
    let subjects: Record<string, number> | undefined;
    try {
      const info = await jsm.streams.info(this.stream, {
        subjects_filter: filter,
      });
      subjects = info.state.subjects;
    } catch (err) {
      if (isStreamNotFound(err)) return [];
      throw err;
    }
    if (!subjects) return [];

    const sessionIdTokenIndex = this.prefix.split('.').length + 1;
    const result: Array<{ sessionId: string; mtime: number }> = [];
    for (const subject of Object.keys(subjects)) {
      const tokens = subject.split('.');
      const sessionId = dec(tokens[sessionIdTokenIndex]!);
      const msg = await jsm.streams.getMessage(this.stream, {
        last_by_subj: subject,
      });
      if (!msg) continue;
      result.push({ sessionId, mtime: msg.time.getTime() });
    }
    return result;
  }

  async delete(key: SessionKey): Promise<void> {
    const jsm = await this.jsm();
    if (key.subpath !== undefined) {
      await jsm.streams.purge(this.stream, { filter: this.subject(key) });
      return;
    }
    const filter = `${this.prefix}.${enc(key.projectKey)}.${enc(key.sessionId)}.>`;
    await jsm.streams.purge(this.stream, { filter });
  }

  async listSubkeys(key: { projectKey: string; sessionId: string }): Promise<string[]> {
    const jsm = await this.jsm();
    const filter = `${this.prefix}.${enc(key.projectKey)}.${enc(key.sessionId)}.${SUB}.*`;
    let subjects: Record<string, number> | undefined;
    try {
      const info = await jsm.streams.info(this.stream, {
        subjects_filter: filter,
      });
      subjects = info.state.subjects;
    } catch (err) {
      if (isStreamNotFound(err)) return [];
      throw err;
    }
    if (!subjects) return [];
    return Object.keys(subjects).map((subject) => {
      const tokens = subject.split('.');
      return dec(tokens[tokens.length - 1]!);
    });
  }
}

/**
 * projectKey/sessionId/subpath are opaque strings that may contain characters
 * illegal in NATS subject tokens ('/', '.', spaces, '*', '>'), so each part is
 * base64url-encoded into a single token.
 */
function enc(s: string): string {
  return Buffer.from(s, 'utf8').toString('base64url');
}

function dec(token: string): string {
  return Buffer.from(token, 'base64url').toString('utf8');
}
