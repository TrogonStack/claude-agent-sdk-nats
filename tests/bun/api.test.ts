/**
 * API edge proofs beyond the vendored conformance suite: constructor
 * defaults, key encoding, malformed entries, missing-stream behavior,
 * concurrency, and documented limits.
 *
 *   docker compose up --wait
 *   SESSION_STORE_NATS_URL=nats://127.0.0.1:4222 bun test tests/bun/api.test.ts
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { connect, type NatsConnection } from '@nats-io/transport-node';
import {
  jetstream,
  jetstreamManager,
  RetentionPolicy,
  StorageType,
  type JetStreamClient,
  type JetStreamManager,
} from '@nats-io/jetstream';
import { NatsSessionStore } from '../../src/index.ts';

const url = process.env.SESSION_STORE_NATS_URL;

describe.skipIf(!url)('NatsSessionStore (API edge proofs)', () => {
  let nc: NatsConnection;
  let js: JetStreamClient;
  let jsm: JetStreamManager;
  const createdStreams: string[] = [];
  let counter = 0;

  beforeAll(async () => {
    nc = await connect({ servers: url });
    js = jetstream(nc);
    jsm = await jetstreamManager(nc);
  });

  async function addStream(name: string, subjects: string[]) {
    createdStreams.push(name);
    await jsm.streams.add({
      name,
      subjects,
      storage: StorageType.File,
      retention: RetentionPolicy.Limits,
      duplicate_window: 2 * 60 * 1_000_000_000,
    });
  }

  async function freshStore(overrides: { jsm?: JetStreamManager } = {}) {
    const suffix = `${counter++}_${process.hrtime.bigint().toString(36).toUpperCase()}`;
    const streamName = `CLAUDE_SESSIONS_API_${suffix}`.replace(/[^A-Z0-9_]/g, '_');
    const prefix = `apitest.${streamName.toLowerCase()}`;
    await addStream(streamName, [`${prefix}.>`]);
    return {
      store: new NatsSessionStore({ js, stream: streamName, prefix, ...overrides }),
      prefix,
      streamName,
    };
  }

  test('constructor defaults: stream CLAUDE_SESSIONS, prefix claude.sessions', async () => {
    await addStream('CLAUDE_SESSIONS', ['claude.sessions.>']);
    const store = new NatsSessionStore({ js });
    const key = { projectKey: 'default-proof', sessionId: 's1' };
    await store.append(key, [{ type: 'a' }]);
    expect(await store.load(key)).toEqual([{ type: 'a' }]);
    const raw = await jsm.streams.getMessage('CLAUDE_SESSIONS', {
      last_by_subj: `claude.sessions.${Buffer.from('default-proof').toString('base64url')}.${Buffer.from('s1').toString('base64url')}.main`,
    });
    expect(raw).not.toBeNull();
  });

  test('prefix normalization strips trailing dots', async () => {
    const { streamName, prefix } = await freshStore();
    const store = new NatsSessionStore({ js, stream: streamName, prefix: `${prefix}...` });
    const key = { projectKey: 'p', sessionId: 's' };
    await store.append(key, [{ type: 'a' }]);
    expect(await store.load(key)).toEqual([{ type: 'a' }]);
  });

  test('explicitly injected jsm is used', async () => {
    const { streamName, prefix } = await freshStore();
    const store = new NatsSessionStore({ js, jsm, stream: streamName, prefix });
    await store.append({ projectKey: 'p', sessionId: 's' }, [{ type: 'a' }]);
    expect((await store.listSessions('p')).map((s) => s.sessionId)).toEqual(['s']);
    await store.delete({ projectKey: 'p', sessionId: 's' });
    expect(await store.load({ projectKey: 'p', sessionId: 's' })).toBeNull();
  });

  test('opaque keys with subject-illegal characters round-trip everywhere', async () => {
    const { store } = await freshStore();
    const projectKey = '/Users/some one/my.project*>';
    const sessionId = 'sess.id/with*weird>chars ☃';
    const subpath = 'subagents/agent-1.2*>';
    await store.append({ projectKey, sessionId }, [{ type: 'main' }]);
    await store.append({ projectKey, sessionId, subpath }, [{ type: 'sub' }]);

    expect(await store.load({ projectKey, sessionId })).toEqual([{ type: 'main' }]);
    expect(await store.load({ projectKey, sessionId, subpath })).toEqual([{ type: 'sub' }]);
    expect((await store.listSessions(projectKey)).map((s) => s.sessionId)).toEqual([sessionId]);
    expect(await store.listSubkeys({ projectKey, sessionId })).toEqual([subpath]);

    await store.delete({ projectKey, sessionId });
    expect(await store.load({ projectKey, sessionId })).toBeNull();
    expect(await store.load({ projectKey, sessionId, subpath })).toBeNull();
    expect(await store.listSubkeys({ projectKey, sessionId })).toEqual([]);
  });

  test('load skips malformed entries and keeps valid ones', async () => {
    const { store, prefix } = await freshStore();
    const key = { projectKey: 'p', sessionId: 's' };
    const subject = `${prefix}.${Buffer.from('p').toString('base64url')}.${Buffer.from('s').toString('base64url')}.main`;
    await store.append(key, [{ type: 'a' }]);
    await js.publish(subject, 'not-json{');
    await store.append(key, [{ type: 'b' }]);
    expect(await store.load(key)).toEqual([{ type: 'a' }, { type: 'b' }]);
  });

  test('load returns null when every entry is malformed', async () => {
    const { store, prefix } = await freshStore();
    const subject = `${prefix}.${Buffer.from('p').toString('base64url')}.${Buffer.from('s').toString('base64url')}.main`;
    await js.publish(subject, 'garbage');
    await js.publish(subject, '{broken');
    expect(await store.load({ projectKey: 'p', sessionId: 's' })).toBeNull();
  });

  test('listSessions and listSubkeys return [] when the stream does not exist', async () => {
    const store = new NatsSessionStore({ js, stream: 'DOES_NOT_EXIST', prefix: 'nope' });
    expect(await store.listSessions('p')).toEqual([]);
    expect(await store.listSubkeys({ projectKey: 'p', sessionId: 's' })).toEqual([]);
  });

  test('load throws when the stream does not exist (pre-provisioning is required)', async () => {
    const store = new NatsSessionStore({ js, stream: 'DOES_NOT_EXIST', prefix: 'nope' });
    expect(store.load({ projectKey: 'p', sessionId: 's' })).rejects.toThrow();
  });

  test('mtime advances with subsequent appends', async () => {
    const { store } = await freshStore();
    const key = { projectKey: 'p', sessionId: 's' };
    await store.append(key, [{ type: 'a' }]);
    const [first] = await store.listSessions('p');
    await Bun.sleep(25);
    await store.append(key, [{ type: 'b' }]);
    const [second] = await store.listSessions('p');
    expect(second!.mtime).toBeGreaterThan(first!.mtime);
  });

  test('a 100-entry batch preserves order', async () => {
    const { store } = await freshStore();
    const key = { projectKey: 'p', sessionId: 's' };
    const entries = Array.from({ length: 100 }, (_, i) => ({ type: 'e', i }));
    await store.append(key, entries);
    expect(await store.load(key)).toEqual(entries);
  });

  test('concurrent appends to distinct sessions stay isolated and ordered', async () => {
    const { store } = await freshStore();
    const sessions = Array.from({ length: 10 }, (_, i) => `s${i}`);
    await Promise.all(
      sessions.map((sessionId) =>
        store.append(
          { projectKey: 'p', sessionId },
          Array.from({ length: 10 }, (_, i) => ({ type: sessionId, i })),
        ),
      ),
    );
    for (const sessionId of sessions) {
      const loaded = await store.load({ projectKey: 'p', sessionId });
      expect(loaded).toEqual(Array.from({ length: 10 }, (_, i) => ({ type: sessionId, i })));
    }
    expect((await store.listSessions('p')).map((s) => s.sessionId).sort()).toEqual(
      [...sessions].sort(),
    );
  });

  test('entries beyond the server max_payload are rejected, not truncated', async () => {
    const { store } = await freshStore();
    const oversized = { type: 'big', blob: 'x'.repeat(1024 * 1024 + 1024) };
    expect(store.append({ projectKey: 'p', sessionId: 's' }, [oversized])).rejects.toThrow();
    expect(await store.load({ projectKey: 'p', sessionId: 's' })).toBeNull();
  });

  test('listSessions is complete across many sessions', async () => {
    const { store } = await freshStore();
    const ids = Array.from({ length: 25 }, (_, i) => `sess-${i}`);
    for (const sessionId of ids) {
      await store.append({ projectKey: 'p', sessionId }, [{ type: 'a' }]);
    }
    const listed = (await store.listSessions('p')).map((s) => s.sessionId).sort();
    expect(listed).toEqual([...ids].sort());
  });

  afterAll(async () => {
    for (const name of createdStreams) {
      try {
        await jsm.streams.delete(name);
      } catch {
        // Stream may not have been created if a prior test failed early.
      }
    }
    await nc.close();
  });
});
