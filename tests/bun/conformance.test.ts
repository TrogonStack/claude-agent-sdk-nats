/**
 * Live conformance suite against a real NATS JetStream server.
 * Skips automatically unless SESSION_STORE_NATS_URL is set.
 *
 *   docker compose up --wait
 *   SESSION_STORE_NATS_URL=nats://127.0.0.1:4222 bun test tests/bun/conformance.test.ts
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
import { runSessionStoreConformance } from './conformance.ts';

const url = process.env.SESSION_STORE_NATS_URL;

describe.skipIf(!url)('NatsSessionStore (live conformance)', () => {
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

  async function freshStore() {
    const suffix = `${counter++}_${process.hrtime.bigint().toString(36).toUpperCase()}`;
    const streamName = `CLAUDE_SESSIONS_TEST_${suffix}`.replace(/[^A-Z0-9_]/g, '_');
    createdStreams.push(streamName);
    const prefix = `test.${streamName.toLowerCase()}`;
    // Provisioning is an operations concern, so tests own it, not the adapter.
    await jsm.streams.add({
      name: streamName,
      subjects: [`${prefix}.>`],
      storage: StorageType.File,
      retention: RetentionPolicy.Limits,
      duplicate_window: 2 * 60 * 1_000_000_000,
    });
    return new NatsSessionStore({ js, stream: streamName, prefix });
  }

  runSessionStoreConformance(freshStore);

  test('listSessions mtime is epoch ms', async () => {
    const store = await freshStore();
    const before = Date.now();
    await store.append({ projectKey: 'p', sessionId: 's' }, [{ type: 'a' }]);
    const [s] = await store.listSessions('p');
    expect(Math.abs(s!.mtime - before)).toBeLessThan(5000);
  });

  test('entries with the same string uuid are deduped', async () => {
    const store = await freshStore();
    const key = { projectKey: 'p', sessionId: 's' };
    await store.append(key, [{ type: 'a', uuid: 'fixed-id' }]);
    await store.append(key, [{ type: 'a', uuid: 'fixed-id' }]);
    const loaded = await store.load(key);
    expect(loaded).toHaveLength(1);
  });

  test('entries without a uuid are never deduped', async () => {
    const store = await freshStore();
    const key = { projectKey: 'p', sessionId: 's' };
    await store.append(key, [{ type: 'a' }]);
    await store.append(key, [{ type: 'a' }]);
    const loaded = await store.load(key);
    expect(loaded).toHaveLength(2);
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
