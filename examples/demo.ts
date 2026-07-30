/**
 * End-to-end demo: run a query with NatsSessionStore, then resume it.
 *
 * Prereqs:
 *   - ANTHROPIC_API_KEY set
 *   - NATS JetStream reachable. For local testing:
 *       docker compose up --wait
 *
 * Run:
 *   SESSION_STORE_NATS_URL=nats://127.0.0.1:4222 bun run examples/demo.ts
 */
import { connect } from '@nats-io/transport-node';
import { jetstream, jetstreamManager, RetentionPolicy, StorageType } from '@nats-io/jetstream';
import { query } from '@anthropic-ai/claude-agent-sdk';
import { NatsSessionStore } from '../src/index.ts';

const url = process.env.SESSION_STORE_NATS_URL ?? 'nats://127.0.0.1:4222';
const nc = await connect({ servers: url });

// Stream provisioning is an operations concern (see README); the demo does
// it inline only so it runs against a bare local server.
const jsm = await jetstreamManager(nc);
try {
  await jsm.streams.info('CLAUDE_SESSIONS_DEMO');
} catch {
  await jsm.streams.add({
    name: 'CLAUDE_SESSIONS_DEMO',
    subjects: ['demo.>'],
    storage: StorageType.File,
    retention: RetentionPolicy.Limits,
    duplicate_window: 2 * 60 * 1_000_000_000,
  });
}

const store = new NatsSessionStore({
  js: jetstream(nc),
  jsm,
  stream: 'CLAUDE_SESSIONS_DEMO',
  prefix: 'demo',
});

async function run(prompt: string, resume?: string) {
  let sessionId: string | undefined;
  for await (const m of query({
    prompt,
    options: { sessionStore: store, resume, maxTurns: 1 },
  })) {
    if (m.type === 'system' && m.subtype === 'init') sessionId = m.session_id;
    if (m.type === 'result') {
      console.log(`[${m.subtype}]`, 'result' in m ? m.result : '');
    }
  }
  return sessionId;
}

const sid = await run('Reply with exactly the word: pineapple');
console.log('session', sid, 'mirrored to nats under prefix demo');

await run('What single word did you just reply with?', sid);
await nc.close();
