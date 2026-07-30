# claude-agent-sdk-nats

**`@trogonstack/claude-agent-sdk-nats` stores Claude Agent SDK conversation transcripts in NATS JetStream.**

**It implements the SDK's `SessionStore` contract on a single JetStream stream.** Every transcript entry the [Claude Agent SDK](https://www.npmjs.com/package/@anthropic-ai/claude-agent-sdk) mirrors becomes a message on a per-session subject, resume replays them in order, and sessions can be listed, deleted, and inspected by subject. It follows the pattern of the SDK's [session-stores reference adapters](https://github.com/anthropics/claude-agent-sdk-typescript/tree/main/examples/session-stores) for S3, Redis, and Postgres.

**It exists because agent sessions written only to local disk die with the machine that ran them.** Mirroring transcripts to JetStream makes sessions durable and resumable from any instance that can reach your NATS cluster, with ordering guaranteed by the server and retention under your operational control.

**It is for teams already running NATS who are building on the Claude Agent SDK.** If you want session durability without adding a database, and you want retention, replication, and cleanup managed with the same tooling as the rest of your streams, this adapter is the missing piece.

## Install

```bash
bun add @trogonstack/claude-agent-sdk-nats
# or: pnpm add @trogonstack/claude-agent-sdk-nats
```

## Usage

```typescript
import { connect } from '@nats-io/transport-node';
import { jetstream } from '@nats-io/jetstream';
import { query } from '@anthropic-ai/claude-agent-sdk';
import { NatsSessionStore } from '@trogonstack/claude-agent-sdk-nats';

const nc = await connect({ servers: 'nats://localhost:4222' });
const sessionStore = new NatsSessionStore({
  js: jetstream(nc),
  prefix: 'transcripts',
});

for await (const message of query({
  prompt: 'Hello!',
  options: { sessionStore },
})) {
  if (message.type === 'result' && message.subtype === 'success') {
    console.log(message.result);
  }
}
```

Resume a previous session by passing its id:

```typescript
for await (const message of query({
  prompt: 'Continue where we left off',
  options: { sessionStore, resume: 'previous-session-id' },
})) {
  // ...
}
```

## Stream provisioning

The adapter never manages stream configuration. Provision the stream with your
usual tooling before pointing the adapter at it:

```bash
nats stream add CLAUDE_SESSIONS \
  --subjects 'claude.sessions.>' \
  --storage file \
  --retention limits \
  --dupe-window 2m \
  --max-age 90d \
  --replicas 3 \
  --discard old --max-msgs=-1 --max-msgs-per-subject=-1 \
  --max-bytes=-1 --max-msg-size=-1 --max-consumers=-1 \
  --allow-rollup --no-deny-delete --no-deny-purge
```

- The example uses the defaults: stream `CLAUDE_SESSIONS`, prefix
  `claude.sessions`. Both are constructor options.
- `subjects` must cover `{prefix}.>` and `retention` must be `limits`
  (work-queue or interest retention deletes entries as they are read).
- `--no-deny-purge` is required: `delete()` purges by subject filter.
- `--dupe-window 2m` dedupes the SDK's at-least-once append retries via the
  `Nats-Msg-Id` header, keyed on each entry's `uuid`.
- Retention (`--max-age`, `--max-bytes`) is yours to choose; the adapter never
  expires messages on its own.
- NATS `max_payload` defaults to 1 MB; raise it if you expect large
  transcript entries. Oversized entries are rejected, never truncated.

## Documentation

- [Design](docs/explanation/design.md): the subject scheme, why listings are
  derived from the stream instead of indexed, and how dedupe works.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
