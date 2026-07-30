# claude-agent-sdk-nats

A NATS JetStream-backed `SessionStore` adapter for the
[Claude Agent SDK](https://www.npmjs.com/package/@anthropic-ai/claude-agent-sdk),
following the pattern of the SDK's
[session-stores examples](https://github.com/anthropics/claude-agent-sdk-typescript/tree/main/examples/session-stores)
(S3, Redis, Postgres).

## Subject scheme

`projectKey`, `sessionId`, and `subpath` are opaque strings that may contain
characters illegal in NATS subject tokens (`/`, `.`, spaces, `*`, `>`), so each
part is base64url-encoded into a single subject token.

| Subject                                                          | Contents                                            |
| ---------------------------------------------------------------- | --------------------------------------------------- |
| `{prefix}.{enc(projectKey)}.{enc(sessionId)}.main`               | Main transcript entries, one message per JSON entry |
| `{prefix}.{enc(projectKey)}.{enc(sessionId)}.sub.{enc(subpath)}` | Subagent transcript entries                         |

`prefix` defaults to `claude.sessions` and the stream defaults to
`CLAUDE_SESSIONS`.

## Stream provisioning

The adapter never creates, updates, or deletes stream configuration. Stream
provisioning is an operations concern: provision the stream with your usual
tooling (NATS CLI, Terraform, GitOps) before pointing the adapter at it.

Required configuration:

- `subjects` must cover `{prefix}.>` (e.g. `claude.sessions.>`)
- `retention: limits` (the adapter relies on limits-based semantics; work-queue
  or interest retention would delete entries as they are read)

Recommended configuration:

- `duplicate_window: 2m` so the `Nats-Msg-Id` header dedupes the SDK's
  at-least-once append retries
- `storage: file` for durability
- `max_age` (or `max_bytes`/`max_msgs`) for retention, according to your
  compliance requirements

Example with the NATS CLI:

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

`--no-deny-purge` matters: `delete()` is implemented as a purge by subject
filter and fails on streams provisioned with `--deny-purge`.

## Install

```bash
bun add @trogonstack/claude-agent-sdk-nats
# or: npm install @trogonstack/claude-agent-sdk-nats
```

The package is ESM-only.

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

### Resume

```typescript
for await (const message of query({
  prompt: 'Continue where we left off',
  options: {
    sessionStore,
    resume: 'previous-session-id',
  },
})) {
  // ...
}
```

## Testing

The conformance suite runs live-only, gated on `SESSION_STORE_NATS_URL`:

```bash
docker compose up --wait
SESSION_STORE_NATS_URL=nats://localhost:4222 bun test
```

## Production notes

- NATS `max_payload` defaults to 1 MB. Large tool-result entries can exceed
  this; raise the server's `max_payload` if you expect large transcript
  entries.
- Retention is the operator's responsibility. Set `max_age` (or another limit)
  on the stream configuration; this adapter never expires messages on its
  own beyond what `delete()` explicitly purges.
- `duplicate_window` on the stream dedupes the SDK's append retries via the
  `Nats-Msg-Id` header, keyed on each entry's `uuid` when present.
