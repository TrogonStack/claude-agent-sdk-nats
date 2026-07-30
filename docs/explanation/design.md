# Design

How the adapter maps the Claude Agent SDK's `SessionStore` contract onto NATS
JetStream, and why it is shaped the way it is.

## One stream, one subject per transcript

Every transcript is a subject inside a single JetStream stream:

| Subject                                                          | Contents                    |
| ---------------------------------------------------------------- | --------------------------- |
| `{prefix}.{enc(projectKey)}.{enc(sessionId)}.main`               | Main transcript entries     |
| `{prefix}.{enc(projectKey)}.{enc(sessionId)}.sub.{enc(subpath)}` | Subagent transcript entries |

`append()` publishes one message per entry, `load()` replays the subject in
stream order through an ordered consumer, and `delete()` purges by subject
filter (`{prefix}.{proj}.{sess}.>` cascades a whole session, including its
subagent transcripts).

A stream fits the contract better than the obvious alternative, a KV bucket.
KV is last-value semantics: one current value per key with bounded history,
which forces either rewriting a growing blob per session on every append or
abusing KV history as a capped log. A transcript is an ordered, append-only
log you replay from the start, which is precisely what a stream subject is.

## Opaque keys are base64url tokens

`projectKey`, `sessionId`, and `subpath` are opaque strings owned by the SDK.
They contain characters that are illegal in NATS subject tokens (`/`, `.`,
spaces, `*`, `>`), so each part is base64url-encoded into exactly one token.
Fixed token positions (`main` and `sub` literals) keep every wildcard filter
precise: `{prefix}.{proj}.*.main` matches main transcripts and nothing else.

## Listings are derived, not indexed

`listSessions()` and `listSubkeys()` ask the stream itself
(`streams.info` with a `subjects_filter`) which subjects currently hold
messages, and `mtime` comes from the last message's server-assigned timestamp
on each `.main` subject.

This has two consequences worth understanding:

- There are no secondary indexes to maintain or corrupt. Purged subjects
  disappear from the subject listing atomically, so `delete()` needs no
  cleanup step. The equivalent Redis adapter maintains two explicit index
  structures for the same behavior.
- Timestamps are the server's, not the writer's. The S3 reference adapter
  derives ordering from the client wall clock and documents clock skew as a
  caveat; JetStream assigns sequence and time at the server, so that failure
  mode does not exist here.

The trade-off is that stream-derived listings stay simple: no search, no
pagination semantics beyond the subject listing. If richer listing is ever
needed, any consumer can fold `{prefix}.>` into an external projection
without touching stored data.

## Dedupe rides on the SDK's retry behavior

The SDK mirrors transcripts best-effort and retries failed appends, so
appends are at-least-once. Each entry carries a stable `uuid`, which the
adapter passes as the `Nats-Msg-Id` header; the stream's `duplicate_window`
then absorbs retry duplicates server-side. Entries without a string `uuid`
are published without the header, because identical uuid-less entries are
legitimate distinct messages and must never be collapsed.

## The stream is operations-owned

The adapter never creates, updates, or deletes stream configuration. Stream
provisioning belongs to whoever operates NATS, with their own tooling and
retention policy; an adapter that silently creates infrastructure with
hardcoded defaults takes that decision away. The cost is one documented
prerequisite; see the
[stream provisioning section](../../README.md#stream-provisioning).
