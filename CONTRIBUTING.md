# Contributing

## Prerequisites

- [mise](https://mise.jdx.dev) (installs the pinned Node.js and Bun from
  `mise.toml`; CI uses the same file)
- Docker (for the live test suite)

## Setup

```bash
mise install
bun install
```

## Tests

The suite runs against a real NATS server and skips unless
`SESSION_STORE_NATS_URL` is set:

```bash
docker compose up --wait
SESSION_STORE_NATS_URL=nats://localhost:4222 bun test
docker compose down -v
```

If the default ports are taken on your machine, override them; the same
compose file is used by CI:

```bash
NATS_PORT=44222 NATS_MONITOR_PORT=48222 docker compose up --wait
SESSION_STORE_NATS_URL=nats://localhost:44222 bun test
```

`tests/bun/conformance.ts` is vendored verbatim from the SDK's
session-stores examples; do not edit it. Adapter-specific tests live in
`tests/bun/api.test.ts`.

## Checks

CI runs these on every pull request; run them locally first:

```bash
bun run fmt        # format (oxfmt)
bun run lint       # oxlint
bun run typecheck  # tsc --noEmit
bun run build      # emits dist/
```

## Demo

An end-to-end `query()` + resume round-trip against a live NATS server.
Requires `ANTHROPIC_API_KEY`:

```bash
SESSION_STORE_NATS_URL=nats://localhost:4222 bun run demo
```

## Commits and pull requests

- Commit messages and PR titles must follow
  [Conventional Commits](https://www.conventionalcommits.org); both are
  enforced in CI.
- All commits require a DCO sign-off (`git commit -s`).

## Releases

Releases are automated with
[release-please](https://github.com/googleapis/release-please): merging the
standing release PR tags the release and publishes to npm via
[trusted publishing](https://docs.npmjs.com/trusted-publishers/). Version
bumps are computed from conventional commit types, so no manual version
changes are needed.
