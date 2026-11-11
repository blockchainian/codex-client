# @blockchainian/codex-client

An ESM TypeScript client for Codex Responses over HTTP/2. Requires Node.js 22 or later and has no runtime dependencies.

```sh
npm install @blockchainian/codex-client
```

```ts
import { createCodexClient } from '@blockchainian/codex-client';

const client = createCodexClient();
try {
  const { output, usage } = await client.respond({
    model: 'gpt-6.1-sol',
    effort: 'low',
    instructions: 'Reply briefly.',
    input: 'Say hello.',
  });
  console.log(output, usage);
} finally {
  await client.close();
}
```

Credentials are read from `~/.codex/auth.json`, created by `codex login`. Set `CODEX_HOME` or pass `codexHome` to use another directory. Expiring tokens are refreshed automatically; refreshes coordinate across clients and processes and replace the auth file atomically with private permissions.

`input` accepts a string or native Responses input items, including image messages. Optional `serviceTier`, `reasoningContext`, `verbosity`, and `responsesLite` are forwarded to Codex. With `schema`, output is parsed JSON; use `respond<T>()` and `validate` to check the expected shape. A validator may be asynchronous; throwing retries the request. `recoverIncomplete` can recover nonempty output from replies stopped by `max_output_tokens`. Its result also passes through `validate`, with `{ incomplete: true }`.

The client defaults to 20 concurrent calls, retries transport and protocol failures up to three times, and shares rate limit pauses across its calls. `AUTH` and `USAGE_LIMIT` errors stop queued and future calls. `close()` drains accepted calls and releases connections. The optional `log` and `event` callbacks receive attempt metrics and rate limit pause events without request or response text; caller-supplied `telemetry` should contain only safe identifiers.

This package uses the Codex ChatGPT backend and requires a compatible ChatGPT login. It is independent of OpenAI.

## Development

```sh
npm ci
npm run check
npm test
npm run build
```

The TypeScript tests use Node's test runner with the `tsx` loader and real local HTTP/2 servers. No live credentials are used by the test suite.
