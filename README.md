# @blockchainian/codex-client

An ESM TypeScript client using the official OpenAI SDK for Codex Responses over HTTP/2. Requires Node.js 22 or later. The OpenAI SDK is a runtime dependency.

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

The client defaults to 20 concurrent calls, retries transport and protocol failures up to three times, and shares rate limit pauses across its calls. `AUTH` and `USAGE_LIMIT` errors stop queued and future calls. `close()` drains accepted calls and releases connections. The optional `log` and `event` callbacks receive attempt metrics and rate limit pause events without request or response text; caller-supplied `telemetry` should contain only safe identifiers. Attempt metrics include status, timing, usage, and rate headers without connection identifiers or per-event stream statistics.

This package uses the Codex ChatGPT backend and requires a compatible ChatGPT login. It is independent of OpenAI.

## Development

```sh
npm ci
npm run check
npm test
npm run build
```

The TypeScript tests use Node's test runner with the `tsx` loader and real local HTTP/2 servers. No live credentials are used by the test suite.

## Native batches and API authentication

Account login remains the default. Select `auth: {type: 'api-key', apiKey}` or the `apiKey` shorthand to use OpenAI API billing. The API-key mode reads `OPENAI_API_KEY` only when no key is supplied. Account mode ignores that environment variable. Combining `auth` with the shorthand fails before any request. API keys are restricted to the official OpenAI origin; loopback overrides support local transport tests.

`client.batches.create({requests})` uploads native JSONL and creates an OpenAI Batch API job. Each request contains a unique `custom_id` and `params` with the same fields as `respond`. `retrieve(id)` and `cancel(id)` return the provider's native batch object. These methods require explicit API-key authentication and do not switch account requests to API billing.

Persist the original request manifest, then pass it to `client.batches.results(id, {requests})`. Results retain `custom_id`; successful items contain `result.response`, while provider and decoding failures remain individual error items. Batch creation does not poll or automatically warm the cache. Completed batches must return exactly one result for every manifest identifier; missing, repeated or unknown identifiers fail validation. Canceled or expired batches can omit requests from their result files, so callers must compare returned identifiers with the manifest. Result files are read line by line. Closing the client refuses new batch operations and waits for accepted requests and active result iterators, without waiting for server-side jobs to finish.

Set `promptCacheKey` and `promptCacheOptions` on both the explicit warm-up request and batch requests. The options map directly to `prompt_cache_options`; supported mode values are `implicit` and `explicit`, and TTL values are `30m` and `12h`. Cache reuse still depends on matching model and prompt prefixes. The client does not change instructions, schemas or inputs to manufacture a cache hit.
