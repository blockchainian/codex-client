import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import type { IncomingHttpHeaders, ServerHttp2Stream } from 'node:http2';
import { createServer } from 'node:http2';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before, test } from 'node:test';

import type { AttemptMetrics, CodexClient, PauseEvent } from '../src/index.js';
import { createCodexClient } from '../src/index.js';

interface Reply {
  status: number;
  headers?: Record<string, string>;
  body: string;
}

interface Received {
  path: string;
  headers: IncomingHttpHeaders;
  body: Record<string, unknown>;
}

type Handler = (request: Received) => Reply;

interface Server {
  url: string;
  requests: Received[];
  close(): Promise<void>;
}

let codexHome = '';

async function readBody(
  stream: ServerHttp2Stream,
): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) chunks.push(chunk as Buffer);
  return JSON.parse(Buffer.concat(chunks).toString('utf8')) as Record<
    string,
    unknown
  >;
}

async function startServer(handler: Handler): Promise<Server> {
  const requests: Received[] = [];
  const server = createServer();
  server.on('stream', (stream, headers) => {
    void readBody(stream).then((body) => {
      const request = { path: headers[':path'] ?? '', headers, body };
      requests.push(request);
      const reply = handler(request);
      // eslint-disable-next-line @typescript-eslint/naming-convention -- Fixtures follow the snake_case auth.json and Responses wire formats.
      stream.respond({ ':status': reply.status, ...reply.headers });
      stream.end(reply.body);
    });
  });
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    requests,
    close: () =>
      new Promise<void>((resolve) => {
        server.close(() => {
          resolve();
        });
      }),
  };
}

function completion(text: string): Reply {
  const events = [
    { type: 'response.output_text.delta', delta: text },
    {
      type: 'response.completed',
      response: {
        status: 'completed',
        // eslint-disable-next-line @typescript-eslint/naming-convention -- Fixtures follow the snake_case auth.json and Responses wire formats.
        usage: { input_tokens: 3, output_tokens: 2 },
        output: [
          {
            type: 'message',
            role: 'assistant',
            content: [{ type: 'output_text', text }],
          },
        ],
      },
    },
  ];
  const body = events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join('');
  // eslint-disable-next-line @typescript-eslint/naming-convention -- Fixtures follow the snake_case auth.json and Responses wire formats.
  return { status: 200, headers: { 'content-type': 'text/event-stream' }, body };
}

function errorReply(status: number, code: string): Reply {
  return { status, body: JSON.stringify({ error: { code, message: code } }) };
}

async function writeAuth(accessToken: string): Promise<void> {
  const auth = {
    /* eslint-disable @typescript-eslint/naming-convention -- Fixtures follow the snake_case auth.json and Responses wire formats. */
    auth_mode: 'chatgpt',
    tokens: {
      access_token: accessToken,
      account_id: 'account',
      refresh_token: 'refresh-1',
    },
    last_refresh: new Date().toISOString(),
    /* eslint-enable @typescript-eslint/naming-convention -- The snake_case fields end here. */
  };
  await writeFile(join(codexHome, 'auth.json'), JSON.stringify(auth));
}

async function withClient(
  handler: Handler,
  run: (client: CodexClient, server: Server) => Promise<void>,
  options: Parameters<typeof createCodexClient>[0] = {},
): Promise<void> {
  const server = await startServer(handler);
  const client = createCodexClient({
    codexHome,
    baseUrl: server.url,
    refreshUrl: `${server.url}/token`,
    retryDelayMs: 0,
    ...options,
  });
  try {
    await run(client, server);
  } finally {
    await client.close();
    await server.close();
  }
}

const call = {
  model: 'gpt-test',
  effort: 'low',
  instructions: 'Reply.',
  input: 'Hi.',
};

before(async () => {
  codexHome = await mkdtemp(join(tmpdir(), 'codex-client-'));
});

after(async () => {
  await rm(codexHome, { recursive: true, force: true });
});

test('createCodexClient rejects invalid options', () => {
  assert.throws(() => createCodexClient({ concurrency: 0 }), /Invalid concurrency/);
  assert.throws(() => createCodexClient({ timeoutMs: 0 }), /Invalid request timing/);
});

test('respond returns the streamed output and usage', async () => {
  await writeAuth('access-1');
  const logs: AttemptMetrics[] = [];
  await withClient(
    () => completion('Hello.'),
    async (client, server) => {
      const result = await client.respond({
        ...call,
        // eslint-disable-next-line @typescript-eslint/naming-convention -- Fixtures follow the snake_case auth.json and Responses wire formats.
        telemetry: { batch_id: 'b1' },
      });
      assert.equal(result.output, 'Hello.');
      assert.equal(result.usage.input_tokens, 3);
      const [request] = server.requests;
      assert.equal(request?.path, '/responses');
      assert.equal(request?.headers.authorization, 'Bearer access-1');
      assert.equal(request?.body['model'], 'gpt-test');
      assert.equal(request?.body['stream'], true);
    },
    { log: (metrics) => logs.push(metrics) },
  );
  assert.deepEqual(
    logs.map((metrics) => [metrics.kind, metrics.status, metrics.batch_id]),
    [['model', 200, 'b1']],
  );
});

test('respond parses schema output and retries when validation fails', async () => {
  await writeAuth('access-1');
  const replies = [completion('{"ok":false}'), completion('{"ok":true}')];
  await withClient(
    () => replies.shift() ?? errorReply(500, 'unexpected'),
    async (client, server) => {
      const result = await client.respond<{ ok: boolean }>({
        ...call,
        schema: { type: 'object' },
        validate: (output) => {
          if (!output.ok) throw new Error('Not ok.');
        },
      });
      assert.deepEqual(result.output, { ok: true });
      assert.equal(server.requests.length, 2);
    },
  );
});

test('respond waits out a rate limit and reports the pause', async () => {
  await writeAuth('access-1');
  const replies = [
    // eslint-disable-next-line @typescript-eslint/naming-convention -- Fixtures follow the snake_case auth.json and Responses wire formats.
    { ...errorReply(429, 'rate_limit_exceeded'), headers: { 'retry-after': '0' } },
    completion('Done.'),
  ];
  const events: PauseEvent[] = [];
  await withClient(
    () => replies.shift() ?? errorReply(500, 'unexpected'),
    async (client) => {
      const result = await client.respond(call);
      assert.equal(result.output, 'Done.');
    },
    { event: (pause) => events.push(pause) },
  );
  assert.deepEqual(
    events.map((pause) => [pause.type, pause.retry_after]),
    [
      ['pause_start', '0'],
      ['pause_end', undefined],
    ],
  );
});

test('respond refreshes the login after a 401 and saves the new token', async () => {
  await writeAuth('access-1');
  await withClient(
    (request) => {
      if (request.path === '/token') {
        // eslint-disable-next-line @typescript-eslint/naming-convention -- Fixtures follow the snake_case auth.json and Responses wire formats.
        return { status: 200, body: JSON.stringify({ access_token: 'access-2' }) };
      }
      const authorized = request.headers.authorization === 'Bearer access-2';
      return authorized ? completion('Refreshed.') : errorReply(401, 'unauthorized');
    },
    async (client, server) => {
      const result = await client.respond(call);
      assert.equal(result.output, 'Refreshed.');
      const token = server.requests.find((request) => request.path === '/token');
      assert.equal(token?.body['refresh_reason'], 'unauthorized_recovery');
    },
  );
  const saved = JSON.parse(await readFile(join(codexHome, 'auth.json'), 'utf8')) as {
    // eslint-disable-next-line @typescript-eslint/naming-convention -- Fixtures follow the snake_case auth.json and Responses wire formats.
    tokens: { access_token: string; refresh_token: string };
  };
  assert.equal(saved.tokens.access_token, 'access-2');
  assert.equal(saved.tokens.refresh_token, 'refresh-1');
});

test('a usage limit stops the client', async () => {
  await writeAuth('access-1');
  await withClient(
    () => errorReply(429, 'usage_limit_reached'),
    async (client) => {
      await assert.rejects(client.respond(call), { code: 'USAGE_LIMIT' });
      await assert.rejects(client.respond(call), { code: 'USAGE_LIMIT' });
    },
  );
});
