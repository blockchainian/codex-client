import assert from 'node:assert/strict';
import { createServer } from 'node:http2';
import type { Http2Server, ServerHttp2Stream } from 'node:http2';
import type { AddressInfo } from 'node:net';
import { test } from 'node:test';

import { createCodexClient } from '../src/index.js';

test('account batches reject before reading credentials or sending requests', async () => {
  const client = createCodexClient({
    auth: { type: 'account' },
    codexHome: '/does-not-exist',
  });
  await assert.rejects(client.batches.create({ requests: [] }), /API-key/);
  await client.close();
});

test('API keys cannot be forwarded to a custom remote origin', () => {
  assert.throws(
    () =>
      createCodexClient({
        apiKey: 'local-test-key',
        baseUrl: 'https://example.com',
      }),
    /official OpenAI/,
  );
});

test('explicit auth and API-key shorthand cannot be combined', () => {
  assert.throws(
    () => createCodexClient({ auth: { type: 'account' }, apiKey: 'local-test-key' }),
    /not both/,
  );
});

test('native batch manifests reject duplicate identifiers before uploading', async () => {
  const client = createCodexClient({ apiKey: 'local-test-key' });
  const params = { model: 'gpt-6.1-sol', effort: 'low', input: 'hello' };
  // eslint-disable-next-line @typescript-eslint/naming-convention -- Native batch inputs use custom_id.
  const request = { custom_id: 'one', params };
  await assert.rejects(
    client.batches.create({ requests: [request, request] }),
    /unique/,
  );
  await client.close();
});

test('closing the client rejects later batch operations before networking', async () => {
  const client = createCodexClient({
    apiKey: 'local-test-key',
    baseUrl: 'http://127.0.0.1:1',
  });
  await client.close();
  await assert.rejects(client.batches.retrieve('not-sent'), /closed/);
  await assert.rejects(client.batches.cancel('not-sent'), /closed/);
  await assert.rejects(client.batches.create({ requests: [] }), /closed/);
});

async function listen(server: Http2Server): Promise<string> {
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });
  const { port } = server.address() as AddressInfo;
  return `http://127.0.0.1:${port}`;
}

test('close drains accepted batch HTTP requests before closing the connection', async () => {
  const server = createServer();
  let received: (stream: ServerHttp2Stream) => void = () => undefined;
  const arrived = new Promise<ServerHttp2Stream>((resolve) => {
    received = resolve;
  });
  server.on('stream', (stream) => {
    stream.resume();
    received(stream);
  });
  const client = createCodexClient({
    apiKey: 'local-test-key',
    baseUrl: await listen(server),
  });
  try {
    const operation = client.batches
      .retrieve('accepted')
      .catch((error: unknown) => error);
    const stream = await arrived;
    let closed = false;
    const closing = client.close().then(() => {
      closed = true;
    });
    await Promise.resolve();
    assert.equal(closed, false);
    // A real local HTTP failure settles the accepted operation without model data.
    // eslint-disable-next-line @typescript-eslint/naming-convention -- HTTP/2 uses the :status pseudo-header.
    stream.respond({ ':status': 503 });
    stream.end();
    const failure = await operation;
    assert.ok(failure instanceof Error);
    await closing;
    assert.equal(closed, true);
  } finally {
    await client.close();
    await new Promise<void>((resolve) => {
      server.close(() => {
        resolve();
      });
    });
  }
});
