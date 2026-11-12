import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http2';
import type { Http2Server, IncomingHttpHeaders } from 'node:http2';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';

import type { Authentication } from '../src/index.js';
import { createCodexClient } from '../src/index.js';

async function listen(
  server: Http2Server,
  received: IncomingHttpHeaders[],
): Promise<string> {
  server.on('stream', (stream, headers) => {
    received.push(headers);
    stream.resume();
    // No model response is needed to inspect the actual HTTP/2 headers.
    // eslint-disable-next-line @typescript-eslint/naming-convention -- HTTP/2 requires the :status pseudo-header.
    stream.respond({ ':status': 400 });
    stream.end();
  });
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });
  const { port } = server.address() as AddressInfo;
  return `http://127.0.0.1:${port}`;
}

async function writeAccountAuth(codexHome: string): Promise<void> {
  /* eslint-disable @typescript-eslint/naming-convention -- Account credentials use the auth.json wire format. */
  await writeFile(
    join(codexHome, 'auth.json'),
    JSON.stringify({
      auth_mode: 'chatgpt',
      tokens: { access_token: 'account-token', account_id: 'account-id' },
      last_refresh: new Date().toISOString(),
    }),
  );
  /* eslint-enable @typescript-eslint/naming-convention -- Account credential fields end here. */
}

function checkHeaders(
  mode: Authentication['type'],
  received: IncomingHttpHeaders[],
): void {
  assert.equal(received.length, mode === 'account' ? 1 : 2);
  for (const headers of received) {
    assert.equal(
      headers.authorization,
      `Bearer ${mode === 'account' ? 'account-token' : 'api-token'}`,
    );
    assert.equal(
      headers.originator,
      mode === 'account' ? 'codex_cli_rs' : undefined,
    );
    assert.equal(
      headers['chatgpt-account-id'],
      mode === 'account' ? 'account-id' : undefined,
    );
  }
}

for (const mode of ['account', 'api-key'] as const) {
  test(`${mode} requests send only their authentication headers`, async () => {
    const codexHome = await mkdtemp(join(tmpdir(), 'codex-auth-headers-'));
    const server = createServer();
    const received: IncomingHttpHeaders[] = [];
    const baseUrl = await listen(server, received);
    await writeAccountAuth(codexHome);
    const auth: Authentication =
      mode === 'account'
        ? { type: 'account' }
        : { type: 'api-key', apiKey: 'api-token' };
    const client = createCodexClient({
      auth,
      codexHome,
      baseUrl,
    });
    try {
      await assert.rejects(
        client.respond({ model: 'gpt-6.1-sol', effort: 'low', input: 'Hi.' }),
        /HTTP 400/,
      );
      if (mode === 'api-key') {
        await assert.rejects(client.batches.retrieve('headers'), { status: 400 });
      }
      checkHeaders(mode, received);
    } finally {
      await client.close();
      await new Promise<void>((resolve) => {
        server.close(() => {
          resolve();
        });
      });
      await rm(codexHome, { recursive: true, force: true });
    }
  });
}
