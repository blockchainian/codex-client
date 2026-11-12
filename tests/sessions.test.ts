import assert from 'node:assert/strict';
import { constants, createServer } from 'node:http2';
import type { Http2Server, ServerHttp2Stream } from 'node:http2';
import type { AddressInfo } from 'node:net';
import { test } from 'node:test';

import { SessionPool, toResponse } from '../src/sessions.js';

interface Server {
  server: Http2Server;
  url: URL;
  sessions: () => number;
}

async function startServer(
  handler: (stream: ServerHttp2Stream) => void,
): Promise<Server> {
  const server = createServer();
  let sessions = 0;
  server.on('session', () => {
    sessions++;
  });
  server.on('stream', (stream) => {
    stream.resume();
    stream.on('error', () => {
      assert.equal(stream.rstCode, constants.NGHTTP2_INTERNAL_ERROR);
    });
    handler(stream);
  });
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });
  const { port } = server.address() as AddressInfo;
  return {
    server,
    url: new URL(`http://127.0.0.1:${port}`),
    sessions: () => sessions,
  };
}

async function withPool(
  handler: (stream: ServerHttp2Stream) => void,
  run: (pool: SessionPool, server: Server) => Promise<void>,
  timeoutMs = 100,
): Promise<void> {
  const server = await startServer(handler);
  const pool = new SessionPool(timeoutMs);
  try {
    await run(pool, server);
  } finally {
    await pool.close();
    await new Promise<void>((resolve, reject) => {
      server.server.close((error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }
}

test('concurrent requests share one HTTP/2 connection', async () => {
  await withPool(
    (stream) => {
      stream.respond();
      stream.end('ok');
    },
    async (pool, server) => {
      const replies = await Promise.all(
        Array.from({ length: 4 }, async () => {
          const response = await pool.post(server.url, '{}', {}).response;
          return toResponse(response).text();
        }),
      );
      assert.deepEqual(replies, ['ok', 'ok', 'ok', 'ok']);
      assert.equal(server.sessions(), 1);
    },
  );
});

test('a stream without response headers times out', async () => {
  await withPool(
    () => {},
    async (pool, server) => {
      await assert.rejects(pool.post(server.url, '{}', {}).response, {
        code: 'REQUEST_TIMEOUT',
      });
    },
  );
});

test('a silent response body times out through the Web Response', async () => {
  await withPool(
    (stream) => {
      stream.respond();
    },
    async (pool, server) => {
      const response = await pool.post(server.url, '{}', {}).response;
      await assert.rejects(toResponse(response).text(), {
        code: 'REQUEST_TIMEOUT',
      });
    },
  );
});

test('continued data can exceed the idle timeout', async () => {
  await withPool(
    (stream) => {
      stream.respond();
      let chunks = 0;
      const timer = setInterval(() => {
        stream.write('x');
        if (++chunks === 8) stream.end();
      }, 30);
      stream.once('close', () => {
        clearInterval(timer);
      });
    },
    async (pool, server) => {
      const started = Date.now();
      const response = await pool.post(server.url, '{}', {}).response;
      assert.equal(await toResponse(response).text(), 'xxxxxxxx');
      assert.ok(Date.now() - started > 100);
    },
  );
});

test('Web Response headers exclude transport headers and cancellation closes', async () => {
  await withPool(
    (stream) => {
      // eslint-disable-next-line @typescript-eslint/naming-convention -- HTTP header names use hyphens.
      stream.respond({ 'content-type': 'text/event-stream' });
      stream.write('data: hello\n\n');
    },
    async (pool, server) => {
      const response = await pool.post(server.url, '{}', {}).response;
      response.headers.connection = 'keep-alive, x-private';
      response.headers['x-private'] = 'transport-only';
      response.headers['keep-alive'] = 'timeout=5';
      const web = toResponse(response);
      assert.equal(web.status, 200);
      assert.equal(web.headers.get('content-type'), 'text/event-stream');
      assert.ok([...web.headers.keys()].every((name) => !name.startsWith(':')));
      for (const name of ['connection', 'x-private', 'keep-alive']) {
        assert.equal(web.headers.has(name), false);
      }
      assert.ok(web.body);
      await web.body.cancel();
      await pool.close();
    },
  );
});

test('SDK GET requests accept an empty body without writing after end', async () => {
  await withPool(
    (stream) => {
      stream.respond();
      stream.end('ok');
    },
    async (pool, server) => {
      const response = await pool.request('GET', server.url, Buffer.alloc(0), {})
        .response;
      assert.equal(await toResponse(response).text(), 'ok');
    },
  );
});
