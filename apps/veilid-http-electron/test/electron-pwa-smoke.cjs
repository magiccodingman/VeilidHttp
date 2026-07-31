const assert = require('node:assert/strict');
const http = require('node:http');
const { Readable } = require('node:stream');
const {
  app,
  BrowserWindow,
  session,
} = require('electron');

const SITE_A = 'aaaaaaaaaaaaaaaaaaaaaaaaaa';
const SITE_B = 'bbbbbbbbbbbbbbbbbbbbbbbbbb';

function createFixtureServer() {
  return http.createServer((request, response) => {
    const host = request.headers.host ?? '';
    const port = response.socket.localPort;
    const originA = `http://${SITE_A}.veilid.localhost:${port}`;
    const pathname = new URL(request.url ?? '/', originA).pathname;
    if (pathname === '/sw.js') {
      response.writeHead(200, {
        'content-type': 'text/javascript; charset=utf-8',
        'cache-control': 'no-store',
      });
      response.end(`
        self.addEventListener('install', event => event.waitUntil(self.skipWaiting()));
        self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));
        self.addEventListener('fetch', event => {
          const url = new URL(event.request.url);
          if (url.pathname === '/worker-data') {
            event.respondWith(new Response('service-worker-response', {
              headers: { 'content-type': 'text/plain' }
            }));
          }
        });
      `);
      return;
    }
    if (pathname === '/cors-ok') {
      response.writeHead(200, {
        'content-type': 'text/plain',
        'access-control-allow-origin': originA,
      });
      response.end('cors-ok');
      return;
    }
    if (pathname === '/cors-denied') {
      response.writeHead(200, { 'content-type': 'text/plain' });
      response.end('cors-denied');
      return;
    }
    if (pathname === '/stream') {
      response.writeHead(200, { 'content-type': 'text/plain' });
      Readable.from((async function* stream() {
        yield 'first-';
        await new Promise(resolve => setTimeout(resolve, 25));
        yield 'second';
      })()).pipe(response);
      return;
    }
    if (!host.startsWith(`${SITE_A}.`) && !host.startsWith(`${SITE_B}.`)) {
      response.writeHead(400);
      response.end('invalid host');
      return;
    }
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    response.end(`<!doctype html>
      <meta charset="utf-8">
      <title>VeilidHttp PWA smoke</title>
      <script>
        window.registrationReady = navigator.serviceWorker.register('/sw.js')
          .then(() => navigator.serviceWorker.ready)
          .then(() => true);
      </script>`);
  });
}

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('fixture server has no TCP port');
  return address.port;
}

async function close(server) {
  await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
}

async function waitForServiceWorker(window) {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const ready = await window.webContents.executeJavaScript(
      'window.registrationReady ? window.registrationReady.catch(() => false) : false',
      true,
    );
    if (ready) return;
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error('service worker did not become ready on the loopback origin');
}

async function browserAssertions(window, originB) {
  return window.webContents.executeJavaScript(`(async () => {
    const cache = await caches.open('pwa-smoke');
    await cache.put('/cached', new Response('cached-value'));
    const cached = await (await cache.match('/cached')).text();

    const indexed = await new Promise((resolve, reject) => {
      const request = indexedDB.open('pwa-smoke', 1);
      request.onupgradeneeded = () => request.result.createObjectStore('values');
      request.onerror = () => reject(request.error);
      request.onsuccess = () => {
        const database = request.result;
        const write = database.transaction('values', 'readwrite');
        write.objectStore('values').put('indexed-value', 'key');
        write.oncomplete = () => {
          const read = database.transaction('values').objectStore('values').get('key');
          read.onerror = () => reject(read.error);
          read.onsuccess = () => resolve(read.result);
        };
      };
    });

    const controlled = Boolean(navigator.serviceWorker.controller);
    const workerResponse = await fetch('/worker-data').then(response => response.text());
    const corsAllowed = await fetch('${originB}/cors-ok').then(response => response.text());
    let corsDenied = false;
    try {
      await fetch('${originB}/cors-denied');
    } catch {
      corsDenied = true;
    }

    const streamResponse = await fetch('/stream');
    const reader = streamResponse.body.getReader();
    const decoder = new TextDecoder();
    let streamed = '';
    while (true) {
      const part = await reader.read();
      if (part.done) break;
      streamed += decoder.decode(part.value, { stream: true });
    }
    streamed += decoder.decode();

    return {
      cached,
      indexed,
      controlled,
      workerResponse,
      corsAllowed,
      corsDenied,
      streamed,
      wasm: typeof WebAssembly === 'object',
      secureContext: isSecureContext,
    };
  })()`, true);
}

app.whenReady().then(async () => {
  const server = createFixtureServer();
  const port = await listen(server);
  const originA = `http://${SITE_A}.veilid.localhost:${port}`;
  const originB = `http://${SITE_B}.veilid.localhost:${port}`;
  const partitionName = `persist:veilid-pwa-smoke-${process.pid}`;
  const targetSession = session.fromPartition(partitionName, { cache: true });
  const window = new BrowserWindow({
    show: false,
    webPreferences: {
      session: targetSession,
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      webSecurity: true,
    },
  });

  try {
    await window.loadURL(`${originA}/`);
    await waitForServiceWorker(window);
    await window.webContents.reload();
    await waitForServiceWorker(window);
    const result = await browserAssertions(window, originB);
    assert.equal(result.cached, 'cached-value');
    assert.equal(result.indexed, 'indexed-value');
    assert.equal(result.controlled, true);
    assert.equal(result.workerResponse, 'service-worker-response');
    assert.equal(result.corsAllowed, 'cors-ok');
    assert.equal(result.corsDenied, true);
    assert.equal(result.streamed, 'first-second');
    assert.equal(result.wasm, true);
    assert.equal(result.secureContext, true);

    window.destroy();
    const reopened = new BrowserWindow({
      show: false,
      webPreferences: {
        session: session.fromPartition(partitionName, { cache: true }),
        contextIsolation: true,
        nodeIntegration: false,
        sandbox: true,
        webSecurity: true,
      },
    });
    await reopened.loadURL(`${originA}/`);
    const persisted = await reopened.webContents.executeJavaScript(
      `caches.open('pwa-smoke').then(cache => cache.match('/cached')).then(response => response.text())`,
      true,
    );
    assert.equal(persisted, 'cached-value');
    reopened.destroy();
    await targetSession.clearStorageData();
    await close(server);
    app.exit(0);
  } catch (error) {
    console.error(error);
    await close(server).catch(() => undefined);
    app.exit(1);
  }
});
