const assert = require('node:assert/strict');
const { once } = require('node:events');
const { createServer } = require('node:http');
const {
  app,
  BrowserWindow,
  session,
} = require('electron');

const SITE_A = 'aaaaaaaaaaaaaaaaaaaaaaaaaa';
const SITE_B = 'bbbbbbbbbbbbbbbbbbbbbbbbbb';
let ORIGIN_A;
let ORIGIN_B;

function responseFor(url) {
  if (![`${SITE_A}.veilid.localhost`, `${SITE_B}.veilid.localhost`].includes(url.hostname)) {
    return new Response('plaintext HTTP outside the virtual Veilid origins is blocked', {
      status: 403,
      headers: { 'content-type': 'text/plain' },
    });
  }
  if (url.pathname === '/sw.js') {
    return new Response(`
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
    `, {
      headers: {
        'content-type': 'text/javascript; charset=utf-8',
        'cache-control': 'no-store',
      },
    });
  }
  if (url.pathname === '/cors-ok') {
    return new Response('cors-ok', {
      headers: {
        'content-type': 'text/plain',
        'access-control-allow-origin': ORIGIN_A,
      },
    });
  }
  if (url.pathname === '/cors-denied') {
    return new Response('cors-denied', {
      headers: { 'content-type': 'text/plain' },
    });
  }
  if (url.pathname === '/stream') {
    const encoder = new TextEncoder();
    const body = new ReadableStream({
      async start(controller) {
        controller.enqueue(encoder.encode('first-'));
        await new Promise(resolve => setTimeout(resolve, 25));
        controller.enqueue(encoder.encode('second'));
        controller.close();
      },
    });
    return new Response(body, {
      headers: { 'content-type': 'text/plain' },
    });
  }
  return new Response(`<!doctype html>
    <meta charset="utf-8">
    <title>VeilidHttp PWA smoke</title>
    <script>
      window.registrationReady = navigator.serviceWorker.register('/sw.js')
        .then(() => navigator.serviceWorker.ready)
        .then(() => true);
    </script>`, {
    headers: { 'content-type': 'text/html; charset=utf-8' },
  });
}

async function writeResponse(webResponse, response) {
  response.statusCode = webResponse.status;
  for (const [name, value] of webResponse.headers.entries()) response.appendHeader(name, value);
  if (!webResponse.body) {
    response.end();
    return;
  }
  const reader = webResponse.body.getReader();
  while (true) {
    const item = await reader.read();
    if (item.done) break;
    if (!response.write(Buffer.from(item.value))) await once(response, 'drain');
  }
  response.end();
}

async function startOriginServer() {
  const server = createServer(async (request, response) => {
    try {
      if (!request.headers.host) throw new Error('missing Host header');
      const url = new URL(request.url ?? '/', `http://${request.headers.host}`);
      await writeResponse(responseFor(url), response);
    } catch (error) {
      response.statusCode = 500;
      response.end(error instanceof Error ? error.message : String(error));
    }
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('loopback test server did not expose a TCP port');
  ORIGIN_A = `http://${SITE_A}.veilid.localhost:${address.port}`;
  ORIGIN_B = `http://${SITE_B}.veilid.localhost:${address.port}`;
  return server;
}

async function waitForServiceWorker(window, requireController = false) {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const ready = await window.webContents.executeJavaScript(
      `window.registrationReady
        ? window.registrationReady.then(() => ${requireController ? 'Boolean(navigator.serviceWorker.controller)' : 'true'}).catch(() => false)
        : false`,
      true,
    );
    if (ready) return;
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error('service worker did not become ready on the VeilidHttp localhost origin');
}

async function browserAssertions(window) {
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
    const corsAllowed = await fetch('${ORIGIN_B}/cors-ok').then(response => response.text());
    let corsDenied = false;
    try {
      await fetch('${ORIGIN_B}/cors-denied');
    } catch {
      corsDenied = true;
    }

    const streamResponse = await fetch('/stream');
    const reader = streamResponse.body.getReader();
    const decoder = new TextDecoder();
    const first = await reader.read();
    const second = await reader.read();
    const done = await reader.read();

    return {
      cached,
      indexed,
      controlled,
      workerResponse,
      corsAllowed,
      corsDenied,
      streamed: decoder.decode(first.value) + decoder.decode(second.value),
      streamDone: done.done,
      wasm: typeof WebAssembly === 'object',
      secureContext: isSecureContext,
      origin: location.origin,
    };
  })()`, true);
}

app.whenReady().then(async () => {
  const originServer = await startOriginServer();
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
    await window.loadURL(`${ORIGIN_A}/`);
    await waitForServiceWorker(window);
    await window.webContents.reload();
    await waitForServiceWorker(window, true);
    const result = await browserAssertions(window);
    assert.equal(result.cached, 'cached-value');
    assert.equal(result.indexed, 'indexed-value');
    assert.equal(result.controlled, true);
    assert.equal(result.workerResponse, 'service-worker-response');
    assert.equal(result.corsAllowed, 'cors-ok');
    assert.equal(result.corsDenied, true);
    assert.equal(result.streamed, 'first-second');
    assert.equal(result.streamDone, true);
    assert.equal(result.wasm, true);
    assert.equal(result.secureContext, true);
    assert.equal(result.origin, ORIGIN_A);

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
    await reopened.loadURL(`${ORIGIN_A}/`);
    const persisted = await reopened.webContents.executeJavaScript(
      `caches.open('pwa-smoke').then(cache => cache.match('/cached')).then(response => response.text())`,
      true,
    );
    assert.equal(persisted, 'cached-value');
    reopened.destroy();
    await targetSession.clearStorageData();
    await new Promise(resolve => originServer.close(resolve));
    app.exit(0);
  } catch (error) {
    console.error(error);
    originServer.close();
    app.exit(1);
  }
});
