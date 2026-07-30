const CACHE = 'veilid-http-fixture-v1';
self.addEventListener('install', (event) => event.waitUntil(caches.open(CACHE).then((cache) => cache.addAll(['/', '/app.js']))));
self.addEventListener('fetch', (event) => event.respondWith(caches.match(event.request).then((cached) => cached || fetch(event.request))));
