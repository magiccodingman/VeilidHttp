const status = document.querySelector('#status');
const response = await fetch('/api/echo?fixture=pwa');
status.textContent = (await response.json()).path;
if ('serviceWorker' in navigator) await navigator.serviceWorker.register('/service-worker.js');
