import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const testDirectory = path.dirname(fileURLToPath(import.meta.url));
const electronRoot = path.resolve(testDirectory, '..');

async function source(relativePath) {
  return readFile(path.join(electronRoot, relativePath), 'utf8');
}

test('loaded sites remain browser-only and privileged permissions default closed', async () => {
  const main = await source('src/main/index.ts');
  assert.match(main, /contextIsolation:\s*true/);
  assert.match(main, /nodeIntegration:\s*false/);
  assert.match(main, /sandbox:\s*true/);
  assert.match(main, /webSecurity:\s*true/);
  assert.match(main, /setPermissionRequestHandler\([\s\S]*callback\(false\)/);
  assert.match(main, /setPermissionCheckHandler\(\(\) => false\)/);
  assert.match(main, /setDevicePermissionHandler\(\(\) => false\)/);
  assert.match(main, /setDisplayMediaRequestHandler\([\s\S]*callback\(\{\}\)/);
});

test('only the trusted shell may invoke privileged Electron IPC handlers', async () => {
  const main = await source('src/main/index.ts');
  assert.match(main, /function assertTrustedShellSender/);
  assert.match(main, /event\.sender !== shellWindow\.webContents/);
  assert.match(main, /event\.senderFrame !== shellWindow\.webContents\.mainFrame/);
  assert.equal((main.match(/assertTrustedShellSender\(event\);/g) ?? []).length, 3);
});

test('site main-frame navigation stays inside the current VeilidHttp origin', async () => {
  const main = await source('src/main/index.ts');
  assert.match(main, /new URL\(navigationUrl\)\.origin === new URL\(routeOrigin\(siteId, originPort\)\)\.origin/);
  assert.doesNotMatch(main, /destination\.protocol === ['"]https:['"]/);
  assert.match(main, /webContents\.on\(['"]will-navigate['"]/);
  assert.match(main, /webContents\.on\(['"]will-redirect['"]/);
  assert.match(main, /setWindowOpenHandler\(\(\) => \(\{ action: ['"]deny['"] \}\)\)/);
});

test('loopback transport requires an Electron-only launch secret and strips it before Veilid', async () => {
  const main = await source('src/main/index.ts');
  const loopback = await source('src/main/loopback-origin.ts');
  assert.match(main, /randomBytes\(32\)\.toString\(['"]base64url['"]\)/);
  assert.match(main, /onBeforeSendHeaders/);
  assert.match(main, /requestHeaders\[LOOPBACK_AUTH_HEADER\] = loopbackClientSecret/);
  assert.match(loopback, /timingSafeEqual/);
  assert.match(loopback, /VeilidHttp loopback access denied/);
  assert.match(loopback, /name\.toLowerCase\(\) === LOOPBACK_AUTH_HEADER/);
  assert.match(loopback, /server\.on\(['"]upgrade['"], \(_request, socket\) => socket\.destroy\(\)\)/);
});

test('packaged builds always launch the bundled native transport executable', async () => {
  const sidecar = await source('src/main/sidecar.ts');
  assert.match(
    sidecar,
    /const executable = app\.isPackaged\s*\? path\.join\(process\.resourcesPath, name\)\s*:\s*process\.env\.VEILID_HTTP_NATIVE_PATH/,
  );
  assert.match(sidecar, /delete childEnvironment\.VEILID_HTTP_NATIVE_PATH/);
  assert.match(sidecar, /shell:\s*false/);
});
