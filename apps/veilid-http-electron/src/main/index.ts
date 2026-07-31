import {
  app,
  BrowserWindow,
  ipcMain,
  protocol,
  session,
  WebContentsView,
  type Session,
} from 'electron';
import { Sidecar } from './sidecar';
import path from 'node:path';
import { isSiteId, routeOrigin } from '../shared/site-id';
import { findLaunchTarget } from './launch-target';

declare const SHELL_WEBPACK_ENTRY: string;
declare const SHELL_PRELOAD_WEBPACK_ENTRY: string;

const SHELL_HEIGHT = 132;
const registeredSessions = new WeakSet<Session>();

protocol.registerSchemesAsPrivileged([
  {
    scheme: 'veilid',
    privileges: {
      standard: true,
      secure: true,
      supportFetchAPI: true,
      corsEnabled: true,
      allowServiceWorkers: true,
      stream: true,
      codeCache: true,
    },
  },
]);

const sidecar = new Sidecar();
let shellWindow: BrowserWindow | undefined;
let siteView: WebContentsView | undefined;

function partitionFor(siteId: string): Session {
  return session.fromPartition(`persist:veilid-site-${siteId}`, { cache: true });
}

function safeStartPath(value: string): string {
  if (!value.startsWith('/') || value.startsWith('//')) throw new Error('Start path must be origin-relative');
  return value;
}

function registerSiteProtocol(targetSession: Session): void {
  if (registeredSessions.has(targetSession)) return;
  registeredSessions.add(targetSession);

  targetSession.protocol.handle('veilid', async (request) => {
    const url = new URL(request.url);
    if (!isSiteId(url.hostname)) return new Response('Invalid Veilid route identifier', { status: 400 });

    try {
      const response = await sidecar.streamRequest<{
        status: number;
        headers: Array<[string, string]>;
      }>('httpRequest', {
        siteId: url.hostname,
        method: request.method,
        pathAndQuery: `${url.pathname}${url.search}`,
        headers: [...request.headers.entries()],
      }, request.body, request.signal);
      return new Response(response.body, {
        status: response.result.status,
        headers: response.result.headers,
      });
    } catch (error) {
      return new Response(error instanceof Error ? error.message : String(error), {
        status: 502,
        headers: { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' },
      });
    }
  });
}

function resizeSiteView(): void {
  if (!shellWindow || !siteView) return;
  const [width = 0, height = 0] = shellWindow.getContentSize();
  siteView.setBounds({ x: 0, y: SHELL_HEIGHT, width, height: Math.max(0, height - SHELL_HEIGHT) });
}

async function openSite(siteId: string, startPath = '/'): Promise<void> {
  if (!shellWindow) throw new Error('Trusted shell is not ready');
  siteView?.webContents.close();
  if (siteView) shellWindow.contentView.removeChildView(siteView);

  const targetSession = partitionFor(siteId);
  registerSiteProtocol(targetSession);
  siteView = new WebContentsView({
    webPreferences: {
      session: targetSession,
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      webSecurity: true,
      allowRunningInsecureContent: false,
    },
  });
  siteView.webContents.setWindowOpenHandler(() => ({ action: 'deny' }));
  siteView.webContents.on('will-navigate', (event, navigationUrl) => {
    const destination = new URL(navigationUrl);
    if (!['veilid:', 'https:'].includes(destination.protocol)) event.preventDefault();
  });
  shellWindow.contentView.addChildView(siteView);
  resizeSiteView();
  await siteView.webContents.loadURL(new URL(safeStartPath(startPath), routeOrigin(siteId)).toString());
}

async function createShell(): Promise<void> {
  shellWindow = new BrowserWindow({
    width: 1280,
    height: 820,
    minWidth: 720,
    minHeight: 480,
    webPreferences: {
      preload: SHELL_PRELOAD_WEBPACK_ENTRY,
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      webSecurity: true,
    },
  });
  shellWindow.webContents.setWindowOpenHandler(() => ({ action: 'deny' }));
  shellWindow.on('resize', resizeSiteView);
  shellWindow.on('closed', () => {
    siteView = undefined;
    shellWindow = undefined;
  });
  await shellWindow.loadURL(SHELL_WEBPACK_ENTRY);
}

ipcMain.handle('veilid-http:open-route', async (_event, input: unknown) => {
  if (!input || typeof input !== 'object') throw new Error('Invalid route request');
  const { routeBlobBase64, startPath = '/' } = input as { routeBlobBase64?: unknown; startPath?: unknown };
  if (typeof routeBlobBase64 !== 'string') throw new Error('RouteBlob must be a string');
  if (typeof startPath !== 'string') throw new Error('Start path must be a string');
  const response = await sidecar.request<{ fingerprint: string }>('importRoute', {
    routeBlobBase64,
  });
  const result = response.result;
  if (!isSiteId(result.fingerprint)) throw new Error('Sidecar returned an invalid route fingerprint');
  await openSite(result.fingerprint, startPath);
  return { siteId: result.fingerprint, origin: routeOrigin(result.fingerprint) };
});

ipcMain.handle('veilid-http:clear-site-data', async (_event, siteId: unknown) => {
  if (typeof siteId !== 'string' || !isSiteId(siteId)) throw new Error('Invalid site identifier');
  const targetSession = partitionFor(siteId);
  await Promise.all([
    targetSession.clearCache(),
    targetSession.clearStorageData({ origin: routeOrigin(siteId) }),
  ]);
});

ipcMain.handle('veilid-http:close-site', () => {
  if (siteView && shellWindow) {
    shellWindow.contentView.removeChildView(siteView);
    siteView.webContents.close();
    siteView = undefined;
  }
});

app.whenReady().then(async () => {
  await sidecar.start();
  registerSiteProtocol(session.defaultSession);
  await createShell();
  const target = findLaunchTarget(process.argv.slice(1), path.dirname(process.execPath));
  if (target) {
    const imported = await sidecar.request<{ fingerprint: string }>('importRoute', {
      routeBlobBase64: target.routeBlobBase64,
    });
    await openSite(imported.result.fingerprint, target.startPath);
  }
});

app.on('before-quit', () => sidecar.stop());
app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

export { partitionFor, registerSiteProtocol, safeStartPath };
