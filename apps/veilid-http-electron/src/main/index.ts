import {
  app,
  BrowserWindow,
  ipcMain,
  session,
  WebContentsView,
  type IpcMainInvokeEvent,
  type Session,
} from 'electron';
import crypto from 'node:crypto';
import path from 'node:path';
import { Sidecar } from './sidecar';
import {
  DEFAULT_ORIGIN_PORT,
  isSiteId,
  ORIGIN_HOST_SUFFIX,
  routeOrigin,
} from '../shared/site-id';
import { findLaunchTarget } from './launch-target';
import { LOOPBACK_AUTH_HEADER, LoopbackOriginServer } from './loopback-origin';

declare const SHELL_WEBPACK_ENTRY: string;
declare const SHELL_PRELOAD_WEBPACK_ENTRY: string;

const SHELL_HEIGHT = 132;
const sidecar = new Sidecar();
const originPort = parseOriginPort(process.env.VEILID_HTTP_ORIGIN_PORT);
const loopbackClientSecret = crypto.randomBytes(32).toString('base64url');
const originServer = new LoopbackOriginServer(sidecar, originPort, loopbackClientSecret);
const configuredSiteSessions = new WeakSet<Session>();
let shellWindow: BrowserWindow | undefined;
let siteView: WebContentsView | undefined;

function parseOriginPort(value: string | undefined): number {
  if (value === undefined || value.length === 0) return DEFAULT_ORIGIN_PORT;
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error('VEILID_HTTP_ORIGIN_PORT must be an integer from 1 through 65535');
  }
  return port;
}

function siteIdFromLoopbackUrl(value: string): string | undefined {
  try {
    const destination = new URL(value);
    if (destination.protocol !== 'http:' || Number(destination.port) !== originPort) return undefined;
    if (!destination.hostname.endsWith(ORIGIN_HOST_SUFFIX)) return undefined;
    const siteId = destination.hostname.slice(0, -ORIGIN_HOST_SUFFIX.length);
    return isSiteId(siteId) ? siteId : undefined;
  } catch {
    return undefined;
  }
}

function configureSiteSession(targetSession: Session): void {
  if (configuredSiteSessions.has(targetSession)) return;
  configuredSiteSessions.add(targetSession);

  // Loaded applications currently receive browser storage/network capabilities only.
  // Privileged browser permissions remain closed until a user-facing permission model exists.
  targetSession.setPermissionRequestHandler((_webContents, _permission, callback) => callback(false));
  targetSession.setPermissionCheckHandler(() => false);
  targetSession.setDevicePermissionHandler(() => false);
  targetSession.setDisplayMediaRequestHandler((_request, callback) => callback({}));

  targetSession.webRequest.onBeforeSendHeaders(
    { urls: ['http://*.veilid.localhost/*'] },
    (details, callback) => {
      if (!siteIdFromLoopbackUrl(details.url)) {
        callback({ requestHeaders: details.requestHeaders });
        return;
      }
      const requestHeaders = { ...details.requestHeaders };
      for (const headerName of Object.keys(requestHeaders)) {
        if (headerName.toLowerCase() === LOOPBACK_AUTH_HEADER) delete requestHeaders[headerName];
      }
      requestHeaders[LOOPBACK_AUTH_HEADER] = loopbackClientSecret;
      callback({ requestHeaders });
    },
  );
}

function partitionFor(siteId: string): Session {
  const targetSession = session.fromPartition(`persist:veilid-site-${siteId}`, { cache: true });
  configureSiteSession(targetSession);
  return targetSession;
}

function safeStartPath(value: string): string {
  if (!value.startsWith('/') || value.startsWith('//')) {
    throw new Error('Start path must be origin-relative');
  }
  return value;
}

function isAllowedSiteNavigation(navigationUrl: string, siteId: string): boolean {
  try {
    return new URL(navigationUrl).origin === new URL(routeOrigin(siteId, originPort)).origin;
  } catch {
    return false;
  }
}

function assertTrustedShellSender(event: IpcMainInvokeEvent): void {
  if (!shellWindow
      || event.sender !== shellWindow.webContents
      || event.senderFrame !== shellWindow.webContents.mainFrame) {
    throw new Error('Untrusted Electron IPC sender');
  }
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

  siteView = new WebContentsView({
    webPreferences: {
      session: partitionFor(siteId),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      webSecurity: true,
      allowRunningInsecureContent: false,
    },
  });
  siteView.webContents.setWindowOpenHandler(() => ({ action: 'deny' }));
  siteView.webContents.on('will-navigate', (event, navigationUrl) => {
    if (!isAllowedSiteNavigation(navigationUrl, siteId)) event.preventDefault();
  });
  siteView.webContents.on('will-redirect', (event, navigationUrl) => {
    if (!isAllowedSiteNavigation(navigationUrl, siteId)) event.preventDefault();
  });
  shellWindow.contentView.addChildView(siteView);
  resizeSiteView();
  await siteView.webContents.loadURL(
    new URL(safeStartPath(startPath), routeOrigin(siteId, originPort)).toString(),
  );
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

ipcMain.handle('veilid-http:open-route', async (event, input: unknown) => {
  assertTrustedShellSender(event);
  if (!input || typeof input !== 'object') throw new Error('Invalid route request');
  const { routeBlobBase64, startPath = '/' } = input as {
    routeBlobBase64?: unknown;
    startPath?: unknown;
  };
  if (typeof routeBlobBase64 !== 'string') throw new Error('RouteBlob must be a string');
  if (typeof startPath !== 'string') throw new Error('Start path must be a string');
  const response = await sidecar.request<{ fingerprint: string }>('importRoute', {
    routeBlobBase64,
  });
  const result = response.result;
  if (!isSiteId(result.fingerprint)) {
    throw new Error('Sidecar returned an invalid route fingerprint');
  }
  await openSite(result.fingerprint, startPath);
  return {
    siteId: result.fingerprint,
    origin: routeOrigin(result.fingerprint, originPort),
  };
});

ipcMain.handle('veilid-http:clear-site-data', async (event, siteId: unknown) => {
  assertTrustedShellSender(event);
  if (typeof siteId !== 'string' || !isSiteId(siteId)) {
    throw new Error('Invalid site identifier');
  }
  const targetSession = partitionFor(siteId);
  await Promise.all([
    targetSession.clearCache(),
    targetSession.clearStorageData({ origin: routeOrigin(siteId, originPort) }),
  ]);
});

ipcMain.handle('veilid-http:close-site', (event) => {
  assertTrustedShellSender(event);
  if (siteView && shellWindow) {
    shellWindow.contentView.removeChildView(siteView);
    siteView.webContents.close();
    siteView = undefined;
  }
});

app.whenReady().then(async () => {
  await sidecar.start();
  await originServer.start();
  await createShell();
  const target = findLaunchTarget(process.argv.slice(1), path.dirname(process.execPath));
  if (target) {
    const imported = await sidecar.request<{ fingerprint: string }>('importRoute', {
      routeBlobBase64: target.routeBlobBase64,
    });
    await openSite(imported.result.fingerprint, target.startPath);
  }
}).catch((error: unknown) => {
  console.error(error);
  app.quit();
});

app.on('before-quit', () => {
  void originServer.stop();
  sidecar.stop();
});
app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

export {
  isAllowedSiteNavigation,
  parseOriginPort,
  partitionFor,
  safeStartPath,
  siteIdFromLoopbackUrl,
};
