import { contextBridge, ipcRenderer } from 'electron';

export interface OpenRouteResult {
  siteId: string;
  origin: string;
}

const api = Object.freeze({
  runtime: 'electron' as const,
  protocolVersion: 1,
  openRoute: (routeBlobBase64: string, startPath = '/') =>
    ipcRenderer.invoke('veilid-http:open-route', { routeBlobBase64, startPath }) as Promise<OpenRouteResult>,
  clearSiteData: (siteId: string) => ipcRenderer.invoke('veilid-http:clear-site-data', siteId) as Promise<void>,
  closeSite: () => ipcRenderer.invoke('veilid-http:close-site') as Promise<void>,
});

contextBridge.exposeInMainWorld('veilidShell', api);
