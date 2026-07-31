interface Window {
  veilidShell: {
    readonly runtime: 'electron';
    readonly protocolVersion: 1;
    openRoute(routeBlobBase64: string, startPath?: string): Promise<{ siteId: string; origin: string }>;
    clearSiteData(siteId: string): Promise<void>;
    closeSite(): Promise<void>;
  };
}
