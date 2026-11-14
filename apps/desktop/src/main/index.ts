import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { app, BrowserWindow, net, protocol, session, shell } from 'electron';
import { registerIpc } from './ipc';
import { APP_URL, resolveAppFile } from './protocol';
import {
  CONTENT_SECURITY_POLICY,
  DEV_CONTENT_SECURITY_POLICY,
  isAllowedExternal,
  windowOptions,
} from './security';

const devServerUrl = app.isPackaged ? undefined : process.env.MRL_DEV_SERVER_URL;
if (devServerUrl && !/^http:\/\/(localhost|127\.0\.0\.1):\d+\/?$/.test(devServerUrl)) {
  throw new Error('MRL_DEV_SERVER_URL must be a local address like http://localhost:5173.');
}
const rendererUrl = devServerUrl ?? APP_URL;
const rendererRoot = app.isPackaged
  ? path.join(process.resourcesPath, 'renderer')
  : path.join(app.getAppPath(), '../web/dist');

const originOf = (url: string) => {
  const parsed = new URL(url);
  return `${parsed.protocol}//${parsed.host}`;
};
const rendererOrigin = originOf(rendererUrl);

function isRendererPage(url: string): boolean {
  try {
    return originOf(url) === rendererOrigin;
  } catch {
    return false;
  }
}

function openExternal(url: string): void {
  if (isAllowedExternal(url)) void shell.openExternal(url);
}

protocol.registerSchemesAsPrivileged([
  { scheme: 'app', privileges: { standard: true, secure: true } },
]);
app.enableSandbox();

app.on('web-contents-created', (_event, contents) => {
  contents.setWindowOpenHandler(({ url }) => {
    openExternal(url);
    return { action: 'deny' };
  });
  contents.on('will-navigate', (event) => {
    if (isRendererPage(event.url)) return;
    event.preventDefault();
    openExternal(event.url);
  });
  contents.on('will-attach-webview', (event) => {
    event.preventDefault();
  });
});

function createWindow(): void {
  const window = new BrowserWindow(windowOptions(path.join(__dirname, '../preload/index.cjs')));
  window.once('ready-to-show', () => {
    window.show();
  });
  void window.loadURL(rendererUrl);
}

void app.whenReady().then(() => {
  session.defaultSession.setPermissionRequestHandler((_contents, _permission, callback) => {
    callback(false);
  });
  session.defaultSession.setPermissionCheckHandler(() => false);

  protocol.handle('app', async (request) => {
    const file = resolveAppFile(rendererRoot, request.url);
    if (!file) return new Response('Not found', { status: 404 });
    try {
      const response = await net.fetch(pathToFileURL(file).toString());
      const headers = new Headers(response.headers);
      headers.set('Content-Security-Policy', CONTENT_SECURITY_POLICY);
      headers.set('X-Content-Type-Options', 'nosniff');
      return new Response(response.body, { status: response.status, headers });
    } catch {
      return new Response('Not found', { status: 404 });
    }
  });

  if (devServerUrl) {
    session.defaultSession.webRequest.onHeadersReceived((details, callback) => {
      callback({
        responseHeaders: {
          ...details.responseHeaders,
          'Content-Security-Policy': [DEV_CONTENT_SECURITY_POLICY],
        },
      });
    });
  }

  registerIpc(isRendererPage);
  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});
