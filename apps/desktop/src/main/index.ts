import { randomBytes } from 'node:crypto';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  app,
  BrowserWindow,
  clipboard,
  crashReporter,
  net,
  Notification,
  powerSaveBlocker,
  protocol,
  session,
  shell,
} from 'electron';
import { ActivityMonitor, describeChange } from './activity';
import { SignIn } from './auth';
import { agentConfigPath, linkLocalAgent } from './core/agent-config';
import { CoreClient } from './core/client';
import { proxyToCore } from './core/proxy';
import { CoreSupervisor, type CoreLaunch } from './core/supervisor';
import { framesOf, CrashReports } from './crash/reports';
import { deepLinkIn, routeDeepLink, SCHEME } from './deeplinks';
import { registerIpc } from './ipc';
import { NavQueue } from './nav';
import { SleepGuard } from './power';
import { APP_URL, resolveAppFile } from './protocol';
import {
  CONTENT_SECURITY_POLICY,
  DEV_CONTENT_SECURITY_POLICY,
  isAllowedExternal,
  windowOptions,
} from './security';
import { SettingsStore } from './settings';
import { AppTray } from './tray';
import { Updates } from './updates';

const devServerUrl = app.isPackaged ? undefined : process.env.MRL_DEV_SERVER_URL;
if (devServerUrl && !/^http:\/\/(localhost|127\.0\.0\.1):\d+\/?$/.test(devServerUrl)) {
  throw new Error('MRL_DEV_SERVER_URL must be a local address like http://localhost:5173.');
}
// Tests and side-by-side installs keep their own settings folder.
if (process.env.MRL_USER_DATA) app.setPath('userData', process.env.MRL_USER_DATA);
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
  {
    scheme: 'app',
    privileges: {
      standard: true,
      secure: true,
      supportFetchAPI: true,
      corsEnabled: true,
      stream: true,
    },
  },
]);
app.enableSandbox();
// Crash dumps stay on this computer (Doc 22 §14); nothing is uploaded.
crashReporter.start({ uploadToServer: false, compress: true });

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

// ---- One app per account: a second start hands its deep link to the first and exits. ----
const primary = app.requestSingleInstanceLock();
if (!primary) app.quit();

const userData = app.getPath('userData');
const secret = randomBytes(32).toString('hex');
const rendererToken = randomBytes(32).toString('base64url');
const exe = (name: string) => (process.platform === 'win32' ? `${name}.exe` : name);
const agentConfig = agentConfigPath();
const linkAgent = app.isPackaged || process.env.MRL_AGENT_CONFIG !== undefined;
const settings = new SettingsStore(path.join(userData, 'desktop-settings.json'));
const reports = new CrashReports(
  [
    path.join(userData, 'crash-reports'),
    path.join(path.dirname(agentConfig), 'crash-reports'),
  ].filter((d, i, all) => all.indexOf(d) === i),
);
const nav = new NavQueue();
const sleep = new SleepGuard(powerSaveBlocker);

function coreLaunch(): CoreLaunch {
  const program =
    process.env.MRL_CORE_BIN ??
    (app.isPackaged
      ? path.join(process.resourcesPath, 'bin', exe('mrl-server'))
      : path.join(app.getAppPath(), '../../target/debug', exe('mrl-server')));
  const config = process.env.MRL_CORE_CONFIG;
  return {
    program,
    args: ['--mode', 'solo', ...(config ? ['--config', config] : [])],
    env: {
      ...process.env,
      MRL_LAUNCH_SECRET: secret,
      MRL_CRASH_DIR: reports.folder,
      MRL_DESKTOP__AGENT_CONFIG: agentConfig,
    },
  };
}

let activity: ActivityMonitor | null = null;
const supervisor: CoreSupervisor = new CoreSupervisor({
  secret,
  launch: coreLaunch,
  log: (m) => {
    console.warn(`MasterRL core: ${m}`);
  },
  onReady: (ep) => {
    activity?.start();
    if (linkAgent) {
      void linkLocalAgent(agentConfig, secret, {
        serverUrl: `http://127.0.0.1:${String(ep.port)}`,
        secretFile: path.join(userData, 'core', 'launch-secret'),
        gateway: ep.grpcPort ? `127.0.0.1:${String(ep.grpcPort)}` : undefined,
      }).catch((e: unknown) => {
        console.warn(`MasterRL: the local agent's settings were not updated: ${String(e)}`);
      });
    }
  },
  onLost: () => activity?.stop(),
});
const core = new CoreClient(() => supervisor.endpoint(), secret);
const signIn = new SignIn({
  core,
  // Linux desktops may not hand `masterrl://` links to the app; a loopback page always works.
  loopback: process.platform === 'linux' || process.env.MRL_SIGNIN_LOOPBACK === '1',
  openBrowser: (url) => shell.openExternal(url),
});
const updates = new Updates({
  current: app.getVersion(),
  core,
  updater: async () => {
    const { autoUpdater } = await import('electron-updater');
    const feed = process.env.MRL_UPDATE_FEED_URL;
    if (feed && /^(https:\/\/|http:\/\/127\.0\.0\.1:\d+\/)/.test(feed)) {
      autoUpdater.setFeedURL({ provider: 'generic', url: feed });
      autoUpdater.forceDevUpdateConfig = !app.isPackaged;
    }
    return autoUpdater;
  },
});

// ---- The window, which closes to the tray. ----
let win: BrowserWindow | null = null;
let tray: AppTray | null = null;
let quitting = false;

function showWindow(route?: string): void {
  if (win) {
    if (win.isMinimized()) win.restore();
    win.show();
    win.focus();
    if (route) nav.push(route);
    return;
  }
  const window = new BrowserWindow(windowOptions(path.join(__dirname, '../preload/index.cjs')));
  win = window;
  window.once('ready-to-show', () => {
    window.show();
  });
  window.on('closed', () => {
    if (win === window) win = null;
  });
  window.webContents.on('render-process-gone', (_e, details) => {
    if (details.reason === 'clean-exit') return;
    void reports
      .write('desktop', {
        version: app.getVersion(),
        message: `The window stopped: ${details.reason}`,
      })
      .then((id) => {
        window.destroy();
        showWindow(`/settings/crash/${id}`);
      });
  });
  void window.loadURL(route ? `${rendererUrl}#${route}` : rendererUrl);
}

function handleDeepLink(raw: string): void {
  routeDeepLink(raw, {
    completeSignIn: (flow, code) => signIn.complete(flow, code),
    navigate: (route) => {
      showWindow(route);
    },
  });
}

process.on('uncaughtException', (e) => {
  console.error('MasterRL:', e);
  void reports
    .write('desktop', { version: app.getVersion(), message: e.message, frames: framesOf(e.stack) })
    .then((id) => {
      showWindow(`/settings/crash/${id}`);
    });
});

app.on('second-instance', (_event, argv) => {
  const link = deepLinkIn(argv);
  if (link) handleDeepLink(link);
  else showWindow();
});
app.on('open-url', (event, url) => {
  event.preventDefault();
  handleDeepLink(url);
});
app.on('child-process-gone', (_event, details) => {
  if (details.reason === 'clean-exit' || details.reason === 'killed') return;
  void reports.write('desktop', {
    version: app.getVersion(),
    message: `A helper process (${details.type}) stopped: ${details.reason}`,
  });
});

async function stopCore(): Promise<void> {
  activity?.stop();
  sleep.set('manual', false);
  await supervisor.stop();
}

void app.whenReady().then(async () => {
  if (!primary) return;
  if (process.env.MRL_NO_PROTOCOL_REGISTER !== '1') {
    if (process.defaultApp && process.argv[1]) {
      app.setAsDefaultProtocolClient(SCHEME, process.execPath, [path.resolve(process.argv[1])]);
    } else {
      app.setAsDefaultProtocolClient(SCHEME);
    }
  }
  session.defaultSession.setPermissionRequestHandler((_contents, _permission, callback) => {
    callback(false);
  });
  session.defaultSession.setPermissionCheckHandler(() => false);

  protocol.handle('app', async (request) => {
    if (new URL(request.url).pathname.startsWith('/api/')) {
      return proxyToCore(request, {
        token: rendererToken,
        target: () => {
          const ep = supervisor.endpoint();
          return ep ? { port: ep.port, secret } : null;
        },
        allowOrigin: devServerUrl ? rendererOrigin : undefined,
      });
    }
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

  await settings.load();
  activity = new ActivityMonitor({
    core,
    onRunning: (jobs) => tray?.setJobs(jobs),
    onTraining: (active) => sleep.set('training', active),
    onChange: (c) => {
      if (!Notification.isSupported()) return;
      const n = new Notification(describeChange(c));
      n.on('click', () => {
        showWindow('/');
      });
      n.show();
    },
  });

  registerIpc(isRendererPage, {
    'app:getVersion': () => app.getVersion(),
    'app:getInfo': () => ({
      version: app.getVersion(),
      platform: process.platform,
      packaged: app.isPackaged,
    }),
    'app:showFolder': async (which) => {
      let dir: string | null | undefined = reports.folder;
      if (which === 'data') {
        const s = await core.call<{ data_dir?: string | null }>('GET', '/api/v1/desktop/state');
        dir = s.ok ? s.value.data_dir : null;
      } else if (which === 'backups') {
        const b = await core.call<{ folder?: string | null }>('GET', '/api/v1/desktop/backups');
        dir = b.ok ? b.value.folder : null;
      }
      return dir ? (await shell.openPath(dir)) === '' : false;
    },
    'core:connection': () => ({ proxyOrigin: 'app://app', token: rendererToken }),
    'core:status': () => supervisor.status(),
    'core:watch': (seen) => supervisor.watch(seen),
    'core:restart': () => supervisor.restart(),
    'auth:signIn': (provider) => signIn.start(provider),
    'auth:cancelSignIn': () => signIn.cancel(),
    'desktop:blockSleep': (on) => sleep.set('manual', on),
    'nav:next': () => nav.next(),
    'crash:list': () => reports.list(),
    'crash:read': (id) => reports.read(id),
    'crash:copy': async (id) => {
      await clipboard.writeText(await reports.read(id));
      return true;
    },
    'crash:delete': (id) => reports.delete(id),
    'crash:openIssue': async (id) => {
      const url = await reports.issueUrl(id);
      if (!isAllowedExternal(url)) return false;
      await shell.openExternal(url);
      return true;
    },
    'update:status': () => updates.status(),
    'update:check': () => updates.check(),
    'update:install': () => updates.install(),
    'settings:get': () => settings.get(),
    'settings:set': (patch) => settings.set(patch),
  });

  tray = new AppTray({
    open: () => {
      showWindow();
    },
    quit: () => {
      app.quit();
    },
  });
  supervisor.start();
  const link = deepLinkIn(process.argv);
  showWindow();
  if (link) handleDeepLink(link);
  if (settings.get().checkForUpdates && app.isPackaged) {
    setTimeout(() => void updates.check(), 10_000);
  }

  app.on('activate', () => {
    showWindow();
  });
});

// Closing the window frees it; the app, the core and training go on in the tray.
app.on('window-all-closed', () => {
  if (!settings.get().closeToTray || !tray) app.quit();
});

// Quit order: the core stops taking work and drains, then its PostgreSQL stops. Agents and
// training are user services and keep running.
app.on('before-quit', (event) => {
  if (quitting) return;
  event.preventDefault();
  quitting = true;
  void stopCore().finally(() => {
    tray?.destroy();
    app.quit();
  });
});
