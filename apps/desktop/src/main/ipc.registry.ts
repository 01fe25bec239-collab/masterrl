import * as z from 'zod/mini';

/** The core's state as the renderer sees it (see `core/supervisor.ts`). */
export const coreStatusSchema = z.object({
  /** Grows with every change; pass it to `core:watch`. */
  version: z.number(),
  phase: z.enum(['starting', 'ready', 'reconnecting', 'failed', 'stopping', 'stopped']),
  /** Restarts since the app opened. */
  restarts: z.number(),
  /** Plain words for `reconnecting` and `failed`. */
  reason: z.optional(z.string()),
  /** When the next start is tried (ms since 1970), while reconnecting. */
  retryAt: z.optional(z.number()),
});

const crashSummarySchema = z.object({
  id: z.string(),
  program: z.string(),
  time: z.string(),
  message: z.string(),
});

const signInResultSchema = z.union([
  z.object({ status: z.literal('signed_in') }),
  z.object({ status: z.literal('cancelled') }),
  z.object({ status: z.literal('failed'), code: z.string(), detail: z.string() }),
]);

const updateStatusSchema = z.object({
  phase: z.enum([
    'idle',
    'checking',
    'up_to_date',
    'available',
    'downloading',
    'ready',
    'preparing',
    'error',
  ]),
  /** The newer version, when one is available. */
  version: z.optional(z.string()),
  /** Download progress 0–100. */
  percent: z.optional(z.number()),
  /** What went wrong, or what the preparation found, in plain words. */
  detail: z.optional(z.string()),
});

const settingsSchema = z.object({
  /** Closing the window keeps the app (and training) running in the tray. */
  closeToTray: z.boolean(),
  /** Look for updates when the app starts. */
  checkForUpdates: z.boolean(),
});

/** Ids of files in the crash report folder: `<program>-<number>-<number>`. */
const crashId = z.string().check(z.regex(/^[a-z][a-z0-9-]{0,40}-\d{1,16}-\d{1,10}$/));

/**
 * Every IPC channel the renderer may call, named `<area>:<action>`.
 * `input` validates the call's argument list. The preload bridge is generated from this list.
 */
export const ipcRegistry = {
  'app:getVersion': { input: z.tuple([]), output: z.string() },
  // <ipc P30>
  /** Version, OS and whether this is an installed build. */
  'app:getInfo': {
    input: z.tuple([]),
    output: z.object({ version: z.string(), platform: z.string(), packaged: z.boolean() }),
  },
  /** Opens one of the app's folders in the file manager. */
  'app:showFolder': {
    input: z.tuple([z.enum(['crash-reports', 'data', 'backups'])]),
    output: z.boolean(),
  },
  /**
   * How the renderer reaches the core: through the app's own address (`app://app/api/…`),
   * with this launch's renderer token. The core's launch secret never leaves Electron main.
   */
  'core:connection': {
    input: z.tuple([]),
    output: z.object({ proxyOrigin: z.string(), token: z.string() }),
  },
  'core:status': { input: z.tuple([]), output: coreStatusSchema },
  /** Resolves when the status version passes `seen` (or after 25 s with the current one). */
  'core:watch': { input: z.tuple([z.number()]), output: coreStatusSchema },
  /** Starts the core again after it failed. */
  'core:restart': { input: z.tuple([]), output: coreStatusSchema },
  /** Sign in in the system browser; resolves when the browser comes back to the app. */
  'auth:signIn': {
    input: z.tuple([z.enum(['google', 'github'])]),
    output: signInResultSchema,
  },
  'auth:cancelSignIn': { input: z.tuple([]), output: z.boolean() },
  /** Keeps the computer awake while true (the core's training activity also does). */
  'desktop:blockSleep': { input: z.tuple([z.boolean()]), output: z.boolean() },
  /** Whether the person asked to keep the computer awake, and whether training does. */
  'desktop:sleepState': {
    input: z.tuple([]),
    output: z.object({ manual: z.boolean(), training: z.boolean(), awake: z.boolean() }),
  },
  /** Resolves with the next screen a deep link asks for (`''` after 25 s without one). */
  'nav:next': { input: z.tuple([]), output: z.string() },
  'crash:list': { input: z.tuple([]), output: z.array(crashSummarySchema) },
  /** The report as it would be shared (already redacted). */
  'crash:read': { input: z.tuple([crashId]), output: z.string() },
  /** Copies the report (already redacted) to the clipboard. */
  'crash:copy': { input: z.tuple([crashId]), output: z.boolean() },
  'crash:delete': { input: z.tuple([crashId]), output: z.boolean() },
  /** Opens GitHub's new-issue form, filled in with the report, for the person to review. */
  'crash:openIssue': { input: z.tuple([crashId]), output: z.boolean() },
  'update:status': { input: z.tuple([]), output: updateStatusSchema },
  'update:check': { input: z.tuple([]), output: updateStatusSchema },
  /** Saves checkpoints and a backup through the core, then installs and restarts. */
  'update:install': { input: z.tuple([]), output: updateStatusSchema },
  'settings:get': { input: z.tuple([]), output: settingsSchema },
  'settings:set': { input: z.tuple([z.partial(settingsSchema)]), output: settingsSchema },
} as const;

export type Channel = keyof typeof ipcRegistry;
export type ChannelInput<C extends Channel> = z.input<(typeof ipcRegistry)[C]['input']>;
export type ChannelOutput<C extends Channel> = z.output<(typeof ipcRegistry)[C]['output']>;
export type CoreStatus = z.output<typeof coreStatusSchema>;
export type UpdateStatus = z.output<typeof updateStatusSchema>;
export type DesktopSettings = z.output<typeof settingsSchema>;
export type SignInResult = z.output<typeof signInResultSchema>;
export type CrashSummary = z.output<typeof crashSummarySchema>;

type Area<C extends Channel> = C extends `${infer A}:${string}` ? A : never;
type Action<C extends Channel> = C extends `${string}:${infer M}` ? M : never;
type Call<C extends Channel> = (...args: ChannelInput<C>) => Promise<ChannelOutput<C>>;

/** What the renderer sees as `window.mrl`, e.g. `window.mrl.app.getVersion()`. */
export type Bridge = {
  [A in Area<Channel>]: {
    [C in Channel as C extends `${A}:${string}` ? Action<C> : never]: Call<C>;
  };
};

export function createBridge(invoke: (channel: Channel, args: unknown[]) => Promise<unknown>) {
  const bridge: Record<string, Record<string, (...args: unknown[]) => Promise<unknown>>> = {};
  for (const channel of Object.keys(ipcRegistry) as Channel[]) {
    const [area = '', action = ''] = channel.split(':');
    (bridge[area] ??= {})[action] = (...args) => invoke(channel, args);
  }
  return bridge as Bridge;
}

/** Validate the renderer's arguments; throws a plain-language error when they do not match. */
export function parseInput<C extends Channel>(channel: C, args: unknown): ChannelInput<C> {
  const result = ipcRegistry[channel].input.safeParse(args);
  if (!result.success) throw new Error(`The app sent data that "${channel}" does not accept.`);
  return result.data as ChannelInput<C>;
}
