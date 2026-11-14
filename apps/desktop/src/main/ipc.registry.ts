import * as z from 'zod/mini';

/**
 * Every IPC channel the renderer may call, named `<area>:<action>`.
 * `input` validates the call's argument list. The preload bridge is generated from this list.
 */
export const ipcRegistry = {
  'app:getVersion': { input: z.tuple([]), output: z.string() },
} as const;

export type Channel = keyof typeof ipcRegistry;
export type ChannelInput<C extends Channel> = z.input<(typeof ipcRegistry)[C]['input']>;
export type ChannelOutput<C extends Channel> = z.output<(typeof ipcRegistry)[C]['output']>;

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
