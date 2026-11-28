import { ipcMain } from 'electron';
import {
  ipcRegistry,
  parseInput,
  type Channel,
  type ChannelInput,
  type ChannelOutput,
} from './ipc.registry';

/** One handler per registry channel; tsc fails until every channel has one. */
export type Handlers = {
  [C in Channel]: (...args: ChannelInput<C>) => ChannelOutput<C> | Promise<ChannelOutput<C>>;
};

/** Register one validated `ipcMain.handle` per registry channel. */
export function registerIpc(isTrustedPage: (url: string) => boolean, handlers: Handlers): void {
  for (const channel of Object.keys(ipcRegistry) as Channel[]) {
    ipcMain.handle(channel, async (event, ...args: unknown[]) => {
      if (!isTrustedPage(event.senderFrame?.url ?? '')) {
        throw new Error(`"${channel}" was called from a page outside the app and was refused.`);
      }
      const handler = handlers[channel] as (...a: unknown[]) => unknown;
      const result = await handler(...(parseInput(channel, args) as unknown[]));
      return ipcRegistry[channel].output.parse(result);
    });
  }
}
