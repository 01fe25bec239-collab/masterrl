import { app, ipcMain } from 'electron';
import {
  ipcRegistry,
  parseInput,
  type Channel,
  type ChannelInput,
  type ChannelOutput,
} from './ipc.registry';

type Handlers = {
  [C in Channel]: (...args: ChannelInput<C>) => ChannelOutput<C> | Promise<ChannelOutput<C>>;
};

const handlers: Handlers = {
  'app:getVersion': () => app.getVersion(),
};

/** Register one validated `ipcMain.handle` per registry channel. */
export function registerIpc(isTrustedPage: (url: string) => boolean): void {
  for (const channel of Object.keys(ipcRegistry) as Channel[]) {
    ipcMain.handle(channel, async (event, ...args: unknown[]) => {
      if (!isTrustedPage(event.senderFrame?.url ?? '')) {
        throw new Error(`"${channel}" was called from a page outside the app and was refused.`);
      }
      const result = await handlers[channel](...parseInput(channel, args));
      return ipcRegistry[channel].output.parse(result);
    });
  }
}
