import { contextBridge, ipcRenderer } from 'electron';
import { createBridge, type Bridge } from '../main/ipc.registry';

contextBridge.exposeInMainWorld(
  'mrl',
  createBridge((channel, args) => ipcRenderer.invoke(channel, ...args)),
);

declare global {
  interface Window {
    /** Typed IPC bridge to Electron main, generated from `ipc.registry.ts`. */
    readonly mrl: Bridge;
  }
}
