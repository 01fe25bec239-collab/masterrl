import { Menu, nativeImage, Tray, type MenuItemConstructorOptions } from 'electron';
import type { RunningJob } from './activity';

/** A 32×32 ring-and-dot mark, drawn in black with transparency (a macOS template image). */
const ICON =
  'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAAeklEQVR42u1XWw4AIAjy/pe2I6ShQi03/wyp+SCzb3nzjdMStxFx0FuSV8Wnko+er3rGI5zqYkrjdVRyGLOzn0PYGQIOxEOAp32/jYveBhk8MAF0+n0C9xMYK0JKG9IHkcQopi8jiXVMFyQSkkxClErIcomPiczX7E1bfSw22PoeeaEAAAAASUVORK5CYII=';

/** What the tray menu can do. */
export interface TrayActions {
  open: () => void;
  quit: () => void;
}

/** Menu items for the running jobs, in plain words. */
export function jobItems(jobs: readonly RunningJob[]): MenuItemConstructorOptions[] {
  const head: MenuItemConstructorOptions = {
    label:
      jobs.length === 0
        ? 'No jobs running'
        : `${String(jobs.length)} job${jobs.length === 1 ? '' : 's'} running`,
    enabled: false,
  };
  const items = jobs.slice(0, 10).map((j) => ({
    label: `${j.name} — ${j.state.toLowerCase()}`,
    enabled: false,
  }));
  return [head, ...items];
}

/**
 * The tray icon: running jobs, Open and Quit. Closing the window keeps the app here, so the
 * core and training keep going while the window's memory is freed.
 */
export class AppTray {
  private tray: Tray;
  private jobs: readonly RunningJob[] = [];

  constructor(private readonly actions: TrayActions) {
    const image = nativeImage.createFromDataURL(ICON).resize({ width: 16, height: 16 });
    image.setTemplateImage(true);
    this.tray = new Tray(image);
    this.tray.on('click', () => {
      if (process.platform !== 'darwin') actions.open();
    });
    this.render();
  }

  setJobs(jobs: readonly RunningJob[]): void {
    this.jobs = jobs;
    this.render();
  }

  private render(): void {
    this.tray.setToolTip(
      this.jobs.length === 0 ? 'MasterRL' : `MasterRL — ${String(this.jobs.length)} running`,
    );
    this.tray.setContextMenu(
      Menu.buildFromTemplate([
        ...jobItems(this.jobs),
        { type: 'separator' },
        { label: 'Open MasterRL', click: this.actions.open },
        { label: 'Quit MasterRL', click: this.actions.quit },
      ]),
    );
  }

  destroy(): void {
    this.tray.destroy();
  }
}
