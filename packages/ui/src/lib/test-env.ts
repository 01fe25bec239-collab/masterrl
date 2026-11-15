/** Browser APIs jsdom lacks, for packages/ui and apps/web tests. Runs before test modules load. */

// jsdom has no canvas; say so quietly instead of logging "not implemented" for every chart.
if (typeof HTMLCanvasElement !== 'undefined') {
  HTMLCanvasElement.prototype.getContext = () => null;
}

// jsdom has no matchMedia (uPlot reads it on import).
if (typeof window !== 'undefined' && typeof window.matchMedia !== 'function') {
  window.matchMedia = (query: string) =>
    ({
      matches: false,
      media: query,
      onchange: null,
      addEventListener: () => undefined,
      removeEventListener: () => undefined,
      dispatchEvent: () => false,
    }) as unknown as MediaQueryList;
}
