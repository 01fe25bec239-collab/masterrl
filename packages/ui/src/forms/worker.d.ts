// Vite's `?worker` imports (used to load Monaco's editor worker).
declare module '*?worker' {
  const WorkerFactory: new () => Worker;
  export default WorkerFactory;
}
