/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Core address; defaults to http://127.0.0.1:7801. */
  readonly VITE_MRL_API_URL?: string;
  /** Dev-only token. The desktop app passes the per-launch token instead (P30). */
  readonly VITE_MRL_API_TOKEN?: string;
  /** `off` turns the dev mock API off. */
  readonly VITE_MRL_MOCKS?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
