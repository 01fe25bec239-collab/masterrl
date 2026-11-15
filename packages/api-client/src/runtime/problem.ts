import { z } from 'zod';

/** RFC 9457 problem details as the core sends them (Doc 09 §6.5 `Problem`). */
export const problemSchema = z.object({
  type: z.string(),
  title: z.string(),
  status: z.number().int(),
  detail: z.string().optional(),
  instance: z.string().optional(),
  code: z.string().optional(),
  trace_id: z.string().optional(),
  errors: z
    .array(
      z.object({
        path: z.string(),
        code: z.string(),
        message: z.string(),
        hint: z.string().optional(),
      }),
    )
    .optional(),
});

export type Problem = z.infer<typeof problemSchema>;

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

/** A failed call. `code` (e.g. `MRL-CAP-012`) is the guidance catalog key for PlainError. */
export class ProblemError extends ApiError {
  readonly problem: Problem;

  constructor(problem: Problem) {
    super(problem.status, problem.detail ?? problem.title);
    // `name` stays "ApiError": callers that only know ApiError keep working.
    this.problem = problem;
  }

  get code(): string | undefined {
    return this.problem.code;
  }

  get traceId(): string | undefined {
    return this.problem.trace_id;
  }

  /** Field errors keyed by JSON pointer, for SchemaForm's `externalErrors`. */
  fieldErrors(): Record<string, string> {
    const out: Record<string, string> = {};
    for (const e of this.problem.errors ?? [])
      out[e.path.startsWith('/') ? e.path : `/${e.path.replace(/\./g, '/')}`] = e.message;
    return out;
  }
}

export const UNREACHABLE_DETAIL = 'The app could not reach its core service on this computer.';

/** Turn a non-2xx response into a ProblemError, even when the body is not problem+json. */
export async function toProblemError(response: Response): Promise<ProblemError> {
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    body = undefined;
  }
  const parsed = problemSchema.safeParse(body);
  if (parsed.success) return new ProblemError(parsed.data);
  return new ProblemError({
    type: 'about:blank',
    title: response.statusText || `The core API answered ${response.status}.`,
    status: response.status,
  });
}

/** For PlainError: `{ code, detail, traceId }` from anything thrown by the client. */
export function describeError(err: unknown): { code?: string; detail?: string; traceId?: string } {
  if (err instanceof ProblemError)
    return { code: err.code, detail: err.problem.detail, traceId: err.traceId };
  if (err instanceof ApiError) return { detail: err.message };
  // fetch() rejects with TypeError when nothing answers. The catalog has no code for this yet.
  if (err instanceof TypeError) return { detail: UNREACHABLE_DETAIL };
  return {};
}
