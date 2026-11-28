// Test helper: the desktop core's endpoints in MSW, with state in memory. Imported by tests only.
import { http, HttpResponse, type JsonBodyType } from 'msw';
import { API_URL } from '../../lib/api';
import type { Schemas } from './session';

const url = (path: string) => `${API_URL}/api/v1${path}`;

const problem = (status: number, code: string, detail: string) =>
  HttpResponse.json(
    { type: 'about:blank', title: detail, status, code, detail },
    { status, headers: { 'Content-Type': 'application/problem+json' } },
  );

/** What the fake core holds; tests change it before or during a test. */
export interface FakeCoreState {
  step: string;
  signIn: Schemas['SignInState'];
  syncSetUp: boolean;
  copies: Schemas['CopyInfo'][];
  teams: Schemas['TeamMembership'][];
  backups: Schemas['DesktopBackup'][];
  tokens: Schemas['TokenInfo'][];
  /** Every call, as `METHOD /path` with its JSON body. */
  calls: { call: string; body: unknown; authorization: string | null }[];
}

const doctor: Schemas['DesktopDoctorReport'] = {
  platform: 'macos-apple-silicon',
  fits: 'Models up to about 8B parameters fit here.',
  checks: [
    { check: 'disk_space', outcome: 'warn', summary: 'Only 20 GB free.', code: 'MRL-ENV-104' },
    { check: 'python', outcome: 'ok', summary: 'Python 3.12 works.' },
  ],
  can: [
    {
      task: 'sft-lora',
      name: 'Fine-tune small models',
      download_first_gb_low: 1,
      download_first_gb_high: 2,
    },
  ],
  cannot: [{ task: 'grpo', name: 'Reinforcement learning', blocked_by: 'needs an NVIDIA GPU' }],
  to_install: [
    {
      id: 'pytorch',
      name: 'PyTorch',
      summary: 'Runs training.',
      download_gb_low: 0.8,
      download_gb_high: 1.2,
    },
  ],
};

/** MSW handlers for the desktop, auth and sync endpoints the P30 screens call. */
export function fakeCore(initial: Partial<FakeCoreState> = {}) {
  const s: FakeCoreState = {
    step: 'welcome',
    signIn: 'signed_out',
    syncSetUp: false,
    copies: [],
    teams: [],
    backups: [],
    tokens: [],
    calls: [],
    ...initial,
  };
  const note = async (request: Request, call: string) => {
    let body: unknown;
    try {
      body = await request.clone().json();
    } catch {
      body = undefined;
    }
    s.calls.push({ call, body, authorization: request.headers.get('Authorization') });
    return body as Record<string, unknown> | undefined;
  };
  const json = (body: JsonBodyType, status = 200) => HttpResponse.json(body, { status });

  const handlers = [
    http.get(url('/desktop/state'), async ({ request }) => {
      await note(request, 'GET /desktop/state');
      return json({
        onboarding: {
          step: s.step,
          completed_at: s.step === 'done' ? '2026-11-28T10:00:00Z' : null,
        },
        data_dir: '/Users/someone/Library/Application Support/MasterRL/postgres',
        bundled_database: true,
        version: '0.1.0',
      });
    }),
    http.put(url('/desktop/onboarding'), async ({ request }) => {
      const body = await note(request, 'PUT /desktop/onboarding');
      s.step = String(body?.step);
      return json({
        step: s.step,
        completed_at: s.step === 'done' ? '2026-11-28T10:00:00Z' : null,
      });
    }),
    http.post(url('/desktop/doctor'), async ({ request }) => {
      await note(request, 'POST /desktop/doctor');
      return json(doctor);
    }),
    http.get(url('/desktop/backups'), async ({ request }) => {
      await note(request, 'GET /desktop/backups');
      return json({ available: true, folder: '/backups', items: s.backups });
    }),
    http.post(url('/desktop/backups'), async ({ request }) => {
      await note(request, 'POST /desktop/backups');
      const made = {
        name: 'masterrl-20261128T110000000Z-manual.dump',
        reason: 'manual',
        size_bytes: 2048,
        created_at: '2026-11-28T11:00:00Z',
      };
      s.backups = [made, ...s.backups];
      return json(made, 201);
    }),
    http.post(url('/desktop/backups/:name/restore'), async ({ request, params }) => {
      await note(request, `POST /desktop/backups/${String(params.name)}/restore`);
      return json({
        restored: String(params.name),
        safety_backup: 'masterrl-20261128T110500000Z-before-restore.dump',
        restart_required: true,
      });
    }),
    http.get(url('/auth/me'), async ({ request }) => {
      await note(request, 'GET /auth/me');
      return json({
        principal_kind: 'local_owner',
        roles: [],
        sign_in: s.signIn,
        user:
          s.signIn === 'active'
            ? { id: 'usr_1', display_name: 'Sam Example', email: 'sam@example.com' }
            : null,
      });
    }),
    http.post(url('/auth/logout'), async ({ request }) => {
      await note(request, 'POST /auth/logout');
      s.signIn = 'signed_out';
      return new HttpResponse(null, { status: 204 });
    }),
    http.get(url('/auth/tokens'), async ({ request }) => {
      await note(request, 'GET /auth/tokens');
      return json({ items: s.tokens, next_cursor: null });
    }),
    http.post(url('/auth/tokens'), async ({ request }) => {
      const body = await note(request, 'POST /auth/tokens');
      const name = typeof body?.name === 'string' ? body.name : '';
      const id = `tok_${String(s.tokens.length + 1)}`;
      s.tokens = [
        { id, name, prefix: 'mrl_pat_AbCd', scopes: ['*'], expires_at: '2027-02-26T00:00:00Z' },
        ...s.tokens,
      ];
      return json(
        {
          id,
          token: 'mrl_pat_AbCdEfGhIjKlMnOp',
          scopes: ['*'],
          expires_at: '2027-02-26T00:00:00Z',
        },
        201,
      );
    }),
    http.delete(url('/auth/tokens/:id'), async ({ request, params }) => {
      await note(request, `DELETE /auth/tokens/${String(params.id)}`);
      s.tokens = s.tokens.filter((t) => t.id !== params.id);
      return new HttpResponse(null, { status: 204 });
    }),
    http.get(url('/sync/status'), async ({ request }) => {
      await note(request, 'GET /sync/status');
      return json({
        set_up: s.syncSetUp,
        sign_in: s.signIn,
        copies: s.signIn === 'active' ? s.copies : null,
        copies_unavailable: s.signIn === 'active' ? null : 'Sign in to see saved copies.',
        last_push_at: s.copies[0]?.at ?? null,
      });
    }),
    http.post(url('/sync/setup'), async ({ request }) => {
      await note(request, 'POST /sync/setup');
      s.syncSetUp = true;
      return json({ recovery_key: 'RK-1111-2222-3333-4444' });
    }),
    http.post(url('/sync/push'), async ({ request }) => {
      await note(request, 'POST /sync/push');
      const version = (s.copies[0]?.version ?? 0) + 1;
      s.copies = [{ version, at: '2026-11-28T12:00:00Z' }, ...s.copies];
      return json({ version, at: '2026-11-28T12:00:00Z', copies: s.copies.length });
    }),
    http.post(url('/sync/restore'), async ({ request }) => {
      const body = await note(request, 'POST /sync/restore');
      if (body?.passphrase !== 'correct horse battery' && !body?.recovery_key) {
        return problem(422, 'MRL-VAL-001', 'That passphrase does not open this setup.');
      }
      return json({
        version: s.copies[0]?.version ?? 1,
        at: '2026-11-27T09:00:00Z',
        profile: {
          schema: 'setup-profile/v1',
          machines: [{ name: 'gpu-box', address: 'gpu-box.local', transport: { kind: 'direct' } }],
          team_memberships: s.teams,
        },
      });
    }),
    http.get(url('/sync/profile'), async ({ request }) => {
      await note(request, 'GET /sync/profile');
      return json({ schema: 'setup-profile/v1', team_memberships: s.teams });
    }),
  ];
  return { state: s, handlers, problem };
}
