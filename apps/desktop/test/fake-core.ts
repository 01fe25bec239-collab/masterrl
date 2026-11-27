// A stand-in for `mrl-server --mode solo` in unit tests. Behaviour from FAKE_CORE_MODE:
//   ok            listen, answer /healthz and echo /api/* (needs the launch secret)
//   exit-now      print an error code on stderr and exit 1 before listening
//   crash-once    like ok, but the first start (no FAKE_CORE_LOG entry yet) exits 3 after 300 ms
//   hang          like ok, but /healthz stops answering after the first 2 checks
//   never-listen  stay up without printing the listening line
//   slow-stop     ignore the first SIGTERM
// Every start and clean stop is appended to FAKE_CORE_LOG, one word per line.
import { appendFileSync, existsSync, readFileSync } from 'node:fs';
import { createServer } from 'node:http';

const mode = process.env.FAKE_CORE_MODE ?? 'ok';
const log = process.env.FAKE_CORE_LOG;
const secret = process.env.MRL_LAUNCH_SECRET ?? '';
const note = (word: string) => {
  if (log) appendFileSync(log, `${word}\n`);
};
const starts = log && existsSync(log) ? readFileSync(log, 'utf8').split('start').length - 1 : 0;
note('start');

if (mode === 'exit-now') {
  process.stderr.write('mrl-server: MRL-ENV-110 no PostgreSQL programs found\n');
  process.exit(1);
}

let checks = 0;
const server = createServer((req, res) => {
  if (req.headers['x-mrl-launch-secret'] !== secret) {
    res.writeHead(401).end();
    return;
  }
  if (req.url === '/healthz') {
    checks += 1;
    if (mode === 'hang' && checks > 2) return; // never answers
    res.writeHead(200, { 'content-type': 'text/plain' }).end('ok');
    return;
  }
  let body = '';
  req.on('data', (c: Buffer) => (body += c.toString()));
  req.on('end', () => {
    res.writeHead(200, { 'content-type': 'application/json', 'x-fake': '1' }).end(
      JSON.stringify({
        method: req.method,
        url: req.url,
        body,
        authorization: req.headers.authorization ?? null,
        cookie: req.headers.cookie ?? null,
      }),
    );
  });
});

server.listen(0, '127.0.0.1', () => {
  const address = server.address();
  if (mode === 'never-listen' || address === null || typeof address === 'string') return;
  process.stdout.write('starting up\n');
  process.stdout.write(`${JSON.stringify({ event: 'listening', port: address.port })}\n`);
  if (mode === 'crash-once' && starts === 0) setTimeout(() => process.exit(3), 300);
});

let terms = 0;
process.on('SIGTERM', () => {
  terms += 1;
  if (mode === 'slow-stop' && terms === 1) return;
  note('stopped');
  server.close(() => process.exit(0));
});
