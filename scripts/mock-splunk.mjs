import { createServer } from 'node:http';

const hostname = process.env.MOCK_SPLUNK_HOST ?? '127.0.0.1';
const port = Number(process.env.MOCK_SPLUNK_PORT ?? 18089);
const jobs = new Map();
let nextJob = 0;

const json = (response, status, body) => {
  response.writeHead(status, { 'Content-Type': 'application/json' });
  response.end(JSON.stringify(body));
};

const server = createServer(async (request, response) => {
  const url = new URL(request.url ?? '/', 'http://mock-splunk');
  if (request.method === 'GET' && url.pathname === '/__health') {
    json(response, 200, { status: 'ok' });
    return;
  }

  if (request.headers.authorization !== 'Bearer grafana-e2e-token') {
    json(response, 401, { messages: [{ type: 'ERROR', text: 'Invalid mock Splunk credentials' }] });
    return;
  }

  if (request.method === 'POST' && url.pathname === '/services/search/jobs') {
    const chunks = [];
    for await (const chunk of request) {
      chunks.push(chunk);
    }
    const form = new URLSearchParams(Buffer.concat(chunks).toString());
    const search = form.get('search') ?? '';
    if (form.get('exec_mode') === 'oneshot') {
      json(response, 200, { fields: [{ name: 'count' }], results: [{ count: '1' }] });
      return;
    }
    if (!search.includes('e2e_metrics') && !search.includes('e2e_variables')) {
      json(response, 400, { messages: [{ type: 'ERROR', text: 'Unknown mock search' }] });
      return;
    }
    const sid = `e2e-${++nextJob}`;
    jobs.set(sid, search);
    if (jobs.size > 1000) {
      jobs.delete(jobs.keys().next().value);
    }
    json(response, 201, { sid });
    return;
  }

  const jobPath = url.pathname.match(/^\/services\/search\/jobs\/(e2e-\d+)(?:\/(results|control))?$/);
  if (!jobPath || !jobs.has(jobPath[1])) {
    json(response, 404, { messages: [{ type: 'ERROR', text: 'Unknown mock search job' }] });
    return;
  }

  if (request.method === 'POST' && jobPath[2] === 'control') {
    jobs.delete(jobPath[1]);
    json(response, 200, {});
    return;
  }
  if (request.method !== 'GET') {
    json(response, 405, { messages: [{ type: 'ERROR', text: 'Unsupported method' }] });
    return;
  }
  if (jobPath[2] !== 'results') {
    json(response, 200, { entry: [{ content: { dispatchState: 'DONE' } }] });
    return;
  }

  const variableSearch = jobs.get(jobPath[1]).includes('e2e_variables');
  const fields = variableSearch ? ['__text', '__value'] : ['_time', 'host', 'count'];
  const now = String(Date.now() / 1000);
  const rows = variableSearch
    ? [
        { __text: 'API server 1', __value: 'api-1' },
        { __text: 'API server 2', __value: 'api-2' },
      ]
    : [
        { _time: now, host: 'api-1', count: '42' },
        { _time: now, host: 'api-2', count: '7' },
      ];
  const offset = Number(url.searchParams.get('offset') ?? 0);
  const count = Number(url.searchParams.get('count') ?? rows.length);
  json(response, 200, { fields: fields.map((name) => ({ name })), results: rows.slice(offset, offset + count) });
});

server.listen(port, hostname, () => {
  console.log(`Mock Splunk listening on http://${hostname}:${port}`);
});

for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => server.close(() => process.exit(0)));
}
