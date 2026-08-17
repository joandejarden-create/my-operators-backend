import fs from 'fs';
import { spawnSync } from 'child_process';

const content = fs.readFileSync(
  'c:/Users/joand/OneDrive/Documents/deal-capture-proxy/tmp-old-home-head-patched.txt',
  'utf8'
);

// Write a tiny JSON payload file the agent can reference; also verify
const payload = {
  page_id: '68108c2a063eeb5d1bd7ae90',
  location: 'head',
  content,
  marker: content.includes('DashDark-style 4-col footer'),
  len: content.length,
};

fs.writeFileSync(
  'c:/Users/joand/OneDrive/Documents/deal-capture-proxy/tmp-set-payload.json',
  JSON.stringify(payload)
);

console.log(JSON.stringify({ len: payload.len, marker: payload.marker }));
