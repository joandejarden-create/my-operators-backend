import fs from 'fs';
import crypto from 'crypto';

const args = JSON.parse(fs.readFileSync('tmp-callmcp-args.json', 'utf8'));
const content = args.actions[0].set_page_freeform_code.content;

// Write content as raw for MCP - also write a one-line JSON args file
args.context =
  'Restores Old Home page HEAD freeform custom code with DashDark footer CSS appended before style close.';
args.actions[0].label = 'set_old_home_head';

fs.writeFileSync('tmp-callmcp-args.json', JSON.stringify(args));

const digest = crypto.createHash('sha256').update(content).digest('hex');
console.log(
  JSON.stringify({
    length: content.length,
    digest,
    starts: content.startsWith('<link rel="preconnect"'),
    marker: content.includes('DashDark-style 4-col footer'),
    argsBytes: fs.statSync('tmp-callmcp-args.json').size,
  })
);
