import fs from 'fs';
import { createRequire } from 'module';

const args = JSON.parse(fs.readFileSync('tmp-callmcp-args.json', 'utf8'));
fs.writeFileSync('tmp-mcp-args-export.cjs', 'module.exports = ' + JSON.stringify(args) + ';\n');

const require = createRequire(import.meta.url);
const m = require('./tmp-mcp-args-export.cjs');
const c = m.actions[0].set_page_freeform_code.content;
console.log(
  [c.length, c.includes('DashDark-style 4-col footer'), c.startsWith('<link rel="preconnect"')].join('|')
);
