import fs from 'fs';
import crypto from 'crypto';

const args = JSON.parse(
  fs.readFileSync(new URL('./dmp-set-embed-shell-c.json', import.meta.url), 'utf8')
);
const code = args.actions[0].set_settings.operations[0].settings[0].static_text.value;
const report = {
  siteId: args.siteId,
  pageId: args.pageId,
  codeLen: code.length,
  codeSha16: crypto.createHash('sha256').update(code).digest('hex').slice(0, 16),
  hasShellC: code.includes('shell.v20260731c.css'),
  hasVersion: code.includes('data-dmp-version="1.1.33"'),
  starts: code.slice(0, 90),
};
console.log(JSON.stringify(report, null, 2));
fs.writeFileSync(
  new URL('./dmp-set-embed-shell-c.ready.json', import.meta.url),
  JSON.stringify(args)
);
