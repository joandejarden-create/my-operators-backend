import fs from 'fs';
import crypto from 'crypto';

const args = JSON.parse(fs.readFileSync('tmp/dmp-set-embed-f15.json', 'utf8'));
const code = args.actions[0].set_settings.operations[0].settings[0].static_text.value;
const invoke = {
  server: 'user-webflow',
  toolName: 'data_element_settings_tool',
  arguments: args,
};
fs.writeFileSync('tmp/dmp-f15-callmcp-invoke.json', JSON.stringify(invoke));
console.log(
  JSON.stringify({
    invokeBytes: fs.statSync('tmp/dmp-f15-callmcp-invoke.json').size,
    codeSha: crypto.createHash('sha256').update(code).digest('hex').slice(0, 16),
    hasF15: code.includes('v20260801f15.css'),
    hasVersion: code.includes('1.1.37'),
  })
);
