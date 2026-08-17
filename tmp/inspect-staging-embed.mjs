import fs from 'fs';
import https from 'https';
import crypto from 'crypto';

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        let d = '';
        res.on('data', (c) => (d += c));
        res.on('end', () => resolve(d));
      })
      .on('error', reject);
  });
}

const html = await get('https://mvp-deal-capture.webflow.io/old-home');
const shell = html.match(/old-home-manual-process\.shell\.v[0-9a-z]+\.css/);
const version = html.match(/data-dmp-version="([^"]+)"/);
const css = html.match(/old-home-manual-process\.v[0-9a-z]+\.css/);
const report = {
  shell: shell?.[0] ?? null,
  version: version?.[1] ?? null,
  css: css?.[0] ?? null,
  hasPlaceholder: html.includes('FILE_PLACEHOLDER'),
  hasPLACEHOLDER: html.includes('PLACEHOLDER_WILL_REPLACE'),
  hasDealalityManual: html.includes('dealality-manual-process'),
  len: html.length,
};
console.log(JSON.stringify(report, null, 2));

const args = JSON.parse(fs.readFileSync('tmp/dmp-set-embed-shell-c.json', 'utf8'));
const code = args.actions[0].set_settings.operations[0].settings[0].static_text.value;
console.log(
  JSON.stringify({
    localCodeLen: code.length,
    localSha16: crypto.createHash('sha256').update(code).digest('hex').slice(0, 16),
    localShellC: code.includes('shell.v20260731c.css'),
  })
);
