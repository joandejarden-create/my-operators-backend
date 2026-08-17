import fs from 'fs';

const src =
  'C:/Users/joand/.cursor/browser-logs/cdp-response-Page.captureScreenshot-2026-07-30T22-21-17-447Z.json';
const raw = JSON.parse(fs.readFileSync(src, 'utf8'));
const b64 = raw.data || raw.result?.data;
if (!b64) {
  console.error('no data', Object.keys(raw));
  process.exit(1);
}
fs.mkdirSync('docs/old-home-problem-deal-desk-snapshots', { recursive: true });
fs.writeFileSync(
  'docs/old-home-problem-deal-desk-snapshots/mobile.png',
  Buffer.from(b64, 'base64')
);
console.log('wrote mobile', Buffer.from(b64, 'base64').length);
