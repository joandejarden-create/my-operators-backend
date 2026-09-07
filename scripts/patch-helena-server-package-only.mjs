#!/usr/bin/env node
import fs from 'fs';

let s = fs.readFileSync('server.js', 'utf8');
const importBlock = `import {
  getHelenaCmoBrief,
  getHelenaCmoConsole,
  getHelenaCmoAttentionCount,
  postHelenaCmoDecisionAction,
  postHelenaCmoApprovalAction,
} from "./api/admin-helena-cmo.js";
import { requireHelenaCmoAdminAccess } from "./middleware/requireHelenaCmoAdminAccess.js";
`;

if (!s.includes('getHelenaCmoConsole')) {
  const marker = '} from "./api/ai-demand-positioning.js";';
  const idx = s.indexOf(marker);
  if (idx < 0) {
    console.error('import marker not found');
    process.exit(1);
  }
  const end = idx + marker.length;
  const nl = s.slice(end, end + 2) === '\r\n' ? '\r\n' : '\n';
  s = s.slice(0, end) + nl + importBlock + s.slice(end + nl.length);
}

if (!s.includes('helenaCmoAdminAuth')) {
  const marker = 'const adminAuth = [memberstackAuth, requireDealalityUser, requireAdminAccess];';
  const idx = s.indexOf(marker);
  if (idx < 0) {
    console.error('adminAuth marker not found');
    process.exit(1);
  }
  const end = idx + marker.length;
  const nl = s.slice(end, end + 2) === '\r\n' ? '\r\n' : '\n';
  const routes = `${nl}const helenaCmoAdminAuth = [
  memberstackAuth,
  requireDealalityUser,
  requireHelenaCmoAdminAccess,
];

// Admin — Helena CMO Founder Console (local decision log; EXECUTE/recurring OFF)
app.get("/api/admin/helena-cmo/brief", ...helenaCmoAdminAuth, getHelenaCmoBrief);
app.get("/api/admin/helena-cmo/console", ...helenaCmoAdminAuth, getHelenaCmoConsole);
app.get("/api/admin/helena-cmo/attention-count", ...helenaCmoAdminAuth, getHelenaCmoAttentionCount);
app.post("/api/admin/helena-cmo/decisions/:id/action", ...helenaCmoAdminAuth, postHelenaCmoDecisionAction);
app.post("/api/admin/helena-cmo/approvals/:id/action", ...helenaCmoAdminAuth, postHelenaCmoApprovalAction);${nl}`;
  s = s.slice(0, end) + routes + s.slice(end);
}

if (/adp-leak-audits|getAdminLeakAuditMeta/.test(s)) {
  console.error('REFUSING: server.js contains ADP leak-audit wiring');
  process.exit(1);
}
fs.writeFileSync('server.js', s);

const p = JSON.parse(fs.readFileSync('package.json', 'utf8'));
p.scripts['test:helena-cmo-founder-console-v1'] = 'node scripts/test-helena-cmo-founder-console-v1.mjs';
p.scripts['helena-cmo-baseline-v1:write'] = 'node scripts/write-helena-cmo-baseline-v1.mjs';
p.scripts['playwright:helena-cmo-founder-console-smoke-v1'] =
  'node scripts/playwright-helena-cmo-founder-console-smoke-v1.mjs';
for (const k of Object.keys(p.scripts)) {
  if (k.includes('adp-leak-audit')) delete p.scripts[k];
}
fs.writeFileSync('package.json', JSON.stringify(p, null, 2) + '\n');
console.log(
  JSON.stringify({
    ok: true,
    hasConsole: s.includes('getHelenaCmoConsole'),
    hasAuth: s.includes('helenaCmoAdminAuth'),
    hasLeak: /leak-audit/.test(s),
  }),
);
