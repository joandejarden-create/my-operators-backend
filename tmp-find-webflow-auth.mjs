import fs from 'fs';
import path from 'path';
import os from 'os';

const home = os.homedir();
const out = [];

function walk(d, n = 0) {
  if (n > 5 || !fs.existsSync(d)) return;
  let entries;
  try {
    entries = fs.readdirSync(d, { withFileTypes: true });
  } catch {
    return;
  }
  for (const e of entries) {
    const p = path.join(d, e.name);
    if (e.isDirectory()) {
      if (/node_modules|\.git/i.test(e.name)) continue;
      walk(p, n + 1);
    } else if (/webflow|token|auth|mcp/i.test(e.name)) {
      out.push(p);
    }
  }
}

walk(path.join(home, '.cursor'));
console.log(out.slice(0, 80).join('\n'));
