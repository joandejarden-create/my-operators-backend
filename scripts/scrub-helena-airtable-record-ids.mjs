#!/usr/bin/env node
import fs from 'fs';
import path from 'path';

const roots = [
  'reports/helena-cmo-manual-week-01',
  'reports/helena-cmo-founder-decisions',
  'reports/helena-cmo-baseline-v1',
  'reports/helena-cmo-operating-law-v1',
  'reports/helena-cmo-baseline-validation-v1',
];

function walk(dir, out = []) {
  if (!fs.existsSync(dir)) return out;
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) walk(p, out);
    else if (/\.(json|md)$/.test(ent.name)) out.push(p);
  }
  return out;
}

const idRe = /"(rec(?!urring)[A-Za-z0-9]{14})"/g;
let changed = 0;
for (const root of roots) {
  for (const file of walk(root)) {
    let s = fs.readFileSync(file, 'utf8');
    const before = s;
    s = s.replace(idRe, '"[AIRTABLE_RECORD_OMITTED]"');
    s = s.replace(/\n\s*"airtableRecordId"\s*:\s*"[^"]*"\s*,?/g, '\n');
    s = s.replace(/record\s+rec(?!urring)[A-Za-z0-9]{14}/g, 'Airtable record omitted from repo');
    s = s.replace(/·\s*rec(?!urring)[A-Za-z0-9]{14}/g, '· Airtable record omitted from repo');
    s = s.replace(/(UPDATED|CREATED|AMENDED[^\n]*)\s*·\s*rec(?!urring)[A-Za-z0-9]{14}/g, '$1 · Airtable record omitted from repo');
    if (s !== before) {
      fs.writeFileSync(file, s);
      changed += 1;
      console.log('cleaned', file);
    }
  }
}
console.log(JSON.stringify({ ok: true, changed }));
