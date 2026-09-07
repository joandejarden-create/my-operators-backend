#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
const re = /"(rec(?!urring)[A-Za-z0-9]{14})"/g;
function walk(d, a = []) {
  if (!fs.existsSync(d)) return a;
  for (const e of fs.readdirSync(d, { withFileTypes: true })) {
    const p = path.join(d, e.name);
    if (e.isDirectory()) walk(p, a);
    else if (/\.(json|md)$/.test(e.name)) a.push(p);
  }
  return a;
}
const hits = [];
for (const root of [
  'reports/helena-cmo-manual-week-01',
  'reports/helena-cmo-founder-decisions',
  'reports/helena-cmo-baseline-v1',
  'reports/helena-cmo-operating-law-v1',
  'reports/helena-cmo-baseline-validation-v1',
]) {
  for (const f of walk(root)) {
    const m = [...fs.readFileSync(f, 'utf8').matchAll(re)].map((x) => x[1]);
    if (m.length) hits.push({ f, m });
  }
}
console.log(JSON.stringify({ remaining: hits.length, hits }, null, 2));
