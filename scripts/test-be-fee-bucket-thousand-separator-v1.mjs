#!/usr/bin/env node
/**
 * Regression: fee-bucket bodies must keep thousand separators intact.
 * Founder defect: Application $85,000 → "Application $85" + orphan "000".
 */
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const atelierPath = path.join(ROOT, "public/js/brand-explorer-atelier-from-api.js");
const src = fs.readFileSync(atelierPath, "utf8");

const start = src.indexOf("function econParseFeeBucketBullets(body)");
const end = src.indexOf("\n  var ECON_FEE_BUCKET_THEME_BY_KEY", start);
if (start < 0 || end < 0) {
  console.error("econParseFeeBucketBullets not found");
  process.exit(2);
}
const fnSrc = src.slice(start, end);

const ctx = {};
vm.createContext(ctx);
vm.runInContext(
  `
  function hasVal(v) { return v != null && String(v).trim() !== ""; }
  function econFeeBucketBulletCase(s) { return String(s || "").trim(); }
  ${fnSrc}
  this.parse = econParseFeeBucketBullets;
  `,
  ctx
);

const JOIN_BODY =
  "Application $85,000 new-build/conversion (+ $400/key over 250); Change of ownership $150,000; PIP fee $10,000; Opening process services $20,000; OnQ hardware/software/install $82,800–$218,800; Required training $5,000–$15,000; Design, market study, and F&B concept costs vary by conversion vs new-build";

const OPERATE_BODY =
  "Steady-state 5% monthly royalty on gross rooms revenue (GRR); 4% monthly program fee on GRR; 4% Hilton Honors on eligible guest folio (waived for on-property enrollment stays); Hilton Advance 1.35% digital direct (max $30/stay); OnQ maintenance $2,295–$6,689/month; Eforea spa royalty 2% gross spa revenue (if spa amendment); Model net contribution after loyalty chargebacks and distribution—not headline RevPAR";

const join = ctx.parse(JOIN_BODY);
const operate = ctx.parse(OPERATE_BODY);
const orphanFrags = [...join, ...operate].filter((b) =>
  /^(000|800|295|689(?:\/[Mm]onth)?)$/.test(String(b).trim())
);

const checks = {
  joinCount: join.length,
  operateCount: operate.length,
  joinHas85000: join.some((b) => /\$85,000/.test(b)),
  joinHas150000: join.some((b) => /\$150,000/.test(b)),
  joinHasOnqRange: join.some((b) => /\$82,800/.test(b) && /\$218,800/.test(b)),
  joinKeepsDesignClause: join.some((b) => /Design,\s*market study,\s*and\s*F&B/i.test(b)),
  operateHasOnqMaint: operate.some((b) => /\$2,295/.test(b) && /\$6,689/.test(b)),
  noOrphanThousandFragments: orphanFrags.length === 0,
  orphanFrags,
  join,
  operate,
};

const ok =
  checks.joinCount === 7 &&
  checks.operateCount === 7 &&
  checks.joinHas85000 &&
  checks.joinHas150000 &&
  checks.joinHasOnqRange &&
  checks.joinKeepsDesignClause &&
  checks.operateHasOnqMaint &&
  checks.noOrphanThousandFragments;

console.log(JSON.stringify({ ok, checks }, null, 2));
process.exit(ok ? 0 : 1);
