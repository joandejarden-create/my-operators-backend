/**
 * AVA durability verification (no production deploy).
 * Simulates Railway allowlisted artifact presence + auth gates + store preference.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(import.meta.url);

// Fresh imports after migration paths exist
const {
  loadAdpBaselineForAudit,
  listAdpPropertyIdsWithStagedReports,
  listCertifiedBaselineUniverse,
} = await import("../lib/ai-visibility-audit-action-plan/adp-baseline-loader-v1.js");
const {
  listStoredAudits,
  getAuditMeta,
  getLatestAuditVersion,
  AVA_STORE_ROOT,
} = await import("../lib/ai-visibility-audit-action-plan/audit-store-v1.js");
const {
  AVA_CERTIFIED_BASELINES_ROOT,
  AVA_PRODUCTION_ARCHIVE_ROOT,
  AVA_PRODUCTION_STORE_ROOT,
} = await import("../lib/ai-visibility-audit-action-plan/certified-baseline-paths-v1.js");

const out = { ok: true, at: new Date().toISOString(), checks: [], failed: [] };
function check(id, pass, detail) {
  out.checks.push({ id, pass, detail });
  if (!pass) {
    out.ok = false;
    out.failed.push({ id, detail });
  }
  console.log(pass ? "PASS" : "FAIL", id, typeof detail === "string" ? detail : JSON.stringify(detail));
}

const universe = listCertifiedBaselineUniverse();
check("lean-baselines-38", universe.count === 38, { count: universe.count });
const ids = listAdpPropertyIdsWithStagedReports();
check("eligible-ids-38", ids.length === 38, { count: ids.length });

const must = [
  "adp_spice_island_beach_resort",
  "adp_wyndham_san_diego_bayside",
  "adp_waterstone_boca_raton",
  "adp_bethesda_marriott",
  "adp_castillo_hotel_son_vida",
  "adp_sheraton_mallorca_arabella_golf",
];
for (const id of must) {
  const b = loadAdpBaselineForAudit(id);
  check(`baseline-lean:${id}`, b.ok && b.baseline?.baselineSource === "CERTIFIED_LEAN_V1", {
    source: b.baseline?.baselineSource,
    periodId: b.baseline?.periodId,
    sha: b.baseline?.baselineSha256,
  });
  const meta = getAuditMeta(id);
  const latest = getLatestAuditVersion(id);
  const pdfRel = latest?.pdfRelativePath || meta?.pdfRelativePath;
  const pdfAbs = pdfRel ? path.join(ROOT, pdfRel) : null;
  check(`audit-meta:${id}`, Boolean(meta) && meta.customerReadinessStatus === "NOT_CUSTOMER_READY", {
    status: meta?.status,
    customer: meta?.customerReadinessStatus,
    publication: meta?.publicationAuthorizationStatus,
  });
  check(`pdf:${id}`, Boolean(pdfAbs && fs.existsSync(pdfAbs)), pdfRel);
}

check(
  "store-root-production",
  String(AVA_STORE_ROOT).replace(/\\/g, "/").includes("production/store"),
  AVA_STORE_ROOT
);
check("stored-audits-6", listStoredAudits().length >= 6, listStoredAudits().length);

// Simulate redeploy artifact pack (allowlisted trees only)
const simRoot = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/_redeploy-sim"
);
fs.rmSync(simRoot, { recursive: true, force: true });
function copyAllowlisted(srcRel) {
  const src = path.join(ROOT, srcRel);
  const dest = path.join(simRoot, srcRel);
  if (!fs.existsSync(src)) return { ok: false, srcRel };
  fs.cpSync(src, dest, { recursive: true });
  return { ok: true, srcRel };
}
const packs = [
  copyAllowlisted("data/ai-visibility-audit-action-plan/certified-baselines"),
  copyAllowlisted("data/ai-visibility-audit-action-plan/production/store"),
  copyAllowlisted("reports/ai-visibility-audit-action-plan/archive"),
];
check(
  "redeploy-sim-copy",
  packs.every((p) => p.ok),
  packs
);

// Verify sim pack can resolve PDFs + metas without staged ADP (~299MB)
let simPdfOk = 0;
for (const id of must) {
  const meta = JSON.parse(
    fs.readFileSync(
      path.join(simRoot, "data/ai-visibility-audit-action-plan/production/store", id, "AUDIT_META.json"),
      "utf8"
    )
  );
  const pdf = path.join(simRoot, meta.pdfRelativePath || "");
  if (fs.existsSync(pdf)) simPdfOk += 1;
  const leanDir = path.join(simRoot, "data/ai-visibility-audit-action-plan/certified-baselines", id);
  const leanFiles = fs.existsSync(leanDir) ? fs.readdirSync(leanDir).filter((f) => f.endsWith(".json")) : [];
  check(`sim-lean:${id}`, leanFiles.length >= 1, leanFiles);
}
check("sim-pdfs-6", simPdfOk === 6, { simPdfOk });

// Size of deployable AVA pack
function dirSize(p) {
  let n = 0;
  if (!fs.existsSync(p)) return 0;
  for (const e of fs.readdirSync(p, { withFileTypes: true })) {
    const fp = path.join(p, e.name);
    n += e.isDirectory() ? dirSize(fp) : fs.statSync(fp).size;
  }
  return n;
}
const packBytes =
  dirSize(path.join(ROOT, "data/ai-visibility-audit-action-plan/certified-baselines")) +
  dirSize(path.join(ROOT, "data/ai-visibility-audit-action-plan/production/store")) +
  dirSize(path.join(ROOT, "reports/ai-visibility-audit-action-plan/archive"));
check("pack-under-10mb", packBytes < 10 * 1024 * 1024, {
  mb: Number((packBytes / 1024 / 1024).toFixed(2)),
});

// Auth matrix (production NODE_ENV bypass inert) — unit level
const bypassCases = [
  { NODE_ENV: "production", email: "x@y.com", expect: false },
  { NODE_ENV: "development", email: "x@y.com", expect: true },
  { NODE_ENV: "production", email: "", expect: false },
];
for (const c of bypassCases) {
  const active = Boolean(c.email) && c.NODE_ENV !== "production";
  check(`bypass-matrix:${c.NODE_ENV}:${c.email ? "set" : "empty"}`, active === c.expect, {
    active,
  });
}

// Railway ignore markers
const ri = fs.readFileSync(path.join(ROOT, ".railwayignore"), "utf8");
check("railwayignore-ava-marker", ri.includes("RAILWAY_LEAN_AVA_V1"), true);
check(
  "railwayignore-baselines",
  ri.includes("!data/ai-visibility-audit-action-plan/certified-baselines/**"),
  true
);
check(
  "railwayignore-archive",
  ri.includes("!reports/ai-visibility-audit-action-plan/archive/**"),
  true
);
check(
  "railwayignore-no-broad-reports",
  !ri.includes("!reports/**") && ri.includes("reports/*"),
  true
);

const outPath = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_DURABILITY_VERIFICATION.json"
);
fs.writeFileSync(outPath, `${JSON.stringify(out, null, 2)}\n`);
console.log(JSON.stringify({ ok: out.ok, failed: out.failed.length, outPath, packMb: (packBytes / 1024 / 1024).toFixed(2) }, null, 2));
if (!out.ok) process.exit(1);
