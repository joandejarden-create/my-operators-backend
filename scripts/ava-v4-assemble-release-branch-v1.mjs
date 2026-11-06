/**
 * Assemble an AVA-only release branch/worktree from origin/main.
 * Preserves the dirty working tree on the original branch.
 *
 *   node scripts/ava-v4-assemble-release-branch-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const SRC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const WT = path.resolve(SRC, "..", "my-operators-backend-ava-v4-release");
const BRANCH = "release/ava-v4-final-customer-quality-v1";

function sh(cmd, cwd = SRC) {
  console.log(`$ ${cmd}`);
  return execSync(cmd, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
}

function copyRecursive(from, to, { excludeDirNames = [] } = {}) {
  if (!fs.existsSync(from)) return 0;
  fs.mkdirSync(to, { recursive: true });
  let n = 0;
  for (const ent of fs.readdirSync(from, { withFileTypes: true })) {
    if (excludeDirNames.includes(ent.name)) continue;
    const a = path.join(from, ent.name);
    const b = path.join(to, ent.name);
    if (ent.isDirectory()) n += copyRecursive(a, b, { excludeDirNames });
    else {
      fs.mkdirSync(path.dirname(b), { recursive: true });
      fs.copyFileSync(a, b);
      n += 1;
    }
  }
  return n;
}

function write(rel, body) {
  const abs = path.join(WT, rel);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, body);
}

// --- worktree ---
if (fs.existsSync(WT)) {
  try {
    sh(`git worktree remove -f "${WT}"`);
  } catch {
    fs.rmSync(WT, { recursive: true, force: true });
    try {
      sh("git worktree prune");
    } catch {
      /* ignore */
    }
  }
}
try {
  sh(`git branch -D ${BRANCH}`);
} catch {
  /* may not exist */
}
sh(`git fetch origin main`);
sh(`git worktree add -b ${BRANCH} "${WT}" origin/main`);

// --- copy AVA trees ---
const trees = [
  ["api/admin-ai-visibility-audit.js", "api/admin-ai-visibility-audit.js"],
  ["lib/ai-visibility-audit-action-plan", "lib/ai-visibility-audit-action-plan"],
  ["public/js/admin-ai-visibility-audit.js", "public/js/admin-ai-visibility-audit.js"],
  ["public/css/admin-ai-visibility-audit.css", "public/css/admin-ai-visibility-audit.css"],
  ["public/css/adp-ai-visibility-audit-report-v1.css", "public/css/adp-ai-visibility-audit-report-v1.css"],
  ["public/adp-ai-visibility-audit-pdf-render.html", "public/adp-ai-visibility-audit-pdf-render.html"],
  [
    "public/js/ai-demand-positioning/adp-ai-visibility-audit-pdf-v1.js",
    "public/js/ai-demand-positioning/adp-ai-visibility-audit-pdf-v1.js",
  ],
  ["config/ava-production-deployment-manifest-v1.json", "config/ava-production-deployment-manifest-v1.json"],
  ["data/ai-visibility-audit-action-plan", "data/ai-visibility-audit-action-plan"],
  ["reports/ai-visibility-audit-action-plan", "reports/ai-visibility-audit-action-plan"],
  ["fixtures/ava-customer-quality", "fixtures/ava-customer-quality"],
];

let copied = 0;
for (const [fromRel, toRel] of trees) {
  const from = path.join(SRC, fromRel);
  const to = path.join(WT, toRel);
  if (!fs.existsSync(from)) {
    console.warn("MISSING", fromRel);
    continue;
  }
  if (fs.statSync(from).isDirectory()) {
    copied += copyRecursive(from, to, {
      excludeDirNames: ["page-previews", "founder-visual-pages", "node_modules"],
    });
  } else {
    fs.mkdirSync(path.dirname(to), { recursive: true });
    fs.copyFileSync(from, to);
    copied += 1;
  }
}

// Scripts: ava-* and test-ava-*
for (const f of fs.readdirSync(path.join(SRC, "scripts"))) {
  if (/^(ava-|test-ava-|generate-ava-)/i.test(f)) {
    fs.copyFileSync(path.join(SRC, "scripts", f), path.join(WT, "scripts", f));
    copied += 1;
  }
}

// Cert docs + staged PDFs (exclude previews)
const pilotSrc = path.join(
  SRC,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const pilotDst = path.join(
  WT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
fs.mkdirSync(pilotDst, { recursive: true });
for (const f of fs.readdirSync(pilotSrc)) {
  if (/^AVA_.*\.(md|json)$/i.test(f) || f === "PREFLIGHT_HOTEL_INVENTORY.json") {
    fs.copyFileSync(path.join(pilotSrc, f), path.join(pilotDst, f));
    copied += 1;
  }
}
copied += copyRecursive(
  path.join(pilotSrc, "portfolio-v4-local-staging"),
  path.join(pilotDst, "portfolio-v4-local-staging"),
  { excludeDirNames: ["page-previews", "founder-visual-pages"] }
);
console.log("copied files:", copied);

// --- patch server.js (AVA imports + routes) ---
{
  let server = fs.readFileSync(path.join(WT, "server.js"), "utf8");
  if (!server.includes("admin-ai-visibility-audit.js")) {
    const importBlock = `import {
  getAdminAvaMeta,
  getAdminAvaHotels,
  getAdminAvaHotel,
  postAdminAvaEvidenceValidation,
  postAdminAvaEvidenceManualVerification,
  postAdminAvaStatus,
  getAdminAvaVersion,
  postAdminAvaActionDecision,
  getAdminAvaPdf,
  postAdminAvaValidatePayload,
  getAdminAvaRecommendations,
  postAdminAvaGeneratePdf,
  getAdminAvaStorageStatus,
} from "./api/admin-ai-visibility-audit.js";
`;
    if (!server.includes('from "./api/admin-report-archive.js"')) {
      throw new Error("server.js missing admin-report-archive import anchor");
    }
    server = server.replace(
      '} from "./api/admin-report-archive.js";\n',
      '} from "./api/admin-report-archive.js";\n' + importBlock
    );

    const routes = `
// AI Visibility Audit & Action Plan (admin staged; same auth as ADP monthly reviews)
app.get("/api/admin/ai-visibility-audit/meta", ...adpMonthlyReviewAdminAuth, getAdminAvaMeta);
app.get("/api/admin/ai-visibility-audit/hotels", ...adpMonthlyReviewAdminAuth, getAdminAvaHotels);
app.get("/api/admin/ai-visibility-audit/storage-status", ...adpMonthlyReviewAdminAuth, getAdminAvaStorageStatus);
app.get("/api/admin/ai-visibility-audit/:propertyId", ...adpMonthlyReviewAdminAuth, getAdminAvaHotel);
app.get("/api/admin/ai-visibility-audit/:propertyId/pdf", ...adpMonthlyReviewAdminAuth, getAdminAvaPdf);
app.get("/api/admin/ai-visibility-audit/:propertyId/recommendations", ...adpMonthlyReviewAdminAuth, getAdminAvaRecommendations);
app.get("/api/admin/ai-visibility-audit/:propertyId/versions/:versionId", ...adpMonthlyReviewAdminAuth, getAdminAvaVersion);
app.post("/api/admin/ai-visibility-audit/:propertyId/validate-payload", ...adpMonthlyReviewAdminAuth, postAdminAvaValidatePayload);
app.post("/api/admin/ai-visibility-audit/:propertyId/evidence-validation", ...adpMonthlyReviewAdminAuth, postAdminAvaEvidenceValidation);
app.post("/api/admin/ai-visibility-audit/:propertyId/evidence-manual-verification", ...adpMonthlyReviewAdminAuth, postAdminAvaEvidenceManualVerification);
app.post("/api/admin/ai-visibility-audit/:propertyId/status", ...adpMonthlyReviewAdminAuth, postAdminAvaStatus);
app.post("/api/admin/ai-visibility-audit/:propertyId/action-decision", ...adpMonthlyReviewAdminAuth, postAdminAvaActionDecision);
app.post("/api/admin/ai-visibility-audit/:propertyId/generate-pdf", ...adpMonthlyReviewAdminAuth, postAdminAvaGeneratePdf);

`;
    if (!server.includes('app.get(\n  "/api/admin/report-archive/:archiveId/pdf"')) {
      // tolerant match
    }
    const anchor =
      'app.get(\n  "/api/admin/report-archive/:archiveId/pdf",\n  ...adpMonthlyReviewAdminAuth,\n  getAdminReportArchivePdf\n);\n';
    if (!server.includes(anchor)) {
      throw new Error("server.js missing report-archive pdf route anchor");
    }
    server = server.replace(anchor, anchor + routes);
    fs.writeFileSync(path.join(WT, "server.js"), server);
    console.log("patched server.js");
  }
}

// --- patch ai-demand-admin.html from main + AVA inserts ---
{
  let html = fs.readFileSync(
    path.join(WT, "public/app/admin/ai-demand-admin.html"),
    "utf8"
  );
  if (!html.includes("admin-ai-visibility-audit.css")) {
    html = html.replace(
      /(<link rel="stylesheet" href="\/css\/admin-ai-demand-table\.css[^"]*" \/>)/,
      `$1\n  <link rel="stylesheet" href="/css/admin-ai-visibility-audit.css?v=ava-ui-v1" />`
    );
  }
  if (!html.includes('data-tab="visibility-audit"')) {
    const tab = `      <button type="button" class="section-nav-item" role="tab" aria-selected="false" data-tab="visibility-audit" id="adaTabVisibilityAudit">
        <span class="section-nav-icon" aria-hidden="true">
          <svg viewBox="0 0 24 24"><path d="M12 3l8 4v6c0 5-3.5 8.5-8 10-4.5-1.5-8-5-8-10V7l8-4z"/><path d="M9 12l2 2 4-4"/></svg>
        </span>
        <span class="section-nav-label">AI Visibility<br>Audit</span>
      </button>
`;
    html = html.replace(
      /(id="adaTabReportArchive"[\s\S]*?<\/button>\n)/,
      `$1${tab}`
    );
  }
  if (!html.includes("adaPanelVisibilityAudit")) {
    const panel = fs
      .readFileSync(path.join(SRC, "public/app/admin/ai-demand-admin.html"), "utf8")
      .match(
        /<section id="adaPanelVisibilityAudit"[\s\S]*?<\/section>\n/
      )?.[0];
    if (!panel) throw new Error("AVA panel not found in source HTML");
    html = html.replace(
      /(\n  <\/div>\n\n  <script src="\/js\/dealality-wave-loader\.js">)/,
      `\n${panel}$1`
    );
  }
  if (!html.includes("admin-ai-visibility-audit.js")) {
    html = html.replace(
      /(<script src="\/js\/admin-report-archive\.js[^"]*"><\/script>)/,
      `$1\n  <script src="/js/admin-ai-visibility-audit.js?v=ava-pdf-channels-v2"></script>`
    );
  }
  fs.writeFileSync(path.join(WT, "public/app/admin/ai-demand-admin.html"), html);
  console.log("patched ai-demand-admin.html");
}

// --- patch .railwayignore: AVA-only block (no Nobu/ADP extras) ---
{
  let ri = fs.readFileSync(path.join(WT, ".railwayignore"), "utf8");
  if (!ri.includes("RAILWAY_LEAN_AVA_V1")) {
    ri += `
# RAILWAY_LEAN_AVA_V1 — AI Visibility Audit durable package (explicit; do NOT un-ignore broad reports/**)
!data/ai-visibility-audit-action-plan/
!data/ai-visibility-audit-action-plan/certified-baselines/
!data/ai-visibility-audit-action-plan/certified-baselines/**
!data/ai-visibility-audit-action-plan/production/
!data/ai-visibility-audit-action-plan/production/store/
!data/ai-visibility-audit-action-plan/production/store/**
# Keep legacy pilot store out of Railway (dev-only history)
data/ai-visibility-audit-action-plan/store/
data/ai-visibility-audit-action-plan/store/**
!reports/ai-visibility-audit-action-plan/
!reports/ai-visibility-audit-action-plan/archive/
!reports/ai-visibility-audit-action-plan/archive/**
reports/ai-visibility-audit-action-plan/archive/**/page-previews*/
reports/ai-visibility-audit-action-plan/archive/**/page-previews*/**
reports/ai-visibility-audit-action-plan/archive/**/*.png
# Certified V4 local staging PDFs (source for draft transfer)
!reports/dealality-ai-visibility-audit-action-plan-pilot-v1/
!reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_*.md
!reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_*.json
!reports/dealality-ai-visibility-audit-action-plan-pilot-v1/portfolio-v4-local-staging/
!reports/dealality-ai-visibility-audit-action-plan-pilot-v1/portfolio-v4-local-staging/**
reports/dealality-ai-visibility-audit-action-plan-pilot-v1/portfolio-v4-local-staging/**/page-previews*/
reports/dealality-ai-visibility-audit-action-plan-pilot-v1/portfolio-v4-local-staging/**/page-previews*/**
reports/dealality-ai-visibility-audit-action-plan-pilot-v1/portfolio-v4-local-staging/**/founder-visual-pages*/
reports/dealality-ai-visibility-audit-action-plan-pilot-v1/portfolio-v4-local-staging/**/founder-visual-pages*/**
reports/dealality-ai-visibility-audit-action-plan-pilot-v1/portfolio-v4-local-staging/**/*.png
reports/dealality-ai-visibility-audit-action-plan-pilot-v1/portfolio-v4-local-staging/**/*.html
`;
    fs.writeFileSync(path.join(WT, ".railwayignore"), ri);
    console.log("patched .railwayignore");
  }
}

// --- patch production-required-assets.json ---
{
  const praPath = path.join(WT, "config/production-required-assets.json");
  const pra = JSON.parse(fs.readFileSync(praPath, "utf8"));
  if (!pra.modules?.AI_VISIBILITY_AUDIT) {
    pra.modules = pra.modules || {};
    // Find modules object - structure may use different key
  }
  // File structure uses top-level keys under required modules
  const raw = fs.readFileSync(praPath, "utf8");
  if (!raw.includes("AI_VISIBILITY_AUDIT")) {
    const srcPra = JSON.parse(
      fs.readFileSync(path.join(SRC, "config/production-required-assets.json"), "utf8")
    );
    // Locate AI_VISIBILITY_AUDIT from dirty source
    const modules = srcPra.modules || srcPra.required_modules || srcPra;
    let avaBlock = null;
    if (srcPra.modules?.AI_VISIBILITY_AUDIT) avaBlock = srcPra.modules.AI_VISIBILITY_AUDIT;
    else {
      // parse from dirty file via regex extract
      const m = fs
        .readFileSync(path.join(SRC, "config/production-required-assets.json"), "utf8")
        .match(/"AI_VISIBILITY_AUDIT"\s*:\s*\{[\s\S]*?\n    \}/);
      if (m) {
        const patched = raw.replace(
          /(\n  \},\n  "postdeploy_smoke")/,
          `,\n    ${m[0]}\n  }$1`.replace("  },\n  \"postdeploy_smoke\"", "")
        );
        // safer: JSON merge
      }
    }
    const dirty = JSON.parse(
      fs.readFileSync(path.join(SRC, "config/production-required-assets.json"), "utf8")
    );
    // Discover container key
    for (const key of Object.keys(dirty)) {
      if (dirty[key]?.AI_VISIBILITY_AUDIT) {
        pra[key] = pra[key] || {};
        pra[key].AI_VISIBILITY_AUDIT = dirty[key].AI_VISIBILITY_AUDIT;
        fs.writeFileSync(praPath, `${JSON.stringify(pra, null, 2)}\n`);
        console.log("patched production-required-assets via", key);
        break;
      }
    }
  }
}

// --- package.json AVA test scripts only ---
{
  const pkgPath = path.join(WT, "package.json");
  const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
  pkg.scripts = pkg.scripts || {};
  Object.assign(pkg.scripts, {
    "test:ava-customer-report-quality-standard":
      "node scripts/test-ava-customer-report-quality-standard-v1.mjs",
    "test:ava-pdf-channel-parity": "node scripts/test-ava-pdf-channel-parity-v1.mjs",
    "test:ava-durable-storage-gates": "node scripts/test-ava-durable-storage-gates-v1.mjs",
    "test:ava-admin-workflow": "node scripts/test-ava-admin-workflow-v1.mjs",
    "ava:verify-38-manifest": "node scripts/ava-v4-verify-38-manifest-v1.mjs",
    "ava:stage-certified-drafts":
      "node scripts/ava-v4-stage-certified-drafts-v1.mjs",
    "ava:production-release-preflight":
      "node scripts/ava-v4-production-release-preflight-v1.mjs",
  });
  fs.writeFileSync(pkgPath, `${JSON.stringify(pkg, null, 2)}\n`);
  console.log("patched package.json scripts");
}

console.log("WORKTREE", WT);
console.log("BRANCH", BRANCH);
