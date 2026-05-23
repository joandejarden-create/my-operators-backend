/**
 * Stage Sprint 0A route changes only (no commit).
 * Restores working-tree server files after partial index staging.
 */
import { execSync } from "child_process";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const FULL_STAGE = [
  "public/app/home.html",
  "public/app/dashboard.js",
  "public/deal-summary.html",
  "api/dashboard-home.js",
  "public/app/dashboard-adapter.js",
  "public/app.js",
  "public/my-deals.html",
  "public/brand-development-dashboard.html",
];

const REDIRECT_BLOCK = `
// Retired brand workspace pipeline page → active brand development dashboard.
function redirectBrandWorkspacePipeline(req, res) {
    const q = req.url.includes("?") ? req.url.slice(req.url.indexOf("?")) : "";
    res.redirect(302, "/brand-development-dashboard" + q);
}
app.get("/brand-workspace-pipeline", redirectBrandWorkspacePipeline);
app.get("/brand-workspace-pipeline.html", redirectBrandWorkspacePipeline);

// Legacy fit-list page → My Deals Brand Shortlist tab.
function redirectRecommendedFitList(req, res) {
    const params = new URLSearchParams(req.query);
    const dealId = params.get("dealId");
    const target = new URL("/my-deals.html", "http://local");
    target.searchParams.set("tab", "target-list");
    if (dealId) target.searchParams.set("dealId", dealId);
    const qs = target.search;
    res.redirect(302, "/my-deals.html" + qs);
}
app.get("/recommended-fit-list", redirectRecommendedFitList);
app.get("/recommended-fit-list.html", redirectRecommendedFitList);

// Winner-selection page not shipped — send users back to My Deals deal compare tab.
function redirectDealCompareSelectWinner(req, res) {
    const params = new URLSearchParams(req.query);
    const dealId = params.get("dealId");
    const target = new URL("/my-deals.html", "http://local");
    target.searchParams.set("tab", "deal-compare");
    if (dealId) target.searchParams.set("dealId", dealId);
    res.redirect(302, "/my-deals.html" + target.search);
}
app.get("/deal-compare-select-winner", redirectDealCompareSelectWinner);
app.get("/deal-compare-select-winner.html", redirectDealCompareSelectWinner);

`;

const BRAND_LIBRARY_OLD = `// Serve the brand library pages
app.get("/brand-library", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'brand-library.html'));
});`;

const BRAND_LIBRARY_NEW = `// Legacy URL: list UI moved to combined Brand Explorer (app shell aliases /brand-library → /brand-explorer-combined).
app.get("/brand-library", (req, res) => {
    res.redirect(302, "/brand-explorer-combined");
});`;

const INSERT_AFTER = [
  {
    file: "server.js",
    marker: `app.get("/webflow-brand-dashboard.html", (req, res) => {
    res.redirect("/app.html#/brand-development-dashboard");
});

`,
  },
  {
    file: "server.upload-ready.js",
    marker: `app.get("/webflow-brand-dashboard.html", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'webflow-brand-dashboard.html'));
});

`,
  },
];

function gitShowHead(relPath) {
  return execSync(`git show HEAD:${relPath}`, { cwd: root, encoding: "utf8" });
}

function apply0aHunks(headContent, marker) {
  let out = headContent;
  if (!out.includes("function redirectBrandWorkspacePipeline")) {
    if (!out.includes(marker)) {
      throw new Error("Could not find webflow-brand-dashboard anchor for redirect insert");
    }
    out = out.replace(marker, marker + REDIRECT_BLOCK);
  }
  if (out.includes(BRAND_LIBRARY_OLD)) {
    out = out.replace(BRAND_LIBRARY_OLD, BRAND_LIBRARY_NEW);
  } else if (!out.includes('res.redirect(302, "/brand-explorer-combined")')) {
    throw new Error("Could not find brand-library block to replace");
  }
  return out;
}

function stageServerPartial(relPath, marker) {
  const abs = path.join(root, relPath);
  const working = fs.readFileSync(abs, "utf8");
  const head = gitShowHead(relPath);
  const stagedContent = apply0aHunks(head, marker);
  fs.writeFileSync(abs, stagedContent);
  execSync(`git add -- "${relPath}"`, { cwd: root, stdio: "inherit" });
  fs.writeFileSync(abs, working);
}

process.chdir(root);
execSync("git reset HEAD", { stdio: "inherit" });

for (const f of FULL_STAGE) {
  execSync(`git add -- "${f}"`, { stdio: "inherit" });
}

for (const { file, marker } of INSERT_AFTER) {
  stageServerPartial(file, marker);
}

console.log("\n--- Staged files (Sprint 0A) ---");
execSync("git diff --cached --name-only", { stdio: "inherit" });
console.log("\n--- Staged diff stat ---");
execSync("git diff --cached --stat", { stdio: "inherit" });
