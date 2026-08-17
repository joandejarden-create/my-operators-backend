/**
 * Harvest public operator website + press pages into Operator Reference Material.
 *
 *   npm run partner-reference:harvest-operators -- --operator arbor --apply
 *   npm run partner-reference:harvest-operators -- --operator brittain --apply
 *   npm run partner-reference:harvest-operators -- --all --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import puppeteer from "puppeteer";
import { captureHtmlWithBrowser } from "../lib/partner-intelligence/harvest-browser-capture.js";
import {
  REFERENCE_SUBFOLDERS,
  ensureReferenceDirectory,
  resolveOperatorReferenceRoot,
  writeCaptureReadme,
} from "../lib/partner-intelligence/reference-material-paths.js";
import {
  OPERATOR_HARVEST_PROFILES,
  getOperatorHarvestProfile,
  listOperatorHarvestProfileKeys,
} from "../lib/partner-intelligence/operator-reference-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const APPLY = process.argv.includes("--apply");

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : null;
}

function selectedProfiles() {
  if (process.argv.includes("--all")) {
    return Object.values(OPERATOR_HARVEST_PROFILES);
  }
  const key = arg("--operator");
  if (!key) return [];
  const profile = getOperatorHarvestProfile(key);
  return profile ? [profile] : [];
}

function initOperatorFolders(companyFolder, root) {
  const companyDir = path.join(root, companyFolder);
  fs.mkdirSync(companyDir, { recursive: true });
  for (const sub of Object.values(REFERENCE_SUBFOLDERS)) {
    fs.mkdirSync(path.join(companyDir, sub), { recursive: true });
  }
  writeCaptureReadme(companyFolder, companyDir);
}

function writeOperatorIndex(companyDir, companyFolder, captures) {
  const rows = captures
    .map((c) => {
      const html = path.basename(c.relativePath || "");
      const mhtml = c.mhtmlRelativePath ? path.basename(c.mhtmlRelativePath) : "";
      const pdf = c.pdfRelativePath ? path.basename(c.pdfRelativePath) : "";
      return `<tr>
        <td>${escapeHtml(c.title)}</td>
        <td>${mhtml ? `<a href="${escapeAttr(mhtml)}">MHTML</a>` : "—"}</td>
        <td>${pdf ? `<a href="${escapeAttr(pdf)}">PDF</a>` : "—"}</td>
        <td><a href="${escapeAttr(html)}">HTML</a></td>
      </tr>`;
    })
    .join("\n");

  const indexHtml = `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><title>${escapeHtml(companyFolder)} — Operator Reference Index</title>
<style>
body{font-family:Inter,system-ui,sans-serif;max-width:960px;margin:2rem auto;padding:0 1rem;line-height:1.5}
table{width:100%;border-collapse:collapse;font-size:0.92rem}
th,td{text-align:left;padding:8px;border-bottom:1px solid #e2e8f0}
.note{background:#f0f4ff;border:1px solid #c5d0f0;border-radius:8px;padding:12px 16px;margin:1rem 0}
</style></head><body>
<h1>${escapeHtml(companyFolder)}</h1>
<p>Operator Explorer reference captures — ${new Date().toISOString().slice(0, 10)}</p>
<div class="note">Open <strong>(archive).mhtml</strong> in Chrome/Edge for best offline viewing. Use PDF snapshots for print/share.</div>
<table><thead><tr><th>Source</th><th>Offline</th><th>PDF</th><th>HTML</th></tr></thead><tbody>
${rows}
</tbody></table></body></html>`;
  fs.writeFileSync(path.join(companyDir, "INDEX.html"), indexHtml, "utf8");
}

function escapeHtml(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function escapeAttr(s) {
  return String(s).replace(/&/g, "&amp;").replace(/"/g, "&quot;");
}

function warmOriginFor(url, domain) {
  if (!domain || !url.includes(domain)) return null;
  if (url.replace(/\/$/, "") === `https://www.${domain}`.replace(/\/$/, "") ||
      url.replace(/\/$/, "") === `https://${domain}`.replace(/\/$/, "")) {
    return null;
  }
  return `https://www.${domain}/`;
}

async function capturePage(sharedBrowser, profile, pageDef, refRoot) {
  const typeKey = pageDef.typeKey || "website-capture";
  const category = pageDef.category || (typeKey === "press" ? "press" : "website");
  const baseOpts = {
    url: pageDef.url,
    title: pageDef.title,
    companyFolder: profile.companyFolder,
    typeKey,
    category,
    referenceRoot: refRoot,
    alsoMhtml: true,
    alsoPdf: true,
    gotoTimeout: 120000,
    warmOrigin: warmOriginFor(pageDef.url, profile.domain),
  };

  async function runCapture(browser, opts = baseOpts) {
    try {
      return await captureHtmlWithBrowser(browser, opts);
    } catch (err) {
      const msg = err?.message || String(err);
      if (!/cloudflare|access denied|rate limit/i.test(msg)) throw err;
      await new Promise((r) => setTimeout(r, 8000));
      return captureHtmlWithBrowser(browser, { ...opts, warmOrigin: null });
    }
  }

  if (profile.cloudflareSensitive && pageDef.url.includes(profile.domain)) {
    const isolated = await puppeteer.launch({ headless: "new" });
    try {
      return await runCapture(isolated, { ...baseOpts, warmOrigin: null });
    } finally {
      await isolated.close();
    }
  }

  return runCapture(sharedBrowser);
}

async function harvestProfile(browser, profile, refRoot) {
  const companyDir = path.join(refRoot, profile.companyFolder);
  initOperatorFolders(profile.companyFolder, refRoot);

  const allPages = [
    ...profile.websitePages.map((p) => ({ ...p, category: p.category || "website" })),
    ...(profile.pressPages || []).map((p) => ({
      ...p,
      category: "press",
      typeKey: p.typeKey || "press",
    })),
  ];

  const report = { company: profile.companyFolder, ok: [], errors: [] };

  for (let i = 0; i < allPages.length; i++) {
    const pageDef = allPages[i];
    console.log(`  [${i + 1}/${allPages.length}]`, pageDef.title);
    if (!APPLY) {
      console.log("    WOULD", pageDef.url);
      continue;
    }
    try {
      const result = await capturePage(browser, profile, pageDef, refRoot);
      report.ok.push({ ...pageDef, ...result });
      console.log("    OK", result.relativePath);
      if (profile.domain && pageDef.url.includes(profile.domain)) {
        await new Promise((r) => setTimeout(r, 4000));
      }
    } catch (err) {
      const msg = err?.message || String(err);
      report.errors.push({ ...pageDef, error: msg });
      console.warn("    FAIL", msg);
    }
  }

  if (APPLY && report.ok.length) {
    writeOperatorIndex(companyDir, profile.companyFolder, report.ok);
    console.log("  Wrote INDEX.html");
  }

  return report;
}

async function main() {
  const profiles = selectedProfiles();
  if (!profiles.length) {
    console.error(
      "Usage: --operator arbor|brittain OR --all   add --apply to capture\nAvailable:",
      listOperatorHarvestProfileKeys().join(", ")
    );
    process.exit(1);
  }

  const refRoot = resolveOperatorReferenceRoot();
  console.log("Operator reference root:", refRoot);
  console.log("Profiles:", profiles.map((p) => p.companyFolder).join(", "));
  console.log("Mode:", APPLY ? "apply" : "dry-run");

  const browser = APPLY ? await puppeteer.launch({ headless: "new" }) : null;
  const fullReport = { generatedAt: new Date().toISOString(), root: refRoot, operators: [] };

  for (const profile of profiles) {
    console.log("\n===", profile.companyFolder, "===");
    if (!APPLY) {
      const count =
        profile.websitePages.length + (profile.pressPages?.length || 0);
      console.log(`Would capture ${count} pages from ${profile.domain}`);
      fullReport.operators.push({ company: profile.companyFolder, dryRun: true, pageCount: count });
      continue;
    }
    const report = await harvestProfile(browser, profile, refRoot);
    fullReport.operators.push(report);
  }

  if (browser) await browser.close();

  const out = path.join(__dirname, "..", "reports", "operator-reference-harvest.json");
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(fullReport, null, 2));
  console.log("\nWrote", out);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
