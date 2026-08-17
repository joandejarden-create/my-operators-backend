/**
 * Download a public PDF/document into Brand Reference Material + optional Source Library row.
 *
 *   npm run partner-reference:download -- --url "https://…" --company "Marriott International" --type development-brochure --title "Fairfield EMEA one-pager"
 *   npm run partner-reference:download -- --url "https://…" --company "Choice Hotels International" --brand "Radisson Blu" --type fdd --apply --register
 *
 * Flags:
 *   --apply          Write file (default: dry-run)
 *   --register       Create Partner Intelligence Source Library row (requires --apply)
 *   --brand-id rec…  Link source to Brand Setup record
 *   --operator-id rec… Link source to Operator Setup master
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildReferenceMaterialPaths,
  ensureReferenceDirectory,
  writeCaptureReadme,
  appendCaptureLog,
  resolveReferenceRoot,
} from "../lib/partner-intelligence/reference-material-paths.js";
import { createPartnerSource } from "../lib/partner-intelligence/airtable-source.js";
import { MAP_PARTNER_SOURCE } from "../api/lib/partner-intelligence-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : null;
}

const APPLY = process.argv.includes("--apply");
const REGISTER = process.argv.includes("--register");
const url = arg("--url");
const company = arg("--company");
const brand = arg("--brand");
const typeKey = arg("--type") || "other";
const title = arg("--title");
const brandId = arg("--brand-id");
const operatorId = arg("--operator-id");
const profileType = arg("--profile-type") || (operatorId ? "Operator" : brandId ? "Brand" : "Brand");

const BROWSER_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

async function downloadUrl(targetUrl) {
  const attempts = [
    "DealalityReferenceCapture/1.0 (+https://dealality.com)",
    BROWSER_USER_AGENT,
  ];
  let lastErr;
  for (const userAgent of attempts) {
    try {
      const res = await fetch(targetUrl, {
        headers: {
          "User-Agent": userAgent,
          Accept: "application/pdf,text/html,*/*",
        },
        redirect: "follow",
      });
      if (!res.ok) throw new Error(`Download failed HTTP ${res.status}: ${targetUrl}`);
      const buf = Buffer.from(await res.arrayBuffer());
      const ct = (res.headers.get("content-type") || "").toLowerCase();
      let ext = ".pdf";
      if (ct.includes("html")) ext = ".html";
      else if (ct.includes("json")) ext = ".json";
      else if (!ct.includes("pdf") && buf.slice(0, 4).toString() !== "%PDF") {
        const fromUrl = path.extname(new URL(targetUrl).pathname);
        if (fromUrl) ext = fromUrl;
      }
      return { buf, contentType: ct, ext };
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr;
}

async function main() {
  if (!url || !/^https?:\/\//i.test(url)) {
    console.error("Usage: --url https://… --company \"Parent Company Name\" [--brand \"Brand\"] [--type development-brochure|fdd|…] [--title \"…\"] [--apply] [--register]");
    process.exit(1);
  }
  if (!company) {
    console.error("--company is required (folder name under Brand Reference Material).");
    process.exit(1);
  }

  const paths = buildReferenceMaterialPaths({
    companyFolder: company,
    brandName: brand || undefined,
    typeKey,
    title: title || undefined,
  });

  console.log("Reference root:", resolveReferenceRoot());
  console.log("Target:", paths.absoluteFile);
  console.log("Relative path:", paths.relativePath);
  console.log("Source type:", paths.typeMeta.sourceType);
  console.log("Mode:", APPLY ? (REGISTER ? "apply+register" : "apply") : "dry-run");

  if (!APPLY) {
    console.log("\nDry run — add --apply to download. Add --register to create Source Library row.");
    process.exit(0);
  }

  const { buf, ext } = await downloadUrl(url);
  const finalPaths =
    ext !== ".pdf" && !paths.fileName.endsWith(ext)
      ? buildReferenceMaterialPaths({
          companyFolder: company,
          brandName: brand || undefined,
          typeKey,
          title: title || paths.fileName.replace(/\.[^.]+$/, ""),
          ext,
        })
      : paths;

  ensureReferenceDirectory(finalPaths.absoluteDir);
  writeCaptureReadme(company, path.join(resolveReferenceRoot(), finalPaths.companyFolder));
  fs.writeFileSync(finalPaths.absoluteFile, buf);
  console.log("Wrote", finalPaths.absoluteFile, `(${buf.length} bytes)`);

  appendCaptureLog(company, {
    url,
    relativePath: finalPaths.relativePath,
    typeKey,
    brand: brand || null,
    title: title || finalPaths.fileName,
  });

  let sourceRecord = null;
  if (REGISTER) {
    const fields = {
      [MAP_PARTNER_SOURCE.sourceTitle]: title || finalPaths.fileName.replace(/\.[^.]+$/, ""),
      [MAP_PARTNER_SOURCE.profileType]: profileType,
      [MAP_PARTNER_SOURCE.sourceUrl]: url,
      [MAP_PARTNER_SOURCE.localFilePath]: finalPaths.relativePath,
      [MAP_PARTNER_SOURCE.sourceType]: finalPaths.typeMeta.sourceType,
      [MAP_PARTNER_SOURCE.sourceOrigin]: finalPaths.typeMeta.origin,
      [MAP_PARTNER_SOURCE.sourceQuality]: typeKey === "fdd" ? "High" : "Medium",
      [MAP_PARTNER_SOURCE.status]: "Captured",
      [MAP_PARTNER_SOURCE.visibility]: "Public",
      [MAP_PARTNER_SOURCE.verifiedSource]: "No",
      [MAP_PARTNER_SOURCE.approvedForExtraction]: "No",
      [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "No",
      [MAP_PARTNER_SOURCE.captureDate]: new Date().toISOString().slice(0, 10),
      [MAP_PARTNER_SOURCE.notes]: `Captured via download-partner-reference-material.mjs`,
    };
    if (brandId) fields[MAP_PARTNER_SOURCE.brand] = [brandId];
    if (operatorId) fields[MAP_PARTNER_SOURCE.operator] = [operatorId];
    sourceRecord = await createPartnerSource(fields);
    console.log("Source Library row:", sourceRecord.id);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    url,
    company,
    brand,
    relativePath: finalPaths.relativePath,
    sourceId: sourceRecord?.id || null,
  };
  const outPath = path.join(ROOT, "reports", "partner-reference-download.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("Report:", outPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
