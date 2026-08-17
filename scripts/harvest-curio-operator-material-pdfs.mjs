/**
 * Deep harvest of public Curio Collection by Hilton PDFs into operator-materials.
 *
 *   npm run harvest-curio-operator-pdfs -- --apply
 *   npm run harvest-curio-operator-pdfs -- --apply --register
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildReferenceMaterialPaths,
  ensureReferenceDirectory,
  appendCaptureLog,
  resolveReferenceRoot,
} from "../lib/partner-intelligence/reference-material-paths.js";
import { createPartnerSource } from "../lib/partner-intelligence/airtable-source.js";
import { MAP_PARTNER_SOURCE } from "../api/lib/partner-intelligence-field-map.js";
import { PILOT_BRANDS } from "../api/lib/partner-intelligence-explorer-field-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const REGISTER = process.argv.includes("--register");
const COMPANY = "Hilton";
const BRAND = "Curio Collection by Hilton";
const BRAND_ID = PILOT_BRANDS?.curioCollection?.brandBasicsId || "receQkxgjlezsc1xg";

/** @type {{ url: string, title: string, category?: string, skipIfExists?: boolean }[]} */
const CURIO_PDF_SOURCES = [
  {
    url: "https://hmd-wp.go-vip.net/wp-content/uploads/2026/03/2026-US-FDD-Curio.pdf",
    title: "2026 US Curio FDD",
    category: "fdd",
  },
  {
    url: "https://hmd-wp.go-vip.net/wp-content/uploads/2025/05/2025-US-FDD-Curio-v.2.pdf",
    title: "2025 US Curio FDD v2",
    category: "fdd-archive",
  },
  {
    url: "https://hmd-wp.go-vip.net/wp-content/uploads/2026/02/2025-Canada-Curio-FDD-as-Amended-Feb.-12-2026.pdf",
    title: "2025 Canada Curio FDD (amended Feb 2026)",
    category: "fdd",
  },
  {
    url: "https://hmd-wp.go-vip.net/wp-content/uploads/2025/12/2025-Mexico-Curio-FDD.pdf",
    title: "2025 Mexico Curio FDD",
    category: "fdd",
  },
  {
    url: "https://griffinstafford.com/wp-content/uploads/2019/02/Curio-Brochure.pdf",
    title: "Curio Collection Brand Brochure (2019)",
    category: "brand-brochure",
  },
  {
    url: "https://assets.hiltonstatic.com/hilton-asset-cache/image/upload/Multimedia/Travel%20Agent/Hilton_Brand_Portfolio_Grid.pdf",
    title: "Hilton Brand Portfolio Grid",
    category: "portfolio-grid",
  },
  {
    url: "https://www.hiltoneventreadyplaybook.com/assets/news/Hilton-Brand-Portfolio-Grid.pdf",
    title: "Hilton Brand Portfolio Grid (Event Ready)",
    category: "portfolio-grid",
  },
  {
    url: "https://stories-editor.hilton.com/wp-content/uploads/2025/05/Hiltons-2024-Americas-Development-Awards-Winners.pdf",
    title: "Hilton 2024 Americas Development Awards Winners",
    category: "development-press",
  },
];

/** Probe patterns for regional FDDs not indexed in search results. */
const PROBE_FDD_URLS = [
  "https://hmd-wp.go-vip.net/wp-content/uploads/2026/03/2026-Brazil-Curio-FDD.pdf",
  "https://hmd-wp.go-vip.net/wp-content/uploads/2025/12/2025-Brazil-Curio-FDD.pdf",
  "https://hmd-wp.go-vip.net/wp-content/uploads/2026/03/2026-Curio-FDD.pdf",
  "https://hmd-wp.go-vip.net/wp-content/uploads/2025/12/2025-Curio-FDD-International.pdf",
  "https://hmd-wp.go-vip.net/wp-content/uploads/2026/03/2026-Thailand-Curio-FDD.pdf",
  "https://hmd-wp.go-vip.net/wp-content/uploads/2025/12/2025-China-Curio-FDD.pdf",
];

async function headOrGet(url) {
  const res = await fetch(url, {
    method: "HEAD",
    headers: {
      "User-Agent": "DealalityReferenceCapture/1.0",
      Accept: "application/pdf,*/*",
    },
    redirect: "follow",
  });
  if (res.ok) {
    const ct = (res.headers.get("content-type") || "").toLowerCase();
    const len = Number(res.headers.get("content-length") || 0);
    if (ct.includes("pdf") || len > 50000) return { ok: true, contentType: ct, size: len };
  }
  return { ok: false, status: res.status };
}

async function downloadPdf(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "DealalityReferenceCapture/1.0 (+https://dealality.com)",
      Accept: "application/pdf,*/*",
    },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${url}`);
  const buf = Buffer.from(await res.arrayBuffer());
  if (buf.length < 1024) throw new Error(`File too small (${buf.length} bytes): ${url}`);
  const magic = buf.slice(0, 4).toString();
  if (magic !== "%PDF") throw new Error(`Not a PDF (${magic.slice(0, 20)}): ${url}`);
  return buf;
}

function deriveFileName(title) {
  return `${title}.pdf`.replace(/[<>:"/\\|?*]/g, "_");
}

async function probeRegionalFdds() {
  const found = [];
  for (const url of PROBE_FDD_URLS) {
    try {
      const meta = await headOrGet(url);
      if (meta.ok) {
        const slug = path.basename(new URL(url).pathname, ".pdf").replace(/-/g, " ");
        found.push({
          url,
          title: slug.replace(/\b\w/g, (c) => c.toUpperCase()),
          category: "fdd-probed",
        });
        console.log("  [probe hit]", url);
      }
    } catch (err) {
      console.warn("  [probe err]", url, err.message);
    }
  }
  return found;
}

async function harvestEntry(entry, refRoot, results) {
  const fileName = deriveFileName(entry.title);
  const paths = buildReferenceMaterialPaths({
    companyFolder: COMPANY,
    brandName: BRAND,
    typeKey: "operator-deck",
    title: entry.title,
    ext: ".pdf",
    referenceRoot: refRoot,
  });

  const out = {
    url: entry.url,
    title: entry.title,
    category: entry.category || "other",
    relativePath: paths.relativePath,
    absoluteFile: paths.absoluteFile,
    status: "pending",
    bytes: 0,
  };

  if (fs.existsSync(paths.absoluteFile)) {
    const stat = fs.statSync(paths.absoluteFile);
    out.status = "skipped-existing";
    out.bytes = stat.size;
    results.push(out);
    console.log("  skip (exists):", paths.relativePath, `(${stat.size} bytes)`);
    return out;
  }

  if (!APPLY) {
    out.status = "dry-run";
    results.push(out);
    console.log("  would download:", entry.title);
    return out;
  }

  const buf = await downloadPdf(entry.url);
  ensureReferenceDirectory(paths.absoluteDir);
  fs.writeFileSync(paths.absoluteFile, buf);
  out.status = "downloaded";
  out.bytes = buf.length;
  results.push(out);
  console.log("  saved:", paths.relativePath, `(${buf.length} bytes)`);

  appendCaptureLog(COMPANY, {
    url: entry.url,
    relativePath: paths.relativePath,
    typeKey: "operator-deck",
    brand: BRAND,
    title: entry.title,
    category: entry.category,
  }, refRoot);

  if (REGISTER) {
    try {
      const source = await createPartnerSource({
        title: entry.title,
        sourceType: MAP_PARTNER_SOURCE.sourceType.operatorDeck,
        origin: MAP_PARTNER_SOURCE.origin.publicWeb,
        sourceUrl: entry.url,
        localFilePath: paths.relativePath,
        brandBasicsIds: [BRAND_ID],
        notes: `Curio operator-materials harvest (${entry.category || "pdf"})`,
      });
      out.sourceLibraryId = source?.id || null;
      console.log("  registered:", out.sourceLibraryId);
    } catch (err) {
      out.registerError = err.message;
      console.warn("  register failed:", err.message);
    }
  }

  return out;
}

async function main() {
  const refRoot = resolveReferenceRoot();
  const operatorDir = path.join(refRoot, COMPANY, "operator-materials");
  console.log("Reference root:", refRoot);
  console.log("Target folder:", operatorDir);
  console.log("Mode:", APPLY ? (REGISTER ? "apply+register" : "apply") : "dry-run");
  console.log("");

  if (!fs.existsSync(refRoot)) {
    console.error("Reference root does not exist:", refRoot);
    process.exit(1);
  }

  ensureReferenceDirectory(operatorDir);

  console.log("Probing regional FDD URL patterns…");
  const probed = await probeRegionalFdds();
  const allSources = [...CURIO_PDF_SOURCES, ...probed];

  const results = [];
  for (const entry of allSources) {
    console.log(`\n→ ${entry.title}`);
    try {
      await harvestEntry(entry, refRoot, results);
    } catch (err) {
      results.push({
        url: entry.url,
        title: entry.title,
        status: "error",
        error: err.message,
      });
      console.error("  ERROR:", err.message);
    }
  }

  const manifest = {
    harvestedAt: new Date().toISOString(),
    brand: BRAND,
    brandBasicsId: BRAND_ID,
    referenceRoot: refRoot,
    operatorMaterialsDir: path.join(COMPANY, "operator-materials").replace(/\\/g, "/"),
    mode: APPLY ? "apply" : "dry-run",
    summary: {
      total: results.length,
      downloaded: results.filter((r) => r.status === "downloaded").length,
      skipped: results.filter((r) => r.status === "skipped-existing").length,
      errors: results.filter((r) => r.status === "error").length,
      dryRun: results.filter((r) => r.status === "dry-run").length,
    },
    results,
  };

  const manifestPath = path.join(ROOT, "fixtures", "curio-operator-materials-pdf-manifest.json");
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
  console.log("\nManifest:", manifestPath);
  console.log("Summary:", manifest.summary);

  if (manifest.summary.errors > 0) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
