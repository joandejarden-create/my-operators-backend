/**
 * LOCAL-ONLY FDD download for Webhound PoC.
 * Never overwrites existing files. Writes only under data/fdd-test/raw/.
 *
 * Usage:
 *   node scripts/_tmp-fdd-poc-download.mjs --manifest reports/fdd-intelligence/fdd-webhound-discovery-rows.json
 */
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const RAW = path.join(ROOT, "data/fdd-test/raw");

const BRAND_PATH = {
  "Curio Collection by Hilton": ["hilton", "curio-collection"],
  Curio: ["hilton", "curio-collection"],
  "Kimpton Hotels & Restaurants": ["ihg", "kimpton"],
  Kimpton: ["ihg", "kimpton"],
  "Tribute Portfolio": ["marriott", "tribute-portfolio"],
  Radisson: ["choice", "radisson"],
  "Hotel Indigo": ["ihg", "hotel-indigo"],
};

function sha256Buf(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

async function downloadOne(row) {
  const brand = row.brand || "";
  const parts = BRAND_PATH[brand];
  if (!parts) throw new Error(`Unknown brand path mapping: ${brand}`);
  const year = row.fdd_year || "unknown-year";
  const dir = path.join(RAW, parts[0], parts[1], String(year));
  ensureDir(dir);

  const url = row.direct_pdf_url;
  if (!url) {
    return { brand, status: "no_direct_pdf_url", row };
  }

  const res = await fetch(url, {
    redirect: "follow",
    headers: {
      "User-Agent":
        "DealalityFddPoc/1.0 (+local-audit; not-for-production)",
      Accept: "application/pdf,*/*",
    },
  });
  if (!res.ok) {
    return { brand, status: "http_error", code: res.status, url };
  }
  const buf = Buffer.from(await res.arrayBuffer());
  if (buf.length < 100) {
    return { brand, status: "empty_or_tiny", size: buf.length, url };
  }
  const head = buf.subarray(0, 5).toString("utf8");
  if (!head.startsWith("%PDF")) {
    return {
      brand,
      status: "not_pdf",
      contentType: res.headers.get("content-type"),
      head: head.slice(0, 20),
      size: buf.length,
      url,
    };
  }

  const hash = sha256Buf(buf);
  const metaPath = path.join(dir, "metadata.json");
  const pdfPath = path.join(dir, `fdd-${hash.slice(0, 12)}.pdf`);

  // Duplicate SHA detection across test tree
  const allMeta = [];
  function walk(d) {
    if (!fs.existsSync(d)) return;
    for (const ent of fs.readdirSync(d, { withFileTypes: true })) {
      const p = path.join(d, ent.name);
      if (ent.isDirectory()) walk(p);
      else if (ent.name === "metadata.json") allMeta.push(p);
    }
  }
  walk(RAW);
  for (const mp of allMeta) {
    try {
      const prev = JSON.parse(fs.readFileSync(mp, "utf8"));
      if (prev.sha256 === hash) {
        return {
          brand,
          status: "duplicate_sha256",
          existing: mp,
          sha256: hash,
          url,
        };
      }
    } catch {
      /* ignore */
    }
  }

  if (fs.existsSync(pdfPath) || fs.existsSync(metaPath)) {
    return { brand, status: "exists_skip_no_overwrite", pdfPath, metaPath, sha256: hash };
  }

  fs.writeFileSync(pdfPath, buf);
  const meta = {
    brand,
    parent_company: row.parent_company || "",
    franchisor: row.franchisor_legal_entity || "",
    fdd_year: row.fdd_year ?? null,
    effective_date: row.effective_date || null,
    amendment_date: row.amendment_date || null,
    jurisdiction: row.jurisdiction || "",
    source_url: url,
    source_page_url: row.source_page_url || "",
    source_domain: row.source_domain || "",
    source_type: row.source_type || "",
    retrieved_at: new Date().toISOString(),
    sha256: hash,
    file_size_bytes: buf.length,
    local_path: pdfPath,
    webhound_discovery: true,
    discovery_confidence: row.discovery_confidence || "",
    notes: row.notes || "",
  };
  fs.writeFileSync(metaPath, JSON.stringify(meta, null, 2));
  return { brand, status: "downloaded", meta };
}

async function main() {
  const args = process.argv.slice(2);
  const mi = args.indexOf("--manifest");
  const manifestPath = mi >= 0 ? path.resolve(args[mi + 1]) : null;
  if (!manifestPath || !fs.existsSync(manifestPath)) {
    console.error("Need --manifest path to discovery JSON array");
    process.exit(1);
  }
  const rows = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  const list = Array.isArray(rows) ? rows : rows.rows || rows.data || [];
  const results = [];
  for (const row of list) {
    try {
      results.push(await downloadOne(row));
    } catch (e) {
      results.push({ brand: row.brand, status: "error", error: e.message });
    }
  }
  const out = path.join(ROOT, "reports/fdd-intelligence/fdd-poc-download-results.json");
  fs.writeFileSync(out, JSON.stringify({ generatedAt: new Date().toISOString(), results }, null, 2));
  console.log(JSON.stringify(results, null, 2));
}

main();
