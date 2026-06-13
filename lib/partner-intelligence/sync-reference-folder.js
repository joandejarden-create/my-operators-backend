/**
 * Sync files from PARTNER_REFERENCE_ROOT/{company}/ → Source Library rows.
 */
import fs from "fs";
import path from "path";
import {
  MAP_PARTNER_SOURCE,
  VAL_PARTNER_SOURCE_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import {
  PILOT_OPERATORS,
  PILOT_BRANDS,
} from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import {
  listPartnerSources,
  createPartnerSource,
  patchPartnerSource,
  resolveReferenceRoot,
  relativeLocalFilePath,
  sanitizeFolderName,
} from "./airtable-source.js";

const READABLE_EXT = new Set([".pdf", ".txt", ".md", ".html", ".htm", ".csv", ".json"]);

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function inferSourceType(filename) {
  const ext = path.extname(filename).toLowerCase();
  if (ext === ".pdf") return "PDF";
  if (ext === ".html" || ext === ".htm") return "Website Capture";
  return "Other";
}

export function getReferenceFolderForOperator(operatorId) {
  for (const pilot of Object.values(PILOT_OPERATORS)) {
    if (pilot.recordId === operatorId) return pilot.referenceFolder;
  }
  return null;
}

export function getReferenceFolderForBrand(brandId) {
  for (const pilot of Object.values(PILOT_BRANDS)) {
    if (pilot.recordId === brandId) return pilot.referenceFolder;
  }
  return null;
}

/**
 * List readable files for a brand pilot (recursive under company folder + brand subpaths).
 * @param {{ referenceFolder: string, includeSubpaths?: string[] }} opts
 */
export function listBrandReferenceFiles(opts) {
  const folder = sanitizeFolderName(opts.referenceFolder);
  const root = resolveReferenceRoot();
  const includeSubpaths = opts.includeSubpaths || [];
  const files = [];
  const seen = new Set();

  function scanDir(relDir) {
    const absDir = path.join(root, ...relDir.split("/"));
    if (!fs.existsSync(absDir)) return;
    for (const name of fs.readdirSync(absDir)) {
      if (name.startsWith(".") || name === "README.md" || name === "_capture-log.json") continue;
      const abs = path.join(absDir, name);
      const rel = path.posix.join(relDir, name).replace(/\\/g, "/");
      if (fs.statSync(abs).isDirectory()) {
        scanDir(rel);
        continue;
      }
      const ext = path.extname(name).toLowerCase();
      if (!READABLE_EXT.has(ext)) continue;
      const key = rel.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      files.push({
        filename: name,
        relativePath: rel.replace(/\\/g, "/"),
        ext,
        sizeBytes: fs.statSync(abs).size,
      });
    }
  }

  if (includeSubpaths.length) {
    for (const sub of includeSubpaths) {
      const rel = path.posix.join(folder, sub.replace(/\\/g, "/"));
      scanDir(rel);
    }
  } else {
    scanDir(folder);
  }

  const brandFilter = opts.brandNameMatch
    ? new RegExp(opts.brandNameMatch, "i")
    : null;
  const filtered = brandFilter
    ? files.filter((f) => brandFilter.test(f.relativePath))
    : files;

  filtered.sort((a, b) => a.relativePath.localeCompare(b.relativePath));
  const absDir = path.join(root, folder);
  return {
    folder,
    absDir,
    files: filtered,
    missing: !fs.existsSync(absDir),
  };
}

export function listReadableReferenceFiles(referenceFolder) {
  const folder = sanitizeFolderName(referenceFolder);
  const absDir = path.join(resolveReferenceRoot(), folder);
  if (!fs.existsSync(absDir)) {
    return { folder, absDir, files: [], missing: true };
  }

  const files = [];
  for (const name of fs.readdirSync(absDir)) {
    if (name.startsWith(".")) continue;
    const abs = path.join(absDir, name);
    if (!fs.statSync(abs).isFile()) continue;
    const ext = path.extname(name).toLowerCase();
    if (!READABLE_EXT.has(ext)) continue;
    files.push({
      filename: name,
      relativePath: relativeLocalFilePath(folder, name),
      ext,
      sizeBytes: fs.statSync(abs).size,
    });
  }
  files.sort((a, b) => a.filename.localeCompare(b.filename));
  return { folder, absDir, files, missing: false };
}

function sourceTitleForFile(filename) {
  const base = path.basename(filename, path.extname(filename));
  return base.replace(/[_-]+/g, " ").replace(/\s+/g, " ").trim();
}

/**
 * Register local reference files as Source Library rows (dedupe by Local File Path).
 * @param {string} operatorId
 * @param {{ region?: string, autoApproveExtraction?: boolean }} opts
 */
export async function syncOperatorReferenceFolder(operatorId, opts = {}) {
  const referenceFolder = getReferenceFolderForOperator(operatorId);
  if (!referenceFolder) {
    return {
      operatorId,
      referenceFolder: null,
      synced: [],
      skipped: [],
      errors: [{ message: "No reference folder mapped for this operator." }],
    };
  }

  const scan = listReadableReferenceFiles(referenceFolder);
  const existing = await listPartnerSources({ operatorId, limit: 100 });
  const byLocalPath = new Map(
    existing.sources
      .filter((s) => nz(s.localFilePath))
      .map((s) => [s.localFilePath.toLowerCase(), s])
  );

  const synced = [];
  const skipped = [];
  const errors = [];

  for (const file of scan.files) {
    const key = file.relativePath.toLowerCase();
    const hit = byLocalPath.get(key);
    if (hit) {
      skipped.push({ localFilePath: file.relativePath, reason: "already_registered", sourceId: hit.id });
      continue;
    }

    const title = sourceTitleForFile(file.filename);
    const fields = {
      [MAP_PARTNER_SOURCE.sourceTitle]: title,
      [MAP_PARTNER_SOURCE.profileType]: "Operator",
      [MAP_PARTNER_SOURCE.operator]: [operatorId],
      [MAP_PARTNER_SOURCE.localFilePath]: file.relativePath,
      [MAP_PARTNER_SOURCE.sourceType]: inferSourceType(file.filename),
      [MAP_PARTNER_SOURCE.sourceOrigin]: "Internal Upload",
      [MAP_PARTNER_SOURCE.sourceQuality]: "Medium",
      [MAP_PARTNER_SOURCE.status]: "Captured",
      [MAP_PARTNER_SOURCE.visibility]: "Public",
      [MAP_PARTNER_SOURCE.verifiedSource]: "No",
      [MAP_PARTNER_SOURCE.approvedForExtraction]: opts.autoApproveExtraction ? "Yes" : "No",
      [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "No",
      [MAP_PARTNER_SOURCE.region]: opts.region || "",
      [MAP_PARTNER_SOURCE.captureDate]: new Date().toISOString().slice(0, 10),
      [MAP_PARTNER_SOURCE.notes]: `Auto-synced from reference folder (${referenceFolder}).`,
    };

    try {
      const created = await createPartnerSource(fields);
      synced.push({ sourceId: created.id, localFilePath: file.relativePath, sourceTitle: title });
      byLocalPath.set(key, created);
    } catch (err) {
      errors.push({ localFilePath: file.relativePath, message: err.message || String(err) });
    }
  }

  return {
    operatorId,
    referenceFolder,
    referenceRoot: resolveReferenceRoot(),
    folderMissing: scan.missing,
    fileCount: scan.files.length,
    synced,
    skipped,
    errors,
  };
}

/**
 * Mark URL sources as readable for batch extraction when they have no local path.
 */
export function isSourceExtractable(source) {
  if (nz(source.localFilePath)) return true;
  if (source.sourceUrl && /^https?:\/\//i.test(source.sourceUrl)) return true;
  return false;
}

function inferBrandSourceType(relativePath) {
  const lower = relativePath.toLowerCase();
  if (lower.includes("/fdd/") || lower.includes("fdd")) return "FDD";
  if (lower.endsWith(".html") || lower.endsWith(".htm")) return "Website Capture";
  if (lower.endsWith(".pdf")) return "PDF";
  return inferSourceType(path.basename(relativePath));
}

/**
 * Register Kimpton / brand reference files (recursive) → Source Library.
 */
export async function syncBrandReferenceFolder(brandId, opts = {}) {
  const pilot = Object.values(PILOT_BRANDS).find((p) => p.recordId === brandId);
  if (!pilot) {
    return {
      brandId,
      referenceFolder: null,
      synced: [],
      skipped: [],
      errors: [{ message: "No reference folder mapped for this brand." }],
    };
  }

  const scan = listBrandReferenceFiles({
    referenceFolder: pilot.referenceFolder,
    includeSubpaths: pilot.includeSubpaths || [],
    brandNameMatch: pilot.brandNameMatch || pilot.brandSlug,
  });

  const existing = await listPartnerSources({ brandId, limit: 100 });
  const byLocalPath = new Map(
    existing.sources
      .filter((s) => nz(s.localFilePath))
      .map((s) => [s.localFilePath.toLowerCase(), s])
  );

  const synced = [];
  const skipped = [];
  const errors = [];

  for (const file of scan.files) {
    const key = file.relativePath.toLowerCase();
    const hit = byLocalPath.get(key);
    if (hit) {
      skipped.push({ localFilePath: file.relativePath, reason: "already_registered", sourceId: hit.id });
      continue;
    }

    const title = sourceTitleForFile(file.filename);
    const fields = {
      [MAP_PARTNER_SOURCE.sourceTitle]: title,
      [MAP_PARTNER_SOURCE.profileType]: "Brand",
      [MAP_PARTNER_SOURCE.brand]: [brandId],
      [MAP_PARTNER_SOURCE.localFilePath]: file.relativePath,
      [MAP_PARTNER_SOURCE.sourceType]: inferBrandSourceType(file.relativePath),
      [MAP_PARTNER_SOURCE.sourceOrigin]: "Brand Provided",
      [MAP_PARTNER_SOURCE.sourceQuality]: "High",
      [MAP_PARTNER_SOURCE.status]: "Captured",
      [MAP_PARTNER_SOURCE.visibility]: "Public",
      [MAP_PARTNER_SOURCE.verifiedSource]: "Yes",
      [MAP_PARTNER_SOURCE.approvedForExtraction]: opts.autoApproveExtraction ? "Yes" : "No",
      [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "No",
      [MAP_PARTNER_SOURCE.region]: pilot.region || "",
      [MAP_PARTNER_SOURCE.captureDate]: new Date().toISOString().slice(0, 10),
      [MAP_PARTNER_SOURCE.notes]: `Auto-synced from brand reference folder (${pilot.referenceFolder}).`,
    };

    try {
      const created = await createPartnerSource(fields);
      synced.push({ sourceId: created.id, localFilePath: file.relativePath, sourceTitle: title });
      byLocalPath.set(key, created);
    } catch (err) {
      errors.push({ localFilePath: file.relativePath, message: err.message || String(err) });
    }
  }

  return {
    brandId,
    referenceFolder: pilot.referenceFolder,
    referenceRoot: resolveReferenceRoot(),
    folderMissing: scan.missing,
    fileCount: scan.files.length,
    synced,
    skipped,
    errors,
  };
}

export async function approveSourcesForBatchExtraction(operatorId) {
  const { sources } = await listPartnerSources({ operatorId, limit: 100 });
  const approved = [];
  for (const source of sources) {
    if (!isSourceExtractable(source)) continue;
    if (source.approvedForExtraction === "Yes") continue;
    await patchPartnerSource(source.id, {
      [MAP_PARTNER_SOURCE.approvedForExtraction]: "Yes",
    });
    approved.push(source.id);
  }
  return approved;
}

export async function approveBrandSourcesForBatchExtraction(brandId) {
  const { sources } = await listPartnerSources({ brandId, limit: 100 });
  const approved = [];
  for (const source of sources) {
    if (!isSourceExtractable(source)) continue;
    if (source.approvedForExtraction === "Yes") continue;
    await patchPartnerSource(source.id, {
      [MAP_PARTNER_SOURCE.approvedForExtraction]: "Yes",
    });
    approved.push(source.id);
  }
  return approved;
}

export { VAL_PARTNER_SOURCE_SELECTS, MAP_PARTNER_SOURCE };
