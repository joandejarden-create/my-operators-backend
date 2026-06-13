/**
 * Paths and naming for Brand Reference Material on disk.
 */
import fs from "fs";
import path from "path";
import {
  resolveReferenceRoot,
  resolveOperatorReferenceRoot,
  sanitizeFolderName,
  relativeLocalFilePath,
} from "./airtable-source.js";

/** @typedef {'development'|'fdd'|'press'|'regional'|'operator-materials'|'website'|'brands'|'inbox'} ReferenceSubfolder */

export const REFERENCE_SUBFOLDERS = {
  development: "development",
  fdd: "fdd",
  press: "press",
  regional: "regional",
  operatorMaterials: "operator-materials",
  website: "website",
  brands: "brands",
  inbox: "inbox",
};

/** Map CLI --type to subfolder + Source Library source type */
export const SOURCE_TYPE_MAP = {
  "development-brochure": { subfolder: "development", sourceType: "Development Brochure", origin: "Public Web" },
  "one-sheet": { subfolder: "development", sourceType: "Development Brochure", origin: "Public Web" },
  "prototype": { subfolder: "development", sourceType: "Prototype / Layout", origin: "Public Web" },
  fdd: { subfolder: "fdd", sourceType: "FDD", origin: "FDD Library" },
  press: { subfolder: "press", sourceType: "Press Release", origin: "Public Web" },
  "media-kit": { subfolder: "press", sourceType: "Press Release", origin: "Public Web" },
  regional: { subfolder: "regional", sourceType: "Development Brochure", origin: "Public Web" },
  "operator-deck": { subfolder: "operator-materials", sourceType: "Operator Deck", origin: "Operator Provided" },
  "case-study": { subfolder: "operator-materials", sourceType: "Case Study", origin: "Public Web" },
  "website-capture": { subfolder: "website", sourceType: "Website Capture", origin: "Public Web" },
  other: { subfolder: "inbox", sourceType: "Other", origin: "Other" },
};

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

export function sanitizeFileName(name) {
  return nz(name)
    .replace(/[<>:"/\\|?*]/g, "_")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 180);
}

/**
 * @param {{ companyFolder: string, brandName?: string, typeKey?: string, title?: string, ext?: string }} opts
 */
export function buildReferenceMaterialPaths(opts) {
  const company = sanitizeFolderName(opts.companyFolder);
  const typeKey = opts.typeKey || "other";
  const typeMeta = SOURCE_TYPE_MAP[typeKey] || SOURCE_TYPE_MAP.other;
  const root = opts.referenceRoot || resolveReferenceRoot();

  let relDir = company;
  // FDDs, press, regional, etc. use type subfolder even when brandName is set (brand goes in filename).
  // Brand subfolder is for development brochures / one-pagers only.
  const useBrandSubfolder = Boolean(opts.brandName) && typeMeta.subfolder === REFERENCE_SUBFOLDERS.development;
  if (useBrandSubfolder) {
    relDir = path.posix.join(company, REFERENCE_SUBFOLDERS.brands, sanitizeFolderName(opts.brandName));
  } else {
    relDir = path.posix.join(company, typeMeta.subfolder);
  }

  const date = new Date().toISOString().slice(0, 10);
  const base =
    sanitizeFileName(opts.title) ||
    sanitizeFileName(`${opts.brandName || company} - ${typeMeta.sourceType} - ${date}`);
  const ext = opts.ext || ".pdf";
  const fileName = base.toLowerCase().endsWith(ext.toLowerCase()) ? base : `${base}${ext}`;
  const relativePath = `${relDir.replace(/\\/g, "/")}/${fileName}`;
  const absoluteDir = path.join(root, ...relDir.split("/"));
  const absoluteFile = path.join(absoluteDir, fileName);

  return {
    companyFolder: company,
    relativeDir: relDir.replace(/\\/g, "/"),
    relativePath: relativePath.replace(/\\/g, "/"),
    absoluteDir,
    absoluteFile,
    fileName,
    typeMeta,
  };
}

export function ensureReferenceDirectory(absoluteDir) {
  fs.mkdirSync(absoluteDir, { recursive: true });
}

export function writeCaptureReadme(companyFolder, absoluteCompanyDir) {
  const readmePath = path.join(absoluteCompanyDir, "README.md");
  if (fs.existsSync(readmePath)) return readmePath;

  const content = `# ${companyFolder} — Reference Material

Dealality Partner Intelligence capture folder.

## Subfolders

| Folder | Use for |
|--------|---------|
| \`development/\` | Owner/developer brochures, one-sheets, brand essence flyers |
| \`fdd/\` | Franchise Disclosure Documents (U.S.) |
| \`regional/\` | CALA, EMEA, Mexico, Australia regional development PDFs |
| \`press/\` | Press releases, media kits (supporting only) |
| \`operator-materials/\` | Management company decks, case studies |
| \`website/\` | Official operator website page captures (HTML/MHTML) |
| \`brands/{Brand Name}/\` | Brand-specific PDFs when parent has many flags |

## Rules

- Official development portals first; verify domain before saving.
- Do not bypass gated materials or use confidential PDFs without permission.
- Register each file in **Partner Intelligence - Source Library** (Status: Captured).
- Extraction does **not** auto-publish to Explorer.

## CLI

\`\`\`bash
npm run partner-reference:download -- --url "https://…" --company "${companyFolder}" --type development-brochure --title "…" --apply --register
npm run partner-reference:init-folder -- --company "${companyFolder}"
\`\`\`
`;
  fs.writeFileSync(readmePath, content, "utf8");
  return readmePath;
}

export function appendCaptureLog(companyFolder, entry, referenceRoot) {
  const root = referenceRoot || resolveReferenceRoot();
  const logPath = path.join(root, sanitizeFolderName(companyFolder), "_capture-log.json");
  let log = [];
  if (fs.existsSync(logPath)) {
    try {
      log = JSON.parse(fs.readFileSync(logPath, "utf8"));
    } catch (_) {
      log = [];
    }
  }
  log.push({ ...entry, capturedAt: new Date().toISOString() });
  fs.mkdirSync(path.dirname(logPath), { recursive: true });
  fs.writeFileSync(logPath, JSON.stringify(log, null, 2), "utf8");
}

export {
  relativeLocalFilePath,
  resolveReferenceRoot,
  resolveOperatorReferenceRoot,
  sanitizeFolderName,
};
