/**
 * Radisson Blu by Choice local PDF source registration (dry-run default).
 * @see docs/data-intelligence/radisson-blu-pi-production-plan.md
 */
import fs from "fs";
import path from "path";
import {
  MAP_PARTNER_SOURCE,
  VAL_PARTNER_SOURCE_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { listPartnerSources, createPartnerSource } from "./airtable-source.js";
import { resolveLocalSourceAbsolutePath } from "./reference-material-paths.js";
import { readLocalSourceText } from "./extract-source-text.js";

/** @see docs/partner-source-library-airtable-fields.md */
const ALLOWED_SOURCE_TYPES = [
  "FDD",
  "Development Brochure",
  "Development Page",
  "Brand Page",
  "Operator Capability Deck",
  "Owner Presentation",
  "Portfolio Page",
  "Case Study",
  "Press Release",
  "Investor Presentation",
  "RFP Response",
  "Internal Note",
  "Website Capture",
  "Other",
];

export const RB_BRAND_ID = "recWPEvxBQxVVzSq3";
export const REPORT_JSON_NAME = "radisson-blu-pdf-register.json";
export const REPORT_MD_NAME = "radisson-blu-pdf-register.md";

/**
 * v1 — register on apply. Future candidates listed for inventory only (enabled: false).
 * Region: CALA is the closest schema option for Americas/CALA company materials (see HE PDF register).
 */
export const RB_PDF_SOURCES = [
  {
    key: "rb-choice-development-one-pager",
    enabled: true,
    sourceTitle: "Radisson Blu Choice development one-pager",
    localFilePath: "Choice Hotels International/Radisson Blu/RADBLU_OnePager_New_Final.pdf",
    sourceType: "Development Brochure",
    sourceOrigin: "Brand Provided",
    sourceQuality: "High",
    region: "CALA",
    sourceUrl: null,
    notes:
      "Choice company development one-pager (Americas franchise footprint; CALA brand-studio origin). Replaces non-extractable Salesforce dev-site shell recC9utJdNaKWR56k for development-model facts.",
  },
];

/** Not registered in v1 — dry-run inventory only */
export const RB_PDF_FUTURE_CANDIDATES = [
  {
    key: "rb-choice-pitch-deck",
    enabled: false,
    sourceTitle: "Radisson Blu CALA development pitch deck",
    localFilePath: "Choice Hotels International/Radisson Blu/RB_PitchDeck_Final.pdf",
    sourceType: "Development Brochure",
    note: "Future — verify global vs Americas footnotes before stewardship.",
  },
  {
    key: "rb-choice-brochure",
    enabled: false,
    sourceTitle: "Radisson Blu brand brochure (Choice)",
    localFilePath: "Choice Hotels International/Radisson Blu/brochure--blu.pdf",
    sourceType: "Development Brochure",
    note: "Future — duplicate of brochure--blu (1).pdf on disk.",
  },
  {
    key: "rb-choice-fdd-2026",
    enabled: false,
    sourceTitle: "Radisson Blu FDD 2026",
    localFilePath: "Choice Hotels International/FDDs/Radisson Blu FDD 2026.pdf",
    sourceType: "FDD",
    note: "Future — high-trust FDD; register separately when approved.",
  },
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

export function buildRbPdfSourceFields(spec) {
  const captureDate = new Date().toISOString().slice(0, 10);
  const errors = [];

  if (!nz(spec.sourceTitle)) errors.push("sourceTitle is required.");
  if (!ALLOWED_SOURCE_TYPES.includes(spec.sourceType)) {
    errors.push(`sourceType must be a schema option; got ${spec.sourceType}`);
  }
  if (!VAL_PARTNER_SOURCE_SELECTS.sourceOrigin.includes(spec.sourceOrigin)) {
    errors.push(`sourceOrigin invalid: ${spec.sourceOrigin}`);
  }
  if (!VAL_PARTNER_SOURCE_SELECTS.sourceQuality.includes(spec.sourceQuality)) {
    errors.push(`sourceQuality invalid: ${spec.sourceQuality}`);
  }

  const fields = {
    [MAP_PARTNER_SOURCE.sourceTitle]: spec.sourceTitle,
    [MAP_PARTNER_SOURCE.profileType]: "Brand",
    [MAP_PARTNER_SOURCE.brand]: [RB_BRAND_ID],
    [MAP_PARTNER_SOURCE.localFilePath]: spec.localFilePath,
    [MAP_PARTNER_SOURCE.sourceType]: spec.sourceType,
    [MAP_PARTNER_SOURCE.sourceOrigin]: spec.sourceOrigin,
    [MAP_PARTNER_SOURCE.sourceQuality]: spec.sourceQuality,
    [MAP_PARTNER_SOURCE.status]: "Captured",
    [MAP_PARTNER_SOURCE.visibility]: "Public",
    [MAP_PARTNER_SOURCE.verifiedSource]: "No",
    [MAP_PARTNER_SOURCE.approvedForExtraction]: "No",
    [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "No",
    [MAP_PARTNER_SOURCE.region]: spec.region,
    [MAP_PARTNER_SOURCE.captureDate]: captureDate,
    [MAP_PARTNER_SOURCE.notes]: spec.notes,
  };
  if (spec.sourceUrl) fields[MAP_PARTNER_SOURCE.sourceUrl] = spec.sourceUrl;

  return { ok: errors.length === 0, errors, fields };
}

export function assessRbPdfFile(spec) {
  const issues = [];
  let resolution = null;
  let textPreview = null;

  try {
    resolution = resolveLocalSourceAbsolutePath(spec.localFilePath);
    const stat = fs.statSync(resolution.absolutePath);
    resolution.sizeBytes = stat.size;

    try {
      const doc = readLocalSourceText(spec.localFilePath);
      const text = nz(doc.text);
      textPreview = {
        kind: doc.kind,
        textLength: text.length,
        preview: text.slice(0, 400),
        dualRootReadable: true,
        resolvedRootKind: doc.resolvedRootKind,
      };
    } catch (readErr) {
      issues.push(`dual-root reader failed: ${readErr.message || readErr}`);
      textPreview = {
        kind: null,
        textLength: 0,
        preview: "",
        dualRootReadable: false,
        readError: readErr.message || String(readErr),
      };
    }
  } catch (err) {
    issues.push(err.message || String(err));
    textPreview = {
      kind: null,
      textLength: 0,
      preview: "",
      dualRootReadable: false,
      readError: err.message || String(err),
    };
  }

  return { resolution, textPreview, issues };
}

function findDuplicate(existing, spec) {
  const pathKey = spec.localFilePath.toLowerCase();
  const titleKey = spec.sourceTitle.toLowerCase();
  const fileNameKey = path.basename
    ? path.basename(spec.localFilePath).toLowerCase()
    : spec.localFilePath.split("/").pop().toLowerCase();

  for (const s of existing) {
    const local = nz(s.localFilePath).toLowerCase();
    const title = nz(s.sourceTitle).toLowerCase();
    if (local && local === pathKey) {
      return { sourceId: s.id, matchType: "local_file_path", sourceTitle: s.sourceTitle };
    }
    if (title && (title === titleKey || title.includes("one-pager") && title.includes("radisson blu"))) {
      return { sourceId: s.id, matchType: "source_title", sourceTitle: s.sourceTitle };
    }
    if (local && local.endsWith(fileNameKey)) {
      return { sourceId: s.id, matchType: "filename", sourceTitle: s.sourceTitle };
    }
  }
  return null;
}

export async function planRadissonBluPdfRegistration() {
  const { sources: existing } = await listPartnerSources({
    brandId: RB_BRAND_ID,
    limit: 100,
  });
  const enabledSpecs = RB_PDF_SOURCES.filter((s) => s.enabled !== false);

  const rows = [];
  for (const spec of enabledSpecs) {
    const fileCheck = assessRbPdfFile(spec);
    const duplicate = findDuplicate(existing, spec);
    const validation = buildRbPdfSourceFields(spec);

    rows.push({
      key: spec.key,
      spec,
      fileCheck,
      duplicate,
      existingSourceId: duplicate?.sourceId || null,
      alreadyRegistered: Boolean(duplicate),
      validation,
      proposedFields: validation.fields,
      registrationStatus: duplicate
        ? "skip_already_registered"
        : fileCheck.issues.length
          ? "blocked_file_missing"
          : !validation.ok
            ? "blocked_validation"
            : !fileCheck.textPreview?.dualRootReadable
              ? "blocked_unreadable"
              : "ready_to_register",
    });
  }

  const futureInventory = RB_PDF_FUTURE_CANDIDATES.map((spec) => {
    const fileCheck = assessRbPdfFile(spec);
    return {
      key: spec.key,
      spec,
      enabled: false,
      fileFound: Boolean(fileCheck.resolution),
      sizeBytes: fileCheck.resolution?.sizeBytes ?? null,
      textLength: fileCheck.textPreview?.textLength ?? 0,
      note: spec.note || null,
    };
  });

  return {
    brandId: RB_BRAND_ID,
    generatedAt: new Date().toISOString(),
    existingSourceCount: existing.length,
    existingSources: existing.map((s) => ({
      id: s.id,
      sourceTitle: s.sourceTitle,
      localFilePath: s.localFilePath,
      sourceType: s.sourceType,
      status: s.status,
    })),
    rows,
    futureInventory,
    summary: {
      total: rows.length,
      ready: rows.filter((r) => r.registrationStatus === "ready_to_register").length,
      skip: rows.filter((r) => r.registrationStatus === "skip_already_registered").length,
      blocked: rows.filter((r) => r.registrationStatus.startsWith("blocked")).length,
      futureCandidates: futureInventory.length,
    },
  };
}

export async function applyRadissonBluPdfRegistration(plan) {
  const applied = [];
  const skipped = [];
  const errors = [];

  for (const row of plan.rows) {
    if (row.registrationStatus === "skip_already_registered") {
      skipped.push({
        key: row.key,
        reason: "already_registered",
        sourceId: row.existingSourceId,
        duplicateMatch: row.duplicate?.matchType,
      });
      continue;
    }
    if (row.registrationStatus !== "ready_to_register") {
      skipped.push({ key: row.key, reason: row.registrationStatus });
      continue;
    }
    try {
      const created = await createPartnerSource(row.validation.fields);
      applied.push({
        key: row.key,
        sourceId: created.id,
        localFilePath: row.spec.localFilePath,
        sourceTitle: row.spec.sourceTitle,
      });
    } catch (err) {
      errors.push({ key: row.key, message: err.message || String(err) });
    }
  }

  return { applied, skipped, errors };
}
