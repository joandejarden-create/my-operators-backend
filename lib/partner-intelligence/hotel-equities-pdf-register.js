/**
 * Hotel Equities CALA PDF source registration (dry-run default).
 * @see docs/data-intelligence/hotel-equities-pdf-enrichment-plan.md
 */
import fs from "fs";
import {
  MAP_PARTNER_SOURCE,
  VAL_PARTNER_SOURCE_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { listPartnerSources, createPartnerSource } from "./airtable-source.js";
import { resolveLocalSourceAbsolutePath } from "./reference-material-paths.js";

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

export const HE_OPERATOR_ID = "recWPKu5laVZxsvpn";
export const REPORT_JSON_NAME = "hotel-equities-pdf-register.json";
export const REPORT_MD_NAME = "hotel-equities-pdf-register.md";

/** Exact on-disk filenames under Operator Reference Material/Hotel Equities CALA/ */
export const HE_PDF_SOURCES = [
  {
    key: "he-cala-marketing-deck",
    sourceTitle: "HE CALA Marketing Presentation March 2026",
    localFilePath: "Hotel Equities CALA/HE CALA Marketing Presentation  March 2026.pdf",
    sourceType: "Operator Capability Deck",
    sourceOrigin: "Operator Provided",
    sourceQuality: "High",
    region: "CALA",
    sourceUrl: null,
    notes: "CALA division marketing deck; Operator Reference Material capture (2026-03).",
  },
  {
    key: "he-cala-company-overview-pdf",
    sourceTitle: "Caribbean & Latin America Hospitality Company — Hotel Equities",
    localFilePath:
      "Hotel Equities CALA/Caribbean & Latin America Hospitality Company _ Hotel Equities.pdf",
    sourceType: "Operator Capability Deck",
    sourceOrigin: "Operator Provided",
    sourceQuality: "Medium",
    region: "CALA",
    sourceUrl: "https://www.hotelequities.com/cala.htm",
    notes:
      "CALA positioning PDF snapshot; complements Website Capture row recy1oDTNe7kyQGbE.",
  },
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

export function buildHePdfSourceFields(spec) {
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
    [MAP_PARTNER_SOURCE.profileType]: "Operator",
    [MAP_PARTNER_SOURCE.operator]: [HE_OPERATOR_ID],
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

export function assessHePdfFile(spec) {
  const issues = [];
  let resolution = null;
  try {
    resolution = resolveLocalSourceAbsolutePath(spec.localFilePath);
    const stat = fs.statSync(resolution.absolutePath);
    resolution.sizeBytes = stat.size;
  } catch (err) {
    issues.push(err.message || String(err));
  }
  return { resolution, issues };
}

export async function planHotelEquitiesPdfRegistration() {
  const { sources: existing } = await listPartnerSources({
    operatorId: HE_OPERATOR_ID,
    limit: 100,
  });
  const byLocalPath = new Map(
    existing
      .filter((s) => nz(s.localFilePath))
      .map((s) => [s.localFilePath.toLowerCase(), s])
  );

  const rows = [];
  for (const spec of HE_PDF_SOURCES) {
    const fileCheck = assessHePdfFile(spec);
    const hit = byLocalPath.get(spec.localFilePath.toLowerCase());
    const validation = buildHePdfSourceFields(spec);

    rows.push({
      key: spec.key,
      spec,
      fileCheck,
      existingSourceId: hit?.id || null,
      alreadyRegistered: Boolean(hit),
      validation,
      registrationStatus: hit
        ? "skip_already_registered"
        : fileCheck.issues.length
          ? "blocked_file_missing"
          : !validation.ok
            ? "blocked_validation"
            : "ready_to_register",
    });
  }

  return {
    operatorId: HE_OPERATOR_ID,
    generatedAt: new Date().toISOString(),
    existingSourceCount: existing.length,
    rows,
    summary: {
      total: rows.length,
      ready: rows.filter((r) => r.registrationStatus === "ready_to_register").length,
      skip: rows.filter((r) => r.registrationStatus === "skip_already_registered").length,
      blocked: rows.filter((r) => r.registrationStatus.startsWith("blocked")).length,
    },
  };
}

export async function applyHotelEquitiesPdfRegistration(plan) {
  const applied = [];
  const skipped = [];
  const errors = [];

  for (const row of plan.rows) {
    if (row.registrationStatus === "skip_already_registered") {
      skipped.push({
        key: row.key,
        reason: "already_registered",
        sourceId: row.existingSourceId,
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
