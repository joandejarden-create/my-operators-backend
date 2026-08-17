/**
 * Brand Explorer truth audit — structured fields only (P0D-A read-only).
 * Free-form narrative paragraphs are NOT eligible as truth facts.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(__dirname, "..", "..", "..");
const PRESENTATION_DIR = path.join(REPO_ROOT, "fixtures");

/** Candidate Explorer fields assessed for structured truth eligibility. */
export const EXPLORER_CANDIDATE_FIELDS = Object.freeze([
  {
    field: "positioning",
    slotKeyPattern: /^overview\.|^positioning\./,
    structured: false,
    governanceState: "AI_ASSISTED",
    completeness: "NARRATIVE_ONLY",
    safeForTruthLayer: "NO",
    source: "Brand Explorer presentation slots",
  },
  {
    field: "conversion_orientation",
    slotKeyPattern: /^standards\.conversion/,
    structured: false,
    governanceState: "CURATED_INTERPRETATION",
    completeness: "NARRATIVE_ONLY",
    safeForTruthLayer: "NO",
    source: "Brand Explorer presentation slots",
  },
  {
    field: "soft_brand_status",
    slotKeyPattern: null,
    structured: true,
    governanceState: "STRUCTURED_GOVERNED_FACT",
    completeness: "BRAND_BASICS_DUPLICATE",
    safeForTruthLayer: "YES",
    source: "Brand Setup - Brand Basics · Brand Architecture / Brand Model",
    note: "Use Brand Basics — not Explorer narrative.",
  },
  {
    field: "owner_positioning_tags",
    slotKeyPattern: /^overview\.tags/,
    structured: false,
    governanceState: "AI_ASSISTED",
    completeness: "MISSING",
    safeForTruthLayer: "NO",
    source: "Brand Explorer presentation slots",
  },
]);

/**
 * Scan presentation fixtures for slot structure (read-only audit).
 */
export function auditBrandExplorerStructuredFields(options = {}) {
  const fixturesDir = options.fixturesDir || PRESENTATION_DIR;
  let fixtureCount = 0;
  let narrativeSlotCount = 0;

  try {
    const files = fs.readdirSync(fixturesDir).filter((f) => f.startsWith("brand-explorer-presentation-") && f.endsWith(".json"));
    fixtureCount = files.length;
    for (const file of files.slice(0, 20)) {
      const raw = JSON.parse(fs.readFileSync(path.join(fixturesDir, file), "utf8"));
      for (const row of raw.rows || []) {
        if (row.body && String(row.body).length > 40) narrativeSlotCount += 1;
      }
    }
  } catch {
    // audit continues with static field registry
  }

  return {
    source: "Brand Explorer presentation fixtures + Brand Basics structured duplicate",
    fixtureFilesSampled: fixtureCount,
    narrativeSlotsSampled: narrativeSlotCount,
    fields: EXPLORER_CANDIDATE_FIELDS.map((f) => ({
      field: f.field,
      sourceTable: f.source,
      structured: f.structured ? "YES" : "NO",
      governanceState: f.governanceState,
      completeness: f.completeness,
      safeForTruthLayer: f.safeForTruthLayer,
    })),
    readOnly: true,
    BRAND_EXPLORER_WRITES: 0,
    protectedFieldsUntouched: [
      "Company Validated",
      "Company Validation Date",
      "Brand Verified",
      "Source Library status",
      "Registry status",
      "Brand Status",
    ],
  };
}

/**
 * Resolve Explorer truth fact — only structured duplicates from Brand Basics.
 */
export function getBrandExplorerTruthFact(claimType, brandBasicsFact) {
  if (claimType === "SOFT_BRAND_COLLECTION" && brandBasicsFact) {
    return {
      ...brandBasicsFact,
      source: "Brand Basics (Explorer narrative excluded)",
    };
  }
  return null;
}
