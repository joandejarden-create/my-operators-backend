/**
 * Spice Island Beach Resort — HPC stewardship UPDATE (bind existing, no create).
 *
 * Canonical: recKRJjcPnb4tVDDS (EXACT_CANONICAL_MATCH from Phase 1 search).
 *
 * Usage:
 *   node scripts/spice-island-beach-resort-hpc-stewardship-apply.mjs
 *   node scripts/spice-island-beach-resort-hpc-stewardship-apply.mjs --apply \
 *     --confirm-spice-island-census-steward-update \
 *     --confirm-hotel-property-census-only \
 *     --confirm-no-legacy-census-writes \
 *     --enable-production-writes
 *
 * Target: AIRTABLE_BASE_ID_ALT / Hotel Property Census only.
 * Never writes to forbidden legacy base appvtnDurnMSjINP6.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  assertProductionCensusWriteTarget,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/hotel-census/spice-island-beach-resort-onboarding-v1");
const APPLY =
  process.argv.includes("--apply") || process.argv.includes("--enable-production-writes");
const ENABLE = process.argv.includes("--enable-production-writes");

const CONFIRMS = {
  stewardUpdate: process.argv.includes("--confirm-spice-island-census-steward-update"),
  hpcOnly: process.argv.includes("--confirm-hotel-property-census-only"),
  noLegacy: process.argv.includes("--confirm-no-legacy-census-writes"),
};

export const CANONICAL_HPC_ID = "recKRJjcPnb4tVDDS";
export const FORBIDDEN_LEGACY_BASE = "appvtnDurnMSjINP6";
export const OFFICIAL_URL = "https://www.spiceislandbeachresort.com/";
export const OFFICIAL_TEAM_URL =
  "https://www.spiceislandbeachresort.com/resort-at-a-glance/the-team";
export const OFFICIAL_PRESS_URL = "https://www.spiceislandbeachresort.com/press/1000";
export const SLH_URL = "https://slh.com/hotels/spice-island-beach-resort";

/** Field-level evidence pack — UPDATE only, never insert. */
export const SPICE_ISLAND_EVIDENCE_PACK = Object.freeze({
  version: "spice_island_beach_resort_census_stewardship_update_v1",
  hotel: "Spice Island Beach Resort",
  duplicateDecision: "EXACT_CANONICAL_MATCH",
  canonicalHpcId: CANONICAL_HPC_ID,
  createAllowed: false,
  fields: [
    {
      field: "Official Property URL",
      value: OFFICIAL_URL,
      prior: "https://slh.com/hotels/spice-island-beach-resort",
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      action: "CORRECT_STALE",
    },
    {
      field: "Source URL",
      value: OFFICIAL_URL,
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      action: "UPDATE",
    },
    {
      field: "Phone",
      value: "+1 473-444-4258",
      sourceUrl: "https://www.spiceislandbeachresort.com/contact",
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      action: "NULL_FILL",
    },
    {
      field: "Rooms / Keys",
      value: 64,
      sourceUrl: SLH_URL,
      authority: "SOFT_BRAND_FIRST_PARTY",
      confidence: "HIGH",
      note: "SLH listing states 64 Rooms; official schema.org describes all-suite property; HPC already 64",
      action: "UNCHANGED_VERIFY",
    },
    {
      field: "Rooms Confidence",
      value: "High",
      sourceUrl: SLH_URL,
      authority: "SOFT_BRAND_FIRST_PARTY",
      confidence: "HIGH",
      action: "UPGRADE",
    },
    {
      field: "Rooms Source URL",
      value: SLH_URL,
      sourceUrl: SLH_URL,
      authority: "SOFT_BRAND_FIRST_PARTY",
      confidence: "HIGH",
      action: "UPDATE",
    },
    {
      field: "Rooms Source Type",
      value: "official_brand_directory",
      sourceUrl: SLH_URL,
      authority: "SOFT_BRAND_FIRST_PARTY",
      confidence: "HIGH",
      action: "UPDATE",
      optional: true,
    },
    {
      field: "Rooms Reviewed Date",
      value: new Date().toISOString().slice(0, 10),
      authority: "STEWARD",
      confidence: "HIGH",
      action: "UPDATE",
    },
    {
      field: "Owner Name",
      value: "Hopkin Family",
      sourceUrl: OFFICIAL_TEAM_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      note: "Family ownership stated on official team page; legal entity percentages UNKNOWN",
      action: "NULL_FILL",
    },
    {
      field: "Owner Type",
      value: "Individual / Family",
      sourceUrl: OFFICIAL_TEAM_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      action: "NULL_FILL",
      optional: true,
    },
    {
      field: "Owner Confidence",
      value: "High",
      sourceUrl: OFFICIAL_TEAM_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      action: "UPDATE",
      optional: true,
    },
    {
      field: "Operator / Management Company",
      value: "Hopkin Family (Owner-Operated)",
      sourceUrl: OFFICIAL_TEAM_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      note: "Official: Owned and operated by Hopkin family",
      action: "NULL_FILL",
    },
    {
      field: "Operator Type",
      value: "Owner-Operated",
      sourceUrl: OFFICIAL_TEAM_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      action: "NULL_FILL",
      optional: true,
    },
    {
      field: "Operator Confidence",
      value: "High",
      sourceUrl: OFFICIAL_TEAM_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      action: "UPDATE",
      optional: true,
    },
    {
      field: "Management Model",
      value: "Owner-Operated",
      sourceUrl: OFFICIAL_TEAM_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      action: "NULL_FILL",
      optional: true,
    },
    {
      field: "Current Brand",
      value: "Small Luxury Hotels of the World",
      sourceUrl: SLH_URL,
      authority: "SOFT_BRAND",
      confidence: "HIGH",
      note: "Soft-brand / collection membership; ownership remains independent family",
      action: "UNCHANGED_VERIFY",
    },
    {
      field: "Affiliation Status",
      value: "Branded",
      sourceUrl: SLH_URL,
      authority: "SOFT_BRAND",
      confidence: "HIGH",
      note: "SLH soft brand → Branded affiliation; not hard franchise",
      action: "UNCHANGED_VERIFY",
    },
    {
      field: "Address",
      value: "Grand Anse Beach",
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      action: "UNCHANGED_VERIFY",
    },
    {
      field: "City",
      value: "St George's",
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      action: "UNCHANGED_VERIFY",
    },
    {
      field: "Country",
      value: "Grenada",
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      action: "UNCHANGED_VERIFY",
    },
    {
      field: "Latitude",
      value: 12.022437,
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY_SCHEMA",
      confidence: "HIGH",
      note: "Official schema.org geo; prior census 12.022338 Medium",
      action: "UPDATE",
    },
    {
      field: "Longitude",
      value: -61.764868,
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY_SCHEMA",
      confidence: "HIGH",
      action: "UPDATE",
    },
    {
      field: "Coordinate Confidence",
      value: "High",
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY_SCHEMA",
      confidence: "HIGH",
      action: "UPGRADE",
      optional: true,
    },
    {
      field: "Enrichment Status",
      value: "Verified — material gaps",
      authority: "STEWARD",
      confidence: "HIGH",
      action: "UPDATE",
      optional: true,
      note: "First-party URL/ownership/rooms verified; legal entity still UNKNOWN",
    },
    {
      field: "Last Reviewed Date",
      value: new Date().toISOString().slice(0, 10),
      authority: "STEWARD",
      confidence: "HIGH",
      action: "UPDATE",
      optional: true,
    },
  ],
  leadership: {
    presidentManagingDirector: {
      name: "Janelle M. Hopkin",
      title: "President & Managing Director",
      sourceUrl: OFFICIAL_TEAM_URL,
      confidence: "HIGH",
      asOf: "2026-08",
    },
    director: {
      name: "Nerissa Hopkin",
      title: "Director",
      sourceUrl: OFFICIAL_TEAM_URL,
      confidence: "HIGH",
    },
    chairman: {
      name: "Lady Betty Hopkin",
      title: "Chairman",
      sourceUrl: OFFICIAL_TEAM_URL,
      confidence: "HIGH",
    },
    resortManager: {
      name: "Sheldon Keens-Douglas",
      title: "Resort Manager",
      sourceUrl: OFFICIAL_PRESS_URL,
      confidence: "HIGH",
      effectiveDate: "2025-07-01",
      note: "Official press 2025-07-04; do not invent alternate titles",
    },
    legalEntity: {
      value: null,
      status: "UNKNOWN",
      note: "Public first-party sources state family ownership; legal entity name/percentages not published",
    },
  },
});

const WRITE_FIELDS = [
  "Official Property URL",
  "Source URL",
  "Phone",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Reviewed Date",
  "Owner Name",
  "Owner Type",
  "Owner Confidence",
  "Operator / Management Company",
  "Operator Type",
  "Operator Confidence",
  "Management Model",
  "Latitude",
  "Longitude",
  "Coordinate Confidence",
  "Enrichment Status",
  "Last Reviewed Date",
  "Rooms Source Type",
];

function resolveAltBase() {
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!baseId) throw new Error("AIRTABLE_BASE_ID_ALT required");
  if (baseId === FORBIDDEN_LEGACY_BASE) {
    throw new Error(`REFUSED: ALT base resolves to forbidden legacy ${FORBIDDEN_LEGACY_BASE}`);
  }
  assertProductionCensusWriteTarget({
    baseId,
    tableName: "Hotel Property Census",
  });
  return baseId;
}

async function loadCanonical(base) {
  const r = await base("Hotel Property Census").find(CANONICAL_HPC_ID);
  return { id: r.id, fields: r.fields || {} };
}

function buildPatch(existing) {
  const patch = {};
  const planned = [];
  const skipped = [];
  for (const item of SPICE_ISLAND_EVIDENCE_PACK.fields) {
    if (!WRITE_FIELDS.includes(item.field)) {
      if (item.action === "UNCHANGED_VERIFY") {
        skipped.push({
          field: item.field,
          action: "UNCHANGED_VERIFY",
          current: existing[item.field] ?? null,
          expected: item.value,
        });
      }
      continue;
    }
    const cur = existing[item.field];
    const next = item.value;
    if (cur === next || (cur == null && next == null)) {
      skipped.push({ field: item.field, action: "ALREADY_SET", current: cur ?? null });
      continue;
    }
    // Prefer null-fill / upgrade; allow CORRECT_STALE for Official URL
    if (
      item.action === "NULL_FILL" &&
      cur != null &&
      String(cur).trim() !== "" &&
      item.field !== "Official Property URL"
    ) {
      skipped.push({
        field: item.field,
        action: "PRESERVE_EXISTING",
        current: cur,
        proposed: next,
      });
      continue;
    }
    patch[item.field] = next;
    planned.push({
      field: item.field,
      from: cur ?? null,
      to: next,
      action: item.action,
      sourceUrl: item.sourceUrl || null,
    });
  }
  return { patch, planned, skipped };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const baseId = resolveAltBase();
  const mainBase = process.env.AIRTABLE_BASE_ID;
  if (mainBase === FORBIDDEN_LEGACY_BASE) {
    // expected — product main may point at legacy; writes must not use it
  }
  if (baseId === FORBIDDEN_LEGACY_BASE) {
    throw new Error("REFUSED forbidden legacy base for write");
  }

  const pat = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!pat) throw new Error("AIRTABLE_API_KEY / AIRTABLE_PAT required");
  const base = new Airtable({ apiKey: pat }).base(baseId);

  const record = await loadCanonical(base);
  const { patch, planned, skipped } = buildPatch(record.fields);

  const preview = {
    generatedAt: new Date().toISOString(),
    phase: "HPC_STEWARDSHIP_UPDATE",
    dryRun: !APPLY,
    baseId,
    forbiddenLegacyBlocked: true,
    canonicalHpcId: CANONICAL_HPC_ID,
    duplicateDecision: "EXACT_CANONICAL_MATCH",
    createAllowed: false,
    propertyName: record.fields["Property Name"] || record.fields["Canonical Property Name"],
    rooms: record.fields["Rooms / Keys"],
    confirms: CONFIRMS,
    enableProductionWrites: ENABLE,
    plannedFieldUpdates: planned,
    skipped,
    patchPreview: patch,
    leadership: SPICE_ISLAND_EVIDENCE_PACK.leadership,
    evidencePackVersion: SPICE_ISLAND_EVIDENCE_PACK.version,
  };

  fs.writeFileSync(
    path.join(OUT, "PHASE4_HPC_STEWARDSHIP_PREVIEW.json"),
    JSON.stringify(preview, null, 2)
  );

  console.log(JSON.stringify({
    dryRun: !APPLY,
    canonicalHpcId: CANONICAL_HPC_ID,
    plannedCount: planned.length,
    skippedCount: skipped.length,
    fields: planned.map((p) => p.field),
  }, null, 2));

  if (!APPLY) {
    console.log("\nDRY-RUN only. Re-run with --apply + confirms + --enable-production-writes.");
    return;
  }

  if (!ENABLE || !CONFIRMS.stewardUpdate || !CONFIRMS.hpcOnly || !CONFIRMS.noLegacy) {
    throw new Error(
      "APPLY refused: need --enable-production-writes --confirm-spice-island-census-steward-update --confirm-hotel-property-census-only --confirm-no-legacy-census-writes"
    );
  }

  if (Object.keys(patch).length === 0) {
    const empty = { ...preview, applied: false, reason: "NO_FIELDS_TO_UPDATE" };
    fs.writeFileSync(
      path.join(OUT, "PHASE4_HPC_STEWARDSHIP_APPLIED.json"),
      JSON.stringify(empty, null, 2)
    );
    console.log("No fields to update.");
    return;
  }

  await base("Hotel Property Census").update(CANONICAL_HPC_ID, patch);
  const after = await loadCanonical(base);
  const applied = {
    ...preview,
    dryRun: false,
    applied: true,
    appliedAt: new Date().toISOString(),
    afterSelected: {
      "Official Property URL": after.fields["Official Property URL"],
      Phone: after.fields.Phone,
      "Owner Name": after.fields["Owner Name"],
      "Operator / Management Company": after.fields["Operator / Management Company"],
      "Rooms / Keys": after.fields["Rooms / Keys"],
      "Rooms Confidence": after.fields["Rooms Confidence"],
      Latitude: after.fields.Latitude,
      Longitude: after.fields.Longitude,
    },
  };
  fs.writeFileSync(
    path.join(OUT, "PHASE4_HPC_STEWARDSHIP_APPLIED.json"),
    JSON.stringify(applied, null, 2)
  );
  console.log("\nAPPLIED", CANONICAL_HPC_ID, planned.length, "fields");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
