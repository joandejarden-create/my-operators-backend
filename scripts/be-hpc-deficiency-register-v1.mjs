#!/usr/bin/env node
/**
 * Read-only HPC deficiency register + correction plan for Curio / Ascend pilots.
 * No Airtable writes. No paid research. No publication.
 *
 * Produces:
 *  - reports/brand-explorer/hpc-footprint-integration-v1/hpc-deficiency-register-curio-ascend.json
 *  - reports/brand-explorer/hpc-footprint-integration-v1/hpc-correction-plan-READONLY.md
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BE_PHASE2_PILOT_BRANDS,
  BE_HPC_FIELDS,
  fetchHpcRowsForBrandMatchers,
  buildBrandHpcFootprintSummary,
  buildCustomerFacingHpcCoverageCopy,
  hpcFootprintLiveConfigured,
} from "../lib/brand-explorer/be-hpc-footprint-summary-v1.js";
import {
  filterHpcRowsForBrandFootprint,
  mapCountryToFootprintRegion,
} from "../lib/brand-explorer/be-airtable-integration-sot-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT_DIR = path.join(ROOT, "reports/brand-explorer/hpc-footprint-integration-v1");
fs.mkdirSync(OUT_DIR, { recursive: true });

const cohort = BE_PHASE2_PILOT_BRANDS.filter((b) =>
  ["curio-collection", "ascend"].includes(b.slug),
);

function classifyDeficiencies(row) {
  const defs = [];
  const country = String(row.country || "").trim();
  const propertyType = String(row.propertyType || "").trim();
  const rooms = row.roomsKeys;
  const region = mapCountryToFootprintRegion(country || row.market || row.continent || "");
  if (!country) defs.push({ field: BE_HPC_FIELDS.country, issue: "missing_country", existing: "" });
  if (!region) {
    defs.push({
      field: "Footprint Region (derived)",
      issue: "unclassified_region",
      existing: country || "",
      note: "Country present but not mapped to AM/CALA/EU/MEA/APAC — expand mapper or fix Country value",
    });
  }
  if (!propertyType || /^unknown$/i.test(propertyType)) {
    defs.push({
      field: BE_HPC_FIELDS.propertyType,
      issue: "unknown_or_missing_property_type",
      existing: propertyType || "",
    });
  }
  if (rooms == null || rooms === "" || !Number.isFinite(Number(rooms)) || Number(rooms) <= 0) {
    defs.push({
      field: BE_HPC_FIELDS.roomsKeys,
      issue: "missing_rooms_keys",
      existing: rooms == null ? null : String(rooms),
    });
  }
  return defs;
}

const register = {
  contract: "BE_HPC_DEFICIENCY_REGISTER_V1",
  generatedAt: new Date().toISOString(),
  liveConfigured: hpcFootprintLiveConfigured(),
  airtableWrites: 0,
  paidResearch: false,
  publication: false,
  writeStatus: "READONLY_PLAN_ONLY — awaiting founder approval of exact correction rows",
  brands: {},
  totals: {
    qualifyingRecords: 0,
    incompleteRecords: 0,
    deficiencyRows: 0,
    verifiedCorrectionCandidates: 0,
    unresolvedFields: 0,
  },
};

for (const b of cohort) {
  const fetchResult = await fetchHpcRowsForBrandMatchers([...b.hpcMatchers], {
    maxPages: 20,
    pageSize: 100,
  });
  const { matched } = filterHpcRowsForBrandFootprint(fetchResult.rows || [], {
    brandMatchers: [...b.hpcMatchers],
  });
  const summary = await buildBrandHpcFootprintSummary({
    brandBasicsRecordId: b.recordId,
    brandName: b.name,
    brandMatchers: [...b.hpcMatchers],
    allowLive: true,
  });

  const records = matched.map((row) => {
    const deficiencies = classifyDeficiencies(row);
    return {
      hpcRecordId: row.id,
      propertyName: row.propertyName || "",
      currentBrand: row.currentBrand || "",
      radarStatus: row.radarStatus || "",
      country: row.country || "",
      derivedRegion: mapCountryToFootprintRegion(row.country || row.market || "") || "Unclassified",
      propertyType: row.propertyType || "",
      roomsKeys: row.roomsKeys ?? null,
      deficiencies,
      incomplete: deficiencies.length > 0,
    };
  });

  const incomplete = records.filter((r) => r.incomplete);
  const deficiencyRows = incomplete.flatMap((r) =>
    r.deficiencies.map((d) => ({
      hpcRecordId: r.hpcRecordId,
      hotelName: r.propertyName,
      fieldToUpdate: d.field,
      existingValue: d.existing,
      proposedReplacement: null,
      supportingEvidence: [],
      verificationStatus: "RESEARCH_REQUIRED",
      issue: d.issue,
      note: d.note || null,
    })),
  );

  register.brands[b.slug] = {
    brandBasicsRecordId: b.recordId,
    brandName: b.name,
    qualifyingMatched: matched.length,
    metrics: summary.metrics,
    displayMetrics: summary.displayMetrics,
    roomsCoverage: summary.roomsCoverage,
    breakdowns: {
      regionHotelSum: (summary.breakdowns?.region || []).reduce((n, r) => n + r.hotels, 0),
      countryHotelSum: (summary.breakdowns?.country || []).reduce((n, r) => n + r.hotels, 0),
      propertyTypeHotelSum: (summary.breakdowns?.propertyType || []).reduce(
        (n, r) => n + r.hotels,
        0,
      ),
      region: summary.breakdowns?.region || [],
      country: summary.breakdowns?.country || [],
      propertyType: summary.breakdowns?.propertyType || [],
    },
    customerFacingCopy: buildCustomerFacingHpcCoverageCopy(summary),
    incompleteRecordCount: incomplete.length,
    deficiencyRowCount: deficiencyRows.length,
    records,
    correctionPlanRows: deficiencyRows,
  };

  register.totals.qualifyingRecords += matched.length;
  register.totals.incompleteRecords += incomplete.length;
  register.totals.deficiencyRows += deficiencyRows.length;
  register.totals.unresolvedFields += deficiencyRows.length;
}

const jsonPath = path.join(OUT_DIR, "hpc-deficiency-register-curio-ascend.json");
fs.writeFileSync(jsonPath, JSON.stringify(register, null, 2));

const curio = register.brands["curio-collection"];
const ascend = register.brands.ascend;
const md = `# HPC Correction Plan (READ-ONLY) — Curio & Ascend

**Status:** \`READONLY_PLAN_ONLY\` — **no production Airtable writes performed.**  
**Generated:** ${register.generatedAt}  
**Live HPC configured:** ${register.liveConfigured}  
**Source:** Platform Airtable → Hotel Property Census → Current Brand matchers → Radar Status Open/Pipeline

## Governance

- Preserve existing HPC record IDs.
- Only verified corrections may be written after founder approval of the exact rows below.
- Do not invent room counts, property types, or geography.
- Unknown / missing classifications remain \`RESEARCH_REQUIRED\` until credible public evidence is attached.
- Brand Explorer, Hotel Explorer, Radar, ADP, and GDI all consume these same HPC records.

## Live footprint snapshot (pre-correction)

| Brand | Open hotels | Known open rooms | Props with known rooms | Props missing rooms | Pipeline | Region sum | Country sum | Archetype sum |
|-------|-------------|------------------|------------------------|---------------------|----------|------------|-------------|---------------|
| Curio | ${curio.metrics.totalOpenHotels} | ${curio.displayMetrics.totalOpenKeys ?? "n/a (incomplete)"} | ${curio.roomsCoverage.openWithKeys} | ${curio.roomsCoverage.openMissingKeys} | ${curio.metrics.totalPipelineHotels} | ${curio.breakdowns.regionHotelSum} | ${curio.breakdowns.countryHotelSum} | ${curio.breakdowns.propertyTypeHotelSum} |
| Ascend | ${ascend.metrics.totalOpenHotels} | ${ascend.displayMetrics.totalOpenKeys ?? "n/a (all missing)"} | ${ascend.roomsCoverage.openWithKeys} | ${ascend.roomsCoverage.openMissingKeys} | ${ascend.metrics.totalPipelineHotels} | ${ascend.breakdowns.regionHotelSum} | ${ascend.breakdowns.countryHotelSum} | ${ascend.breakdowns.propertyTypeHotelSum} |

### Customer-facing coverage copy (current)

**Curio:** ${curio.customerFacingCopy}

**Ascend:** ${ascend.customerFacingCopy}

### Region mapping note

Saint Lucia is now mapped to **CALA** in \`mapCountryToFootprintRegion\` (country was already present on the HPC record). No Airtable Country write required for that hotel for region display. Unclassified region count for Curio is now **0**.

## Deficiency totals

| Metric | Count |
|--------|------:|
| Qualifying HPC records (both brands) | ${register.totals.qualifyingRecords} |
| Incomplete records (≥1 deficiency) | ${register.totals.incompleteRecords} |
| Field-level deficiency rows | ${register.totals.deficiencyRows} |
| Verified correction candidates ready to write | **0** (research evidence not yet attached) |
| Unresolved fields | ${register.totals.unresolvedFields} |

## Curio — required remediation targets

| Target | Live count | Notes |
|--------|----------:|-------|
| Missing Rooms / Keys | ${curio.roomsCoverage.openMissingKeys} | Do not treat as zero; Known Rooms = ${curio.displayMetrics.totalOpenKeys} from ${curio.roomsCoverage.openWithKeys} properties |
| Unknown / blank Property Type | ${curio.records.filter((r) => r.deficiencies.some((d) => d.issue === "unknown_or_missing_property_type")).length} | Must use controlled taxonomy; no generic filler |
| Unclassified region (derived) | ${curio.records.filter((r) => r.deficiencies.some((d) => d.issue === "unclassified_region")).length} | After mapper fix should be 0 |

## Ascend — required remediation targets

| Target | Live count | Notes |
|--------|----------:|-------|
| Missing Rooms / Keys | ${ascend.roomsCoverage.openMissingKeys} | All ${ascend.metrics.totalOpenHotels} open properties lack keys |
| Unknown / blank Property Type | ${ascend.records.filter((r) => r.deficiencies.some((d) => d.issue === "unknown_or_missing_property_type")).length} | |
| Unclassified region (derived) | ${ascend.records.filter((r) => r.deficiencies.some((d) => d.issue === "unclassified_region")).length} | |

## Proposed write template (empty until evidence attached)

For each approved row after research:

| HPC record ID | Hotel name | Field | Existing | Verified replacement | Evidence URLs | Research date | Confidence | Verification status |
|---------------|------------|-------|----------|----------------------|---------------|---------------|------------|---------------------|
| *(none approved yet — see JSON register for RESEARCH_REQUIRED rows)* | | | | | | | | |

Full record-level register: \`hpc-deficiency-register-curio-ascend.json\`

## Cross-product impact

Updating Rooms / Keys, Property Type, or Country on these HPC IDs will refresh Brand Explorer footprint summaries, Radar Open/Pipeline inventory, Hotel Explorer property cards, ADP/GDI consumers that read the same Hotel Property Census table. No Brand Explorer fixture duplication required after authorized writes.

## Explicit non-claims

- No production Airtable data was changed in this cycle.
- No paid research was executed.
- Curio and Ascend remain **NOT APPROVED**.
- Do not claim all records resolved until every required field has credible supporting evidence.
`;

const mdPath = path.join(OUT_DIR, "hpc-correction-plan-READONLY.md");
fs.writeFileSync(mdPath, md);
console.log(
  JSON.stringify(
    {
      ok: true,
      jsonPath,
      mdPath,
      totals: register.totals,
      airtableWrites: 0,
    },
    null,
    2,
  ),
);
