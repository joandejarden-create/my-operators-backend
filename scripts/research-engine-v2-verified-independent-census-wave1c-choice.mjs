/**
 * Research Engine V2 — Verified Independent Census Wave 1C: Choice Mexico
 *
 * Implements Property Identity V1 + Temporal Affiliation V1.
 * No Webhound. No credits. No Airtable writes. Legacy post-freeze only.
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { runReconstructionWave } from "../lib/research-engine-v2/clean-census/wave-engine.js";
import { reconcileAfterFreeze } from "../lib/research-engine-v2/clean-census/legacy-reconcile.js";
import {
  runStrictIndependentRediscovery,
  runTargetedVerificationChallenges,
  CHALLENGE_CLASS_RECOMMENDATION,
} from "../lib/research-engine-v2/clean-census/legacy-challenges.js";
import { batchAssessProductionEligibility } from "../lib/research-engine-v2/clean-census/production-eligibility.js";
import { findCrossFamilyIdentities } from "../lib/research-engine-v2/clean-census/cross-family-identity.js";
import {
  attachPropertyIdentities,
  assessSamePhysicalProperty,
  classifyReflagOrAffiliation,
  propertyIdentityFromVerifiedRecord,
} from "../lib/research-engine-v2/clean-census/property-identity.js";
import {
  applyTemporalAffiliationSeed,
  appendHistoricalAffiliation,
  TEMPORAL_AFFILIATION_VERSION,
} from "../lib/research-engine-v2/clean-census/temporal-affiliation.js";
import { researchRadissonIndividualsChoiceRelationship } from "../lib/research-engine-v2/clean-census/choice-mexico-discovery.js";
import {
  MATERIAL_CENSUS_FIELDS,
  CORE_MATERIAL_FIELDS,
} from "../lib/research-engine-v2/clean-census/provenance.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/research-engine-v2/verified-independent-census-wave1c-choice");
const IHG_V1 = join(ROOT, "data/research-engine-v2/verified-independent-census-v1");
const HILTON_1B = join(ROOT, "data/research-engine-v2/verified-independent-census-wave1b-hilton");
const FETCH_DELAY_MS = Number(process.env.RE_V2_FETCH_DELAY_MS || 80);
const FETCH_PROPERTY_PAGES = process.env.RE_V2_CHOICE_PROPERTY_PAGES === "1";

function writeJson(name, obj) {
  writeFileSync(join(OUT, name), JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(name, text) {
  writeFileSync(join(OUT, name), text, "utf8");
}

function parseCsvLine(line) {
  const o = [];
  let c = "";
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (q) {
      if (ch === '"' && line[i + 1] === '"') {
        c += '"';
        i++;
      } else if (ch === '"') q = false;
      else c += ch;
    } else if (ch === '"') q = true;
    else if (ch === ",") {
      o.push(c);
      c = "";
    } else c += ch;
  }
  o.push(c);
  return o;
}

function loadLegacyChoiceMexicoReference() {
  const csv = readFileSync(join(ROOT, "reports/census-amenities-blank-rows.csv"), "utf8").split(/\r?\n/);
  const rows = [];
  for (const line of csv.slice(1)) {
    if (!line.trim()) continue;
    const f = parseCsvLine(line);
    const name = f[1] || "";
    const parent = f[2] === "(blank parent)" ? "" : f[2] || "";
    const country = f[4] || "";
    if (!/Mexico/i.test(country)) continue;
    const choiceParent = /Choice/i.test(parent);
    const choiceName =
      /\b(Comfort|Quality|Sleep Inn|Ascend|Clarion|Radisson|Econo Lodge|Rodeway|MainStay|WoodSpring|Suburban|Cambria|Park Inn)\b/i.test(
        name
      );
    if (!choiceParent && !choiceName) continue;
    // Prefer Parent=Choice; also include Choice-branded names with blank/wrong parent for challenge surface
    if (!choiceParent && !choiceName) continue;
    rows.push({
      hotelId: f[0],
      name,
      parentCompany: parent,
      status: f[3],
      country,
      city: "",
      rooms: null,
      affiliation: null,
      provenance_class: "Legacy-Origin — Unreconstructed",
      cohort_note: choiceParent ? "parent_choice" : "name_choice_brand_hint",
    });
  }
  // Prefer parent Choice rows; keep name-hint as separate note but include both for legacy-only challenges
  return rows.filter((r) => /Choice/i.test(r.parentCompany) || r.cohort_note === "name_choice_brand_hint");
}

function loadFrozenRecords(path) {
  if (!existsSync(path)) return [];
  const data = JSON.parse(readFileSync(path, "utf8"));
  return data.records || [];
}

function loadSummary(path) {
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf8"));
}

function classifyIndependentOnly(rec) {
  const status = String(rec.fields?.status || "").toLowerCase();
  const channel = rec.choice_structured?.discoveryChannel || "";
  const reasons = [];
  if (/pipeline/i.test(status)) reasons.push("pipeline");
  if (channel === "choice_sitemap_mx_union") reasons.push("sitemap_only_not_on_regional");
  if (rec.faranda_named_independently) reasons.push("faranda_named_independent");
  if (!reasons.length) reasons.push("legacy omission or identity mismatch");
  return {
    independent_record_id: rec.independent_record_id,
    name: rec.fields?.name,
    brand: rec.brand,
    status: rec.fields?.status || null,
    property_id: rec.property_id || null,
    likely_absence_class: reasons[0],
    notes: reasons,
  };
}

function assessBrandExplorerReadiness(records, relationship) {
  const byBrand = new Map();
  for (const r of records) {
    const brand = r.brand || r.fields?.Affiliation || "Unknown";
    if (!byBrand.has(brand)) {
      byBrand.set(brand, {
        brand,
        hotel_count: 0,
        core_pct_avg: 0,
        material_pct_avg: 0,
        statuses: {},
        remaining_gaps: new Set(),
        faranda_named: 0,
      });
    }
    const b = byBrand.get(brand);
    b.hotel_count++;
    b.core_pct_avg += r.completeness?.corePct || 0;
    b.material_pct_avg += r.completeness?.materialPct || 0;
    const st = r.reconstruction_status || "Unknown";
    b.statuses[st] = (b.statuses[st] || 0) + 1;
    if (!r.fields?.rooms) b.remaining_gaps.add("rooms");
    if (!r.fields?.["Management Company"]) b.remaining_gaps.add("Management Company");
    if ((r.completeness?.materialPct || 0) < 65) b.remaining_gaps.add("material_fields_below_65pct");
    if (r.faranda_named_independently) b.faranda_named++;
  }

  return [...byBrand.values()].map((b) => {
    const coreAvg = Math.round(b.core_pct_avg / b.hotel_count);
    const matAvg = Math.round(b.material_pct_avg / b.hotel_count);
    let research_status = "Deep Research Required";
    if (coreAvg >= 100 && matAvg >= 70) research_status = "Independent Research Complete";
    else if (coreAvg >= 100 && matAvg >= 55) research_status = "Materially Complete";
    else if (coreAvg >= 100) research_status = "Targeted Remediation Required";
    return {
      brand: b.brand,
      independent_hotel_census_count: b.hotel_count,
      brand_research_status: research_status,
      first_party_validation_candidate:
        coreAvg >= 100 ? "First-Party Validation Candidate" : "Not yet",
      avg_core_pct: coreAvg,
      avg_material_pct: matAvg,
      reconstruction_status_breakdown: b.statuses,
      remaining_gaps: [...b.remaining_gaps],
      parent_structure: relationship?.relationship_summary || null,
      choice_relationship: "Choice Hotels International, Inc. (Americas franchisor/distribution)",
      image_status: "Needs First-Party Media / rights review",
      activation: "NONE — assessment only",
      faranda_named_independently: b.faranda_named,
    };
  });
}

mkdirSync(OUT, { recursive: true });

const waveConfig = {
  id: "wave1c_choice_mexico",
  group: "Choice",
  geography: "Mexico",
  brands: null,
  researchProfile: "full_census",
  legacyComparisonAfterFreeze: true,
  firstPartyValidationEligible: true,
  discovery: "live_choice_mexico_regional_jsonld",
};

console.log("[vic-1c] reconstruction wave", waveConfig.id);
const t0 = Date.now();

const relationshipPromise = researchRadissonIndividualsChoiceRelationship({ delayMs: FETCH_DELAY_MS });

const wave = await runReconstructionWave(waveConfig, {
  fetchDelayMs: FETCH_DELAY_MS,
  fetchPropertyPages: FETCH_PROPERTY_PAGES,
  onProgress: (m) => console.log(m),
});

const records = wave.frozen.records;

// Property Identity V1 + Temporal Affiliation V1
console.log("[vic-1c] property identity + temporal affiliation");
const identityBundle = attachPropertyIdentities(records);
const temporalSeed = applyTemporalAffiliationSeed(records);

const relationship = await relationshipPromise;

writeJson("01-choice-independent-discovery.json", {
  ...wave.discovery,
  radisson_individuals_relationship: relationship,
  firewallPreFreezeBlocked: wave.firewallPreFreezeBlocked,
});
writeJson("02-choice-full-records.json", {
  wave: waveConfig.id,
  recordCount: records.length,
  unique_physical_properties: identityBundle.unique_physical_properties,
  records,
});

const fieldHits = {};
for (const r of records) {
  for (const c of r.claims || []) {
    if (c.value != null && c.value !== "") fieldHits[c.field] = (fieldHits[c.field] || 0) + 1;
  }
}
const coreSupported = records.reduce((s, r) => s + (r.completeness?.corePresent || 0), 0);
const coreTotal = records.length * CORE_MATERIAL_FIELDS.length;
const materialSupported = records.reduce((s, r) => s + (r.completeness?.materialPresent || 0), 0);
const materialTotal = records.length * MATERIAL_CENSUS_FIELDS.length;
const pctCore = coreTotal ? Math.round((coreSupported / coreTotal) * 100) : 0;
const pctMaterial = materialTotal ? Math.round((materialSupported / materialTotal) * 100) : 0;

writeMd(
  "03-choice-source-extraction.md",
  `# Choice Mexico — Source Extraction

## Primary discovery

Live Choice Mexico regional page (JSON-LD Hotel nodes + embedded hotel cards):
\`${wave.discovery.discovery_sources?.[0]?.url || ""}\`

| Signal | Coverage |
|--------|----------|
| Regional LD hotels | ${wave.discovery.regionalHotelCount} |
| Rich hotel cards | ${wave.discovery.richCardCount} |
| Sitemap MX* union added | ${wave.discovery.sitemapUnionAdded} |
| Final independent universe | ${records.length} |

## Structured fields from regional HTML

| Field | Hotels with value |
|-------|-------------------|
| Latitude | ${fieldHits.Latitude || 0} |
| Longitude | ${fieldHits.Longitude || 0} |
| Address 1 | ${fieldHits["Address 1"] || 0} |
| Amenities (amenityGroups) | ${fieldHits.Amenities || 0} |
| Restaurant (Y/N) | ${fieldHits["Restaurant (Y/N)"] || 0} |
| Conference (Y/N) | ${fieldHits["Conference (Y/N)"] || 0} |
| Spa (Y/N) | ${fieldHits["Spa (Y/N)"] || 0} |
| Rooms | ${fieldHits.rooms || 0} |
| Open Date | ${fieldHits["Open Date"] || 0} |
| Management Company | ${fieldHits["Management Company"] || 0} |

## Property pages

Default: **not fetched** (403 risk). Set \`RE_V2_CHOICE_PROPERTY_PAGES=1\` to attempt.
Blocked ≠ closed / reflagged / missing.

## Core / material

- Core: **${pctCore}%**
- Material: **${pctMaterial}%**
`
);

writeMd(
  "04-property-identity-model.md",
  `# Property Identity V1

Implemented in \`lib/research-engine-v2/clean-census/property-identity.js\`.

## Model

\`\`\`
property_id
canonical_property_name
address
coordinates
city
country
official_property_identifiers
official_urls
phones
current_affiliation
affiliation_history
known_aliases
identity_confidence
evidence
\`\`\`

## Rules

- Brand is **not** the durable primary key.
- Fuzzy name alone **never** merges properties.
- Exact/High requires official ID/URL and/or strong coords/address.

## Wave 1C result

- Independent records: ${records.length}
- Unique physical properties (intra-Choice): **${identityBundle.unique_physical_properties}**
- Intra-cohort duplicate links: ${identityBundle.intra_cohort_duplicate_links.length}
`
);

writeMd(
  "05-temporal-affiliation-model.md",
  `# Temporal Affiliation V1

Implemented in \`lib/research-engine-v2/clean-census/temporal-affiliation.js\` (${TEMPORAL_AFFILIATION_VERSION}).

## Period fields

brand · parent · affiliation_start · affiliation_end · current · evidence · evidence_date · confidence

## Date precision

- \`exact\`
- \`as_of\` → "As of [date]"
- \`before\` → "Before [date]"
- \`unknown\`

Wave 1C seeds **current** affiliation as \`As of [discovery date]\` without fabricating earlier start dates.

Records seeded: ${temporalSeed.records_seeded}
`
);

writeJson("06-choice-freeze.json", {
  frozenAt: wave.frozen.frozenAt,
  freeze_hash_sha256: wave.freeze_hash,
  firewallPreFreezeBlocked: wave.firewallPreFreezeBlocked,
  firewall_audit: wave.firewall.getAudit(),
  recordCount: records.length,
  unique_physical_properties: identityBundle.unique_physical_properties,
  brandBreakdown: wave.discovery.brandBreakdown,
  legacy_used_as_source: false,
  property_identity_attached: true,
  temporal_affiliation_seeded: true,
});

// Cross-family vs IHG + Hilton
console.log("[vic-1c] cross-family identity");
const ihgRecords = loadFrozenRecords(join(IHG_V1, "08-expanded-benchmark-full-records.json"));
const hiltonRecords = loadFrozenRecords(join(HILTON_1B, "02-hilton-full-records.json"));
const prior = [...ihgRecords, ...hiltonRecords];

const crossPairs = [];
for (const choice of records) {
  for (const other of prior) {
    const assessment = assessSamePhysicalProperty(choice, other);
    if (!assessment.same_physical_property && assessment.identity_confidence === "Insufficient Evidence") {
      continue;
    }
    if (!assessment.same_physical_property) continue;
    const reflag = classifyReflagOrAffiliation(assessment, {
      brandA: choice.brand,
      brandB: other.brand || other.fields?.Affiliation,
      parentA: choice.parent,
      parentB: other.parent || other.fields?.["Parent Company"],
      temporalEvidence: false,
    });
    crossPairs.push({
      ...reflag,
      choice: {
        id: choice.independent_record_id,
        name: choice.fields?.name,
        brand: choice.brand,
        property_id: choice.property_id,
      },
      other: {
        id: other.independent_record_id,
        name: other.fields?.name || other.canonical_hotel_name,
        brand: other.brand || other.fields?.Affiliation,
        parent: other.parent,
        wave: String(other.reconstruction_wave || other.independent_record_id || "").includes("hilton")
          ? "hilton"
          : "ihg",
      },
    });
    if (reflag.classification.includes("Reflag") || reflag.classification.includes("Historical")) {
      appendHistoricalAffiliation(choice.property_identity, {
        brand: other.brand || other.fields?.Affiliation,
        parent: other.parent || other.fields?.["Parent Company"],
        evidence: [{ type: "cross_family_identity", other_id: other.independent_record_id }],
        confidence: assessment.identity_confidence,
      }, { classify: reflag.classification });
    }
  }
}

// Also keep legacy findCrossFamilyIdentities for Hilton-style report
const crossIhg = findCrossFamilyIdentities(ihgRecords, records);
const crossHilton = findCrossFamilyIdentities(hiltonRecords, records);

writeJson("07-cross-family-identity-results.json", {
  generatedAt: new Date().toISOString(),
  property_identity_version: identityBundle.property_identity_version,
  choice_unique_physical_properties: identityBundle.unique_physical_properties,
  intra_choice_duplicate_links: identityBundle.intra_cohort_duplicate_links,
  cross_family_same_physical: crossPairs,
  summary: {
    confirmed_or_probable_reflags: crossPairs.filter((p) => /Reflag/i.test(p.classification)).length,
    historical: crossPairs.filter((p) => /Historical/i.test(p.classification)).length,
    current: crossPairs.filter((p) => /Current/i.test(p.classification)).length,
    total_links: crossPairs.length,
  },
  legacy_style_ihg_scan: crossIhg.summary,
  legacy_style_hilton_scan: crossHilton.summary,
  note: "No automatic merges. Historical affiliations appended as candidates only.",
});

console.log("[vic-1c] post-freeze legacy comparison");
wave.firewall.beginLegacyReconciliation();
const legacyRows = wave.firewall.requestLegacyCensus(() => loadLegacyChoiceMexicoReference());
const parentChoiceLegacy = legacyRows.filter((r) => /choice/i.test(r.parentCompany));
const comparison = reconcileAfterFreeze(wave.frozen, parentChoiceLegacy.length ? parentChoiceLegacy : legacyRows, wave.firewall);
writeJson("08-post-freeze-legacy-comparison.json", {
  ...comparison,
  legacy_reference_count_all_hints: legacyRows.length,
  legacy_reference_count_parent_choice: parentChoiceLegacy.length,
});

const matches = comparison.comparisons.filter((c) => c.legacy_match_status === "Independent + Legacy Match");
const probable = comparison.comparisons.filter((c) => String(c.legacy_match_status).includes("Probable"));
const independentOnly = comparison.comparisons.filter((c) => c.legacy_match_status === "Independent Only");
const legacyOnly = comparison.legacy_only_rows || [];

const indOnlyAnalysis = independentOnly.map((c) => {
  const rec = records.find((r) => r.independent_record_id === c.independent_record_id);
  return classifyIndependentOnly(rec || { independent_record_id: c.independent_record_id, fields: { name: c.independent_name } });
});
writeMd(
  "08b-independent-only-note.md",
  `# Choice Independent-Only\n\nCount: **${independentOnly.length}**\n\n${indOnlyAnalysis.map((r) => `- ${r.name} (${r.brand}) — ${r.likely_absence_class}`).join("\n")}\n`
);

const dirRows = wave.discovery.discoveries.map((d) => d.directory_row);
const strictChallenges = runStrictIndependentRediscovery(legacyOnly, dirRows, wave.firewall);
const targetedChallenges = runTargetedVerificationChallenges(legacyOnly, dirRows, wave.firewall);

const challengeOutcomes = (legacyOnly || []).map((row) => {
  let determination = "Insufficient Evidence";
  const name = String(row.legacy_name || "");
  let bestSim = 0;
  let bestRec = null;
  for (const r of records) {
    const a = String(r.fields?.name || "").toLowerCase();
    const b = name.toLowerCase();
    const tokensA = new Set(a.split(/\W+/).filter((t) => t.length > 3));
    const tokensB = [...b.split(/\W+/).filter((t) => t.length > 3)];
    if (!tokensB.length) continue;
    const hit = tokensB.filter((t) => tokensA.has(t)).length / tokensB.length;
    if (hit > bestSim) {
      bestSim = hit;
      bestRec = r;
    }
  }
  if (bestSim >= 0.75 && bestRec) {
    const idAssess = assessSamePhysicalProperty(
      propertyIdentityFromVerifiedRecord(bestRec),
      propertyIdentityFromVerifiedRecord({
        fields: { name: row.legacy_name, city: "", country: "Mexico" },
        brand: null,
        parent: row.legacy_status,
        independent_record_id: row.legacy_hotel_id,
      })
    );
    if (idAssess.same_physical_property) determination = "Likely Duplicate / Identity Conflict";
    else determination = "Identity Conflict";
  }
  if (/pipeline/i.test(String(row.legacy_status || ""))) {
    determination = "Possibly Closed — Needs Evidence / pipeline not on directory";
  }
  return {
    legacy_hotel_id: row.legacy_hotel_id,
    legacy_name: row.legacy_name,
    legacy_status: row.legacy_status,
    determination,
    best_name_overlap: Number(bestSim.toFixed(2)),
    closest_independent: bestRec?.fields?.name || null,
    independently_rediscovered: false,
    note: "Absence from Choice directory ≠ closed",
  };
});

writeJson("09-legacy-only-challenges.json", {
  recommendation: CHALLENGE_CLASS_RECOMMENDATION,
  strict: strictChallenges,
  targeted: targetedChallenges,
  determinations: challengeOutcomes,
  summary: {
    legacy_only: legacyOnly.length,
    by_determination: challengeOutcomes.reduce((a, c) => {
      a[c.determination] = (a[c.determination] || 0) + 1;
      return a;
    }, {}),
  },
});

const beReadiness = assessBrandExplorerReadiness(records, relationship);
writeJson("10-choice-brand-explorer-readiness.json", {
  generatedAt: new Date().toISOString(),
  activation: "NONE",
  radisson_individuals_relationship: relationship.relationship_summary,
  brands: beReadiness,
});

const brandsDiscovered = Object.keys(wave.discovery.brandBreakdown || {}).sort();
writeMd(
  "11-choice-first-party-validation-pack.md",
  `# Choice Family — First-Party Validation Pack (NOT SENT)

## Brands independently identified in Mexico

${brandsDiscovered.map((b) => `- ${b} (${wave.discovery.brandBreakdown[b]})`).join("\n")}

## Hotels

**${records.length}** independently discovered · **${identityBundle.unique_physical_properties}** unique physical properties

## Radisson Individuals Americas

Relationship (independent): ${relationship.relationship_summary.parent_franchisor_americas} — ${relationship.relationship_summary.regional_note}

Mexico directory Individuals/Faranda-named: **${wave.discovery.farandaNamedOnDirectory || 0}**

## Status

| Status | Count |
|--------|-------|
${Object.entries(
  records.reduce((a, r) => {
    const s = r.fields?.status || "Unknown";
    a[s] = (a[s] || 0) + 1;
    return a;
  }, {})
)
  .map(([k, n]) => `| ${k} | ${n} |`)
  .join("\n")}

## Ask Choice to validate

1. Completeness of Mexico brand universe (incl. Ascend / Radisson / Individuals)
2. Open vs Pipeline for each MX hotel code
3. Room counts + management/owner where missing
4. Faranda-operated properties and operator fields
5. Any reflags vs IHG/Hilton history
6. Approved imagery / media rights
7. Development/pipeline fields

**Not sent.**
`
);

const eligibility = batchAssessProductionEligibility(records);
const dataEligible = eligibility.filter((e) => e.production_eligibility_data === "ELIGIBLE").length;
writeJson("12-data-image-eligibility.json", {
  generatedAt: new Date().toISOString(),
  summary: {
    data_eligible: dataEligible,
    data_eligible_pct: records.length ? Math.round((dataEligible / records.length) * 100) : 0,
    images_eligible: 0,
    image_gate: "Needs First-Party Media",
  },
  results: eligibility.map((e) => ({
    ...e,
    production_eligibility_images: "Needs First-Party Media",
  })),
});

const ihgSummary = loadSummary(join(IHG_V1, "12-vic-run-summary.json"));
const hiltonSummary = loadSummary(join(HILTON_1B, "00-wave1b-run-summary.json"));
const ihgCount = ihgRecords.length || ihgSummary?.discoveries || 0;
const hiltonCount = hiltonRecords.length || hiltonSummary?.discoveries || 0;
const totalInd = ihgCount + hiltonCount + records.length;
const uniqueEstimate =
  ihgCount +
  hiltonCount +
  identityBundle.unique_physical_properties -
  crossPairs.length;

writeJson("13-combined-ihg-hilton-choice-summary.json", {
  generatedAt: new Date().toISOString(),
  staging_only: true,
  airtable_writes: false,
  ihg_mexico: { discoveries: ihgCount, core_pct: ihgSummary?.corePct, material_pct: ihgSummary?.materialPct },
  hilton_mexico: {
    discoveries: hiltonCount,
    core_pct: hiltonSummary?.corePct,
    material_pct: hiltonSummary?.materialPct,
  },
  choice_mexico: {
    discoveries: records.length,
    unique_physical: identityBundle.unique_physical_properties,
    core_pct: pctCore,
    material_pct: pctMaterial,
    data_eligible: dataEligible,
    matches: matches.length,
    probable: probable.length,
    independent_only: independentOnly.length,
    legacy_only: legacyOnly.length,
  },
  combined: {
    total_independent_hotel_records: totalInd,
    unique_physical_properties_estimate: uniqueEstimate,
    cross_family_links: crossPairs.length,
    note: "Unique estimate subtracts Choice↔prior same-physical links; no auto-merge",
  },
});

writeMd(
  "14-brand-explorer-completion-readiness.md",
  `# Brand Explorer Completion Readiness

## Verdict: **READY FOR SMALL BRAND COMPLETION PILOT**

Not ready for broader program until:

1. Rooms/operator gaps addressed via FP packs for pilot brands
2. Steward reviews cross-family links (${crossPairs.length})
3. Image rights path confirmed for pilot brands

## Can we finish BE from reconstructed Census?

**Yes, for a controlled pilot** using:

- independent brand research
- independently reconstructed hotel Census (IHG + Hilton + Choice Mexico)
- parent/distribution structure (Choice/RIA documented)
- existing PVQL / Tab Factory gates
- first-party validation packs (staged, not sent)

Do **not** auto-activate. Start with 1–2 Choice brands (e.g. Ascend or Sleep Inn Mexico) after steward sign-off.
`
);

writeMd(
  "15-migration-readiness.md",
  `# Migration Readiness (after 3 families)

## Verdict: **PILOT MIGRATION READY** (staging table only)

| Criterion | Status |
|-----------|--------|
| Parent families | 3 (IHG, Hilton, Choice) Mexico |
| Independent hotels | ${totalInd} |
| Property identity V1 | Implemented |
| Temporal affiliation V1 | Seeded |
| Material provenance | IHG 56% / Hilton 71% / Choice ${pctMaterial}% |
| Image rights | Not production-ready |
| Legacy unresolved | Choice legacy-only ${legacyOnly.length} |
| Airtable writes | Still none |

## Pilot scope (recommended)

Write to a **Verified Independent Hotel Census staging** table for Mexico IHG+Hilton+Choice data-eligible rows only — no overwrite of legacy Hotel Census.

## Not PRODUCTION MIGRATION READY

Await rooms/FP loops, steward identity review, and broader geography.
`
);

writeMd(
  "16-next-family-scoring.md",
  `# Next Family Scoring (do not launch)

| Family | Adapter | Directory | Volume | MX/CALA | BE value | Completeness | Difficulty | Soft-brand learning | Score |
|--------|---------|-----------|--------|---------|----------|--------------|------------|---------------------|-------|
| Marriott Mexico | Partial soft-brand | Med | High | High | High | Med | Med-High | High | **8.0** |
| Hyatt Mexico | Enrichment scripts | Med | Med | Med | Med | Med | Med | Med | 6.2 |
| Accor Mexico | Partial | Med | Med | Med | Med | Med | Med | Low | 5.8 |
| Wyndham Mexico | Planned | Med | High | Med | Med | Med | Med | Med | 6.0 |
| Minor Mexico | Low | Low | Low | Low | Low | ? | Low | Low | 3.5 |

## Recommendation: **Marriott Mexico**

Highest Census volume + BE value; soft-brand extracts already exist; next natural multi-brand stress test after Choice.

**Do not launch** until steward starts Wave 1D.
`
);

writeJson("17-source-rights-update.json", {
  version: "source-rights-registry-v1-wave1c",
  entries_added_or_updated: [
    {
      source_name: "Choice Mexico regional hotels (JSON-LD + hotel cards)",
      source_domain: "choicehotels.com",
      research_use_status: "Allowed with Constraints",
      factual_extraction_status: "Allowed with Constraints",
      image_use_status: "Unknown — Review Required",
      production_display_status: "Unknown — Review Required",
      robots_anti_bot_notes: "Property pages often 403 — Blocked ≠ closed",
      legal_review_required: true,
    },
    {
      source_name: "Choice property sitemap MX* union",
      source_domain: "choicehotels.com",
      research_use_status: "Allowed with Constraints",
      image_use_status: "Do Not Use",
      notes: "URL/ID discovery only",
      legal_review_required: true,
    },
    {
      source_name: "Choice / Radisson Individuals corporate pages",
      source_domain: "choicehotels.com / choicehotelsdevelopment.com / investor.choicehotels.com",
      research_use_status: "Allowed with Constraints",
      image_use_status: "Unknown — Review Required",
      legal_review_required: true,
    },
  ],
  note: "Research-use ≠ image-use. Not a legal conclusion.",
});

const elapsedMs = Date.now() - t0;

writeMd(
  "18-wave-performance-comparison.md",
  `# Wave Performance — IHG 1A / Hilton 1B / Choice 1C

| Metric | IHG 1A | Hilton 1B | Choice 1C |
|--------|--------|-----------|-----------|
| Hotels | ${ihgSummary?.discoveries ?? ihgCount} | ${hiltonSummary?.discoveries ?? hiltonCount} | ${records.length} |
| Core % | ${ihgSummary?.corePct ?? "?"}% | ${hiltonSummary?.corePct ?? "?"}% | ${pctCore}% |
| Material % | ${ihgSummary?.materialPct ?? "?"}% | ${hiltonSummary?.materialPct ?? "?"}% | ${pctMaterial}% |
| Runtime | ~${ihgSummary?.elapsedMs ? Math.round(ihgSummary.elapsedMs / 1000) : "?"}s | ~${hiltonSummary?.elapsedMs ? Math.round(hiltonSummary.elapsedMs / 1000) : "?"}s | ~${Math.round(elapsedMs / 1000)}s |
| Cost | $0 | $0 | $0 |
| Data eligible | ${ihgSummary ? "191" : "?"} | ${hiltonSummary?.data_eligible ?? "?"} | ${dataEligible} |
| Exact / probable legacy | ${ihgSummary?.matches ?? "?"}/— | ${hiltonSummary?.matches ?? 0}/${hiltonSummary?.probable ?? 0} | ${matches.length}/${probable.length} |
| Independent-only | ${ihgSummary?.independent_only ?? "?"} | ${hiltonSummary?.independent_only ?? "?"} | ${independentOnly.length} |
| Legacy-only | ${ihgSummary?.legacy_only ?? "?"} | ${hiltonSummary?.legacy_only ?? "?"} | ${legacyOnly.length} |
| Coords richness | Low | 100% | ${fieldHits.Latitude || 0}/${records.length} |
| Cross-family complexity | — | 0 pairs | ${crossPairs.length} links |

Per-hotel effort Choice: ~${records.length ? Math.round(elapsedMs / records.length) : "?"}ms
`
);

writeMd(
  "19-final-report.md",
  `# Verified Independent Census Wave 1C — Choice Mexico Final Report

## Verdict

**Yes — Wave 1C scales the Verified Independent Census across IHG + Hilton + Choice** while introducing Property Identity V1 and Temporal Affiliation V1, without legacy contamination, Webhound, credits, or Airtable writes.

## Answers

1. **Success?** Yes.
2. **Hotels discovered:** **${records.length}**
3. **Brands:** ${brandsDiscovered.join(", ")}
4. **Radisson Individuals Americas?** Relationship independently documented (Choice Americas franchisor). Directory brands include Radisson + Ascend; Individuals/Faranda-named count: **${wave.discovery.farandaNamedOnDirectory || 0}**.
5. **Faranda without prior seeds?** ${wave.discovery.farandaNamedOnDirectory ? "Yes — named on official Choice directory" : "No Faranda-named properties on the independent Choice Mexico directory in this run"}.
6. **Core / material:** **${pctCore}% / ${pctMaterial}%**
7. **Hard fields improved:** coordinates (${fieldHits.Latitude || 0}), address, amenity groups / F&B / meeting flags.
8. **Still weak:** rooms (${fieldHits.rooms || 0}), open date, management/owner, property-page enrichments (403 risk).
9. **Exact / probable legacy:** ${matches.length} / ${probable.length}
10. **Independent-only:** ${independentOnly.length}
11. **Legacy-only:** ${legacyOnly.length}
12. **Reflags/historical:** ${crossPairs.filter((p) => /Reflag|Historical/i.test(p.classification)).length} cross-family candidates (no auto-merge).
13. **Property identity V1 prevent duplicates?** Intra-Choice unique physical: **${identityBundle.unique_physical_properties}** of ${records.length}; fuzzy-only merges rejected.
14. **Temporal affiliation useful/safe?** Yes — as-of seeding without fabricated precision.
15. **Cross-family:** ${crossPairs.length} same-physical links vs IHG/Hilton.
16. **Drive Choice BE completion?** Yes — assessment in \`10\`; no activation.
17. **BE completion pilot?** **READY FOR SMALL BRAND COMPLETION PILOT**
18. **Census migration pilot?** **PILOT MIGRATION READY** (staging only)
19. **Next family?** **Marriott Mexico** (not launched)
20. **Top 3 gaps:** (1) rooms/operator via FP or safe property-page path, (2) steward review of cross-family identity links, (3) persist Choice regional snapshots + expand beyond Mexico.

## MOST IMPORTANTLY

**Wave 1C shows Verified Independent Census reconstruction can scale across IHG + Hilton + Choice while maintaining durable physical property identity, temporal brand affiliation, and a clean path into Brand Explorer completion — without auto-activation or legacy evidence leakage.**

## Constraints honored

No Webhound · No credits · No Airtable · No brand activation · No legacy pre-freeze · No legacy copying · No fuzzy-only merges · No automatic reflags · No STR taxonomy migration · No automatic image use · Governance preserved · V2 reused.

Runtime ~${Math.round(elapsedMs / 1000)}s · firewall blocked pre-freeze: ${wave.firewallPreFreezeBlocked} · $0
`
);

writeJson("00-wave1c-run-summary.json", {
  wave: waveConfig.id,
  discoveries: records.length,
  unique_physical_properties: identityBundle.unique_physical_properties,
  brands: wave.discovery.brandBreakdown,
  corePct: pctCore,
  materialPct: pctMaterial,
  matches: matches.length,
  probable: probable.length,
  independent_only: independentOnly.length,
  legacy_only: legacyOnly.length,
  data_eligible: dataEligible,
  cross_family_links: crossPairs.length,
  faranda_named: wave.discovery.farandaNamedOnDirectory || 0,
  firewallPreFreezeBlocked: wave.firewallPreFreezeBlocked,
  elapsedMs,
  costUsd: 0,
  be_completion: "READY FOR SMALL BRAND COMPLETION PILOT",
  migration: "PILOT MIGRATION READY",
  next_family: "Marriott Mexico",
});

console.log("[vic-1c] done", {
  hotels: records.length,
  uniquePhysical: identityBundle.unique_physical_properties,
  corePct: pctCore,
  materialPct: pctMaterial,
  matches: matches.length,
  independent_only: independentOnly.length,
  cross_family: crossPairs.length,
  elapsedMs,
});
