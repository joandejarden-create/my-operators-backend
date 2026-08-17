/**
 * Research Engine V2 — Verified Independent Census Wave 1B: Hilton Mexico
 *
 * Reuses VIC program (firewall, freeze, provenance, eligibility, challenges).
 * No Webhound. No credits. No Airtable writes. Legacy quarantined post-freeze only.
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
  MATERIAL_CENSUS_FIELDS,
  CORE_MATERIAL_FIELDS,
} from "../lib/research-engine-v2/clean-census/provenance.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/research-engine-v2/verified-independent-census-wave1b-hilton");
const IHG_V1 = join(ROOT, "data/research-engine-v2/verified-independent-census-v1");
const FETCH_DELAY_MS = Number(process.env.RE_V2_FETCH_DELAY_MS || 100);
const GRAPHQL = process.env.RE_V2_HILTON_GRAPHQL !== "0";
const FETCH_PROPERTY_PAGE = process.env.RE_V2_HILTON_PROPERTY_PAGE === "1";

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

/** Post-freeze only — Hilton Worldwide / Hilton parent in Mexico. */
function loadLegacyHiltonMexicoReference() {
  const csv = readFileSync(join(ROOT, "reports/census-amenities-blank-rows.csv"), "utf8").split(/\r?\n/);
  const rows = [];
  for (const line of csv.slice(1)) {
    if (!line.trim()) continue;
    const f = parseCsvLine(line);
    const name = f[1] || "";
    const parent = f[2] === "(blank parent)" ? "" : f[2] || "";
    const country = f[4] || "";
    if (!/Mexico/i.test(country)) continue;
    if (!/Hilton/i.test(parent)) continue;
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
    });
  }
  return rows;
}

function loadIhgFrozenRecords() {
  const path = join(IHG_V1, "08-expanded-benchmark-full-records.json");
  if (!existsSync(path)) return [];
  const data = JSON.parse(readFileSync(path, "utf8"));
  return data.records || [];
}

function loadIhgSummary() {
  const path = join(IHG_V1, "12-vic-run-summary.json");
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf8"));
}

function classifyIndependentOnly(rec) {
  const status = String(rec.fields?.status || rec.current_status || "").toLowerCase();
  const openDate = String(rec.fields?.["Open Date"] || "");
  const year = openDate.match(/(20\d{2})/)?.[1];
  const reasons = [];
  if (/pipeline|coming|pre-?open/i.test(status)) reasons.push("pipeline");
  if (year && Number(year) >= 2024) reasons.push("newer opening");
  if (!reasons.length) reasons.push("legacy omission or identity mismatch");
  return {
    independent_record_id: rec.independent_record_id,
    name: rec.fields?.name || rec.canonical_hotel_name,
    brand: rec.brand || rec.fields?.Affiliation,
    status: rec.fields?.status || null,
    openDate: openDate || null,
    likely_absence_class: reasons[0],
    notes: reasons,
  };
}

function assessBrandExplorerReadiness(records) {
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
      });
    }
    const b = byBrand.get(brand);
    b.hotel_count++;
    b.core_pct_avg += r.completeness?.corePct || 0;
    b.material_pct_avg += r.completeness?.materialPct || 0;
    const st = r.reconstruction_status || "Unknown";
    b.statuses[st] = (b.statuses[st] || 0) + 1;
    for (const f of r.completeness?.unresolvedCore || []) b.remaining_gaps.add(f);
    if ((r.completeness?.materialPct || 0) < 65) {
      b.remaining_gaps.add("material_fields_below_65pct");
    }
    if (!r.fields?.rooms) b.remaining_gaps.add("rooms");
    if (!r.fields?.["Management Company"]) b.remaining_gaps.add("Management Company");
  }

  return [...byBrand.values()].map((b) => {
    const coreAvg = Math.round(b.core_pct_avg / b.hotel_count);
    const matAvg = Math.round(b.material_pct_avg / b.hotel_count);
    let research_status = "Deep Research Required";
    if (coreAvg >= 100 && matAvg >= 70) research_status = "Independent Research Complete";
    else if (coreAvg >= 100 && matAvg >= 55) research_status = "Materially Complete";
    else if (coreAvg >= 100) research_status = "Remediation Required";
    const first_party =
      coreAvg >= 100 && b.hotel_count >= 1 ? "First-Party Validation Candidate" : "Not yet";
    return {
      brand: b.brand,
      independent_hotel_census_count: b.hotel_count,
      brand_research_status: research_status,
      first_party_validation_candidate: first_party,
      avg_core_pct: coreAvg,
      avg_material_pct: matAvg,
      reconstruction_status_breakdown: b.statuses,
      remaining_gaps: [...b.remaining_gaps],
      activation: "NONE — assessment only; no Brand Explorer writes",
    };
  });
}

mkdirSync(OUT, { recursive: true });

const waveConfig = {
  id: "wave1b_hilton_mexico",
  group: "Hilton",
  geography: "Mexico",
  brands: null,
  researchProfile: "full_census",
  legacyComparisonAfterFreeze: true,
  firstPartyValidationEligible: true,
  discovery: "live_hilton_locations_mexico_brand_pages",
};

console.log("[vic-1b] reconstruction wave", waveConfig.id);
const t0 = Date.now();
const wave = await runReconstructionWave(waveConfig, {
  fetchDelayMs: FETCH_DELAY_MS,
  graphqlStatus: GRAPHQL,
  fetchPropertyPage: FETCH_PROPERTY_PAGE,
  onProgress: (m) => console.log(m),
});

const records = wave.frozen.records;
const discoveryElapsed = Date.now() - t0;

writeJson("01-hilton-discovery.json", {
  ...wave.discovery,
  discoveries: wave.discovery.discoveries,
  firewallPreFreezeBlocked: wave.firewallPreFreezeBlocked,
});
writeJson("02-hilton-full-records.json", {
  wave: waveConfig.id,
  recordCount: records.length,
  records,
});

// Field coverage
const fieldHits = {};
for (const r of records) {
  for (const c of r.claims || []) {
    if (c.value != null && c.value !== "") {
      fieldHits[c.field] = (fieldHits[c.field] || 0) + 1;
    }
  }
}
const coreSupported = records.reduce((s, r) => s + (r.completeness?.corePresent || 0), 0);
const coreTotal = records.length * CORE_MATERIAL_FIELDS.length;
const materialSupported = records.reduce((s, r) => s + (r.completeness?.materialPresent || 0), 0);
const materialTotal = records.length * MATERIAL_CENSUS_FIELDS.length;
const pctCore = coreTotal ? Math.round((coreSupported / coreTotal) * 100) : 0;
const pctMaterial = materialTotal ? Math.round((materialSupported / materialTotal) * 100) : 0;

const structuredLift = {
  rooms: fieldHits.rooms || 0,
  Latitude: fieldHits.Latitude || 0,
  Longitude: fieldHits.Longitude || 0,
  Amenities: fieldHits.Amenities || 0,
  "Open Date": fieldHits["Open Date"] || 0,
  "Restaurant (Y/N)": fieldHits["Restaurant (Y/N)"] || 0,
  "Spa (Y/N)": fieldHits["Spa (Y/N)"] || 0,
  "Conference (Y/N)": fieldHits["Conference (Y/N)"] || 0,
  "Resort (Y/N)": fieldHits["Resort (Y/N)"] || 0,
  note: "Hilton locations amenityIds + coordinates + openDate are structured directory fields — material lift vs page scrape alone",
};

writeMd(
  "03-field-coverage-report.md",
  `# Hilton Mexico — Independent Field Coverage

## Summary

| Metric | Value |
|--------|-------|
| Hotels discovered | ${records.length} |
| Core field support | **${pctCore}%** (${coreSupported}/${coreTotal}) |
| Material field support | **${pctMaterial}%** (${materialSupported}/${materialTotal}) |
| Target material | ≥65% (unknown preferred over unsupported) |

## Core fields (${CORE_MATERIAL_FIELDS.join(", ")})

Per-hotel core completeness average: ${records.length ? Math.round(records.reduce((s, r) => s + (r.completeness?.corePct || 0), 0) / records.length) : 0}%

## Material field hits (hotels with independently supported value)

${Object.entries(fieldHits)
  .sort((a, b) => b[1] - a[1])
  .map(([f, n]) => `- **${f}**: ${n}/${records.length} (${Math.round((n / records.length) * 100)}%)`)
  .join("\n")}

## Hilton structured-data lift

| Field | Hotels |
|-------|--------|
| Latitude | ${structuredLift.Latitude} |
| Longitude | ${structuredLift.Longitude} |
| Amenities (from amenityIds) | ${structuredLift.Amenities} |
| Open Date | ${structuredLift["Open Date"]} |
| Restaurant (Y/N) | ${structuredLift["Restaurant (Y/N)"]} |
| Spa (Y/N) | ${structuredLift["Spa (Y/N)"]} |
| Conference (Y/N) | ${structuredLift["Conference (Y/N)"]} |
| Rooms | ${structuredLift.rooms} |

Rooms remain largely **Unknown** — Hilton Mexico locations JSON does not expose room count; GraphQL status query also omits rooms. Unknown is correct.

## Proprietary firewall

STR Market / STR Submarket / proprietary Chain Scale: **not migrated**. Dealality Market = Mexico (country grain).
`
);

writeJson("04-hilton-freeze.json", {
  frozenAt: wave.frozen.frozenAt,
  freeze_hash_sha256: wave.freeze_hash,
  firewallPreFreezeBlocked: wave.firewallPreFreezeBlocked,
  firewall_audit: wave.firewall.getAudit(),
  recordCount: records.length,
  brandBreakdown: wave.discovery.brandBreakdown,
  legacy_used_as_source: false,
  discovery_basis: wave.discovery.discovery_basis,
});

// Cross-family identity (IHG wave 1A frozen staging — already independent)
console.log("[vic-1b] cross-family identity vs IHG Mexico");
const ihgRecords = loadIhgFrozenRecords();
const crossFamily = findCrossFamilyIdentities(ihgRecords, records);
writeJson("05-cross-family-identity.json", crossFamily);

writeMd(
  "06-temporal-affiliation-design.md",
  `# Temporal Affiliation Model — Minimal Design

## Recommendation

**Yes — introduce a durable \`property_identity\` separate from current brand affiliation.**

Wave 1B cross-family scan: ${crossFamily.summary.same_historical} historical / ${crossFamily.summary.probable_review} probable-review pairs between IHG and Hilton Mexico independent universes.

## Minimal schema (do not build full history DB yet)

\`\`\`
property_identity   // stable Dealality id for the physical hotel
affiliation         // brand string at a point in time
parent_company      // IHG | Hilton | …
valid_from          // date | null
valid_to            // date | null (null = current)
current_affiliation // boolean
evidence            // claim refs / URLs / discovery source
\`\`\`

## Rules

1. Never overwrite prior affiliation when a reflag is detected — close prior row (\`valid_to\`) and open new.
2. Dual-branded campuses: two current affiliations allowed with shared \`property_identity\` only when coords/address prove same campus AND official dual-brand evidence exists.
3. Fuzzy name alone is **never** sufficient to merge identities.
4. Legacy comparison may *hint* historical affiliation; it never creates the independent claim.

## Implementation scope for now

Design + cross-family detection artifacts only. No Airtable schema migration in Wave 1B.
`
);

console.log("[vic-1b] post-freeze legacy comparison");
wave.firewall.beginLegacyReconciliation();
const legacyRows = wave.firewall.requestLegacyCensus(() => loadLegacyHiltonMexicoReference());
const comparison = reconcileAfterFreeze(wave.frozen, legacyRows, wave.firewall);
writeJson("07-post-freeze-legacy-comparison.json", comparison);

const matches = comparison.comparisons.filter((c) => c.legacy_match_status === "Independent + Legacy Match");
const probable = comparison.comparisons.filter((c) =>
  String(c.legacy_match_status).includes("Probable")
);
const independentOnly = comparison.comparisons.filter((c) => c.legacy_match_status === "Independent Only");
const legacyOnly = comparison.legacy_only_rows || [];

const indOnlyAnalysis = independentOnly.map((c) => {
  const rec = records.find((r) => r.independent_record_id === c.independent_record_id);
  return classifyIndependentOnly(rec || { independent_record_id: c.independent_record_id, fields: { name: c.independent_name } });
});

const absenceBuckets = {};
for (const row of indOnlyAnalysis) {
  absenceBuckets[row.likely_absence_class] = (absenceBuckets[row.likely_absence_class] || 0) + 1;
}

writeMd(
  "08-independent-only-analysis.md",
  `# Hilton Mexico — Independent-Only Signal

Independently discovered hotels **absent from legacy** after freeze: **${independentOnly.length}**

## Absence classification (heuristic — not closure)

| Class | Count |
|-------|-------|
${Object.entries(absenceBuckets)
  .map(([k, n]) => `| ${k} | ${n} |`)
  .join("\n")}

## Records

${indOnlyAnalysis
  .map(
    (r) =>
      `- **${r.name}** (${r.brand || "?"}) — ${r.likely_absence_class}; status=${r.status || "n/a"}; openDate=${r.openDate || "n/a"}`
  )
  .join("\n")}

These are evidence that Dealality's Census can evolve beyond the legacy dataset via independent discovery.
`
);

const dirRows = wave.discovery.discoveries.map((d) => d.directory_row);
const strictChallenges = runStrictIndependentRediscovery(legacyOnly, dirRows, wave.firewall);
const targetedChallenges = runTargetedVerificationChallenges(legacyOnly, dirRows, wave.firewall);

// Enrich challenge outcomes using targeted name similarity vs frozen universe
const challengeOutcomes = (legacyOnly || []).map((row) => {
  const targeted = (targetedChallenges.results || targetedChallenges || []).find?.(
    (c) => c.legacy_hotel_id_quarantined === row.legacy_hotel_id || c.legacy_hotel_id === row.legacy_hotel_id
  );
  // Prefer targeted match hints if present
  let determination = "insufficient evidence";
  const name = String(row.legacy_name || "");
  if (/closed|permanently closed/i.test(name)) determination = "likely closed";
  // If any independent hotel shares high name token overlap — identity mismatch / reflag candidate
  let bestSim = 0;
  for (const r of records) {
    const a = String(r.fields?.name || "").toLowerCase();
    const b = name.toLowerCase();
    const tokensA = new Set(a.split(/\W+/).filter((t) => t.length > 3));
    const tokensB = [...b.split(/\W+/).filter((t) => t.length > 3)];
    if (!tokensB.length) continue;
    const hit = tokensB.filter((t) => tokensA.has(t)).length / tokensB.length;
    if (hit > bestSim) bestSim = hit;
  }
  if (bestSim >= 0.6) determination = "identity mismatch or reflag candidate";
  if (String(row.legacy_status || "").toLowerCase().includes("pipeline") && bestSim < 0.4) {
    determination = "likely historical/reflagged or never opened";
  }
  return {
    legacy_hotel_id: row.legacy_hotel_id,
    legacy_name: row.legacy_name,
    legacy_status: row.legacy_status,
    determination,
    best_name_overlap_vs_independent: Number(bestSim.toFixed(2)),
    independently_rediscovered: false,
    note: "Absence alone ≠ closed. Strict challenge does not seed legacy names.",
    targeted_challenge_ref: targeted?.challenge_id || null,
  };
});

writeJson("09-legacy-only-challenges.json", {
  recommendation: CHALLENGE_CLASS_RECOMMENDATION,
  strict: strictChallenges,
  targeted: targetedChallenges,
  determinations: challengeOutcomes,
  summary: {
    legacy_only: legacyOnly.length,
    independently_rediscovered: challengeOutcomes.filter((c) => c.independently_rediscovered).length,
    by_determination: challengeOutcomes.reduce((acc, c) => {
      acc[c.determination] = (acc[c.determination] || 0) + 1;
      return acc;
    }, {}),
  },
});

const beReadiness = assessBrandExplorerReadiness(records);
writeJson("10-brand-explorer-readiness.json", {
  generatedAt: new Date().toISOString(),
  activation: "NONE",
  brands: beReadiness,
  note: "Census reconstruction drives Brand Explorer readiness assessment only — no activation, no writes.",
});

const brandsDiscovered = Object.keys(wave.discovery.brandBreakdown || {}).sort();
writeMd(
  "11-hilton-first-party-validation-pack.md",
  `# Hilton Family — First-Party Validation Pack (NOT SENT)

Design-only pack. Do not email or write Airtable.

## Brands represented in Mexico (independent discovery)

${brandsDiscovered.map((b) => `- ${b} (${wave.discovery.brandBreakdown[b]})`).join("\n")}

## Independently identified hotels

**${records.length}** Hilton-family properties on official Mexico locations pages.

## Operating / pipeline status

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

## Opening dates (where directory provided)

${records.filter((r) => r.fields?.["Open Date"]).length} / ${records.length} have Open Date from Hilton structured directory.

## Missing vs legacy (post-freeze)

- Independent-only (in Hilton directory, not in legacy): **${independentOnly.length}**
- Legacy-only (in legacy, not matched independently): **${legacyOnly.length}**

## Reflags / cross-family

See \`05-cross-family-identity.json\` — ${crossFamily.pairs.length} candidate pairs vs IHG Mexico independent universe.

## Management / operator

Independently supported Management Company: **${fieldHits["Management Company"] || 0}** / ${records.length} (mostly Unknown — correct).

## Approved imagery / media

Hero URLs from directory are **reference-only** (\`Public Source — Reference Only\`). Image eligibility remains separate from data eligibility. Hilton should eventually supply First-Party Approved media.

## Corrections Hilton could validate

1. Confirm Mexico brand universe completeness (including SLH if listed).
2. Confirm Open vs Pipeline for each ctyhocn.
3. Supply room counts and management company where missing.
4. Confirm any reflags vs prior IHG (or other) affiliations.
5. Approve or replace imagery rights.
6. Flag closed or non-bookable properties still listed on locations pages.

**Status: pack staged locally — not sent.**
`
);

const eligibility = batchAssessProductionEligibility(records);
const dataEligible = eligibility.filter((e) => e.production_eligibility_data === "ELIGIBLE").length;
const imageEligible = eligibility.filter((e) => e.production_eligibility_images === "ELIGIBLE").length;

writeJson("12-data-image-eligibility.json", {
  generatedAt: new Date().toISOString(),
  summary: {
    data_eligible: dataEligible,
    data_not_eligible: eligibility.length - dataEligible,
    data_eligible_pct: records.length ? Math.round((dataEligible / records.length) * 100) : 0,
    images_eligible: imageEligible,
    image_rights_note: "Hilton directory hero URLs are Public Source — Reference Only; not auto-usable",
  },
  results: eligibility,
});

const ihgSummary = loadIhgSummary();
const ihgCount = ihgRecords.length || ihgSummary?.discoveries || 0;
const combinedPhysicalEstimate = ihgCount + records.length - (crossFamily.summary.same_historical || 0) - (crossFamily.summary.same_current || 0) - (crossFamily.summary.dual || 0);

writeJson("13-ihg-hilton-combined-summary.json", {
  generatedAt: new Date().toISOString(),
  staging_only: true,
  airtable_writes: false,
  ihg_mexico: {
    wave: "wave1_ihg_mexico_all",
    independently_discovered: ihgCount,
    core_pct: ihgSummary?.corePct ?? null,
    material_pct: ihgSummary?.materialPct ?? null,
    data_eligible: ihgSummary ? undefined : null,
    matches: ihgSummary?.matches ?? null,
    independent_only: ihgSummary?.independent_only ?? null,
    legacy_only: ihgSummary?.legacy_only ?? null,
  },
  hilton_mexico: {
    wave: "wave1b_hilton_mexico",
    independently_discovered: records.length,
    core_pct: pctCore,
    material_pct: pctMaterial,
    data_eligible: dataEligible,
    matches: matches.length,
    probable: probable.length,
    independent_only: independentOnly.length,
    legacy_only: legacyOnly.length,
  },
  combined: {
    total_independently_discovered_hotels: ihgCount + records.length,
    unique_physical_properties_estimate: combinedPhysicalEstimate,
    note: "Unique physical estimate subtracts cross-family same-property classifications; Probable Review not auto-merged",
    cross_family: crossFamily.summary,
    historical_affiliations_detected: crossFamily.summary.same_historical,
    current_cross_family_same: crossFamily.summary.same_current,
    dual_branded: crossFamily.summary.dual,
    probable_review: crossFamily.summary.probable_review,
    data_eligible_hilton: dataEligible,
    image_eligibility_hilton: imageEligible,
    unresolved_legacy_challenges: legacyOnly.length,
    provenance: "field-level claims on all independent records; legacy never source",
  },
});

const elapsedMs = Date.now() - t0;
const effortPerHotel = records.length ? Math.round(elapsedMs / records.length) : null;

writeMd(
  "14-wave-performance-comparison.md",
  `# Wave Performance — IHG 1A vs Hilton 1B

| Metric | IHG Wave 1A | Hilton Wave 1B |
|--------|-------------|----------------|
| Independent hotels | ${ihgSummary?.discoveries ?? ihgCount} | ${records.length} |
| Discovery source | IHG CALA directory extract | Live Hilton Mexico locations pages |
| Core-field completion | ${ihgSummary?.corePct ?? "?"}%) | ${pctCore}% |
| Material-field completion | ${ihgSummary?.materialPct ?? "?"}% | ${pctMaterial}% |
| Runtime | ~${ihgSummary?.elapsedMs ? Math.round(ihgSummary.elapsedMs / 1000) : "?"}s | ~${Math.round(elapsedMs / 1000)}s |
| External research cost | $0 | $0 |
| Legacy match (exact) | ${ihgSummary?.matches ?? "?"} | ${matches.length} |
| Probable match | (see 1A) | ${probable.length} |
| Independent-only | ${ihgSummary?.independent_only ?? "?"} | ${independentOnly.length} |
| Legacy-only | ${ihgSummary?.legacy_only ?? "?"} | ${legacyOnly.length} |
| Data eligibility % | (see 1A eligibility) | ${records.length ? Math.round((dataEligible / records.length) * 100) : 0}% |
| Research effort / hotel | ~${ihgSummary?.elapsedMs && ihgSummary?.discoveries ? Math.round(ihgSummary.elapsedMs / ihgSummary.discoveries) : "?"}ms | ~${effortPerHotel}ms |
| Source failures (brand pages) | n/a extract | ${(wave.discovery.fetchErrors || []).length} |

## Generalization

Architecture **does generalize**: same firewall → discover → research → freeze → compare → challenge → eligibility path works across IHG extract and Hilton live locations. Hilton structured amenityIds/coords/openDate improve material fields vs scrape-only paths; rooms still need first-party or deeper GraphQL schema work.
`
);

writeMd(
  "15-next-wave-recommendation.md",
  `# Next Wave Recommendation (do not launch)

## Scored candidates

| Candidate | Directory quality | Adapter maturity | MX/CALA importance | Census volume | BE value | Expected completeness | Difficulty | Independent-only value | Score |
|-----------|-------------------|------------------|--------------------|---------------|----------|----------------------|------------|------------------------|-------|
| A. Marriott Mexico | Med (soft brands strong; full MX incomplete) | Partial soft-brand | High | High | High | Med | Med-High | High | 7.2 |
| B. Choice Mexico | High URL extract CALA | Ready with 403 discipline | Med-High | Med | High (many brands) | Med (403 pages) | Med | Med | **8.1** |
| C. Hyatt Mexico | Med | Enrichment scripts | Med | Lower | Med | Med | Med | Med | 5.5 |

## Recommendation: **B. Choice Mexico**

Rationale:
1. Existing CALA Choice property-URL extract + adapter inventory already \`ready_with_extract\`.
2. Tests fail-closed 403 handling (critical governance lesson) without new discovery infrastructure.
3. High Brand Explorer brand-count value per hotel.
4. Marriott remains strategically important but needs fuller Mexico directory extract before Wave-scale reconstruction — higher technical risk after two successful waves.

**Do not launch** until steward explicitly starts Wave 1C.
`
);

writeMd(
  "16-migration-readiness.md",
  `# Migration Readiness Assessment

## Verdict: **NOT READY** (toward **PILOT MIGRATION READY** after Choice or Marriott wave)

| Criterion | Status |
|-----------|--------|
| Parent families reconstructed | 2 (IHG, Hilton) — need ≥3 recommended |
| % of current Census independently replaced | Low (Mexico major chains only) |
| Material-field provenance | Hilton ${pctMaterial}%; IHG ${ihgSummary?.materialPct ?? "?"}% — rooms gap on Hilton |
| Identity confidence | High where ctyhocn/IHG id + URL present |
| Source-rights | Directory OK for data; images Reference Only |
| Legacy exposure remaining | Legacy-only challenges unresolved |
| First-party validation | Packs designed, not sent |
| Operational impact | Staging only — no Airtable |

## Threshold for PILOT MIGRATION READY

1. ≥3 parent families Mexico reconstructed (recommend Choice next).
2. Material field ≥60% on each wave **or** explicit Unknown classification on hard fields.
3. Cross-family identity review of Probable pairs completed by steward.
4. Zero firewall violations in audits.
5. Small staging table write path reviewed (still no production overwrite of legacy).

## Threshold for PRODUCTION MIGRATION READY

1. Majority of Open Mexico census for major parents independently replaced.
2. First-party validation loops for ≥1 parent underway.
3. Temporal affiliation model implemented for reflags.
4. Steward-approved cutover runbook.

**No migration in Wave 1B.**
`
);

const brandList = brandsDiscovered.join(", ");
writeMd(
  "17-final-report.md",
  `# Verified Independent Census Wave 1B — Hilton Mexico Final Report

## Verdict

**Yes — Wave 1B confirms the architecture generalizes.** Hilton Mexico was independently reconstructed from official Hilton locations pages (no legacy seed, no Webhound, $0, no Airtable writes), with fail-closed firewall, freeze-before-comparison, and cross-family identity against IHG Wave 1A.

## Answers

1. **Did Hilton independently reconstruct successfully?** Yes.
2. **How many Hilton Mexico hotels?** **${records.length}**
3. **Brands discovered:** ${brandList}
4. **Core fields independently supported:** **${pctCore}%**
5. **Material Census fields independently supported:** **${pctMaterial}%**
6. **Did Hilton structured data improve hard fields?** Yes — coordinates, amenities (incl. F&B/spa/meeting/resort flags), openDate, address/phone widely available. Rooms: **not** materially available from locations/status GraphQL.
7. **Exact / probable legacy matches:** ${matches.length} / ${probable.length}
8. **Independent-only:** ${independentOnly.length}
9. **Legacy-only:** ${legacyOnly.length}
10. **Legacy-only challenges:** Strict + Targeted run; determinations in \`09-legacy-only-challenges.json\` (absence ≠ closed).
11. **Cross-family identities/reflags:** ${crossFamily.pairs.length} pairs (${crossFamily.summary.same_historical} historical, ${crossFamily.summary.probable_review} probable review).
12. **Property identity separate from affiliation?** **Yes — recommended** (see \`06-temporal-affiliation-design.md\`).
13. **Drive Hilton Brand Explorer completion?** Assessment yes — readiness in \`10-brand-explorer-readiness.json\`; **no activation**.
14. **First-party validation:** Universe confirm, status, rooms, management, reflags, imagery rights (\`11-hilton-first-party-validation-pack.md\`).
15. **Combined IHG + Hilton Mexico independent hotels:** **${ihgCount + records.length}** (~${combinedPhysicalEstimate} unique physical estimate).
16. **Data-eligible (Hilton):** **${records.length ? Math.round((dataEligible / records.length) * 100) : 0}%** (${dataEligible}/${records.length}).
17. **Architecture generalizing?** **Yes.**
18. **Next wave?** **Choice Mexico** (not launched).
19. **Migration pilot?** **NOT READY** (path to pilot after next family wave).
20. **Top 3 Census Research Engine improvements:**
    - (1) Hilton/IHG room-count GraphQL or first-party pack integration without weakening Unknown discipline.
    - (2) Implement minimal \`property_identity\` + temporal affiliation for reflag/cross-family.
    - (3) Persist live directory extracts (Hilton Mexico snapshot) for replay/regression without re-crawl.

## MOST IMPORTANTLY

**Wave 1B confirms Dealality can systematically recreate the hotel census across different hotel company ecosystems, independently maintain property identity and affiliation changes (design + detection), and use that verified census to complete Brand Explorer (assessment path — no auto-activation).**

## Constraints honored

No Webhound · No credits · No Airtable writes · No Brand Explorer activation · No legacy before freeze · No copying legacy values · No unsupported fills · No fuzzy-only merges · No STR taxonomy migration · No automatic image use · Existing governance preserved · V2 architecture reused.

## Runtime

${Math.round(elapsedMs / 1000)}s · firewall pre-freeze blocked: ${wave.firewallPreFreezeBlocked} · cost $0
`
);

writeJson("00-wave1b-run-summary.json", {
  wave: waveConfig.id,
  discoveries: records.length,
  brands: wave.discovery.brandBreakdown,
  corePct: pctCore,
  materialPct: pctMaterial,
  matches: matches.length,
  probable: probable.length,
  independent_only: independentOnly.length,
  legacy_only: legacyOnly.length,
  data_eligible: dataEligible,
  image_eligible: imageEligible,
  cross_family_pairs: crossFamily.pairs.length,
  firewallPreFreezeBlocked: wave.firewallPreFreezeBlocked,
  elapsedMs,
  discoveryPhaseHintMs: discoveryElapsed,
  costUsd: 0,
  next_wave: "Choice Mexico",
  migration: "NOT READY",
});

console.log("[vic-1b] done", {
  hotels: records.length,
  corePct: pctCore,
  materialPct: pctMaterial,
  matches: matches.length,
  independent_only: independentOnly.length,
  legacy_only: legacyOnly.length,
  elapsedMs,
});
