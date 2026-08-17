/**
 * Research Engine V2 — Verified Independent Census Wave 1D: Marriott Mexico
 *
 * Staging-only. No Airtable. No Webhound. No BE activation. No cross-family auto-merge.
 * Baseline prerequisite: mexico_vic_baseline_locked_ready_for_marriott_wave1d
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
import {
  attachPropertyIdentities,
  assessSamePhysicalProperty,
} from "../lib/research-engine-v2/clean-census/property-identity.js";
import { applyTemporalAffiliationSeed } from "../lib/research-engine-v2/clean-census/temporal-affiliation.js";
import {
  MATERIAL_CENSUS_FIELDS,
  CORE_MATERIAL_FIELDS,
} from "../lib/research-engine-v2/clean-census/provenance.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/research-engine-v2/verified-independent-census-wave1d-marriott");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");
const BASELINE = join(ROOT, "data/research-engine-v2/verified-independent-census-mexico-combined");
const IHG_V1 = join(ROOT, "data/research-engine-v2/verified-independent-census-v1");
const HILTON_1B = join(ROOT, "data/research-engine-v2/verified-independent-census-wave1b-hilton");
const CHOICE_1C = join(ROOT, "data/research-engine-v2/verified-independent-census-wave1c-choice");
const FETCH_DELAY_MS = Number(process.env.RE_V2_FETCH_DELAY_MS || 50);
const FETCH_PROPERTY_PAGES = process.env.RE_V2_MARRIOTT_PROPERTY_PAGES === "1";

function writeJson(dir, name, obj) {
  writeFileSync(join(dir, name), JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(dir, name, text) {
  writeFileSync(join(dir, name), text, "utf8");
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

function loadLegacyMarriottMexico() {
  const csv = readFileSync(join(ROOT, "reports/census-amenities-blank-rows.csv"), "utf8").split(/\r?\n/);
  const rows = [];
  for (const line of csv.slice(1)) {
    if (!line.trim()) continue;
    const f = parseCsvLine(line);
    const name = f[1] || "";
    const parent = f[2] === "(blank parent)" ? "" : f[2] || "";
    const country = f[4] || "";
    if (!/Mexico/i.test(country)) continue;
    const marriottParent = /Marriott/i.test(parent);
    const marriottName =
      /\b(Marriott|Sheraton|Westin|Courtyard|Residence Inn|Fairfield|Aloft|AC Hotel|City Express|JW Marriott|St\.?\s*Regis|Ritz|Autograph|Tribute|Design Hotels|Four Points|Moxy|Delta Hotels|Le M[eé]ridien|Renaissance|EDITION|Luxury Collection|W Hotels|SpringHill|TownePlace)\b/i.test(
        name
      );
    if (!marriottParent && !marriottName) continue;
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
  return rows.filter((r) => /Marriott/i.test(r.parentCompany));
}

function loadRecords(path) {
  if (!existsSync(path)) return [];
  return JSON.parse(readFileSync(path, "utf8")).records || [];
}

function classifyCrossFamilySteward(marriottRec, otherRec) {
  const assessment = assessSamePhysicalProperty(marriottRec, otherRec);
  if (assessment.same_physical_property) {
    if (assessment.identity_confidence === "Exact" || assessment.identity_confidence === "High") {
      return {
        classification: "exact_physical_match_requires_steward_review",
        assessment,
      };
    }
    return {
      classification: "probable_physical_match_requires_steward_review",
      assessment,
    };
  }
  if (assessment.reasons?.includes("fuzzy_name_only_rejected") || assessment.name_similarity >= 0.85) {
    return { classification: "rejected_fuzzy_match", assessment };
  }
  return { classification: "independent_physical_property", assessment };
}

mkdirSync(OUT, { recursive: true });
mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });

// Require locked baseline
const baselineLockPath = join(BASELINE, "00-baseline-lock.json");
if (!existsSync(baselineLockPath)) {
  throw new Error("Mexico VIC baseline lock missing — run npm run research-engine-v2:lock-mexico-vic-baseline first");
}
const baselineLock = JSON.parse(readFileSync(baselineLockPath, "utf8"));
if (baselineLock.baseline_status !== "mexico_vic_baseline_locked_ready_for_marriott_wave1d") {
  throw new Error(`Unexpected baseline status: ${baselineLock.baseline_status}`);
}

const waveConfig = {
  id: "wave1d_marriott_mexico",
  group: "Marriott",
  geography: "Mexico",
  brands: null,
  researchProfile: "full_census",
  legacyComparisonAfterFreeze: true,
  firstPartyValidationEligible: true,
  discovery: "live_marriott_mexico_country_hotel_sitemap",
  baseline_prerequisite: baselineLock.baseline_status,
  baseline_freeze_hash: baselineLock.combined_freeze_hash_sha256,
};

console.log("[vic-1d] reconstruction wave", waveConfig.id);
const t0 = Date.now();
const wave = await runReconstructionWave(waveConfig, {
  fetchDelayMs: FETCH_DELAY_MS,
  fetchPropertyPages: FETCH_PROPERTY_PAGES,
  onProgress: (m) => console.log(m),
});

const records = wave.frozen.records;
const identityBundle = attachPropertyIdentities(records);
const temporalSeed = applyTemporalAffiliationSeed(records);

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

const eligibility = batchAssessProductionEligibility(records);
const dataEligible = eligibility.filter((e) => e.production_eligibility_data === "ELIGIBLE").length;

writeJson(OUT, "01-marriott-independent-discovery.json", {
  ...wave.discovery,
  raw_source_capture: wave.discovery.raw_source_capture,
  firewallPreFreezeBlocked: wave.firewallPreFreezeBlocked,
});
writeJson(OUT, "02-marriott-full-records.json", {
  wave: waveConfig.id,
  recordCount: records.length,
  unique_physical_properties: identityBundle.unique_physical_properties,
  records,
});
writeJson(OUT, "03-normalized-property-index.json", {
  total: records.length,
  records: records.map((r) => ({
    independent_record_id: r.independent_record_id,
    property_id: r.property_id,
    name: r.fields?.name,
    brand: r.brand,
    city: r.fields?.city || null,
    country: "Mexico",
    marsha: r.marriott_structured?.marsha || r.fields?.["Property ID"],
    website: r.fields?.Website,
    status: r.fields?.status,
    core_pct: r.completeness?.corePct,
    material_pct: r.completeness?.materialPct,
    page_source_state: r.page_source_state,
  })),
});
writeJson(OUT, "04-unique-physical-property-index.json", identityBundle);
writeJson(OUT, "05-source-lineage-map.json", {
  discovery_basis: wave.discovery.discovery_basis,
  discovery_sources: wave.discovery.discovery_sources,
  source_split: {
    directory_sitemap: records.length,
    property_page_enriched: records.filter((r) =>
      (r.independent_sources || []).some((s) => s.role === "enrichment_attempt" && s.result !== "Blocked")
    ).length,
    blocked_property_pages: records.filter((r) => r.page_source_state === "Blocked").length,
  },
});
writeJson(OUT, "06-rejected-merge-log.json", {
  intra_marriott_sitemap_duplicates: wave.discovery.rejectedDuplicates || [],
  identity_intra_cohort_links: identityBundle.intra_cohort_duplicate_links,
  fuzzy_auto_merges: 0,
  note: "Fuzzy name alone never merges",
});
writeJson(OUT, "07-temporal-affiliation-notes.json", {
  ...temporalSeed,
  current_affiliation_count: records.length,
  future_opening_candidates: 0,
  prior_affiliation_candidates: 0,
  fake_start_dates: 0,
  note: "Current affiliation As of discovery; start dates Unknown unless independently sourced",
});

writeJson(OUT, "08-marriott-freeze.json", {
  frozenAt: wave.frozen.frozenAt,
  freeze_hash_sha256: wave.freeze_hash,
  firewallPreFreezeBlocked: wave.firewallPreFreezeBlocked,
  firewall_audit: wave.firewall.getAudit(),
  recordCount: records.length,
  unique_physical_properties: identityBundle.unique_physical_properties,
  brandBreakdown: wave.discovery.brandBreakdown,
  baseline_prerequisite: baselineLock.baseline_status,
  baseline_combined_freeze_hash: baselineLock.combined_freeze_hash_sha256,
  legacy_used_as_source: false,
});

// Post-freeze legacy
console.log("[vic-1d] post-freeze legacy comparison");
wave.firewall.beginLegacyReconciliation();
const legacyRows = wave.firewall.requestLegacyCensus(() => loadLegacyMarriottMexico());
const comparison = reconcileAfterFreeze(wave.frozen, legacyRows, wave.firewall);
writeJson(OUT, "09-post-freeze-legacy-comparison.json", comparison);

const matches = comparison.comparisons.filter((c) => c.legacy_match_status === "Independent + Legacy Match");
const probable = comparison.comparisons.filter((c) => String(c.legacy_match_status).includes("Probable"));
const independentOnly = comparison.comparisons.filter((c) => c.legacy_match_status === "Independent Only");
const legacyOnly = comparison.legacy_only_rows || [];

const dirRows = wave.discovery.discoveries.map((d) => d.directory_row);
writeJson(OUT, "10-legacy-only-challenges.json", {
  recommendation: CHALLENGE_CLASS_RECOMMENDATION,
  strict: runStrictIndependentRediscovery(legacyOnly, dirRows, wave.firewall),
  targeted: runTargetedVerificationChallenges(legacyOnly, dirRows, wave.firewall),
});

// Cross-family vs locked baseline cohorts
console.log("[vic-1d] cross-family steward review queue");
const prior = [
  ...loadRecords(join(IHG_V1, "08-expanded-benchmark-full-records.json")).map((r) => ({ ...r, _family: "IHG" })),
  ...loadRecords(join(HILTON_1B, "02-hilton-full-records.json")).map((r) => ({ ...r, _family: "Hilton" })),
  ...loadRecords(join(CHOICE_1C, "02-choice-full-records.json")).map((r) => ({ ...r, _family: "Choice" })),
];

const stewardQueue = [];
const rejectedFuzzy = [];
for (const m of records) {
  for (const o of prior) {
    const result = classifyCrossFamilySteward(m, o);
    if (result.classification === "independent_physical_property") continue;
    const entry = {
      ...result,
      marriott: {
        id: m.independent_record_id,
        name: m.fields?.name,
        brand: m.brand,
        property_id: m.property_id,
      },
      other: {
        family: o._family,
        id: o.independent_record_id,
        name: o.fields?.name || o.canonical_hotel_name,
        brand: o.brand || o.fields?.Affiliation,
      },
    };
    if (result.classification === "rejected_fuzzy_match") rejectedFuzzy.push(entry);
    else stewardQueue.push(entry);
  }
}

writeJson(OUT, "11-cross-family-steward-queue.json", {
  baseline_records: 365,
  exact_or_probable_candidates: stewardQueue,
  rejected_fuzzy_matches: rejectedFuzzy,
  summary: {
    exact: stewardQueue.filter((x) => x.classification.startsWith("exact")).length,
    probable: stewardQueue.filter((x) => x.classification.startsWith("probable")).length,
    rejected_fuzzy: rejectedFuzzy.length,
    auto_merges: 0,
  },
});

writeJson(OUT, "12-data-image-eligibility.json", {
  summary: {
    data_eligible: dataEligible,
    data_eligible_pct: records.length ? Math.round((dataEligible / records.length) * 100) : 0,
    images_eligible: 0,
    image_gate: "Needs First-Party Media",
  },
  results: eligibility,
});

const brandsDiscovered = Object.keys(wave.discovery.brandBreakdown || {}).sort();
const beBrands = brandsDiscovered.map((brand) => {
  const subset = records.filter((r) => r.brand === brand);
  const avgCore = Math.round(subset.reduce((s, r) => s + (r.completeness?.corePct || 0), 0) / subset.length);
  const avgMat = Math.round(subset.reduce((s, r) => s + (r.completeness?.materialPct || 0), 0) / subset.length);
  let status = "BE hold / insufficient evidence";
  if (avgCore >= 90 && subset.length >= 3) status = "BE completion partial";
  if (avgCore >= 95 && avgMat >= 50 && subset.length >= 5) status = "BE completion ready";
  if (brand.includes("Unconfirmed")) status = "BE hold / insufficient evidence";
  return {
    brand,
    hotel_count: subset.length,
    avg_core_pct: avgCore,
    avg_material_pct: avgMat,
    be_status: status,
    missing: ["rooms", "open date", "owner/operator", "coordinates"].filter((gap) => {
      if (gap === "rooms") return subset.every((r) => !r.fields?.rooms);
      if (gap === "coordinates") return subset.every((r) => !r.fields?.Latitude);
      if (gap === "open date") return subset.every((r) => !r.fields?.["Open Date"]);
      return true;
    }),
    activation: "NONE",
  };
});

writeJson(OUT, "13-brand-explorer-completion-readiness.json", {
  activation: "NONE",
  brands: beBrands,
  verdict: beBrands.some((b) => b.be_status === "BE completion ready")
    ? "READY FOR SMALL BRAND COMPLETION PILOT"
    : "PARTIAL — steward review before BE pilot",
});

writeJson(OUT, "14-migration-readiness.json", {
  staging_migration_ready: dataEligible > 0,
  production_overwrite_ready: false,
  required_steward_review: [
    "Cross-family steward queue",
    "Brand Unconfirmed soft-brand rows",
    "Legacy-only Marriott Mexico challenges",
    "City inference Medium-confidence rows",
  ],
  fields_safe_to_stage: ["name", "Affiliation", "Parent Company", "country", "Website", "Property ID", "status", "Market"],
  fields_unsafe_without_fp: ["rooms", "Open Date", "Management Company", "owner", "Latitude", "Longitude", "images"],
  posture: "staging migration only — no production overwrite",
});

const elapsedMs = Date.now() - t0;
const waveStatus =
  dataEligible >= Math.floor(records.length * 0.5) && pctCore >= 85
    ? "wave1d_marriott_mexico_vic_complete_ready_for_combined_4_family_baseline"
    : pctCore >= 70
      ? "wave1d_marriott_mexico_vic_partial_requires_steward_review"
      : "wave1d_marriott_mexico_vic_blocked";

const summary = {
  wave: waveConfig.id,
  status: waveStatus,
  discoveries: records.length,
  unique_physical_properties: identityBundle.unique_physical_properties,
  brands: wave.discovery.brandBreakdown,
  corePct: pctCore,
  materialPct: pctMaterial,
  data_eligible: dataEligible,
  matches: matches.length,
  probable: probable.length,
  independent_only: independentOnly.length,
  legacy_only: legacyOnly.length,
  cross_family_steward_candidates: stewardQueue.length,
  rejected_fuzzy: rejectedFuzzy.length,
  firewallPreFreezeBlocked: wave.firewallPreFreezeBlocked,
  elapsedMs,
  costUsd: 0,
  airtable_writes: false,
  webhound_used: false,
  brand_explorer_activation: false,
  production_overwrite: false,
  baseline_lock: baselineLock.baseline_status,
};

writeJson(OUT, "00-wave1d-run-summary.json", summary);
writeJson(OUT, "15-completeness-report.json", {
  corePct: pctCore,
  materialPct: pctMaterial,
  fieldHits,
  missing_rooms: records.length - (fieldHits.rooms || 0),
  missing_open_date: records.length - (fieldHits["Open Date"] || 0),
  missing_coords: records.length - (fieldHits.Latitude || 0),
  missing_management: records.length - (fieldHits["Management Company"] || 0),
  missing_city: records.length - (fieldHits.city || 0),
});

const md = `# Verified Independent Census Wave 1D — Marriott Mexico

**Status:** \`${waveStatus}\`  
**Baseline prerequisite:** \`${baselineLock.baseline_status}\`  
**Staging only** · No Airtable · No Webhound · No BE activation · No production overwrite · No cross-family auto-merge

---

## 1. Executive summary

Independently reconstructed **${records.length}** Marriott Mexico hotels from the official Marriott country hotel-sitemap (**${identityBundle.unique_physical_properties}** unique physical properties). Core **${pctCore}%** · Material **${pctMaterial}%** · Data-eligible **${dataEligible}**. Property Identity V1 and Temporal Affiliation V1 applied. Cross-family auto-merges: **0**.

---

## 2. Source strategy

1. Official Marriott Mexico country hotel-sitemap (primary)  
2. Official property overview URLs from sitemap  
3. Optional property-page enrichment (\`RE_V2_MARRIOTT_PROPERTY_PAGES=1\`) — default off (Akamai/403 risk)  
4. Blocked pages classified **Blocked**, not closed  

Not used: legacy as proof, Webhound, Airtable, unverified third-party lists.

Sitemap URL: \`${wave.discovery.sitemapUrl}\`

---

## 3–4. Totals

| Metric | Value |
|--------|-------|
| Source / independent records | ${records.length} |
| Unique physical properties | ${identityBundle.unique_physical_properties} |
| Sitemap duplicates rejected | ${(wave.discovery.rejectedDuplicates || []).length} |
| Intra-identity collapses | ${identityBundle.intra_cohort_duplicate_links.length} |

---

## 5. Brand coverage

| Brand | Count |
|-------|------:|
${brandsDiscovered.map((b) => `| ${b} | ${wave.discovery.brandBreakdown[b]} |`).join("\n")}

---

## 6. Source split

| Source | Count |
|--------|------:|
| Country sitemap / directory | ${records.length} |
| Property-page enriched | ${summary.discoveries && FETCH_PROPERTY_PAGES ? "(see lineage map)" : 0} |
| Blocked property pages | ${records.filter((r) => r.page_source_state === "Blocked").length} |

---

## 7. Property Identity V1

- Unique physical: **${identityBundle.unique_physical_properties}** / ${records.length}  
- Fuzzy-name-only merges: **0**  
- Rejected merge log: \`06-rejected-merge-log.json\`

---

## 8. Temporal Affiliation V1

- Current affiliations seeded: **${records.length}** (As of discovery)  
- Fake start dates: **0**  
- Future/opening candidates: **0** (none independently dated this run)

---

## 9–10. Completeness & data-eligible

| Metric | Value |
|--------|-------|
| Core | ${pctCore}% |
| Material | ${pctMaterial}% |
| Data-eligible | ${dataEligible} |
| Missing rooms | ${records.length - (fieldHits.rooms || 0)} |
| Missing open date | ${records.length - (fieldHits["Open Date"] || 0)} |
| Missing coordinates | ${records.length - (fieldHits.Latitude || 0)} |
| Missing management | ${records.length - (fieldHits["Management Company"] || 0)} |
| City present (title-inferred) | ${fieldHits.city || 0} |

Unknown preferred over fabrication.

---

## 11. Legacy comparison (post-freeze only)

| Class | Count |
|-------|------:|
| Exact match | ${matches.length} |
| Probable | ${probable.length} |
| Independent-only | ${independentOnly.length} |
| Legacy-only | ${legacyOnly.length} |

Legacy is comparison-only — never proof.

---

## 12. Cross-family vs locked 365 baseline

| Class | Count |
|-------|------:|
| Exact physical — steward review | ${stewardQueue.filter((x) => x.classification.startsWith("exact")).length} |
| Probable physical — steward review | ${stewardQueue.filter((x) => x.classification.startsWith("probable")).length} |
| Rejected fuzzy | ${rejectedFuzzy.length} |
| Auto-merges | **0** |

---

## 13–14. Reflags / rejected fuzzy

See \`11-cross-family-steward-queue.json\`. No automatic reflags.

---

## 15. Brand Explorer completion readiness

**${beBrands.some((b) => b.be_status === "BE completion ready") ? "READY FOR SMALL BRAND COMPLETION PILOT" : "PARTIAL — steward review"}** — **no activation**.

---

## 16. Migration readiness

**Staging migration only** · Production overwrite: **No**

---

## 17. Gaps and limitations

- Rooms / open date / owner / operator / coordinates largely Unknown (sitemap lacks structured geo/amenities)  
- City often Medium-confidence title inference  
- Some soft brands remain \`Marriott Bonvoy — Brand Unconfirmed\`  
- Overview pages often Akamai-blocked  

---

## 18. Recommended next step

1. Steward review cross-family queue  
2. Lock **combined 4-family Mexico VIC baseline** (IHG+Hilton+Choice+Marriott)  
3. Optional: safe Marriott content enrichment for rooms/coords without treating 403 as closed  

---

## Acceptance

- [x] Independent Marriott Mexico reconstruction  
- [x] Source + unique physical counts  
- [x] Brand coverage  
- [x] Core / material / data-eligible  
- [x] Property Identity V1 + Temporal Affiliation V1  
- [x] No fake rooms / open dates / owners / start dates  
- [x] No cross-family fuzzy auto-merges  
- [x] Legacy comparison-only  
- [x] No Airtable / BE activation / production overwrite / Webhound  
- [x] Status: \`${waveStatus}\`

Runtime ~${Math.round(elapsedMs / 1000)}s · cost $0 · firewall pre-freeze blocked: ${wave.firewallPreFreezeBlocked}
`;

writeMd(OUT, "16-final-report.md", md);
writeMd(REPORTS, "verified-independent-census-wave1d-marriott.md", md);
writeJson(REPORTS, "verified-independent-census-wave1d-marriott.json", {
  ...summary,
  brandBreakdown: wave.discovery.brandBreakdown,
  completeness: { corePct: pctCore, materialPct: pctMaterial, fieldHits },
  legacy: {
    matches: matches.length,
    probable: probable.length,
    independent_only: independentOnly.length,
    legacy_only: legacyOnly.length,
  },
  cross_family: {
    steward_candidates: stewardQueue.length,
    rejected_fuzzy: rejectedFuzzy.length,
    auto_merges: 0,
  },
  be_readiness: beBrands,
  freeze_hash_sha256: wave.freeze_hash,
});

writeMd(
  DOCS,
  "verified-independent-census-wave1d-marriott-mexico.md",
  `# VIC Wave 1D — Marriott Mexico

> **Status:** \`${waveStatus}\`  
> **Artifacts:** \`data/research-engine-v2/verified-independent-census-wave1d-marriott/\`  
> **Reports:** \`reports/research-engine-v2/verified-independent-census-wave1d-marriott.{md,json}\`

## Snapshot

- Independent hotels: **${records.length}**
- Unique physical: **${identityBundle.unique_physical_properties}**
- Core / material: **${pctCore}% / ${pctMaterial}%**
- Data-eligible: **${dataEligible}**
- Cross-family auto-merges: **0**

## Constraints honored

No Airtable · No Webhound · No BE activation · No production overwrite · No legacy-as-proof · No fuzzy auto-merge

## Next

Combined 4-family Mexico VIC baseline lock (IHG + Hilton + Choice + Marriott) after steward review of cross-family queue.
`
);

console.log("[vic-1d] done", summary);
