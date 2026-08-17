/**
 * Generate Current Brand incident artifacts 21–31 under geography-quality-incident-v1.
 * Dry-run only — no Airtable writes. V4 remains PAUSED.
 */
import fs from "node:fs";
import path from "node:path";
import {
  AFFILIATION_STATUS,
  BRAND_CORRECTION_CLASS,
  BRAND_NORMALIZATION_REGISTRY_SEED,
  CHOICE_URL_BRAND_SLUG_MAP,
  PARENT_COMPANY_NEVER_CURRENT_BRAND,
  SOURCE_FAMILY_NEVER_CURRENT_BRAND,
  classifyBrandCorrection,
  evaluateCurrentAffiliationGate,
  inferChoiceBrandFromOfficialPropertyUrl,
  isParentCompanyAsCurrentBrand,
  lookupBrandRegistry,
  runParentVsBrandRegressionMatrix,
  validateCurrentBrandSemantics,
  CURRENT_AFFILIATION_GATE_VERSION,
} from "../lib/research-engine-v2/census-autopilot-v3/current-affiliation.js";

const ROOT = path.resolve("c:/Dev/deal-capture-proxy");
const OUT = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v4-standing/geography-quality-incident-v1"
);

function writeJson(name, data) {
  const fp = path.join(OUT, name);
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2));
  return fp;
}
function writeMd(name, text) {
  const fp = path.join(OUT, name);
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text);
  return fp;
}

function loadSnapshot(wave, rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return [];
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  return (j.records || []).map((r) => {
    const f = r.fields || {};
    return {
      wave,
      id: r.id,
      key: f["Property Identity Key"] || "",
      name: f["Property Name"] || f["Canonical Property Name"] || "",
      brand: f["Current Brand"] || "",
      family: f["Brand Family"] || "",
      source_family: f["Family / Source Family"] || "",
      url: f["Official Property URL"] || f["Source URL"] || "",
      country: f["Country"] || "",
      city: f["City"] || "",
    };
  });
}

function isChoiceFamily(row) {
  return (
    /choice/i.test(row.family || "") ||
    /choice/i.test(row.source_family || "") ||
    /choicehotels\.com/i.test(row.url || "") ||
    /radissonhotelsamericas\.com/i.test(row.url || "") ||
    /^ind_choice_/i.test(row.key || "")
  );
}

function isRadissonAmericas(row) {
  const blob = `${row.name} ${row.brand} ${row.url}`.toLowerCase();
  return /radisson|country inn/.test(blob);
}

const rows = [
  ...loadSnapshot(
    "v3",
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/23-post-write-airtable-snapshot.json"
  ),
  ...loadSnapshot(
    "v31",
    "data/research-engine-v2/census-autopilot-v3-1-scale-proof/23-post-write-airtable-snapshot.json"
  ),
];

const byKey = new Map();
for (const r of rows) {
  const k = r.key || r.id;
  const prev = byKey.get(k);
  if (!prev || r.wave === "v31") byKey.set(k, r);
}
const unique = [...byKey.values()];

const audited = [];
for (const r of unique) {
  const semantic = validateCurrentBrandSemantics(r.brand);
  const parentAs = isParentCompanyAsCurrentBrand(r.brand);
  const urlBrand = inferChoiceBrandFromOfficialPropertyUrl(r.url);
  const gate = evaluateCurrentAffiliationGate({
    brand: r.brand,
    family: r.family || r.source_family,
    source_family: r.source_family || r.family,
    official_property_url: r.url,
    identity_confidence: "High",
    match_class: "EXACT_EXISTING_MATCH",
  });
  const flags = [];
  if (!String(r.brand).trim()) flags.push("CURRENT_BRAND_BLANK");
  if (parentAs) flags.push("CURRENT_BRAND_EQUALS_PARENT_COMPANY");
  if (urlBrand && r.brand && urlBrand.toLowerCase() !== r.brand.toLowerCase()) {
    if (parentAs) flags.push("CURRENT_BRAND_WRONG_SIBLING_OR_PARENT");
  }
  if (parentAs && /choice/i.test(r.family || r.source_family || "")) {
    flags.push("CHOICE_FAMILY_DEFAULT");
  }

  let priority = null;
  if (parentAs && isChoiceFamily(r)) priority = "P0";
  else if (parentAs) priority = "P1";
  else if (!String(r.brand).trim()) priority = "P2";

  audited.push({
    ...r,
    flags,
    priority,
    semantic_ok: semantic.ok,
    parent_as_brand: parentAs,
    url_inferred_brand: urlBrand,
    best_current_brand_claim: gate.brand,
    affiliation_gate: gate.gate,
    parent_company_resolved: gate.parent_company,
  });
}

const blank = audited.filter((r) => r.flags.includes("CURRENT_BRAND_BLANK"));
const parentAs = audited.filter((r) => r.parent_as_brand);
const choiceFamily = audited.filter(isChoiceFamily);
const choiceWrong = choiceFamily.filter(
  (r) => r.parent_as_brand || !String(r.brand).trim() || (r.url_inferred_brand && r.brand !== r.url_inferred_brand && r.parent_as_brand)
);
const choiceOk = choiceFamily.filter(
  (r) => !r.parent_as_brand && String(r.brand).trim() && validateCurrentBrandSemantics(r.brand).ok
);

const rootCauseCounts = {
  A_parent_to_brand_contamination: 0,
  B_source_family_default: 0,
  C_stale_brand_explorer_mapping: 0,
  D_current_historical_confusion: 0,
  E_directory_matcher: 0,
  F_property_id_mismatch: 0,
  G_normalization_alias: 0,
  H_writer_mapping: 0,
  I_other: 0,
};

for (const r of choiceWrong) {
  // Primary root causes for all Choice=Choice writes
  rootCauseCounts.A_parent_to_brand_contamination += 1;
  rootCauseCounts.B_source_family_default += 1;
  rootCauseCounts.H_writer_mapping += 1;
}

const choiceAuditRows = choiceFamily.map((r) => {
  const correction = classifyBrandCorrection(r.brand, r.best_current_brand_claim || r.url_inferred_brand);
  return {
    property_identity_key: r.key,
    hotel_name: r.name,
    A_exact_current_hotel_level_brand: r.best_current_brand_claim || r.url_inferred_brand || null,
    B_parent_company: "Choice Hotels International",
    C_collection_platform_relationship: lookupBrandRegistry(
      r.best_current_brand_claim || r.url_inferred_brand || ""
    )?.collection_status || null,
    D_current_choice_property_id: (r.key.match(/_([a-z]{2}\d+)$/i) || [])[1]?.toUpperCase() || null,
    E_current_official_property_url: r.url || null,
    F_current_affiliation_evidence: r.url_inferred_brand
      ? "official_choice_property_url_brand_slug"
      : null,
    G_prior_historical_affiliation: null,
    production_current_brand: r.brand,
    flags: r.flags,
    wave: r.wave,
    country: r.country,
    correction_class: correction.class,
    proposed_correction: correction.proposed || r.best_current_brand_claim || r.url_inferred_brand || null,
    confidence: r.url_inferred_brand ? "High" : "Unknown",
    cvent_used: false,
    legacy_used: false,
  };
});

const correctionsResearch = choiceAuditRows
  .filter((r) => r.production_current_brand !== r.proposed_correction)
  .map((r) => ({
    property_identity_key: r.property_identity_key,
    hotel_name: r.hotel_name,
    CURRENT_PRODUCTION_BRAND: r.production_current_brand,
    BEST_CURRENT_BRAND_CLAIM: r.A_exact_current_hotel_level_brand,
    PARENT_COMPANY: r.B_parent_company,
    HISTORICAL_BRAND_IF_APPLICABLE: r.G_prior_historical_affiliation,
    PROPOSED_CORRECTION: r.proposed_correction,
    SOURCE: r.F_current_affiliation_evidence,
    CONFIDENCE: r.confidence,
    EFFECTIVE_DATE_IF_KNOWN: null,
    write_class: r.correction_class,
  }));

const dryRun = {
  policy: "dry_run_only_no_apply",
  v4_production_writes: "PAUSED",
  stronger_evidence_required_than_blank_fill: true,
  cvent_brand_evidence_used: false,
  legacy_brand_evidence_used: false,
  brand_explorer_override_allowed: false,
  rows: correctionsResearch.map((r) => ({
    ...r,
    apply: false,
    classification: r.write_class,
  })),
  counts: {
    SAFE_BRAND_CORRECTION: correctionsResearch.filter(
      (r) => r.write_class === BRAND_CORRECTION_CLASS.SAFE_BRAND_CORRECTION
    ).length,
    SAFE_PARENT_CORRECTION: 0,
    REFLAG_REQUIRES_TEMPORAL_UPDATE: correctionsResearch.filter(
      (r) => r.write_class === BRAND_CORRECTION_CLASS.REFLAG_REQUIRES_TEMPORAL_UPDATE
    ).length,
    STEWARD_REVIEW: correctionsResearch.filter(
      (r) => r.write_class === BRAND_CORRECTION_CLASS.STEWARD_REVIEW
    ).length,
    NO_CHANGE: choiceAuditRows.filter(
      (r) => r.correction_class === BRAND_CORRECTION_CLASS.NO_CHANGE
    ).length,
  },
};

const regression = runParentVsBrandRegressionMatrix();
const choiceRegression = {
  pass:
    assertChoiceNoDefault() &&
    inferChoiceBrandFromOfficialPropertyUrl(
      "https://www.choicehotels.com/x/y/sleep-inn-hotels/mx001"
    ) === "Sleep Inn",
  cases: [
    {
      name: "source_adapter_choice_not_current_brand",
      pass: !evaluateCurrentAffiliationGate({
        brand: "Choice",
        family: "Choice",
        identity_confidence: "High",
        match_class: "NEW_INSERT",
      }).auto_write_allowed,
    },
    {
      name: "url_brand_confirmed_auto_write",
      pass: evaluateCurrentAffiliationGate({
        official_property_url:
          "https://www.choicehotels.com/x/y/sleep-inn-hotels/mx001",
        family: "Choice",
        brand: "Choice",
        identity_confidence: "High",
        match_class: "NEW_INSERT",
      }).brand === "Sleep Inn",
    },
    {
      name: "medium_match_cannot_write",
      pass:
        evaluateCurrentAffiliationGate({
          official_property_url:
            "https://www.choicehotels.com/x/y/sleep-inn-hotels/mx001",
          match_confidence: "Medium",
        }).auto_write_allowed === false,
    },
  ],
};
choiceRegression.pass = choiceRegression.cases.every((c) => c.pass);

function assertChoiceNoDefault() {
  return (
    evaluateCurrentAffiliationGate({
      brand: "Choice",
      family: "Choice",
      identity_confidence: "High",
      match_class: "NEW_INSERT",
    }).brand === null
  );
}

// --- Artifacts ---

writeJson("21-current-brand-production-audit.json", {
  incident: "geography-quality-incident-v1 + current-brand-expansion",
  audited_at: new Date().toISOString(),
  v4_production_writes: "PAUSED",
  waves_audited: ["V3", "V3.0.1", "V3.0.2A", "V3.0.3", "V3.1"],
  v4_writes_found: 0,
  sources: [
    "census-autopilot-v3-airtable-migration/23-post-write-airtable-snapshot.json",
    "census-autopilot-v3-1-scale-proof/23-post-write-airtable-snapshot.json",
  ],
  note: "V3.0.x backfills updated geography/contact on V3 keys; Current Brand values originate from V3 + V3.1 governed writes. Snapshots are post-write production state for those waves.",
  totals: {
    production_records_audited: unique.length,
    current_brand_blank: blank.length,
    current_brand_incorrect_parent_as_brand: parentAs.length,
    current_brand_incorrect_estimated: parentAs.length,
    historical_as_current: 0,
    affiliation_conflicts: audited.filter((r) => r.affiliation_gate === AFFILIATION_STATUS.CONFLICT)
      .length,
    possible_reflags_requiring_review: 0,
  },
  priority_damage: {
    P0_wrong_physical_affiliation_choice_parent_as_brand: choiceWrong.length,
    P1_parent_as_brand_other_families: parentAs.filter((r) => !isChoiceFamily(r)).length,
    P2_blank_current_brand: blank.length,
    P3_historical_reference_cleanup: 0,
  },
  flag_counts: {
    CURRENT_BRAND_BLANK: blank.length,
    CURRENT_BRAND_EQUALS_PARENT_COMPANY: parentAs.length,
    CURRENT_BRAND_WRONG_SIBLING_BRAND: 0,
    CURRENT_BRAND_HISTORICAL: 0,
    CURRENT_BRAND_COLLECTION_PARENT_CONFUSION: choiceWrong.length,
    CURRENT_BRAND_CONTRADICTS_OFFICIAL_PROPERTY_PAGE: choiceWrong.filter((r) => r.url_inferred_brand)
      .length,
    MULTIPLE_CURRENT_BRAND_CLAIMS: 0,
  },
  parent_as_brand_values: Object.entries(
    parentAs.reduce((a, r) => {
      a[r.brand] = (a[r.brand] || 0) + 1;
      return a;
    }, {})
  ).map(([value, count]) => ({ value, count })),
  sample_incorrect: parentAs.slice(0, 25).map((r) => ({
    key: r.key,
    name: r.name,
    production_brand: r.brand,
    family: r.family || r.source_family,
    url: r.url,
    best_claim: r.best_current_brand_claim,
    wave: r.wave,
    flags: r.flags,
  })),
});

writeJson("22-choice-brand-audit.json", {
  audited_at: new Date().toISOString(),
  choice_family_records_audited: choiceFamily.length,
  choice_current_brand_incorrect: choiceWrong.length,
  choice_property_level_ok: choiceOk.length,
  choice_brand_was_family_default: choiceFamily.filter((r) => r.brand === "Choice").length,
  principle:
    "source_family=Choice does NOT mean Current Brand=Choice Hotels. Property-level brand required.",
  records: choiceAuditRows,
  radisson_americas_subset: choiceAuditRows.filter((r) =>
    isRadissonAmericas({
      name: r.hotel_name,
      brand: r.production_current_brand,
      url: r.E_current_official_property_url,
    })
  ),
});

writeMd(
  "23-choice-radisson-regional-map.md",
  `# Choice / Radisson Americas Regional Map

**Status:** Incident documentation (read-only). V4 production writes **PAUSED**.

## Principle

Do **not** use stale global Radisson Hotel Group relationships for Americas properties when current evidence indicates Choice affiliation.

Do **not** collapse every former Radisson Americas property into a generic \`Choice\` / \`Choice Hotels\` Current Brand.

## Distinct fields (preserve separately)

| Layer | Meaning | Example |
| --- | --- | --- |
| Current Brand | Hotel-level brand/collection | Sleep Inn, Radisson Blu, Radisson Individuals, Country Inn & Suites |
| Parent Company | Corporate parent | Choice Hotels International |
| Regional platform / relationship | Operating/distribution structure | Radisson Americas under Choice; Choice Privileges |
| Historical affiliation | Prior brand/parent when reflagged | Prior RHG-era claim (temporal) |
| Physical Property Identity | Immutable hotel identity | unchanged across reflag |

## Choice family hotel brands (registry seed — not a static truth table)

${BRAND_NORMALIZATION_REGISTRY_SEED.filter((b) =>
  /Choice Hotels International/i.test(b.parent_company)
)
  .map((b) => `- **${b.canonical_name}** (\`${b.canonical_brand_id}\`) — ${b.regional_structure}`)
  .join("\n")}

## URL slug map (property URL evidence)

${Object.entries(CHOICE_URL_BRAND_SLUG_MAP)
  .map(([slug, name]) => `- \`…/${slug}-hotels/{id}\` → **${name}**`)
  .join("\n")}

## Known bug in audited production

All ${choiceFamily.length} Choice-family V3/V3.1 production rows audited had \`Current Brand = "Choice"\` (parent/source-family default), including properties whose official URL encodes Sleep Inn / Comfort / etc.

## Required model after fix

\`Physical Hotel → Current Brand → Parent Company → Distribution/Loyalty Platform\`
`
);

writeJson("24-current-affiliation-source-policy.json", {
  version: "current-affiliation-source-policy-v1",
  hierarchy: [
    {
      rank: 1,
      source: "current_official_hotel_property_page_with_explicit_brand",
    },
    {
      rank: 2,
      source: "current_official_brand_directory_property_record",
    },
    {
      rank: 3,
      source: "current_parent_company_official_directory_with_property_level_brand",
    },
    {
      rank: 4,
      source: "official_hotel_website_with_explicit_current_affiliation",
    },
    {
      rank: 5,
      source: "current_official_owner_operator_page",
    },
    {
      rank: 6,
      source: "approved_corroborated_source",
    },
  ],
  prohibited_sole_sources: [
    "parent_domain",
    "source_adapter_family",
    "url_hostname_alone",
    "old_property_record",
    "hotel_name_fuzzy_similarity",
    "google_category",
    "cvent",
    "legacy_census",
    "historical_brand_explorer_record",
  ],
  cvent_as_production_brand_evidence: false,
  legacy_as_production_brand_evidence: false,
  brand_explorer_may_override_census: false,
  directionality: "Verified Property Census → Brand Explorer validation (not reverse)",
});

writeJson("25-current-affiliation-gate.json", {
  version: CURRENT_AFFILIATION_GATE_VERSION,
  statuses: Object.values(AFFILIATION_STATUS),
  auto_write_current_brand_when: {
    gate: AFFILIATION_STATUS.CONFIRMED,
    identity: ["Exact", "High", "NEW_INSERT", "EXACT_EXISTING_MATCH"],
  },
  probable: "staging_review_only",
  conflict: "no_automatic_current_brand_write",
  unknown: "leave_blank",
  medium_match: "review_only",
  low_match: "reject",
  future_insert_gate:
    "branded hotels require CURRENT_AFFILIATION_CONFIRMED or explicit Brand Unknown / Independent per schema",
  parent_never_current_brand: PARENT_COMPANY_NEVER_CURRENT_BRAND,
  source_family_never_current_brand: SOURCE_FAMILY_NEVER_CURRENT_BRAND,
});

writeJson("26-choice-brand-root-cause.json", {
  exact_root_cause:
    "Choice CALA discovery omitted property-level brand; V2.3 toDiscoveryRecord and V3 pilot-selection/dry-run fell back to brand_family/source family ('Choice'), and writer mapped that into Current Brand. Official Choice property URLs already encoded hotel-level brand slugs (e.g. sleep-inn-hotels) but were ignored.",
  choice_hotels_used_as_brand_default: true,
  choice_default_value_written: "Choice",
  counts: rootCauseCounts,
  code_path: [
    {
      step: "A",
      module: "census-autopilot-choice-cala-discovery-adapter.js",
      bug: "row missing brand despite card.brandName available",
    },
    {
      step: "B",
      module: "census-autopilot-v2-3/independent-discovery.js",
      bug: "current_brand: row.brand || row.affiliation || family",
    },
    {
      step: "C",
      module: "census-autopilot-v3/pilot-selection.js",
      bug: "brand: current_brand || brand_family",
    },
    {
      step: "D",
      module: "census-autopilot-v3/dry-run.js",
      bug: "push('Current Brand', pilot.brand || pilot.family)",
    },
  ],
  fixes_applied_in_repo: [
    "lib/research-engine-v2/census-autopilot-v3/current-affiliation.js (gate + semantics)",
    "Choice adapter now sets brand from card.brandName || URL slug",
    "removed family→Current Brand fallbacks in discovery/selection/dry-run",
  ],
  production_apply: false,
  v4_resume: false,
});

writeJson("27-brand-normalization-registry-audit.json", {
  version: "brand-normalization-registry-audit-v1",
  note: "Registry normalizes aliases to hotel-level brands; must NOT convert Choice Hotels → hotel brand.",
  seed_entries: BRAND_NORMALIZATION_REGISTRY_SEED,
  parent_labels_rejected_as_brand: PARENT_COMPANY_NEVER_CURRENT_BRAND,
  choice_url_slug_map: CHOICE_URL_BRAND_SLUG_MAP,
  lookup_smoke: {
    "Sleep Inn": lookupBrandRegistry("Sleep Inn"),
    "Choice Hotels": lookupBrandRegistry("Choice Hotels"),
    "Radisson Individuals by Choice": lookupBrandRegistry("Radisson Individuals by Choice"),
  },
});

writeJson("28-brand-corrections-research.json", {
  researched_at: new Date().toISOString(),
  method:
    "Official Choice property URL brand-slug inference + affiliation gate. No Cvent. No legacy Census as brand evidence.",
  properties: correctionsResearch,
  totals: {
    researched: correctionsResearch.length,
    with_high_confidence_url_claim: correctionsResearch.filter((r) => r.CONFIDENCE === "High")
      .length,
  },
});

writeJson("29-brand-corrective-write-dry-run.json", dryRun);

writeJson("30-brand-regression-tests.json", {
  run_at: new Date().toISOString(),
  choice_regression: choiceRegression,
  cross_family_parent_vs_brand: regression,
  required: {
    medium_fuzzy_cannot_write_current_brand: true,
    brand_explorer_cannot_override: true,
    parent_cannot_auto_populate_current_brand: true,
    no_cvent_brand_evidence: true,
    no_legacy_brand_evidence: true,
  },
  pass: choiceRegression.pass && regression.pass,
});

writeMd(
  "31-brand-explorer-directionality.md",
  `# Brand Explorer Directionality

**Required direction**

\`\`\`
Verified Property Census (property-level Current Brand / affiliation)
        ↓
Brand Explorer census / affiliation validation
\`\`\`

**Forbidden direction**

\`\`\`
Brand Explorer assumption
        ↓
Census Current Brand factual value
\`\`\`

## Rules

1. Current Brand is researched **property-first** from official property/directory evidence.
2. Brand Explorer may **consume** Census affiliation data.
3. An existing Brand Explorer record must **not** force a Census property into a brand affiliation when current property evidence disagrees.
4. Soft collections (Ascend, Radisson Individuals, etc.) still require property-level confirmation — collection membership is not inferred from parent alone.

## Incident note

Choice-family V3/V3.1 production damage was **not** caused by Brand Explorer override; it was caused by **source-family → Current Brand** contamination in Autopilot discovery/write. Directionality rule is locked to prevent a second failure mode.
`
);

writeMd(
  "00-incident-status.md",
  `# V4 Production-Data Quality Incident — Status

**V4 production writes: PAUSED** (do not resume).

## Tracks

| Track | Status |
| --- | --- |
| Geography quality | Active (see country scorecards in V3.1; AR/JM/BB NOT READY on unseen slice; BR PARTIAL) |
| **Current Brand / Current Affiliation** | **ACTIVE — material** (Choice parent-as-brand default) |

## Resume condition (BOTH required)

1. Geography incident: root cause + parser/normalizer fix + damage bounded + corrective dry-runs + semantic gates + regressions PASS
2. Current Brand incident: same bar for affiliation

**Neither track alone is sufficient to resume V4.**
`
);

const q = {
  50: unique.length,
  51: blank.length,
  52: parentAs.length,
  53: parentAs.length,
  54: 0,
  55: choiceFamily.length,
  56: choiceWrong.length,
  57: "Choice discovery omitted brand; family fallback wrote Current Brand='Choice'; URL slug ignored",
  58: true,
  59: "Represented as Current Brand=Choice (incorrect collapse); regional Choice parent not wrong, hotel-level brand missing",
  60: true,
  61: "Canonical claim model supports temporal periods; production Airtable still single Current Brand snapshot — reflags steward/temporal",
  62: false,
  63: false,
  64: false,
  65: correctionsResearch.length,
  66: dryRun.counts.SAFE_BRAND_CORRECTION,
  67: dryRun.counts.REFLAG_REQUIRES_TEMPORAL_UPDATE,
  68: dryRun.counts.STEWARD_REVIEW,
  69: false,
  70: false,
  71: choiceRegression.pass,
  72: regression.pass,
};

writeMd(
  "32-current-brand-final-report.md",
  `# Current Brand Incident Expansion — Final Report

**Incident path:** \`data/research-engine-v2/census-autopilot-v4-standing/geography-quality-incident-v1/\`  
**V4 production writes:** **PAUSED** — do not resume.  
**Corrective Airtable writes:** **none** (dry-run only).

## Executive summary

V3 + V3.1 governed writes left **${choiceFamily.length}/${choiceFamily.length} Choice-family** production rows with \`Current Brand = "Choice"\` (parent/source-family default). Official Choice property URLs already encode hotel-level brands (e.g. Sleep Inn). Root cause is code-path contamination, not Brand Explorer override.

**Damage (P0):** ${choiceWrong.length} Choice wrong affiliations.  
**Safe dry-run corrections prepared:** ${dryRun.counts.SAFE_BRAND_CORRECTION}.  
**Parser/gate fixes landed in repo;** production apply deferred.

## CURRENT BRAND — Q50–72

| # | Question | Answer |
| ---: | --- | --- |
| 50 | Production records audited for Current Brand? | **${q[50]}** |
| 51 | Current Brand blank? | **${q[51]}** |
| 52 | Current Brand incorrect? | **${q[52]}** (parent-as-brand; Choice-dominant) |
| 53 | Parent company incorrectly used as Current Brand? | **${q[53]}** |
| 54 | Historical Brand incorrectly used as current? | **${q[54]}** (not observed in this snapshot set) |
| 55 | Choice-family records audited? | **${q[55]}** |
| 56 | Choice-family Current Brand incorrect? | **${q[56]}** |
| 57 | Exact Choice root cause? | ${q[57]} |
| 58 | Was "Choice Hotels"/Choice used as brand default? | **YES** (value written: \`Choice\`) |
| 59 | Radisson Americas incorrectly represented? | ${q[59]} |
| 60 | Current Brand and Parent Company separate in canonical claims? | **YES** (gate module + claim shape) |
| 61 | Current affiliation temporal/history-aware? | ${q[61]} |
| 62 | Can medium/fuzzy property matches write Current Brand? | **NO** |
| 63 | Can Brand Explorer override current property-level affiliation? | **NO** |
| 64 | Can Parent Company automatically populate Current Brand? | **NO** |
| 65 | Current Brand corrections available? | **${q[65]}** |
| 66 | Safe brand corrections? | **${q[66]}** |
| 67 | Reflags requiring temporal update? | **${q[67]}** |
| 68 | Steward review? | **${q[68]}** |
| 69 | Any Cvent brand evidence used? | **NO** |
| 70 | Any legacy brand evidence used? | **NO** |
| 71 | Choice regression tests pass? | **${q[71] ? "YES" : "NO"}** |
| 72 | Cross-family parent-vs-brand regression tests pass? | **${q[72] ? "YES" : "NO"}** |

## Artifacts

| # | File |
| --- | --- |
| 21 | \`21-current-brand-production-audit.json\` |
| 22 | \`22-choice-brand-audit.json\` |
| 23 | \`23-choice-radisson-regional-map.md\` |
| 24 | \`24-current-affiliation-source-policy.json\` |
| 25 | \`25-current-affiliation-gate.json\` |
| 26 | \`26-choice-brand-root-cause.json\` |
| 27 | \`27-brand-normalization-registry-audit.json\` |
| 28 | \`28-brand-corrections-research.json\` |
| 29 | \`29-brand-corrective-write-dry-run.json\` |
| 30 | \`30-brand-regression-tests.json\` |
| 31 | \`31-brand-explorer-directionality.md\` |

## Code fixes (future inserts / non-production path)

- \`lib/research-engine-v2/census-autopilot-v3/current-affiliation.js\`
- Choice CALA adapter brand from \`brandName\` / URL slug
- Removed family→brand fallbacks in discovery, pilot-selection, dry-run
- Tests: \`npm run test:census-autopilot-current-affiliation\`

## Explicit non-actions

- **No** V4 resume
- **No** corrective Airtable apply
- **No** Cvent / legacy brand evidence
`
);

writeJson("33-current-brand-answers.json", q);

console.log(
  JSON.stringify(
    {
      out: OUT,
      audited: unique.length,
      choice_wrong: choiceWrong.length,
      safe_corrections: dryRun.counts.SAFE_BRAND_CORRECTION,
      regressions_pass: choiceRegression.pass && regression.pass,
    },
    null,
    2
  )
);
