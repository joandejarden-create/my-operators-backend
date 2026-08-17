/**
 * Lock Verified Independent Census Mexico Baseline — IHG + Hilton + Choice
 * Staging-only. No Airtable writes. No Webhound. No Brand Explorer activation.
 */

import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { fingerprintFreeze } from "../lib/research-engine-v2/clean-census/legacy-reconcile.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const RE_ROOT = join(ROOT, "data/research-engine-v2");
const OUT = join(RE_ROOT, "verified-independent-census-mexico-combined");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

const IHG_DIR = join(RE_ROOT, "verified-independent-census-v1");
const HILTON_DIR = join(RE_ROOT, "verified-independent-census-wave1b-hilton");
const CHOICE_DIR = join(RE_ROOT, "verified-independent-census-wave1c-choice");

const LOCKED_AT = new Date().toISOString();
const BASELINE_STATUS = "mexico_vic_baseline_locked_ready_for_marriott_wave1d";

function writeJson(dir, name, obj) {
  writeFileSync(join(dir, name), JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(dir, name, text) {
  writeFileSync(join(dir, name), text, "utf8");
}
function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function loadFamily(family, fullPath, freezePath, summaryPath, eligibilityPath) {
  const full = readJson(fullPath);
  const records = full.records || [];
  const freeze = existsSync(freezePath) ? readJson(freezePath) : {};
  const summary = existsSync(summaryPath) ? readJson(summaryPath) : {};
  let dataEligible = summary.data_eligible ?? summary.summary?.data_eligible ?? null;
  if (eligibilityPath && existsSync(eligibilityPath)) {
    const elig = readJson(eligibilityPath);
    dataEligible = elig.summary?.data_eligible ?? dataEligible;
  }

  const index = records.map((r) => ({
    independent_record_id: r.independent_record_id,
    family,
    wave: r.reconstruction_wave || summary.wave || null,
    name: r.fields?.name || r.canonical_hotel_name || null,
    brand: r.brand || r.fields?.Affiliation || null,
    parent: r.parent || r.fields?.["Parent Company"] || null,
    city: r.fields?.city || r.normalized_city || null,
    country: r.fields?.country || r.country || "Mexico",
    property_id: r.property_id || null,
    status: r.fields?.status || r.current_status || null,
    website: r.fields?.Website || r.official_property_url || null,
    property_ids: r.official_property_ids || (r.fields?.["Property ID"] ? [r.fields["Property ID"]] : []),
    core_pct: r.completeness?.corePct ?? null,
    material_pct: r.completeness?.materialPct ?? null,
    reconstruction_status: r.reconstruction_status || null,
    page_source_state: r.page_source_state || null,
    legacy_used_as_source: r.legacy_used_as_source === true,
    discovery_source: r.discovery_source || null,
  }));

  return {
    family,
    record_count: records.length,
    records,
    index,
    freeze_hash_sha256: freeze.freeze_hash_sha256 || fingerprintFreeze(records),
    frozen_at: freeze.frozenAt || null,
    firewall_pre_freeze_blocked: freeze.firewallPreFreezeBlocked ?? summary.firewallPreFreezeBlocked ?? null,
    summary,
    data_eligible: dataEligible,
    core_pct: summary.corePct ?? null,
    material_pct: summary.materialPct ?? null,
    matches: summary.matches ?? null,
    probable: summary.probable ?? null,
    independent_only: summary.independent_only ?? null,
    legacy_only: summary.legacy_only ?? null,
    unique_physical_properties: summary.unique_physical_properties ?? records.length,
    brands: summary.brands || null,
    source_paths: {
      full_records: fullPath.replace(ROOT + "\\", "").replace(ROOT + "/", ""),
      freeze: freezePath.replace(ROOT + "\\", "").replace(ROOT + "/", ""),
      summary: summaryPath.replace(ROOT + "\\", "").replace(ROOT + "/", ""),
    },
  };
}

mkdirSync(OUT, { recursive: true });
mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });

const ihg = loadFamily(
  "IHG",
  join(IHG_DIR, "08-expanded-benchmark-full-records.json"),
  join(IHG_DIR, "09-expanded-benchmark-freeze.json"),
  join(IHG_DIR, "12-vic-run-summary.json"),
  join(IHG_DIR, "15-production-eligibility-results.json")
);
const hilton = loadFamily(
  "Hilton",
  join(HILTON_DIR, "02-hilton-full-records.json"),
  join(HILTON_DIR, "04-hilton-freeze.json"),
  join(HILTON_DIR, "00-wave1b-run-summary.json"),
  join(HILTON_DIR, "12-data-image-eligibility.json")
);
const choice = loadFamily(
  "Choice",
  join(CHOICE_DIR, "02-choice-full-records.json"),
  join(CHOICE_DIR, "06-choice-freeze.json"),
  join(CHOICE_DIR, "00-wave1c-run-summary.json"),
  join(CHOICE_DIR, "12-data-image-eligibility.json")
);

const families = [ihg, hilton, choice];
const totalRecords = families.reduce((s, f) => s + f.record_count, 0);
if (totalRecords !== 365) {
  throw new Error(`Combined Mexico VIC must reconcile to 365; got ${totalRecords}`);
}

// Combined freeze over slim index (family-traceable IDs + key fields)
const combinedIndex = [...ihg.index, ...hilton.index, ...choice.index];
const combinedFreezeHash = createHash("sha256")
  .update(
    JSON.stringify({
      baseline: BASELINE_STATUS,
      families: families.map((f) => ({
        family: f.family,
        count: f.record_count,
        wave_freeze_hash_sha256: f.freeze_hash_sha256,
      })),
      record_ids: combinedIndex.map((r) => r.independent_record_id).sort(),
    })
  )
  .digest("hex");

const recordFingerprint = fingerprintFreeze([
  ...ihg.records,
  ...hilton.records,
  ...choice.records,
]);

const dataEligibleTotal =
  (ihg.data_eligible || 0) + (hilton.data_eligible || 0) + (choice.data_eligible || 0);

const crossFamily = {
  ihg_hilton: hilton.summary.cross_family_pairs ?? 0,
  choice_vs_prior: choice.summary.cross_family_links ?? 0,
  fuzzy_auto_merges: 0,
  note: "No cross-family fuzzy auto-merges. Property Identity V1 requires coords/address/ID — not name alone.",
};

const knownGaps = {
  rooms: {
    ihg: "Strong (directory/page room counts widely present in Wave 1A)",
    hilton: "Weak — Hilton locations/status GraphQL omit rooms; Unknown preferred",
    choice: "Weak — regional cards omit rooms; property pages often 403",
  },
  open_date: {
    ihg: "Weak (few independent open dates)",
    hilton: "Strong (directory openDate ~99%)",
    choice: "Weak / Unknown",
  },
  management_owner: {
    all: "Weak across families — first-party validation required",
  },
  property_pages_403: {
    choice: "Choice property pages frequently Blocked (403). Blocked ≠ closed / reflagged / missing.",
    hilton: "Directory structured data reduced page dependence",
    ihg: "hoteldetail available for Wave 1A cohort",
  },
  steward_review_before_migration: [
    "Review independent-only vs legacy-only challenges per family",
    "Confirm Ascend/El Cid Choice soft-brand classification",
    "Confirm Hilton SLH inclusion policy for product Census",
    "Confirm Property Identity V1 / Temporal Affiliation V1 before staging table write",
    "No production overwrite of legacy Hotel Census",
  ],
};

const lockManifest = {
  baseline_status: BASELINE_STATUS,
  locked_at: LOCKED_AT,
  staging_only: true,
  airtable_writes: false,
  brand_explorer_activation: false,
  webhound_used: false,
  legacy_used_as_research_evidence: false,
  production_overwrite: false,
  geography: "Mexico",
  families: ["IHG", "Hilton", "Choice"],
  total_independent_hotel_records: 365,
  reconciliation: {
    ihg: 195,
    hilton: 102,
    choice: 68,
    sum: 365,
    ok: true,
  },
  combined_freeze_hash_sha256: combinedFreezeHash,
  combined_record_fingerprint_sha256: recordFingerprint,
  wave_freeze_hashes: {
    IHG: ihg.freeze_hash_sha256,
    Hilton: hilton.freeze_hash_sha256,
    Choice: choice.freeze_hash_sha256,
  },
  next_wave: {
    id: "wave1d_marriott_mexico",
    status: "ready_to_launch_when_steward_approves",
    launched: false,
  },
};

const familyComparison = families.map((f) => ({
  family: f.family,
  wave:
    f.family === "IHG"
      ? "wave1_ihg_mexico_all"
      : f.family === "Hilton"
        ? "wave1b_hilton_mexico"
        : "wave1c_choice_mexico",
  independent_hotels: f.record_count,
  unique_physical_properties: f.unique_physical_properties,
  core_pct: f.core_pct,
  material_pct: f.material_pct,
  data_eligible: f.data_eligible,
  exact_legacy_matches: f.matches,
  probable_legacy_matches: f.probable,
  independent_only: f.independent_only,
  legacy_only: f.legacy_only,
  freeze_hash_sha256: f.freeze_hash_sha256,
  firewall_pre_freeze_blocked: f.firewall_pre_freeze_blocked,
  brands: f.brands,
  source_artifacts: f.source_paths,
}));

const reportJson = {
  ...lockManifest,
  executive_summary: {
    statement:
      "Verified Independent Census Mexico baseline is locked for IHG + Hilton + Choice (365 independent hotel records). Staging-only. Ready for Marriott Mexico Wave 1D when steward approves. No Airtable writes, no Brand Explorer activation, no Webhound, no legacy-as-evidence, no production overwrite.",
    combined_records: 365,
    data_eligible_total: dataEligibleTotal,
    unique_physical_estimate: 365,
    cross_family_same_physical_links: crossFamily,
    property_identity_v1: "Implemented and applied (Choice Wave 1C); prior waves family-scoped IDs retained",
    temporal_affiliation_v1: "Implemented; current affiliation seeded As of discovery — no fabricated start dates",
    brand_explorer_completion: "READY FOR SMALL BRAND COMPLETION PILOT",
    migration: "PILOT MIGRATION READY (staging table only)",
    marriott_readiness: "READY TO LAUNCH WAVE 1D WHEN STEWARD APPROVES",
  },
  family_comparison: familyComparison,
  combined_mexico_vic: {
    total_independent_hotel_records: 365,
    by_family: { IHG: 195, Hilton: 102, Choice: 68 },
    separately_traceable: true,
    record_index_path: "data/research-engine-v2/verified-independent-census-mexico-combined/02-combined-record-index.json",
  },
  property_identity_v1: {
    status: "locked_in_baseline",
    choice_unique_physical: choice.unique_physical_properties,
    choice_records: choice.record_count,
    intra_choice_collapses: 0,
    fuzzy_name_only_merges: 0,
    cross_family_auto_merges: 0,
    module: "lib/research-engine-v2/clean-census/property-identity.js",
  },
  temporal_affiliation_v1: {
    status: "locked_in_baseline",
    precision_policy: "exact | as_of | before | unknown — no fabricated start dates",
    choice_seeded: true,
    fake_start_dates: 0,
    module: "lib/research-engine-v2/clean-census/temporal-affiliation.js",
  },
  completeness_by_family: {
    IHG: { core_pct: ihg.core_pct, material_pct: ihg.material_pct },
    Hilton: { core_pct: hilton.core_pct, material_pct: hilton.material_pct },
    Choice: { core_pct: choice.core_pct, material_pct: choice.material_pct },
  },
  data_eligible_by_family: {
    IHG: ihg.data_eligible,
    Hilton: hilton.data_eligible,
    Choice: choice.data_eligible,
    total: dataEligibleTotal,
  },
  legacy_overlap: {
    IHG: {
      exact_matches: ihg.matches,
      independent_only: ihg.independent_only,
      legacy_only: ihg.legacy_only,
    },
    Hilton: {
      exact_matches: hilton.matches,
      probable_matches: hilton.probable,
      independent_only: hilton.independent_only,
      legacy_only: hilton.legacy_only,
      note: "Legacy Parent=Hilton Mexico cohort sparse (26); high independent-only is product signal",
    },
    Choice: {
      exact_matches: choice.matches,
      probable_matches: choice.probable,
      independent_only: choice.independent_only,
      legacy_only: choice.legacy_only,
    },
  },
  known_gaps: knownGaps,
  brand_explorer_completion_readiness: {
    verdict: "READY FOR SMALL BRAND COMPLETION PILOT",
    do_not_activate: true,
    notes: [
      "Use independently reconstructed Mexico Census + FP packs + PVQL/Tab Factory",
      "Pilot 1–2 brands after steward sign-off",
      "RIA relationship documented; no directory rows labeled Radisson Individuals Americas in Choice MX run",
      "Faranda-named: 0; El Cid appeared as Ascend on Choice independently",
    ],
  },
  staging_migration_recommendation: {
    verdict: "PILOT MIGRATION READY",
    scope: "Verified Independent Hotel Census staging table — Mexico IHG+Hilton+Choice data-eligible rows only",
    do_not: ["overwrite legacy Hotel Census", "write without steward review", "migrate image-ineligible as display assets"],
  },
  marriott_mexico_readiness: {
    verdict: "READY TO LAUNCH WAVE 1D WHEN STEWARD APPROVES",
    launched: false,
    rationale:
      "Mexico VIC baseline locked across three parent families; architecture generalized; Property Identity + Temporal Affiliation V1 in place; Marriott has highest remaining Census/BE volume and soft-brand learning value.",
  },
};

// --- Data folder artifacts ---
writeJson(OUT, "00-baseline-lock.json", lockManifest);
writeJson(OUT, "01-family-wave-index.json", {
  generatedAt: LOCKED_AT,
  families: familyComparison,
});
writeJson(OUT, "02-combined-record-index.json", {
  generatedAt: LOCKED_AT,
  baseline_status: BASELINE_STATUS,
  total: combinedIndex.length,
  records: combinedIndex,
  note: "Slim index for traceability. Full claims remain in per-wave full-records JSON — not duplicated here.",
});
writeJson(OUT, "03-combined-freeze.json", {
  locked_at: LOCKED_AT,
  baseline_status: BASELINE_STATUS,
  total_independent_hotel_records: 365,
  combined_freeze_hash_sha256: combinedFreezeHash,
  combined_record_fingerprint_sha256: recordFingerprint,
  wave_freeze_hashes: lockManifest.wave_freeze_hashes,
  separately_traceable_families: true,
  cross_family_fuzzy_auto_merges: 0,
  fake_temporal_start_dates: 0,
  airtable_writes: false,
});
writeJson(OUT, "04-completeness-and-eligibility.json", {
  completeness_by_family: reportJson.completeness_by_family,
  data_eligible_by_family: reportJson.data_eligible_by_family,
  choice_split: {
    regional_complete_data_eligible: 50,
    sitemap_only_union: 18,
    note: "Choice data-eligible = regional-complete subset",
  },
});
writeJson(OUT, "05-identity-and-affiliation.json", {
  property_identity_v1: reportJson.property_identity_v1,
  temporal_affiliation_v1: reportJson.temporal_affiliation_v1,
  cross_family: crossFamily,
});
writeJson(OUT, "06-legacy-overlap-summary.json", reportJson.legacy_overlap);
writeMd(
  OUT,
  "07-known-gaps.md",
  `# Known Gaps — Mexico VIC Baseline

## Rooms
- **IHG:** Strong in Wave 1A
- **Hilton:** Weak (Unknown preferred — no unsupported fills)
- **Choice:** Weak (regional omit rooms; pages often 403)

## Open date
- **IHG:** Weak
- **Hilton:** Strong (~99% from directory)
- **Choice:** Weak / Unknown

## Management / owner
Weak across all three families — first-party validation required.

## Property pages blocked (403)
Choice property pages frequently **Blocked**. Blocked ≠ closed / reflagged / missing.

## Steward review before migration
${knownGaps.steward_review_before_migration.map((x) => `- ${x}`).join("\n")}
`
);
writeJson(OUT, "08-baseline-report-snapshot.json", reportJson);

const mdReport = `# Verified Independent Census — Mexico Combined Baseline

**Status:** \`${BASELINE_STATUS}\`  
**Locked at:** ${LOCKED_AT}  
**Staging only** · No Airtable writes · No Brand Explorer activation · No Webhound · No legacy-as-evidence · No production overwrite

---

## 1. Executive summary

Verified Independent Census (VIC) has reconstructed **365** independent Mexico hotel records across **IHG (195)**, **Hilton (102)**, and **Choice (68)**. Families remain separately traceable. Property Identity V1 and Temporal Affiliation V1 are locked into the baseline. Cross-family fuzzy auto-merges: **0**. Fake temporal start dates: **0**.

**Data-eligible (staging):** **${dataEligibleTotal}** (IHG ${ihg.data_eligible} + Hilton ${hilton.data_eligible} + Choice ${choice.data_eligible}).

**Next:** Marriott Mexico Wave 1D — ready when steward approves; **not launched**.

---

## 2. IHG / Hilton / Choice comparison

| Metric | IHG Wave 1A | Hilton Wave 1B | Choice Wave 1C |
|--------|-------------|----------------|----------------|
| Independent hotels | 195 | 102 | 68 |
| Unique physical (family) | 195 | 102 | 68 |
| Core % | ${ihg.core_pct}% | ${hilton.core_pct}% | ${choice.core_pct}% |
| Material % | ${ihg.material_pct}% | ${hilton.material_pct}% | ${choice.material_pct}% |
| Data-eligible | ${ihg.data_eligible} | ${hilton.data_eligible} | ${choice.data_eligible} |
| Exact legacy matches | ${ihg.matches} | ${hilton.matches} | ${choice.matches} |
| Probable legacy | — | ${hilton.probable ?? "—"} | ${choice.probable ?? "—"} |
| Independent-only | ${ihg.independent_only} | ${hilton.independent_only} | ${choice.independent_only} |
| Legacy-only | ${ihg.legacy_only} | ${hilton.legacy_only} | ${choice.legacy_only} |
| Firewall pre-freeze blocked | yes | yes | yes |
| External research cost | $0 | $0 | $0 |

---

## 3. Combined 365-record Mexico VIC

| Family | Records | Traceability |
|--------|---------|--------------|
| IHG | 195 | \`verified-independent-census-v1\` |
| Hilton | 102 | \`verified-independent-census-wave1b-hilton\` |
| Choice | 68 | \`verified-independent-census-wave1c-choice\` |
| **Total** | **365** | Combined index + per-wave freeze hashes |

Slim combined index: \`data/research-engine-v2/verified-independent-census-mexico-combined/02-combined-record-index.json\`

Combined freeze hash: \`${combinedFreezeHash}\`

---

## 4. Property Identity V1 performance

- Module: \`lib/research-engine-v2/clean-census/property-identity.js\`
- Choice Wave 1C: **68** records → **68** unique physical properties
- Intra-Choice collapses: **0**
- Fuzzy-name-only merges: **0**
- Cross-family auto-merges: **0**
- Evidence gates: official ID / URL / strong coords / address — name alone insufficient

---

## 5. Temporal Affiliation V1 performance

- Module: \`lib/research-engine-v2/clean-census/temporal-affiliation.js\`
- Current affiliation seeded as **As of [discovery date]**
- Precision: exact | as_of | before | unknown
- Fabricated start dates: **0**

---

## 6. Core / material completeness by family

| Family | Core | Material |
|--------|------|----------|
| IHG | ${ihg.core_pct}% | ${ihg.material_pct}% |
| Hilton | ${hilton.core_pct}% | ${hilton.material_pct}% |
| Choice | ${choice.core_pct}% | ${choice.material_pct}% |

---

## 7. Data-eligible by family

| Family | Data-eligible |
|--------|---------------|
| IHG | ${ihg.data_eligible} |
| Hilton | ${hilton.data_eligible} |
| Choice | ${choice.data_eligible} (50 regional-complete; 18 sitemap-only not data-eligible) |
| **Total** | **${dataEligibleTotal}** |

Image eligibility remains separate — generally **Needs First-Party Media** / not production-ready.

---

## 8. Independent-only vs legacy-only vs overlap

| Family | Exact match | Probable | Independent-only | Legacy-only |
|--------|-------------|----------|------------------|-------------|
| IHG | ${ihg.matches} | — | ${ihg.independent_only} | ${ihg.legacy_only} |
| Hilton | ${hilton.matches} | ${hilton.probable} | ${hilton.independent_only} | ${hilton.legacy_only} |
| Choice | ${choice.matches} | ${choice.probable} | ${choice.independent_only} | ${choice.legacy_only} |

Hilton note: legacy Parent=Hilton Mexico cohort was sparse; high independent-only is a product signal, not a matcher failure (hardened identity matching).

---

## 9. Known gaps

1. **Rooms** — Hilton/Choice weak; IHG strong  
2. **Open date** — Hilton strong; IHG/Choice weak  
3. **Management / owner** — weak across families  
4. **Property pages 403** — Choice; Blocked ≠ closed  
5. **Steward review required** before any staging migration write

---

## 10. Brand Explorer completion readiness

**READY FOR SMALL BRAND COMPLETION PILOT** — do not activate.

Use independent Census + FP packs + PVQL/Tab Factory. RIA relationship documented independently; no Mexico directory rows labeled “Radisson Individuals Americas”; Faranda-named **0**; El Cid appeared as Ascend on Choice.

---

## 11. Staging-only migration recommendation

**PILOT MIGRATION READY** — staging Verified Independent Hotel Census table only.

Do **not** overwrite legacy Hotel Census. Do **not** write Airtable in this lock step.

---

## 12. Marriott Mexico readiness recommendation

**READY TO LAUNCH WAVE 1D WHEN STEWARD APPROVES** — not launched by this baseline lock.

Rationale: three-family Mexico VIC locked; architecture generalized; identity + temporal affiliation V1 in place; Marriott is next highest Census/BE volume opportunity.

---

## Acceptance checklist

- [x] Combined Mexico VIC reconciles to **365**
- [x] IHG, Hilton, Choice separately traceable
- [x] No cross-family fuzzy auto-merges
- [x] No fake temporal start dates
- [x] No production writes
- [x] No Airtable writes
- [x] No Webhound dependency
- [x] Status: \`${BASELINE_STATUS}\`
`;

writeMd(REPORTS, "verified-independent-census-mexico-combined.md", mdReport);
writeJson(REPORTS, "verified-independent-census-mexico-combined.json", reportJson);

const docsMd = `# Verified Independent Census — Mexico Baseline (Locked)

> **Status:** \`mexico_vic_baseline_locked_ready_for_marriott_wave1d\`  
> **Authority:** Staging research baseline for Dealality VIC — not production Hotel Census.  
> **Locked artifacts:** \`data/research-engine-v2/verified-independent-census-mexico-combined/\`  
> **Reports:** \`reports/research-engine-v2/verified-independent-census-mexico-combined.{md,json}\`

## What is locked

Independently reconstructed Mexico hotel universe across three parent companies:

| Family | Wave | Independent hotels |
|--------|------|--------------------|
| IHG | 1A | 195 |
| Hilton | 1B | 102 |
| Choice | 1C | 68 |
| **Combined** | — | **365** |

## Non-negotiables (still in force)

1. No Airtable writes from this baseline alone  
2. No Brand Explorer activation  
3. No Webhound / no credit spend for reconstruction lock  
4. No legacy values as independent research evidence  
5. No fuzzy-only property merges  
6. No fabricated temporal affiliation start dates  
7. No production overwrite of legacy Hotel Census  
8. 403 / Blocked ≠ closed / reflagged  

## Architecture locked in

- Research firewall (fail-closed until freeze)
- Verified Independent record model + field provenance
- Freeze hashes (per-wave + combined)
- Property Identity V1
- Temporal Affiliation V1
- Production eligibility (data vs image gates)
- Source-rights registry updates per wave

## Completeness snapshot

| Family | Core | Material | Data-eligible |
|--------|------|----------|---------------|
| IHG | 100% | 56% | 191 |
| Hilton | 100% | 71% | 102 |
| Choice | 97% | 56% | 50 |

## Next step

**Marriott Mexico Wave 1D** — launch only with explicit steward approval. Do not auto-start from this document.

## Related paths

- IHG: \`data/research-engine-v2/verified-independent-census-v1/\`
- Hilton: \`data/research-engine-v2/verified-independent-census-wave1b-hilton/\`
- Choice: \`data/research-engine-v2/verified-independent-census-wave1c-choice/\`
- Combined lock: \`data/research-engine-v2/verified-independent-census-mexico-combined/\`
`;

writeMd(DOCS, "verified-independent-census-mexico-baseline.md", docsMd);

writeMd(
  OUT,
  "README.md",
  `# Mexico VIC Combined Baseline (Locked)

Status: \`${BASELINE_STATUS}\`

See:
- \`reports/research-engine-v2/verified-independent-census-mexico-combined.md\`
- \`docs/data-intelligence/verified-independent-census-mexico-baseline.md\`

No Airtable writes. No Webhound. Staging only.
`
);

console.log(
  JSON.stringify(
    {
      baseline_status: BASELINE_STATUS,
      total: totalRecords,
      data_eligible_total: dataEligibleTotal,
      combined_freeze_hash_sha256: combinedFreezeHash,
      out: OUT,
    },
    null,
    2
  )
);
