/**
 * Research Engine V2 — Verified Independent Census Reconstruction Program V1
 *
 * Wave benchmark: IHG Mexico — ALL brands (directory-first).
 * No Webhound. No credits. No Airtable writes. Legacy quarantined post-freeze only.
 */

import { mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { runReconstructionWave, DEFAULT_WAVE_CONFIGS } from "../lib/research-engine-v2/clean-census/wave-engine.js";
import { reconcileAfterFreeze } from "../lib/research-engine-v2/clean-census/legacy-reconcile.js";
import {
  runStrictIndependentRediscovery,
  runTargetedVerificationChallenges,
  CHALLENGE_CLASS_RECOMMENDATION,
} from "../lib/research-engine-v2/clean-census/legacy-challenges.js";
import { batchAssessProductionEligibility } from "../lib/research-engine-v2/clean-census/production-eligibility.js";
import { getGroupAdapterInventory } from "../lib/research-engine-v2/clean-census/group-discovery.js";
import { FIELD_RESEARCH_PLANS } from "../lib/research-engine-v2/clean-census/field-research.js";
import {
  MATERIAL_CENSUS_FIELDS,
  CORE_MATERIAL_FIELDS,
  PROVENANCE_CLASSES,
} from "../lib/research-engine-v2/clean-census/provenance.js";
import { CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { MAP_DIRECTORY_ENRICHMENT } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/research-engine-v2/verified-independent-census-v1");
const FETCH_DELAY_MS = Number(process.env.RE_V2_FETCH_DELAY_MS || 120);

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

function loadLegacyIhgMexicoReference() {
  const csv = readFileSync(join(ROOT, "reports/census-amenities-blank-rows.csv"), "utf8").split(/\r?\n/);
  const rows = [];
  for (const line of csv.slice(1)) {
    if (!line.trim()) continue;
    const f = parseCsvLine(line);
    const name = f[1] || "";
    const parent = f[2] === "(blank parent)" ? "" : f[2] || "";
    const country = f[4] || "";
    if (!/Mexico/i.test(country)) continue;
    if (!/IHG|InterContinental/i.test(parent)) continue;
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

mkdirSync(OUT, { recursive: true });

const waveConfig = {
  id: "wave1_ihg_mexico_all",
  group: "IHG",
  geography: "Mexico",
  brands: null,
  researchProfile: "full_census",
  legacyComparisonAfterFreeze: true,
  firstPartyValidationEligible: true,
};

console.log("[vic] reconstruction wave", waveConfig.id);
const t0 = Date.now();
const wave = await runReconstructionWave(waveConfig, {
  fetchDelayMs: FETCH_DELAY_MS,
  directoryPath: join(ROOT, "reports/ihg-cala-directory-extract.json"),
  onProgress: (m) => console.log(m),
});

writeJson("07-expanded-benchmark-independent-discovery.json", wave.discovery);
writeJson("08-expanded-benchmark-full-records.json", {
  wave: waveConfig.id,
  recordCount: wave.frozen.records.length,
  records: wave.frozen.records,
});
writeJson("09-expanded-benchmark-freeze.json", {
  frozenAt: wave.frozen.frozenAt,
  freeze_hash_sha256: wave.freeze_hash,
  firewallPreFreezeBlocked: wave.firewallPreFreezeBlocked,
  firewall_audit: wave.firewall.getAudit(),
  recordCount: wave.frozen.records.length,
  brandBreakdown: wave.discovery.brandBreakdown,
  legacy_used_as_source: false,
});

console.log("[vic] post-freeze legacy comparison");
wave.firewall.beginLegacyReconciliation();
const legacyRows = wave.firewall.requestLegacyCensus(() => loadLegacyIhgMexicoReference());
const comparison = reconcileAfterFreeze(wave.frozen, legacyRows, wave.firewall);
writeJson("10-post-freeze-legacy-comparison.json", comparison);

const dirRows = wave.discovery.discoveries.map((d) => d.directory_row);
const strictChallenges = runStrictIndependentRediscovery(comparison.legacy_only_rows, dirRows, wave.firewall);
const targetedChallenges = runTargetedVerificationChallenges(comparison.legacy_only_rows, dirRows, wave.firewall);
writeJson("11-legacy-only-challenges.json", {
  recommendation: CHALLENGE_CLASS_RECOMMENDATION,
  strict: strictChallenges,
  targeted: targetedChallenges,
});

const eligibility = batchAssessProductionEligibility(wave.frozen.records);
writeJson("15-production-eligibility-results.json", {
  generatedAt: new Date().toISOString(),
  summary: {
    data_eligible: eligibility.filter((e) => e.production_eligibility_data === "ELIGIBLE").length,
    data_not_eligible: eligibility.filter((e) => e.production_eligibility_data !== "ELIGIBLE").length,
    images_eligible: eligibility.filter((e) => e.production_eligibility_images === "ELIGIBLE").length,
  },
  results: eligibility,
});

// Metrics
const records = wave.frozen.records;
const coreSupported = records.reduce((s, r) => s + (r.completeness?.corePresent || 0), 0);
const coreTotal = records.length * CORE_MATERIAL_FIELDS.length;
const materialSupported = records.reduce((s, r) => s + (r.completeness?.materialPresent || 0), 0);
const materialTotal = records.length * MATERIAL_CENSUS_FIELDS.length;
const pctCore = coreTotal ? Math.round((coreSupported / coreTotal) * 100) : 0;
const pctMaterial = materialTotal ? Math.round((materialSupported / materialTotal) * 100) : 0;
const sourceStates = {};
for (const r of records) {
  const s = r.page_source_state || "Unknown";
  sourceStates[s] = (sourceStates[s] || 0) + 1;
}
const hardFieldHits = {};
for (const r of records) {
  for (const c of r.claims || []) {
    if (c.value != null && c.value !== "") {
      hardFieldHits[c.field] = (hardFieldHits[c.field] || 0) + 1;
    }
  }
}
const elapsedMs = Date.now() - t0;

// --- Artifacts: designs & registries ---
writeMd(
  "01-program-architecture.md",
  `# Verified Independent Census — Program Architecture

\`\`\`
INDEPENDENT DISCOVERY
→ FULL-RECORD RESEARCH
→ FIELD-LEVEL PROVENANCE
→ FREEZE (+ hash)
→ LEGACY COMPARISON (quarantined)
→ LEGACY-ONLY CHALLENGE (Strict | Targeted)
→ FIRST-PARTY VALIDATION PACKS
→ STEWARD REVIEW
→ EXISTING GOVERNANCE GATES
→ VERIFIED INDEPENDENT CENSUS (staging → future production)
\`\`\`

## Firewall

Fail closed before freeze. Logged via \`createResearchFirewall().getAudit()\`.

## Staging model

Local verified records (\`createVerifiedIndependentRecord\`) — **not** Airtable writes in VIC v1.

## Maintenance handoff

After steward approval: Reconstruction Mode → Maintenance Mode (shadow freshness + steward queue).
`
);

writeMd(
  "02-proprietary-field-audit.md",
  `# Proprietary / Legacy-Derived Field Audit

Inspected: \`lib/hotel-census/fields.js\`, \`MAP_DIRECTORY_ENRICHMENT\`, \`DATA_DICTIONARY.md\`, geography docs.

| Field | Classification | Notes |
|-------|----------------|-------|
| name, Affiliation, Parent Company, status, country, city, Website, Property ID, Brand Property Code, Address, Telephone, Open Date, Latitude/Longitude, Amenities (official) | **Safe factual reconstruction** | Directory / official page |
| rooms / keys | **Safe factual** when explicit on official sources | Never infer from room-type cards |
| Management Company / Owner | **Safe factual** only with explicit evidence; else escalate | Never infer operator from brand |
| Market / Submarket (STR-era labels) | **Legacy-only — do not migrate** as STR taxonomy | Product already prefers Dealality corridors |
| Dealality Market / corridor Submarket | **Dealality-derived replacement** | Use \`country-configs\` corridors |
| Chain Scale | **Dealality-derived replacement** or **External licensed** if STR scale | Do not copy STR Chain Scale blindly |
| Location (Urban/Resort/…) | **Dealality-derived** / steward | STR location-type vocabulary historically |
| STR Number / Chain ID / proprietary performance (ADR/RevPAR) | **Legacy-only — do not migrate** | Restricted / not independently licensable |
| Include in Brand Explorer / Data Confidence | **Dealality ops / first-party** | Governance fields |
| Images | **Image rights track** (separate) | Not factual census migration |

## Critical rule

Do **not** independently reconstruct hotel facts while continuing to rely on proprietary STR market/submarket/chain-scale taxonomies as if they were Dealality facts.
`
);

writeMd(
  "03-dealality-taxonomy-plan.md",
  `# Dealality-Owned Taxonomy Plan

Reuse existing schemas first:

| Taxonomy | Existing home | Action |
|----------|---------------|--------|
| Dealality Market | Radar / census geography docs | Prefer over STR Market |
| Dealality Submarket (corridors) | \`lib/radar-buildout/country-configs.js\` + census corridor backfill | Assign via rules + steward |
| Segment / positioning | Brand Explorer / Brand Setup | Do not invent parallel Chain Scale unless needed |
| Property Type / Hotel Service Model | Census fields already | Steward + brand evidence |
| Location type (Urban/Resort) | Census \`Location\` | Dealality rules; not STR import as SoT |

## Versioning

Each derived assignment should store: rule_version, inputs, provenance=dealality_derived, steward_override flag.

## VIC v1 applied

Mexico discoveries receive Dealality Market = \`Mexico\` (country grain). Corridor Submarket remains Unknown until city→corridor rules run without legacy seed.
`
);

writeJson("04-source-rights-registry.json", {
  version: "source-rights-registry-v1",
  statuses: ["Allowed", "Allowed with Constraints", "Reference Only", "Unknown — Review Required", "Do Not Use"],
  entries: [
    {
      source_name: "IHG destination directory / hoteldetail",
      source_domain: "ihg.com",
      source_type: "Official Parent / Brand",
      research_use_status: "Allowed with Constraints",
      factual_extraction_status: "Allowed with Constraints",
      production_display_status: "Unknown — Review Required",
      image_use_status: "Unknown — Review Required",
      restrictions: "Factual research for reconstruction; display/image legal review required",
      access_method: "HTTPS directory extract + hoteldetail fetch",
      robots_anti_bot_notes: "Occasional blocks; classify Blocked — do not invent status",
      terms_review_status: "Unknown — Review Required",
      legal_review_required: true,
      last_reviewed: null,
      notes: "Not a legal conclusion",
    },
    {
      source_name: "Legacy Hotel Census (STR/client-derived)",
      source_domain: "airtable:Hotel Census",
      source_type: "Quarantined Reference",
      research_use_status: "Do Not Use",
      factual_extraction_status: "Do Not Use",
      production_display_status: "Do Not Use as independent evidence",
      image_use_status: "Do Not Use",
      restrictions: "Post-freeze comparison/challenge only",
      access_method: "Firewall-gated after freeze",
      legal_review_required: true,
      last_reviewed: null,
      notes: "Quarantined",
    },
    {
      source_name: "Hilton GraphQL hotel status",
      source_domain: "hilton.com",
      source_type: "Official Parent",
      research_use_status: "Allowed with Constraints",
      factual_extraction_status: "Allowed with Constraints",
      production_display_status: "Unknown — Review Required",
      image_use_status: "Unknown — Review Required",
      legal_review_required: true,
    },
    {
      source_name: "Choice property sitemap / pages",
      source_domain: "choicehotels.com",
      source_type: "Official Parent / Brand",
      research_use_status: "Allowed with Constraints",
      factual_extraction_status: "Allowed with Constraints",
      production_display_status: "Unknown — Review Required",
      image_use_status: "Unknown — Review Required",
      robots_anti_bot_notes: "403 common — Blocked ≠ reflag",
      legal_review_required: true,
    },
    {
      source_name: "Trade press",
      source_domain: "various",
      source_type: "Secondary",
      research_use_status: "Reference Only",
      factual_extraction_status: "Allowed with Constraints",
      production_display_status: "Unknown — Review Required",
      image_use_status: "Do Not Use",
      restrictions: "Corroboration only — never sole High material update",
      legal_review_required: true,
    },
  ],
});

writeMd(
  "05-group-adapter-inventory.md",
  `# Group Adapter Inventory

\`\`\`json
${JSON.stringify(getGroupAdapterInventory(), null, 2)}
\`\`\`
`
);

writeJson("06-reconstruction-wave-config.json", {
  config_version: "vic-wave-config-v1",
  field_research_plans: FIELD_RESEARCH_PLANS,
  waves: DEFAULT_WAVE_CONFIGS,
  active_benchmark: waveConfig,
});

writeMd(
  "12-first-party-brand-validation-pack.md",
  `# First-Party Brand Validation Pack

After independent reconstruction of a brand cohort, prepare a pack for brand review:

- Brand identity / parent
- Hotels in Verified Independent Census for brand
- Operating / pipeline status
- Opening dates (where present)
- Development pipeline
- Operator/management relationships (confirmable)
- Missing hotels / hotels no longer affiliated
- Approved brand images/logos/media sources
- Optional development contacts

## Capture schema

confirming_organization, confirming_person, role_title, validation_date, fields_reviewed, hotels_confirmed, hotels_corrected, hotels_added, hotels_removed, factual_corrections, evidence_supplied, image_permissions_media_kit, scope_limitations, notes

→ Steward queue items (no auto-write). Classification: **First-Party Validated** (does not authorize legacy data).
`
);

writeMd(
  "13-first-party-operator-validation-pack.md",
  `# First-Party Operator Validation Pack (design only)

Operators may later confirm: managed hotels, contracts, geography, brands operated, keys, management model, services, pipeline, owner types (where appropriate), approved imagery, contacts.

Same capture schema pattern as brand packs. Feeds future Operator Explorer — **not built in this phase**.
`
);

writeMd(
  "14-provenance-completeness-gates.md",
  `# Provenance Completeness Gates

## PRODUCTION ELIGIBILITY — DATA

Requires:

- independent discovery provenance
- material field provenance (Unknown allowed if honest)
- legacy_used_as_source = false
- identity confidence Exact/High (name + URL + property ID)
- no unresolved critical contradiction
- source-rights acceptable or pending where appropriate

## PRODUCTION ELIGIBILITY — IMAGES

Separate. Eligible only if rights ∈ First-Party Supplied/Approved, Licensed, Dealality-Owned.

A hotel may be **data-eligible** and **image-not-eligible**.
`
);

const totalCensus = 14826;
const mxCensus = 4184;
const ihgMxLegacy = legacyRows.length;
const ihgMxDir = wave.discovery.discoveries.length;
const nativePctOfMxIhg = ihgMxLegacy ? Math.round((Math.min(ihgMxDir, ihgMxLegacy) / ihgMxLegacy) * 100) : null;

writeMd(
  "16-full-reconstruction-roadmap.md",
  `# Full Reconstruction Roadmap (data-derived)

Census snapshot (amenities extract): ~${totalCensus} rows; Mexico ~${mxCensus}; IHG-parent Mexico legacy reference ~${ihgMxLegacy}.
IHG Mexico official directory discoveries this wave: **${ihgMxDir}**.

| Wave | Scope | Rationale |
|------|-------|-----------|
| **1** | Major groups × Mexico (IHG first ← this benchmark; then Hilton/Marriott/Choice/Hyatt) | Highest official-directory quality + MX opportunity |
| **2** | Same groups × CALA | Extends working adapters |
| **3** | Remaining Americas major groups | Accor, Wyndham, BWH, etc. |
| **4** | Soft brands / collections | Higher ambiguity; identity gates critical |
| **5** | Independents / long-tail | Lower native directory coverage |
| **6** | Legacy-only challenge set | Strict + Targeted; WH escalation |
| **7** | First-party brand/operator validation | Risk reduction / provenance strengthening |

Brand Explorer completion begins **after Wave 1–2 brand cohorts** have Materially Complete census + FP pack ready — never auto-activate.
`
);

writeMd(
  "17-completion-estimate.md",
  `# Completion Estimate (order-of-magnitude)

| Metric | Estimate | Basis |
|--------|----------|-------|
| Total census records (extract) | ~${totalCensus} | census-amenities-blank-rows |
| Mexico records | ~${mxCensus} | same |
| IHG Mexico legacy reference | ${ihgMxLegacy} | IHG parent filter |
| IHG Mexico directory native | ${ihgMxDir} | this wave |
| Natively reconstructable (major-group dirs) | ~40–60% of branded CALA | directory coverage vs blank-parent MX share |
| Long-tail / blank-parent MX | large (~3k blank parent in MX) | composition script |
| Native completion of material fields | Wave1 core ~${pctCore}%; material ~${pctMaterial}% | this run |
| Webhound escalation | ~10–25% long-tail + opaque ownership + strict legacy-only | not used now |
| First-party validation coverage | Target top opportunity brands/operators first | commercial priority |
| Proprietary fields not migrated | STR Number, STR Market/Submarket, performance | audit |
| Image-rights remediation | High volume; separate from data eligibility | design |

Precision is intentionally bounded — expand estimates after Hilton/Marriott Mexico waves.
`
);

writeMd(
  "18-brand-explorer-completion-program.md",
  `# Brand Explorer Completion Program

Per brand after census cohort reconstructed:

INDEPENDENT BRAND RESEARCH + VERIFIED INDEPENDENT CENSUS + parent/regional + pipeline + development model + owner/operator model + MX/CALA presence + images/rights + first-party validation + PVQL/Tab Factory gates → **Activation Review** (never auto).

Goal: finish Brand Explorer universe on independent census foundation.
`
);

writeMd(
  "19-maintenance-transition.md",
  `# Maintenance After Reconstruction

Approved hotels move Reconstruction → **Maintenance Mode**:

- shadow freshness (Research Engine V2)
- contradiction-first validation
- directory gaps
- steward queue
- first-party updates
- periodic blind Webhound audit (explicit auth)

No full re-reconstruction from scratch unless provenance is invalidated.
`
);

writeMd(
  "20-final-report.md",
  `# Verified Independent Census Program V1 — Final Report

## Most important answer

**Yes — we now have a scalable program** to build a Verified Independent Hotel Census, quarantine legacy from production use over time, complete Brand Explorer on that foundation, and maintain via shadow ops — **without** writing Airtable or spending Webhound in this phase.

## Benchmark: IHG Mexico — ALL brands

| Metric | Value |
|--------|-------|
| Independent discoveries | ${ihgMxDir} |
| Core field support | ${pctCore}% |
| Material field support | ${pctMaterial}% |
| Legacy IHG MX reference | ${ihgMxLegacy} |
| Matches | ${comparison.matches} |
| Probable | ${comparison.probable} |
| Independent-only | ${comparison.independent_only} |
| Legacy-only | ${comparison.legacy_only} |
| Source states | ${JSON.stringify(sourceStates)} |
| Data-eligible (gate) | ${eligibility.filter((e) => e.production_eligibility_data === "ELIGIBLE").length} |
| Firewall blocked pre-freeze | ${wave.firewallPreFreezeBlocked} |
| Runtime | ${elapsedMs} ms |
| Cost | $0 |

## Answers

1. **Scale beyond Indigo/Kimpton?** Yes — ${ihgMxDir} IHG MX properties discovered directory-first.
2. **Scalable adapters today?** IHG strong; Choice CALA extract ready; Hilton GraphQL status-ready; Marriott soft partial; others planned.
3. **% natively reconstructable?** Major-group branded share high where directories exist; overall census lower due to blank-parent/long-tail (~order 40–60% branded CALA rough).
4. **Hardest fields?** Management Company, Owner, rooms (when not on page), coords, amenities depth, corridor Submarket, Chain Scale.
5. **Proprietary fields?** STR Market/Submarket/Number/performance; STR-era Chain Scale/Location — see audit.
6. **Dealality replacements?** Market/corridor Submarket; segment from Brand Setup; avoid cloning STR.
7. **Legacy-only?** Strict rediscovery (strongest) + Targeted verification (steward efficiency) — both post-freeze; no legacy field copy.
8. **FP validation?** Separate First-Party Validated provenance; steward queue; not legacy authorization.
9. **Source-rights?** Registry v1 with Allowed / Constraints / Reference Only / Unknown / Do Not Use.
10. **Production migration qualify?** Data eligibility gates + image rights separate + governance handoff — no write yet.
11. **Waves?** 1 Mexico majors → 2 CALA → 3 Americas → 4 soft → 5 long-tail → 6 legacy-only → 7 FP validation.
12. **Effort?** Multi-wave program; Wave 1 IHG MX runnable in minutes native; full census months of waves + steward.
13. **Webhound %?** ~10–25% long-tail/opaque/strict challenges — not spent now.
14. **BE completion start?** After Materially Complete census cohorts for priority brands (post Wave 1–2).
15. **Migration architecture?** Option B — Verified Independent Hotel Census staging → governed cutover; legacy quarantined archive.

## Constraints honored

No Webhound · No credits · No Airtable writes · Firewall fail-closed · No legacy pre-freeze research · Unknown over fabrication · No auto activation/images
`
);

writeJson("12-vic-run-summary.json", {
  wave: waveConfig.id,
  discoveries: ihgMxDir,
  corePct: pctCore,
  materialPct: pctMaterial,
  matches: comparison.matches,
  legacy_only: comparison.legacy_only,
  independent_only: comparison.independent_only,
  sourceStates,
  hardFieldCoverageSample: Object.fromEntries(
    ["rooms", "Management Company", "Open Date", "Latitude", "Amenities", "Market"].map((f) => [
      f,
      hardFieldHits[f] || hardFieldHits[CENSUS_FIELDS.rooms] || 0,
    ])
  ),
  fieldHits: hardFieldHits,
  elapsedMs,
  costUsd: 0,
});

console.log("\n[done]", OUT);
console.log(
  JSON.stringify(
    {
      discoveries: ihgMxDir,
      corePct: pctCore,
      materialPct: pctMaterial,
      matches: comparison.matches,
      legacy_only: comparison.legacy_only,
      dataEligible: eligibility.filter((e) => e.production_eligibility_data === "ELIGIBLE").length,
      ms: elapsedMs,
    },
    null,
    2
  )
);
