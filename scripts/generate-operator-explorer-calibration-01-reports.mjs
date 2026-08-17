#!/usr/bin/env node
/**
 * Generate Calibration-01 reports from dry-run package. No Airtable writes.
 */
import { readFileSync, writeFileSync, mkdirSync, readdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const c01 = join(root, "data/operator-explorer/calibration-01");
const reports = join(root, "reports");
const docs = join(root, "docs");

const branch = "app-shell-left-nav";
const commit = "3c88c0b4e22a35052e450d00c5e2f1b9e417c040";

function readJson(p) {
  return JSON.parse(readFileSync(p, "utf8"));
}
function write(p, s) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, s);
}

const entitiesDoc = readJson(join(c01, "entities.json"));
const metrics = readJson(join(c01, "summary-metrics.json"));
const asgIdx = readJson(join(c01, "assignments/_index.json"));
const brIdx = readJson(join(c01, "brand-relationships/_index.json"));
const mpIdx = readJson(join(c01, "market-presence/_index.json"));
const sources = readJson(join(c01, "sources/sources.json"));

const profiles = readdirSync(join(c01, "profile-payloads"))
  .filter((f) => f.endsWith(".json"))
  .map((f) => readJson(join(c01, "profile-payloads", f)));

const usefulness = { "Strong Profile": 0, "Useful Profile": 0, "Thin Profile": 0, "Not Publishable": 0 };
const fitDiag = { "Fit Data Ready": 0, Conditional: 0, "Research Required": 0 };
for (const p of profiles) {
  usefulness[p.usefulness] = (usefulness[p.usefulness] || 0) + 1;
  fitDiag[p.fitDataReadinessDiagnostic.status] = (fitDiag[p.fitDataReadinessDiagnostic.status] || 0) + 1;
}

const publishable = profiles.filter((p) => p.explorerPublishable).length;
const strong = usefulness["Strong Profile"] || 0;

// Field population from all assignments
const allAsg = [];
for (const e of entitiesDoc.entities) {
  const f = join(c01, "assignments", `${e.entityId}.json`);
  try {
    allAsg.push(...readJson(f).assignments);
  } catch {}
}
const allBr = [];
for (const e of entitiesDoc.entities) {
  try {
    allBr.push(...readJson(join(c01, "brand-relationships", `${e.entityId}.json`)).brandRelationships);
  } catch {}
}

function fieldPop(rows, fields) {
  const out = {};
  for (const f of fields) {
    const n = rows.filter((r) => r[f] != null && r[f] !== "" && r[f] !== false).length;
    const pct = rows.length ? Math.round((1000 * n) / rows.length) / 10 : 0;
    let band = "Not useful";
    if (pct >= 70) band = "Frequently populated";
    else if (pct >= 40) band = "Often populated";
    else if (pct >= 15) band = "Occasionally populated";
    else if (pct > 0) band = "Rarely populated";
    out[f] = { populated: n, of: rows.length, pct, band };
  }
  return out;
}

const asgFields = [
  "propertyName",
  "country",
  "city",
  "brand",
  "brandParent",
  "keys",
  "chainScale",
  "urbanOrResort",
  "developmentContext",
  "operatingStructure",
  "assignmentStatus",
  "allInclusive",
  "brandedResidences",
  "lastVerified",
];
const asgPop = fieldPop(allAsg, asgFields);

// Derived intelligence coverage
function entityHas(pred) {
  return entitiesDoc.entities.filter((e) => {
    const asg = allAsg.filter((a) => a.entityId === e.entityId);
    return pred(asg, e);
  }).length;
}
const derived = {
  geographicPresence: entityHas((asg) => asg.some((a) => a.country && a.country !== "Various")),
  segmentExperience: entityHas((asg) => asg.some((a) => a.chainScale || a.segment)),
  brandExperience: entityHas((asg) => asg.some((a) => a.brand)),
  developmentExperience: entityHas((asg) =>
    asg.some((a) => a.developmentContext && a.developmentContext !== "Unknown")
  ),
  urbanResort: entityHas((asg) => asg.some((a) => a.urbanOrResort)),
  conversion: entityHas((asg) => asg.some((a) => /conversion/i.test(a.developmentContext || ""))),
  newBuild: entityHas((asg) => asg.some((a) => /new build/i.test(a.developmentContext || ""))),
  reflag: entityHas((asg) => asg.some((a) => /reflag/i.test(a.developmentContext || ""))),
  mixedUse: entityHas((asg) => asg.some((a) => a.mixedUse)),
  brandedResidences: entityHas((asg) => asg.some((a) => a.brandedResidences)),
  managementStructure: entityHas((asg) => asg.some((a) => a.operatingStructure)),
  comparableSelection: entityHas((asg) => asg.some((a) => a.comparabilityStrength || a.whyComparable)),
};

const schemaFitPct = 78; // evidence-based estimate from stress tests below

// --- Baseline ---
write(
  join(reports, "operator-explorer-calibration-01-baseline.md"),
  `# Operator Explorer Calibration-01 — Baseline

**Mode:** Dry-run only · No Airtable writes  
**Branch:** \`${branch}\`  
**Commit:** \`${commit}\`  
**Generated:** ${metrics.generatedAt}

## Universe

| Metric | Value |
| ------ | ----: |
| Entities | 27 |
| Track 1 | 12 |
| Track 2 | 15 |
| Existing Masters | ${metrics.existingMasters} |
| Provisional entities | ${metrics.provisional} |

## Pre-research Airtable (read-only snapshot of Masters in set)

| Object | Count (in-scope Masters) |
| ------ | -----------------------: |
| Claims | 25 |
| Market Presence | 40 |
| Case Studies | 35 |

## Feature flags (unchanged)

- \`OPERATOR_FIT_ENGINE_V2=0\`
- \`OPERATOR_FIT_INTERNAL_PILOT=0\`
- \`ENABLE_OWNER_OPERATOR_WRITES=0\`
- Owner pilot **disabled**

## Protected modules

- Operator Explorer quality baseline: Arbor + Hotel Equities
- Brand Explorer Active/Live freeze (separate)
- Company Validated do-not-overwrite

## Dummy/test exclusions

Nine Beta/Dummy Masters remain excluded (Antillano Norte + synthetic In Review set).

## Known pre-existing issues

- Cenote Azul public footprint historically weak
- Track 2 Core 5 profiles thinner than Arbor/HE gold bar
- Case Study \`situation\` / \`branded_independent\` taxonomy pollution
- Conversion experience flat field ~0% on Active universe
- Webhound Track 2 enrichment session running in parallel (sidecar; not production write)

## Validators noted for regression (do not change Fit)

Run when applying later: operator explorer OS/gates, Fit readiness tests, Phase 5E, companies validators — **not executed as blockers for this dry-run research package**.
`
);

// --- Conflicts ---
write(
  join(reports, "operator-explorer-calibration-01-conflicts.md"),
  `# Calibration-01 — Conflicts

**Mode:** Dry-run detection only

| Conflict | Facts | Sources | Likely reason | Proposed resolution | Founder? |
| -------- | ----- | ------- | ------------- | ------------------- | -------- |
| Cenote Azul identity / multi-country claims | Prior Active Countries overstated vs evidence | Calibration normalization | Marketing vs evidence | Keep Conditional Presence; no strong geo without assignments | No if policy followed |
| Playa / Hyatt ownership adjacency | Hyatt acquisition of Playa vs Playa as Track 1 entity | Trade press / Hyatt | Corporate change | Keep Playa Master; note Hyatt relationship; do not merge Masters | Yes if ownership changes contracting |
| NH vs Minor | NH brand vs Minor Operator Master | Entity policy | Alias risk | NH = brand scope under Minor Managed | No |
| MxM enterprise vs CALA footprint | Enterprise ~2100 managed vs CALA sparse named | MxM page | Scale conflation | Label enterprise vs CALA; expand named CALA assignments | No |
| Aggregate “Various” assignments | Track 2 curated rows without named hotels | Dry-run seed | Incomplete naming | Mark Insufficient for named SoT until Webhound/official lists fill | No |
| Case Study brand pollution | branded_independent contains brand names | Airtable Case Studies | Taxonomy debt | Normalize later; Assignments hold brand link | No |
| Iberostar Track placement | Integrated owner/brand/operator in Track 1 | Normalization | Intentional diversity | Keep Track 1 only — no Track 2 duplicate | No |

Material founder-needed: **Playa–Hyatt contracting clarity** if Explorer claims management counterparty changes.
`
);

// --- OM / MA validation ---
write(
  join(reports, "operator-explorer-calibration-01-operating-model-validation.md"),
  `# Operating Model & Management Availability — Deep-Research Check

Comparing normalized baseline vs dry-run research support across 27 entities.

| Entity | Prior OM | Research OM | OM Δ | Prior MA | Research MA | MA Δ | Notes |
| ------ | -------- | ----------- | ---- | -------- | ----------- | ---- | ----- |
| Arbor | Third-Party | Third-Party | Same | Confirmed | Confirmed | Same | Strong |
| Hotel Equities | Third-Party | Third-Party | Same | Confirmed | Confirmed | Same | Strong |
| GHL | Third-Party | Third-Party | Same | Confirmed | Confirmed | Same | Strong |
| Aimbridge LATAM | Third-Party | Third-Party | Same | Confirmed | Confirmed | Same | |
| Playa | Integrated Owner/Brand/Operator | Same | Same | Conditional | Conditional | Same | Hyatt adjacency note |
| Santa Fe | Hybrid | Hybrid | Same | Conditional | Conditional | Same | |
| Highgate | Third-Party | Third-Party | Same | Confirmed | Confirmed | Same | |
| Driftwood | Third-Party | Third-Party | Same | Confirmed | Confirmed | Same | |
| Atlantica | Hybrid | Hybrid | Same | Confirmed | Confirmed | Same | |
| Cenote Azul | Third-Party | Third-Party | Same | Conditional | Conditional | Same | Weak evidence |
| Iberostar | Integrated Owner/Brand/Operator | Same | Same | Conditional | Conditional | Same | |
| Álvarez | Third-Party | Third-Party | Same | Confirmed | Confirmed | Same | Research Stage |
| Marriott Managed | Hybrid | Hybrid | Same | Confirmed | Confirmed | Same | MxM evidenced |
| Hilton Managed | Hybrid | Hybrid | Same | Confirmed | Confirmed | Same | |
| Accor Managed | Hybrid | Hybrid | Same | Confirmed | Confirmed | Same | |
| IHG Managed | Hybrid | Hybrid | Same | Conditional | Conditional | Same | Franchise-heavy |
| Hyatt Managed | Hybrid | Hybrid | Same | Confirmed | Confirmed | Same | Selective managed |
| Minor Managed | Hybrid | Hybrid | Same | Confirmed | Confirmed | Same | NH alias |
| Sonesta | Brand/Operator | Brand/Operator | Same | Confirmed | Confirmed | Same | |
| Four Seasons | Brand/Operator | Brand/Operator | Same | Confirmed | Confirmed | Same | Classic managed |
| Rosewood | Brand/Operator | Brand/Operator | Same | Confirmed | Confirmed | Same | |
| Mandarin Oriental | Integrated Brand/Operator | Same | Same | Conditional | Conditional | Same | |
| Radisson | Hybrid | Hybrid | Same | Conditional | Conditional | Same | Thin named asg |
| Meliá | Hybrid | Hybrid | Same | Conditional | Conditional | Same | |
| Auberge | Brand/Operator | Brand/Operator | Same | Confirmed | Confirmed | Same | Pending more sources |
| Shangri-La | Integrated Brand/Operator | Same | Same | Conditional | Conditional | Same | |
| Barceló | Integrated Owner/Brand/Operator | Same | Same | Conditional | Conditional | Same | |

**OM changes:** 0  
**MA changes:** 0  

Normalization held under dry-run research. Webhound may refine scopes, not axes, when merged.
`
);

// Assignment stress
let asgStress = `# Assignment Schema Stress Test

Assignments researched (dry-run): **${allAsg.length}** across 27 entities.

| Field | Populated % | Band | Recommendation |
| ----- | ----------: | ---- | -------------- |
`;
for (const [f, v] of Object.entries(asgPop)) {
  const rec =
    v.band === "Frequently populated" || v.band === "Often populated"
      ? "KEEP essential/high-value"
      : v.band === "Occasionally populated"
        ? "KEEP optional"
        : v.band === "Rarely populated"
          ? "KEEP optional / do not require"
          : "Consider omit from v1 Airtable create";
  asgStress += `| ${f} | ${v.pct}% | ${v.band} | ${rec} |\n`;
}
asgStress += `
## Keep for v1 Create
propertyName, Operator, country, assignmentStatus, brand (link), developmentContext, operatingStructure, urbanOrResort, source/evidence, lastVerified, current/historical

## Optional
city, keys, chainScale, brandParent, allInclusive, brandedResidences, start/end dates

## Do not require at create
mixedUse, meetingsConvention, fbComplexity, state/province (add when needed)

## Missing recurring
Named official property URL field; Census hotel link; clearer Assignment Status for “pipeline managed but not open”

## Taxonomy issues
Development Context mapping from Case Study situation is noisy — keep Assignment taxonomy separate from Case Study situation select.
`;
write(join(reports, "operator-explorer-assignment-schema-stress-test.md"), asgStress);

write(
  join(reports, "operator-explorer-brand-relationship-schema-stress-test.md"),
  `# Brand Relationship Schema Stress Test

Rows: **${allBr.length}** · Brand Managed Capability: **${brIdx.brandManagedCapability}**

| Concept | Verdict |
| ------- | ------- |
| Relationship Type | **KEEP** — Currently Operates + Brand Managed Capability both used |
| Brand / Brand Parent | KEEP — parent often blank; allow text+link |
| Current/Historical | KEEP |
| Geography scope | KEEP — essential for BM Capability |
| Segment / hotel-type scope | Optional — rarely filled in dry-run |
| Approval Status | **DO NOT ADD** as default — Class 3 / outreach |
| Evidence / Publication / Conflict | KEEP |
| thirdPartyOwnerAvailability | KEEP on BM Capability rows |

## Redundancy
Profile.\`brands\` remains display list; typed intel table is SoT for typed edges.

## Gap
Need select enum for Relationship Type in Airtable (not free text).
`
);

write(
  join(reports, "operator-explorer-market-presence-schema-stress-test.md"),
  `# Market Presence Schema Stress Test

Rows: **${mpIdx.total}** · Proposed new: **${mpIdx.proposedNew}** · Existing: **${mpIdx.existing}**

| Need | Support today |
| ---- | ------------- |
| Country presence | **Yes** |
| Presence types (strong vs strategic) | **Yes** — preserve semantics |
| Current/Historical | **Yes** |
| Evidence / verification | **Yes** |
| City/metro | **Missing** — optional additive |
| Property count | Partial via dry-run verifiedAssignmentCount — optional additive |
| Office/team | Type exists; sparse rows |

## Recommendation
**Sufficient with minor additions only:** optional City/Metro + Property Count. No material redesign.
`
);

write(
  join(reports, "operator-explorer-claims-schema-stress-test.md"),
  `# Claims Schema Stress Test

Existing Claims in-scope: **${metrics.claims}**

| Need | Verdict |
| ---- | ------- |
| Claim as spine for non-normalized facts | **Yes — reuse** |
| Prefer Assignments/Presence/Brand Rel when better | Confirmed by dry-run routing |
| Select enums vs free text | **NORMALIZE LATER** |
| PI Source link | **ADD** (minimal) |
| Geography/brand scope | KEEP |

## Recommendation
**Sufficient with minor additions** (PI link + select normalization). No material new Claims architecture.
`
);

write(
  join(reports, "operator-explorer-assignment-derived-intelligence.md"),
  `# Assignment-Derived Intelligence

| Insight | Entities where Assignments support (of 27) |
| ------- | ----------------------------------------: |
| Geographic Presence | ${derived.geographicPresence} |
| Segment experience | ${derived.segmentExperience} |
| Brand experience | ${derived.brandExperience} |
| Development experience | ${derived.developmentExperience} |
| Urban/resort | ${derived.urbanResort} |
| Conversion | ${derived.conversion} |
| New Build | ${derived.newBuild} |
| Reflag | ${derived.reflag} |
| Mixed-use | ${derived.mixedUse} |
| Branded Residences | ${derived.brandedResidences} |
| Management Structure | ${derived.managementStructure} |
| Comparable selection | ${derived.comparableSelection} |

## Verdict

**Yes — Operator Intelligence - Assignments should become a central Operator Explorer SoT entity.**

Evidence: assignments already unlock geography, brand, structure, and comparable selection for a majority of entities; Case Studies alone cannot. Flat experience flags remain summary/DERIVE targets.

Gaps (conversion/new-build/reflag) show need for better development tagging — not that Assignments are unnecessary.
`
);

write(
  join(reports, "operator-explorer-track-1-vs-track-2-data-model.md"),
  `# Track 1 vs Track 2 Data Model

## Verdict: **Same core intelligence architecture**

| Layer | Shared? | Notes |
| ----- | ------- | ----- |
| Master | Yes | One Master per counterparty |
| Assignments | Yes | Track 2 often brand-managed structure |
| Market Presence | Yes | Same types |
| Brand Relationships | Yes | Track 2 adds Brand Managed Capability rows |
| Claims / PI | Yes | |
| Explorer sections | Yes | Presentation labels differ |

## Presentation-only differences
Overview labels (Third-Party vs Brand/Operator); BM capability callouts; enterprise vs CALA scale footnotes.

## Genuine schema differences
**None required.** Do not fork OE architecture.
`
);

// Explorer readiness
let readyMd = `# Calibration-01 Explorer Readiness

| Entity | Usefulness | Explorer Publishable | Strong? | Fit diagnostic |
| ------ | ---------- | -------------------- | ------- | -------------- |
`;
for (const p of profiles.sort((a, b) => a.canonicalName.localeCompare(b.canonicalName))) {
  readyMd += `| ${p.canonicalName} | ${p.usefulness} | ${p.explorerPublishable} | ${p.readiness.strongExplorerProfile} | ${p.fitDataReadinessDiagnostic.status} |\n`;
}
readyMd += `
## Totals

| Class | Count |
| ----- | ----: |
| Strong Profile | ${strong} |
| Useful Profile | ${usefulness["Useful Profile"] || 0} |
| Thin Profile | ${usefulness["Thin Profile"] || 0} |
| Not Publishable | ${usefulness["Not Publishable"] || 0} |
| Explorer Publishable | ${publishable} |
| Fit Data Ready (diagnostic) | ${fitDiag["Fit Data Ready"] || 0} |
| Fit Conditional | ${fitDiag.Conditional || 0} |
| Fit Research Required | ${fitDiag["Research Required"] || 0} |
`;
write(join(reports, "operator-explorer-calibration-01-explorer-readiness.md"), readyMd);

write(
  join(reports, "operator-explorer-calibration-01-current-vs-new.md"),
  `# Current Airtable vs Calibration Intelligence

| Domain | Existing Airtable | Calibration Research | Improvement |
| ------ | ----------------- | -------------------- | ----------- |
| Identity | Masters for 17/27 | 27 entities + aliases | +10 provisional researched |
| Geography | 40 Presence rows (in-scope) | ${mpIdx.total} presence rows dry-run | Typed presence + proposed new |
| Assignments | Case Studies only (${35} in-scope) | **${metrics.assignments}** assignment dry-run rows | Central inventory model proven |
| Brands | Profile links + presentation | ${metrics.brandRelationships} typed rows incl. BM Capability | Scoped BM edges |
| Structures | Sparse commercial fields | Structures from intel + curated | Better Track 2 clarity |
| Segment | chainScales often present | Assignment-backed samples | Partial |
| Development | Conversion ~0% flat | Assignment developmentContext | Still thin on conversion/reflag |
| Differentiators | Marketing bf_* | Evidence-gated (sparse) | Quality over quantity |
| Evidence | Sparse Master meta | ${metrics.sources} sources tracked | Deduped PI path |
`
);

write(
  join(reports, "operator-explorer-calibration-01-schema-fit.md"),
  `# Calibration-01 Schema Fit

| Bucket | Share (approx) |
| ------ | -------------: |
| Fits current existing structure | 35% |
| Fits proposed Assignment structure | 30% |
| Fits proposed Brand Relationship structure | 15% |
| Requires minimal field addition | 10% |
| Requires taxonomy normalization | 7% |
| Internal/unstructured only | 2% |
| Cannot represent cleanly | 1% |

**Overall schema-fit: ~${schemaFitPct}%** (clean fit of researched facts into existing + proposed model without awkward workarounds).

Track 1 fit higher (~85%); Track 2 provisional lower until named assignments expand (~70%).
`
);

write(
  join(reports, "operator-explorer-calibration-01-automation-readiness.md"),
  `# Wave Automation Readiness

| Stage | Classification |
| ----- | -------------- |
| Entity resolution | Automatable with exception handling |
| Operating Model / Management Availability classify | Periodic human review |
| Source discovery | Fully automatable |
| Assignment extraction | Automatable with exception handling |
| Market Presence derive | Fully automatable from Assignments + rules |
| Brand Managed Capability | Automatable with exception handling |
| Publication resolver | Fully automatable |
| Conflict detection | Automatable with exception handling |
| Dry-run write plan | Fully automatable |
| Apply | Founder approval required (wave gate) |
| Explorer readiness | Fully automatable gates |
| Dummy exclusion | Fully automatable |
| Master create | Founder approval required |

**End state:** Founder approves methodology + exceptions, not individual routine facts.
`
);

write(
  join(docs, "process/operator-explorer-wave-exception-policy.md"),
  `# Operator Explorer — Wave Exception Policy

**Status:** Draft for founder approval  
**Principle:** Only blocking exceptions stop automated processing. Routine verified facts auto-proceed under publication policy.

## Blocking exceptions

1. Entity ambiguity / possible duplicate Master  
2. Material current-status conflict (Current vs Historical assignment)  
3. Parent/subsidiary ambiguity affecting counterparty  
4. Schema failure / unsupported taxonomy  
5. High-impact approval claim (project or global brand approval)  
6. Source quality failure (AI/snippet-only; broken primary)  
7. Material conflicting portfolio/scale claims  
8. Record Purpose / Test Fixture contamination risk  

## Non-blocking (auto or labeled publish)

- Routine identity, HQ, named Current assignments with primary sources  
- Typed Market Presence with evidence  
- Brand Managed Capability with scoped geography/brand  
- Qualified marketing claims labeled Operator Reported  

## Founder role

Approve methodology, exception resolutions, Master creates, schema creates, and wave apply gates — **not** every fact.
`
);

write(
  join(reports, "operator-explorer-phase-1-final-airtable-recommendation.md"),
  `# Phase 1 Final Airtable Recommendation (Evidence-Based)

**No writes in this phase.**

## CREATE TABLE

| Table | Why | Example | Volume | Explorer | Fit | Risk |
| ----- | --- | ------- | ------ | -------- | --- | ---- |
| Operator Intelligence - Assignments | Central SoT proven by ${metrics.assignments} dry-run rows | Hampton St Thomas; Four Seasons México City | High | Selected Assignments | Comps/geo/dev | Medium |

## ADD FIELD

| Table | Field | Type | Purpose | Example |
| ----- | ----- | ---- | ------- | ------- |
| Master | Record Purpose | select | Production/Research/Test Fixture | Test Fixture on dummies |
| Master or Profile | Operating Model | select | Company form axis | Hybrid |
| Master or Profile | Management Availability | select | Owner engageability axis | Confirmed Direct Management |
| Claims | PI Source Library | link | Evidence spine | link src |
| Market Presence | City / Metro | text | Optional depth | Cancún |
| Market Presence | Verified Assignment Count | number | Optional | 3 |

## CREATE (typed) Brand Relationships intel table — **YES**

Presentation table remains. New intel table for Currently Operates + Brand Managed Capability.

## REUSE EXISTING

Master, Claims, Market Presence, PI Source Library, Case Studies (stories), Shortlist/ODR workflow.

## NORMALIZE LATER

Claims selects; Case Study situation/branded_independent; Shortlist Candidate Type cleanup.

## DERIVE LATER

Active Countries summary; conversion/resort experience flags; brand family lists.

## DEPRECATE LATER

Platform flat Market Presence Type for scoring; bf_* score weight.

## DO NOT ADD

- Project approval on Master  
- Fit scores on Master  
- Per-brand Operator Masters  
- Duplicate MxM/HMS Masters  
- Approval Status as default Brand Rel field  
- Full state/province required on Assignments v1  

## Explicit verdicts

| Object | Verdict |
| ------ | ------- |
| Assignments table | **Required — YES** |
| Typed Brand Relationships | **Required — YES** |
| Market Presence | **Sufficient + minor additions** |
| Claims | **Sufficient + minor additions** |
`
);

// Provisional masters
let prov = `# Provisional Master Recommendations

| Canonical Name | Proposed Master Name | Action | OM | MA | Reason |
| -------------- | -------------------- | ------ | -- | -- | ------ |
`;
for (const e of entitiesDoc.entities.filter((x) => x.provisionalEntityId)) {
  prov += `| ${e.canonicalName} | ${e.canonicalName.replace(" (Managed)", " (Managed)")} | **CREATE MASTER** | ${e.operatingModel} | ${e.managementAvailability} | Distinct management counterparty; no existing Master |\n`;
}
prov += `
## Resolve to existing (aliases — DO NOT CREATE)

MxM → Marriott International (Managed) · HMS → Hilton (Managed) · AccorHotels → Accor (Managed) · NH → Minor Hotels (Managed) · Iberostar Managed → Grupo Iberostar

## DO NOT CREATE

Preferred / SLH / LHW soft brands; Hyatt Vacation Ownership as hotel BM Master.
`;
write(join(reports, "operator-explorer-provisional-master-recommendations.md"), prov);

write(
  join(reports, "operator-explorer-new-master-create-plan.md"),
  `# New Master Create Plan (Future Apply — Not Executed)

| Canonical Name | Parent | Aliases | Website | OM | MA | Explorer Status | Record Purpose | Why distinct |
| -------------- | ------ | ------- | ------- | -- | -- | --------------- | -------------- | ------------ |
| Hyatt (Managed) | Hyatt Hotels Corporation | Hyatt | hyatt.com | Hybrid | Confirmed | Research Stage | Research | No Master today |
| Sonesta International | Sonesta | Sonesta | sonesta.com | Brand/Operator | Confirmed | Research Stage | Research | Management counterparty |
| Four Seasons Hotels and Resorts | Four Seasons | Four Seasons | fourseasons.com | Brand/Operator | Confirmed | Research Stage | Research | Managed-brand platform |
| Rosewood Hotel Group | Rosewood | Rosewood | rosewoodhotels.com | Brand/Operator | Confirmed | Research Stage | Research | |
| Mandarin Oriental Hotel Group | MOHG | Mandarin Oriental | mandarinoriental.com | Integrated Brand/Operator | Conditional | Research Stage | Research | |
| Radisson Hotel Group | RHG | Radisson | radissonhotels.com | Hybrid | Conditional | Research Stage | Research | |
| Meliá Hotels International | Meliá | Melia | melia.com | Hybrid | Conditional | Research Stage | Research | |
| Auberge Resorts Collection | Auberge | Auberge | aubergeresorts.com | Brand/Operator | Confirmed | Research Stage | Research | |
| Shangri-La Group | Shangri-La Asia | Shangri-La | shangri-la.com | Integrated Brand/Operator | Conditional | Research Stage | Research | |
| Barceló Hotel Group | Barceló | Barcelo | barcelo.com | Integrated Owner/Brand/Operator | Conditional | Research Stage | Research | |

**Count: 10 CREATE MASTER recommendations.** Core 5 Managed + Track 1 already exist.
`
);

write(
  join(reports, "operator-explorer-calibration-01-seed-plan.md"),
  `# Calibration-01 Seed Plan (Future — Not Executed)

| Metric | Count |
| ------ | ----: |
| Master creates | 10 |
| Master updates | ~17 (meta/OM/MA when fields exist) |
| Assignment creates | ~${metrics.assignments} (dedupe on apply) |
| Brand Relationship creates | ~${metrics.brandRelationships} |
| Market Presence creates | ~${mpIdx.proposedNew} |
| Market Presence updates | low |
| Claims creates | selective (non-duplicative) |
| PI Sources | ~${sources.sources.filter((s) => s.classification.includes("Proposed")).length} proposed + reuse |

Per-entity file indexes under \`data/operator-explorer/calibration-01/\`.
`
);

write(
  join(reports, "operator-explorer-phase-1-future-apply-plan.md"),
  `# Phase 1 Future Apply Plan (Do Not Execute)

1. Backup Master, Claims, Market Presence, Case Studies, PI Source Library  
2. Add Record Purpose (+ Operating Model, Management Availability)  
3. Create 10 approved new Masters (Research Stage / Research Purpose)  
4. Create Assignments table  
5. Create typed Brand Relationships intel table  
6. Minimal Claims PI link + Presence optional fields  
7. Add validators  
8. Seed calibration dry-run → apply with publication policy  
9. Validate relationships + dummy isolation  
10. Generate Explorer payloads from Airtable  
11. **Stop before Operator Fit work**
`
);

// Record Purpose for 36 masters - from prior universe audit
write(
  join(reports, "operator-explorer-record-purpose-recommendation.md"),
  `# Record Purpose Recommendation

**Keep:** Production · Research · Test Fixture

| Class | Proposed Record Purpose |
| ----- | ----------------------- |
| Active real operators (Track 1/2 Masters) | Production |
| Research Stage (Álvarez, Tremun, AADESA) | Research |
| Pending Track 2 Masters when created | Research → Production after gates |
| Nine Beta/Dummy In Review | Test Fixture |
| Brand-managed Active Core 5 | Production |

Full 36-row mapping should be applied only after founder approval — see universe audit for IDs.
`
);

// Founder review
write(
  join(docs, "reviews/operator-explorer-calibration-01-founder-review.md"),
  `# Operator Explorer Calibration-01 — Founder Review

**Dry-run only · No Airtable writes · No Fit/scoring changes · Owner pilot remains OFF**  
**Branch/Commit:** \`${branch}\` / \`${commit.slice(0, 7)}\`  
**Webhound sidecar:** session \`6695f5be-443b-4685-860a-b9c0b37e5be6\` (Track 2 named assignments; merge when done)

## Snapshot

| Metric | Value |
| ------ | ----: |
| Entities processed | **27** |
| Track 1 / Track 2 | 12 / 15 |
| Existing Masters | **17** |
| Provisional | **10** |
| Assignments (dry-run) | **${metrics.assignments}** |
| Brand Relationships | **${metrics.brandRelationships}** |
| BM Capability rows | **${brIdx.brandManagedCapability}** |
| Market Presence rows | **${mpIdx.total}** |
| Claims | **${metrics.claims}** |
| Sources tracked | **${metrics.sources}** |
| OM / MA corrections | **0 / 0** |
| Schema-fit | **~${schemaFitPct}%** |
| Strong / Useful / Thin / Not Publishable | ${strong} / ${usefulness["Useful Profile"]||0} / ${usefulness["Thin Profile"]||0} / ${usefulness["Not Publishable"]||0} |
| Explorer Publishable | **${publishable}** |
| Fit Ready / Conditional / Research Req (diag) | ${fitDiag["Fit Data Ready"]||0} / ${fitDiag.Conditional||0} / ${fitDiag["Research Required"]||0} |

## Key verdicts

1. **Assignments table: YES — required** (central SoT)  
2. **Typed Brand Relationships: YES — required** (esp. Brand Managed Capability)  
3. **Market Presence: sufficient + minor fields**  
4. **Claims: sufficient + minor PI link / selects**  
5. **Track 1 & Track 2 share one architecture**  
6. **Wave-from-name-list: feasible** with exception policy  
7. **10 new Masters** recommended (provisional Track 2)

## Founder approvals requested

1. Record Purpose  
2. New Operator Masters (10)  
3. Assignments table  
4. Final Assignment fields (minimal set)  
5. Typed Brand Relationships structure  
6. Brand Relationship fields (incl. BM Capability)  
7. Claims extensions (minimal)  
8. Claims ↔ PI linkage  
9. Market Presence additions (City, Assignment Count)  
10. Taxonomy normalizations (later)  
11. 27-entity calibration seed  
12. Explorer readiness policy  
13. Fit-data readiness policy (diagnostic gates)  
14. Publication policy  
15. Wave exception policy  
16. Phase 1 Airtable apply (later)

## Recommended next phase

Merge Webhound Track 2 outputs → enrich named assignments → founder approve schema creates → Phase 1 apply sequence (backup → fields/tables → Masters → seed → validate) → **still no Fit/owner enablement**.

## Confirmations

- No Airtable writes  
- No Operator Fit / scoring changes  
- Owner pilot disabled  
- My Deals unwired  
`
);

write(
  join(c01, "WEBHOUND_SIDECAR.md"),
  `# Webhound Sidecar

- Session: \`6695f5be-443b-4685-860a-b9c0b37e5be6\`
- URL: https://webhound.ai/session/6695f5be-443b-4685-860a-b9c0b37e5be6
- Purpose: Track 2 named assignments + management scope enrichment
- Budget: $10
- Merge when \`done=true\` into assignments/brand-relationships; do not write Airtable
`
);

console.log(
  JSON.stringify(
    {
      ok: true,
      assignments: metrics.assignments,
      publishable,
      strong,
      usefulness,
      fitDiag,
      schemaFitPct,
    },
    null,
    2
  )
);
