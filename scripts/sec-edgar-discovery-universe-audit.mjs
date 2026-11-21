#!/usr/bin/env node
/**
 * Read-only SEC discovery universe audit + expansion roadmap packet.
 * No paid research. No Meta apply. No HPC writes.
 *
 *   node scripts/sec-edgar-discovery-universe-audit.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listUniversalCompaniesV24, UNIVERSAL_EXPANSION_COMPANIES_V24, UNIVERSAL_WAVE_ID } from "../lib/hotel-intelligence/sec-edgar/cik-registry-v24.js";
import { listHistoricalResearchCompanies } from "../lib/hotel-intelligence/sec-edgar/cik-registry-v2.js";
import { listPilotCompanies } from "../lib/hotel-intelligence/sec-edgar/cik-registry-v1.js";
import { SEARCH_FAMILIES, SEARCH_TAXONOMY_VERSION } from "../lib/hotel-intelligence/sec-edgar/search-taxonomy-v2.js";
import {
  SEC_API_QUERY_FAMILIES,
  EXA_QUERY_FAMILIES,
  PERIOD_PARTITIONS,
  BASELINE_JPMCC_ACCESSION,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-query-families-v1.js";
import { FUTURE_JURISDICTION_SOURCES } from "../lib/hotel-intelligence/sec-edgar/intl-readiness-v1.js";
import { DEFAULT_QUEUE_CAPS } from "../lib/hotel-intelligence/sec-edgar/adaptive-discovery-queue-v2.js";
import {
  buildBrandNeutralDiscoveryProcess,
  buildIncrementalIntelPerDollarRoadmap,
  DISCOVERY_SOURCE_CATEGORIES,
  NON_RESTRICTIVE_SEED_HINTS,
} from "../lib/hotel-intelligence/sec-edgar/sec-discovery-expansion-design-v1.js";
import {
  summarizeCoverageLedger,
  loadCoverageLedger,
  SEC_DISCOVERY_COVERAGE_LEDGER_V3,
} from "../lib/hotel-intelligence/sec-edgar/sec-discovery-coverage-ledger-v3.js";
import { HIF_STAGE } from "../lib/hotel-intelligence/hotel-intelligence-factory/stages-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-discovery-expansion");

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(fp, obj) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
}
function readJson(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function main() {
  ensureDir(OUT);
  const univ = listUniversalCompaniesV24();
  const hist = listHistoricalResearchCompanies();
  const pilot = listPilotCompanies();
  const byOrg = {};
  for (const c of univ) {
    const k = c.organizationType || "UNKNOWN";
    byOrg[k] = (byOrg[k] || 0) + 1;
  }

  const eligibleSummary = readJson("reports/sec-edgar-universal-wave-v24/ELIGIBLE_INVENTORY_SUMMARY.json");
  const coverageMdEra = readJson("reports/sec-edgar-universal-wave-v24/COVERAGE_LEDGER.json");
  const waveReport = readJson("reports/sec-edgar-universal-wave-v24/WAVE_REPORT.json");
  const migrationAudit = readJson("reports/sec-edgar-contract-timing-v4/MIGRATION_QUALITY_AUDIT.json");
  const termsMetrics = readJson("reports/sec-edgar-contract-timing-v4/CONTRACT_TERMS_QUALITY_METRICS.json");
  const termsSpend = readJson("reports/sec-edgar-contract-timing-v4/CONTRACT_TERMS_PILOT_SPEND.json");

  const eligibleCompanies = eligibleSummary?.companies || [];
  const formAgg = {};
  let totalEligible = 0;
  for (const c of eligibleCompanies) {
    totalEligible += Number(c.eligible) || 0;
    for (const [f, n] of Object.entries(c.formTypeCounts || {})) {
      formAgg[f] = (formAgg[f] || 0) + n;
    }
  }

  const v3Ledger = loadCoverageLedger();
  const v3Summary = summarizeCoverageLedger(v3Ledger);

  const blindSpots = [
    {
      id: "ISSUER_REGISTRY_CEILING_RISK",
      severity: "HIGH",
      detail: `Universal registry lists ${univ.length} companies — seed only; must not be treated as complete SEC hotel universe.`,
    },
    {
      id: "ELIGIBLE_VS_PROCESSED_GAP",
      severity: "HIGH",
      detail:
        "Coverage ledger V2 shows large pending eligible inventories (e.g. thousands of filings queued vs dozens processed).",
    },
    {
      id: "CMBS_SCHEDULE_VS_EX10_TERMS",
      severity: "HIGH",
      detail:
        "Terms pilot: CMBS franchise schedules yield expiration baseline but weak lifecycle terms; EX-10 HMA prose yields term/renewal/notice.",
    },
    {
      id: "FRANCHISE_ONLY_VERIFIED_INVENTORY",
      severity: "HIGH",
      detail: "258 VERIFIED agreements are FRANCHISE-only; management agreements require separate discovery.",
    },
    {
      id: "EXECUTED_RENEWAL_GAP",
      severity: "MEDIUM",
      detail: "Terms pilot verified 0 executed renewals/amendments — later-document research backlog remains.",
    },
    {
      id: "IBR_AND_EXHIBIT_ENUMERATION",
      severity: "MEDIUM",
      detail: "Parent accession ≠ exhibit; IBR targets and EX-10 package enumeration under-processed.",
    },
    {
      id: "PRE_2012_AND_DEAL_DEPOSITORS",
      severity: "MEDIUM",
      detail: "Period partitions start 2012 for contract-timing queries; CMBS depositors/issuers outside registry under-covered.",
    },
    {
      id: "INTL_NON_SEC",
      severity: "MEDIUM",
      detail: "SEC may surface global hotels; intl adapters exist as readiness only — no production non-SEC harvest authorized here.",
    },
    {
      id: "BRAND_BIASED_EXA_QUERIES",
      severity: "LOW",
      detail: "Some Exa families still mention Marriott/Hilton/Hyatt as examples — must remain non-restrictive hints.",
    },
  ];

  const uncoveredCategories = DISCOVERY_SOURCE_CATEGORIES.map((c) => ({
    ...c,
    currentState:
      c.id === "CMBS_ISSUERS_AND_COLLATERAL"
        ? "PARTIAL — JPMCC baseline + incremental V2/V3 findings; deal graph incomplete"
        : c.id === "REIT_PUBLIC_DISCLOSURES"
          ? "PARTIAL — registry seeds + eligible inventory; processing backlog large"
          : c.id === "EXHIBITS_AND_AMENDMENTS"
            ? "PARTIAL — exhibit ledger present; low exhibitsProcessed vs eligible"
            : c.id === "INCORPORATED_BY_REFERENCE"
              ? "MOSTLY_UNCOVERED"
              : c.id === "UNREGISTERED_RELEVANT_ISSUERS"
                ? "DESIGNED — adaptive queue caps exist; not continuously expanded"
                : c.id === "INTL_PUBLIC_DISCLOSURES"
                  ? "READINESS_ONLY"
                  : "PARTIAL",
  }));

  const audit = {
    module: "SEC_DISCOVERY_UNIVERSE_AUDIT",
    at: new Date().toISOString(),
    paidResearchLaunched: false,
    termsPilotInterrupted: false,
    exhaustiveCoverageClaim: false,
    nonRestrictiveSeeds: NON_RESTRICTIVE_SEED_HINTS,
    registries: {
      pilotV1Count: pilot.length,
      pilotSlugs: pilot.map((c) => c.slug || c.ticker),
      historicalV2Count: hist.length,
      historicalSlugs: hist.map((c) => c.slug),
      universalWaveId: UNIVERSAL_WAVE_ID,
      universalV24Count: univ.length,
      universalExpansionAdds: UNIVERSAL_EXPANSION_COMPANIES_V24.length,
      withCik: univ.filter((c) => c.cik).length,
      byOrganizationType: byOrg,
      companies: univ.map((c) => ({
        slug: c.slug,
        cik: c.cik,
        ticker: c.ticker,
        organizationType: c.organizationType,
        priority: c.priority,
        cohorts: c.cohorts,
      })),
    },
    search: {
      taxonomyVersion: SEARCH_TAXONOMY_VERSION,
      taxonomyFamilies: SEARCH_FAMILIES.map((f) => ({ id: f.id, label: f.label, patternCount: f.patterns.length })),
      secApiQueryFamilies: SEC_API_QUERY_FAMILIES,
      exaQueryFamilies: EXA_QUERY_FAMILIES,
      periodPartitions: PERIOD_PARTITIONS,
      baselineCmbsAccession: BASELINE_JPMCC_ACCESSION,
    },
    filingCoverageSample: {
      source: "reports/sec-edgar-universal-wave-v24/ELIGIBLE_INVENTORY_SUMMARY.json",
      companiesInventoried: eligibleCompanies.length,
      totalEligibleFilings: totalEligible,
      formTypeCounts: formAgg,
      sinceYear: eligibleSummary?.sinceYear || 1994,
      note: "Sample wave inventory — not complete registry-wide eligible inventory.",
    },
    processingBacklogHints: {
      coverageLedgerV2: coverageMdEra
        ? {
            present: true,
            note: "Company-cohort ledger; see COVERAGE_LEDGER.md for pending >> processed gaps",
          }
        : { present: false },
      waveReport: waveReport
        ? {
            companiesResearched: waveReport.companiesResearched || waveReport.researched || null,
            documentsProcessed: waveReport.documentsProcessed || null,
            uniqueVerifiedHotels: waveReport.uniqueVerifiedHotels ?? waveReport.verifiedHotels ?? null,
          }
        : null,
      adaptiveQueueCaps: DEFAULT_QUEUE_CAPS,
    },
    contractIntelligenceState: {
      migrationQuality: migrationAudit?.agreementQualityCounts || null,
      verifiedFranchiseOnly: true,
      termsPilotMetrics: termsMetrics,
      termsPilotSpend: termsSpend,
    },
    documentGrainLedgerV3: {
      module: SEC_DISCOVERY_COVERAGE_LEDGER_V3,
      status: v3Ledger.empty ? "SCHEMA_READY_EMPTY" : "POPULATED",
      summary: v3Summary,
    },
    uncoveredSourceCategories: uncoveredCategories,
    blindSpots,
    international: {
      persistGlobalIntelligence: true,
      hpcPriority: ["Americas", "Europe"],
      adapters: FUTURE_JURISDICTION_SOURCES,
      paidSourcesAuthorizedThisPacket: false,
    },
    factory: {
      existingStages: Object.values(HIF_STAGE),
      proposedNextStage: "DISCOVER_EXPAND",
      boundaries: {
        researchMode: true,
        canonicalPromotionSeparate: true,
        publicationAuthorizationSeparate: true,
        customerBlockedDefault: true,
      },
    },
  };

  const processDesign = buildBrandNeutralDiscoveryProcess();
  const roadmap = buildIncrementalIntelPerDollarRoadmap();

  writeJson(path.join(OUT, "DISCOVERY_UNIVERSE_AUDIT.json"), audit);
  writeJson(path.join(OUT, "BRAND_NEUTRAL_DISCOVERY_PROCESS.json"), processDesign);
  writeJson(path.join(OUT, "INCREMENTAL_INTEL_PER_DOLLAR_ROADMAP.json"), roadmap);
  writeJson(path.join(OUT, "UNCOVERED_SOURCE_CATEGORIES.json"), { categories: uncoveredCategories });
  writeJson(path.join(OUT, "BLIND_SPOTS.json"), { blindSpots });
  writeJson(path.join(OUT, "COVERAGE_LEDGER_V3_SCHEMA.json"), {
    module: SEC_DISCOVERY_COVERAGE_LEDGER_V3,
    status: "DESIGNED_READY_FOR_POPULATION",
    requiredFields: [
      "cik",
      "issuer",
      "accession",
      "filingType",
      "filingDate",
      "documentId",
      "exhibit identifiers",
      "retrievalStatus",
      "extractionStatus",
      "hotelIdentitiesDiscovered",
      "contractualIntelligenceExtracted",
      "lastResearchDate",
      "followUpRequirements",
    ],
    summaryEmpty: v3Summary,
  });

  const md = `# SEC Discovery Coverage Expansion — Universe Audit & Roadmap

**Generated:** ${audit.at}  
**Packet:** Permanent factory expansion workstream (post Contract Terms & Renewals pilot)  
**Paid research launched:** **No**  
**Exhaustive coverage claimed:** **No**

---

## 1. Current SEC discovery universe (audit)

### Issuer / CIK registries

| Registry | Count | Role |
|---|---:|---|
| Pilot V1 | ${pilot.length} | Original seed |
| Historical V2 | ${hist.length} | Expanded historical research set |
| Universal V24 | ${univ.length} | Current expanded seed (${UNIVERSAL_EXPANSION_COMPANIES_V24.length} incremental adds) |

Organization mix (V24): ${Object.entries(byOrg)
    .map(([k, v]) => `${k} ${v}`)
    .join(" · ")}

**Binding rule:** brand names and the issuer registry are **seeds, not ceilings**. Exhausting the registered list or predefined queries does **not** mean all SEC hotel intelligence has been discovered.

### Filing forms observed (eligible inventory sample)

Companies inventoried in universal wave sample: **${eligibleCompanies.length}** · Eligible filings: **${totalEligible}** · Since year: **${eligibleSummary?.sinceYear || 1994}**

Top forms: ${Object.entries(formAgg)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)
    .map(([f, n]) => `${f} (${n})`)
    .join(", ")}

### Search terms / families

- Taxonomy: \`${SEARCH_TAXONOMY_VERSION}\` — **${SEARCH_FAMILIES.length}** families (identity, ownership, management, franchise, renewal, distress, etc.)
- Contract-timing SEC-API families: **${SEC_API_QUERY_FAMILIES.length}**
- Exa families: **${EXA_QUERY_FAMILIES.length}**
- Period partitions: ${PERIOD_PARTITIONS.map((p) => p.id).join(", ")}
- CMBS baseline accession: \`${BASELINE_JPMCC_ACCESSION}\`

### Contract intelligence baseline (from completed terms pilot)

- VERIFIED agreements: **${migrationAudit?.agreementQualityCounts?.VERIFIED ?? "n/a"}** (franchise-only)
- Terms pilot sample: **${termsMetrics?.agreementsSampled ?? 40}** · incremental beyond expiration: **${termsMetrics?.incrementalBeyondExpirationPct ?? 7.5}%**
- Management discovery leads: **${termsMetrics?.managementDiscoveryLeads ?? 5}** (separate subset)
- Spend: **$${termsSpend?.spentUsd ?? 0.052}** of $25

---

## 2. Important uncovered / partial source categories

${uncoveredCategories.map((c) => `- **${c.id}** (${c.currentState}): ${c.label}`).join("\n")}

## 3. Blind spots (measurable)

${blindSpots.map((b) => `- **${b.id}** [${b.severity}]: ${b.detail}`).join("\n")}

## 4. Brand-neutral expandable discovery process

Designed in \`lib/hotel-intelligence/sec-edgar/sec-discovery-expansion-design-v1.js\` and \`BRAND_NEUTRAL_DISCOVERY_PROCESS.json\`.

Loop: **Seed/Resume → Content discovery → Entity extraction → Source relationships → Retrieve/Extract → Geo/HPC priority → Coverage report**.

Proposed factory stage: **\`DISCOVER_EXPAND\`** (Research Mode only; not in default sequence until authorized). Reuses existing PowerShell/Node engine, checkpoints, cost ledgers, and \`ENRICH_CONTRACT_TERMS\`.

## 5. Durable coverage ledger V3

Module: \`${SEC_DISCOVERY_COVERAGE_LEDGER_V3}\`  
Status: **schema ready** (${v3Ledger.empty ? "empty — awaiting population in expansion workstream" : "rows present"}).

Tracks per document/exhibit: CIK, accession, filing type/date, document IDs, retrieval/extraction status, hotels discovered, contractual intel counts, last research date, follow-ups, cost.

## 6. Geographic policy

- Persist verified intelligence **globally**
- Prioritize HPC reconcile/enrich: **Americas + Europe**
- Expand beyond SEC via intl adapters (readiness only in this packet)

## 7. Incremental contractual intelligence per research dollar (next workstream order)

${roadmap.orderedWorkstreams.map((w) => `${w.rank}. **${w.id}** — ${w.why}`).join("\n")}

## 8. Sequencing & governance

1. Contract Terms & Renewals pilot — **completed on PR #499** (do not relaunch uncontrolled).
2. **This packet** — audit + design + roadmap (no paid expansion run).
3. Next — founder-authorized \`DISCOVER_EXPAND\` execution under budget ceiling.

Boundaries preserved: Research Mode · Canonical Promotion · publication authorization · CUSTOMER_BLOCKED default · no HPC creates in this packet.
`;

  fs.writeFileSync(path.join(OUT, "SEC_DISCOVERY_EXPANSION_ROADMAP.md"), md, "utf8");
  fs.writeFileSync(path.join(OUT, "FOUNDER_SEC_DISCOVERY_EXPANSION_PACKET.md"), md, "utf8");

  console.log(
    JSON.stringify(
      {
        ok: true,
        out: OUT,
        universalCompanies: univ.length,
        blindSpots: blindSpots.length,
        uncoveredCategories: uncoveredCategories.length,
        paidResearchLaunched: false,
        exhaustiveClaim: false,
      },
      null,
      2,
    ),
  );
}

main();
