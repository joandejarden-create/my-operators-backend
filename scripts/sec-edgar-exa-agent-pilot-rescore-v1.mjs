#!/usr/bin/env node
/**
 * Offline rescore of Exa Agent 30-case pilot against independent verification gates.
 * Does not re-run paid Agent. Preserves original evidence artifacts.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { AGENT_FINDING_CLASS } from "../lib/hotel-intelligence/sec-edgar/exa-agent-deep-verification-v1.js";
import { VERIFICATION_GATE } from "../lib/hotel-intelligence/sec-edgar/contract-intelligence-identity-layers-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DIR = path.join(ROOT, "reports/sec-edgar-exa-agent-pilot-v1");
const OUT = path.join(ROOT, "reports/sec-edgar-exa-agent-pilot-closeout-v1");

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

function main() {
  const prior = readJson(path.join(DIR, "COMPARATIVE_EVALUATION.json"));
  const brandDelta = readJson(path.join(DIR, "BRAND_CONVERSION_TARGETS_AGENT_DELTA.json"));
  const opp = readJson(path.join(ROOT, "reports/sec-edgar-commercial-targets-v1/OPPORTUNITIES.json"));
  const brand = readJson(path.join(ROOT, "reports/sec-edgar-commercial-targets-v1/BRAND_CONVERSION_TARGETS.json"));
  const mgmt = readJson(path.join(ROOT, "reports/sec-edgar-commercial-targets-v1/MANAGEMENT_CONTRACT_TARGETS.json"));

  const findings = prior.incrementalFindings || [];
  const correctedFindings = findings.map((f) => {
    const gates = f.gates || {};
    const statusGate = gates[VERIFICATION_GATE.CURRENT_STATUS_VERIFIED] === true;
    const termsGate = gates[VERIFICATION_GATE.CONTRACT_TERMS_VERIFIED] === true;
    const isClosed = f.classification === AGENT_FINDING_CLASS.CLOSED_OR_SUPERSEDED;
    const isSignal = f.classification === AGENT_FINDING_CLASS.VERIFIED_COMMERCIAL_SIGNAL;
    return {
      ...f,
      correctedLabels: {
        incrementalResearchLead: true,
        verifiedContractualFact:
          Boolean(f.incremental?.newEndDate) && termsGate && statusGate && !isClosed,
        verifiedCurrentStatus: Boolean(f.incremental?.statusResolution) && statusGate,
        actionableCommercialProspect:
          !isClosed &&
          !isSignal &&
          f.classification !== AGENT_FINDING_CLASS.RESEARCH_REQUIRED &&
          statusGate,
        ownerOperatorSignalLevel: f.incremental?.ownerOrOperator
          ? statusGate
            ? "VERIFIED_CURRENT_OWNER_OPERATOR"
            : "COMMERCIAL_SIGNAL_STATUS_UNCONFIRMED"
          : null,
        closedSupersededIsNotActiveSalesOpportunity: isClosed,
      },
    };
  });

  const correctedMetrics = {
    verifiedExpirationDates: {
      searchOnly: prior.metrics.verifiedExpirationDates.searchOnly,
      searchPlusAgent: prior.metrics.verifiedExpirationDates.searchOnly + 0,
      incremental: 0,
      note: "Joplin termination date is a CLOSED_OR_SUPERSEDED research lead; CURRENT_STATUS_VERIFIED=false — not credited as newly verified expiration",
    },
    verifiedNoticePeriods: prior.metrics.verifiedNoticePeriods,
    verifiedCurrentStatusResolutions: {
      searchOnly: 0,
      searchPlusAgent: 0,
      incremental: 0,
      note: "Hampton Inn Joplin CURRENT_STATUS_VERIFIED remains false (complaint ≠ final judgment / primary reopen still required)",
    },
    executedRenewalsConfirmed: prior.metrics.executedRenewalsConfirmed,
    verifiedOwnerOperatorIdentities: {
      searchOnly: 0,
      searchPlusAgent: 0,
      incremental: 0,
      note: "Courtyard San Mateo Foster City retained as VERIFIED_COMMERCIAL_SIGNAL / STATUS_UNCONFIRMED — not fully verified current owner/operator identity",
    },
    ownerOperatorCommercialSignals: {
      searchOnly: 0,
      searchPlusAgent: 1,
      incremental: 1,
      hotels: ["Courtyard San Mateo Foster City"],
    },
    incrementalResearchLeads: {
      searchOnly: 0,
      searchPlusAgent: correctedFindings.length,
      incremental: correctedFindings.length,
      hotels: correctedFindings.map((f) => f.hotel),
    },
    closedOrSupersededResearchLeads: {
      count: correctedFindings.filter((f) => f.classification === AGENT_FINDING_CLASS.CLOSED_OR_SUPERSEDED)
        .length,
      hotels: correctedFindings
        .filter((f) => f.classification === AGENT_FINDING_CLASS.CLOSED_OR_SUPERSEDED)
        .map((f) => f.hotel),
      note: "Not counted as new active sales opportunities",
    },
    qualifiedBrandConversionTargets: {
      searchOnly: prior.metrics.qualifiedBrandConversionTargets.searchOnly,
      searchPlusAgent: prior.metrics.qualifiedBrandConversionTargets.searchOnly,
      incremental: 0,
      note: "CLOSED_OR_SUPERSEDED and ownership signals are not actionable qualified Brand Conversion increments",
    },
    qualifiedManagementTargets: prior.metrics.qualifiedManagementTargets,
    falseOrUnsupportedFindings: prior.metrics.falseOrUnsupportedFindings,
    actualExpenditure: prior.metrics.actualExpenditure,
  };

  const spend = correctedMetrics.actualExpenditure.incremental;
  const unitEconomics = {
    costPerNewlyVerifiedContractDate: null,
    costPerNewlyVerifiedNoticePeriod: null,
    costPerResolvedContractualStatus: null,
    costPerDistinctIncrementalQualifiedHotelTarget: null,
    costPerIncrementalResearchLead:
      correctedFindings.length > 0 ? Number((spend / correctedFindings.length).toFixed(4)) : null,
    distinctIncrementalResearchLeads: correctedFindings.length,
    distinctIncrementalActionableProspects: 0,
    pctAgentResultsRequiringManualCorrection: prior.unitEconomics.pctAgentResultsRequiringManualCorrection,
    pctAgentRunsNoIncrementalValue: prior.unitEconomics.pctAgentRunsNoIncrementalValue,
    priorHeadlineOvercountNote:
      "Prior headline credited Joplin as verified current-status + qualified target and Foster City as verified owner/operator; corrected against gates.",
  };

  // Inventory snapshot (Search-first commercial lists — not Agent deltas)
  const AS_OF = "2026-11-21";
  function monthsBetween(asOf, end) {
    const a = new Date(asOf);
    const e = new Date(end);
    return (e.getUTCFullYear() - a.getUTCFullYear()) * 12 + (e.getUTCMonth() - a.getUTCMonth());
  }
  const tierAB = (opp.opportunities || []).filter(
    (r) =>
      (r.tier === "TIER_A_VERIFIED_DECISION_WINDOW" || r.tier === "TIER_B_VERIFIED_CONTRACT_EXPIRATION") &&
      r.verifiedEndDate,
  );
  const franchise = tierAB.filter((r) => /FRANCHISE|LICENSE/i.test(r.agreementType || ""));
  const management = tierAB.filter((r) => String(r.agreementType).toUpperCase() === "MANAGEMENT");
  const histF = franchise.filter((r) => monthsBetween(AS_OF, r.verifiedEndDate) < 0);
  const futF = franchise.filter((r) => monthsBetween(AS_OF, r.verifiedEndDate) >= 0);
  const histM = management.filter((r) => monthsBetween(AS_OF, r.verifiedEndDate) < 0);
  const futM = management.filter((r) => monthsBetween(AS_OF, r.verifiedEndDate) >= 0);
  const notice = (opp.opportunities || []).filter((r) => r.noticePeriodDays != null && !String(r.tier).includes("HOLD"));

  const payload = {
    version: "exa-agent-pilot-closeout-rescore-v1",
    rescoredAt: new Date().toISOString(),
    researchAsOf: AS_OF,
    founderDecision: {
      exaAgentRouting: "MANUAL_ESCALATION_ONLY",
      autoAgentRouting: false,
      primaryDiscovery: ["Exa Search", "Free SEC EDGAR", "source-specific workflows"],
    },
    priorHeadlineMetrics: prior.metrics,
    correctedMetrics,
    unitEconomics,
    correctedFindings,
    brandDeltaReclassified: (brandDelta.targets || []).map((t) => ({
      ...t,
      actionableCommercialProspect: false,
      role:
        t.classification === AGENT_FINDING_CLASS.CLOSED_OR_SUPERSEDED
          ? "CLOSED_OR_SUPERSEDED_RESEARCH_LEAD"
          : t.classification === AGENT_FINDING_CLASS.VERIFIED_COMMERCIAL_SIGNAL
            ? "OWNER_OPERATOR_COMMERCIAL_SIGNAL_STATUS_UNCONFIRMED"
            : "RESEARCH_LEAD",
    })),
    commercialInventoryAsOf: {
      brandDistinctHotels: brand.count ?? brand.targets?.length,
      managementDistinctHotels: mgmt.count ?? mgmt.targets?.length,
      verifiedFranchiseExpirationsHistorical: histF.length,
      verifiedFranchiseExpirationsFuture: futF.length,
      verifiedManagementExpirationsHistorical: histM.length,
      verifiedManagementExpirationsFuture: futM.length,
      verifiedNoticePeriodsOnNonHoldOpportunities: notice.length,
      ownerFieldPresentUnverifiedDepth: (opp.opportunities || []).filter((r) => r.owner).length,
      worldwideGeographicRestriction: "NONE",
      researchHorizonFilter: "NONE",
    },
    originalEvidencePreserved: {
      comparativeEvaluation: "reports/sec-edgar-exa-agent-pilot-v1/COMPARATIVE_EVALUATION.json",
      agentResults: "reports/sec-edgar-exa-agent-pilot-v1/AGENT_INVESTIGATION_RESULTS.json",
      brandDelta: "reports/sec-edgar-exa-agent-pilot-v1/BRAND_CONVERSION_TARGETS_AGENT_DELTA.json",
    },
  };

  writeJson(path.join(OUT, "CORRECTED_EVALUATION_METRICS.json"), payload);
  writeJson(path.join(DIR, "COMPARATIVE_EVALUATION_CORRECTED.json"), payload);

  const md = [];
  md.push("# FOUNDER REPORT — Exa Agent Pilot Closeout (Corrected)");
  md.push("");
  md.push(`Rescored: ${payload.rescoredAt} · Research as-of: ${AS_OF}`);
  md.push("Original pilot evidence preserved · no paid Agent re-run");
  md.push("");
  md.push("## Founder decision");
  md.push("- **MANUAL_ESCALATION_ONLY** approved");
  md.push("- Automatic Agent routing: **OFF**");
  md.push("- Primary discovery: Exa Search + free SEC EDGAR + source-specific workflows");
  md.push("");
  md.push("## Corrected Agent evaluation metrics");
  md.push("");
  md.push("| Metric | Prior headline | Corrected |");
  md.push("|---|---:|---:|");
  md.push(
    `| Verified expiration dates (incremental) | ${prior.metrics.verifiedExpirationDates.incremental} | **${correctedMetrics.verifiedExpirationDates.incremental}** |`,
  );
  md.push(
    `| Verified current-status resolutions (incremental) | ${prior.metrics.verifiedCurrentStatusResolutions.incremental} | **${correctedMetrics.verifiedCurrentStatusResolutions.incremental}** |`,
  );
  md.push(
    `| Verified owner/operator identities (incremental) | ${prior.metrics.verifiedOwnerOperatorIdentities.incremental} | **${correctedMetrics.verifiedOwnerOperatorIdentities.incremental}** |`,
  );
  md.push(
    `| Owner/operator commercial signals (incremental) | — | **${correctedMetrics.ownerOperatorCommercialSignals.incremental}** |`,
  );
  md.push(
    `| Actionable qualified Brand Conversion increments | ${prior.metrics.qualifiedBrandConversionTargets.incremental} | **${correctedMetrics.qualifiedBrandConversionTargets.incremental}** |`,
  );
  md.push(
    `| Incremental research leads (preserved) | ${findings.length} | **${correctedMetrics.incrementalResearchLeads.incremental}** |`,
  );
  md.push(`| Agent spend (USD) | ${spend} | ${spend} |`);
  md.push("");
  md.push("### Classification notes");
  md.push(
    "- **Hampton Inn Joplin:** `CLOSED_OR_SUPERSEDED` research lead; `CURRENT_STATUS_VERIFIED=false` — not a new active sales opportunity; termination candidate still needs primary-document reopen.",
  );
  md.push(
    "- **Courtyard San Mateo Foster City:** ownership/operator **commercial signal** (Clarion Partners / Marriott, Jan 2025 claim) at `STATUS_UNCONFIRMED` — preserved; not fully verified current identity.",
  );
  md.push("");
  md.push("## Routing policy");
  md.push("- Default: `EXA_AGENT_DEFAULT_ROUTING = MANUAL_ESCALATION_ONLY`");
  md.push("- Requires `explicitManualEscalation` + `spendAuthorization` per batch");
  md.push("- Factory-wide auto-discovery Agent invocation: blocked");
  md.push("");
  md.push("## Commercial inventory (Search-first, as-of research date)");
  const inv = payload.commercialInventoryAsOf;
  md.push(`- Brand Conversion distinct hotels: **${inv.brandDistinctHotels}**`);
  md.push(`- Management Contract distinct hotels: **${inv.managementDistinctHotels}**`);
  md.push(`- Franchise expirations historical / future: **${inv.verifiedFranchiseExpirationsHistorical}** / **${inv.verifiedFranchiseExpirationsFuture}**`);
  md.push(`- Management expirations historical / future: **${inv.verifiedManagementExpirationsHistorical}** / **${inv.verifiedManagementExpirationsFuture}**`);
  md.push(`- Notice periods (non-HOLD opportunities): **${inv.verifiedNoticePeriodsOnNonHoldOpportunities}**`);
  md.push("- Geo / horizon research restrictions: **NONE**");
  md.push("");
  md.push("## Next Search-driven expansion");
  md.push("1. Hotel-specific franchise expirations (hist + future)");
  md.push("2. Named management agreement expirations (EX-10 + intl)");
  md.push("3. Notice periods / deadlines with hotel attribution");
  md.push("4. Renewal / amendment / termination status resolution");
  md.push("5. Owner / management company verification via HE/HI");
  md.push("6. Named decision-makers from reliable sources only");
  md.push("Lease: classify/retain incidental only — deferred capability.");
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT_EXA_AGENT_PILOT_CLOSEOUT.md"), md.join("\n"), "utf8");
  fs.writeFileSync(path.join(DIR, "FOUNDER_REPORT_EXA_AGENT_VS_SEARCH_CORRECTED.md"), md.join("\n"), "utf8");

  console.log(
    JSON.stringify(
      {
        correctedIncremental: {
          verifiedExpirations: correctedMetrics.verifiedExpirationDates.incremental,
          verifiedCurrentStatus: correctedMetrics.verifiedCurrentStatusResolutions.incremental,
          verifiedOwnerOperator: correctedMetrics.verifiedOwnerOperatorIdentities.incremental,
          researchLeads: correctedMetrics.incrementalResearchLeads.incremental,
          actionableBrand: correctedMetrics.qualifiedBrandConversionTargets.incremental,
        },
        inventory: inv,
      },
      null,
      2,
    ),
  );
}

main();
