/**
 * GDI Contact Intelligence V1.1 — official source path recovery canary (Bethesda).
 *
 *   node scripts/gdi-contact-intelligence-v1-1-source-recovery-canary.mjs
 *   node scripts/gdi-contact-intelligence-v1-1-source-recovery-canary.mjs --apply
 *   node scripts/gdi-contact-intelligence-v1-1-source-recovery-canary.mjs --network-domain
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  summarizeContactCoverage,
  classifyContactTier,
  CONTACT_TIER,
} from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import {
  summarizeWeakContactAudit,
  recoverOfficialContactSources,
  DOMAIN_CONFIDENCE,
} from "../lib/group-demand-intelligence/contact-source-recovery-v1-1.js";
import {
  evaluateContactJevShadow,
  scoreJevRoutingOutcome,
} from "../lib/group-demand-intelligence/contact-jev-shadow-v1-1.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const NETWORK_DOMAIN = process.argv.includes("--network-domain");
const HOTEL = "recLuxvwwxID7U2B8";

const PRIORITY_IDS = new Set([
  "gdi_opp_umd_alumni_weekend_watch",
  "gdi_opp_marriott_hq_adjacent_corporate_watch",
  "gdi_pe_781f12393f8117e7",
  "gdi_opp_regulatory_information_conference_20260930",
  "gdi_opp_accp_annual_meeting_20261001",
  "gdi_opp_acvnu_renal_week_20270412",
]);

function tierRank(t) {
  return {
    [CONTACT_TIER.NO_CONTACT]: 0,
    [CONTACT_TIER.GENERIC_ONLY]: 1,
    [CONTACT_TIER.ORGANIZATION_PATH]: 2,
    [CONTACT_TIER.FUNCTIONAL_CONTACT]: 3,
    [CONTACT_TIER.NAMED_PARTIAL]: 4,
    [CONTACT_TIER.NAMED_DIRECT]: 5,
  }[t] ?? 0;
}

async function main() {
  const outDir = path.join(
    ROOT,
    "reports/group-demand-intelligence/contact-intelligence-v1-1"
  );
  fs.mkdirSync(outDir, { recursive: true });

  const doc = await loadOpportunitiesCanonical(HOTEL);
  const all = filterCustomerFacingOpportunities(doc.opportunities || []);
  const actionable = all.filter((o) => !/_disqualified$/i.test(o.id || ""));
  const before = summarizeContactCoverage(actionable);
  const weakAudit = summarizeWeakContactAudit(
    actionable.filter((o) => {
      const t = classifyContactTier(o);
      return [
        CONTACT_TIER.NO_CONTACT,
        CONTACT_TIER.ORGANIZATION_PATH,
        CONTACT_TIER.FUNCTIONAL_CONTACT,
        CONTACT_TIER.NAMED_PARTIAL,
      ].includes(t);
    })
  );

  // Cohort: priority + weak tiers first
  const cohort = [...actionable].sort((a, b) => {
    const pa = PRIORITY_IDS.has(a.id) ? 0 : 1;
    const pb = PRIORITY_IDS.has(b.id) ? 0 : 1;
    if (pa !== pb) return pa - pb;
    return tierRank(classifyContactTier(a)) - tierRank(classifyContactTier(b));
  });
  const selected = cohort.slice(0, 20);

  const rows = [];
  let working = [...(doc.opportunities || [])];
  let updated = 0;
  let domainsResolved = 0;
  let missingDomains = 0;
  let pathsAdded = 0;
  let totalFetches = 0;
  let jevCalls = 0;
  let jevTech = 0;
  let jevPolicy = 0;
  const sourceTypeAgg = {};
  const jevAgg = {
    CONTACT_SOURCE_PATH: { n: 0, jevBetter: 0, gdiBetter: 0, same: 0, unknown: 0, highConfWrong: 0 },
    CONTACT_FOLLOWUP_TYPE: { n: 0, jevBetter: 0, gdiBetter: 0, same: 0, unknown: 0, highConfWrong: 0 },
  };
  const upgrades = {
    noContactToUsable: 0,
    orgToFunctional: 0,
    functionalToNamed: 0,
    namedPartialToDirect: 0,
  };

  for (const o of selected) {
    const recovery = await recoverOfficialContactSources(o, {
      allowNetworkDomainResolution: NETWORK_DOMAIN,
      budget: { maxAdditionalFetches: 4, maxDomainQueries: 3 },
    });
    totalFetches += recovery.metrics.additionalFetches;

    if (recovery.domainState.confidence === DOMAIN_CONFIDENCE.NO_OFFICIAL_DOMAIN) {
      missingDomains += 1;
    } else if (
      recovery.domainState.source === "network_resolver" ||
      (!o.officialSource && recovery.opportunity.officialSource)
    ) {
      domainsResolved += 1;
    }
    if (!o.officialSource && recovery.opportunity.officialSource) pathsAdded += 1;

    for (const sy of recovery.sourceYield || []) {
      const k = sy.sourceType || "UNKNOWN";
      if (!sourceTypeAgg[k]) {
        sourceTypeAgg[k] = { pages: 0, named: 0, functional: 0, email: 0, phone: 0, upgrades: 0 };
      }
      sourceTypeAgg[k].pages += 1;
      sourceTypeAgg[k].named += sy.named;
      sourceTypeAgg[k].functional += sy.functional;
      sourceTypeAgg[k].email += sy.emails;
      sourceTypeAgg[k].phone += sy.phones;
      if (recovery.improved) sourceTypeAgg[k].upgrades += 1;
    }

    const shadow = await evaluateContactJevShadow({
      opportunity: o,
      domainState: recovery.domainState,
      gdiRoute: recovery.gdiRoute,
      forceShadow: true,
    });
    jevCalls += shadow.calls;
    jevTech += shadow.techFallbacks;
    jevPolicy += shadow.policyFallbacks;
    const scored = scoreJevRoutingOutcome({
      shadowEval: shadow,
      recoveryResult: recovery.result,
      actualSourceTypes: (recovery.sourceYield || []).map((s) => s.sourceType),
    });
    for (const key of ["CONTACT_SOURCE_PATH", "CONTACT_FOLLOWUP_TYPE"]) {
      for (const m of Object.keys(jevAgg[key])) {
        jevAgg[key][m] += scored[key][m] || 0;
      }
    }

    const b = recovery.beforeTier;
    const a = recovery.afterTier;
    if (b === CONTACT_TIER.NO_CONTACT && a !== CONTACT_TIER.NO_CONTACT) {
      upgrades.noContactToUsable += 1;
    }
    if (b === CONTACT_TIER.ORGANIZATION_PATH && a === CONTACT_TIER.FUNCTIONAL_CONTACT) {
      upgrades.orgToFunctional += 1;
    }
    if (
      b === CONTACT_TIER.FUNCTIONAL_CONTACT &&
      (a === CONTACT_TIER.NAMED_PARTIAL || a === CONTACT_TIER.NAMED_DIRECT)
    ) {
      upgrades.functionalToNamed += 1;
    }
    if (b === CONTACT_TIER.NAMED_PARTIAL && a === CONTACT_TIER.NAMED_DIRECT) {
      upgrades.namedPartialToDirect += 1;
    }

    if (APPLY && recovery.improved) {
      const write = await promoteQualifiedGdiOpportunity({
        candidate: {
          ...recovery.opportunity,
          weeklyDeltaState: o.weeklyDeltaState,
          isNewThisWeek: o.isNewThisWeek,
          jevContactShadow: {
            shadow: true,
            decisions: (shadow.decisions || []).map((d) => ({
              jevDecisionId: d.decisionId,
              jevDecisionType: d.decisionType,
              jevDecision: d.jevRoute,
              jevConfidence: d.jevConfidence,
              jevShadow: true,
              jevEvaluatedAt: d.evaluatedAt,
            })),
          },
        },
        existingOpps: working,
        hotelId: HOTEL,
        runId: "gdi_contact_intelligence_v1_1_source_recovery",
        method: "official_source_path_recovery_v1_1",
        dryRun: false,
        forceUpdateId: o.id,
        materialUpdateOnly: true,
      });
      if (write.opportunity) {
        working = working.filter((x) => x.id !== o.id).concat([write.opportunity]);
        updated += 1;
        recovery.opportunity = write.opportunity;
        recovery.afterTier = classifyContactTier(write.opportunity);
      }
    } else if (recovery.improved) {
      // Dry-run: reflect in-memory for after coverage
      working = working.filter((x) => x.id !== o.id).concat([recovery.opportunity]);
    }

    rows.push({
      opportunityId: o.id,
      title: o.title,
      beforeTier: b,
      afterTier: recovery.afterTier,
      improved: recovery.improved,
      result: recovery.result,
      domainConfidence: recovery.domainState.confidence,
      domainHost: recovery.domainState.host,
      officialSource: recovery.opportunity.officialSource || null,
      primaryContact:
        recovery.opportunity.primaryContactName ||
        recovery.opportunity.primaryContact?.name ||
        null,
      primaryRole:
        recovery.opportunity.primaryContactRole ||
        recovery.opportunity.primaryContact?.role ||
        null,
      publicEmail: Boolean(
        recovery.opportunity.primaryContactEmail || recovery.opportunity.primaryContact?.email
      ),
      publicPhone: Boolean(
        recovery.opportunity.primaryContactPhone || recovery.opportunity.primaryContact?.phone
      ),
      fetches: recovery.metrics.additionalFetches,
      gdiRoute: recovery.gdiRoute,
      jevAgreement: (shadow.decisions || []).filter((d) => d.agreement).length,
      jevTechFallbacks: shadow.techFallbacks,
    });
  }

  const afterDoc = APPLY ? await loadOpportunitiesCanonical(HOTEL) : { opportunities: working };
  const afterAll = filterCustomerFacingOpportunities(afterDoc.opportunities || []);
  const afterActionable = afterAll.filter((o) => !/_disqualified$/i.test(o.id || ""));
  const after = summarizeContactCoverage(afterActionable);

  const priorityRows = {};
  for (const id of PRIORITY_IDS) {
    const beforeRow = actionable.find((o) => o.id === id);
    const row = rows.find((r) => r.opportunityId === id);
    const afterRow = afterActionable.find((o) => o.id === id);
    priorityRows[id] = {
      before: beforeRow ? classifyContactTier(beforeRow) : null,
      after: row?.afterTier || (afterRow ? classifyContactTier(afterRow) : null),
      title: beforeRow?.title || row?.title || id,
      domainHost: row?.domainHost || null,
      officialSource: row?.officialSource || afterRow?.officialSource || null,
      primaryContact: row?.primaryContact || null,
    };
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    networkDomain: NETWORK_DOMAIN,
    hotelId: HOTEL,
    before: before.counts,
    after: after.counts,
    upgrades,
    domainRecovery: {
      missingOfficialDomains: missingDomains,
      domainsResolved,
      officialSourcePathsAdded: pathsAdded,
    },
    yield: {
      currentContactFetches: totalFetches,
      jevRoutedSimulatedFetches: totalFetches, // shadow — same path; no alternate execution
      fetchesSaved: 0,
      namedContactsLost: 0,
    },
    sourceTypeAgg,
    jev: {
      calls: jevCalls,
      techFallbacks: jevTech,
      policyFallbacks: jevPolicy,
      highConfWrong: jevAgg.CONTACT_SOURCE_PATH.highConfWrong,
      routing: jevAgg,
      shadowOnly: true,
    },
    surfe: { autoCalls: 0, piiPersisted: 0 },
    rowsUpdated: updated,
    priorityRows,
    weakAuditSample: weakAudit.slice(0, 25),
    rows,
  };

  const outPath = path.join(
    outDir,
    `SOURCE_RECOVERY_CANARY_${APPLY ? "APPLY" : "DRY"}_${Date.now()}.json`
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        apply: APPLY,
        before: report.before,
        after: report.after,
        upgrades: report.upgrades,
        domainRecovery: report.domainRecovery,
        jev: { calls: jevCalls, techFallbacks: jevTech },
        improvedRows: rows.filter((r) => r.improved).length,
        priorityRows,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
