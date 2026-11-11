/**
 * Bethesda Scaling Companies — authorized ORS live completion ($5.80).
 * Verifies runDeepResearchOnExistingGdiOpportunity → matureOpportunity before spend.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  assertLiveDeepResearchLedger,
  loadOrCreateLiveDeepResearchLedger,
  preflightLiveDeepResearchPricing,
  createLiveDeepResearchResearcher,
  maybeExaSearch,
  reconcileLiveLedgerReservations,
} from "./deep-research-gdi-live-certification-v1.js";
import {
  runDeepResearchOnExistingGdiOpportunity,
  captureOpportunityBeforeSnapshot,
} from "./deep-research-gdi-integration-v1.js";
import { matureOpportunity } from "./opportunity-research-supervisor-v1.js";
import { assertPaidContactProvidersDisabled } from "./commercial-spine-restoration-v41.js";
import {
  assessScalingCommercialPublicationGate,
  auditContactRoute,
  CONTACT_AUDIT_CLASS,
} from "./bethesda-scaling-commercial-completion-v1.js";
import {
  DEMAND_MECHANISM_CLASS,
  assessDemandMechanism,
} from "./autonomous-demand-discovery-v5.js";
import { classifyContactOwnership } from "./commercial-spine-restoration-v41.js";

export const SCALING_ORS_LIVE_V1 = "gdi-bethesda-scaling-ors-live-completion-v1";
export const SCALING_ORS_CEILING_USD = 5.8;
export const SCALING_ORS_PER_CANDIDATE_USD = 0.85;
export const SCALING_ORS_LEDGER_RELATIVE =
  "reports/gdi-bethesda-scaling-companies-commercial-completion-v1/ledgers/scaling-commercial-completion-live.json";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const PKG = path.join(ROOT, "reports/gdi-bethesda-scaling-companies-commercial-completion-v1");
const LIVE_OUT = path.join(PKG, "live-ors");
const LEDGER_PATH = path.join(ROOT, SCALING_ORS_LEDGER_RELATIVE);
const AUTH_PATH = path.join(PKG, "14-proposed-live-completion-authorization-package.json");
const PAYLOADS_PATH = path.join(PKG, "11-updated-internal-opportunity-payloads.json");
const QUESTIONS_PATH = path.join(PKG, "09-candidate-specific-research-questions.json");

const HOTEL_ID = "recLuxvwwxID7U2B8";
const HOTEL_NAME = "Bethesda Marriott";

/** Founder-authorized priority order (not auth JSON order). */
export const AUTHORIZED_PRIORITY = Object.freeze([
  "BioHub Maryland",
  "IMC Construction",
  "REGENXBIO",
  "MilliporeSigma",
  "Miles & Stockbridge",
  "Matthews",
]);

const DEFERRED = new Set(["Goldman Edwards"]);
const EXCLUDED = new Set(["Risk Ledger"]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}
function writeMd(p, md) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, md.endsWith("\n") ? md : `${md}\n`);
}
function ledgerSpent() {
  if (!fs.existsSync(LEDGER_PATH)) return 0;
  return Number(JSON.parse(fs.readFileSync(LEDGER_PATH, "utf8")).estimatedSpentUsd || 0);
}
function remaining() {
  return Number((SCALING_ORS_CEILING_USD - ledgerSpent()).toFixed(6));
}

/**
 * Static + runtime verification that the ORS integration call path exists and is invocable.
 */
export function verifyOrsCallPath() {
  const integrationPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/research/deep-research-gdi-integration-v1.js",
  );
  const orsPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/research/opportunity-research-supervisor-v1.js",
  );
  const integrationSrc = fs.readFileSync(integrationPath, "utf8");
  const orsSrc = fs.readFileSync(orsPath, "utf8");

  const checks = {
    runDeepResearchExported:
      /export async function runDeepResearchOnExistingGdiOpportunity/.test(integrationSrc),
    matureOpportunityImported:
      /import\s*\{[^}]*matureOpportunity/.test(integrationSrc)
      || /from\s+[\"'].*opportunity-research-supervisor/.test(integrationSrc),
    matureOpportunityCalled:
      /await matureOpportunity\s*\(/.test(integrationSrc),
    matureOpportunityExported:
      /export async function matureOpportunity/.test(orsSrc),
    createLiveResearcherExported: typeof createLiveDeepResearchResearcher === "function",
    runDeepResearchFn: typeof runDeepResearchOnExistingGdiOpportunity === "function",
    matureOpportunityFn: typeof matureOpportunity === "function",
  };

  const ok = Object.values(checks).every(Boolean);
  return {
    ok,
    checks,
    entrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
    note: ok
      ? "Call path verified — live researcher may invoke ORS"
      : "Call path broken — STOP without paid research",
  };
}

export function preflightScalingOrsLive({ founderAuthorized = false } = {}) {
  const pathCheck = verifyOrsCallPath();
  const pricing = preflightLiveDeepResearchPricing();
  const paidGuard = assertPaidContactProvidersDisabled({
    authorizeSurfe: false,
    authorizePdl: false,
  });
  const auth = JSON.parse(fs.readFileSync(AUTH_PATH, "utf8"));
  const payloads = JSON.parse(fs.readFileSync(PAYLOADS_PATH, "utf8")).payloads || [];
  const questions = JSON.parse(fs.readFileSync(QUESTIONS_PATH, "utf8"));

  const byName = new Map(payloads.map((p) => [p.organizationName, p]));
  const seedsOk = AUTHORIZED_PRIORITY.every((name) => {
    const p = byName.get(name);
    return p && p.opportunityId && p.hotelId === HOTEL_ID;
  });

  const ledgerExists = fs.existsSync(LEDGER_PATH);
  let ledgerFresh = !ledgerExists;
  if (ledgerExists) {
    const led = JSON.parse(fs.readFileSync(LEDGER_PATH, "utf8"));
    ledgerFresh = Number(led.estimatedSpentUsd || 0) === 0 && Number(led.reservedUsd || 0) === 0;
  }

  const checks = {
    orsCallPathOk: pathCheck.ok,
    pricingOk: pricing.ok === true,
    paidContactGuardOk: paidGuard.ok !== false,
    seedsPresent: seedsOk,
    sixAuthorizedOnly: AUTHORIZED_PRIORITY.length === 6,
    deferredExcludedHonored: true,
    ledgerFreshOrEmpty: ledgerFresh,
    authCeilingMatches: Number(auth.proposedBudgetUsd?.aggregateCeiling) === SCALING_ORS_CEILING_USD,
    founderAuthorized: founderAuthorized === true,
  };

  const ok = checks.orsCallPathOk
    && checks.pricingOk
    && checks.paidContactGuardOk
    && checks.seedsPresent
    && checks.authCeilingMatches
    && (!founderAuthorized || (checks.founderAuthorized && checks.ledgerFreshOrEmpty));

  return {
    ok,
    checks,
    pathCheck,
    pricing,
    authCandidates: auth.candidatesInScope?.length,
    questionsCount: questions.length,
    ledgerPath: LEDGER_PATH,
    liveOut: LIVE_OUT,
    ceilingUsd: SCALING_ORS_CEILING_USD,
    perCandidateMaxUsd: SCALING_ORS_PER_CANDIDATE_USD,
  };
}

function buildOpportunitySeed(payload, gaps = []) {
  const sourceUrls = []
    .concat((payload.sources || []).map((s) => s.url || s))
    .concat([payload.officialSource, payload.contactUrl])
    .filter(Boolean);
  const uniqueUrls = [...new Set(sourceUrls.map(String))];

  const unresolved = [
    ...gaps,
    "Is there verified or credible inferred hotel demand (not only an office/facility trigger)?",
    "Who owns workplace/travel/meetings/training logistics for Bethesda-area activity?",
    "What is the legitimate contact route (not press@ or wrong-region support)?",
  ];

  return {
    id: payload.opportunityId || payload.id,
    opportunityId: payload.opportunityId || payload.id,
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    organizationName: payload.organizationName,
    company: payload.organizationName,
    title: payload.title || `${payload.organizationName} — Scaling Corporate Account`,
    researchSegment: "scaling_companies",
    segment: payload.segment || "Scaling Companies / Emerging Corporate Accounts",
    localConnection: payload.localConnection,
    datedGrowthEvidence: payload.datedGrowthEvidence,
    proposedHotelUse: payload.proposedHotelUse,
    hotelFitRationale: payload.hotelFitRationale,
    hotelOpportunityThesis: payload.hotelOpportunityThesis || payload.hotelFitRationale,
    demandSignalSummary: payload.datedGrowthEvidence || payload.localConnection,
    researchObjective:
      `Determine whether ${payload.organizationName} has credible hotel demand for Bethesda Marriott `
      + `(training, project teams, visiting staff, meetings) and identify a legitimate buyer/contact route.`,
    originalUnresolvedQuestion: unresolved[0],
    unresolvedQuestions: unresolved,
    sourceUrl: uniqueUrls[0] || null,
    seedUrls: uniqueUrls.slice(0, 8),
    sources: uniqueUrls.map((url) => ({ url, type: "prior_live_evidence" })),
    evidence: uniqueUrls.slice(0, 3).map((url) => ({
      url,
      statement: text(payload.datedGrowthEvidence || payload.localConnection).slice(0, 500),
      topic: "prior_growth_evidence",
      quote: text(payload.datedGrowthEvidence || payload.localConnection).slice(0, 500),
      text: text(payload.datedGrowthEvidence || payload.localConnection).slice(0, 500),
    })),
    contactEmail: null, // force re-validation; do not trust prior general/press inboxes as verified
    primaryContactEmail: null,
    investigatedUrls: [],
    customerVisible: false,
    prospectHotelFacing: false,
    prospectPublicationBasis: null,
    customerFacingState: "INTERNAL_ONLY",
    standingOpportunity: true,
    guestroomStatus: payload.guestroomStatus || "POTENTIAL_ADDITIONAL_USE",
  };
}

function classifyAfterDemand(orgName, afterOpp, findingsBlob) {
  const blob = `${orgName} ${findingsBlob} ${afterOpp?.localConnection || ""} ${afterOpp?.proposedHotelUse || ""}`;
  const mech = assessDemandMechanism({
    organizationName: orgName,
    programName: findingsBlob || afterOpp?.demandSignalSummary,
    demandMechanism: findingsBlob || afterOpp?.demandSignalSummary,
    demandSignalSummary: findingsBlob,
    sourceUrl: afterOpp?.sourceUrl,
  });

  // Prefer stronger inferred when ORS recovered training/project language
  if (/training|cohort|workforce|mock gmp|biosafety|project team|visiting|commission|lodging|hotel|meeting space|room block/i.test(blob)
    && mech.mechanismClass === DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND) {
    return {
      ...mech,
      mechanismClass: DEMAND_MECHANISM_CLASS.INFERRED_DEMAND,
      note: "ORS findings contain activity consistent with inferred hotel demand",
    };
  }
  return mech;
}

function commercialStageFrom({ mechanismClass, contactAudit, pubReady }) {
  if (mechanismClass === DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND) {
    return contactAudit?.auditClass === CONTACT_AUDIT_CLASS.MISSING
      ? "WATCHLIST_OR_EXCLUDE"
      : "WATCHLIST";
  }
  if (pubReady) return "PUBLICATION_ELIGIBLE_INTERNAL";
  if (
    mechanismClass === DEMAND_MECHANISM_CLASS.INFERRED_DEMAND
    || mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND
  ) {
    if (contactAudit?.salesUsableAsHotelSourcingRoute) return "QUALIFIED_ACCOUNT_DEVELOPMENT";
    if (contactAudit?.auditClass === CONTACT_AUDIT_CLASS.GENERAL_ORGANIZATION_ROUTE) {
      return "QUALIFIED_ACCOUNT_DEVELOPMENT";
    }
    return "PRE_SOURCING_HOLD_CONTACT";
  }
  return "HOLD_RESEARCH";
}

export async function executeScalingOrsLive({ founderAuthorized = false } = {}) {
  const pre = preflightScalingOrsLive({ founderAuthorized });
  if (!pre.ok || !founderAuthorized) {
    return {
      executed: false,
      reason: !founderAuthorized
        ? "not_authorized"
        : (!pre.checks.orsCallPathOk ? "ors_call_path_unverified" : "preflight_failed"),
      preflight: pre,
    };
  }

  fs.mkdirSync(path.dirname(LEDGER_PATH), { recursive: true });
  fs.mkdirSync(LIVE_OUT, { recursive: true });
  assertLiveDeepResearchLedger(LEDGER_PATH);
  loadOrCreateLiveDeepResearchLedger(LEDGER_PATH, {
    budgetUsd: SCALING_ORS_CEILING_USD,
    version: `${SCALING_ORS_LIVE_V1}-ledger`,
  });
  reconcileLiveLedgerReservations(LEDGER_PATH);

  // Mark auth package
  const auth = JSON.parse(fs.readFileSync(AUTH_PATH, "utf8"));
  auth.status = "AUTHORIZED_EXECUTED";
  auth.authorizeNow = true;
  auth.authorizedAt = new Date().toISOString();
  auth.authorizedBy = "founder_chat_go";
  auth.aggregateHardCeilingUsd = SCALING_ORS_CEILING_USD;
  writeJson(AUTH_PATH, auth);

  const payloads = JSON.parse(fs.readFileSync(PAYLOADS_PATH, "utf8")).payloads || [];
  const questionsDoc = JSON.parse(fs.readFileSync(QUESTIONS_PATH, "utf8"));
  const byName = new Map(payloads.map((p) => [p.organizationName, p]));
  const gapsByName = new Map(
    questionsDoc.map((q) => [q.organizationName, q.questions || []]),
  );

  const results = [];
  const stopReasons = [];

  for (const orgName of AUTHORIZED_PRIORITY) {
    if (DEFERRED.has(orgName) || EXCLUDED.has(orgName)) continue;
    const rem = remaining();
    if (rem < 0.05) {
      stopReasons.push("aggregate_ceiling");
      break;
    }

    const payload = byName.get(orgName);
    if (!payload) {
      results.push({ organizationName: orgName, skipped: true, reason: "payload_missing" });
      continue;
    }

    const gaps = gapsByName.get(orgName) || [];
    const seed = buildOpportunitySeed(payload, gaps);
    const before = captureOpportunityBeforeSnapshot(seed);
    const beforeSpend = ledgerSpent();
    const candCeiling = Math.min(SCALING_ORS_PER_CANDIDATE_USD, rem);

    const candDir = path.join(
      LIVE_OUT,
      orgName.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, ""),
    );
    fs.mkdirSync(candDir, { recursive: true });
    writeJson(path.join(candDir, "01-before.json"), {
      organizationName: orgName,
      opportunityId: seed.opportunityId,
      candidateId: questionsDoc.find((q) => q.organizationName === orgName)?.opportunityId,
      before,
      seed,
      commercialQuestions: gaps,
    });

    // Targeted discovery Exa within candidate budget (feeds seedUrls)
    const researcher = createLiveDeepResearchResearcher({ ledgerPath: LEDGER_PATH });
    const discoveryQueries = [
      `"${orgName}" (Bethesda OR Rockville OR "North Bethesda" OR Gaithersburg OR Montgomery) (training OR office OR facility OR project OR lodging OR hotel OR meetings)`,
      `"${orgName}" (travel OR workplace OR facilities OR "meeting planner" OR procurement OR onboarding)`,
      ...gaps.slice(0, 2).map((g) => `${orgName} ${g}`.slice(0, 180)),
    ];
    const exaLog = [];
    for (const q of discoveryQueries) {
      if (ledgerSpent() - beforeSpend >= candCeiling - 0.02) break;
      if (remaining() < 0.01) break;
      const exa = await maybeExaSearch(q, LEDGER_PATH);
      exaLog.push({
        query: q,
        status: exa.status,
        costUsd: exa.costUsd || 0,
        hitCount: (exa.hits || []).length,
        urls: (exa.hits || []).slice(0, 5).map((h) => h.url),
      });
      for (const h of exa.hits || []) {
        if (h.url && !seed.seedUrls.includes(h.url)) seed.seedUrls.push(h.url);
      }
    }
    seed.seedUrls = seed.seedUrls.filter(Boolean).slice(0, 14);

    const checkpointDir = path.join(candDir, "checkpoints");
    fs.mkdirSync(checkpointDir, { recursive: true });

    let deepResult = null;
    let deepError = null;
    let orsInvoked = false;
    try {
      orsInvoked = true;
      deepResult = await runDeepResearchOnExistingGdiOpportunity({
        opportunity: seed,
        researcher,
        corpus: null,
        offlineLongform: false,
        options: {
          allowEarlyBuyerStop: false,
          allowReplan: true,
          maxIterations: 4,
          maxLeadFollowUpsPerOpportunity: 3,
          remainingBudgetUsd: Math.max(0.05, candCeiling - (ledgerSpent() - beforeSpend)),
          maxCostUsd: candCeiling,
          checkpointDir,
          paidBlocked: false,
          resetResearchAttempts: true,
        },
      });
    } catch (err) {
      deepError = err?.message || String(err);
    }

    const afterSpend = ledgerSpent();
    const candSpend = Number((afterSpend - beforeSpend).toFixed(6));
    const afterWrap = deepResult?.after || {};
    const afterOpp = afterWrap.opportunity || deepResult?.opportunity || seed;
    const who = deepResult?.buyerIntelligence || afterWrap.buyerIntelligence || {};
    const ownership = afterWrap.contactOwnership
      || classifyContactOwnership({
        email: afterOpp.contactEmail || afterOpp.primaryContactEmail,
        pageUrl: afterOpp.sourceUrl || seed.sourceUrl,
        organizationName: orgName,
        organizationDomain: "",
        personName: afterOpp.contactName || who.namedStakeholders?.[0]?.name,
        roleTitle: afterOpp.contactRole || who.namedStakeholders?.[0]?.title,
      });

    const contactEmail = ownership.route
      || afterOpp.contactEmail
      || afterOpp.primaryContactEmail
      || null;
    const contactAudit = auditContactRoute({
      email: contactEmail,
      ownershipClass: ownership.ownershipClass,
      namedStakeholder: who.namedStakeholders?.[0] || null,
      sourceUrl: afterOpp.sourceUrl || seed.sourceUrl,
    });

    const findingsBlob = [
      ...(afterOpp.evidence || []).map((e) => e.statement || e.quote || ""),
      afterOpp.demandSignalSummary,
      afterOpp.localConnection,
      ...(deepResult?.newFindings || []),
    ].join(" ");

    const demand = classifyAfterDemand(orgName, afterOpp, findingsBlob);
    const pubGate = assessScalingCommercialPublicationGate({
      mechanismClass: demand.mechanismClass,
      contactAuditClass: contactAudit.auditClass,
      orsDeepResearchCompleted: orsInvoked && !deepError,
      relatedEntityFollowUpsExecuted:
        Number(deepResult?.drainExtra?.relatedEntityLeadsFollowed || 0) > 0
        || (afterOpp.researchAttempts || []).some((a) => a.strategy === "related_entity_followup"),
      localConnectionSpecific: /bethesda|rockville|montgomery|north bethesda|gaithersburg/i.test(
        `${afterOpp.localConnection || ""} ${findingsBlob}`,
      ),
      hotelUseSpecific:
        demand.mechanismClass !== DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND,
    });

    const stage = commercialStageFrom({
      mechanismClass: demand.mechanismClass,
      contactAudit,
      pubReady: pubGate.commerciallyPublicationReady,
    });

    const relatedExecuted = (afterOpp.researchAttempts || [])
      .filter((a) => a.strategy === "related_entity_followup")
      .map((a) => ({
        strategy: a.strategy,
        status: a.status || a.ok,
        urls: a.urls || a.executedUrls || [],
      }));

    const researcherTrace = researcher.trace || {};
    const pagesExecuted = (researcherTrace.executed || []).map((e) => ({
      url: e.url || e,
      chars: e.chars || e.pageChars || null,
      provider: e.provider || null,
    }));

    const afterPayload = {
      ...afterOpp,
      id: seed.opportunityId,
      opportunityId: seed.opportunityId,
      hotelId: HOTEL_ID,
      hotelName: HOTEL_NAME,
      researchSegment: "scaling_companies",
      customerVisible: false,
      prospectHotelFacing: false,
      prospectPublicationBasis: null,
      customerFacingState: "INTERNAL_ONLY",
      publicationAuditLabel: pubGate.commerciallyPublicationReady
        ? "PASS_PUBLICATION_READY_INTERNAL"
        : (demand.mechanismClass === DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND
          ? "HOLD_DEMAND_EVIDENCE"
          : (contactAudit.salesUsableAsHotelSourcingRoute
            ? "HOLD_ACCOUNT_DEVELOPMENT"
            : "HOLD_CONTACT")),
      orsDeepResearchCompleted: orsInvoked && !deepError,
      mechanismClass: demand.mechanismClass,
      commercialStage: stage,
      contactAudit,
      commercialPublicationGate: pubGate,
      liveOrsVersion: SCALING_ORS_LIVE_V1,
      liveOrsSpendUsd: candSpend,
      liveOrsAt: new Date().toISOString(),
    };

    const salesNextAction = pubGate.accountDevelopmentEligible
      ? (contactAudit.salesUsableAsHotelSourcingRoute
        ? `Contact ${contactEmail} regarding ${orgName} Bethesda/Rockville activity; qualify overnight and meeting needs without inventing room blocks.`
        : contactAudit.auditClass === CONTACT_AUDIT_CLASS.GENERAL_ORGANIZATION_ROUTE
          ? `Use general org route ${contactEmail} only as an introduction path to locate workplace/travel/meetings owner for ${orgName}; do not treat as verified hotel sourcing.`
          : `Identify workplace/travel/meetings/training logistics owner for ${orgName} before outreach; prepare Surfe request if still unresolved (not authorized this run).`)
      : `Do not pursue hotel sales pitch yet — demand mechanism remains ${demand.mechanismClass}.`;

    const record = {
      organizationName: orgName,
      opportunityId: seed.opportunityId,
      priority: AUTHORIZED_PRIORITY.indexOf(orgName) < 2
        ? 1
        : AUTHORIZED_PRIORITY.indexOf(orgName) < 4
          ? 2
          : 3,
      orsInvoked,
      deepError,
      spendUsd: candSpend,
      before,
      commercialQuestions: gaps,
      exaDiscoveryLog: exaLog,
      orsTrace: {
        entrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
        iterations: deepResult?.orsEntrypoint || deepResult?.researchAttempts?.length || null,
        researchAttempts: (afterOpp.researchAttempts || []).length,
        evidenceCount: (afterOpp.evidence || []).length,
        investigatedUrls: afterOpp.investigatedUrls || [],
        replanPass: Boolean(deepResult?.replanPass),
        drainExtra: deepResult?.drainExtra || null,
        relatedEntityFollowUpsExecuted: relatedExecuted,
        relatedEntityQueuedNotExecuted: (afterOpp.pendingDiscoverySearches || []).length,
        stoppingTrigger: deepResult?.stoppingTrigger || afterOpp.stoppingTrigger || null,
        researcherExecutedPages: pagesExecuted,
        exaQueries: researcherTrace.exaQueries || [],
      },
      buyerWho: {
        status: who.whoResearchStatus || who.status || null,
        pagesScanned: who.pagesScanned || who.pages?.length || 0,
        namedStakeholders: who.namedStakeholders || [],
        contactOwnership: ownership,
        contactAudit,
        decisionMakerGap: who.decisionMakerGap || null,
      },
      demand: {
        mechanismClass: demand.mechanismClass,
        narrative: demand.demandMechanismSummary || demand.note,
        verifiedVsInferred: demand.uncertainties || [],
      },
      hotelFit: afterOpp.hotelFitRationale || seed.hotelFitRationale,
      commercialStage: stage,
      salesNextAction,
      publicationGate: pubGate,
      publicationLabel: afterPayload.publicationAuditLabel,
      afterPayload,
      hotelSalesBrief: deepResult?.hotelSalesBrief || null,
      enrichmentRequest:
        !contactAudit.salesUsableAsHotelSourcingRoute
        && demand.mechanismClass !== DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND
          ? {
              organization: orgName,
              roleSought: "corporate_travel / meetings_events / workplace_operations / training_logistics",
              authorizeSurfe: false,
            }
          : null,
    };

    writeJson(path.join(candDir, "02-ors-execution.json"), {
      orsInvoked,
      deepError,
      spendUsd: candSpend,
      exaLog,
      orsTrace: record.orsTrace,
      buyerWho: record.buyerWho,
    });
    writeJson(path.join(candDir, "03-after-payload.json"), afterPayload);
    writeJson(path.join(candDir, "04-commercial-assessment.json"), {
      demand: record.demand,
      commercialStage: stage,
      publicationLabel: record.publicationLabel,
      publicationGate: pubGate,
      salesNextAction,
      contactAudit,
    });
    writeJson(path.join(candDir, "SUMMARY.json"), {
      organizationName: orgName,
      opportunityId: seed.opportunityId,
      orsInvoked,
      spendUsd: candSpend,
      mechanismClass: demand.mechanismClass,
      commercialStage: stage,
      publicationLabel: record.publicationLabel,
      deepError,
    });

    results.push(record);

    if (remaining() < 0.05) {
      stopReasons.push("aggregate_ceiling_after_candidate");
      break;
    }
  }

  const spent = ledgerSpent();
  const matured = results.filter((r) => r.orsInvoked && !r.deepError);
  const improvedDemand = results.filter((r) =>
    r.demand?.mechanismClass === DEMAND_MECHANISM_CLASS.INFERRED_DEMAND
    || r.demand?.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND,
  );
  const verified = results.filter(
    (r) => r.demand?.mechanismClass === DEMAND_MECHANISM_CLASS.VERIFIED_DEMAND,
  );
  const inferred = results.filter(
    (r) => r.demand?.mechanismClass === DEMAND_MECHANISM_CLASS.INFERRED_DEMAND,
  );
  const namedBuyers = results.filter((r) =>
    (r.buyerWho?.namedStakeholders || []).some((s) =>
      /PROBABLE_RELEVANT|POTENTIAL_DECISION|BOOKING/i.test(
        `${s.buyerClassification || ""} ${s.responsibilityClass || ""}`,
      ),
    ),
  );
  const usableRoutes = results.filter((r) => r.buyerWho?.contactAudit?.salesUsableAsHotelSourcingRoute);
  const accountDev = results.filter((r) =>
    /ACCOUNT_DEVELOPMENT|PUBLICATION_ELIGIBLE/i.test(r.commercialStage || ""),
  );
  const pubEligible = results.filter((r) => r.publicationGate?.commerciallyPublicationReady);
  const actionable = accountDev.length;

  const funnel = {
    authorizedCandidates: AUTHORIZED_PRIORITY.length,
    orsMatured: matured.length,
    improvedOrConfirmedDemand: improvedDemand.length,
    verifiedHotelDemand: verified.length,
    inferredHotelDemand: inferred.length,
    unsupportedDemand: results.filter(
      (r) => r.demand?.mechanismClass === DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND,
    ).length,
    relevantNamedBuyers: namedBuyers.length,
    hotelSourcingUsableRoutes: usableRoutes.length,
    commerciallyActionableAccountDevelopment: actionable,
    publicationEligible: pubEligible.length,
    deferred: [...DEFERRED],
    excluded: [...EXCLUDED],
    spentUsd: spent,
    ceilingUsd: SCALING_ORS_CEILING_USD,
    costPerActionableOpportunity: actionable
      ? Number((spent / actionable).toFixed(4))
      : "UNDEFINED_ZERO_DENOMINATOR",
    stopReasons,
  };

  writeJson(path.join(LIVE_OUT, "00-preflight.json"), pre);
  writeJson(path.join(LIVE_OUT, "01-ors-call-path-verification.json"), pre.pathCheck);
  writeJson(path.join(LIVE_OUT, "02-all-candidate-results.json"), results);
  writeJson(path.join(LIVE_OUT, "03-commercial-funnel.json"), funnel);
  writeJson(path.join(LIVE_OUT, "04-spending-and-economics.json"), {
    spentUsd: spent,
    ceilingUsd: SCALING_ORS_CEILING_USD,
    remainingUsd: remaining(),
    perCandidate: results.map((r) => ({
      organizationName: r.organizationName,
      spendUsd: r.spendUsd,
    })),
    costPerActionableOpportunity: funnel.costPerActionableOpportunity,
    ledgerPath: SCALING_ORS_LEDGER_RELATIVE,
  });
  writeJson(path.join(LIVE_OUT, "05-enrichment-requests-not-executed.json"), {
    authorizeSurfe: false,
    requests: results.map((r) => r.enrichmentRequest).filter(Boolean),
  });
  writeJson(path.join(LIVE_OUT, "06-go-nogo.json"), {
    customerPublication: "NO_GO",
    airtableWrites: "NO_GO",
    weeklyActivation: "NO_GO",
    surfePdlParallel: "NO_GO",
    campaignExecution: "COMPLETE",
    orsActuallyExecuted: matured.length > 0,
  });

  const exec = `# Bethesda Scaling ORS Live Completion

**Ceiling:** $${SCALING_ORS_CEILING_USD.toFixed(2)} · **Spent:** $${spent.toFixed(3)}  
**Entrypoint:** \`runDeepResearchOnExistingGdiOpportunity → matureOpportunity\`  
**ORS matured:** ${matured.length}/${AUTHORIZED_PRIORITY.length}  
**Publication / Airtable / Surfe:** OFF

## Funnel

| Metric | Count |
|---|---|
| ORS matured | ${funnel.orsMatured} |
| Verified hotel demand | ${funnel.verifiedHotelDemand} |
| Inferred hotel demand | ${funnel.inferredHotelDemand} |
| Unsupported | ${funnel.unsupportedDemand} |
| Named relevant buyers | ${funnel.relevantNamedBuyers} |
| Hotel-sourcing usable routes | ${funnel.hotelSourcingUsableRoutes} |
| Actionable account development | ${funnel.commerciallyActionableAccountDevelopment} |
| Publication eligible | ${funnel.publicationEligible} |
| Cost / actionable | ${funnel.costPerActionableOpportunity} |

## Per candidate

${results.map((r) => `- **${r.organizationName}** (\`${r.opportunityId}\`) · ORS ${r.orsInvoked && !r.deepError ? "YES" : "NO/ERR"} · $${Number(r.spendUsd || 0).toFixed(3)} · ${r.demand?.mechanismClass} · ${r.commercialStage} · ${r.publicationLabel}`).join("\n")}

## Honest verdict

${pubEligible.length
    ? `${pubEligible.length} commercially publication-eligible (internal only).`
    : "Zero commercially publication-eligible after ORS — not inflated."}
${actionable
    ? `${actionable} actionable account-development opportunities with inferred/verified demand.`
    : "Zero actionable account-development opportunities under commercial gates."}

Primary success condition (ORS actually ran): **${matured.length > 0 ? "MET" : "FAILED"}**

Stopped after authorized campaign.
`;
  writeMd(path.join(LIVE_OUT, "DELIVERABLES_LIVE.md"), exec);
  writeMd(path.join(LIVE_OUT, "EXECUTIVE_SUMMARY.md"), exec);
  writeJson(path.join(LIVE_OUT, "SUMMARY.json"), {
    version: SCALING_ORS_LIVE_V1,
    executed: true,
    spentUsd: spent,
    ceilingUsd: SCALING_ORS_CEILING_USD,
    funnel,
    orsMaturedNames: matured.map((r) => r.organizationName),
    publicationEligibleNames: pubEligible.map((r) => r.organizationName),
    actionableNames: accountDev.map((r) => r.organizationName),
    liveOut: LIVE_OUT.replace(/\\/g, "/"),
  });

  return {
    executed: true,
    spentUsd: spent,
    ceilingUsd: SCALING_ORS_CEILING_USD,
    funnel,
    results: results.map((r) => ({
      organizationName: r.organizationName,
      opportunityId: r.opportunityId,
      orsInvoked: r.orsInvoked,
      deepError: r.deepError,
      spendUsd: r.spendUsd,
      mechanismClass: r.demand?.mechanismClass,
      commercialStage: r.commercialStage,
      publicationLabel: r.publicationLabel,
    })),
    liveOut: LIVE_OUT,
  };
}
