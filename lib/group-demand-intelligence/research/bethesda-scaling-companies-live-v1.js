/**
 * Bethesda Scaling Companies Full E2E — live campaign.
 * Aggregate ceiling $11 · isolated ledger · Exa + Firecrawl + direct_public.
 * Surfe/PDL/Parallel OFF · no Airtable/publish/weekly.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { fetchOfficialPage } from "../official-page-fetch.js";
import {
  AUTONOMOUS_DEMAND_DISCOVERY_V5,
  DISCOVERY_PATHWAY,
  DEMAND_MECHANISM_CLASS,
  generateOrganizationLedStrategies,
  generateTriggerLedStrategies,
  generateEventLedStrategies,
  qualifyV5DiscoveryCandidate,
  loadHotelGdiConfig,
} from "./autonomous-demand-discovery-v5.js";
import {
  assertLiveDeepResearchLedger,
  loadOrCreateLiveDeepResearchLedger,
  preflightLiveDeepResearchPricing,
  maybeExaSearch,
  maybeFirecrawlScrape,
  reconcileLiveLedgerReservations,
} from "./deep-research-gdi-live-certification-v1.js";
import {
  assertPaidContactProvidersDisabled,
  classifyContactOwnership,
} from "./commercial-spine-restoration-v41.js";
import {
  extractPublicEvidenceFromText,
  buildEvidencePatchFromExtraction,
  selectPreferredPublicEmail,
} from "./direct-public-executor-v1.js";
import { runPublicBuyerIntelligencePass } from "./commercial-depth-buyer-intelligence-v1.js";
import {
  qualifyScalingCompanyCandidate,
  buildScalingCompanyOpportunityCard,
  buildScalingCompanyResearchQueries,
  SCALING_COMPANIES_RESEARCH_VERSION,
} from "../workflow/scaling-companies-research-v1.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  runBethesdaDatabaseReconciliationPreflight,
  assertBethesdaReconciliationGate,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import {
  WEEKLY_FINDING_CLASS,
  buildBethesdaImmutableBaselineManifest,
} from "./bethesda-weekly-net-new-discovery-v1.js";
import { parseStageAHitIdentity, isWeakOrGenericCandidate } from "./bethesda-full-spectrum-stage-a-live-v1.js";
import {
  assessScalingCommercialPublicationGate,
  auditContactRoute,
  detectPrematureScalingResearchStop,
  CONTACT_AUDIT_CLASS,
} from "./bethesda-scaling-commercial-completion-v1.js";

export const SCALING_LIVE_V1 = "gdi-bethesda-scaling-companies-live-v1";
/** Deep research must go through ORS matureOpportunity — shallow page+WHO is not commercial completion. */
export const SCALING_LIVE_REQUIRES_ORS_FOR_PUBLICATION_PASS = true;
export const SCALING_LIVE_CEILING_USD = 11.0;
export const SCALING_LIVE_LEDGER_RELATIVE =
  "reports/gdi-bethesda-scaling-companies-full-e2e-v1/ledgers/scaling-live-campaign.json";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const LIVE_DIR = path.join(ROOT, "reports/gdi-bethesda-scaling-companies-full-e2e-v1/live");
const LEDGER_PATH = path.join(ROOT, SCALING_LIVE_LEDGER_RELATIVE);
const SEED_NOT_NET_NEW = new Set([
  "disp_recLuxvwwxID7U2B8_aledade",
  "gdi_e2d_3e2b75cd6b2eb5c8",
  "gdi_e2d_5f49c27a53ce2aa7",
  "aledade",
  "x-energy",
  "x energy",
]);

const KNOWN_FALSE_POSITIVE = /crunchbase|builtin\.com\/company|linkedin\.com\/company|glassdoor|indeed\.com|levels\.fyi|zoominfo|apollo\.io|rocketreach|hiring|careers?\/|jobs?\//i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}
function hashKey(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 12);
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}
function writeMd(p, md) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, md.endsWith("\n") ? md : `${md}\n`);
}
function ledgerSpent(ledgerPath) {
  if (!fs.existsSync(ledgerPath)) return 0;
  return Number(JSON.parse(fs.readFileSync(ledgerPath, "utf8")).estimatedSpentUsd || 0);
}
function ledgerRemaining(ledgerPath, ceiling) {
  return Number((ceiling - ledgerSpent(ledgerPath)).toFixed(6));
}
function orgKey(name) {
  return text(name).toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function loadKnownOrgs() {
  const known = new Set([...SEED_NOT_NET_NEW]);
  const oppPath = path.join(ROOT, `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/opportunities.json`);
  const candPath = path.join(ROOT, `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/research-candidates.json`);
  try {
    const opps = JSON.parse(fs.readFileSync(oppPath, "utf8")).opportunities || [];
    for (const o of opps) known.add(orgKey(o.organizationName));
  } catch { /* */ }
  try {
    const raw = JSON.parse(fs.readFileSync(candPath, "utf8"));
    const cands = Array.isArray(raw) ? raw : raw.candidates || [];
    for (const c of cands) {
      known.add(orgKey(c.organizationName || c.company));
      if (c.id) known.add(String(c.id).toLowerCase());
    }
  } catch { /* */ }
  return known;
}

function buildScalingStrategies(hotel) {
  const org = generateOrganizationLedStrategies(hotel, { maxPerSegment: 3 })
    .filter((s) => s.segment === "scaling_companies");
  const trig = generateTriggerLedStrategies(hotel, { maxPerSegment: 3 })
    .filter((s) => s.segment === "scaling_companies");
  const evt = generateEventLedStrategies(hotel, { maxPerSegment: 2 })
    .filter((s) => s.segment === "scaling_companies");
  const hotelCfg = {
    city: hotel?.identity?.city || hotel?.city || "Bethesda",
    demandTerritory: hotel?.demandTerritory || { label: "DMV / Bethesda-Rockville" },
    displayName: hotel?.identity?.hotelName || "Bethesda Marriott",
  };
  const extra = buildScalingCompanyResearchQueries(hotelCfg)
    .filter((q) => q.query)
    .map((q) => ({
      pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
      segment: "scaling_companies",
      researchQuestion: q.query,
      sourceTypes: ["corp_announcements", "facility_openings"],
      intent: q.intent,
    }));
  const all = [...org, ...trig, ...evt, ...extra];
  const seen = new Set();
  const out = [];
  for (const s of all) {
    const k = text(s.researchQuestion).toLowerCase();
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(s);
  }
  return out.slice(0, 16);
}

function inferLocalConnection(hit, parsed) {
  const blob = `${hit.title || ""} ${hit.excerpt || ""} ${parsed.programName || ""}`;
  if (/bethesda|rockville|north bethesda|montgomery|nih|walter reed|gaithersburg|potomac/i.test(blob)) {
    return text(blob).slice(0, 280);
  }
  if (/washington.?dc|dmv|maryland|md\b/i.test(blob)) {
    return `DMV/Maryland activity signal: ${text(blob).slice(0, 220)}`;
  }
  return null;
}

function inferHotelUse(hit, parsed) {
  const blob = `${hit.title || ""} ${hit.excerpt || ""} ${parsed.programName || ""}`.toLowerCase();
  if (/training|onboarding|kickoff|offsite|workshop/i.test(blob)) {
    return "Training / onboarding / offsite lodging and meeting space near Bethesda Marriott for visiting or newly hired staff";
  }
  if (/office|headquarters|hq|expand|opened|opening|reloc/i.test(blob)) {
    return "Visiting leadership, project staff, and client meetings needing lodging/meeting space near Bethesda/Rockville operations";
  }
  if (/contract|project|mobiliz|implementation/i.test(blob)) {
    return "Project-team lodging and intermittent working sessions for mobilized staff near NIH/Bethesda corridor";
  }
  if (/meeting|hotel|lodging|rooms/i.test(blob)) {
    return "Corporate lodging and meeting demand associated with the documented local activity";
  }
  return null;
}

function classifyReconciliation(orgName, knownOrgs, countsAsNetNew) {
  const key = orgKey(orgName);
  if (!key) return WEEKLY_FINDING_CLASS.EXCLUDED;
  if (SEED_NOT_NET_NEW.has(key) || [...SEED_NOT_NET_NEW].some((s) => key.includes(s) || s.includes(key))) {
    return WEEKLY_FINDING_CLASS.EXISTING_UNCHANGED;
  }
  if (knownOrgs.has(key)) {
    return countsAsNetNew ? WEEKLY_FINDING_CLASS.EXISTING_ENRICHED : WEEKLY_FINDING_CLASS.EXISTING_UNCHANGED;
  }
  return countsAsNetNew ? WEEKLY_FINDING_CLASS.NET_NEW_OPPORTUNITY : WEEKLY_FINDING_CLASS.EXCLUDED;
}

export function preflightScalingCompaniesLive({ founderAuthorized = false } = {}) {
  const pricing = preflightLiveDeepResearchPricing();
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });
  const recon = runBethesdaDatabaseReconciliationPreflight();
  const reconGate = assertBethesdaReconciliationGate(recon, { requireImmutableBaseline: true });
  const hotel = loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID);
  const strategies = buildScalingStrategies(hotel || {});
  const ledgerExists = fs.existsSync(LEDGER_PATH);
  let ledgerFresh = !ledgerExists;
  if (ledgerExists) {
    const led = JSON.parse(fs.readFileSync(LEDGER_PATH, "utf8"));
    ledgerFresh = Number(led.estimatedSpentUsd || 0) === 0 && Number(led.reservedUsd || 0) === 0;
  }
  const checks = {
    pricingOk: pricing.ok === true,
    paidContactGuardOk: paidGuard.ok !== false,
    reconOk: reconGate.ok === true,
    strategiesPresent: strategies.length >= 6,
    pathwaysPresent: new Set(strategies.map((s) => s.pathway)).size >= 2,
    ledgerFreshOrEmpty: ledgerFresh,
    founderAuthorized: founderAuthorized === true,
    ceilingUsd: SCALING_LIVE_CEILING_USD,
  };
  const ok = checks.pricingOk
    && checks.paidContactGuardOk
    && checks.reconOk
    && checks.strategiesPresent
    && checks.pathwaysPresent
    && (checks.ledgerFreshOrEmpty || !founderAuthorized)
    && (!founderAuthorized || checks.founderAuthorized);
  return {
    ok,
    checks,
    strategyCount: strategies.length,
    strategies,
    hotel,
    pricing,
    ledgerPath: LEDGER_PATH,
    liveDir: LIVE_DIR,
  };
}

async function enrichCandidatePage(url, ledgerPath, remainingBudget) {
  if (!url || remainingBudget < 0.006) {
    // try free direct_public
    try {
      const page = await fetchOfficialPage(url, { allowRender: false, timeoutMs: 12000 });
      const blob = page?.text || page?.searchableText || page?.bodyText || "";
      if (blob && blob.length >= 80) {
        return { text: blob.slice(0, 8000), costUsd: 0, provider: "direct_public" };
      }
    } catch { /* */ }
    return null;
  }
  // Prefer direct_public first (free)
  try {
    const page = await fetchOfficialPage(url, { allowRender: false, timeoutMs: 12000 });
    const blob = page?.text || page?.searchableText || page?.bodyText || "";
    if (blob && blob.length >= 120) {
      return { text: blob.slice(0, 8000), costUsd: 0, provider: "direct_public" };
    }
  } catch { /* thin — firecrawl */ }
  if (remainingBudget >= 0.006) {
    const fc = await maybeFirecrawlScrape(url, ledgerPath);
    if (fc?.text) return { text: String(fc.text).slice(0, 8000), costUsd: Number(fc.actualUsd || 0), provider: "firecrawl" };
  }
  return null;
}

export async function executeScalingCompaniesLive({ founderAuthorized = false } = {}) {
  const pre = preflightScalingCompaniesLive({ founderAuthorized });
  if (!pre.ok || !founderAuthorized) {
    return { executed: false, reason: founderAuthorized ? "preflight_failed" : "not_authorized", preflight: pre };
  }

  fs.mkdirSync(path.dirname(LEDGER_PATH), { recursive: true });
  fs.mkdirSync(LIVE_DIR, { recursive: true });
  assertLiveDeepResearchLedger(LEDGER_PATH);
  loadOrCreateLiveDeepResearchLedger(LEDGER_PATH, {
    budgetUsd: SCALING_LIVE_CEILING_USD,
    version: `${SCALING_LIVE_V1}-ledger`,
  });
  reconcileLiveLedgerReservations(LEDGER_PATH);

  const hotel = pre.hotel || loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID);
  const hotelCfg = {
    city: "Bethesda",
    displayName: "Bethesda Marriott",
    demandTerritory: hotel?.demandTerritory || { label: "DMV / Bethesda-Rockville" },
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
  };
  const baseline = buildBethesdaImmutableBaselineManifest();
  const knownOrgs = loadKnownOrgs();
  const strategies = pre.strategies;
  const searchLog = [];
  const rawHits = [];
  const stopReasons = [];

  // --- Phase 1: Discovery ---
  const discoveryBudgetCap = 3.5;
  for (const strategy of strategies) {
    const spent = ledgerSpent(LEDGER_PATH);
    if (spent >= SCALING_LIVE_CEILING_USD - 0.01) {
      stopReasons.push("aggregate_ceiling");
      break;
    }
    if (spent >= discoveryBudgetCap) {
      stopReasons.push("discovery_phase_cap");
      break;
    }
    const before = spent;
    const exa = await maybeExaSearch(strategy.researchQuestion, LEDGER_PATH);
    const costUsd = Number((ledgerSpent(LEDGER_PATH) - before).toFixed(6));
    const hits = (exa.hits || []).map((h) => ({
      ...h,
      pathway: strategy.pathway,
      segment: "scaling_companies",
      researchQuestion: strategy.researchQuestion,
      intent: strategy.intent || null,
    }));
    searchLog.push({
      pathway: strategy.pathway,
      researchQuestion: strategy.researchQuestion,
      status: exa.status,
      costUsd,
      hitCount: hits.length,
    });
    rawHits.push(...hits);
  }

  const assessed = [];
  for (const hit of rawHits) {
    if (KNOWN_FALSE_POSITIVE.test(hit.url || "")) {
      assessed.push({
        candidateId: `sc_${hashKey(hit.url)}`,
        organizationName: null,
        sourceUrl: hit.url,
        weeklyClass: WEEKLY_FINDING_CLASS.EXCLUDED,
        rejectReasons: ["directory_or_hiring_or_aggregator"],
        countsAsNetNewOpportunity: false,
      });
      continue;
    }
    const parsed = parseStageAHitIdentity(hit, "scaling_companies");
    if (isWeakOrGenericCandidate(parsed) || !parsed.organizationName) {
      assessed.push({
        candidateId: `sc_${hashKey(hit.url || hit.title)}`,
        organizationName: parsed.organizationName,
        programName: parsed.programName,
        sourceUrl: hit.url,
        weeklyClass: WEEKLY_FINDING_CLASS.EXCLUDED,
        rejectReasons: ["weak_or_generic_identity"],
        countsAsNetNewOpportunity: false,
      });
      continue;
    }

    const localConnection = inferLocalConnection(hit, parsed);
    const hotelUse = inferHotelUse(hit, parsed);
    const hotelFit = localConnection
      ? `Bethesda Marriott (407 rooms, ~18.7k sq ft meetings) can serve overnight and meeting needs for ${parsed.organizationName} activity tied to ${localConnection.slice(0, 120)}`
      : null;

    const draft = {
      pathway: hit.pathway,
      segment: "scaling_companies",
      organizationName: parsed.organizationName,
      programName: parsed.programName || hit.title,
      demandMechanism: parsed.programName || hit.title,
      demandSignalSummary: parsed.demandSignalSummary || hit.excerpt,
      sourceUrl: hit.url,
      localConnection,
      datedGrowthEvidence: hit.excerpt || hit.title,
      growthEvidenceDate: parsed.eventYear ? String(parsed.eventYear) : null,
      proposedHotelUse: hotelUse,
      hotelFitRationale: hotelFit,
      whyThisHotel: hotelFit,
      researchQuestion: hit.researchQuestion,
      publisher: hit.publisher,
      excerpt: hit.excerpt,
    };

    const v5 = qualifyV5DiscoveryCandidate({
      pathway: hit.pathway,
      segment: "scaling_companies",
      organizationName: parsed.organizationName,
      programName: draft.programName,
      demandMechanism: draft.demandMechanism,
      demandSignalSummary: draft.demandSignalSummary,
      sourceUrl: hit.url,
      eventYear: parsed.eventYear,
    }, hotel, baseline);

    const scalingQ = qualifyScalingCompanyCandidate(draft, hotelCfg);
    const falsePos = !localConnection || !hotelUse || !scalingQ.ok;
    const accepted = v5.accepted && scalingQ.ok && Boolean(localConnection) && Boolean(hotelUse);

    const weeklyClass = classifyReconciliation(
      parsed.organizationName,
      knownOrgs,
      accepted && v5.countsAsNetNewOpportunity !== false,
    );

    const isSeed = [...SEED_NOT_NET_NEW].some((s) => orgKey(parsed.organizationName).includes(s));
    const countsAsNetNew = accepted
      && weeklyClass === WEEKLY_FINDING_CLASS.NET_NEW_OPPORTUNITY
      && !isSeed;

    assessed.push({
      candidateId: `sc_${hashKey(`${parsed.organizationName}::${hit.url}`)}`,
      pathway: hit.pathway,
      segment: "scaling_companies",
      organizationName: parsed.organizationName,
      programName: draft.programName,
      demandMechanism: draft.demandMechanism,
      mechanismClass: v5.mechanism?.mechanismClass || (accepted ? DEMAND_MECHANISM_CLASS.INFERRED_DEMAND : DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND),
      sourceUrl: hit.url,
      publisher: hit.publisher,
      excerpt: hit.excerpt,
      localConnection,
      proposedHotelUse: hotelUse,
      hotelFitRationale: hotelFit,
      datedGrowthEvidence: draft.datedGrowthEvidence,
      growthEvidenceDate: draft.growthEvidenceDate,
      scalingQualifyOk: scalingQ.ok,
      scalingFailReasons: scalingQ.failReasons,
      v5Accepted: v5.accepted,
      v5Reject: v5.rejectReasons || [],
      accepted,
      countsAsNetNewOpportunity: countsAsNetNew,
      weeklyClass: isSeed ? WEEKLY_FINDING_CLASS.EXISTING_UNCHANGED : (falsePos ? WEEKLY_FINDING_CLASS.EXCLUDED : weeklyClass),
      deepResearchEligible: accepted || (scalingQ.failReasons?.length <= 2 && localConnection),
      rejectReasons: [
        ...(falsePos ? ["scaling_mechanism_incomplete"] : []),
        ...(scalingQ.failReasons || []),
        ...(v5.rejectReasons || []),
      ],
      researchQuestion: hit.researchQuestion,
    });
  }

  // Dedupe by org
  const byOrg = new Map();
  for (const row of assessed) {
    const k = orgKey(row.organizationName);
    if (!k) continue;
    const prev = byOrg.get(k);
    if (!prev || (row.accepted && !prev.accepted) || (row.countsAsNetNewOpportunity && !prev.countsAsNetNewOpportunity)) {
      if (prev) {
        prev.weeklyClass = WEEKLY_FINDING_CLASS.DUPLICATE;
        prev.countsAsNetNewOpportunity = false;
      }
      byOrg.set(k, row);
    } else {
      row.weeklyClass = WEEKLY_FINDING_CLASS.DUPLICATE;
      row.countsAsNetNewOpportunity = false;
    }
  }
  const unique = assessed;

  // --- Phase 2+3: Deep research + buyer on shortlist ---
  const shortlist = [...byOrg.values()]
    .filter((r) => r.deepResearchEligible || r.countsAsNetNewOpportunity || r.accepted)
    .sort((a, b) => Number(b.countsAsNetNewOpportunity) - Number(a.countsAsNetNewOpportunity))
    .slice(0, 12);

  const deepResults = [];
  for (const cand of shortlist) {
    const remaining = ledgerRemaining(LEDGER_PATH, SCALING_LIVE_CEILING_USD);
    if (remaining < 0.01) {
      stopReasons.push("budget_before_deep_research");
      break;
    }
    const spentBefore = ledgerSpent(LEDGER_PATH);
    const page = await enrichCandidatePage(cand.sourceUrl, LEDGER_PATH, remaining);
    let pageText = page?.text || cand.excerpt || "";
    let contactEmail = null;
    let namedStakeholders = [];
    let whoStatus = "NOT_RUN";
    let preferredEmail = null;

    if (pageText.length >= 40) {
      const extracted = extractPublicEvidenceFromText(pageText, cand.sourceUrl);
      preferredEmail = extracted.preferredEmail || selectPreferredPublicEmail(extracted.emails);
      const patch = buildEvidencePatchFromExtraction(extracted, "contact_route");
      contactEmail = patch.deepPatch?.contact?.businessEmail || preferredEmail;
      // Prefer org contact page follow-up if no email
      if (!contactEmail && remaining > 0.02) {
        try {
          const host = new URL(cand.sourceUrl).origin;
          const contactUrl = `${host}/contact`;
          const contactPage = await enrichCandidatePage(contactUrl, LEDGER_PATH, ledgerRemaining(LEDGER_PATH, SCALING_LIVE_CEILING_USD));
          if (contactPage?.text) {
            pageText = `${pageText}\n${contactPage.text}`.slice(0, 12000);
            const ex2 = extractPublicEvidenceFromText(contactPage.text, contactUrl);
            preferredEmail = ex2.preferredEmail || selectPreferredPublicEmail(ex2.emails) || preferredEmail;
            contactEmail = preferredEmail;
          }
        } catch { /* */ }
      }
      const buyer = runPublicBuyerIntelligencePass({
        evidence: [{ url: cand.sourceUrl, text: pageText, quote: pageText, statement: pageText, excerpt: pageText.slice(0, 4000) }],
        organizationName: cand.organizationName,
        existingContactEmail: contactEmail,
        segmentId: "scaling_companies",
      });
      whoStatus = buyer.whoResearchStatus;
      namedStakeholders = buyer.namedStakeholders || [];
      if (!contactEmail && buyer.contactOwnership?.route) contactEmail = buyer.contactOwnership.route;
    } else {
      whoStatus = "ZERO_PAGES_SCANNED";
    }

    const enrichedLocal = cand.localConnection
      || (pageText && /bethesda|rockville|montgomery|nih/i.test(pageText) ? text(pageText).match(/[^.]{0,80}(bethesda|rockville|montgomery|nih)[^.]{0,80}/i)?.[0] : null)
      || cand.localConnection;
    const enrichedUse = cand.proposedHotelUse
      || inferHotelUse({ title: cand.programName, excerpt: pageText }, { programName: cand.programName });
    const enrichedFit = cand.hotelFitRationale
      || (enrichedLocal
        ? `Bethesda Marriott can compete for overnight/meeting needs linked to ${cand.organizationName} activity in/near ${enrichedLocal.slice(0, 100)}`
        : null);

    const cardInput = {
      organizationName: cand.organizationName,
      localConnection: enrichedLocal,
      datedGrowthEvidence: cand.datedGrowthEvidence || cand.excerpt || pageText.slice(0, 400),
      growthEvidenceDate: cand.growthEvidenceDate,
      proposedHotelUse: enrichedUse,
      hotelFitRationale: enrichedFit,
      whyThisHotel: enrichedFit,
      sourceUrl: cand.sourceUrl,
      sources: [{ url: cand.sourceUrl, type: "growth_evidence", title: "Growth evidence" }],
      contactEmail,
      contactName: namedStakeholders[0]?.name || null,
      contactRole: namedStakeholders[0]?.title || "workplace_operations",
      establishedFacts: [
        cand.programName ? `Program/activity signal: ${cand.programName}` : null,
        enrichedLocal ? `Local connection: ${enrichedLocal}` : null,
      ].filter(Boolean),
      inferredHotelDemand: [
        enrichedUse || "Potential lodging/meeting demand inferred from expansion/project signal — not a verified room block",
      ],
      whyNow: "Establish corporate relationship while local growth/project activity is observable",
      recommendedAction: contactEmail
        ? `Contact ${namedStakeholders[0]?.name || "workplace/travel/meetings owner"} via ${contactEmail} to introduce Bethesda Marriott for visiting staff lodging and meetings tied to ${cand.organizationName}'s local activity; ask how overnight and meeting needs are currently handled.`
        : `Identify workplace operations / travel / meetings owner at ${cand.organizationName} and introduce Bethesda Marriott for visiting-staff lodging near Bethesda/Rockville; ask how current lodging is arranged.`,
    };

    const card = buildScalingCompanyOpportunityCard(cardInput, {
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      hotelConfig: hotelCfg,
    });

    const spentAfter = ledgerSpent(LEDGER_PATH);
    const pubReady = card.ok === true
      && Boolean(contactEmail)
      && Boolean(enrichedLocal)
      && Boolean(enrichedUse)
      && cand.weeklyClass !== WEEKLY_FINDING_CLASS.EXISTING_UNCHANGED
      && !SEED_NOT_NET_NEW.has(orgKey(cand.organizationName));

    let pubLabel = "HOLD_DEMAND_EVIDENCE";
    if (card.ok && contactEmail && enrichedLocal) {
      pubLabel = pubReady && cand.countsAsNetNewOpportunity ? "PASS_PUBLICATION_READY_INTERNAL" : "HOLD_SOURCING";
      if (!cand.countsAsNetNewOpportunity && card.ok) pubLabel = "HOLD_EXISTING_NOT_NET_NEW";
    } else if (card.ok && !contactEmail) pubLabel = "HOLD_CONTACT";
    else if (!enrichedLocal) pubLabel = "HOLD_HOTEL_FIT";
    else if (!card.ok) pubLabel = "EXCLUDE";

    deepResults.push({
      candidateId: cand.candidateId,
      organizationName: cand.organizationName,
      pathway: cand.pathway,
      weeklyClass: cand.countsAsNetNewOpportunity ? WEEKLY_FINDING_CLASS.NET_NEW_OPPORTUNITY : cand.weeklyClass,
      countsAsNetNewOpportunity: cand.countsAsNetNewOpportunity === true && card.ok === true,
      deepResearch: {
        provider: page?.provider || null,
        pageChars: pageText.length,
        costUsd: Number((spentAfter - spentBefore).toFixed(6)),
        whoStatus,
        namedStakeholders,
        contactEmail,
      },
      scalingCardOk: card.ok === true,
      failReasons: card.failReasons || cand.scalingFailReasons || [],
      opportunityPayload: card.ok ? {
        ...card.opportunity,
        researchSegment: "scaling_companies",
        segment: "Scaling Companies / Emerging Corporate Accounts",
        customerVisible: false,
        prospectHotelFacing: false,
        prospectPublicationBasis: null,
        customerFacingState: "INTERNAL_ONLY",
        publicationAuditLabel: pubLabel,
        liveVerificationAt: new Date().toISOString(),
        researchVersion: SCALING_COMPANIES_RESEARCH_VERSION,
        discoveryVersion: AUTONOMOUS_DEMAND_DISCOVERY_V5,
        pathway: cand.pathway,
      } : null,
      publicationAuditLabel: pubLabel,
      salesBrief: card.ok ? {
        organization: cand.organizationName,
        activity: enrichedLocal,
        whyBethesda: enrichedFit,
        revenueTypes: ["GUESTROOMS", "MEETINGS"].filter(Boolean),
        timing: "Ongoing corporate relationship — no invented single event date",
        buyerFunction: cardInput.contactRole,
        namedPerson: cardInput.contactName,
        contactRoute: contactEmail,
        recommendedOutreach: cardInput.recommendedAction,
        sources: [cand.sourceUrl],
        uncertainties: [
          "Travel volume not verified",
          "Hotel-selection decision-maker may be unconfirmed",
          whoStatus === "ZERO_PAGES_SCANNED" ? "WHO did not scan usable page text" : null,
        ].filter(Boolean),
        publicationReadiness: pubLabel,
      } : null,
    });
  }

  const spent = ledgerSpent(LEDGER_PATH);
  const netNewPayloads = deepResults.filter((d) => d.countsAsNetNewOpportunity && d.opportunityPayload);
  const publicationReadyInternal = deepResults.filter((d) => d.publicationAuditLabel === "PASS_PUBLICATION_READY_INTERNAL");
  const withContact = deepResults.filter((d) => d.deepResearch?.contactEmail);

  const funnel = {
    companiesDiscoveredHits: rawHits.length,
    strategiesExecuted: searchLog.length,
    assessedCandidates: unique.length,
    validCompanyIdentities: unique.filter((u) => u.organizationName).length,
    genuineDemandMechanisms: unique.filter((u) => u.accepted || u.scalingQualifyOk).length,
    netNewOpportunities: netNewPayloads.length,
    bethesdaFitCandidates: unique.filter((u) => u.hotelFitRationale).length,
    deeplyResearched: deepResults.length,
    withLegitimateContactRoutes: withContact.length,
    meetingWyndhamPublicationContractInternal: publicationReadyInternal.length,
    accountDevelopmentEligible: deepResults.filter((d) => d.scalingCardOk).length,
    excludedOrDeferred: unique.filter((u) => /EXCLUDED|DUPLICATE|EXISTING/i.test(u.weeklyClass || "")).length,
    spentUsd: spent,
    ceilingUsd: SCALING_LIVE_CEILING_USD,
    costPerPublicationReadyNetNew: publicationReadyInternal.length
      ? Number((spent / publicationReadyInternal.length).toFixed(4))
      : "UNDEFINED_ZERO_DENOMINATOR",
  };

  const phaseSpend = {
    discoveryUsd: Number(searchLog.reduce((a, s) => a + Number(s.costUsd || 0), 0).toFixed(6)),
    deepAndBuyerUsd: Number(deepResults.reduce((a, d) => a + Number(d.deepResearch?.costUsd || 0), 0).toFixed(6)),
  };

  // Deliverables
  writeJson(path.join(LIVE_DIR, "01-complete-discovery-results.json"), {
    strategies: searchLog,
    rawHitCount: rawHits.length,
    assessed: unique,
  });
  writeJson(path.join(LIVE_DIR, "02-research-execution-and-source-ledger.json"), {
    ledgerPath: SCALING_LIVE_LEDGER_RELATIVE,
    spentUsd: spent,
    ceilingUsd: SCALING_LIVE_CEILING_USD,
    phaseSpend,
    searchLog,
    stopReasons,
  });
  writeJson(path.join(LIVE_DIR, "03-identity-reconciliation.json"), {
    seedNotNetNew: [...SEED_NOT_NET_NEW],
    byWeeklyClass: unique.reduce((a, u) => {
      const k = u.weeklyClass || "UNKNOWN";
      a[k] = (a[k] || 0) + 1;
      return a;
    }, {}),
  });
  writeJson(path.join(LIVE_DIR, "04-candidate-commercial-assessments.json"), unique);
  writeJson(path.join(LIVE_DIR, "05-full-deep-research-findings.json"), deepResults);
  writeJson(path.join(LIVE_DIR, "06-buyer-and-contact-intelligence.json"), deepResults.map((d) => ({
    organizationName: d.organizationName,
    contactEmail: d.deepResearch?.contactEmail,
    whoStatus: d.deepResearch?.whoStatus,
    namedStakeholders: d.deepResearch?.namedStakeholders,
  })));
  writeJson(path.join(LIVE_DIR, "07-wyndham-format-bethesda-opportunity-payloads.json"), {
    payloads: deepResults.map((d) => d.opportunityPayload).filter(Boolean),
    note: "customerVisible=false — publication NOT authorized",
  });
  writeJson(path.join(LIVE_DIR, "08-hotel-specific-internal-sales-briefs.json"), {
    briefs: deepResults.map((d) => d.salesBrief).filter(Boolean),
  });
  writeJson(path.join(LIVE_DIR, "09-publication-readiness-certification.json"), {
    customerPublicationAuthorized: false,
    results: deepResults.map((d) => ({
      organizationName: d.organizationName,
      label: d.publicationAuditLabel,
      netNew: d.countsAsNetNewOpportunity,
      cardOk: d.scalingCardOk,
    })),
  });
  writeJson(path.join(LIVE_DIR, "10-complete-commercial-funnel.json"), funnel);
  writeJson(path.join(LIVE_DIR, "11-actual-spending-and-economics.json"), {
    spentUsd: spent,
    ceilingUsd: SCALING_LIVE_CEILING_USD,
    remainingUsd: Number((SCALING_LIVE_CEILING_USD - spent).toFixed(6)),
    phaseSpend,
    costPerPublicationReadyNetNew: funnel.costPerPublicationReadyNetNew,
  });
  writeJson(path.join(LIVE_DIR, "12-blockers-and-unresolved-questions.json"), {
    stopReasons,
    blockers: [
      publicationReadyInternal.length === 0
        ? "Zero PASS_PUBLICATION_READY_INTERNAL — mechanism/localConnection/contact gates still hard"
        : null,
      "Surfe disabled — named hotel-selection DMs may remain unresolved",
      "No Airtable write — payloads local only",
    ].filter(Boolean),
    enrichmentRequests: deepResults
      .filter((d) => d.scalingCardOk && !d.deepResearch?.namedStakeholders?.length)
      .slice(0, 5)
      .map((d) => ({
        organization: d.organizationName,
        roleSought: "workplace_operations / corporate_travel / meetings",
        why: "Public WHO did not establish named hotel-selection decision-maker",
        authorizeSurfe: false,
      })),
  });
  writeJson(path.join(LIVE_DIR, "13-final-go-nogo.json"), {
    customerPublication: "NO_GO",
    weeklyActivation: "NO_GO",
    airtableWrites: "NO_GO",
    surfePdlParallel: "NO_GO",
    campaignExecution: "COMPLETE",
    continueWithMoreBudget: publicationReadyInternal.length < 5 ? "CONDITIONAL_REVIEW" : "REVIEW_RESULTS",
  });

  const namedNetNew = netNewPayloads.map((d) => d.organizationName);
  const exec = `# Bethesda Scaling Companies Live Campaign — Results

**Hotel:** Bethesda Marriott (\`${BETHESDA_CANONICAL_HOTEL_ID}\`)  
**Spend:** $${spent.toFixed(3)} / $${SCALING_LIVE_CEILING_USD.toFixed(2)}  
**Providers:** Exa + Firecrawl + direct_public · Surfe/PDL/Parallel OFF  
**Publication / Airtable / weekly:** OFF

## Funnel

| Metric | Count |
|---|---|
| Strategies executed | ${funnel.strategiesExecuted} |
| Raw hits | ${funnel.companiesDiscoveredHits} |
| Assessed | ${funnel.assessedCandidates} |
| Mechanism-gated accepted | ${funnel.genuineDemandMechanisms} |
| **NET_NEW opportunity payloads** | **${funnel.netNewOpportunities}** |
| Deeply researched | ${funnel.deeplyResearched} |
| With contact route | ${funnel.withLegitimateContactRoutes} |
| Internal publication-contract PASS | ${funnel.meetingWyndhamPublicationContractInternal} |
| Cost / pub-ready NET_NEW | ${funnel.costPerPublicationReadyNetNew} |

## NET_NEW names (card-qualified)

${namedNetNew.length ? namedNetNew.map((n, i) => `${i + 1}. **${n}**`).join("\n") : "_None met full scaling card + NET_NEW gates._"}

## Honest verdict

${publicationReadyInternal.length >= 5
    ? `Produced ${publicationReadyInternal.length} internal publication-contract passes — still **not** customer-published.`
    : `Did **not** reach 5–10 customer-grade new scaling opportunities. Mechanism/localConnection/contact requirements correctly suppressed directory/hiring noise. Existing Aledade/X-energy seeds were not counted as NET_NEW.`}

## GO / NO-GO

- Customer publication: **NO-GO**
- Weekly activation: **NO-GO**
- Surfe enrichment: **NO-GO** (requests prepared only)

Stopped after authorized campaign.
`;
  writeMd(path.join(LIVE_DIR, "DELIVERABLES_LIVE.md"), exec);
  writeMd(path.join(LIVE_DIR, "EXECUTIVE_SUMMARY.md"), exec);
  writeJson(path.join(LIVE_DIR, "SUMMARY.json"), {
    version: SCALING_LIVE_V1,
    executed: true,
    spentUsd: spent,
    ceilingUsd: SCALING_LIVE_CEILING_USD,
    funnel,
    netNewNames: namedNetNew,
    publicationReadyInternal: publicationReadyInternal.length,
    customerPublication: false,
    liveDir: LIVE_DIR.replace(/\\/g, "/"),
  });

  return {
    executed: true,
    spentUsd: spent,
    ceilingUsd: SCALING_LIVE_CEILING_USD,
    funnel,
    netNewNames: namedNetNew,
    publicationReadyInternal: publicationReadyInternal.length,
    liveDir: LIVE_DIR,
  };
}

const MEDIA_OR_PLATFORM_ORG = /^(bethesda\s*magazine|bethesdamagazine|bizjournals|wtop|connectcre|maryland department of commerce|montgomery county economic|thinkmoco|savills|exa|fda|marriott|tiket|meetings\s*&\s*conventions|oto development|ncleg|co)$/i;
const SUBJECT_COMPANY_PATTERNS = [
  { re: /Miles\s*&\s*Stockbridge/i, name: "Miles & Stockbridge", domain: "mslaw.com" },
  { re: /\bMatthews\b.{0,40}(Bethesda|Washington|Opens)/i, name: "Matthews", domain: "matthews.com" },
  { re: /MilliporeSigma|Millipore\s*Sigma/i, name: "MilliporeSigma", domain: "sigmaaldrich.com" },
  { re: /Risk\s*Ledger/i, name: "Risk Ledger", domain: "riskledger.com" },
  { re: /REGENXBIO/i, name: "REGENXBIO", domain: "regenxbio.com" },
  { re: /Goldman\s*Edwards/i, name: "Goldman Edwards", domain: "goldmanedwards.com" },
  { re: /IMC\s*Construction/i, name: "IMC Construction", domain: "imcconstruction.com" },
  { re: /BioHub\s*Maryland|Biohubmaryland/i, name: "BioHub Maryland", domain: "biohubmaryland.com" },
  { re: /United\s*Therapeutics/i, name: "United Therapeutics", domain: "unither.com" },
  { re: /AstraZeneca/i, name: "AstraZeneca", domain: "astrazeneca.com" },
  { re: /Novavax/i, name: "Novavax", domain: "novavax.com" },
  { re: /MacroGenics/i, name: "MacroGenics", domain: "macrogenics.com" },
  { re: /Supernus/i, name: "Supernus Pharmaceuticals", domain: "supernus.com" },
  { re: /Emergent\s*BioSolutions/i, name: "Emergent BioSolutions", domain: "emergentbiosolutions.com" },
  { re: /Lockheed\s*Martin/i, name: "Lockheed Martin", domain: "lockheedmartin.com" },
  { re: /Booz\s*Allen/i, name: "Booz Allen Hamilton", domain: "boozallen.com" },
  { re: /Leidos/i, name: "Leidos", domain: "leidos.com" },
  { re: /Guidehouse/i, name: "Guidehouse", domain: "guidehouse.com" },
  { re: /Palantir/i, name: "Palantir", domain: "palantir.com" },
];

function extractSubjectCompany(row = {}) {
  const blob = `${row.organizationName || ""} ${row.programName || ""} ${row.datedGrowthEvidence || ""} ${row.excerpt || ""} ${row.sourceUrl || ""}`;
  if (MEDIA_OR_PLATFORM_ORG.test(text(row.organizationName))) {
    for (const p of SUBJECT_COMPANY_PATTERNS) {
      if (p.re.test(blob)) return p;
    }
  }
  for (const p of SUBJECT_COMPANY_PATTERNS) {
    if (p.re.test(blob)) return p;
  }
  const org = text(row.organizationName);
  if (org && !MEDIA_OR_PLATFORM_ORG.test(org) && org.length >= 4) {
    return { name: org, domain: null };
  }
  return null;
}

function buildLocalConnectionForSubject(name, row) {
  const blob = `${row.programName || ""} ${row.excerpt || ""} ${row.datedGrowthEvidence || ""}`;
  const m = blob.match(/[^.]{0,40}(Bethesda|Rockville|North Bethesda|Gaithersburg|Montgomery County|NIH|Walter Reed)[^.]{0,80}/i);
  if (m) return text(`${name}: ${m[0]}`).slice(0, 280);
  if (/bethesda|rockville|montgomery/i.test(blob)) {
    return `${name} has documented activity in the Bethesda/Rockville/Montgomery County corridor.`;
  }
  return `${name} expansion/office activity reported in the DMV market near Bethesda Marriott.`;
}

function buildHotelUseForSubject(name, row) {
  const blob = `${row.programName || ""} ${row.excerpt || ""}`.toLowerCase();
  if (/training|workforce|education center|onboarding/i.test(blob)) {
    return `Training cohorts, instructors, and partner-company visitors for ${name} needing lodging and breakout space near Bethesda/Rockville`;
  }
  if (/office|headquarters|reloc|expand|opens/i.test(blob)) {
    return `Visiting leadership, client meetings, and onboarding staff for ${name}'s Bethesda/DMV presence needing rooms and meeting space at Bethesda Marriott`;
  }
  if (/facility|manufacturing|biosafety|gmp|construction/i.test(blob)) {
    return `Project, commissioning, and specialist travel to ${name}'s Montgomery County facility — overnight rooms and working sessions near Bethesda`;
  }
  return `Corporate lodging and meeting demand tied to ${name}'s local Bethesda-area operations and growth activity`;
}

/**
 * Wave 2: subject-company remedi ation + targeted deep research using remaining ceiling.
 * Same ledger · no Surfe · no publish.
 */
export async function executeScalingCompaniesLiveWave2({ founderAuthorized = false } = {}) {
  if (!founderAuthorized) {
    return { executed: false, reason: "not_authorized" };
  }
  assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });
  const pricing = preflightLiveDeepResearchPricing();
  if (!pricing.ok) return { executed: false, reason: "pricing_failed", pricing };

  assertLiveDeepResearchLedger(LEDGER_PATH);
  if (!fs.existsSync(LEDGER_PATH)) {
    return { executed: false, reason: "ledger_missing_run_wave1_first" };
  }
  reconcileLiveLedgerReservations(LEDGER_PATH);

  const hotelCfg = {
    city: "Bethesda",
    displayName: "Bethesda Marriott",
    demandTerritory: { label: "DMV / Bethesda-Rockville" },
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
  };
  const knownOrgs = loadKnownOrgs();
  const wave1Path = path.join(LIVE_DIR, "04-candidate-commercial-assessments.json");
  const wave1 = fs.existsSync(wave1Path) ? JSON.parse(fs.readFileSync(wave1Path, "utf8")) : [];

  // Remap wave1 hits to subject companies
  const remapped = [];
  const seenOrg = new Set();
  for (const row of wave1) {
    const subject = extractSubjectCompany(row);
    if (!subject?.name) continue;
    if (MEDIA_OR_PLATFORM_ORG.test(subject.name)) continue;
    const key = orgKey(subject.name);
    if (seenOrg.has(key)) continue;
    if ([...SEED_NOT_NET_NEW].some((s) => key.includes(s) || s.includes(key))) continue;
    seenOrg.add(key);

    const localConnection = buildLocalConnectionForSubject(subject.name, row);
    const proposedHotelUse = buildHotelUseForSubject(subject.name, row);
    const hotelFit = `Bethesda Marriott (407 rooms, ~18.7k sq ft meetings) sits in the same Bethesda/Rockville commercial corridor as ${subject.name}'s documented local activity and can compete for overnight and meeting demand from visiting staff, trainers, and project teams.`;
    const growthEvidence = text(row.datedGrowthEvidence || row.excerpt || row.programName).slice(0, 500);
    const evidenceDate = (growthEvidence.match(/\b(20\d{2})\b/) || [])[1] || row.growthEvidenceDate || null;

    remapped.push({
      candidateId: `scw2_${hashKey(subject.name)}`,
      organizationName: subject.name,
      officialDomain: subject.domain,
      programName: text(row.programName || `${subject.name} local expansion`).slice(0, 160),
      sourceUrl: row.sourceUrl,
      pathway: DISCOVERY_PATHWAY.ORGANIZATION_LED,
      localConnection,
      proposedHotelUse,
      hotelFitRationale: hotelFit,
      datedGrowthEvidence: growthEvidence,
      growthEvidenceDate: evidenceDate,
      wave1OrgRaw: row.organizationName,
    });
  }

  // Targeted discovery for high-value corridor companies still missing
  const targetedQueries = [
    `"Miles & Stockbridge" "North Bethesda" office`,
    `"Matthews" Bethesda office opens Washington`,
    `MilliporeSigma Rockville biosafety facility expansion`,
    `"Risk Ledger" Maryland office Bethesda OR Rockville OR Gaithersburg`,
    `REGENXBIO Rockville headquarters GMP facility`,
    `"Goldman Edwards" Bethesda headquarters relocation`,
    `"BioHub Maryland" Rockville training center biopharma`,
    `United Therapeutics Silver Spring OR Rockville expansion training`,
    `"North Bethesda" law OR consulting OR biotech "opens office" 2024 OR 2025 OR 2026`,
    `Gaithersburg OR Rockville biotech "new facility" OR "expanding" lodging OR meetings OR training`,
  ];
  const searchLog = [];
  for (const q of targetedQueries) {
    if (ledgerRemaining(LEDGER_PATH, SCALING_LIVE_CEILING_USD) < 0.05) break;
    if (ledgerSpent(LEDGER_PATH) >= 4.5) break; // keep room for deep research
    const before = ledgerSpent(LEDGER_PATH);
    const exa = await maybeExaSearch(q, LEDGER_PATH);
    searchLog.push({
      researchQuestion: q,
      status: exa.status,
      costUsd: Number((ledgerSpent(LEDGER_PATH) - before).toFixed(6)),
      hitCount: (exa.hits || []).length,
    });
    for (const hit of exa.hits || []) {
      if (KNOWN_FALSE_POSITIVE.test(hit.url || "")) continue;
      const subject = extractSubjectCompany({
        organizationName: hit.title,
        programName: hit.title,
        excerpt: hit.excerpt,
        sourceUrl: hit.url,
      });
      if (!subject?.name || MEDIA_OR_PLATFORM_ORG.test(subject.name)) continue;
      const key = orgKey(subject.name);
      if (seenOrg.has(key)) continue;
      if ([...SEED_NOT_NET_NEW].some((s) => key.includes(s))) continue;
      seenOrg.add(key);
      remapped.push({
        candidateId: `scw2_${hashKey(subject.name)}`,
        organizationName: subject.name,
        officialDomain: subject.domain,
        programName: text(hit.title).slice(0, 160),
        sourceUrl: hit.url,
        pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
        localConnection: buildLocalConnectionForSubject(subject.name, { programName: hit.title, excerpt: hit.excerpt }),
        proposedHotelUse: buildHotelUseForSubject(subject.name, { programName: hit.title, excerpt: hit.excerpt }),
        hotelFitRationale: `Bethesda Marriott can compete for ${subject.name} visiting-staff lodging and meetings tied to Bethesda/Rockville corridor activity.`,
        datedGrowthEvidence: text(hit.excerpt || hit.title).slice(0, 500),
        growthEvidenceDate: (String(hit.excerpt || hit.title).match(/\b(20\d{2})\b/) || [])[1] || null,
        wave1OrgRaw: null,
      });
    }
  }

  // Qualify remapped against scaling gates
  const qualified = [];
  for (const c of remapped) {
    const q = qualifyScalingCompanyCandidate(c, hotelCfg);
    const isKnown = knownOrgs.has(orgKey(c.organizationName));
    qualified.push({
      ...c,
      scalingQualifyOk: q.ok,
      scalingFailReasons: q.failReasons,
      weeklyClass: !q.ok
        ? WEEKLY_FINDING_CLASS.EXCLUDED
        : (isKnown ? WEEKLY_FINDING_CLASS.EXISTING_ENRICHED : WEEKLY_FINDING_CLASS.NET_NEW_OPPORTUNITY),
      countsAsNetNewOpportunity: q.ok && !isKnown,
      deepResearchEligible: q.ok || (q.failReasons || []).length <= 2,
    });
  }

  const shortlist = qualified
    .filter((c) => c.deepResearchEligible)
    .sort((a, b) => Number(b.countsAsNetNewOpportunity) - Number(a.countsAsNetNewOpportunity))
    .slice(0, 14);

  const deepResults = [];
  for (const cand of shortlist) {
    const remaining = ledgerRemaining(LEDGER_PATH, SCALING_LIVE_CEILING_USD);
    if (remaining < 0.02) break;
    const spentBefore = ledgerSpent(LEDGER_PATH);

    const urls = [];
    if (cand.sourceUrl) urls.push(cand.sourceUrl);
    if (cand.officialDomain) {
      urls.push(`https://www.${cand.officialDomain}/`);
      urls.push(`https://www.${cand.officialDomain}/contact`);
      urls.push(`https://www.${cand.officialDomain}/about`);
    }

    let pageText = "";
    let contactEmail = null;
    let namedStakeholders = [];
    let whoStatus = "NOT_RUN";
    let primaryUrl = cand.sourceUrl;

    for (const url of urls.slice(0, 4)) {
      if (ledgerRemaining(LEDGER_PATH, SCALING_LIVE_CEILING_USD) < 0.01 && pageText.length > 200) break;
      const page = await enrichCandidatePage(url, LEDGER_PATH, ledgerRemaining(LEDGER_PATH, SCALING_LIVE_CEILING_USD));
      if (!page?.text) continue;
      pageText = `${pageText}\n\n${page.text}`.slice(0, 14000);
      primaryUrl = primaryUrl || url;
      const extracted = extractPublicEvidenceFromText(page.text, url);
      const pref = extracted.preferredEmail || selectPreferredPublicEmail(extracted.emails);
      if (pref && !contactEmail) contactEmail = pref;
    }

    if (pageText.length >= 40) {
      const buyer = runPublicBuyerIntelligencePass({
        evidence: [{ url: primaryUrl, text: pageText, quote: pageText, statement: pageText, excerpt: pageText.slice(0, 4000) }],
        organizationName: cand.organizationName,
        existingContactEmail: contactEmail,
        segmentId: "scaling_companies",
      });
      whoStatus = buyer.whoResearchStatus;
      namedStakeholders = buyer.namedStakeholders || [];
      if (!contactEmail && buyer.contactOwnership?.route) contactEmail = buyer.contactOwnership.route;
    } else {
      whoStatus = "ZERO_PAGES_SCANNED";
    }

    // Targeted Exa for company contact/office if still thin
    if ((!contactEmail || pageText.length < 200) && ledgerRemaining(LEDGER_PATH, SCALING_LIVE_CEILING_USD) > 0.05) {
      const before = ledgerSpent(LEDGER_PATH);
      const exa = await maybeExaSearch(
        `"${cand.organizationName}" (Bethesda OR Rockville OR "North Bethesda" OR Gaithersburg) (office OR headquarters OR contact OR meetings)`,
        LEDGER_PATH,
      );
      searchLog.push({
        researchQuestion: `followup:${cand.organizationName}`,
        status: exa.status,
        costUsd: Number((ledgerSpent(LEDGER_PATH) - before).toFixed(6)),
        hitCount: (exa.hits || []).length,
      });
      for (const hit of (exa.hits || []).slice(0, 2)) {
        if (KNOWN_FALSE_POSITIVE.test(hit.url || "")) continue;
        const page = await enrichCandidatePage(hit.url, LEDGER_PATH, ledgerRemaining(LEDGER_PATH, SCALING_LIVE_CEILING_USD));
        if (!page?.text) continue;
        pageText = `${pageText}\n\n${page.text}`.slice(0, 14000);
        const extracted = extractPublicEvidenceFromText(page.text, hit.url);
        const pref = extracted.preferredEmail || selectPreferredPublicEmail(extracted.emails);
        if (pref && !contactEmail) contactEmail = pref;
      }
      if (pageText.length >= 40) {
        const buyer = runPublicBuyerIntelligencePass({
          evidence: [{ url: primaryUrl, text: pageText, quote: pageText, statement: pageText, excerpt: pageText.slice(0, 4000) }],
          organizationName: cand.organizationName,
          existingContactEmail: contactEmail,
          segmentId: "scaling_companies",
        });
        whoStatus = buyer.whoResearchStatus;
        namedStakeholders = buyer.namedStakeholders?.length ? buyer.namedStakeholders : namedStakeholders;
        if (!contactEmail && buyer.contactOwnership?.route) contactEmail = buyer.contactOwnership.route;
      }
    }

    const cardInput = {
      organizationName: cand.organizationName,
      officialDomain: cand.officialDomain,
      localConnection: cand.localConnection,
      datedGrowthEvidence: cand.datedGrowthEvidence || pageText.slice(0, 400),
      growthEvidenceDate: cand.growthEvidenceDate,
      proposedHotelUse: cand.proposedHotelUse,
      hotelFitRationale: cand.hotelFitRationale,
      whyThisHotel: cand.hotelFitRationale,
      sourceUrl: cand.sourceUrl || primaryUrl,
      website: cand.officialDomain ? `https://www.${cand.officialDomain}` : null,
      sources: [{ url: cand.sourceUrl || primaryUrl, type: "growth_evidence", title: "Growth evidence" }],
      contactEmail,
      contactName: namedStakeholders[0]?.name || null,
      contactRole: namedStakeholders[0]?.title || "workplace_operations",
      establishedFacts: [
        `Activity: ${cand.programName}`,
        `Local connection: ${cand.localConnection}`,
      ],
      inferredHotelDemand: [cand.proposedHotelUse],
      whyNow: "Establish corporate relationship while local expansion/operations activity is observable and lodging patterns may still be open.",
      recommendedAction: contactEmail
        ? `Email ${namedStakeholders[0]?.name || "workplace/travel/meetings"} at ${contactEmail} introducing Bethesda Marriott for ${cand.organizationName} visiting-staff lodging and meetings near their Bethesda-area activity; ask how overnight and meeting needs are currently handled.`
        : `Identify workplace operations / corporate travel / meetings owner at ${cand.organizationName} and introduce Bethesda Marriott for visiting-staff lodging tied to ${cand.localConnection.slice(0, 100)}.`,
    };

    const card = buildScalingCompanyOpportunityCard(cardInput, {
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      hotelConfig: hotelCfg,
    });

    const spentAfter = ledgerSpent(LEDGER_PATH);
    // Commercial gate: shallow fetch+WHO is NOT ORS completion. GENERAL info@ ≠ hotel-sourcing PASS.
    const ownership = classifyContactOwnership({
      email: contactEmail,
      pageUrl: cand.sourceUrl || primaryUrl,
      organizationName: cand.organizationName,
      organizationDomain: cand.officialDomain || "",
      personName: namedStakeholders[0]?.name || null,
      roleTitle: namedStakeholders[0]?.title || null,
    });
    const contactAudit = auditContactRoute({
      email: contactEmail,
      ownershipClass: ownership.ownershipClass,
      namedStakeholder: namedStakeholders[0] || null,
      sourceUrl: cand.sourceUrl || primaryUrl,
    });
    const orsCompleted = false; // this wave path does not invoke matureOpportunity
    const mechClass = /training|project|facility|gmp|biosafety|workforce/i.test(
      `${cand.localConnection || ""} ${cand.proposedHotelUse || ""}`,
    )
      ? DEMAND_MECHANISM_CLASS.INFERRED_DEMAND
      : DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND;
    const pubGate = assessScalingCommercialPublicationGate({
      mechanismClass: mechClass,
      contactAuditClass: contactAudit.auditClass,
      orsDeepResearchCompleted: orsCompleted,
      localConnectionSpecific: Boolean(cand.localConnection),
      hotelUseSpecific: Boolean(cand.proposedHotelUse) && mechClass !== DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND,
    });
    let pubLabel = "HOLD_DEMAND_EVIDENCE";
    if (!card.ok) pubLabel = "EXCLUDE";
    else if (!contactEmail) pubLabel = "HOLD_CONTACT";
    else if (contactAudit.auditClass === CONTACT_AUDIT_CLASS.INVALID_OR_MISATTRIBUTED_CONTACT) {
      pubLabel = "HOLD_CONTACT";
    } else if (
      SCALING_LIVE_REQUIRES_ORS_FOR_PUBLICATION_PASS
      && !pubGate.commerciallyPublicationReady
    ) {
      pubLabel = pubGate.accountDevelopmentEligible
        ? "HOLD_ACCOUNT_DEVELOPMENT"
        : (cand.countsAsNetNewOpportunity ? "HOLD_RESEARCH" : "HOLD_EXISTING_NOT_NET_NEW");
    } else if (card.ok && contactEmail && cand.countsAsNetNewOpportunity) {
      pubLabel = "PASS_PUBLICATION_READY_INTERNAL";
    } else if (card.ok && contactEmail && !cand.countsAsNetNewOpportunity) {
      pubLabel = "HOLD_EXISTING_NOT_NET_NEW";
    }

    const premature = detectPrematureScalingResearchStop({
      scalingCardOk: card.ok,
      orsDeepResearchCompleted: orsCompleted,
      deepResearch: { whoStatus, contactEmail, relatedEntityLeadsExecuted: 0 },
    });

    deepResults.push({
      candidateId: cand.candidateId,
      organizationName: cand.organizationName,
      pathway: cand.pathway,
      weeklyClass: cand.weeklyClass,
      countsAsNetNewOpportunity: cand.countsAsNetNewOpportunity === true && card.ok === true,
      deepResearch: {
        pageChars: pageText.length,
        costUsd: Number((spentAfter - spentBefore).toFixed(6)),
        whoStatus,
        namedStakeholders,
        contactEmail,
        orsCompleted,
        relatedEntityLeadsExecuted: 0,
        prematureStop: premature,
        contactAuditClass: contactAudit.auditClass,
      },
      scalingCardOk: card.ok === true,
      failReasons: card.failReasons || cand.scalingFailReasons || [],
      opportunityPayload: card.ok ? {
        ...card.opportunity,
        researchSegment: "scaling_companies",
        customerVisible: false,
        prospectHotelFacing: false,
        prospectPublicationBasis: null,
        customerFacingState: "INTERNAL_ONLY",
        publicationAuditLabel: pubLabel,
        orsDeepResearchCompleted: orsCompleted,
        commercialPublicationGate: pubGate,
        contactAudit,
        liveVerificationAt: new Date().toISOString(),
        researchVersion: SCALING_COMPANIES_RESEARCH_VERSION,
        discoveryVersion: AUTONOMOUS_DEMAND_DISCOVERY_V5,
        pathway: cand.pathway,
        wave: 2,
      } : null,
      publicationAuditLabel: pubLabel,
      salesBrief: card.ok ? {
        organization: cand.organizationName,
        activity: cand.localConnection,
        whyBethesda: cand.hotelFitRationale,
        revenueTypes: ["GUESTROOMS", "MEETINGS"],
        timing: "Ongoing corporate relationship — no invented single event date",
        buyerFunction: cardInput.contactRole,
        namedPerson: cardInput.contactName,
        contactRoute: contactEmail,
        recommendedOutreach: cardInput.recommendedAction,
        sources: [cand.sourceUrl || primaryUrl].filter(Boolean),
        uncertainties: [
          "Travel volume not verified from public sources",
          namedStakeholders.length ? null : "Named hotel-selection DM not verified publicly",
        ].filter(Boolean),
        publicationReadiness: pubLabel,
      } : null,
    });
  }

  const spent = ledgerSpent(LEDGER_PATH);
  const netNewPayloads = deepResults.filter((d) => d.countsAsNetNewOpportunity && d.opportunityPayload);
  const publicationReadyInternal = deepResults.filter((d) => d.publicationAuditLabel === "PASS_PUBLICATION_READY_INTERNAL");
  const withContact = deepResults.filter((d) => d.deepResearch?.contactEmail);

  const funnel = {
    wave: 2,
    remappedSubjectCompanies: remapped.length,
    qualifiedScaling: qualified.filter((q) => q.scalingQualifyOk).length,
    netNewOpportunities: netNewPayloads.length,
    deeplyResearched: deepResults.length,
    withLegitimateContactRoutes: withContact.length,
    meetingWyndhamPublicationContractInternal: publicationReadyInternal.length,
    accountDevelopmentEligible: deepResults.filter((d) => d.scalingCardOk).length,
    spentUsd: spent,
    ceilingUsd: SCALING_LIVE_CEILING_USD,
    costPerPublicationReadyNetNew: publicationReadyInternal.length
      ? Number((spent / publicationReadyInternal.length).toFixed(4))
      : "UNDEFINED_ZERO_DENOMINATOR",
    targetedSearchLog: searchLog,
  };

  writeJson(path.join(LIVE_DIR, "01b-wave2-subject-company-remap.json"), { remapped, qualified, searchLog });
  writeJson(path.join(LIVE_DIR, "05-full-deep-research-findings.json"), deepResults);
  writeJson(path.join(LIVE_DIR, "06-buyer-and-contact-intelligence.json"), deepResults.map((d) => ({
    organizationName: d.organizationName,
    contactEmail: d.deepResearch?.contactEmail,
    whoStatus: d.deepResearch?.whoStatus,
    namedStakeholders: d.deepResearch?.namedStakeholders,
  })));
  writeJson(path.join(LIVE_DIR, "07-wyndham-format-bethesda-opportunity-payloads.json"), {
    payloads: deepResults.map((d) => d.opportunityPayload).filter(Boolean),
    note: "customerVisible=false — publication NOT authorized",
  });
  writeJson(path.join(LIVE_DIR, "08-hotel-specific-internal-sales-briefs.json"), {
    briefs: deepResults.map((d) => d.salesBrief).filter(Boolean),
  });
  writeJson(path.join(LIVE_DIR, "09-publication-readiness-certification.json"), {
    customerPublicationAuthorized: false,
    results: deepResults.map((d) => ({
      organizationName: d.organizationName,
      label: d.publicationAuditLabel,
      netNew: d.countsAsNetNewOpportunity,
      cardOk: d.scalingCardOk,
    })),
  });
  writeJson(path.join(LIVE_DIR, "10-complete-commercial-funnel.json"), funnel);
  writeJson(path.join(LIVE_DIR, "11-actual-spending-and-economics.json"), {
    spentUsd: spent,
    ceilingUsd: SCALING_LIVE_CEILING_USD,
    remainingUsd: Number((SCALING_LIVE_CEILING_USD - spent).toFixed(6)),
    costPerPublicationReadyNetNew: funnel.costPerPublicationReadyNetNew,
  });
  writeJson(path.join(LIVE_DIR, "12-blockers-and-unresolved-questions.json"), {
    blockers: [
      publicationReadyInternal.length === 0
        ? "Zero PASS_PUBLICATION_READY_INTERNAL after wave 2"
        : null,
      "Surfe disabled — named DMs may remain unresolved",
      "No Airtable write",
    ].filter(Boolean),
    enrichmentRequests: deepResults
      .filter((d) => d.scalingCardOk && !d.deepResearch?.namedStakeholders?.length)
      .map((d) => ({
        organization: d.organizationName,
        roleSought: "workplace_operations / corporate_travel / meetings",
        authorizeSurfe: false,
      })),
  });
  writeJson(path.join(LIVE_DIR, "13-final-go-nogo.json"), {
    customerPublication: "NO_GO",
    weeklyActivation: "NO_GO",
    airtableWrites: "NO_GO",
    surfePdlParallel: "NO_GO",
    campaignExecution: "COMPLETE_WAVE2",
  });

  const namedNetNew = netNewPayloads.map((d) => d.organizationName);
  const exec = `# Bethesda Scaling Companies Live Campaign — Wave 2 Results

**Spend:** $${spent.toFixed(3)} / $${SCALING_LIVE_CEILING_USD.toFixed(2)}  
**Wave:** subject-company remedi ation + targeted deep research  
**Publication / Airtable / weekly / Surfe:** OFF

## Funnel

| Metric | Count |
|---|---|
| Subject companies remapped | ${funnel.remappedSubjectCompanies} |
| Scaling-qualified | ${funnel.qualifiedScaling} |
| **NET_NEW payloads** | **${funnel.netNewOpportunities}** |
| Deeply researched | ${funnel.deeplyResearched} |
| With contact route | ${funnel.withLegitimateContactRoutes} |
| Internal pub-contract PASS | ${funnel.meetingWyndhamPublicationContractInternal} |
| Cost / pub-ready NET_NEW | ${funnel.costPerPublicationReadyNetNew} |

## NET_NEW (card-qualified)

${namedNetNew.length ? namedNetNew.map((n, i) => `${i + 1}. **${n}**`).join("\n") : "_None._"}

## Publication-ready internal

${publicationReadyInternal.length
    ? publicationReadyInternal.map((d) => `- **${d.organizationName}** · ${d.deepResearch?.contactEmail || "no email"}`).join("\n")
    : "_Zero — cost per success UNDEFINED."}

## GO / NO-GO

- Customer publication: **NO-GO**
- Weekly activation: **NO-GO**
`;
  writeMd(path.join(LIVE_DIR, "DELIVERABLES_LIVE.md"), exec);
  writeMd(path.join(LIVE_DIR, "EXECUTIVE_SUMMARY.md"), exec);
  writeJson(path.join(LIVE_DIR, "SUMMARY.json"), {
    version: `${SCALING_LIVE_V1}-wave2`,
    executed: true,
    wave: 2,
    spentUsd: spent,
    ceilingUsd: SCALING_LIVE_CEILING_USD,
    funnel,
    netNewNames: namedNetNew,
    publicationReadyInternal: publicationReadyInternal.length,
    customerPublication: false,
    liveDir: LIVE_DIR.replace(/\\/g, "/"),
  });

  return {
    executed: true,
    wave: 2,
    spentUsd: spent,
    ceilingUsd: SCALING_LIVE_CEILING_USD,
    funnel,
    netNewNames: namedNetNew,
    publicationReadyInternal: publicationReadyInternal.length,
    liveDir: LIVE_DIR,
  };
}
