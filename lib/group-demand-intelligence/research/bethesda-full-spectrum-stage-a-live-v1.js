/**
 * Bethesda Full-Spectrum Stage A — founder-authorized live discovery only.
 * Hard ceiling $1.50 · isolated ledger · no Stage B · no Surfe/PDL/Parallel · no Airtable.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { SUPERVISOR_EXPECTED_SEGMENTS } from "./research-constants-v1.js";
import { RESEARCH_SEGMENT_LABEL } from "../workflow/hotel-segment-research-plan-v1.js";
import {
  generateHotelDemandDiscoveryStrategies,
  loadHotelGdiConfig,
  normalizeDiscoveryHit,
  screenDiscoveryHitCommercialRelevance,
  assessCapturableHotelDemandFit,
  BETHESDA_SEGMENT_DISCOVERY_WEIGHT,
} from "./commercial-demand-discovery-v4.js";
import {
  assertLiveDeepResearchLedger,
  loadOrCreateLiveDeepResearchLedger,
  preflightLiveDeepResearchPricing,
  maybeExaSearch,
} from "./deep-research-gdi-live-certification-v1.js";
import {
  assertPaidContactProvidersDisabled,
  classifyPublisherKind,
  PUBLISHER_KIND,
} from "./commercial-spine-restoration-v41.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
  assertBethesdaReconciliationGate,
  runBethesdaDatabaseReconciliationPreflight,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import {
  WEEKLY_FINDING_CLASS,
  buildBethesdaImmutableBaselineManifest,
  classifyFindingAgainstBethesdaBaseline,
} from "./bethesda-weekly-net-new-discovery-v1.js";
import {
  PROPOSED_STAGED_PILOT_BUDGET_USD,
  REPORTS_DIR as FULL_SPECTRUM_REPORTS_DIR,
} from "./bethesda-full-spectrum-discovery-v1.js";

export const BETHESDA_FULL_SPECTRUM_STAGE_A_LIVE_V1 = "gdi-bethesda-full-spectrum-stage-a-live-v1";
export const STAGE_A_CEILING_USD = PROPOSED_STAGED_PILOT_BUDGET_USD.stageADiscoveryUsd; // 1.5

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const LIVE_DIR = path.join(ROOT, FULL_SPECTRUM_REPORTS_DIR, "live-stage-a");
const LEDGER_DIR = path.join(LIVE_DIR, "ledgers");
const STAGE_A_LEDGER = path.join(LEDGER_DIR, "bethesda-full-spectrum-stage-a-discovery.json");

/** Underrepresented-first order for extra discovery budget after one-per-segment coverage. */
export const STAGE_A_SEGMENT_PRIORITY = Object.freeze([
  "corporate_meetings_training",
  "professional_planners_dmc",
  "scaling_companies",
  "local_meetings_catering",
  "exhibitor_sponsor_teams",
  "entertainment_production",
  "tours_educational_specialist",
  "weddings_social_private_events",
  "sports",
  "associations_government",
  "medical_academic",
]);

const GENERIC_ORG_TITLES = /^(hotel|hotels|hotel\s*&\s*travel|travel|lodging|housing|venue|venues|catering|meeting\s*planner|hotel\s*headquarters|accommodations?|home|about|contact|news|events|calendar|search)$/i;

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

function organizationFromHostname(url) {
  try {
    const host = new URL(url).hostname.replace(/^www\./i, "");
    const leaf = host.split(".").slice(0, -1).join(".").split(".").pop() || host;
    const cleaned = leaf.replace(/-?org$/i, "").replace(/-?inc$/i, "").replace(/^\d{4}/, "").replace(/-/g, " ").trim();
    if (!cleaned || cleaned.length < 2) return null;
    if (/^[a-z0-9]{2,8}$/i.test(cleaned.replace(/\s+/g, ""))) {
      return cleaned.replace(/\s+/g, "").toUpperCase();
    }
    return cleaned.replace(/\b\w/g, (c) => c.toUpperCase());
  } catch {
    return null;
  }
}

function organizationFromTitle(title) {
  const t = text(title);
  const afterDash = t.split(/\s+[|\u2013\u2014–—]\s+/);
  if (afterDash.length >= 2) {
    const right = text(afterDash[afterDash.length - 1]);
    if (right && !GENERIC_ORG_TITLES.test(right) && right.length > 4) return right.slice(0, 120);
  }
  return null;
}

export function parseStageAHitIdentity(hit = {}, segment = null) {
  const title = text(hit.title);
  const excerpt = text(hit.excerpt);
  const url = hit.url || hit.sourceUrl || "";
  const blob = `${title} ${excerpt}`;
  const fromTitle = organizationFromTitle(title);
  const fromHost = organizationFromHostname(url);
  let organizationName = fromTitle || fromHost || title.split(/\s+[|\u2013\u2014\-]\s+/)[0] || title;
  organizationName = text(organizationName).replace(/\s*[|:].*$/, "").slice(0, 120);
  if (GENERIC_ORG_TITLES.test(organizationName) || organizationName.length < 3) {
    organizationName = fromHost || fromTitle || null;
  }
  const yearMatch = blob.match(/\b(202[6-9]|2030)\b/) || String(url).match(/\b(202[6-9]|2030)\b/);
  const programName = /conference|summit|symposium|meeting|tournament|cup|gala|workshop|forum|congress|offsite|retreat|training|incentive|dmc|wedding|production|tour|seminar|kickoff/i.test(blob)
    ? title.slice(0, 160)
    : (title.slice(0, 160) || null);
  return {
    organizationName: organizationName ? text(organizationName) : null,
    programName,
    eventYear: yearMatch ? yearMatch[1] : null,
    demandSignalSummary: excerpt.slice(0, 400) || title,
    sourceUrl: url || null,
    publisher: hit.publisher || null,
    independentPublisher: hit.independent === true,
    segment,
    geoHintRelevant: /bethesda|montgomery|nih|walter\s*reed|rockville|dmv|maryland|washington\s*(dc|d\.c)|northern\s*virginia|fairfax|alexandria|arlington|potomac/i.test(`${blob} ${url}`),
    rejectedGenericTitle: !organizationName || GENERIC_ORG_TITLES.test(organizationName),
  };
}

export function isWeakOrGenericCandidate(parsed = {}) {
  if (!parsed.organizationName || parsed.rejectedGenericTitle) return true;
  if (GENERIC_ORG_TITLES.test(parsed.organizationName)) return true;
  if (/^(hotel|travel|lodging|housing|venue|catering)\b/i.test(parsed.organizationName)
    && parsed.organizationName.split(/\s+/).length <= 3) {
    return true;
  }
  return false;
}

/**
 * Select strategies: 1+ per segment, then fill underrepresented-first.
 */
export function selectStageAStrategies(strategyPack, { maxStrategies = 36 } = {}) {
  const bySeg = new Map();
  for (const s of strategyPack.strategies || []) {
    if (!bySeg.has(s.segment)) bySeg.set(s.segment, []);
    bySeg.get(s.segment).push(s);
  }
  const selected = [];
  const seen = new Set();
  const add = (s) => {
    if (!s || selected.length >= maxStrategies) return;
    const key = s.questionFingerprint || s.researchQuestion;
    if (seen.has(key)) return;
    seen.add(key);
    selected.push(s);
  };

  // Round 1: every canonical segment gets one
  for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) {
    add((bySeg.get(seg) || [])[0]);
  }
  // Round 2–N: underrepresented priority extras
  for (const seg of STAGE_A_SEGMENT_PRIORITY) {
    const list = bySeg.get(seg) || [];
    const weight = BETHESDA_SEGMENT_DISCOVERY_WEIGHT[seg] || 1;
    const take = Math.min(list.length, Math.max(1, weight));
    for (let i = 0; i < take; i += 1) add(list[i]);
  }
  // Fill remainder
  for (const s of strategyPack.strategies || []) add(s);
  return selected;
}

export function preflightBethesdaFullSpectrumStageA({ founderAuthorized = false } = {}) {
  const pricing = preflightLiveDeepResearchPricing();
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });
  const recon = runBethesdaDatabaseReconciliationPreflight();
  const reconGate = assertBethesdaReconciliationGate(recon, {
    allowPaidResearch: founderAuthorized,
    founderAuthorizedPaid: founderAuthorized,
  });
  const baseline = buildBethesdaImmutableBaselineManifest({ reconciliation: recon });
  const config = loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID) || {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
    city: "Bethesda",
  };
  const strategyPack = generateHotelDemandDiscoveryStrategies(config, { maxPerSegment: 4 });
  const selected = selectStageAStrategies(strategyPack);

  // Refuse if ledger already has spend from a prior attempt with different identity — require fresh file
  let ledgerFresh = true;
  if (fs.existsSync(STAGE_A_LEDGER)) {
    const led = JSON.parse(fs.readFileSync(STAGE_A_LEDGER, "utf8"));
    if (Number(led.estimatedSpentUsd || 0) > 0) {
      ledgerFresh = false;
    }
  }

  const checks = {
    founderAuthorized: founderAuthorized === true,
    pricingOk: pricing.ok === true,
    paidContactGuardOk: paidGuard.ok === true,
    reconciliationGateOk: reconGate.ok === true,
    baselineImmutable: baseline.immutable === true,
    baselineDistinctOpportunities: baseline.counts?.distinctCanonicalOpportunities,
    baselineSourceRecords: baseline.counts?.sourceRecords,
    allElevenStrategies: strategyPack.allElevenSegmentsCovered === true,
    selectedStrategyCount: selected.length,
    stageACeilingUsd: STAGE_A_CEILING_USD,
    ledgerPath: STAGE_A_LEDGER.replace(/\\/g, "/"),
    ledgerIsolatedNew: true,
    ledgerFreshOrEmpty: ledgerFresh,
    reusePriorNineDollarBalance: false,
    stageBDisabled: true,
    surfePdlParallelOff: true,
    airtableOff: true,
    weeklyOff: true,
    publicationOff: true,
    exaCredential: pricing.checks?.exaCredential,
    firecrawlCredential: pricing.checks?.firecrawlCredential,
  };

  const ok = checks.founderAuthorized
    && checks.pricingOk
    && checks.paidContactGuardOk
    && checks.reconciliationGateOk
    && checks.baselineImmutable
    && checks.allElevenStrategies
    && checks.ledgerFreshOrEmpty
    && Number(checks.stageACeilingUsd) === 1.5;

  return {
    ok,
    checks,
    pricing,
    reconGate,
    strategyCount: selected.length,
    underrepresentedPrioritized: strategyPack.underrepresentedSegmentsPrioritized || [],
  };
}

const COMPETITOR_HOTEL_ORG = /\b(canopy(?:\s+by\s+hilton)?|marriott|hilton|hyatt|ihg|wyndham|westin|sheraton|renaissance|courtyard|residence\s*inn|embassy\s*suites|doubletree|four\s*seasons|bethesdan)\b/i;
const SOCIAL_OR_DIRECTORY_HOST = /\b(linkedin\.com|facebook\.com|twitter\.com|x\.com|instagram\.com|youtube\.com|tiktok\.com)\b/i;
const HIRING_NEWS_ONLY = /\b(hiring|jobs?|careers?|companies hiring|workforce|open roles|laid-?off|talent for)\b/i;
const HOUSING_TAB_TITLE = /^(travel and lodging|hotel accomodation|hotel accommodation|hotels?|housing|hotel information|play and stay|nike .+ hotel info)\b/i;
const SPECIFIC_PROGRAM = /\b(conference|summit|symposium|tournament|gala|workshop|forum|congress|offsite|retreat|training programme|training program|incentive|rfp|host city|meet-up|meetup|hill day|advocacy day|education conference|kickoff|invitational|annual meeting|coaches conference)\b/i;
const PLANNER_MECHANISM = /\b(dmc|destination management|meeting planner|incentive travel)\b/i;
const COMMITTED_COMPETITOR_VENUE = /\b(hyatt\s+regency\s+bethesda|bethesda\s+north\s+marriott|natcher\s+(conference|auditorium|center)|william\s+f\.?\s+bolger|bolger\s+(hotel|center)|canopy\s+(by\s+hilton|washington)|held\s+at\s+(the\s+)?(hyatt|hilton|bethesdan)|capital\s+hilton|hilton\s+rosslyn|the\s+bethesdan(\s+hotel)?|neuroscience\s+center\s*\||6001\s+executive\s+boulevard)\b/i;
const PLATFORM_ORG = /^(exa|cvent|events?\s*registration|visitmontgomery|washington\s*dc|bethesdamagazine)$/i;
const EXA_PLACES_URL = /exa\.ai\/library\/places/i;
const CVB_HOTEL_DIRECTORY = /visitmontgomery\.com\/(directory|meetings)|washington\.org\/meetings\/find/i;
const AUTHOR_PAGE = /\bauthor\s+at\b|\/author\//i;
const OPEN_HOST_RFP = /\b(request for proposal|request for proposals|\brfp\b|host\s+city|host\s+202[6-9]|conference host|meet-?up rfp|call for host|event management services)\b/i;
const DMC_LANDING_ONLY = /\b(destination management|dmc services|incentive travel|full service travel|meet our team|our team|event production across|corporate av|av vendors)\b/i;
const NEWS_PUBLISHER = /bethesdamagazine\.com|washingtonpost\.com|bizjournals\.com/i;
const PAST_EVENT_REVIEW = /\b(2024|2025)\b.{0,40}\b(meeting review|recap|held at|brought together)\b|\b(meeting review|recap)\b.{0,20}\b(2024|2025)\b/i;
const VENDOR_LANDING = /\b(event production|corporate av|audio,?\s*video|lighting and live production)\b/i;

/**
 * Commercial quality gate — Stage A discovery hits ≠ automatic NET_NEW commercial prospects.
 */
export function applyStageACommercialQualityGate(row = {}) {
  const org = text(row.organizationName);
  const program = text(row.programName || row.demandMechanism);
  const url = String(row.sourceUrl || "");
  const evidence = text(row.demandEvidence || "");
  const blob = `${org} ${program} ${evidence}`;
  const pubKind = classifyPublisherKind(url, program);
  const reasons = [];
  let commercialVerdict = "EXCLUDE";

  if (!org || GENERIC_ORG_TITLES.test(org) || org.length < 3 || PLATFORM_ORG.test(org)) {
    reasons.push("WEAK_OR_GENERIC_ORG_IDENTITY");
  }
  if (SOCIAL_OR_DIRECTORY_HOST.test(url) || EXA_PLACES_URL.test(url)) {
    reasons.push("SOCIAL_DIRECTORY_OR_SYNTHETIC_PUBLISHER");
  }
  if (CVB_HOTEL_DIRECTORY.test(url) || /visitmontgomery/i.test(org)) {
    reasons.push("CVB_OR_HOTEL_DIRECTORY_LISTING");
  }
  if (pubKind === PUBLISHER_KIND.VENUE_OR_HOTEL_MARKETING
    || COMPETITOR_HOTEL_ORG.test(org)
    || /canopybethesda/i.test(url)) {
    reasons.push("COMPETITOR_OR_VENUE_MARKETING_PAGE");
  }
  if (NEWS_PUBLISHER.test(url) || (HIRING_NEWS_ONLY.test(`${org} ${program}`) && !OPEN_HOST_RFP.test(blob))) {
    reasons.push("HIRING_OR_EXPANSION_NEWS_WITHOUT_DEMAND_MECHANISM");
  }
  if (HOUSING_TAB_TITLE.test(program) || row.housingTabOnly
    || /\bhotel info\b/i.test(program)) {
    reasons.push("GENERIC_HOUSING_OR_TRAVEL_TAB");
  }
  if (AUTHOR_PAGE.test(`${program} ${url}`) || /matt ney/i.test(blob)) {
    reasons.push("AUTHOR_OR_BLOG_NOISE");
  }
  if (/bethesda premier cup|bethesdasoccertournaments|premiercup\.bethesda/i.test(`${blob} ${url}`)) {
    reasons.push("BASELINE_RELATED_BETHESDA_PREMIER_CUP");
  }
  if (/linkedin/i.test(org)) {
    reasons.push("PLATFORM_NAME_AS_ORGANIZATION");
  }
  if (PAST_EVENT_REVIEW.test(blob)) {
    reasons.push("PAST_EVENT_REVIEW_NOT_FORWARD_DEMAND");
  }
  if (VENDOR_LANDING.test(blob) && !OPEN_HOST_RFP.test(blob) && !/\b(202[6-9]).{0,40}(conference|summit|gala|meeting)\b/i.test(blob)) {
    reasons.push("VENDOR_LANDING_NOT_HOTEL_DEMAND");
  }
  if (/\b(elkridge|national\s+harbor|baltimore|annapolis|tysons|rosslyn)\b/i.test(blob)
    && !/\bbethesda\b/i.test(blob)
    && !OPEN_HOST_RFP.test(blob)) {
    reasons.push("DISTANT_VENUE_WITHOUT_BETHESDA_PATHWAY");
  }

  const hasProgram = SPECIFIC_PROGRAM.test(blob) || OPEN_HOST_RFP.test(blob);
  const isPlannerLanding = (PLANNER_MECHANISM.test(blob) || VENDOR_LANDING.test(blob))
    && DMC_LANDING_ONLY.test(blob)
    && !OPEN_HOST_RFP.test(blob)
    && !/\b(202[6-9]|2030)\b.{0,60}(conference|summit|tournament|gala|hill day)\b/i.test(blob);
  const committedElsewhere = COMMITTED_COMPETITOR_VENUE.test(blob);

  if (!hasProgram && !PLANNER_MECHANISM.test(blob)) {
    reasons.push("NO_SPECIFIC_PROGRAM_OR_PLANNER_MECHANISM");
  }
  if (committedElsewhere) {
    reasons.push("COMMITTED_TO_NAMED_COMPETITOR_OR_CAMPUS_VENUE");
  }
  if (isPlannerLanding) {
    reasons.push("DMC_OR_PLANNER_LANDING_WITHOUT_NAMED_PROGRAM");
  }

  const hardNoise = reasons.some((r) => [
    "WEAK_OR_GENERIC_ORG_IDENTITY",
    "SOCIAL_DIRECTORY_OR_SYNTHETIC_PUBLISHER",
    "CVB_OR_HOTEL_DIRECTORY_LISTING",
    "COMPETITOR_OR_VENUE_MARKETING_PAGE",
    "HIRING_OR_EXPANSION_NEWS_WITHOUT_DEMAND_MECHANISM",
    "GENERIC_HOUSING_OR_TRAVEL_TAB",
    "AUTHOR_OR_BLOG_NOISE",
    "BASELINE_RELATED_BETHESDA_PREMIER_CUP",
    "PLATFORM_NAME_AS_ORGANIZATION",
    "NO_SPECIFIC_PROGRAM_OR_PLANNER_MECHANISM",
    "PAST_EVENT_REVIEW_NOT_FORWARD_DEMAND",
    "VENDOR_LANDING_NOT_HOTEL_DEMAND",
    "DISTANT_VENUE_WITHOUT_BETHESDA_PATHWAY",
  ].includes(r));

  let weeklyClass = row.weeklyClass;
  let countsAsNetNew = row.countsAsNetNew === true;
  let excludeReason = row.excludeReason;
  let demandCredibleStrict = false;
  let stageBEligible = false;
  let accountDevelopmentOnly = false;

  if (hardNoise) {
    weeklyClass = WEEKLY_FINDING_CLASS.EXCLUDED;
    countsAsNetNew = false;
    excludeReason = reasons.join("|");
    commercialVerdict = "EXCLUDE_NOISE";
  } else if (committedElsewhere) {
    // Local competitor commitment may imply overflow — not Stage B deep-research default
    weeklyClass = WEEKLY_FINDING_CLASS.EXCLUDED;
    countsAsNetNew = false;
    excludeReason = reasons.join("|");
    commercialVerdict = "DEFER_COMMITTED_ELSEWHERE_OVERFLOW_POSSIBLE";
  } else if (isPlannerLanding) {
    accountDevelopmentOnly = true;
    countsAsNetNew = false;
    weeklyClass = WEEKLY_FINDING_CLASS.EXCLUDED;
    excludeReason = "DMC_OR_PLANNER_LANDING_WITHOUT_NAMED_PROGRAM";
    commercialVerdict = "ACCOUNT_DEVELOPMENT_ONLY";
  } else if (countsAsNetNew && hasProgram) {
    demandCredibleStrict = true;
    const fitOk = /CAPTURABLE/i.test(String(row.fitClass || ""));
    const openRfp = OPEN_HOST_RFP.test(blob);
    const localDmvChapter = /\b(mpi\s+potomac|potomac.?s\s+educational|icf\s+metro\s*dc|metro\s*dc\s+chapter|capital\s+coaches\s+conference)\b/i.test(blob);
    // Open host RFPs may lack geo yet still deserve research if DMV chapter or underrepresented
    const stageBFit = fitOk || localDmvChapter || (openRfp && row.underrepresentedSegment);
    stageBEligible = demandCredibleStrict && stageBFit && !row.housingTabOnly;
    commercialVerdict = stageBEligible
      ? "PROMISING_FOR_STAGE_B_REVIEW"
      : (openRfp ? "OPEN_RFP_WEAK_GEO_REVIEW" : "WEAK_OR_NEEDS_RESEARCH");
  } else {
    countsAsNetNew = false;
    weeklyClass = WEEKLY_FINDING_CLASS.EXCLUDED;
    excludeReason = (excludeReason || reasons.join("|") || "not_commercially_credible");
    commercialVerdict = "EXCLUDED";
  }

  return {
    ...row,
    weeklyClass,
    countsAsNetNew,
    excludeReason,
    demandCredible: demandCredibleStrict,
    demandCredibleLoose: row.demandCredible === true,
    commercialQualityFlags: reasons,
    commercialQuality: commercialVerdict,
    commercialVerdict,
    accountDevelopmentOnly,
    deepResearchEligible: stageBEligible,
    stageBEligible,
    publisherKind: pubKind,
  };
}

/** Re-score + quality-gate a unique assessed batch (no API spend). */
export function finalizeStageAAssessments(rows = []) {
  return rows.map((raw) => {
    // Prefer pre-gate identity fields if a prior remedi ate already mutated the row
    const base = {
      ...raw,
      weeklyClass: raw.weeklyClassPreGate || raw.weeklyClass,
      countsAsNetNew: raw.countsAsNetNewPreGate != null
        ? raw.countsAsNetNewPreGate
        : raw.countsAsNetNew,
      demandCredible: raw.demandCredibleLoose != null
        ? raw.demandCredibleLoose
        : raw.demandCredible,
      excludeReason: raw.excludeReasonPreGate || null,
    };
    if (base.countsAsNetNewPreGate == null && raw.countsAsNetNew != null) {
      base.countsAsNetNewPreGate = raw.countsAsNetNew;
      base.weeklyClassPreGate = raw.weeklyClass;
    }
    const gated = applyStageACommercialQualityGate({
      ...base,
      countsAsNetNew: base.countsAsNetNewPreGate != null
        ? base.countsAsNetNewPreGate
        : (/NET_NEW/i.test(String(base.weeklyClassPreGate || base.weeklyClass || ""))
          || base.countsAsNetNew === true),
    });
    const rank = scoreStageBEligibility(gated);
    const stageBEligible = gated.stageBEligible === true
      && gated.countsAsNetNew
      && gated.demandCredible
      && !gated.housingTabOnly
      && rank.score >= 40
      && gated.commercialVerdict === "PROMISING_FOR_STAGE_B_REVIEW";
    const limitedOpenRfp = gated.commercialVerdict === "OPEN_RFP_WEAK_GEO_REVIEW";
    return {
      ...gated,
      countsAsNetNewPreGate: base.countsAsNetNewPreGate ?? raw.countsAsNetNew,
      weeklyClassPreGate: base.weeklyClassPreGate || raw.weeklyClass,
      stageBScore: stageBEligible || limitedOpenRfp ? rank.score : 0,
      stageBScoreReasons: rank.reasons,
      stageBEligible,
      deepResearchEligible: stageBEligible,
      openRfpReviewOnly: limitedOpenRfp,
      recommendedStageBBudgetUsd: stageBEligible
        ? (rank.score >= 60 ? 0.45 : 0.35)
        : (limitedOpenRfp ? 0.25 : 0),
      stageBResearchQuestions: buildStageBQuestions(gated),
    };
  });
}

function scoreStageBEligibility(candidate = {}) {
  let score = 0;
  const reasons = [];
  if (!(candidate.countsAsNetNew && candidate.demandCredible)) {
    return { score: 0, reasons: ["not_commercially_credible_net_new"] };
  }
  if (candidate.weeklyClass === WEEKLY_FINDING_CLASS.NET_NEW_OPPORTUNITY
    || candidate.weeklyClass === "NET_NEW") {
    score += 25;
    reasons.push("net_new");
  }
  if (candidate.demandCredible) {
    score += 20;
    reasons.push("credible_demand");
  }
  if (candidate.fitClass === "POTENTIALLY_CAPTURABLE_CORE") {
    score += 22;
    reasons.push("core_geo_fit");
  } else if (candidate.fitClass === "POTENTIALLY_CAPTURABLE_DMV_OR_ACCOUNT") {
    score += 14;
    reasons.push("dmv_or_account_fit");
  }
  if (candidate.deepResearchEligible) {
    score += 10;
    reasons.push("deep_research_eligible");
  }
  if (candidate.eventYear && Number(candidate.eventYear) >= 2026) {
    score += 8;
    reasons.push("forward_looking");
  }
  if (candidate.underrepresentedSegment) {
    score += 4;
    reasons.push("underrepresented_segment_bonus_small");
  }
  if (candidate.contactRouteHint) {
    score += 6;
    reasons.push("contact_hint");
  }
  if (candidate.housingTabOnly) {
    score -= 12;
    reasons.push("housing_tab_penalty");
  }
  if ((candidate.commercialQualityFlags || []).length) {
    score = 0;
    reasons.push("quality_gate_failed");
  }
  return { score: Math.max(0, score), reasons };
}

function buildStageBQuestions(c = {}) {
  return [
    `What is the specific dated programme or recurring demand mechanism for ${c.organizationName}?`,
    `Could Bethesda Marriott (${c.fitClass || "fit TBD"}) realistically compete for rooms/meetings — geography, capacity, timing?`,
    `Who controls hotel sourcing / housing / meeting venue selection at ${c.organizationName}?`,
    `Is there an open sourcing window, preferred-hotel process, or account-development path (not a committed distant host)?`,
    `What legitimate contact route (named buyer, functional inbox, or intermediary) reaches the sourcing owner?`,
  ];
}

/**
 * Execute Stage A discovery only.
 */
export async function executeBethesdaFullSpectrumStageA({
  founderAuthorized = false,
  maxStrategies = 36,
} = {}) {
  const preflight = preflightBethesdaFullSpectrumStageA({ founderAuthorized });
  if (!founderAuthorized) {
    return { executed: false, reason: "founder_authorized_required", preflight };
  }
  if (!preflight.ok) {
    return { executed: false, reason: "preflight_failed", preflight };
  }

  fs.mkdirSync(LIVE_DIR, { recursive: true });
  fs.mkdirSync(LEDGER_DIR, { recursive: true });

  // Fresh ledger only — if prior spend exists, refuse (do not reuse)
  if (fs.existsSync(STAGE_A_LEDGER)) {
    const existing = JSON.parse(fs.readFileSync(STAGE_A_LEDGER, "utf8"));
    if (Number(existing.estimatedSpentUsd || 0) > 0) {
      return {
        executed: false,
        reason: "ledger_already_has_spend_create_new_isolated_file",
        ledgerPath: STAGE_A_LEDGER.replace(/\\/g, "/"),
      };
    }
  }

  assertLiveDeepResearchLedger(STAGE_A_LEDGER);
  loadOrCreateLiveDeepResearchLedger(STAGE_A_LEDGER, {
    budgetUsd: STAGE_A_CEILING_USD,
    version: `${BETHESDA_FULL_SPECTRUM_STAGE_A_LIVE_V1}-discovery-ledger`,
  });

  const baseline = buildBethesdaImmutableBaselineManifest();
  const config = loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID) || {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
    city: "Bethesda",
    demandTerritory: { label: "DMV" },
  };
  if (!config.city) config.city = "Bethesda";
  if (!config.displayName) config.displayName = BETHESDA_CANONICAL_HOTEL_NAME;

  const strategyPack = generateHotelDemandDiscoveryStrategies(config, { maxPerSegment: 4 });
  const selected = selectStageAStrategies(strategyPack, { maxStrategies });

  const searchLog = [];
  const allHits = [];
  const stopReasons = [];

  for (const strategy of selected) {
    const remaining = ledgerRemaining(STAGE_A_LEDGER, STAGE_A_CEILING_USD);
    if (remaining < 0.004) {
      stopReasons.push("discovery_budget_exhausted");
      searchLog.push({
        segment: strategy.segment,
        strategy: strategy.researchQuestion,
        status: "budget_exhausted",
        hits: 0,
        costUsd: 0,
      });
      break;
    }
    const result = await maybeExaSearch(strategy.researchQuestion, STAGE_A_LEDGER);
    searchLog.push({
      segment: strategy.segment,
      strategy: strategy.researchQuestion,
      status: result.status,
      hits: (result.hits || []).length,
      costUsd: result.costUsd || 0,
      underrepresentedPriority: strategy.underrepresentedPriority === true,
      sourceTypes: strategy.sourceTypes || [],
    });
    for (const hit of result.hits || []) {
      allHits.push({ hit, strategy });
    }
  }

  const assessed = [];
  for (const { hit, strategy } of allHits) {
    const parsed = parseStageAHitIdentity(hit, strategy.segment);
    parsed.researchQuestion = strategy.researchQuestion;
    parsed.discoveryWeight = strategy.discoveryWeight;
    parsed.underrepresentedSegment = (BETHESDA_SEGMENT_DISCOVERY_WEIGHT[strategy.segment] || 0) >= 3;

    if (isWeakOrGenericCandidate(parsed)) {
      assessed.push({
        ...parsed,
        weeklyClass: WEEKLY_FINDING_CLASS.EXCLUDED,
        excludeReason: "generic_or_weak_identity",
        stageBEligible: false,
      });
      continue;
    }

    const screen = screenDiscoveryHitCommercialRelevance(parsed, config);
    const fit = screen.capturableFit || assessCapturableHotelDemandFit(parsed, config);
    if (!screen.pass) {
      assessed.push({
        ...parsed,
        weeklyClass: WEEKLY_FINDING_CLASS.EXCLUDED,
        excludeReason: (screen.rejectReasons || []).join("|") || "commercial_screen_fail",
        fitClass: fit.fitClass,
        stageBEligible: false,
        screen,
      });
      continue;
    }

    const normalized = normalizeDiscoveryHit(parsed, {
      hotel: config,
      strategy: { segment: strategy.segment, researchQuestion: strategy.researchQuestion },
    });

    let classification = classifyFindingAgainstBethesdaBaseline({
      organizationName: normalized.organizationName,
      programName: normalized.programName,
      eventName: normalized.programName,
      eventYear: parsed.eventYear || null,
      demandSignalSummary: normalized.demandSignalSummary,
      demandMechanism: normalized.programName || normalized.demandSignalSummary,
      sourceUrl: normalized.sourceUrl,
      segment: normalized.segment,
    }, baseline);

    if (!normalized.programName
      && !/conference|meeting|tournament|summit|symposium|offsite|retreat|training|incentive|wedding|production|seminar/i.test(
        normalized.demandSignalSummary || "",
      )) {
      classification = {
        class: WEEKLY_FINDING_CLASS.EXCLUDED,
        reason: "no_specific_demand_mechanism",
        countsAsNetNew: false,
        resolutionKey: classification.resolutionKey,
      };
    } else if (parsed.eventYear && Number(parsed.eventYear) < 2026) {
      classification = {
        class: WEEKLY_FINDING_CLASS.EXCLUDED,
        reason: "past_cycle",
        countsAsNetNew: false,
        resolutionKey: classification.resolutionKey,
      };
    }

    const demandCredible = Boolean(
      normalized.programName
      || /offsite|retreat|training|meeting|conference|tournament|incentive|dmc|wedding|production|seminar|symposium/i.test(
        `${normalized.programName || ""} ${normalized.demandSignalSummary || ""}`,
      ),
    );
    const housingTabOnly = fit.housingTabOnly === true;
    const underrepresented = (BETHESDA_SEGMENT_DISCOVERY_WEIGHT[strategy.segment] || 0) >= 3;

    const row = {
      candidateId: `stagea_${hashKey(`${normalized.organizationName}::${normalized.programName}::${normalized.sourceUrl}`)}`,
      organizationName: normalized.organizationName,
      programName: normalized.programName,
      demandMechanism: normalized.programName || normalized.demandSignalSummary,
      primarySegment: strategy.segment,
      segmentLabel: RESEARCH_SEGMENT_LABEL[strategy.segment] || strategy.segment,
      sourceUrl: normalized.sourceUrl,
      publisher: normalized.publisher || parsed.publisher,
      independentPublisher: normalized.independentPublisher,
      eventYear: parsed.eventYear,
      geographyHint: parsed.geoHintRelevant ? "DMV_OR_LOCAL_HINT" : "GEO_UNVERIFIED",
      demandEvidence: normalized.demandSignalSummary,
      evidenceKind: housingTabOnly ? "INFERRED_FROM_HOUSING_OR_THIN_PAGE" : "DISCOVERY_SNIPPET",
      verifiedVsInferred: housingTabOnly ? "INFERRED" : "UNVERIFIED_DISCOVERY_EVIDENCE",
      fitClass: fit.fitClass,
      deepResearchEligible: fit.deepResearchEligible === true && classification.countsAsNetNew === true,
      accountDevelopmentEligible: fit.accountDevelopmentEligible === true,
      housingTabOnly,
      knownCommitments: /headquarters|already contracted|host hotel/i.test(
        `${normalized.demandSignalSummary || ""} ${normalized.programName || ""}`,
      ) ? "POSSIBLE_COMMITMENT_LANGUAGE_IN_SNIPPET" : "UNKNOWN",
      likelyBuyerFunction: underrepresented && /dmc|planner/i.test(strategy.segment)
        ? "third_party_planner_or_dmc_sourcing"
        : "program_or_housing_owner_unverified",
      contactRouteHint: null,
      contactFabricated: false,
      weeklyClass: classification.class,
      weeklyReason: classification.reason,
      countsAsNetNew: classification.countsAsNetNew === true,
      demandCredible,
      underrepresentedSegment: underrepresented,
      researchQuestion: strategy.researchQuestion,
      excludeReason: classification.countsAsNetNew ? null : (classification.reason || classification.class),
      unansweredQuestions: buildStageBQuestions({
        organizationName: normalized.organizationName,
        fitClass: fit.fitClass,
      }),
      hotelFacingPublication: false,
      stageBEligible: false,
    };

    assessed.push(row);
  }

  // Dedupe assessed by org+program
  const seenKeys = new Set();
  const deduped = [];
  for (const row of assessed) {
    const key = `${text(row.organizationName).toLowerCase()}::${text(row.programName || row.demandMechanism).toLowerCase()}`;
    if (seenKeys.has(key) && row.countsAsNetNew) {
      row.weeklyClass = WEEKLY_FINDING_CLASS.DUPLICATE;
      row.excludeReason = "duplicate_in_stage_a_batch";
      row.countsAsNetNew = false;
      row.stageBEligible = false;
    }
    if (row.organizationName) seenKeys.add(key);
    deduped.push(row);
  }

  const uniqueAssessed = finalizeStageAAssessments(deduped);
  const spent = ledgerSpent(STAGE_A_LEDGER);
  return writeStageALiveDeliverables({
    uniqueAssessed,
    searchLog,
    stopReasons,
    selected,
    allHits,
    baseline,
    spent,
    remediated: false,
  });
}

/**
 * Rewrite Stage A deliverables from saved inventory with strict commercial gates.
 * Zero additional provider spend.
 */
export function remediateBethesdaFullSpectrumStageAFromInventory({
  inventoryPath = path.join(LIVE_DIR, "03-complete-candidate-inventory.raw-pre-gate.json"),
} = {}) {
  const primaryInv = path.join(LIVE_DIR, "03-complete-candidate-inventory.json");
  const rawPath = fs.existsSync(inventoryPath) ? inventoryPath : primaryInv;
  if (!fs.existsSync(rawPath)) {
    throw new Error(`Stage A inventory missing: ${rawPath}`);
  }
  // Preserve first-seen pre-gate inventory once
  if (!fs.existsSync(path.join(LIVE_DIR, "03-complete-candidate-inventory.raw-pre-gate.json"))
    && fs.existsSync(primaryInv)) {
    fs.copyFileSync(primaryInv, path.join(LIVE_DIR, "03-complete-candidate-inventory.raw-pre-gate.json"));
  }
  const inv = JSON.parse(fs.readFileSync(
    fs.existsSync(path.join(LIVE_DIR, "03-complete-candidate-inventory.raw-pre-gate.json"))
      ? path.join(LIVE_DIR, "03-complete-candidate-inventory.raw-pre-gate.json")
      : rawPath,
    "utf8",
  ));
  const tracesPath = path.join(LIVE_DIR, "02-eleven-segment-discovery-traces.json");
  const summaryPath = path.join(LIVE_DIR, "SUMMARY.json");
  const traces = fs.existsSync(tracesPath)
    ? JSON.parse(fs.readFileSync(tracesPath, "utf8"))
    : { searchLog: [], stopReasons: [], strategiesSelected: 0 };
  const priorSummary = fs.existsSync(summaryPath)
    ? JSON.parse(fs.readFileSync(summaryPath, "utf8"))
    : {};
  const recon = runBethesdaDatabaseReconciliationPreflight();
  const baseline = buildBethesdaImmutableBaselineManifest({ reconciliation: recon });
  const uniqueAssessed = finalizeStageAAssessments(inv.candidates || []);
  const spent = ledgerSpent(STAGE_A_LEDGER);
  return writeStageALiveDeliverables({
    uniqueAssessed,
    searchLog: traces.searchLog || [],
    stopReasons: traces.stopReasons || ["remediated_commercial_quality_gate_zero_new_spend"],
    selected: { length: traces.strategiesSelected || 0 },
    allHits: { length: priorSummary.grossHits || 0 },
    baseline,
    spent,
    remediated: true,
  });
}

export function writeStageALiveDeliverables({
  uniqueAssessed,
  searchLog = [],
  stopReasons = [],
  selected = { length: 0 },
  allHits = { length: 0 },
  baseline = {},
  spent = 0,
  remediated = false,
} = {}) {
  const netNew = uniqueAssessed.filter((r) => r.countsAsNetNew);
  const stageBRanked = [...uniqueAssessed]
    .filter((r) => r.stageBEligible)
    .sort((a, b) => (b.stageBScore || 0) - (a.stageBScore || 0));
  const openRfpReview = [...uniqueAssessed]
    .filter((r) => r.openRfpReviewOnly)
    .sort((a, b) => (b.stageBScore || 0) - (a.stageBScore || 0));
  const accountDev = uniqueAssessed.filter((r) => r.accountDevelopmentOnly || r.commercialVerdict === "ACCOUNT_DEVELOPMENT_ONLY");
  const deferredCommitted = uniqueAssessed.filter((r) => r.commercialVerdict === "DEFER_COMMITTED_ELSEWHERE_OVERFLOW_POSSIBLE");

  const funnelBySegment = {};
  for (const seg of SUPERVISOR_EXPECTED_SEGMENTS) {
    const searches = searchLog.filter((s) => s.segment === seg);
    const rows = uniqueAssessed.filter((r) => r.primarySegment === seg);
    funnelBySegment[seg] = {
      label: RESEARCH_SEGMENT_LABEL[seg] || seg,
      searchesPerformed: searches.length,
      searchStatuses: searches.map((s) => s.status),
      candidatesDiscovered: rows.length,
      validIdentities: rows.filter((r) => r.organizationName && !/WEAK_OR_GENERIC/i.test(String(r.excludeReason || ""))).length,
      genuineNetNew: rows.filter((r) => r.countsAsNetNew).length,
      credibleDemand: rows.filter((r) => r.demandCredible && r.countsAsNetNew).length,
      bethesdaFit: rows.filter((r) => /CAPTURABLE/i.test(String(r.fitClass || ""))).length,
      duplicates: rows.filter((r) => r.weeklyClass === WEEKLY_FINDING_CLASS.DUPLICATE
        || /duplicate/i.test(String(r.excludeReason || ""))).length,
      exclusions: rows.filter((r) => !r.countsAsNetNew).length,
      stageBEligible: rows.filter((r) => r.stageBEligible).length,
      candidateNames: rows.map((r) => ({
        organization: r.organizationName,
        program: r.programName,
        class: r.weeklyClass,
        commercialQuality: r.commercialQuality,
        fit: r.fitClass,
        stageB: r.stageBEligible,
        flags: r.commercialQualityFlags || [],
      })),
      spendUsdOnSegment: Number(searches.reduce((a, s) => a + Number(s.costUsd || 0), 0).toFixed(6)),
    };
  }

  writeJson(path.join(LIVE_DIR, "01-provider-and-spending-ledger.json"), {
    ledgerPath: STAGE_A_LEDGER.replace(/\\/g, "/"),
    ceilingUsd: STAGE_A_CEILING_USD,
    spentUsd: spent,
    remainingUsd: ledgerRemaining(STAGE_A_LEDGER, STAGE_A_CEILING_USD),
    isolatedFromPriorAuthorizations: true,
    reusePriorBalances: false,
    additionalSpendOnRemediationUsd: 0,
    providersUsed: ["exa"],
    providersForbidden: ["surfe", "pdl", "parallel"],
    stageBNotExecuted: true,
    ledger: fs.existsSync(STAGE_A_LEDGER)
      ? JSON.parse(fs.readFileSync(STAGE_A_LEDGER, "utf8"))
      : null,
  });
  writeJson(path.join(LIVE_DIR, "02-eleven-segment-discovery-traces.json"), {
    strategiesSelected: selected.length || searchLog.length,
    searchLog,
    stopReasons,
  });
  writeJson(path.join(LIVE_DIR, "03-complete-candidate-inventory.json"), {
    count: uniqueAssessed.length,
    commercialQualityGateApplied: true,
    remediatedFromSavedHits: remediated,
    candidates: uniqueAssessed,
  });
  writeJson(path.join(LIVE_DIR, "04-net-new-identity-reconciliation.json"), {
    baselineDistinctOpportunities: baseline.counts?.distinctCanonicalOpportunities,
    byClass: Object.fromEntries(
      Object.values(WEEKLY_FINDING_CLASS).map((c) => [
        c,
        uniqueAssessed.filter((r) => r.weeklyClass === c).length,
      ]),
    ),
    commercialQuality: Object.fromEntries(
      [
        "PROMISING_FOR_STAGE_B_REVIEW",
        "OPEN_RFP_WEAK_GEO_REVIEW",
        "WEAK_OR_NEEDS_RESEARCH",
        "ACCOUNT_DEVELOPMENT_ONLY",
        "DEFER_COMMITTED_ELSEWHERE_OVERFLOW_POSSIBLE",
        "EXCLUDE_NOISE",
        "EXCLUDED",
      ].map((k) => [k, uniqueAssessed.filter((r) => r.commercialQuality === k).length]),
    ),
    netNew: netNew.map((r) => ({
      candidateId: r.candidateId,
      organizationName: r.organizationName,
      programName: r.programName,
      segment: r.primarySegment,
      sourceUrl: r.sourceUrl,
      commercialQuality: r.commercialQuality,
      stageBEligible: r.stageBEligible,
    })),
    note: "Research-memory recoveries are not counted as NET_NEW. Noise (housing tabs, competitor hotels, hiring lists, LinkedIn, Premier Cup baseline) excluded.",
  });
  writeJson(path.join(LIVE_DIR, "05-geographic-and-hotel-fit-assessments.json"), {
    items: uniqueAssessed.map((r) => ({
      candidateId: r.candidateId,
      organizationName: r.organizationName,
      fitClass: r.fitClass,
      deepResearchEligible: r.deepResearchEligible,
      accountDevelopmentEligible: r.accountDevelopmentEligible,
      housingTabOnly: r.housingTabOnly,
      geographyHint: r.geographyHint,
      commercialQuality: r.commercialQuality,
    })),
  });
  writeJson(path.join(LIVE_DIR, "06-commercial-evidence-assessment.json"), {
    items: uniqueAssessed.filter((r) => r.countsAsNetNew || r.stageBEligible).map((r) => ({
      candidateId: r.candidateId,
      organizationName: r.organizationName,
      demandMechanism: r.demandMechanism,
      evidenceKind: r.evidenceKind,
      verifiedVsInferred: r.verifiedVsInferred,
      demandCredible: r.demandCredible,
      knownCommitments: r.knownCommitments,
      commercialQuality: r.commercialQuality,
    })),
  });
  writeJson(path.join(LIVE_DIR, "07-candidate-exclusion-reasons.json"), {
    excluded: uniqueAssessed
      .filter((r) => !r.countsAsNetNew)
      .map((r) => ({
        organizationName: r.organizationName,
        programName: r.programName,
        weeklyClass: r.weeklyClass,
        excludeReason: r.excludeReason,
        commercialQualityFlags: r.commercialQualityFlags || [],
        segment: r.primarySegment,
      })),
  });
  writeJson(path.join(LIVE_DIR, "08-segment-by-segment-funnel.json"), funnelBySegment);
  writeJson(path.join(LIVE_DIR, "09-ranked-candidates-for-stage-b.json"), {
    autoAdvanceToStageB: false,
    ranked: stageBRanked.slice(0, 20).map((r, i) => ({
      rank: i + 1,
      candidateId: r.candidateId,
      organizationName: r.organizationName,
      programName: r.programName,
      primarySegment: r.primarySegment,
      fitClass: r.fitClass,
      stageBScore: r.stageBScore,
      stageBScoreReasons: r.stageBScoreReasons,
      recommendedStageBBudgetUsd: r.recommendedStageBBudgetUsd,
      sourceUrl: r.sourceUrl,
      demandEvidence: (r.demandEvidence || "").slice(0, 400),
      commercialQuality: r.commercialQuality,
    })),
    openRfpWeakGeoReview: openRfpReview.slice(0, 10).map((r, i) => ({
      rank: i + 1,
      organizationName: r.organizationName,
      programName: r.programName,
      primarySegment: r.primarySegment,
      fitClass: r.fitClass,
      sourceUrl: r.sourceUrl,
      recommendedUsdIfAuthorized: r.recommendedStageBBudgetUsd,
    })),
    accountDevelopmentOnly: accountDev.slice(0, 15).map((r) => ({
      organizationName: r.organizationName,
      programName: r.programName,
      sourceUrl: r.sourceUrl,
      segment: r.primarySegment,
    })),
    deferredCommittedElsewhere: deferredCommitted.slice(0, 15).map((r) => ({
      organizationName: r.organizationName,
      programName: r.programName,
      sourceUrl: r.sourceUrl,
      note: "Named venue already committed — overflow only if room-block gap verified",
    })),
    note: "Ranking for review only — Stage B requires separate founder GO. Do not inflate toward 10–15.",
  });
  writeJson(path.join(LIVE_DIR, "10-stage-b-research-questions.json"), {
    byCandidate: [...stageBRanked, ...openRfpReview].slice(0, 15).map((r) => ({
      candidateId: r.candidateId,
      organizationName: r.organizationName,
      commercialQuality: r.commercialQuality,
      questions: r.stageBResearchQuestions,
    })),
  });
  writeJson(path.join(LIVE_DIR, "11-recommended-budget-allocation-by-candidate.json"), {
    stageBCeilingIfAuthorizedUsd: PROPOSED_STAGED_PILOT_BUDGET_USD.stageBDeepResearchUsd,
    buyerReserveUsd: PROPOSED_STAGED_PILOT_BUDGET_USD.stageBBuyerReserveUsd,
    allocations: stageBRanked.slice(0, 12).map((r) => ({
      candidateId: r.candidateId,
      organizationName: r.organizationName,
      recommendedUsd: r.recommendedStageBBudgetUsd,
      score: r.stageBScore,
    })),
    openRfpOptionalAllocations: openRfpReview.slice(0, 5).map((r) => ({
      organizationName: r.organizationName,
      recommendedUsd: r.recommendedStageBBudgetUsd,
      note: "Only if founder accepts weak-geo open host RFP research",
    })),
    sumRecommendedUsd: Number(
      stageBRanked.slice(0, 12)
        .reduce((a, r) => a + Number(r.recommendedStageBBudgetUsd || 0), 0)
        .toFixed(4),
    ),
  });

  const strengths = [];
  const weaknesses = [];
  const netNewUnderrep = netNew.filter((r) => r.underrepresentedSegment);
  if (netNewUnderrep.length) {
    strengths.push(`${netNewUnderrep.length} commercially gated NET_NEW from underrepresented segments`);
  }
  if (netNew.length === 0) {
    weaknesses.push("Zero genuine NET_NEW after baseline + commercial quality gate");
  }
  if (spent > 0 && netNew.length === 0) {
    weaknesses.push("Spend incurred without commercially credible NET_NEW — discovery still noisy");
  }
  if (uniqueAssessed.filter((r) => r.commercialQuality === "EXCLUDE_NOISE").length) {
    weaknesses.push(
      `${uniqueAssessed.filter((r) => r.commercialQuality === "EXCLUDE_NOISE").length} hits excluded as noise (housing tabs, competitor hotels, hiring lists, social, baseline sports)`,
    );
  }
  const emptySegs = SUPERVISOR_EXPECTED_SEGMENTS.filter((s) => funnelBySegment[s].genuineNetNew === 0);
  if (emptySegs.length) {
    weaknesses.push(`No commercially credible NET_NEW in segments: ${emptySegs.join(", ")}`);
  }
  if (searchLog.some((s) => s.status === "ok")) {
    strengths.push("Live Exa discovery executed across planned strategies under isolated ledger");
  }
  if (remediated) {
    strengths.push("Commercial quality gate re-applied offline to saved hits (no additional spend)");
  }

  writeJson(path.join(LIVE_DIR, "12-commercial-strengths-and-weaknesses.json"), {
    strengths,
    weaknesses,
    netNewCount: netNew.length,
    stageBEligibleCount: stageBRanked.length,
    noiseExcludedCount: uniqueAssessed.filter((r) => r.commercialQuality === "EXCLUDE_NOISE").length,
    spentUsd: spent,
  });

  const stageBGo = stageBRanked.length >= 3 && spent <= STAGE_A_CEILING_USD + 1e-9
    ? "CONDITIONAL_GO_STAGE_B_AFTER_FOUNDER_REVIEW"
    : (stageBRanked.length >= 1
      ? "CONDITIONAL_GO_LIMITED_STAGE_B_AFTER_REVIEW"
      : "NO_GO_STAGE_B_INSUFFICIENT_NET_NEW_QUALITY");

  writeJson(path.join(LIVE_DIR, "13-go-nogo-stage-b.json"), {
    decision: stageBGo,
    autoAdvance: false,
    stageBNotExecuted: true,
    rationale: [
      `Stage A spent $${spent} of $${STAGE_A_CEILING_USD}`,
      `Genuine commercially gated NET_NEW: ${netNew.length}`,
      `Stage B eligible (strict): ${stageBRanked.length}`,
      "Separate founder GO required before any deep research spend",
      "Surfe/PDL/Parallel remain OFF",
      remediated ? "Deliverables remediated from saved Stage A hits (zero new spend)" : "Fresh Stage A write",
    ],
    doNot: [
      "execute Stage B without new authorization",
      "count Stage A candidates as hotel-facing actionable",
      "publish or write Airtable",
      "activate weekly production",
    ],
  });

  const summary = {
    version: BETHESDA_FULL_SPECTRUM_STAGE_A_LIVE_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    executed: true,
    remediated,
    completedAt: new Date().toISOString(),
    spentUsd: spent,
    ceilingUsd: STAGE_A_CEILING_USD,
    strategiesAttempted: searchLog.length,
    strategiesOk: searchLog.filter((s) => s.status === "ok").length,
    grossHits: allHits.length || 0,
    assessedCandidates: uniqueAssessed.length,
    genuineNetNew: netNew.length,
    stageBEligible: stageBRanked.length,
    noiseExcluded: uniqueAssessed.filter((r) => r.commercialQuality === "EXCLUDE_NOISE").length,
    stageBDecision: stageBGo,
    stageBExecuted: false,
    hotelFacingActionableClaimed: 0,
    paidContactEnrichment: 0,
    airtableWrites: 0,
    stopReasons,
    liveDir: LIVE_DIR.replace(/\\/g, "/"),
  };
  writeJson(path.join(LIVE_DIR, "SUMMARY.json"), summary);

  writeMd(path.join(LIVE_DIR, "DELIVERABLES_LIVE.md"), `# Bethesda Full-Spectrum Stage A — Live Discovery

**Executed:** yes · **Spend:** $${spent} / $${STAGE_A_CEILING_USD}${remediated ? " · **Remediated commercial gate:** yes (no new spend)" : ""}  
**Genuine NET_NEW (post quality gate):** ${netNew.length} · **Stage B eligible:** ${stageBRanked.length} · **Open-RFP weak-geo:** ${openRfpReview.length} · **Noise/deferred excluded:** ${uniqueAssessed.filter((r) => !r.countsAsNetNew).length}  
**Stage B:** NOT executed · Decision: **${stageBGo}**

## Stage B shortlist (strict)

${stageBRanked.length
    ? stageBRanked.map((r, i) => `${i + 1}. **${r.organizationName}** — ${r.programName || r.demandMechanism} (\`${r.primarySegment}\`) · score ${r.stageBScore} · $${r.recommendedStageBBudgetUsd} · ${r.sourceUrl || ""}`).join("\n")
    : "_Empty — insufficient commercially credible NET_NEW for Stage B._"}

## Open host / RFP (weak geo — founder judgment)

${openRfpReview.length
    ? openRfpReview.map((r, i) => `${i + 1}. **${r.organizationName}** — ${r.programName} (\`${r.primarySegment}\`) · fit=${r.fitClass} · ${r.sourceUrl || ""}`).join("\n")
    : "_None._"}

## Account-development only (not Stage B programs)

${accountDev.length
    ? accountDev.slice(0, 12).map((r) => `- ${r.organizationName} — ${r.programName || ""}`).join("\n")
    : "_None retained._"}

## Deferred — committed competitor/campus venue

${deferredCommitted.length
    ? deferredCommitted.slice(0, 12).map((r) => `- ${r.organizationName} — ${r.programName || ""}`).join("\n")
    : "_None._"}

## All commercially gated NET_NEW names

${netNew.length
    ? netNew.map((r, i) => `${i + 1}. **${r.organizationName}** — ${r.programName || r.demandMechanism} (\`${r.primarySegment}\`) · ${r.commercialQuality}`).join("\n")
    : "_None after baseline + commercial quality gate._"}

## Strengths

${strengths.map((s) => `- ${s}`).join("\n") || "- (none)"}

## Weaknesses

${weaknesses.map((s) => `- ${s}`).join("\n") || "- (none)"}

Hotel-facing actionable from Stage A: **0** (discovery only — by design).
`);

  return summary;
}
