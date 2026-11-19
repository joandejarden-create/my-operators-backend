/**
 * GDI Phase 2B — targeted official-domain deepen for two productive cases only.
 * Reuses fetchOfficialPage + public extractors + ORS/eligibility/challenger.
 * Hard-denies paid providers. No Airtable / publication writes.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { fetchOfficialPage } from "../official-page-fetch.js";
import { extractPublicEvidenceFromText } from "../research/direct-public-executor-v1.js";
import { runPublicBuyerIntelligencePass } from "../research/commercial-depth-buyer-intelligence-v1.js";
import { classifyDemandMechanismEvidence } from "../demand-mechanism-research-v1.js";
import {
  assessRetrievedPageQuality,
  extractAndCacheNav,
} from "../source-discovery-v1.js";
import { evaluateOpportunity } from "../research/opportunity-research-supervisor-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import { recoverCommercialEvidence } from "../commercial-evidence-recovery-v1.js";
import { DENIED_METERED_PROVIDERS } from "../adaptive-public-research-controller-v1.js";
import { PUBLIC_FETCH_COST_USD } from "../workflow/gdi-evidence-acquisition-v2.js";
import { runBaselineCase } from "./gdi-20opp-adaptive-investigation-experiment-v1.js";
import {
  loadFrozenCase,
  runIndependentEvidenceChallenger as runPrepChallenger,
  scorePhase2Arm as scorePrepArm,
} from "./gdi-20opp-live-research-prep-v1.js";

export const PHASE2B_ID = "gdi_20opp_phase2b_official_domain_deepen_v1";
export const PHASE2B_VERSION = "2026-11-19.1";
export const MAX_RETRIEVALS_PER_CASE = 12;
export const MAX_PAID_USD = 0;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export const PHASE2B_CASES = Object.freeze([
  {
    opportunityId: "gdi_lmc_00562bf81b1e1957",
    hotelId: "recnPB3Q02NO8Ma1o",
    hotelName: "Wyndham San Diego Bayside",
    allowedOrigins: Object.freeze([
      "https://cmaa-sd.org",
      "https://cmaasd.starchapter.com",
      "https://www.cmaa.org",
    ]),
    seedUrls: Object.freeze([
      "https://cmaa-sd.org/board.php",
      "https://cmaa-sd.org/meetinginfo.php?id=170&ts=1746580016",
      "https://cmaa-sd.org/meetinginfo.php?id=206&ts=1772512890",
      "https://cmaa-sd.org/",
      "https://cmaa-sd.org/events.php",
      "https://cmaa-sd.org/about.php",
      "https://cmaa-sd.org/contact.php",
    ]),
    priorityPathRe:
      /board|officer|director|staff|contact|about|event|meeting|award|banquet|calendar|leadership/i,
  },
  {
    opportunityId: "gdi_opp_2027_ddaa_annual_conference_8",
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    allowedOrigins: Object.freeze(["https://www.appalachiandevelopment.org"]),
    seedUrls: Object.freeze([
      "https://www.appalachiandevelopment.org/events/2027-ddaa-annual-conference/",
      "https://www.appalachiandevelopment.org/about/",
      "https://www.appalachiandevelopment.org/about-us/",
      "https://www.appalachiandevelopment.org/board-members-officers/",
      "https://www.appalachiandevelopment.org/contact-us/",
      "https://www.appalachiandevelopment.org/events/",
      "https://www.appalachiandevelopment.org/events/annual-conference/",
    ]),
    priorityPathRe:
      /board|officer|staff|contact|about|2027|conference|housing|hotel|accommodation|registration|agenda/i,
  },
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function originOf(url) {
  try {
    return new URL(url).origin;
  } catch {
    return "";
  }
}

function normalizeUrl(url) {
  try {
    const u = new URL(url);
    u.hash = "";
    return u.toString().replace(/\/$/, "");
  } catch {
    return "";
  }
}

export function assertZeroPaidProviders() {
  return {
    ok: Number(PUBLIC_FETCH_COST_USD) === 0,
    denied: [...DENIED_METERED_PROVIDERS],
    publicFetchCostUsd: PUBLIC_FETCH_COST_USD,
  };
}

function collectSeedUrls(opportunity, caseCfg) {
  const fromOpp = [
    opportunity.website,
    opportunity.organizationWebsite,
    opportunity.sourceUrl,
    opportunity.eventUrl,
    ...(opportunity.sources || []).map((s) => s.url || s.sourceUrl),
    ...(opportunity.evidence || []).map((e) => e.url || e.sourceUrl),
    opportunity.contact?.sourceUrl,
    opportunity.contact?.emailSource,
  ].filter(Boolean);
  return [...new Set([...caseCfg.seedUrls, ...fromOpp].map(normalizeUrl).filter(Boolean))];
}

function isAllowedUrl(url, caseCfg) {
  const origin = originOf(url);
  return caseCfg.allowedOrigins.some((o) => origin === o || origin === o.replace(/\/$/, ""));
}

function rankUrl(url, caseCfg) {
  try {
    const p = new URL(url).pathname + new URL(url).search;
    if (caseCfg.priorityPathRe.test(p)) return 0;
    if (/housing|hotel|room.?block|accommodation/i.test(p)) return 1;
    if (/event|meeting|conference|banquet/i.test(p)) return 2;
    return 5;
  } catch {
    return 9;
  }
}

function scanCompetitorVenue(textBlob) {
  const hits = [];
  const patterns = [
    /marriott marquis/i,
    /hilton\b/i,
    /hyatt\b/i,
    /westin\b/i,
    /sheraton\b/i,
    /hosted (at|by)|held at|venue[:\s]/i,
    /official hotel|headquarters hotel|host hotel/i,
  ];
  for (const re of patterns) {
    if (re.test(textBlob)) hits.push(re.source);
  }
  return hits;
}

function scanLodgingDemand(textBlob) {
  return {
    housingLanguage: /housing|hotel block|room block|accommodation|official hotel|host hotel|headquarters hotel/i.test(
      textBlob,
    ),
    meetingsLanguage: /meeting|banquet|conference|seminar|training|awards/i.test(textBlob),
    timingHits: [...new Set(textBlob.match(/\b(2026|2027|2028)\b/g) || [])],
  };
}

/**
 * Deepen one case with same-origin official fetches only.
 */
export async function deepenOfficialDomainCase(caseCfg, opts = {}) {
  const t0 = Date.now();
  const billing = assertZeroPaidProviders();
  if (!billing.ok) {
    throw new Error("Refusing Phase 2B — public fetch cost not verified $0");
  }

  const frozen = loadFrozenCase(caseCfg.opportunityId);
  const baselineOpp = frozen.frozenOpportunity;
  const baseline = runBaselineCase(baselineOpp, { opportunityId: caseCfg.opportunityId });

  const recovery = recoverCommercialEvidence(baselineOpp);
  let working = JSON.parse(
    JSON.stringify(recovery.recoveredOpportunity || baselineOpp),
  );

  const maxRetrievals = Number(opts.maxRetrievalsPerCase) || MAX_RETRIEVALS_PER_CASE;
  const attempted = new Set();
  const queue = collectSeedUrls(baselineOpp, caseCfg)
    .filter((u) => isAllowedUrl(u, caseCfg))
    .sort((a, b) => rankUrl(a, caseCfg) - rankUrl(b, caseCfg));

  const pages = [];
  const failures = [];
  const navCache = new Map();
  const retrievalRecords = [];
  let retrievals = 0;
  let actualPaidUsd = 0;

  while (queue.length && retrievals < maxRetrievals) {
    queue.sort((a, b) => rankUrl(a, caseCfg) - rankUrl(b, caseCfg));
    const url = queue.shift();
    const key = normalizeUrl(url);
    if (!key || attempted.has(key)) continue;
    if (!isAllowedUrl(url, caseCfg)) continue;
    attempted.add(key);

    const started = Date.now();
    const page = await fetchOfficialPage(url, {
      allowRender: false,
      timeoutMs: 20000,
    });
    retrievals += 1;
    const latencyMs = Date.now() - started;
    actualPaidUsd += Number(PUBLIC_FETCH_COST_USD) || 0;

    if (!page?.ok) {
      failures.push({
        url,
        error: page?.error || "fetch_failed",
        latencyMs,
        provider: "official_page",
      });
      retrievalRecords.push({
        mocked: false,
        strategyId: "OFFICIAL_DOMAIN_DEEPEN",
        informationFound: false,
        retrievals: 1,
        urls: [url],
        provenance: { provider: "official_page", phase: "2B" },
      });
      continue;
    }

    const quality = assessRetrievedPageQuality(page, url);
    const finalUrl = page.finalUrl || url;
    if (!isAllowedUrl(finalUrl, caseCfg)) {
      failures.push({
        url,
        finalUrl,
        error: "redirected_off_allowlist",
        latencyMs,
      });
      continue;
    }

    const bodyText = page.text || "";
    const extraction = extractPublicEvidenceFromText(bodyText, finalUrl);
    const demandClass = classifyDemandMechanismEvidence(working, extraction, {
      sourceUrl: finalUrl,
    });
    const buyerPass = runPublicBuyerIntelligencePass({
      evidence: [
        {
          url: finalUrl,
          text: bodyText,
          statement: bodyText,
          retrievedAt: new Date().toISOString(),
          sourceFamily: "FREE_PUBLIC_FETCH",
        },
      ],
      organizationName: working.organizationName || caseCfg.opportunityId,
      existingContactEmail: working.contact?.email || null,
      segmentId: working.researchSegment || "association_events",
    });

    const lodging = scanLodgingDemand(bodyText);
    const competitor = scanCompetitorVenue(bodyText);
    // Exclude asset/API noise from "useful" commercial evidence.
    const isNoiseUrl =
      /\.(css|js|png|jpe?g|gif|svg|woff2?)(\?|$)/i.test(finalUrl) ||
      /\/wp-json\//i.test(finalUrl) ||
      /\/wp-content\/plugins\//i.test(finalUrl);
    const useful =
      !isNoiseUrl &&
      quality.usable === true &&
      (extraction.hasHousingLanguage ||
        extraction.hasStaffRoleLanguage ||
        extraction.emails?.length ||
        (buyerPass.stakeholders || []).length > 0 ||
        lodging.housingLanguage ||
        competitor.length > 0 ||
        (/2027/.test(bodyText) && /conference/i.test(finalUrl)));

    pages.push({
      url: finalUrl,
      seedUrl: url,
      ok: true,
      usable: quality.usable === true,
      useful,
      latencyMs,
      retrievedAt: new Date().toISOString(),
      titleHint: text(bodyText).slice(0, 80),
      extraction: {
        emails: extraction.emails || [],
        preferredEmail: extraction.preferredEmail || null,
        phones: extraction.phones || [],
        hasHousingLanguage: extraction.hasHousingLanguage === true,
        hasStaffRoleLanguage: extraction.hasStaffRoleLanguage === true,
        futureYears: extraction.futureYears || [],
        attendeeCount: extraction.attendeeCount,
        roomHint: extraction.roomHint,
      },
      demandClass: demandClass
        ? {
            evidenceClass: demandClass.evidenceClass || demandClass.class || null,
            strengthensEligibility: demandClass.strengthensEligibility === true,
          }
        : null,
      stakeholders: (buyerPass.stakeholders || []).map((s) => ({
        name: s.name,
        title: s.title,
        email: s.email || null,
        buyerClassification: s.buyerClassification,
        decisionMakerClass: s.decisionMakerClass,
        responsibilityClass: s.responsibilityClass,
        sourceUrl: s.sourceUrl,
        verifiedAt: s.verifiedAt,
        // Only keep emails that appeared on the page extraction (no fabrication)
        emailPubliclyPresent:
          s.email &&
          (extraction.emails || []).some(
            (e) => e.toLowerCase() === String(s.email).toLowerCase(),
          ),
      })),
      lodging,
      competitorVenueSignals: competitor,
      excerpt: text(bodyText).slice(0, 500),
    });

    retrievalRecords.push({
      mocked: false,
      strategyId: "OFFICIAL_DOMAIN_DEEPEN",
      informationFound: useful,
      retrievals: 1,
      urls: [finalUrl],
      provenance: { provider: "official_page", phase: "2B", useful },
    });

    // Discover same-origin nav and enqueue
    try {
      const nav = extractAndCacheNav(bodyText, finalUrl, navCache) || [];
      for (const n of nav) {
        const nu = normalizeUrl(n);
        if (nu && isAllowedUrl(nu, caseCfg) && !attempted.has(nu)) {
          queue.push(nu);
        }
      }
    } catch {
      /* ignore */
    }

    // Also enqueue obvious same-origin hrefs from priority paths
    const hrefRe = /href=["']([^"']+)["']/gi;
    let m;
    while ((m = hrefRe.exec(page.html || bodyText))) {
      try {
        const abs = new URL(m[1], finalUrl).toString();
        const nu = normalizeUrl(abs);
        if (nu && isAllowedUrl(nu, caseCfg) && !attempted.has(nu) && rankUrl(nu, caseCfg) <= 2) {
          queue.push(nu);
        }
      } catch {
        /* ignore */
      }
    }
  }

  // Aggregate stakeholders with public evidence only
  const namedContacts = [];
  const seenPeople = new Set();
  for (const p of pages) {
    for (const s of p.stakeholders || []) {
      const key = `${text(s.name).toLowerCase()}|${text(s.title).toLowerCase()}`;
      if (!s.name || seenPeople.has(key)) continue;
      seenPeople.add(key);
      namedContacts.push({
        ...s,
        email: s.emailPubliclyPresent ? s.email : null,
        contactKind: /PROBABLE_RELEVANT_BUYER|POTENTIAL_DECISION_MAKER/i.test(
          `${s.buyerClassification} ${s.decisionMakerClass}`,
        )
          ? "direct_or_probable_buyer"
          : "entry_point_or_influencer",
        confidence: s.emailPubliclyPresent ? "named_with_public_email" : "named_role_only",
      });
    }
  }

  const usefulPages = pages.filter((p) => p.useful);
  const housingPages = pages.filter(
    (p) => p.extraction.hasHousingLanguage || p.lodging.housingLanguage,
  );
  const competitorPages = pages.filter((p) => (p.competitorVenueSignals || []).length);

  // Do NOT auto-promote: only annotate working clone for evaluation
  working.phase2bDeepenV1 = {
    pagesFetched: pages.length,
    usefulPages: usefulPages.length,
    namedContactsFound: namedContacts.length,
    housingSignalPages: housingPages.map((p) => p.url),
    competitorSignals: competitorPages.map((p) => ({
      url: p.url,
      signals: p.competitorVenueSignals,
    })),
  };

  // Apply best publicly evidenced named contact only if email present OR role is commercially relevant
  const bestContact = namedContacts.find((c) => c.email) ||
    namedContacts.find((c) => c.contactKind === "direct_or_probable_buyer") ||
    namedContacts[0] ||
    null;
  if (bestContact && !working.contact?.email) {
    working.contact = {
      ...(working.contact || {}),
      name: bestContact.name,
      fullName: bestContact.name,
      title: bestContact.title,
      role: bestContact.title,
      email: bestContact.email || null,
      sourceUrl: bestContact.sourceUrl,
      emailVerificationStatus: bestContact.email ? "PUBLIC_PAGE_OBSERVED" : "UNVERIFIED",
      contactDataOrigin: "PUBLIC_SOURCE",
      phase2b: true,
    };
  }

  const independent = runPrepChallenger({
    baselineOpportunity: baselineOpp,
    workingOpportunity: working,
    recovery,
    retrievalRecords,
  });

  let shared = evaluateSharedCommercialEligibility(working);
  // Preserve gates — never auto SALES_READY from deepen alone without challenger
  if (independent.salesReadyBlockedByChallenger && shared.salesReady === true) {
    shared = {
      ...shared,
      status: "RESEARCH_CONTINUATION",
      salesReady: false,
      commercialEligible: false,
      challengerOverride: "blocked_unsupported_or_unverified_buyer",
      gaps: [...(shared.gaps || []), "CHALLENGER_BLOCKED_SALES_READY"],
    };
  }

  const supervisor = evaluateOpportunity(working, { requireFutureDemand: false });
  const costs = { actualPaidUsd, estimatedUsd: 0, retrievals, failures: failures.length };
  const rubric = scorePrepArm({
    shared,
    challenger: independent,
    costs,
    genuinelyNewVerifiedCount: usefulPages.length,
    recoveryApplied:
      recovery?.overlaysApplied?.demand === true ||
      recovery?.overlaysApplied?.buyer === true,
  });
  const baselineRubric = scorePrepArm({
    shared: baseline.sharedEligibility,
    challenger: {
      ...baseline.challenger,
      after: baseline.challenger,
      contact: baseline.challenger.contactRelevance,
    },
    costs: baseline.costs,
    genuinelyNewVerifiedCount: 0,
    recoveryApplied: false,
  });

  const demandAssessment = {
    confirmedHotelDemand: housingPages.some(
      (p) => /hotel block|room block|official hotel|host hotel|headquarters hotel/i.test(p.excerpt),
    ),
    reliableInferredDemand:
      independent.after?.reliablyInferredDemand === true ||
      usefulPages.some((p) => p.lodging.meetingsLanguage),
    speculativeDemand: independent.after?.speculative === true,
    alreadyBookedElsewhere: competitorPages.length > 0,
    competitorEvidence: competitorPages.map((p) => ({
      url: p.url,
      signals: p.competitorVenueSignals,
      excerpt: p.excerpt,
    })),
  };

  return {
    opportunityId: caseCfg.opportunityId,
    organization: text(working.organizationName || baselineOpp.organizationName),
    hotelId: caseCfg.hotelId,
    hotelName: caseCfg.hotelName,
    durationMs: Date.now() - t0,
    billing,
    baseline: {
      sharedEligibility: baseline.sharedEligibility,
      challenger: baseline.challenger,
      rubric: baselineRubric,
      thesis: text(
        baselineOpp.hotelOpportunityThesis ||
          baselineOpp.thesis ||
          baselineOpp.proposedHotelUse ||
          "",
      ),
      contact: baselineOpp.contact || null,
      frozenSignals: {
        eventActivityEvidence: baselineOpp.eventActivityEvidence || null,
        venueSourcingStatus: baselineOpp.venueSourcingStatus || null,
        roomDemandStatus: baselineOpp.roomDemandStatus || null,
        qualification: baselineOpp.opportunityQualification || null,
        qualificationFailureReason: baselineOpp.qualificationFailureReason || null,
        eventLocationSummary: baselineOpp.eventLocationSummary || null,
        timing: baselineOpp.eventStartDate || baselineOpp.eventDateDisplay || baselineOpp.timing || null,
      },
    },
    deepen: {
      retrievals,
      actualPaidUsd,
      failures,
      pagesFetched: pages.length,
      usefulPages: usefulPages.map((p) => ({
        url: p.url,
        retrievedAt: p.retrievedAt,
        demandClass: p.demandClass,
        emails: p.extraction.emails,
        housing: p.extraction.hasHousingLanguage || p.lodging.housingLanguage,
        stakeholders: p.stakeholders,
        competitorVenueSignals: p.competitorVenueSignals,
        excerpt: p.excerpt,
      })),
      namedContacts,
      bestContact,
      demandAssessment,
      allPageUrls: pages.map((p) => p.url),
    },
    after: {
      sharedEligibility: {
        status: shared.status,
        salesReady: shared.salesReady === true,
        gaps: shared.gaps || [],
        challengerOverride: shared.challengerOverride || null,
      },
      supervisor: {
        blockers: (supervisor.blockers || []).map((b) => b.key || b),
        publishable: supervisor.publishable === true,
        hardStopReason: supervisor.hardStop?.reason || null,
      },
      independentChallenger: independent,
      rubric,
      thesis: text(
        working.hotelOpportunityThesis ||
          working.hotelDemandThesis ||
          working.proposedHotelUse ||
          "",
      ),
      contact: working.contact || null,
    },
    comparison: {
      eligibilityBefore: baseline.sharedEligibility.status,
      eligibilityAfter: shared.status,
      deltaRubric: rubric.total - baselineRubric.total,
      newUsefulPages: usefulPages.length,
      newNamedContacts: namedContacts.length,
      newPublicEmails: namedContacts.filter((c) => c.email).length,
      commercialDemandImproved: demandAssessment.confirmedHotelDemand,
      actionableContactImproved: Boolean(
        namedContacts.find((c) => c.email) ||
          (namedContacts.find((c) => c.contactKind === "direct_or_probable_buyer") &&
            !(baselineOpp.contact?.name)),
      ),
      actualPaidUsd,
    },
  };
}

export async function runPhase2bDeepen(opts = {}) {
  const outDir =
    opts.outDir ||
    path.join(ROOT, "reports", "gdi-20opp-phase2b-official-domain-deepen-v1");
  fs.mkdirSync(outDir, { recursive: true });

  const caseResults = [];
  for (const cfg of PHASE2B_CASES) {
    caseResults.push(await deepenOfficialDomainCase(cfg, opts));
  }

  const totals = {
    cases: caseResults.length,
    actualPaidUsdTotal: caseResults.reduce((s, c) => s + c.comparison.actualPaidUsd, 0),
    totalRetrievals: caseResults.reduce((s, c) => s + c.deepen.retrievals, 0),
    totalUsefulPages: caseResults.reduce((s, c) => s + c.comparison.newUsefulPages, 0),
    totalNamedContacts: caseResults.reduce((s, c) => s + c.comparison.newNamedContacts, 0),
    totalPublicEmails: caseResults.reduce((s, c) => s + c.comparison.newPublicEmails, 0),
    demandImprovedCount: caseResults.filter((c) => c.comparison.commercialDemandImproved)
      .length,
    contactImprovedCount: caseResults.filter((c) => c.comparison.actionableContactImproved)
      .length,
    eligibilityUpgrades: caseResults.filter(
      (c) =>
        c.comparison.eligibilityBefore !== "SALES_READY" &&
        c.comparison.eligibilityAfter === "SALES_READY",
    ).length,
  };

  const recommendation = decidePhase2b(totals, caseResults);

  const report = {
    schema: PHASE2B_ID,
    version: PHASE2B_VERSION,
    executedAt: new Date().toISOString(),
    authorization: {
      mode: "live_public_official_domain_only",
      maxPaidUsd: MAX_PAID_USD,
      actualPaidUsdTotal: totals.actualPaidUsdTotal,
      deniedProviders: [...DENIED_METERED_PROVIDERS],
      productionWrites: false,
      airtableWrites: false,
      customerPublication: false,
      autoSalesReadyPromotion: false,
    },
    totals,
    recommendation,
    caseResults,
  };

  fs.writeFileSync(path.join(outDir, "RESULTS.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(path.join(outDir, "PHASE2B_REPORT.md"), renderPhase2bMarkdown(report));
  return { report, outDir };
}

function decidePhase2b(totals, caseResults) {
  if (totals.actualPaidUsdTotal > 0) {
    return {
      decision: "STOP",
      rationale: "Paid spend detected — abort further deepen",
    };
  }
  if (totals.eligibilityUpgrades > 0) {
    return {
      decision: "PROCEED",
      rationale: "Eligibility upgraded with challenger-safe public evidence",
      bottleneck: null,
    };
  }
  if (totals.demandImprovedCount > 0 || totals.contactImprovedCount > 0) {
    return {
      decision: "ITERATE",
      rationale:
        "Official-domain deepen produced some commercial demand or contact signal, but not enough for sales-ready",
      bottleneck: identifyBottleneck(caseResults),
      capabilityNeeded: null,
    };
  }
  // More same-domain URLs without demand/contact lift is not productive.
  return {
    decision: "STOP",
    rationale:
      "Additional official-domain research did not materially improve commercial demand confidence or actionable verified named contacts. Further URL expansion on the same allowlists is unproductive.",
    bottleneck: identifyBottleneck(caseResults),
    capabilityNeeded:
      "JS-rendered board/directory extraction that pairs visible name+title with on-page mailto (no fabricated emails); plus lodging/housing publication signals when absent from conference pages. Do not treat unpublished StarChapter preview shells or thin WP chrome as commercial contact evidence.",
  };
}

function identifyBottleneck(caseResults) {
  const notes = [];
  for (const c of caseResults) {
    if (c.deepen.demandAssessment.alreadyBookedElsewhere) {
      notes.push(`${c.opportunityId}: competitor/already-booked signals present`);
    }
    if (c.comparison.newUsefulPages === 0) {
      notes.push(`${c.opportunityId}: no useful new pages within allowlist`);
    } else if (c.comparison.newNamedContacts === 0) {
      notes.push(`${c.opportunityId}: useful pages but no extractable named commercial contacts`);
    } else if (c.comparison.newPublicEmails === 0) {
      notes.push(`${c.opportunityId}: named people found without public emails`);
    }
    if (
      !c.deepen.demandAssessment.confirmedHotelDemand &&
      c.baseline.frozenSignals.qualificationFailureReason === "TOO_EARLY"
    ) {
      notes.push(`${c.opportunityId}: timing too early / lodging not published`);
    }
  }
  return notes.join("; ") || "source_discovery_exhausted_on_allowlisted_domains";
}

function renderPhase2bMarkdown(report) {
  return `# GDI Phase 2B — Official-Domain Deepen

**Decision:** ${report.recommendation.decision}  
**Actual paid spend:** $${report.totals.actualPaidUsdTotal}  
**Bottleneck:** ${report.recommendation.bottleneck || "n/a"}

${report.recommendation.rationale}

## Totals
| Metric | Value |
|--------|-------|
| Retrievals | ${report.totals.totalRetrievals} |
| Useful pages | ${report.totals.totalUsefulPages} |
| Named contacts | ${report.totals.totalNamedContacts} |
| Public emails | ${report.totals.totalPublicEmails} |
| Demand improved | ${report.totals.demandImprovedCount} |
| Contact improved | ${report.totals.contactImprovedCount} |
| Eligibility upgrades | ${report.totals.eligibilityUpgrades} |

## Cases
${report.caseResults
  .map(
    (c) => `### ${c.opportunityId} — ${c.organization} @ ${c.hotelName}
- Eligibility: ${c.comparison.eligibilityBefore} → ${c.comparison.eligibilityAfter}
- Useful pages: ${c.comparison.newUsefulPages}
- Named contacts: ${c.comparison.newNamedContacts} (public emails: ${c.comparison.newPublicEmails})
- Confirmed hotel demand: ${c.deepen.demandAssessment.confirmedHotelDemand}
- Already booked / competitor signals: ${c.deepen.demandAssessment.alreadyBookedElsewhere}
- Paid: $${c.comparison.actualPaidUsd}
- Best contact: ${c.deepen.bestContact ? `${c.deepen.bestContact.name} / ${c.deepen.bestContact.title} (${c.deepen.bestContact.confidence})` : "none"}
`,
  )
  .join("\n")}
`;
}
