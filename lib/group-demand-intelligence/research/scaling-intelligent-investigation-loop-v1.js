/**
 * Scaling Intelligent Investigation Loop V1
 *
 * Adaptive research continuation for Full E2E: assess gaps → select next
 * strategy → execute (free public / saved evidence) → update → reassess.
 *
 * Does NOT invent buyers or demand. Does NOT call Surfe/PDL/Parallel.
 * Optional paid Exa is authorization-gated via paidSearch({ query }) from the
 * hotel runner when -AuthorizePaidResearch + remaining ledger budget exist.
 *
 * Closing the gap between offline remediation overlays and the normal
 * Invoke-GdiScalingCompanies.ps1 Full E2E path.
 */

import {
  evaluateScalingPublicationQualification,
  SCALING_PUBLICATION_STATUS,
} from "../scaling-publication-qualification-v1.js";
import {
  selectNextBuyerResearchStrategy,
  BUYER_RESEARCH_STRATEGY_SEQUENCE,
  SCALING_BUYER_RESEARCH_STRATEGIES_V1,
} from "../scaling-buyer-research-strategies-v1.js";
import { runPublicBuyerIntelligencePass } from "./commercial-depth-buyer-intelligence-v1.js";
import {
  extractPublicEvidenceFromText,
  selectPreferredPublicEmail,
} from "./direct-public-executor-v1.js";
import { fetchOfficialPage } from "../official-page-fetch.js";
import {
  resolveOfficialContactCandidateUrls,
  inferLocalConnectionForHotel,
  inferHotelUseForHotel,
} from "./scaling-companies-discovery-handoff-v1.js";
import { applyScalingSalesPresentation } from "../scaling-companies-sales-presentation-v1.js";
import { applyScalingTwoContactStandard } from "../scaling-two-contact-standard-v1.js";
import {
  attachSecondaryNamedContact,
  looksLikePersonName,
} from "../secondary-named-contact-v1.js";
import { applyScalingActivityChronology } from "../scaling-activity-chronology-v1.js";
import {
  applyScalingCommercialIntelligenceBrief,
  buildHotelSpecificFitAssessment,
} from "../scaling-commercial-intelligence-brief-v1.js";
import { attachGdi2ShadowEvaluation } from "../gdi2-shadow-publication-evaluation-v1.js";
import { planBuyerRecovery } from "../buyer-recovery-planner-v1.js";
import { classifyResearchContinuationDisposition } from "../research-continuation-disposition-v1.js";
import {
  mayStopForPublicationQualification,
  applyCommercialResearchCompletion,
  RESEARCH_COMPLETION_STATE,
} from "./commercial-research-completion-contract-v1.js";

export const SCALING_INTELLIGENT_INVESTIGATION_LOOP_V1 =
  "gdi-scaling-intelligent-investigation-loop-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hostnameOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "");
  } catch {
    return "";
  }
}

const NEWS_HOST =
  /commercialobserver|prnewswire|selectflorida|therealdeal|sfbwmag|miamiherald|cbs12|citybiz|floridatrend|palmbeachpost|techhubsouthflorida|businesswire|globenewswire|linkedin|facebook|twitter/i;

/**
 * Derive organization base URL from opportunity sources / website fields.
 * Hotel-generic — no hardcoded company map required for continuation.
 */
export function resolveOrganizationBaseUrls(opportunity = {}) {
  const urls = [];
  const add = (u) => {
    const t = text(u);
    if (!t || !/^https?:\/\//i.test(t)) return;
    try {
      const parsed = new URL(t);
      const host = parsed.hostname.replace(/^www\./i, "");
      if (NEWS_HOST.test(host)) return;
      const root = `${parsed.protocol}//${parsed.hostname}`;
      if (!urls.includes(root)) urls.push(root);
    } catch {
      /* ignore */
    }
  };
  add(opportunity.organizationWebsite || opportunity.website || opportunity.companyWebsite);
  add(opportunity.contactUrl || opportunity.organizationContactUrl);
  for (const s of opportunity.sources || []) {
    add(s?.url || s);
  }
  for (const u of opportunity.evidenceUrls || []) add(u);
  if (opportunity.sourceUrl) add(opportunity.sourceUrl);
  return urls.slice(0, 4);
}

/** Strategy-specific path suffixes beyond the shared contact/leadership set. */
const STRATEGY_PATH_SUFFIXES = Object.freeze({
  OFFICIAL_LEADERSHIP_PAGES: [
    "/leadership",
    "/team",
    "/our-team",
    "/people",
    "/about",
    "/about-us",
    "/who-we-are",
    "/about/leadership",
  ],
  MEETINGS_INCENTIVES_SALES_OPS: [
    "/about",
    "/team",
    "/leadership",
    "/sales",
    "/careers",
    "/news",
    "/contact",
  ],
  WORKPLACE_FACILITIES_REAL_ESTATE: [
    "/about",
    "/about-us",
    "/careers",
    "/company",
    "/contact",
  ],
  PEOPLE_HR_TRAINING: [
    "/people",
    "/careers",
    "/about",
    "/about-us",
    "/team",
    "/contact",
  ],
  REGIONAL_OPS_EXPANSION: [
    "/about",
    "/locations",
    "/offices",
    "/leadership",
    "/team",
    "/news",
  ],
  PROCUREMENT_ADMIN_EA: ["/contact", "/contact-us", "/about", "/team"],
  PRESS_NAMED_THEN_VERIFY_AFFILIATION: ["/news", "/press", "/about", "/leadership"],
  PUBLIC_PROFESSIONAL_PROFILES: ["/team", "/people", "/leadership", "/about"],
  ORG_ROUTE_ASK_FOR_NAMED: ["/contact", "/contact-us", "/connect"],
});

/**
 * Build free-public URLs for the selected buyer strategy.
 */
export function buildBuyerStrategyCandidateUrls(opportunity = {}, strategy = {}) {
  const urls = [];
  const add = (u) => {
    const t = text(u);
    if (t && /^https?:\/\//i.test(t) && !urls.includes(t)) urls.push(t);
  };

  const bases = resolveOrganizationBaseUrls(opportunity);
  for (const base of bases) {
    const shared = resolveOfficialContactCandidateUrls(
      opportunity.organizationName,
      base,
    );
    for (const u of shared) add(u);
    const suffixes = STRATEGY_PATH_SUFFIXES[strategy.id] || [];
    for (const suf of suffixes) {
      add(`${base.replace(/\/$/, "")}${suf}`);
    }
  }

  // Always include known source URLs so offline/saved-evidence can re-mine them
  for (const s of opportunity.sources || []) add(s?.url || s);
  add(opportunity.sourceUrl);

  return urls.slice(0, 16);
}

async function defaultFetchPage(url, { allowRender = false } = {}) {
  if (!url) return { text: "", provider: null, url: null };
  try {
    const page = await fetchOfficialPage(url, {
      allowRender: false,
      timeoutMs: 12000,
    });
    let blob = page?.text || page?.searchableText || page?.bodyText || "";
    if ((!blob || blob.length < 80) && allowRender) {
      const rendered = await fetchOfficialPage(url, {
        allowRender: true,
        timeoutMs: 20000,
      });
      blob = rendered?.text || rendered?.searchableText || rendered?.bodyText || "";
    }
    if (blob && blob.length >= 80) {
      return {
        text: String(blob).slice(0, 14000),
        provider: "direct_public",
        url,
      };
    }
  } catch {
    /* blocked / offline */
  }
  return { text: "", provider: null, url };
}

/**
 * Mine already-held opportunity text as evidence pages (no network).
 */
export function collectSavedEvidencePages(opportunity = {}) {
  const pages = [];
  const push = (url, blob) => {
    const t = text(blob);
    if (t.length < 40) return;
    pages.push({
      url: url || opportunity.sourceUrl || null,
      statement: t.slice(0, 12000),
      text: t.slice(0, 12000),
      sourceFamily: "SAVED_OPPORTUNITY_EVIDENCE",
    });
  };
  push(
    opportunity.sourceUrl,
    [
      opportunity.growthSourceBodySnippet,
      opportunity.datedGrowthEvidence,
      opportunity.whatIsHappening,
      opportunity.localConnection,
      opportunity.companyBackground,
      opportunity.activityHeadline,
    ]
      .filter(Boolean)
      .join("\n"),
  );
  for (const s of opportunity.sources || []) {
    push(s?.url || null, s?.excerpt || s?.snippet || s?.quote || s?.statement || "");
  }
  for (const e of opportunity.evidence || []) {
    push(e?.url || null, e?.statement || e?.quote || e?.excerpt || e?.text || "");
  }
  return pages;
}

function mergeNamedStakeholders(existing = [], incoming = []) {
  const map = new Map();
  for (const s of [...(existing || []), ...(incoming || [])]) {
    const name = text(s?.name);
    if (!name) continue;
    const key = name.toLowerCase();
    const prev = map.get(key) || {};
    map.set(key, {
      ...prev,
      ...s,
      name,
      title: text(s.title || prev.title) || null,
      email: s.email || prev.email || null,
      phone: s.phone || prev.phone || null,
      sourceUrl: s.sourceUrl || prev.sourceUrl || null,
      whyRelevant: s.whyRelevant || s.responsibilityRationale || prev.whyRelevant || null,
    });
  }
  return [...map.values()];
}

function stripInvalidAttribution(opportunity = {}) {
  const qual = evaluateScalingPublicationQualification(opportunity);
  if (!qual.attribution?.invalid) return { opportunity, changed: false, note: null };
  const reasons = qual.attribution.reasons || [];
  let next = { ...opportunity };
  let note = "Attribution correction applied";
  // Detach primary email when media/landlord mismatch flagged
  if (
    reasons.some((r) => /email|employer|media|landlord|airport/i.test(String(r)))
  ) {
    if (next.primaryContact?.email && next.contactEmail === next.primaryContact.email) {
      next = {
        ...next,
        contactEmail: next.organizationContactEmail || null,
        primaryContact: {
          ...next.primaryContact,
          email: null,
          emailDetachedReason: "INVALID_BUYER_CONTACT_ATTRIBUTION",
        },
      };
      note = "Detached mismatched primary email; preserved named person for re-route";
    }
  }
  return { opportunity: next, changed: true, note };
}

/**
 * Select the highest-value next commercial research strategy from current gaps.
 */
export function selectNextInvestigationStrategy(input = {}) {
  const gaps = Array.isArray(input.gaps) ? input.gaps : [];
  const attempts = Array.isArray(input.attempts) ? input.attempts : [];
  const tried = new Set(
    attempts.map((a) => String(a?.strategyId || "").trim()).filter(Boolean),
  );

  if (gaps.includes("INVALID_BUYER_CONTACT_ATTRIBUTION")) {
    const id = "CORRECT_EMPLOYER_EMAIL_ATTRIBUTION";
    if (!tried.has(id)) {
      return {
        strategyId: id,
        kind: "ATTRIBUTION",
        label: "Correct employer/email ownership before deeper buyer search",
        exhausted: false,
      };
    }
  }

  if (gaps.includes("VERIFIED_COMPANY_ACTIVITY") && !tried.has("ACTIVITY_CHRONOLOGY_EXTRACTION")) {
    return {
      strategyId: "ACTIVITY_CHRONOLOGY_EXTRACTION",
      kind: "CHRONOLOGY",
      label: "Extract original announcement dates and continuing presence signals",
      exhausted: false,
    };
  }

  if (
    gaps.includes("REALISTIC_HOTEL_DEMAND_THESIS") &&
    !tried.has("COMPANY_DEMAND_MECHANISM")
  ) {
    return {
      strategyId: "COMPANY_DEMAND_MECHANISM",
      kind: "DEMAND",
      label: "Develop guestroom / catering demand thesis from company activity",
      exhausted: false,
    };
  }

  if (
    (gaps.includes("ONE_NAMED_RELEVANT_BUYER") ||
      gaps.includes("USABLE_CONTACT_ROUTE")) &&
    !tried.has("EXISTING_EVIDENCE_BUYER_RECOVERY")
  ) {
    return {
      strategyId: "EXISTING_EVIDENCE_BUYER_RECOVERY",
      kind: "BUYER_SAVED",
      label: "Re-mine saved sources and namedStakeholders before new fetches",
      exhausted: false,
    };
  }

  if (
    gaps.includes("ONE_NAMED_RELEVANT_BUYER") ||
    gaps.includes("USABLE_CONTACT_ROUTE")
  ) {
    const buyerPick = selectNextBuyerResearchStrategy({
      gaps,
      attempts,
      attributionInvalid: gaps.includes("INVALID_BUYER_CONTACT_ATTRIBUTION"),
    });
    if (buyerPick.nextStrategy) {
      return {
        strategyId: buyerPick.nextStrategy.id,
        kind: "BUYER_FETCH",
        label: buyerPick.nextStrategy.label,
        queryHints: buyerPick.nextStrategy.queryHints,
        caution: buyerPick.nextStrategy.caution || null,
        exhausted: false,
        remainingBuyerStrategies: buyerPick.remaining.map((s) => s.id),
      };
    }
  }

  if (!tried.has("HOTEL_SPECIFIC_FIT") && gaps.length) {
    return {
      strategyId: "HOTEL_SPECIFIC_FIT",
      kind: "HOTEL_FIT",
      label: "Hotel location / facilities / transport fit vs company activity",
      exhausted: false,
    };
  }

  if (!tried.has("GOLD_STANDARD_BRIEF_REGENERATION") && gaps.length === 0) {
    return {
      strategyId: "GOLD_STANDARD_BRIEF_REGENERATION",
      kind: "BRIEF",
      label: "Regenerate San Diego gold-standard commercial brief from structured research",
      exhausted: false,
    };
  }

  return {
    strategyId: null,
    kind: null,
    label: null,
    exhausted: true,
    note:
      "Productive free/saved strategies exhausted — preserve opportunity; stage paid research proposal if still commercially promising.",
  };
}

function applyBuyerPassToOpportunity(opportunity, buyerPass, hotel) {
  let next = {
    ...opportunity,
    namedStakeholders: mergeNamedStakeholders(
      opportunity.namedStakeholders,
      buyerPass.namedStakeholders || [],
    ),
    whoResearchStatus: buyerPass.whoResearchStatus,
    whoResearchSucceeded: buyerPass.whoResearchSucceeded === true,
  };

  const topBuyer =
    (buyerPass.roleRelevantStakeholders || [])[0] ||
    (buyerPass.namedStakeholders || [])[0] ||
    null;

  // Prefer the named person's email over generic org mailboxes.
  const preferred =
    topBuyer?.email ||
    selectPreferredPublicEmail(
      (buyerPass.namedStakeholders || [])
        .map((s) => s.email)
        .filter(Boolean),
    ) ||
    buyerPass.contactOwnership?.route ||
    null;

  if (topBuyer?.name && !text(next.contactName || next.primaryContact?.name)) {
    next.contactName = topBuyer.name;
    next.contactTitle = topBuyer.title || null;
    next.primaryContact = {
      ...(next.primaryContact || {}),
      name: topBuyer.name,
      title: topBuyer.title || null,
      email: topBuyer.email || next.primaryContact?.email || null,
      phone: topBuyer.phone || next.primaryContact?.phone || null,
      sourceUrl: topBuyer.sourceUrl || null,
      whyRelevant:
        topBuyer.whyRelevant ||
        topBuyer.responsibilityRationale ||
        "Named from official/public company materials during investigation loop",
    };
  }

  if (preferred && !next.contactEmail) {
    next.contactEmail = preferred;
    if (next.primaryContact) {
      next.primaryContact = { ...next.primaryContact, email: preferred };
    }
  }

  if (
    !next.organizationContactEmail &&
    buyerPass.contactOwnership?.route &&
    /info@|contact@|hello@|newbiz@/i.test(buyerPass.contactOwnership.route)
  ) {
    next.organizationContactEmail = buyerPass.contactOwnership.route;
  }

  next = attachSecondaryNamedContact(next, { allowSavedEvidence: true });
  next = applyScalingTwoContactStandard(
    applyScalingSalesPresentation(next, {
      hotelDisplayName: hotel?.displayName || hotel?.name || null,
      hotel,
      applyCommercialIntelligenceBrief: true,
    }),
    { force: true },
  );
  return next;
}

function buildPaidBuyerSearchQueries(opportunity = {}, hotel = {}, strategyId = "") {
  const org = text(opportunity.organizationName);
  const city = text(hotel.city || opportunity.city || "");
  if (!org) return [];
  const base = [
    `"${org}" ${city} (director OR manager OR "vice president" OR VP OR jefe OR director) (travel OR meetings OR facilities OR workplace OR events OR procurement OR "recursos humanos" OR capacitación)`.trim(),
    `"${org}" (leadership OR team OR "about us" OR equipo OR liderazgo) ${city}`.trim(),
  ];
  if (
    /MEETINGS_INCENTIVES|PRESS_NAMED|PUBLIC_PROFESSIONAL|REGIONAL_OPS|PEOPLE_HR/i.test(
      strategyId,
    )
  ) {
    base.push(
      `"${org}" ("sales kickoff" OR "incentive travel" OR retreat OR incentivo OR "convención de ventas" OR "viaje de incentivo" OR "employee recognition") (director OR manager OR "sales operations" OR "eventos corporativos")`.trim(),
    );
  }
  return base.filter(Boolean).slice(0, 2);
}

async function executeStrategy({
  strategy,
  opportunity,
  hotel,
  fetchPage,
  allowFreeFetch,
  pageFetchBudget,
  paidSearch = null,
}) {
  const attemptedUrls = [];
  const found = [];
  const rejected = [];
  let pagesFetched = 0;
  let costUsd = 0;
  let next = { ...opportunity };
  let informationFound = false;

  if (strategy.kind === "ATTRIBUTION") {
    const fixed = stripInvalidAttribution(next);
    next = fixed.opportunity;
    informationFound = fixed.changed;
    return {
      opportunity: next,
      informationFound,
      informationRejected: [],
      urlsAttempted: [],
      pagesFetched: 0,
      note: fixed.note,
    };
  }

  if (strategy.kind === "CHRONOLOGY") {
    next = applyScalingActivityChronology(next, {
      hotelCity: hotel?.city || null,
    });
    // Try to strengthen local connection from saved snippets
    if (!next.localConnection) {
      const inferred = inferLocalConnectionForHotel(
        {
          title: next.activityHeadline || next.organizationName,
          excerpt: next.growthSourceBodySnippet || next.datedGrowthEvidence || "",
          url: next.sourceUrl,
        },
        { organizationName: next.organizationName, programName: next.activityHeadline },
        hotel,
      );
      if (inferred) {
        next.localConnection = inferred;
        informationFound = true;
        found.push("localConnection_inferred_from_saved_evidence");
      }
    }
    next = applyScalingCommercialIntelligenceBrief(next, hotel, {});
    informationFound = informationFound || Boolean(next.activityChronology?.originalEventDate);
    return {
      opportunity: next,
      informationFound,
      informationRejected: rejected,
      urlsAttempted: attemptedUrls,
      pagesFetched: 0,
      note: "Chronology + continuity reassessment from held evidence",
    };
  }

  if (strategy.kind === "DEMAND") {
    if (!next.proposedHotelUse && !next.hotelOpportunityThesis) {
      const use = inferHotelUseForHotel(
        {
          title: next.activityHeadline || next.organizationName,
          excerpt:
            next.growthSourceBodySnippet ||
            next.whatIsHappening ||
            next.localConnection ||
            "",
          url: next.sourceUrl,
        },
        { programName: next.activityHeadline },
        hotel,
      );
      if (use) {
        next.proposedHotelUse = use;
        next.hotelOpportunityThesis = use;
        informationFound = true;
        found.push("hotel_demand_thesis_inferred");
      } else {
        rejected.push("insufficient_public_mechanism_language_for_demand_thesis");
      }
    }
    if (!next.localConnection) {
      const inferred = inferLocalConnectionForHotel(
        {
          title: next.activityHeadline || next.organizationName,
          excerpt: next.growthSourceBodySnippet || next.datedGrowthEvidence || "",
          url: next.sourceUrl,
        },
        { organizationName: next.organizationName },
        hotel,
      );
      if (inferred) {
        next.localConnection = inferred;
        informationFound = true;
      }
    }
    next = applyScalingCommercialIntelligenceBrief(next, hotel, {});
    return {
      opportunity: next,
      informationFound,
      informationRejected: rejected,
      urlsAttempted: attemptedUrls,
      pagesFetched: 0,
      note: "Demand mechanism reassessment — no invented RFP/volume",
    };
  }

  if (strategy.kind === "HOTEL_FIT") {
    const fit = buildHotelSpecificFitAssessment(next, hotel);
    if (fit?.whyThisHotel) {
      next.whyThisHotel = fit.whyThisHotel;
      next.hotelFitRationale = fit.whyThisHotel;
      informationFound = true;
      found.push("hotel_fit_assessment");
    }
    next = applyScalingCommercialIntelligenceBrief(next, hotel, {});
    return {
      opportunity: next,
      informationFound,
      informationRejected: rejected,
      urlsAttempted: attemptedUrls,
      pagesFetched: 0,
      note: "Hotel-specific fit from inventory profile + company locality",
    };
  }

  if (strategy.kind === "BRIEF") {
    next = applyScalingTwoContactStandard(
      applyScalingSalesPresentation(next, {
        hotelDisplayName: hotel?.displayName || hotel?.name || null,
        hotel,
        applyCommercialIntelligenceBrief: true,
      }),
      { force: true },
    );
    informationFound = Boolean(next.salesBrief || next.commercialIntelligenceBrief);
    return {
      opportunity: next,
      informationFound,
      informationRejected: rejected,
      urlsAttempted: attemptedUrls,
      pagesFetched: 0,
      note: "Gold-standard brief regenerated from structured research",
    };
  }

  // BUYER_SAVED or BUYER_FETCH
  // Promote already-held namedStakeholders before/without network (remediation pattern).
  if (
    strategy.kind === "BUYER_SAVED" &&
    Array.isArray(next.namedStakeholders) &&
    next.namedStakeholders.length > 0
  ) {
    const held = next.namedStakeholders.find((s) =>
      looksLikePersonName(s?.name),
    );
    if (held?.name && !text(next.contactName || next.primaryContact?.name)) {
      next = {
        ...next,
        contactName: held.name,
        contactTitle: held.title || null,
        contactEmail: held.email || next.contactEmail || null,
        contactPhone: held.phone || next.contactPhone || null,
        primaryContact: {
          ...(next.primaryContact || {}),
          name: held.name,
          title: held.title || null,
          email: held.email || next.primaryContact?.email || null,
          phone: held.phone || next.primaryContact?.phone || null,
          sourceUrl: held.sourceUrl || null,
          whyRelevant:
            held.whyRelevant ||
            held.responsibilityRationale ||
            "Promoted from held namedStakeholders during investigation loop",
        },
      };
      informationFound = true;
      found.push(`promoted_held_stakeholder:${held.name}`);
    }
    next = attachSecondaryNamedContact(next, { allowSavedEvidence: true });
    next = applyScalingTwoContactStandard(
      applyScalingSalesPresentation(next, {
        hotelDisplayName: hotel?.displayName || hotel?.name || null,
        hotel,
        applyCommercialIntelligenceBrief: true,
      }),
      { force: true },
    );
    const afterPromote = evaluateScalingPublicationQualification(next);
    if (afterPromote.publicationQualified || informationFound) {
      return {
        opportunity: next,
        informationFound: true,
        informationRejected: rejected,
        urlsAttempted: attemptedUrls,
        pagesFetched: 0,
        note: "Promoted held namedStakeholders + regenerated brief",
      };
    }
  }

  const evidencePages = collectSavedEvidencePages(next);
  let pageBudgetLeft = Math.max(0, Number(pageFetchBudget) || 0);

  if (strategy.kind === "BUYER_FETCH" && allowFreeFetch && pageBudgetLeft > 0) {
    // Paid Exa discovery (optional) — find leadership/buyer pages when official
    // path suffixes alone are insufficient (Mexico CDMX / Cancún buyer gaps).
    if (typeof paidSearch === "function") {
      for (const query of buildPaidBuyerSearchQueries(
        next,
        hotel,
        strategy.strategyId,
      )) {
        if (pageBudgetLeft <= 0) break;
        try {
          const exa = await paidSearch({ query, opportunity: next, strategy });
          costUsd += Number(exa?.costUsd || 0);
          for (const hit of exa?.hits || []) {
            const url = text(hit.url || hit.sourceUrl);
            if (!url || !/^https?:/i.test(url)) continue;
            if (NEWS_HOST.test(hostnameOf(url))) continue;
            if (attemptedUrls.includes(url)) continue;
            if (pageBudgetLeft <= 0) break;
            attemptedUrls.push(url);
            const page = await fetchPage(url, {
              allowRender: /team|leadership|people|about/i.test(url),
            });
            pagesFetched += 1;
            pageBudgetLeft -= 1;
            const body =
              text(page?.text) ||
              text(hit.excerpt || hit.text || hit.highlights?.join?.(" "));
            if (!body || body.length < 80) {
              rejected.push({ url, reason: "exa_hit_empty_or_blocked" });
              continue;
            }
            evidencePages.push({
              url,
              statement: body,
              text: body,
              sourceFamily: "PAID_EXA_THEN_FETCH",
              retrievedAt: new Date().toISOString(),
            });
            found.push(`exa_hit:${hostnameOf(url)}`);
          }
        } catch (err) {
          rejected.push({
            reason: "paid_search_failed",
            message: String(err?.message || err).slice(0, 160),
          });
        }
      }
    }

    const candidateUrls = buildBuyerStrategyCandidateUrls(next, {
      id: strategy.strategyId,
    });
    for (const url of candidateUrls) {
      if (pageBudgetLeft <= 0) break;
      if (evidencePages.some((p) => p.url === url && text(p.statement).length >= 80)) {
        continue;
      }
      attemptedUrls.push(url);
      const page = await fetchPage(url, {
        allowRender: /team|leadership|people/i.test(url),
      });
      pagesFetched += 1;
      pageBudgetLeft -= 1;
      if (!page.text || page.text.length < 80) {
        rejected.push({ url, reason: "empty_or_blocked_page" });
        continue;
      }
      evidencePages.push({
        url,
        statement: page.text,
        text: page.text,
        sourceFamily: "FREE_PUBLIC_FETCH",
        retrievedAt: new Date().toISOString(),
      });
      // Also absorb structured extraction into opportunity source corpus
      try {
        const extracted = extractPublicEvidenceFromText(page.text, url);
        if (extracted?.emails?.length || extracted?.phones?.length) {
          found.push(`contact_signals:${hostnameOf(url)}`);
        }
      } catch {
        /* ignore */
      }
    }
  }

  if (!evidencePages.length) {
    return {
      opportunity: next,
      informationFound: false,
      informationRejected: ["no_saved_or_fetched_evidence_pages"],
      urlsAttempted: attemptedUrls,
      pagesFetched,
      note: "No usable page text for buyer recovery",
    };
  }

  const buyerPass = runPublicBuyerIntelligencePass({
    evidence: evidencePages,
    organizationName: next.organizationName,
    existingContactEmail: next.contactEmail || next.organizationContactEmail,
    segmentId: "scaling_companies",
  });

  const beforeNames = (next.namedStakeholders || []).map((s) =>
    text(s.name).toLowerCase(),
  );
  next = applyBuyerPassToOpportunity(next, buyerPass, hotel);
  const afterNames = (next.namedStakeholders || []).map((s) =>
    text(s.name).toLowerCase(),
  );
  const newNames = afterNames.filter((n) => n && !beforeNames.includes(n));
  informationFound =
    newNames.length > 0 ||
    Boolean(next.contactName || next.primaryContact?.name) ||
    buyerPass.whoResearchSucceeded === true;

  if (newNames.length) found.push(...newNames.map((n) => `named:${n}`));
  if (!informationFound) {
    rejected.push(buyerPass.decisionMakerGap || "no_new_named_buyer_from_pages");
  }

  return {
    opportunity: next,
    informationFound,
    informationRejected: rejected,
    urlsAttempted: attemptedUrls,
    pagesFetched,
    costUsd,
    note: `Buyer pass ${buyerPass.whoResearchStatus}; stakeholders=${(buyerPass.namedStakeholders || []).length}`,
    buyerPassSummary: {
      whoResearchStatus: buyerPass.whoResearchStatus,
      namedCount: (buyerPass.namedStakeholders || []).length,
      roleRelevantCount: (buyerPass.roleRelevantStakeholders || []).length,
    },
  };
}

/**
 * Run the adaptive investigation loop on one opportunity.
 *
 * @returns {{ opportunity, qualification, attempts, disposition, metrics }}
 */
export async function runScalingIntelligentInvestigationLoop({
  opportunity = {},
  hotel = {},
  allowFreeFetch = true,
  maxIterations = 6,
  maxPageFetches = 8,
  fetchPage = defaultFetchPage,
  paidSearch = null,
} = {}) {
  let current = opportunity && typeof opportunity === "object" ? { ...opportunity } : {};
  const attempts = Array.isArray(current.buyerResearchAttempts)
    ? [...current.buyerResearchAttempts]
    : [];
  let pagesUsed = 0;
  let totalCostUsd = 0;
  const unresolvedQuestions = [];
  let stopReason = null;

  let qualification = evaluateScalingPublicationQualification(current);

  // Always regenerate brief once when already qualified but salesBrief missing
  if (
    qualification.publicationQualified &&
    !current.salesBrief &&
    !current.commercialIntelligenceBrief
  ) {
    current = applyScalingTwoContactStandard(
      applyScalingSalesPresentation(current, {
        hotelDisplayName: hotel?.displayName || hotel?.name || null,
        hotel,
        applyCommercialIntelligenceBrief: true,
      }),
      { force: true },
    );
    qualification = evaluateScalingPublicationQualification(current);
  }

  for (let i = 0; i < maxIterations; i += 1) {
    if (
      qualification.publicationQualified &&
      mayStopForPublicationQualification(current, hotel)
    ) {
      // One brief polish when publication + commercial gaps closed
      if (!current.salesBrief) {
        current = applyScalingTwoContactStandard(
          applyScalingSalesPresentation(current, {
            hotelDisplayName: hotel?.displayName || hotel?.name || null,
            hotel,
            applyCommercialIntelligenceBrief: true,
          }),
          { force: true },
        );
      }
      stopReason = "PUBLICATION_QUALIFIED";
      break;
    }
    if (
      qualification.publicationQualified &&
      !mayStopForPublicationQualification(current, hotel)
    ) {
      current = {
        ...current,
        publicationQualifiedStopDeferred: true,
        scalingPublicationQualified: true,
      };
    }

    const nextStrategy = selectNextInvestigationStrategy({
      gaps: qualification.gaps || [],
      attempts,
    });

    if (nextStrategy.exhausted || !nextStrategy.strategyId) {
      stopReason = "STRATEGIES_EXHAUSTED";
      unresolvedQuestions.push(...(qualification.gaps || []));
      break;
    }

    const pageBudget = Math.max(0, maxPageFetches - pagesUsed);
    const executed = await executeStrategy({
      strategy: nextStrategy,
      opportunity: current,
      hotel,
      fetchPage,
      allowFreeFetch: allowFreeFetch && pageBudget > 0,
      pageFetchBudget: pageBudget,
      paidSearch,
    });

    pagesUsed += executed.pagesFetched || 0;
    totalCostUsd += Number(executed.costUsd || 0);
    current = executed.opportunity;
    attempts.push({
      at: new Date().toISOString(),
      strategyId: nextStrategy.strategyId,
      kind: nextStrategy.kind,
      label: nextStrategy.label,
      informationFound: executed.informationFound === true,
      informationRejected: executed.informationRejected || [],
      urlsAttempted: executed.urlsAttempted || [],
      pagesFetched: executed.pagesFetched || 0,
      note: executed.note || null,
      costUsd: Number(executed.costUsd || 0),
      provider:
        Number(executed.costUsd || 0) > 0
          ? "paid_exa_plus_public"
          : "direct_public_or_saved",
    });

    qualification = evaluateScalingPublicationQualification(current);

    if (!executed.informationFound && nextStrategy.kind === "BUYER_FETCH") {
      // Continue to next buyer strategy — do not stop after one unsuccessful search
      continue;
    }
  }

  if (!stopReason) {
    stopReason = qualification.publicationQualified
      ? "PUBLICATION_QUALIFIED"
      : "ITERATION_CEILING";
    if (!qualification.publicationQualified) {
      unresolvedQuestions.push(...(qualification.gaps || []));
    }
  }

  // Final brief when we have enough structure even if not yet publication-qualified
  current = applyScalingTwoContactStandard(
    applyScalingSalesPresentation(current, {
      hotelDisplayName: hotel?.displayName || hotel?.name || null,
      hotel,
      applyCommercialIntelligenceBrief: true,
    }),
    { force: true },
  );
  qualification = evaluateScalingPublicationQualification(current);

  const nextViable = selectNextInvestigationStrategy({
    gaps: qualification.gaps || [],
    attempts,
  });

  current = {
    ...current,
    buyerResearchAttempts: attempts,
    investigationLoopVersion: SCALING_INTELLIGENT_INVESTIGATION_LOOP_V1,
    investigationLoopDisposition: stopReason,
    investigationLoopUnresolved: unresolvedQuestions,
    publicationQualified: qualification.publicationQualified === true,
    researchContinuation:
      qualification.researchContinuation === true ||
      (qualification.publicationQualified === true &&
        !mayStopForPublicationQualification(current, hotel)),
    preserveOpportunity: true,
  };

  current = applyCommercialResearchCompletion(current, hotel, {
    executionStopReason: stopReason,
    strategiesExhausted: stopReason === "STRATEGIES_EXHAUSTED",
    iterationCeiling: stopReason === "ITERATION_CEILING",
  });
  if (
    stopReason === "PUBLICATION_QUALIFIED" &&
    current.commercialResearchCompletionState ===
      RESEARCH_COMPLETION_STATE.RESEARCH_CONTINUATION
  ) {
    current.investigationLoopDisposition = "PUBLICATION_QUALIFIED_COMMERCIAL_CONTINUATION";
    current.researchContinuation = true;
  }

  const continuationShadow = classifyResearchContinuationDisposition({
    opportunity: current,
    hotel: hotel || {},
    strategyEmptyResults: attempts.some((a) => a.informationFound === false),
    authorizedBudgetExhausted: false,
    contactNotYetVerified: qualification.gaps?.includes("ONE_NAMED_RELEVANT_BUYER"),
    lodgingEvidenceIncomplete: qualification.gaps?.includes("REALISTIC_HOTEL_DEMAND_THESIS"),
  });
  const shadow = attachGdi2ShadowEvaluation(
    {
      opportunity: current,
      hotelFacingEligible: qualification.publicationQualified === true,
      researchContinuation: current.researchContinuation === true,
      scalingQualification: qualification,
    },
    {
      hotel,
      segmentFamily: "scaling_companies",
      existingSalesReady:
        current.commerciallyResearchComplete === true &&
        qualification.publicationQualified === true,
      existingResearchContinuation: current.researchContinuation === true,
    },
  );
  const recovery =
    current.commerciallyResearchComplete === true
      ? null
      : planBuyerRecovery(current, {
          hotel,
          attempts,
          paidResearchAuthorized: false,
        });

  return {
    version: SCALING_INTELLIGENT_INVESTIGATION_LOOP_V1,
    opportunity: current,
    qualification,
    attempts,
    disposition: current.investigationLoopDisposition || stopReason,
    nextViableStrategy: nextViable.exhausted ? null : nextViable,
    // Phase 2B shadow — does not change disposition / publicationQualified.
    gdi2ShadowEvaluationV1: shadow.gdi2ShadowEvaluationV1,
    gdi2ContinuationDispositionV1: continuationShadow,
    gdi2BuyerRecoveryPlanV1: recovery,
    commercialResearchCompletion: {
      state: current.commercialResearchCompletionState,
      acceptance: current.commercialDrawerAcceptance,
      gaps: current.commercialMaterialGaps,
    },
    metrics: {
      iterations: attempts.length,
      pagesFetched: pagesUsed,
      costUsd: Number(totalCostUsd.toFixed(6)),
      publicationQualified: qualification.publicationQualified === true,
      commerciallyComplete: current.commerciallyResearchComplete === true,
      gapsRemaining: qualification.gaps || [],
      namedBuyerPresent: qualification.gates?.oneNamedRelevantBuyer === true,
      salesBriefPresent: Boolean(
        current.salesBrief || current.commercialIntelligenceBrief,
      ),
    },
    note:
      current.investigationLoopDisposition === "PUBLICATION_QUALIFIED_COMMERCIAL_CONTINUATION"
        ? "Publication qualified but commercial gaps remain — continuation required."
        : stopReason === "PUBLICATION_QUALIFIED"
          ? "Investigation closed publication and commercial gaps without inventing contacts."
          : "Opportunity preserved for research continuation — not silent-withdrawn.",
  };
}

/**
 * Apply the loop across matured Full E2E rows (handoff + lifecycle).
 */
export async function runInvestigationLoopOnMaturedRows({
  maturedRows = [],
  hotel = {},
  allowFreeFetch = true,
  maxIterations = 6,
  maxPageFetches = 8,
  fetchPage = defaultFetchPage,
  paidSearch = null,
} = {}) {
  const results = [];
  const out = [];
  for (const row of maturedRows || []) {
    const opp = row?.opportunity || row?.opportunityPayload;
    if (!opp || typeof opp !== "object") {
      out.push(row);
      continue;
    }
    // Skip EXCLUDE / identity-failed rows
    if (
      /EXCLUDE|BETTER_FIT_ELSEWHERE/i.test(String(row.publicationAuditLabel || "")) ||
      (opp.customerVisible === false &&
        /EXCLUDE/i.test(String(opp.scalingCommercialDisposition || "")))
    ) {
      out.push(row);
      continue;
    }
    const loop = await runScalingIntelligentInvestigationLoop({
      opportunity: opp,
      hotel,
      allowFreeFetch,
      maxIterations,
      maxPageFetches,
      fetchPage,
      paidSearch,
    });
    results.push({
      id: loop.opportunity.id || null,
      organizationName: loop.opportunity.organizationName,
      disposition: loop.disposition,
      publicationQualified: loop.qualification.publicationQualified,
      gaps: loop.qualification.gaps,
      attempts: loop.attempts.map((a) => a.strategyId),
      metrics: loop.metrics,
    });
    const label = loop.qualification.publicationQualified
      ? "PUBLICATION_READY"
      : row.publicationAuditLabel || "RESEARCH_CONTINUATION";
    out.push({
      ...row,
      opportunity: loop.opportunity,
      opportunityPayload: loop.opportunity,
      publicationAuditLabel: label,
      hotelFacingEligible:
        loop.qualification.publicationQualified === true
          ? true
          : row.hotelFacingEligible,
      investigationLoop: {
        disposition: loop.disposition,
        gaps: loop.qualification.gaps,
        attemptCount: loop.attempts.length,
      },
    });
  }
  return {
    version: SCALING_INTELLIGENT_INVESTIGATION_LOOP_V1,
    maturedRows: out,
    results,
    buyerStrategiesCatalog: BUYER_RESEARCH_STRATEGY_SEQUENCE.map((s) => s.id),
    strategiesModule: SCALING_BUYER_RESEARCH_STRATEGIES_V1,
  };
}
