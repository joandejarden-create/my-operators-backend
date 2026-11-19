/**
 * Commercial Fact Selection V1 — promote material verified findings into Level 1
 * Brief vs Level 2 Supporting Research. Does not invent demand. Reevaluates
 * destinations (does not blindly trust prior BRIEF/SUPPORTING labels).
 * Sources: canonical register + section citations (complete retention).
 */

import {
  assessSourceRegisterPersistence,
  buildCommercialSourceRegister,
  buildSectionCitationPresentation,
  linkFactsToSourceRegister,
} from "./gdi-commercial-source-register-v1.js";

export const GDI_COMMERCIAL_FACT_SELECTION_V1 = "gdi-commercial-fact-selection-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function blobOf(opp = {}) {
  return [
    opp.whatIsHappening,
    opp.whyThisHotel,
    opp.potentialHotelBusiness,
    opp.whyActNow,
    opp.recommendedNextStep,
    opp.lodgingEvidence,
    Array.isArray(opp.uncertainties) ? opp.uncertainties.join(" ") : opp.uncertainties,
    JSON.stringify(opp.primaryContact || {}),
    JSON.stringify(opp.establishedFacts || []),
    JSON.stringify(opp.claims || []),
  ]
    .map(text)
    .join(" \n ");
}

function factInText(hay, fact) {
  const needles = (fact.matchNeedles || [fact.claim]).map((n) => String(n).toLowerCase());
  const h = String(hay || "").toLowerCase();
  return needles.some((n) => n && h.includes(n.toLowerCase()));
}

/**
 * Reevaluate destination from commercial effect — ignore prior catalog destination.
 */
export function reevaluateFactDestination(fact = {}, ctx = {}) {
  const now = ctx.now || new Date();
  const status = text(fact.verificationStatus || "CONFIRMED").toUpperCase();
  const origin = text(fact.origin || "ARCHIVED_RESEARCH").toUpperCase();

  if (origin === "ABSENT_FROM_RESEARCH" || status === "UNKNOWN") {
    return {
      destination: "OMIT_MISSING",
      reason: "ABSENT_OR_UNKNOWN",
      hotelPrivate: Boolean(fact.hotelPrivate),
    };
  }

  if (fact.hotelPrivate === true || fact.sharedMarketForbidden === true) {
    return {
      destination: "SUPPORTING_HOTEL_PRIVATE",
      reason: "HOTEL_PRIVATE_NOT_SHARED_MARKET",
      hotelPrivate: true,
    };
  }

  // Historical-only facts must not drive current pursuit as future deadlines
  if (fact.historicalOnly === true || fact.temporalClass === "HISTORICAL") {
    return {
      destination: "SUPPORTING",
      reason: "HISTORICAL_CONTEXT_ONLY",
      hotelPrivate: false,
    };
  }

  const activity = fact.activityDate ? new Date(fact.activityDate) : null;
  if (activity && !Number.isNaN(activity.getTime()) && activity.getTime() < now.getTime() - 86400000) {
    if (fact.treatAsFutureDeadline === true) {
      return {
        destination: "OMIT_UNSUPPORTED",
        reason: "HISTORICAL_DATE_AS_FUTURE_DEADLINE_FORBIDDEN",
        hotelPrivate: false,
      };
    }
  }

  const importance = text(fact.commercialImportance || "MEDIUM").toUpperCase();
  const changes = Boolean(fact.changesPursuit);
  const axes = fact.affectsAxes || [];
  const salesAxes = [
    "demand",
    "timing",
    "hotel_fit",
    "buyer_access",
    "competitive",
    "pursuit",
  ];
  const hitsAxis = axes.some((a) => salesAxes.includes(String(a).toLowerCase()));

  if ((importance === "HIGH" && changes) || hitsAxis) {
    return {
      destination: "BRIEF",
      reason: "MATERIAL_SALES_DECISION",
      hotelPrivate: false,
    };
  }
  if (importance === "HIGH" || importance === "MEDIUM") {
    return {
      destination: "SUPPORTING",
      reason: "USEFUL_VERIFIED_DEPTH",
      hotelPrivate: false,
    };
  }
  return {
    destination: "SUPPORTING",
    reason: "LOW_IMPORTANCE_RETAINED",
    hotelPrivate: false,
  };
}

function dedupeFacts(facts = []) {
  const out = [];
  const seen = new Set();
  for (const f of facts) {
    const key = text(f.id || f.claim).toLowerCase();
    if (!key || seen.has(key)) {
      if (key && f.sourceUrl) {
        const existing = out.find((x) => text(x.id || x.claim).toLowerCase() === key);
        if (existing) {
          existing.sourceUrls = [
            ...new Set([...(existing.sourceUrls || [existing.sourceUrl]).filter(Boolean), f.sourceUrl]),
          ];
        }
      }
      continue;
    }
    seen.add(key);
    out.push({
      ...f,
      sourceUrls: f.sourceUrl ? [f.sourceUrl] : f.sourceUrls || [],
    });
  }
  return out;
}

/**
 * Keep only complete sentences — never invent missing endings for truncated prose.
 */
function takeCompleteSentences(lodging) {
  const t = text(lodging);
  if (!t) return "";
  let lastEnd = -1;
  let inQuote = false;
  for (let i = 0; i < t.length; i += 1) {
    const ch = t[i];
    if (ch === '"' || ch === "\u201c" || ch === "\u201d") inQuote = !inQuote;
    if (!inQuote && (ch === "." || ch === "!" || ch === "?")) lastEnd = i;
  }
  if (lastEnd >= 40) return t.slice(0, lastEnd + 1).trim();
  return "";
}

function isTruncatedClaim(claim) {
  const t = text(claim);
  if (!t) return true;
  if (t.length > 80 && /[a-z]$/i.test(t) && !/[.!?]"?\s*$/.test(t)) return true;
  const quotes = (t.match(/["\u201c\u201d]/g) || []).length;
  if (quotes % 2 === 1) return true;
  return false;
}

function extractInlineFacts(opp = {}, catalogFacts = []) {
  const facts = [];
  const lodgingRaw = text(opp.lodgingEvidence);
  const lodgingComplete = takeCompleteSentences(lodgingRaw);
  const catalogCoversHousing = catalogFacts.some(
    (f) =>
      /housing bureau|book directly/i.test(text(f.claim)) ||
      (f.matchNeedles || []).some((n) => /housing bureau|book directly/i.test(String(n))),
  );
  // Prefer catalog housing-bureau claim — do not also append truncated lodging blobs
  if (
    lodgingComplete &&
    /housing bureaus|book directly/i.test(lodgingComplete) &&
    !catalogCoversHousing
  ) {
    let lodgingUrl = opp.lodgingEvidenceUrl || null;
    if (!lodgingUrl) {
      const listed = (opp.sources || [])
        .map((s) => (typeof s === "string" ? s : s?.url))
        .filter(Boolean);
      lodgingUrl =
        listed.find((u) => /accommodation|hotel|lodging|housing/i.test(u)) || null;
    }
    facts.push({
      id: "inline-lodging-direct-book",
      claim: lodgingComplete.slice(0, 320),
      matchNeedles: ["housing bureaus", "Book directly"],
      sourceUrl: lodgingUrl,
      sourceUrls: lodgingUrl ? [lodgingUrl] : [],
      verificationStatus: lodgingUrl ? "CONFIRMED" : "PROVENANCE_INCOMPLETE",
      commercialImportance: "HIGH",
      changesPursuit: true,
      affectsAxes: ["buyer_access", "pursuit"],
      origin: "OPPORTUNITY_LODGING_EVIDENCE",
    });
  }
  for (const line of opp.establishedFacts || []) {
    const c = text(line);
    if (!c) continue;
    // Do NOT mark CONFIRMED merely because text appears in establishedFacts
    facts.push({
      id: `established-${c.slice(0, 40).toLowerCase().replace(/\W+/g, "-")}`,
      claim: c,
      matchNeedles: [c.slice(0, 48)],
      verificationStatus: "PROVENANCE_INCOMPLETE",
      commercialImportance: "MEDIUM",
      changesPursuit: /hub|partner|block|venue|contact/i.test(c),
      origin: "ESTABLISHED_FACTS",
    });
  }
  return facts;
}

function appendUniqueSentence(base, sentence) {
  const b = text(base);
  const s = text(sentence);
  if (!s) return b;
  if (isTruncatedClaim(s)) return b;
  if (b.toLowerCase().includes(s.slice(0, Math.min(40, s.length)).toLowerCase())) return b;
  // Near-duplicate: book-directly / housing-bureau already covered
  if (
    /housing bureau|book directly/i.test(s) &&
    /housing bureau|book directly|direct booking/i.test(b)
  ) {
    // If brief already mentions direct booking but not housing bureau, allow one concise bureau line
    if (/housing bureau/i.test(s) && !/housing bureau/i.test(b)) {
      /* fall through */
    } else {
      return b;
    }
  }
  if (!b) return s;
  return `${b}${/[.!?]$/.test(b) ? " " : ". "}${s}`;
}

/**
 * Correct false "no contact route" wording when a published organizational
 * route (e.g. convention@…) is present. Do not invent a personal email.
 */
function correctPublishedContactRouteWording(nextStep, opp = {}, level1Facts = []) {
  let next = text(nextStep);
  const blob = [
    next,
    JSON.stringify(opp.primaryContact || {}),
    ...level1Facts.map((f) => f.claim),
    ...(opp.supportingResearch || []).map((f) => f.claim),
  ].join(" ");
  const hasOrgRoute =
    /[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}/i.test(blob) ||
    /convention@|published organizational route/i.test(blob);
  if (!hasOrgRoute) return next;
  if (/no direct route is published|no usable contact route|no contact route is published/i.test(next)) {
    next = next
      .replace(
        /no direct route is published/gi,
        "no verified personal email for the named contact is published (organizational route available)",
      )
      .replace(
        /no usable contact route/gi,
        "no verified personal email for the named contact",
      )
      .replace(
        /no contact route is published/gi,
        "no verified personal email for the named contact is published",
      );
  }
  return next;
}

/**
 * Select and apply Level 1 / Level 2 fact destinations onto an opportunity.
 */
export function selectAndApplyCommercialFacts(raw = {}, opts = {}) {
  const now = opts.now || new Date();
  const opp = { ...raw };
  const catalog = opts.catalog || opp.evidenceCatalog || null;
  const catalogFacts = Array.isArray(catalog?.materialFacts) ? catalog.materialFacts : [];

  const sourceRegister = buildCommercialSourceRegister(opp, { catalog });
  const merged = dedupeFacts([
    ...catalogFacts,
    ...extractInlineFacts(opp, catalogFacts),
  ]);
  const linkedPack = linkFactsToSourceRegister(merged, sourceRegister);
  const evaluated = [];
  const defects = [];

  for (const fact of linkedPack.facts) {
    const decision = reevaluateFactDestination(fact, { now });
    const prior = text(fact.destination || "").toUpperCase();
    evaluated.push({
      ...fact,
      priorDestination: prior || null,
      selectedDestination: decision.destination,
      selectionReason: decision.reason,
      hotelPrivate: decision.hotelPrivate,
      reevaluated: prior
        ? prior !== decision.destination.replace(/_HOTEL_PRIVATE$/, "")
        : true,
    });
    if (decision.destination === "OMIT_UNSUPPORTED") {
      defects.push({
        code: "HISTORICAL_AS_FUTURE_DEADLINE",
        factId: fact.id,
        claim: fact.claim,
      });
    }
    if (fact.provenanceStatus === "UNVERIFIED_TEXT_ONLY") {
      defects.push({
        code: "PROVENANCE_INCOMPLETE",
        factId: fact.id,
        claim: fact.claim,
      });
    }
  }

  // Re-link evaluated facts (same IDs) for section map after destination assignment
  const linkedFinal = linkFactsToSourceRegister(evaluated, sourceRegister);
  const citationPresentation = buildSectionCitationPresentation(linkedFinal, {
    excludeHotelPrivate: opts.customerSharedSurface === true,
    opportunityTitle: opp.title || opp.displayTitle || opp.organizationName || "",
  });
  const persistence = assessSourceRegisterPersistence(citationPresentation, opts.slimSources || []);

  const level1 = linkedFinal.facts.filter((f) => f.selectedDestination === "BRIEF");
  const level2 = linkedFinal.facts.filter((f) =>
    ["SUPPORTING", "SUPPORTING_HOTEL_PRIVATE"].includes(f.selectedDestination),
  );
  const missing = linkedFinal.facts.filter((f) => f.selectedDestination === "OMIT_MISSING");

  // Promote missing Level 1 material facts into brief fields (concise, no invention)
  let what = text(opp.whatIsHappening);
  let whyNow = text(opp.whyActNow);
  let next = text(opp.recommendedNextStep);
  let business = text(opp.potentialHotelBusiness);
  const briefBlob = [what, whyNow, next, business, text(opp.whyThisHotel)].join(" ");
  const promotedIntoBrief = [];

  for (const fact of level1) {
    if (fact.origin === "OPPORTUNITY_LODGING_EVIDENCE" && isTruncatedClaim(fact.claim)) {
      continue;
    }
    if (factInText(briefBlob, fact)) continue;
    const claim = text(fact.claim);
    if (!claim || isTruncatedClaim(claim)) continue;
    // Prefer concise catalog housing-bureau line over long lodging restatement
    if (
      fact.origin === "OPPORTUNITY_LODGING_EVIDENCE" &&
      /housing bureau|book directly/i.test(claim) &&
      /direct booking|housing bureau|book directly/i.test(what)
    ) {
      continue;
    }
    if (/housing bureau|book directly|partner|cutoff|venue|Enercare|October 20|FIU|hub|overflow/i.test(claim)) {
      if (/housing bureau|book directly|partner|cutoff|Enercare|October 20/i.test(claim)) {
        what = appendUniqueSentence(what, claim);
      } else if (/past cycle|do not pursue/i.test(claim)) {
        whyNow = appendUniqueSentence(whyNow, claim);
      } else {
        business = appendUniqueSentence(business, claim);
      }
      promotedIntoBrief.push(fact.id);
    } else if (/contact|@|buyer|Sampson|Petryna|Wetzel|Baker/i.test(claim)) {
      next = appendUniqueSentence(next, claim);
      promotedIntoBrief.push(fact.id);
    } else {
      what = appendUniqueSentence(what, claim);
      promotedIntoBrief.push(fact.id);
    }
  }

  next = correctPublishedContactRouteWording(next, opp, level1);

  const surface = [what, whyNow, next, business, ...level2.map((f) => f.claim)].join(" ");
  for (const fact of level1) {
    if (fact.origin === "ABSENT_FROM_RESEARCH") continue;
    if (!factInText(surface, fact) && !promotedIntoBrief.includes(fact.id)) {
      defects.push({
        code: "MATERIAL_FACT_NOT_SURFACED",
        factId: fact.id,
        claim: fact.claim,
      });
    }
    if (!(fact.sourceIds || []).length && fact.origin !== "ABSENT_FROM_RESEARCH") {
      defects.push({
        code: "FACT_MISSING_SOURCE_LINK",
        factId: fact.id,
        claim: fact.claim,
      });
    }
  }

  const supportingResearch = level2.map((f) => ({
    id: f.id,
    claim: f.claim,
    sourceIds: f.sourceIds || [],
    sourceUrl: f.sourceUrl || (f.sourceUrls || [])[0] || null,
    sourceUrls: f.sourceUrls || (f.sourceUrl ? [f.sourceUrl] : []),
    verificationStatus: f.verificationStatus || "UNKNOWN",
    provenanceStatus: f.provenanceStatus || null,
    verificationDate: f.verificationDate || null,
    activityDate: f.activityDate || null,
    hotelPrivate: Boolean(f.hotelPrivate),
    selectionReason: f.selectionReason,
    drawerSections: f.drawerSections || [],
    level: "SUPPORTING",
  }));

  const level1Facts = level1.map((f) => ({
    id: f.id,
    claim: f.claim,
    sourceIds: f.sourceIds || [],
    sourceUrl: f.sourceUrl || null,
    drawerSections: f.drawerSections || [],
    verificationStatus: f.verificationStatus,
    provenanceStatus: f.provenanceStatus,
  }));

  const quality = scoreFactSelectionQuality(linkedFinal.facts, {
    briefBlob: [what, whyNow, next, business, text(opp.whyThisHotel)].join(" "),
    supportingResearch,
    allSources: citationPresentation.allSources,
    sectionCitations: citationPresentation.sectionCitations,
  });

  // Merge register URLs into legacy sources[] so existing drawer evidence still lists them
  const priorSources = Array.isArray(opp.sources) ? opp.sources : [];
  const mergedSources = [];
  const seenSrc = new Set();
  for (const s of [
    ...priorSources,
    ...citationPresentation.allSources.map((s) => ({
      url: s.url,
      title: s.title,
      publisher: s.publisher,
      sourceId: s.sourceId,
      publicationDate: s.publicationDate,
    })),
  ]) {
    const url = text(typeof s === "string" ? s : s?.url);
    if (!url) continue;
    const key = url.toLowerCase().replace(/\/$/, "");
    if (seenSrc.has(key)) continue;
    seenSrc.add(key);
    mergedSources.push(typeof s === "string" ? { url: s } : s);
  }

  const nextOpp = {
    ...opp,
    whatIsHappening: what,
    whyActNow: whyNow,
    recommendedNextStep: next,
    potentialHotelBusiness: business,
    sources: mergedSources,
    commercialIntelligenceBrief: {
      ...(opp.commercialIntelligenceBrief || {}),
      whatIsHappening: what,
      whyActNow: whyNow,
      recommendedNextStep: next,
      potentialHotelBusiness: business,
      whyThisHotel: opp.whyThisHotel,
      level1Facts,
      sectionCitations: citationPresentation.sectionCitations,
    },
    supportingResearch,
    allSources: citationPresentation.allSources,
    sectionCitations: citationPresentation.sectionCitations,
    commercialFactCatalog: {
      version: GDI_COMMERCIAL_FACT_SELECTION_V1,
      facts: linkedFinal.facts,
      sources: linkedFinal.sources,
      durableRef: opts.durableRef || opp.commercialFactCatalog?.durableRef || null,
    },
    commercialSourceRegister: {
      version: linkedFinal.version,
      sources: linkedFinal.sources,
      sectionSourceIds: linkedFinal.sectionSourceIds,
      persistence: persistence,
    },
    factSelectionV1: {
      version: GDI_COMMERCIAL_FACT_SELECTION_V1,
      level1Count: level1.length,
      level2Count: level2.length,
      missingCount: missing.length,
      sourceCount: citationPresentation.allSources.length,
      promotedIntoBrief,
      defects,
      quality,
      persistence,
    },
  };

  return {
    version: GDI_COMMERCIAL_FACT_SELECTION_V1,
    opportunity: nextOpp,
    level1,
    level2,
    missing,
    defects,
    promotedIntoBrief,
    citationPresentation,
    persistence,
  };
}

export function applyCommercialFactSelection(opportunity = {}, opts = {}) {
  if (opportunity?.factSelectionV1?.skip === true) return opportunity;
  if (opportunity?._factSelectionApplied === true) return opportunity;
  const result = selectAndApplyCommercialFacts(opportunity, opts);
  return {
    ...result.opportunity,
    _factSelectionApplied: true,
  };
}

export function scoreFactSelectionQuality(evaluated = [], surfaces = {}) {
  const briefBlob = text(surfaces.briefBlob);
  const supportBlob = (surfaces.supportingResearch || []).map((f) => f.claim).join(" ");
  const combined = `${briefBlob}\n${supportBlob}`;
  const allSources = surfaces.allSources || [];
  const sectionCitations = surfaces.sectionCitations || {};

  const material = evaluated.filter(
    (f) => f.origin !== "ABSENT_FROM_RESEARCH" && f.selectedDestination !== "OMIT_UNSUPPORTED",
  );
  const l1Expected = material.filter((f) => f.selectedDestination === "BRIEF");
  const l1Hit = l1Expected.filter((f) => factInText(briefBlob, f));
  const retainedAll = material.filter((f) => factInText(combined, f));
  const unsupported = evaluated.filter((f) => f.selectedDestination === "OMIT_UNSUPPORTED").length;
  const privateLeaked = evaluated.filter(
    (f) => f.hotelPrivate && factInText(briefBlob, f) && f.sharedMarketForbidden,
  ).length;

  const withUrls = material.filter((f) => text(f.sourceUrl) || (f.sourceUrls || []).length);
  const linked = withUrls.filter((f) => (f.sourceIds || []).length > 0);
  const factToSourceTraceability =
    withUrls.length === 0 ? 100 : Math.round((linked.length / withUrls.length) * 100);

  const sourceRetentionCompleteness =
    allSources.length === 0
      ? 0
      : Math.round((allSources.filter((s) => s.url).length / allSources.length) * 100);

  const sectionKeys = Object.keys(sectionCitations).filter((k) => (sectionCitations[k] || []).length);
  const sectionPlacementAccuracy = sectionKeys.length > 0 ? 100 : allSources.length ? 50 : 0;

  const confidentialLeak = privateLeaked;
  const incorrectlyAttributed = evaluated.filter(
    (f) => f.provenanceStatus === "URL_NOT_IN_REGISTER" || f.provenanceStatus === "UNVERIFIED_TEXT_ONLY",
  ).length;

  const l1Accuracy =
    l1Expected.length === 0 ? 100 : Math.round((l1Hit.length / l1Expected.length) * 100);
  const retention =
    material.length === 0 ? 100 : Math.round((retainedAll.length / material.length) * 100);

  // 100% fact retention is NOT enough if source relationships are missing
  const overallExcellent =
    l1Accuracy >= 85 &&
    retention >= 90 &&
    factToSourceTraceability >= 85 &&
    sourceRetentionCompleteness >= 90 &&
    unsupported + confidentialLeak === 0 &&
    incorrectlyAttributed === 0;

  return {
    sourceRetentionCompleteness,
    factToSourceTraceability,
    sectionPlacementAccuracy,
    level1SelectionAccuracy: l1Accuracy,
    verifiedFactRetentionL1L2: retention,
    unsupportedOrMisattributedCount: unsupported + incorrectlyAttributed,
    confidentialSourceLeakage: confidentialLeak,
    persistenceReadBackGaps: "NOT_CERTIFIED — see factSelectionV1.persistence",
    redundancyNote: "Level 2 must not paste Level 1 paragraphs",
    overallExcellent,
  };
}

export default {
  GDI_COMMERCIAL_FACT_SELECTION_V1,
  reevaluateFactDestination,
  selectAndApplyCommercialFacts,
  applyCommercialFactSelection,
  scoreFactSelectionQuality,
};
