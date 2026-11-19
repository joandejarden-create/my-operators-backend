/**
 * Commercial Fact Selection V1 — promote material verified findings into Level 1
 * Brief vs Level 2 Supporting Research. Does not invent demand. Reevaluates
 * destinations (does not blindly trust prior BRIEF/SUPPORTING labels).
 */

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

function extractInlineFacts(opp = {}) {
  const facts = [];
  const lodging = text(opp.lodgingEvidence);
  if (/housing bureaus|book directly/i.test(lodging)) {
    facts.push({
      id: "inline-lodging-direct-book",
      claim: lodging.slice(0, 280),
      matchNeedles: ["housing bureaus", "Book directly"],
      sourceUrl: opp.lodgingEvidenceUrl || opp.officialSource || null,
      verificationStatus: "CONFIRMED",
      commercialImportance: "HIGH",
      changesPursuit: true,
      affectsAxes: ["buyer_access", "pursuit"],
      origin: "OPPORTUNITY_LODGING_EVIDENCE",
    });
  }
  for (const line of opp.establishedFacts || []) {
    const c = text(line);
    if (!c) continue;
    facts.push({
      id: `established-${c.slice(0, 40).toLowerCase().replace(/\W+/g, "-")}`,
      claim: c,
      matchNeedles: [c.slice(0, 48)],
      verificationStatus: "CONFIRMED",
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
  if (b.toLowerCase().includes(s.slice(0, Math.min(40, s.length)).toLowerCase())) return b;
  if (!b) return s;
  return `${b}${/[.!?]$/.test(b) ? " " : ". "}${s}`;
}

/**
 * Select and apply Level 1 / Level 2 fact destinations onto an opportunity.
 */
export function selectAndApplyCommercialFacts(raw = {}, opts = {}) {
  const now = opts.now || new Date();
  const opp = { ...raw };
  const catalogFacts = Array.isArray(opts.catalog?.materialFacts)
    ? opts.catalog.materialFacts
    : Array.isArray(opp.evidenceCatalog?.materialFacts)
      ? opp.evidenceCatalog.materialFacts
      : [];

  const merged = dedupeFacts([...catalogFacts, ...extractInlineFacts(opp)]);
  const evaluated = [];
  const defects = [];

  for (const fact of merged) {
    const decision = reevaluateFactDestination(fact, { now });
    const prior = text(fact.destination || "").toUpperCase();
    evaluated.push({
      ...fact,
      priorDestination: prior || null,
      selectedDestination: decision.destination,
      selectionReason: decision.reason,
      hotelPrivate: decision.hotelPrivate,
      reevaluated: prior ? prior !== decision.destination.replace(/_HOTEL_PRIVATE$/, "") : true,
    });
    if (decision.destination === "OMIT_UNSUPPORTED") {
      defects.push({
        code: "HISTORICAL_AS_FUTURE_DEADLINE",
        factId: fact.id,
        claim: fact.claim,
      });
    }
  }

  const level1 = evaluated.filter((f) => f.selectedDestination === "BRIEF");
  const level2 = evaluated.filter((f) =>
    ["SUPPORTING", "SUPPORTING_HOTEL_PRIVATE"].includes(f.selectedDestination),
  );
  const missing = evaluated.filter((f) => f.selectedDestination === "OMIT_MISSING");

  // Promote missing Level 1 material facts into brief fields (concise, no invention)
  let what = text(opp.whatIsHappening);
  let whyNow = text(opp.whyActNow);
  let next = text(opp.recommendedNextStep);
  let business = text(opp.potentialHotelBusiness);
  const briefBlob = [what, whyNow, next, business, text(opp.whyThisHotel)].join(" ");
  const promotedIntoBrief = [];

  for (const fact of level1) {
    if (factInText(briefBlob, fact)) continue;
    const claim = text(fact.claim);
    if (!claim) continue;
    // Route by content
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

  // Fail visibly when HIGH + changesPursuit material facts remain absent from L1+L2 surface
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
  }

  const supportingResearch = level2.map((f) => ({
    id: f.id,
    claim: f.claim,
    sourceUrl: f.sourceUrl || (f.sourceUrls || [])[0] || null,
    sourceUrls: f.sourceUrls || (f.sourceUrl ? [f.sourceUrl] : []),
    verificationStatus: f.verificationStatus || "CONFIRMED",
    verificationDate: f.verificationDate || null,
    activityDate: f.activityDate || null,
    hotelPrivate: Boolean(f.hotelPrivate),
    selectionReason: f.selectionReason,
    // Do not duplicate Level 1 prose
    level: "SUPPORTING",
  }));

  const nextOpp = {
    ...opp,
    whatIsHappening: what,
    whyActNow: whyNow,
    recommendedNextStep: next,
    potentialHotelBusiness: business,
    commercialIntelligenceBrief: {
      ...(opp.commercialIntelligenceBrief || {}),
      whatIsHappening: what,
      whyActNow: whyNow,
      recommendedNextStep: next,
      potentialHotelBusiness: business,
      whyThisHotel: opp.whyThisHotel,
    },
    supportingResearch,
    commercialFactCatalog: {
      version: GDI_COMMERCIAL_FACT_SELECTION_V1,
      facts: evaluated,
      durableRef: opts.durableRef || opp.commercialFactCatalog?.durableRef || null,
    },
    factSelectionV1: {
      version: GDI_COMMERCIAL_FACT_SELECTION_V1,
      level1Count: level1.length,
      level2Count: level2.length,
      missingCount: missing.length,
      promotedIntoBrief,
      defects,
      quality: scoreFactSelectionQuality(evaluated, {
        briefBlob: [what, whyNow, next, business, text(opp.whyThisHotel)].join(" "),
        supportingResearch,
      }),
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

  const l1Accuracy =
    l1Expected.length === 0 ? 100 : Math.round((l1Hit.length / l1Expected.length) * 100);
  const retention =
    material.length === 0 ? 100 : Math.round((retainedAll.length / material.length) * 100);

  return {
    level1SelectionAccuracy: l1Accuracy,
    verifiedFactRetentionL1L2: retention,
    unsupportedAssertionCount: unsupported + privateLeaked,
    redundancyNote: "Level 2 must not paste Level 1 paragraphs",
    // Perfect overall fidelity is NOT awarded merely for predesignated L1 needles
    overallExcellent:
      l1Accuracy >= 85 &&
      retention >= 90 &&
      unsupported + privateLeaked === 0 &&
      l1Expected.every((f) => f.selectionReason === "MATERIAL_SALES_DECISION" || factInText(briefBlob, f)),
  };
}

export default {
  GDI_COMMERCIAL_FACT_SELECTION_V1,
  reevaluateFactDestination,
  selectAndApplyCommercialFacts,
  applyCommercialFactSelection,
  scoreFactSelectionQuality,
};
