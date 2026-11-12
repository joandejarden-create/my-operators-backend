/**
 * Bethesda Marriott — 67-record sales-opportunity baseline + CMI population V1.
 *
 * Corrects the operating model: the frozen 67 IDs are the sales-team baseline
 * inventory (19 published + 48 staged), not permanent founder-only research.
 *
 * Customer publication of the additional 48 requires an explicit founder
 * approval receipt. Until then, customer/share surfaces stay at the approved 19.
 *
 * COMPETITIVE_MARKET_INTELLIGENCE is a separate population (verified competitor
 * bookings) — not a fifteenth demand segment and not Sales Ready inflation.
 *
 * Zero paid research: classification uses existing cached opportunity fields only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

function hasCompetitiveVenueEvidence(o = {}) {
  if (o.competitiveVenueEvidence === true) return true;
  if (o.competitorVenueConfirmed === true) return true;
  const sourcing = upper(o.venueSourcingStatus || o.hotelSourcingStatus);
  if (
    (sourcing === "VENUE_CONFIRMED" || sourcing === "HISTORICAL_HOST") &&
    (o.competitiveCaptureEligible === true ||
      /COMPETITOR|CAPTURE|OVERFLOW/i.test(text(o.competitiveTrack)))
  ) {
    return true;
  }
  return false;
}

export const BETHESDA_67_SALES_BASELINE_CMI_V1 =
  "gdi-bethesda-67-sales-baseline-cmi-v1";

export const BETHESDA_HOTEL_ID = "recLuxvwwxID7U2B8";

/** Opportunity Type population values (orthogonal to demand-segment filters). */
export const INVENTORY_POPULATION = Object.freeze({
  SALES_OPPORTUNITIES: "SALES_OPPORTUNITIES",
  COMPETITIVE_MARKET_INTELLIGENCE: "COMPETITIVE_MARKET_INTELLIGENCE",
  ALL_COMMERCIAL_INTELLIGENCE: "ALL_COMMERCIAL_INTELLIGENCE",
});

export const INVENTORY_POPULATION_LABEL = Object.freeze({
  [INVENTORY_POPULATION.SALES_OPPORTUNITIES]: "Sales Opportunities",
  [INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE]:
    "Competitive Market Intelligence",
  [INVENTORY_POPULATION.ALL_COMMERCIAL_INTELLIGENCE]:
    "All Commercial Intelligence",
});

export const CMI_CLASS = Object.freeze({
  COMPETITIVE_MARKET_INTELLIGENCE: "COMPETITIVE_MARKET_INTELLIGENCE",
  NEEDS_FURTHER_VERIFICATION: "NEEDS_FURTHER_VERIFICATION",
  NOT_CMI: "NOT_CMI",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FORENSIC_RAW =
  "reports/gdi-bethesda-67-to-19-forensic-reconciliation-v1/01-frozen67-vs-facing19-raw.json";
const FORENSIC_SUMMARY =
  "reports/gdi-bethesda-67-to-19-forensic-reconciliation-v1/00-summary.json";
const PROTECTED_FREEZE =
  "reports/gdi-bethesda-dmv-territory-expansion-v1/00-protected-67-baseline-freeze.json";
const APPROVAL_RECEIPT =
  "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/bethesda-67-customer-publication-approval.json";

function readJson(rel) {
  const p = path.isAbsolute(rel) ? rel : path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function text(v) {
  return String(v == null ? "" : v).trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

let _manifestCache = null;

/**
 * Exact frozen 67-ID baseline (19 published + 48 staged).
 */
export function loadBethesda67SalesBaselineManifest(opts = {}) {
  if (_manifestCache && !opts.forceReload) return _manifestCache;
  const freeze = readJson(PROTECTED_FREEZE);
  const forensic = readJson(FORENSIC_RAW);
  const summary = readJson(FORENSIC_SUMMARY);
  const approved19 = freeze?.approvedCustomerFacingIds || forensic?.keptIds || [];
  const historical48 =
    freeze?.historicalResearchInventoryIds ||
    (forensic?.missingRows || []).map((r) => r.id);
  const all67 = [...new Set([...approved19, ...historical48])];
  if (all67.length !== 67) {
    throw new Error(
      `BETHESDA_67_BASELINE_INVALID: expected 67 unique ids, got ${all67.length}`
    );
  }
  if (approved19.length !== 19 || historical48.length !== 48) {
    throw new Error(
      `BETHESDA_67_SPLIT_INVALID: approved=${approved19.length} historical=${historical48.length}`
    );
  }
  _manifestCache = {
    version: BETHESDA_67_SALES_BASELINE_CMI_V1,
    hotelId: BETHESDA_HOTEL_ID,
    hotelName: "Bethesda Marriott",
    provenance: {
      protectedFreeze: PROTECTED_FREEZE,
      forensicRaw: FORENSIC_RAW,
      forensicSummary: FORENSIC_SUMMARY,
      note: "Frozen PR #211 67-ID sales-team baseline (19 published + 48 staged).",
    },
    counts: {
      totalUniqueBaselineIds: 67,
      publishedCustomerFacing: 19,
      stagedForSalesTeamPublication: 48,
    },
    publishedIds: [...approved19],
    stagedIds: [...historical48],
    baselineIds: all67,
    forensicCategoryBreakdown: summary?.categoryBreakdownMissing48 || null,
    subcategoryBreakdown: summary?.subcategoryBreakdown || null,
  };
  return _manifestCache;
}

export function isBethesda67BaselineId(id, manifest = null) {
  const m = manifest || loadBethesda67SalesBaselineManifest();
  return m.baselineIds.includes(String(id || ""));
}

export function isBethesda67PublishedId(id, manifest = null) {
  const m = manifest || loadBethesda67SalesBaselineManifest();
  return m.publishedIds.includes(String(id || ""));
}

export function isBethesda67StagedId(id, manifest = null) {
  const m = manifest || loadBethesda67SalesBaselineManifest();
  return m.stagedIds.includes(String(id || ""));
}

/**
 * Explicit founder approval required before the 48 enter the hotel customer feed.
 * Never treat env absence as approval.
 */
export function isBethesda67CustomerPublicationApproved(opts = {}) {
  if (opts.forceApproved === true) return true;
  if (opts.forceApproved === false) return false;
  const env = String(
    process.env.GDI_BETHESDA_67_CUSTOMER_PUBLICATION_APPROVED || ""
  ).trim();
  if (env === "1" || /^true$/i.test(env)) return true;
  const receipt = readJson(opts.receiptPath || APPROVAL_RECEIPT);
  if (!receipt || typeof receipt !== "object") return false;
  if (receipt.approved !== true) return false;
  if (receipt.hotelId && receipt.hotelId !== BETHESDA_HOTEL_ID) return false;
  if (receipt.baselineVersion && receipt.baselineCount !== 67) return false;
  return true;
}

export function isInventoryPopulationValue(value) {
  const v = String(value || "");
  return Object.values(INVENTORY_POPULATION).includes(v);
}

/**
 * Commercial maturity / sales usability label for honest inventory staging.
 */
export function classifySalesBaselineMaturity(opp = {}, forensicRow = null, opts = {}) {
  const salesUsability = upper(
    forensicRow?.salesUsability ||
      opp.salesUsability ||
      opp.hotelSalesUsability ||
      ""
  );
  const maturity = upper(
    opp.commercialMaturity ||
      opp.gdiMaturityState ||
      forensicRow?.currentMaturity ||
      forensicRow?.persistedMaturity ||
      ""
  );
  const invClass = upper(
    opp.commercialInventoryClass || forensicRow?.commercialInventoryClass || ""
  );
  const priority = upper(opp.priority || forensicRow?.currentPriority || "");
  const engine = upper(
    forensicRow?.engineCommercialCertification ||
      opp.engineCommercialCertification ||
      ""
  );
  const qual = upper(opp.opportunityQualification || "");
  const publishedBaseline =
    opts.publishedBaseline === true ||
    opp.salesBaselineMembership === "PUBLISHED_19" ||
    (opp.customerVisible === true &&
      isBethesda67PublishedId(opp.id || opp.opportunityId));

  if (invClass === "SALES_READY" || maturity === "SALES_READY") {
    return {
      key: "SALES_READY",
      label: "Sales Ready",
      customerSafe: true,
    };
  }
  if (
    invClass === "ACCOUNT_DEVELOPMENT" ||
    maturity === "ACCOUNT_DEVELOPMENT" ||
    salesUsability === "ACCOUNT_DEVELOPMENT" ||
    engine === "ACCOUNT_DEVELOPMENT_ELIGIBLE"
  ) {
    return {
      key: "ACCOUNT_DEVELOPMENT",
      label: "Account Development",
      customerSafe: true,
    };
  }
  // Published customer-feed rows without explicit maturity stamps are active
  // sales inventory — prefer Sales Ready when qualification is verified/open.
  if (publishedBaseline) {
    if (
      engine === "SALES_READY_ELIGIBLE" ||
      qual === "VERIFIED_OPEN" ||
      qual === "ACTIONABLE_NOW" ||
      qual === "STRONG"
    ) {
      return {
        key: "SALES_READY",
        label: "Sales Ready",
        customerSafe: true,
      };
    }
    return {
      key: "ACCOUNT_DEVELOPMENT",
      label: "Account Development",
      customerSafe: true,
    };
  }
  if (engine === "SALES_READY_ELIGIBLE") {
    return {
      key: "SALES_READY",
      label: "Sales Ready",
      customerSafe: true,
    };
  }
  if (
    salesUsability === "QUALIFY_FIRST" ||
    /QUALIF/.test(maturity) ||
    engine.includes("QUALIF")
  ) {
    return {
      key: "QUALIFICATION_REQUIRED",
      label: "Qualification Required",
      customerSafe: true,
    };
  }
  if (
    /PAST|INACTIVE|HISTORIC/.test(salesUsability) ||
    /PAST|INACTIVE/.test(maturity) ||
    forensicRow?.subcategory === "PAST_EVENT_SURFACE_INACTIVE"
  ) {
    return {
      key: "PAST_INACTIVE",
      label: "Past / inactive activity",
      customerSafe: true,
    };
  }
  if (
    /FUTURE/.test(salesUsability) ||
    /FUTURE/.test(maturity) ||
    priority === "WATCHLIST" ||
    salesUsability === "MONITOR"
  ) {
    return {
      key: "RESEARCH_CONTINUATION_MONITOR",
      label: "Research Continuation / Monitor",
      customerSafe: true,
    };
  }
  if (/FUTURE/.test(text(opp.bookingWindowStatus))) {
    return {
      key: "FUTURE_BUSINESS",
      label: "Future business",
      customerSafe: true,
    };
  }
  return {
    key: "RESEARCH_CONTINUATION_MONITOR",
    label: "Research Continuation / Monitor",
    customerSafe: true,
  };
}

/**
 * Customer-readiness assessment for staged (non-published) baseline rows.
 * Does not invent content; classifies what is already known.
 */
export function assessStagedCustomerReadiness(opp = {}, forensicRow = null) {
  const maturity = classifySalesBaselineMaturity(opp, forensicRow);
  const exclusion = forensicRow?.exclusionReasons || [];
  const namedContact = text(
    forensicRow?.namedContact ||
      opp.primaryContact?.name ||
      opp.primaryContactName ||
      ""
  );
  const hasContact = Boolean(namedContact);
  const demand = upper(
    forensicRow?.demandConfidence ||
      opp.guestroomDemandConfidence ||
      opp.demandEvidenceLabel ||
      ""
  );
  const privacyConcern =
    /email|phone|personal/i.test(text(opp.privacyFlags)) ||
    opp.containsUnverifiedPersonalContact === true;
  const duplicateOf = text(
    opp.duplicateOfOpportunityId ||
      opp.canonicalDuplicateOf ||
      forensicRow?.duplicateOf ||
      ""
  );

  let readiness = "REQUIRES_ADDITIONAL_RESEARCH";
  let reasons = [];

  if (duplicateOf) {
    readiness = "DUPLICATE";
    reasons.push(`Duplicate of ${duplicateOf}`);
  } else if (maturity.key === "PAST_INACTIVE") {
    readiness = "HISTORICAL_OR_INACTIVE";
    reasons.push("Past or inactive commercial surface");
  } else if (
    forensicRow?.classification === "A" ||
    forensicRow?.subcategory === "PAST_EVENT_SURFACE_INACTIVE"
  ) {
    readiness = "HISTORICAL_OR_INACTIVE";
    reasons.push("Forensic category A / past-event inactive");
  } else if (
    maturity.key === "SALES_READY" ||
    maturity.key === "ACCOUNT_DEVELOPMENT"
  ) {
    if (hasContact && demand && !/SPECULATIVE|UNKNOWN/.test(demand)) {
      readiness = "CUSTOMER_READY_CONTENT";
      reasons.push("Named contact + non-speculative demand + AD/SR maturity");
    } else {
      readiness = "INCOMPLETE_OR_UNCERTAIN";
      reasons.push("AD/SR maturity but contact or demand still thin");
    }
  } else if (maturity.key === "QUALIFICATION_REQUIRED") {
    readiness = "REQUIRES_ADDITIONAL_RESEARCH";
    reasons.push("Qualification gaps remain (host/lodging/fit open)");
  } else {
    readiness = "REQUIRES_ADDITIONAL_RESEARCH";
    reasons.push("Monitor / research-continuation posture");
  }

  if (privacyConcern) {
    readiness = "PRIVACY_OR_DATA_QUALITY";
    reasons.push("Privacy or data-quality concern flagged");
  }

  if (exclusion.includes("customerVisible=false")) {
    reasons.push("Not yet customerVisible (publication gate)");
  }
  if (exclusion.includes("lacks_approved_commercial_class")) {
    reasons.push("Lacks approved commercial inventory class for feed");
  }

  return {
    opportunityId: opp.id || forensicRow?.id || "",
    readiness,
    maturity,
    namedContact: namedContact || null,
    demandConfidence: demand || null,
    exclusionReasons: exclusion,
    reasons,
    publicationReviewReady: readiness === "CUSTOMER_READY_CONTENT",
    autoPublishAllowed: false,
  };
}

/**
 * Zero-paid verified CMI seeds from existing Exa / recovery research
 * (reports/gdi-bethesda-67-sales-baseline-cmi-v1/03-cmi-verified.json).
 * Not speculative — ID-keyed so classification survives presentation DTO trim.
 */
export const BETHESDA_EXA_VERIFIED_CMI_SEEDS = Object.freeze({
  gdi_opp_nado_ddaa_washcon_2027_overflow: {
    competitorHotel: "Crystal Gateway Marriott",
    evidence:
      "Existing Exa recovery: 2027 host and published room rates public at Crystal Gateway; overflow/waitlist posture — verified competitor host booking.",
  },
  gdi_opp_nado_ddaa_washcon_2028: {
    competitorHotel: "Crystal Gateway Marriott",
    evidence:
      "Existing Exa recovery: published March 19–22, 2028 Crystal Gateway Marriott host — verified competitor host booking.",
  },
  gdi_opp_acvnu_renal_week_20270412: {
    competitorHotel: "The Bethesdan Hotel (Tapestry)",
    evidence:
      "Existing Exa recovery: ACVNU Renal Week 2027 official room block at The Bethesdan Hotel — verified competitor booking.",
  },
  gdi_opp_world_biomaterials_2028_overflow: {
    competitorHotel: "Bethesda North Marriott Hotel & Conference Center",
    evidence:
      "Named competitor host with primary-venue-selected overflow thesis (verified CMI artifact).",
  },
  gdi_opp_aad_2028_overflow: {
    competitorHotel: "Bethesda North Marriott Hotel & Conference Center",
    evidence:
      "Named competitor host with primary-venue-selected overflow thesis (verified CMI artifact).",
  },
  gdi_opp_afcea_hits_2027: {
    competitorHotel: "Hyatt Regency Bethesda",
    evidence:
      "Named competitor host with primary-venue-selected overflow thesis (verified CMI artifact).",
  },
  gdi_opp_ecs_251_2027_overflow: {
    competitorHotel: "Bethesda North Marriott Hotel & Conference Center",
    evidence:
      "Named competitor host with primary-venue-selected overflow thesis (verified CMI artifact).",
  },
});

/**
 * Verified competitor-booked CMI standard (strict).
 * Historical hosting / speculative competitor mentions → NEEDS_FURTHER_VERIFICATION.
 */
export function classifyCompetitiveMarketIntelligence(opp = {}) {
  const id = String(opp.id || opp.opportunityId || "");
  const seed = BETHESDA_EXA_VERIFIED_CMI_SEEDS[id] || null;
  const competitorHotel = text(
    seed?.competitorHotel ||
      opp.competitorHotel ||
      opp.competitorVenue ||
      opp.likelyCompetitor ||
      opp.confirmedCompetitorVenue ||
      opp.competitiveDemand?.competitorHotel ||
      ""
  );
  const sourcing = upper(opp.venueSourcingStatus || opp.hotelSourcingStatus);
  const overflowNamed =
    /overflow/i.test(id) ||
    /\boverflow\b/i.test(text(opp.title || opp.displayTitle));
  const strictBookingFlag =
    opp.competitorVenueConfirmed === true ||
    opp.competitiveVenueEvidence === true ||
    sourcing === "VENUE_CONFIRMED" ||
    upper(opp.competitorBookingEvidenceClass) === "VERIFIED_BOOKING" ||
    upper(opp.competitiveBookingStatus) === "VERIFIED_BOOKING";
  // Verified CMI = Exa seed, explicit booking flag, or explicitly named overflow
  // opportunity with primary venue selected elsewhere. Do not promote ordinary
  // Sales Opportunities merely from a competitor hotel string + overflow stamp.
  const bookingEvidence =
    Boolean(seed) ||
    strictBookingFlag ||
    (overflowNamed &&
      Boolean(competitorHotel) &&
      sourcing === "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE");

  const speculative =
    opp.competitiveCaptureEligible === true && !bookingEvidence;
  const hasCompChipSignal = hasCompetitiveVenueEvidence(opp);

  if (bookingEvidence && competitorHotel) {
    return {
      class: CMI_CLASS.COMPETITIVE_MARKET_INTELLIGENCE,
      label: "Competitive Market Intelligence",
      verified: true,
      competitorHotel,
      reason: seed
        ? seed.evidence
        : strictBookingFlag
          ? "Verified competitor venue/booking evidence present"
          : "Named competitor host with primary-venue-selected overflow thesis",
      seedId: seed ? id : null,
    };
  }
  if (bookingEvidence && !competitorHotel) {
    return {
      class: CMI_CLASS.NEEDS_FURTHER_VERIFICATION,
      label: "Needs further verification",
      verified: false,
      competitorHotel: null,
      reason: "Booking-like signal without named competitor venue",
    };
  }
  if (hasCompChipSignal || speculative || competitorHotel) {
    return {
      class: CMI_CLASS.NEEDS_FURTHER_VERIFICATION,
      label: "Needs further verification",
      verified: false,
      competitorHotel: competitorHotel || null,
      reason:
        "Competitor association or capture eligibility without verified booking standard",
    };
  }
  return {
    class: CMI_CLASS.NOT_CMI,
    label: "Not CMI",
    verified: false,
    competitorHotel: null,
    reason: "No competitor booking evidence",
  };
}

export function isVerifiedCmiOpportunity(opp = {}) {
  return (
    classifyCompetitiveMarketIntelligence(opp).class ===
    CMI_CLASS.COMPETITIVE_MARKET_INTELLIGENCE
  );
}

/**
 * Annotate opportunities with baseline + CMI population metadata (non-mutating copy).
 */
export function annotateInventoryPopulations(opportunities = [], opts = {}) {
  const hotelId = opts.hotelId || null;
  const manifest =
    hotelId === BETHESDA_HOTEL_ID || opts.forceBethesdaManifest
      ? loadBethesda67SalesBaselineManifest()
      : null;
  const forensicById = new Map();
  if (manifest) {
    const forensic = readJson(FORENSIC_RAW);
    for (const row of forensic?.missingRows || []) {
      forensicById.set(row.id, row);
    }
  }

  return (opportunities || []).map((opp) => {
    const id = String(opp.id || opp.opportunityId || "");
    const forensicRow = forensicById.get(id) || null;
    const onBaseline = manifest ? isBethesda67BaselineId(id, manifest) : false;
    const published = manifest
      ? isBethesda67PublishedId(id, manifest)
      : Boolean(opp.customerVisible !== false);
    const staged = manifest ? isBethesda67StagedId(id, manifest) : false;
    const maturity = classifySalesBaselineMaturity(opp, forensicRow, {
      publishedBaseline: published,
    });
    const readiness = staged
      ? assessStagedCustomerReadiness(opp, forensicRow)
      : null;
    const cmi = classifyCompetitiveMarketIntelligence(opp);
    const populations = [];
    if (onBaseline || !manifest) {
      // Bethesda 67 baseline + non-Bethesda commercial feed → Sales Opportunities.
      populations.push(INVENTORY_POPULATION.SALES_OPPORTUNITIES);
    }
    if (cmi.verified) {
      populations.push(INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE);
    }

    return {
      ...opp,
      inventoryPopulations: populations,
      inventoryPopulationPrimary:
        populations[0] || INVENTORY_POPULATION.SALES_OPPORTUNITIES,
      salesBaselineMembership: onBaseline
        ? published
          ? "PUBLISHED_19"
          : "STAGED_48"
        : "OUTSIDE_67_BASELINE",
      salesBaselineMaturityKey: maturity.key,
      salesBaselineMaturityLabel: maturity.label,
      stagedCustomerReadiness: readiness?.readiness || null,
      cmiClass: cmi.class,
      cmiVerified: cmi.verified === true,
      cmiCompetitorHotel: cmi.competitorHotel,
      cmiClassificationReason: cmi.reason,
    };
  });
}

export function filterByInventoryPopulation(opportunities = [], population) {
  const pop = String(population || INVENTORY_POPULATION.SALES_OPPORTUNITIES);
  if (!pop || pop === INVENTORY_POPULATION.ALL_COMMERCIAL_INTELLIGENCE) {
    return opportunities || [];
  }
  return (opportunities || []).filter((o) => {
    const pops = o.inventoryPopulations || [o.inventoryPopulationPrimary];
    return pops.includes(pop);
  });
}

/**
 * Headline counts — never conflate Sales Opportunities with Sales Ready.
 */
export function buildInventoryPopulationCounts(opportunities = []) {
  const rows = opportunities || [];
  const sales = rows.filter((o) =>
    (o.inventoryPopulations || []).includes(
      INVENTORY_POPULATION.SALES_OPPORTUNITIES
    )
  );
  const cmi = rows.filter((o) => o.cmiVerified === true);
  const maturity = {};
  for (const o of sales) {
    const k = o.salesBaselineMaturityKey || "UNKNOWN";
    maturity[k] = (maturity[k] || 0) + 1;
  }
  return {
    salesOpportunities: sales.length,
    competitiveMarketIntelligence: cmi.length,
    allCommercialIntelligence: new Set([
      ...sales.map((o) => o.id),
      ...cmi.map((o) => o.id),
    ]).size,
    salesReadyAmongSales: maturity.SALES_READY || 0,
    accountDevelopmentAmongSales: maturity.ACCOUNT_DEVELOPMENT || 0,
    maturityBreakdown: maturity,
    headline: `${sales.length} Sales Opportunities + ${cmi.length} Competitive Intelligence Records`,
  };
}

/**
 * Inventory audience — separate sales-team working inventory from published feed.
 * Never treat localhost / query-string alone as publication approval.
 */
export const GDI_INVENTORY_AUDIENCE = Object.freeze({
  SALES_TEAM_INVENTORY: "SALES_TEAM_INVENTORY",
  PUBLISHED_CUSTOMER_FEED: "PUBLISHED_CUSTOMER_FEED",
});

export const BETHESDA_SALES_TEAM_LIST_SCOPE = "BETHESDA_SALES_TEAM_INVENTORY_67";
export const BETHESDA_PUBLISHED_CUSTOMER_LIST_SCOPE =
  "BETHESDA_PUBLISHED_CUSTOMER_FEED_19";

/**
 * Hotels whose working sales inventory may exceed the published customer feed.
 * Env: GDI_SALES_TEAM_INVENTORY_HOTEL_IDS (comma-separated). Default: Bethesda only.
 * `gdiPilotUnauthRead` alone never expands inventory for hotels outside this set.
 */
export function resolveSalesTeamInventoryHotelAllowlist(env = process.env) {
  const raw = env?.GDI_SALES_TEAM_INVENTORY_HOTEL_IDS;
  if (raw == null || String(raw).trim() === "") {
    return new Set([BETHESDA_HOTEL_ID]);
  }
  return new Set(
    String(raw)
      .split(/[,;\s]+/)
      .map((s) => String(s || "").trim())
      .filter(Boolean)
  );
}

function collectUserHotelScopeIds(user = {}) {
  const bags = [
    user.allowedHotelIds,
    user.gdiHotelIds,
    user.hotelIds,
    user.authorizedHotelIds,
    user.gdiAllowedHotelIds,
  ];
  const out = [];
  for (const bag of bags) {
    if (!Array.isArray(bag)) continue;
    for (const id of bag) {
      const s = String(id || "").trim();
      if (s && !out.includes(s)) out.push(s);
    }
  }
  return out;
}

/**
 * Hotel-scoped authorization for expanded (unpublished-inclusive) sales inventory.
 * Pilot unauth is insufficient unless the requested hotel is allowlisted.
 * A Dealality user scoped to other hotels cannot expand Bethesda inventory.
 */
export function assertHotelScopedSalesTeamInventoryAccess(
  req = {},
  opts = {}
) {
  const hotelId = String(opts.hotelId || req?.params?.hotelId || "").trim();
  const allowlist = resolveSalesTeamInventoryHotelAllowlist(opts.env || process.env);
  const hotelAllowlisted = Boolean(hotelId) && allowlist.has(hotelId);

  if (!hotelId) {
    return {
      ok: false,
      reason: "hotel_id_required",
      hotelAllowlisted: false,
    };
  }
  if (!hotelAllowlisted) {
    return {
      ok: false,
      reason: "hotel_not_on_sales_team_inventory_allowlist",
      hotelAllowlisted: false,
    };
  }

  // Share / external surfaces: never expand.
  if (
    opts.forcePublished === true ||
    opts.shareSurface === true ||
    req?.gdiShareCapability ||
    req?.gdiSignedShare ||
    req?.gdiExternalShare === true
  ) {
    return {
      ok: false,
      reason: "share_or_external_surface",
      hotelAllowlisted: true,
    };
  }

  const user = req?.dealalityUser || null;
  if (user) {
    const scoped = collectUserHotelScopeIds(user);
    if (scoped.length > 0 && !scoped.includes(hotelId)) {
      return {
        ok: false,
        reason: "user_hotel_scope_excludes_requested_hotel",
        hotelAllowlisted: true,
        userHotelScope: scoped,
      };
    }
    // Admin / unscoped Dealality staff: allowlisted hotel only.
    return {
      ok: true,
      reason: user.isAdmin
        ? "dealality_admin_allowlisted_hotel"
        : scoped.length
          ? "dealality_user_hotel_scoped"
          : "dealality_user_allowlisted_hotel",
      hotelAllowlisted: true,
      via: "dealalityUser",
    };
  }

  // Pilot unauth read (gdiPilotReadAuth) — hotel allowlist is the hotel-specific gate.
  // Does not unlock unpublished inventory for non-allowlisted hotels.
  if (req?.gdiPilotUnauthRead === true) {
    return {
      ok: true,
      reason: "pilot_unauth_allowlisted_hotel",
      hotelAllowlisted: true,
      via: "gdiPilotUnauthRead",
    };
  }

  return {
    ok: false,
    reason: "no_sales_team_inventory_authorization",
    hotelAllowlisted: true,
  };
}

/**
 * Resolve which inventory the caller is authorized to see.
 *
 * Sales-team inventory (67): requires hotel-scoped authorization via
 * `assertHotelScopedSalesTeamInventoryAccess` (allowlisted hotel + Dealality
 * user for that hotel, or pilot-unauth only for allowlisted hotels).
 * Published customer feed (19): share/capability surfaces, anonymous
 * non-pilot contexts, cross-hotel-scoped users, or failed sales-team auth.
 */
export function resolveGdiInventoryAudience(req = {}, opts = {}) {
  const hotelId = String(opts.hotelId || req?.params?.hotelId || "").trim();
  const q = req?.query || opts.query || {};
  const explicit = String(
    q.inventoryAudience || q.audience || opts.inventoryAudience || ""
  )
    .trim()
    .toUpperCase();

  if (
    explicit === "PUBLISHED" ||
    explicit === "PUBLISHED_CUSTOMER" ||
    explicit === "PUBLISHED_CUSTOMER_FEED" ||
    explicit === GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED
  ) {
    return GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED;
  }

  // Share / external capability routes must never expand to sales-team inventory.
  if (
    opts.forcePublished === true ||
    opts.shareSurface === true ||
    req?.gdiShareCapability ||
    req?.gdiSignedShare ||
    req?.gdiExternalShare === true
  ) {
    return GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED;
  }

  const wantsSalesTeam =
    explicit === "SALES_TEAM" ||
    explicit === "SALES_TEAM_INVENTORY" ||
    explicit === GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY ||
    opts.forceSalesTeam === true;

  const access = assertHotelScopedSalesTeamInventoryAccess(req, {
    hotelId,
    forcePublished: opts.forcePublished,
    shareSurface: opts.shareSurface,
    env: opts.env,
  });

  if (hotelId === BETHESDA_HOTEL_ID) {
    // Default Bethesda Opportunities page → 67 only when hotel-scoped auth passes.
    if (access.ok) {
      return GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY;
    }
    if (wantsSalesTeam && !access.ok) {
      return GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED;
    }
    return GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED;
  }

  if (wantsSalesTeam && access.ok) {
    return GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY;
  }

  return GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED;
}

/**
 * Detail/list parity: unpublished rows require sales-team inventory audience.
 * Returns { ok, audience, reason } — callers should 404 on !ok for unpublished.
 */
export function assertOpportunityReadableForInventoryAudience(
  req = {},
  opportunity = {},
  opts = {}
) {
  const hotelId = String(opts.hotelId || req?.params?.hotelId || "").trim();
  const opportunityId = String(
    opportunity?.id || opportunity?.opportunityId || opts.opportunityId || ""
  ).trim();
  const audience =
    opts.inventoryAudience ||
    resolveGdiInventoryAudience(req, { hotelId, ...opts });

  if (audience === GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY) {
    return { ok: true, audience, reason: "sales_team_inventory" };
  }

  // Published audience: only published Bethesda baseline IDs (or customer-facing).
  if (hotelId === BETHESDA_HOTEL_ID && opportunityId) {
    const manifest = loadBethesda67SalesBaselineManifest();
    const published = new Set(manifest.publishedIds || []);
    if (published.has(opportunityId)) {
      return { ok: true, audience, reason: "published_baseline_id" };
    }
    if ((manifest.baselineIds || []).includes(opportunityId)) {
      return {
        ok: false,
        audience,
        reason: "unpublished_baseline_requires_sales_team_inventory",
      };
    }
  }

  if (opts.isCustomerFacing === true) {
    return { ok: true, audience, reason: "customer_facing" };
  }
  if (opts.isCustomerFacing === false) {
    return {
      ok: false,
      audience,
      reason: "not_customer_facing_under_published_audience",
    };
  }

  return { ok: true, audience, reason: "published_audience_default" };
}

/**
 * Select the authorized sales-opportunity set for a surface.
 * Customer/share: published 19 until founder approval; then full 67 baseline.
 * Sales-team / founder review: always full 67 when rows are available.
 */
export function selectBethesdaSalesOpportunityInventory(
  opportunities = [],
  opts = {}
) {
  const manifest = loadBethesda67SalesBaselineManifest();
  const byId = new Map(
    (opportunities || []).map((o) => [String(o.id || o.opportunityId), o])
  );
  const approved = isBethesda67CustomerPublicationApproved(opts);
  const founderReview = opts.founderReview === true;
  const salesTeamInventory =
    opts.salesTeamInventory === true ||
    opts.inventoryAudience === GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY;
  const ids =
    founderReview || salesTeamInventory || approved
      ? manifest.baselineIds
      : manifest.publishedIds;
  const selected = [];
  const missing = [];
  for (const id of ids) {
    const row = byId.get(id);
    if (row) selected.push(row);
    else missing.push(id);
  }
  return {
    hotelId: BETHESDA_HOTEL_ID,
    approved,
    founderReview,
    salesTeamInventory,
    inventoryAudience:
      founderReview || salesTeamInventory || approved
        ? GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY
        : GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED,
    expectedCount: ids.length,
    selected,
    missingIds: missing,
    publishedCount: manifest.publishedIds.filter((id) => byId.has(id)).length,
    stagedPresentCount: manifest.stagedIds.filter((id) => byId.has(id)).length,
  };
}
