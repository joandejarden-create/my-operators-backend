/**
 * Permanent certified GDI drawer commercial-content contract.
 *
 * Applies to all hotels, all demand segments, auth + external share.
 * Does not invent commercial facts. Empty core sections must be explicit
 * uncertainty or eligibility demotion — never silent blanks.
 */

export const GDI_CERTIFIED_DRAWER_COMMERCIAL_CONTENT_CONTRACT_V1 =
  "gdi-certified-drawer-commercial-content-contract-v1";

export const CERTIFIED_DRAWER_SECTION_IDS = Object.freeze([
  "opportunity_overview",
  "what_is_happening",
  "potential_business",
  "why_this_hotel",
  "why_act_now",
  "facts_and_uncertainties",
  "who_to_contact",
  "recommended_next_step",
  "supporting_evidence",
  "hotel_validation",
]);

/** Core commercial narrative fields that must not be silently empty. */
export const CERTIFIED_CORE_COMMERCIAL_FIELDS = Object.freeze([
  "whatIsHappening",
  "potentialHotelBusiness",
  "whyThisHotel",
  "whyActNow",
  "recommendedNextStep",
]);

function text(v) {
  return String(v == null ? "" : v)
    .replace(/\s+/g, " ")
    .trim();
}

function firstText(...vals) {
  for (const v of vals) {
    const s = text(v);
    if (s) return s;
  }
  return "";
}

export function appearsTruncatedCustomerProse(value) {
  const s = text(value);
  if (!s) return false;
  if (/[.!?)"'\u201d\u2019]\s*$/.test(s)) return false;
  if (s.length < 180) return false;
  // Mid-sentence cut or dangling connector.
  if (/[—–-]\s*$/.test(s)) return true;
  if (/\b(and|or|with|for|the|a|an|to|of|in)\s*$/i.test(s)) return true;
  if (/[a-z0-9,;:]\s*$/i.test(s) && s.length >= 220) return true;
  return false;
}

/**
 * Shared non-specific hotel-fit / research boilerplate that is honest but weak
 * for a Director of Sales. Detected so drawers can demote or rewrite display.
 */
export const GENERIC_COMMERCIAL_PROSE_PATTERNS = Object.freeze([
  /plausible local venue for chapter, civic, and community programmes/i,
  /when independent demand and a commercial contact are verified/i,
  /subject to evidence/i,
  /confirm meeting activity/i,
  /awaiting evidence/i,
  /research continuation\s*[—-]\s*establish a verified named commercial contact/i,
  /original research body is missing from recoverable artifacts/i,
  /not a confirmed hotel (catering )?booking/i,
]);

export function detectGenericCommercialProse(value) {
  const s = text(value);
  if (!s) return { generic: false, matches: [] };
  const matches = GENERIC_COMMERCIAL_PROSE_PATTERNS.filter((re) => re.test(s)).map(
    (re) => re.source,
  );
  return { generic: matches.length > 0, matches };
}

/**
 * Prefer org-specific hotel-fit language over repeated catering boilerplate.
 * Does not invent demand — only reframes generic template into explicit uncertainty.
 */
export function sanitizeGenericHotelFitNarrative(opportunity = {}, rawFit = "") {
  const fit = text(rawFit);
  const detected = detectGenericCommercialProse(fit);
  if (!detected.generic) return { text: fit, rewritten: false, matches: [] };

  const org = firstText(
    opportunity.organizationName,
    opportunity.displayCompanyName,
    opportunity.companyName,
  );
  const hotel = firstText(opportunity.hotelName, opportunity.hotelDisplayName);
  const rewritten = text(
    [
      org || "This organization",
      hotel
        ? `may use ${hotel} banquet / day-meeting space when a dated programme and commercial buyer are verified`
        : "may use this hotel's banquet / day-meeting space when a dated programme and commercial buyer are verified",
      "— hotel booking is not evidenced by the current research body.",
    ].join(" "),
  );
  return { text: rewritten, rewritten: true, matches: detected.matches };
}

export function composeOverviewIdentityLine(opportunity = {}) {
  const org = firstText(
    opportunity.displayCompanyName,
    opportunity.organizationName,
    opportunity.companyName,
  );
  const title = firstText(
    opportunity.displayTitle,
    opportunity.drawerHeaderTitle,
    opportunity.eventDisplayName,
    opportunity.eventName,
    opportunity.title,
  );
  if (!org && !title) return "Programme TBD";
  if (!org) return title;
  if (!title) return org;
  if (org.toLowerCase() === title.toLowerCase()) return org;
  const orgPrefix = `${org} — `;
  if (title.toLowerCase().startsWith(orgPrefix.toLowerCase())) return title;
  if (title.toLowerCase().startsWith(`${org.toLowerCase()} - `)) {
    return `${org} — ${title.slice(org.length).replace(/^\s*[-—–]\s*/, "")}`;
  }
  // Title already starts with org name (no separator).
  if (title.toLowerCase().startsWith(org.toLowerCase())) return title;
  return `${org} — ${title}`;
}

export function resolveCoreCommercialBodies(opportunity = {}) {
  const cib = opportunity.commercialIntelligenceBrief || {};
  const sales = opportunity.salesBrief || {};
  const aegb = opportunity.associationEventsGoldBrief || {};
  const what = firstText(
    cib.whatIsHappening,
    sales.whatIsHappening,
    opportunity.whatIsHappening,
    opportunity.summaryWhat,
    opportunity.tileCustomerSummary,
  );
  const pot = firstText(
    cib.potentialBusinessForThisHotel,
    cib.potentialHotelBusiness,
    sales.potentialBusinessForThisHotel,
    aegb.potentialBusinessForHotel,
    opportunity.potentialHotelBusiness,
    opportunity.potentialBusiness,
    opportunity.summaryWhyMatters,
  );
  const whyHotelRaw = firstText(
    cib.whyThisHotel,
    sales.whyThisHotel,
    aegb.whyThisHotel,
    opportunity.whyThisHotel,
    opportunity.summaryWhyHotel,
    opportunity.hotelFitRationale,
    opportunity.localConnection,
  );
  const whyHotelSanitized = sanitizeGenericHotelFitNarrative(
    opportunity,
    whyHotelRaw,
  );
  const whyHotel = whyHotelSanitized.text;
  const whyNow = firstText(
    cib.whyActNow,
    sales.whyActNow,
    opportunity.whyActNow,
    opportunity.whyEstablishContactNow,
    opportunity.whyNow,
    opportunity.summaryWhyNow,
  );
  const nextCandidates = [
    opportunity.recommendedNextStep,
    cib.recommendedNextStep,
    sales.recommendedNextStep,
    opportunity.recommendedAction,
    opportunity.recommendedNextAction,
  ]
    .map(text)
    .filter(Boolean);
  let next = "";
  for (const c of nextCandidates) {
    if (!appearsTruncatedCustomerProse(c)) {
      next = c;
      break;
    }
  }
  if (!next && nextCandidates.length) {
    next = nextCandidates.reduce((a, b) => (b.length > a.length ? b : a));
  }

  return {
    whatIsHappening: what,
    potentialHotelBusiness: pot,
    whyThisHotel: whyHotel,
    whyActNow: whyNow,
    recommendedNextStep: next,
    whyThisHotelRewrittenFromGeneric: whyHotelSanitized.rewritten === true,
    whyThisHotelGenericMatches: whyHotelSanitized.matches || [],
  };
}

export function resolveContactRelevance(opportunity = {}) {
  const c =
    opportunity.canonicalCustomerPresentation?.contact ||
    opportunity.primaryContact ||
    {};
  const explicit = firstText(
    c.relevance,
    c.whyThisContact,
    c.whyRelevant,
    opportunity.whoPrimaryReason,
    opportunity.whyThisContact,
  );
  if (explicit) return explicit;
  const klass = text(c.contactClass || opportunity.contactClass);
  if (klass === "RELEVANT_COMMERCIAL_ENTRY_POINT") {
    return "Named commercial entry point for lodging / partnership qualification — not verified as the person who controls team accommodation.";
  }
  if (klass === "DECISION_MAKER") {
    return "Named decision-maker relevant to this lodging opportunity.";
  }
  if (klass === "OPERATIONS_OR_PROCUREMENT") {
    return "Operational or procurement contact for accommodation sourcing.";
  }
  if (text(c.name) && text(c.title || c.role || c.professionalTitle)) {
    return `${text(c.title || c.role || c.professionalTitle)} — relevance should be confirmed against the lodging buyer for this opportunity.`;
  }
  return "";
}

/**
 * Evaluate one opportunity against the certified commercial drawer contract.
 */
export function evaluateCertifiedDrawerCommercialContent(opportunity = {}, opts = {}) {
  const bodies = resolveCoreCommercialBodies(opportunity);
  const identity = composeOverviewIdentityLine(opportunity);
  const defects = [];
  const warnings = [];
  const classify = (code, layer, detail) => {
    defects.push({ code, layer, detail });
  };
  const warn = (code, layer, detail) => {
    warnings.push({ code, layer, detail });
  };

  const org = firstText(
    opportunity.organizationName,
    opportunity.displayCompanyName,
  );
  if (org && identity.toLowerCase().startsWith(`${org.toLowerCase()} — ${org.toLowerCase()}`)) {
    classify("duplicated_organization_in_title", "frontend_rendering", identity);
  }

  for (const field of CERTIFIED_CORE_COMMERCIAL_FIELDS) {
    const value = bodies[field] || "";
    if (!value) {
      classify("missing_core_commercial_section", "canonical_dto_or_projection", field);
      continue;
    }
    if (appearsTruncatedCustomerProse(value)) {
      classify("truncated_customer_prose", "publication_or_dto", field);
    }
    // whyThisHotel may already be sanitized; still flag raw opportunity fields.
    if (field !== "whyThisHotel") {
      const generic = detectGenericCommercialProse(value);
      if (generic.generic) {
        classify("generic_commercial_boilerplate", "canonical_dto_or_projection", field);
      }
    }
  }
  if (bodies.whyThisHotelRewrittenFromGeneric) {
    warn(
      "generic_hotel_fit_boilerplate_rewritten",
      "presentation_sanitizer",
      (bodies.whyThisHotelGenericMatches || []).join("|") || "whyThisHotel",
    );
  }

  const contactName = firstText(
    opportunity.canonicalCustomerPresentation?.contact?.name,
    opportunity.primaryContact?.name,
    opportunity.tileContactName,
  );
  const relevance = resolveContactRelevance(opportunity);
  if (contactName && !relevance) {
    classify("missing_contact_relevance", "canonical_dto_or_projection", contactName);
  }

  const emptyCore = CERTIFIED_CORE_COMMERCIAL_FIELDS.filter((f) => !bodies[f]);
  const requiresResearch = emptyCore.length > 0;
  const demoteCandidate =
    !bodies.whatIsHappening ||
    (!bodies.potentialHotelBusiness && !bodies.whyThisHotel);

  return {
    version: GDI_CERTIFIED_DRAWER_COMMERCIAL_CONTENT_CONTRACT_V1,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    hotelId: opts.hotelId || opportunity.hotelId || null,
    identityLine: identity,
    bodies,
    contactName: contactName || null,
    contactRelevance: relevance || null,
    defects,
    warnings,
    emptyCoreCount: emptyCore.length,
    truncatedCount: defects.filter((d) => d.code === "truncated_customer_prose").length,
    genericBoilerplateCount: defects.filter(
      (d) => d.code === "generic_commercial_boilerplate",
    ).length,
    hotelFitRewritten: warnings.some(
      (w) => w.code === "generic_hotel_fit_boilerplate_rewritten",
    ),
    missingContactRelevance: Boolean(contactName && !relevance),
    requiresAdditionalResearch: requiresResearch,
    readinessDemotionCandidate: demoteCandidate,
    ok: defects.length === 0,
  };
}

export function summarizeCertifiedDrawerAudit(rows = []) {
  return {
    audited: rows.length,
    withMissingCommercialContent: rows.filter((r) => r.emptyCoreCount > 0).length,
    withTruncatedText: rows.filter((r) => r.truncatedCount > 0).length,
    withGenericBoilerplate: rows.filter((r) => (r.genericBoilerplateCount || 0) > 0)
      .length,
    withHotelFitRewrite: rows.filter((r) => r.hotelFitRewritten).length,
    withInvalidOrMissingContactRelevance: rows.filter((r) => r.missingContactRelevance)
      .length,
    requiringAdditionalResearch: rows.filter((r) => r.requiresAdditionalResearch)
      .length,
    requiringReadinessDemotion: rows.filter((r) => r.readinessDemotionCandidate)
      .length,
    fullyPassing: rows.filter((r) => r.ok).length,
  };
}
