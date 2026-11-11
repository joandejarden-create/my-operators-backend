/**
 * Run-052 commercial-depth audit — Bethesda benchmark vs Chapultepec inventory.
 * Read-only. Does not invent contacts, room nights, or paid research.
 */
export const CHAPULTEPEC_COMMERCIAL_DEPTH_AUDIT_V1 =
  "gdi-chapultepec-commercial-depth-audit-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function len(v) {
  return text(v).length;
}

function gold(o) {
  return o?.associationEventsGoldBrief || o?.salesBrief || {};
}

function pick(o, ...keys) {
  for (const k of keys) {
    const v = o?.[k];
    if (v != null && text(v)) return v;
  }
  const g = gold(o);
  for (const k of keys) {
    const v = g?.[k];
    if (v != null && text(v)) return v;
  }
  return null;
}

const GENERIC_FIT =
  /sits in the Chapultepec\s*\/\s*Reforma\s*\/\s*Polanco corridor|Seven meeting rooms and executive facilities|Ballroom capacity up to\s*~?220|used by Mexico City medical and association programmes/i;

const SPECIFIC_COMPETITIVE =
  /overflow|preferred hotel|host hotel|room block|stay-to-play|housing bureau|Crystal Gateway|Hilton [A-Z]|Marriott [A-Z]|sede oficial|hospedaje oficial|hoteles oficiales|Spargo|HBC |prior host|historically (uses|used)|official hotel|compet(e|ing|itor)|Lodging path class|without a confirmed exclusive host|can open a sourcing conversation/i;

const DEMAND_MECHANISM =
  /guestroom|room night|lodging|housing|meeting|catering|speaker|delegate|overflow|block|hospedaje|alojamiento|sal[oó]n|sede/i;

const HONEST_LIMIT =
  /not proof|unconfirmed|verify|unknown|to be confirmed|not yet|research continuation|do not|limitation|unresolved|pending/i;

/**
 * Dimension scores 0–1. Separated; never collapsed into a single opaque confidence.
 */
export function scoreCommercialDepthDimensions(opportunity = {}, { hotelId = null } = {}) {
  const g = gold(opportunity);
  const summary = text(
    pick(opportunity, "summaryWhat", "whatIsHappening", "opportunityOverview") ||
      g.opportunityOverview ||
      g.eventHistoryAndActivity,
  );
  const whyHotel = text(
    pick(opportunity, "whyThisHotel", "hotelFitRationale", "bethesdaWinThesis", "chapultepecFit") ||
      g.whyThisHotel,
  );
  const whyNow = text(pick(opportunity, "whyNow", "buyingAndSourcingStatus") || g.buyingAndSourcingStatus);
  const action = text(
    pick(opportunity, "recommendedAction", "salesActionRecommended") ||
      g.recommendedSalesApproach,
  );
  const winThesis = text(
    pick(opportunity, "bethesdaWinThesis", "hotelOpportunityThesis", "hotelDemandThesis") ||
      g.potentialBusinessForHotel,
  );
  const contactName = text(
    opportunity.contactName ||
      opportunity.primaryContact?.name ||
      g.whoToContact?.primary?.name,
  );
  const whyContact = text(
    pick(opportunity, "whyThisContact") ||
      g.whoToContact?.primary?.whyThisContact ||
      g.whoToContact?.whyThisContact,
  );
  const orgRoute = Boolean(
    g.whoToContact?.routes?.url ||
      opportunity.organizationRouteUrl ||
      opportunity.contactUrl ||
      (/Identify the association|organiz/i.test(action) && !contactName),
  );
  const sources = Array.isArray(opportunity.sources) ? opportunity.sources : [];
  const sourceCount = sources.filter((s) => text(s?.url || s)).length;
  const chronologyOk = !/Paris|LASA2026 \(Paris\)/i.test(text(opportunity.title)) ||
    /LASA2027|future|CDMX|Ciudad de M/i.test(`${opportunity.title} ${summary} ${whyHotel}`);

  const identityAndEvent = Math.min(
    1,
    (len(summary) >= 80 ? 0.35 : len(summary) >= 40 ? 0.2 : 0.05) +
      (len(whyNow) >= 40 ? 0.25 : 0.05) +
      (sourceCount >= 2 ? 0.25 : sourceCount >= 1 ? 0.1 : 0) +
      (chronologyOk ? 0.15 : 0),
  );

  const hotelDemand =
    DEMAND_MECHANISM.test(`${summary} ${winThesis} ${g.potentialBusinessForHotel || ""}`)
      ? len(winThesis) >= 60
        ? 0.85
        : 0.55
      : 0.2;

  const propertySpecific = (() => {
    if (!whyHotel) return 0.05;
    if (GENERIC_FIT.test(whyHotel) && !SPECIFIC_COMPETITIVE.test(whyHotel)) return 0.25;
    if (SPECIFIC_COMPETITIVE.test(`${whyHotel} ${winThesis}`)) return 0.9;
    if (len(whyHotel) >= 120 && !GENERIC_FIT.test(whyHotel)) return 0.7;
    return 0.4;
  })();

  const historicalCompetitive = SPECIFIC_COMPETITIVE.test(
    `${whyHotel} ${winThesis} ${summary} ${whyNow}`,
  )
    ? 0.8
    : /compet|prior|previous edition|edición anterior|históric/i.test(
          `${summary} ${g.eventHistoryAndActivity || ""}`,
        )
      ? 0.45
      : 0.15;

  const orgRoleClass = text(
    opportunity.orgRoleClass ||
      g.whoToContact?.orgRoleClass ||
      g.orgRoleClass ||
      opportunity.buyerOrgAccessibility?.orgRoleClass,
  );
  const accessClass = text(
    opportunity.sourcingAccessibilityClass ||
      g.sourcingAccessibilityClass ||
      opportunity.buyerOrgAccessibility?.sourcingAccessibilityClass,
  );
  const actionQuality = text(
    pick(opportunity, "recommendedAction", "salesActionRecommended") ||
      g.recommendedSalesApproach,
  );
  const functionalAction =
    /Open with|Approach the|meetings \/ congress|housing|PCO|referral into/i.test(
      actionQuality,
    ) && !/^Identify the association meetings \/ congress organizer/i.test(actionQuality);
  const contactRelevance = contactName
    ? whyContact && len(whyContact) >= 40
      ? 0.9
      : 0.55
    : orgRoleClass && accessClass && (orgRoute || functionalAction)
      ? 0.72
      : orgRoute && functionalAction
        ? 0.65
        : orgRoute
          ? 0.55
          : 0.15;

  const commercialStrategy =
    (len(action) >= 60 ? 0.4 : 0.1) +
    (HONEST_LIMIT.test(`${action} ${summary} ${whyHotel}`) ? 0.25 : 0.05) +
    (/Ask |Identify |Confirm |Email |Contact /i.test(action) ? 0.25 : 0.05) +
    (len(whyNow) >= 30 ? 0.1 : 0);

  const evidenceQuality = Math.min(
    1,
    (sourceCount >= 3 ? 0.45 : sourceCount >= 1 ? 0.25 : 0.05) +
      (HONEST_LIMIT.test(`${summary} ${whyHotel}`) ? 0.25 : 0.1) +
      (!GENERIC_FIT.test(whyHotel) || SPECIFIC_COMPETITIVE.test(whyHotel) ? 0.2 : 0.05) +
      (Number(opportunity.evidenceConfidence) > 85 && sourceCount < 2 ? -0.2 : 0.1),
  );

  const salesUsability = Math.min(
    1,
    (propertySpecific >= 0.7 ? 0.3 : propertySpecific >= 0.4 ? 0.15 : 0.05) +
      (contactRelevance >= 0.5 ? 0.25 : 0.1) +
      (commercialStrategy >= 0.6 ? 0.25 : 0.1) +
      (identityAndEvent >= 0.5 ? 0.2 : 0.05),
  );

  const researchCoverage = Math.min(
    1,
    identityAndEvent * 0.2 +
      hotelDemand * 0.15 +
      propertySpecific * 0.2 +
      historicalCompetitive * 0.15 +
      contactRelevance * 0.15 +
      evidenceQuality * 0.15,
  );

  const dims = {
    researchCoverage: Number(researchCoverage.toFixed(3)),
    evidenceSupport: Number(Math.max(0, evidenceQuality).toFixed(3)),
    commercialReasoning: Number(commercialStrategy.toFixed(3)),
    propertySpecificity: Number(propertySpecific.toFixed(3)),
    contactRelevance: Number(contactRelevance.toFixed(3)),
    salesUsability: Number(salesUsability.toFixed(3)),
    identityAndEvent: Number(identityAndEvent.toFixed(3)),
    hotelDemandIntelligence: Number(hotelDemand.toFixed(3)),
    historicalCompetitive: Number(historicalCompetitive.toFixed(3)),
    presentationIntegrity: Number(
      (/#{1,6}\s|Skip to main content|##\s*MAF/i.test(summary) ? 0.1 : 0.85).toFixed(3),
    ),
  };

  const gaps = [];
  if (dims.propertySpecificity < 0.5) gaps.push("GENERIC_OR_WEAK_HOTEL_FIT");
  if (dims.historicalCompetitive < 0.4) gaps.push("MISSING_PRIOR_HOST_OR_COMPETITIVE");
  if (dims.contactRelevance < 0.4) gaps.push("WEAK_BUYER_OR_ORG_ROUTE");
  if (dims.hotelDemandIntelligence < 0.5) gaps.push("WEAK_DEMAND_MECHANISM");
  if (dims.identityAndEvent < 0.45) gaps.push("WEAK_EVENT_IDENTITY_OR_TIMING");
  if (GENERIC_FIT.test(whyHotel) && !SPECIFIC_COMPETITIVE.test(whyHotel)) {
    gaps.push("GENERIC_CHAPULTEPEC_CORRIDOR_TEMPLATE");
  }
  if (sourceCount === 0) gaps.push("NO_SOURCES");

  return {
    version: CHAPULTEPEC_COMMERCIAL_DEPTH_AUDIT_V1,
    hotelId: hotelId || opportunity.hotelId || null,
    opportunityId: opportunity.opportunityId || opportunity.id || null,
    title: opportunity.title || null,
    segment: opportunity.researchSegment || opportunity.segment || null,
    qualification: opportunity.opportunityQualification || g.qualification?.status || null,
    dimensions: dims,
    gaps,
    signals: {
      sourceCount,
      genericFitTemplate: GENERIC_FIT.test(whyHotel),
      specificCompetitiveLanguage: SPECIFIC_COMPETITIVE.test(`${whyHotel} ${winThesis}`),
      hasNamedContact: Boolean(contactName),
      orgRouteOnly: !contactName && orgRoute,
      whyHotelPreview: whyHotel.slice(0, 180),
      actionPreview: action.slice(0, 160),
      summaryPreview: summary.slice(0, 160),
    },
    notes: [
      "Scores measure investigation depth / usability proxies from persisted fields.",
      "High researchCoverage with RESEARCH_CONTINUATION or DO_NOT_PURSUE can still be valid.",
      "Does not invent missing commercial facts.",
    ],
  };
}

export function compareToBethesdaBenchmark(caseScore, benchmarkScores = []) {
  const avg = (key) => {
    if (!benchmarkScores.length) return null;
    const sum = benchmarkScores.reduce(
      (a, b) => a + Number(b.dimensions?.[key] || 0),
      0,
    );
    return Number((sum / benchmarkScores.length).toFixed(3));
  };
  const deltas = {};
  for (const key of Object.keys(caseScore.dimensions || {})) {
    const b = avg(key);
    deltas[key] = b == null ? null : Number((caseScore.dimensions[key] - b).toFixed(3));
  }
  return { bethesdaDimensionAverages: Object.fromEntries(
    Object.keys(caseScore.dimensions || {}).map((k) => [k, avg(k)]),
  ), deltas };
}
