/**
 * GDI commercial sales quality helpers (run-031).
 * Corrects run-030 personhood / outreach-route / gold-brief completeness defects.
 * Does NOT change strict FullE2E publication eligibility.
 */

export const GDI_COMMERCIAL_SALES_QUALITY_V1 = 'gdi-commercial-sales-quality-v1';

export const GOLD_BRIEF_STATUS = {
  COMPLETE_AND_ACTIONABLE: 'COMPLETE_AND_ACTIONABLE',
  ACTIONABLE_WITH_DISCLOSED_GAPS: 'ACTIONABLE_WITH_DISCLOSED_GAPS',
  RESEARCH_CONTINUATION: 'RESEARCH_CONTINUATION',
  INSUFFICIENT_COMMERCIAL_EVIDENCE: 'INSUFFICIENT_COMMERCIAL_EVIDENCE',
};

export const DOS_GRADE = {
  ASSIGN_NOW: 'ASSIGN_NOW',
  ASSIGN_FOR_DISCOVERY: 'ASSIGN_FOR_DISCOVERY',
  RESEARCH_FIRST: 'RESEARCH_FIRST',
  DO_NOT_PURSUE: 'DO_NOT_PURSUE',
};

export const OUTREACH_ROUTE_KIND = {
  VERIFIED_DIRECT_EMAIL: 'VERIFIED_DIRECT_EMAIL',
  VERIFIED_PUBLIC_PHONE: 'VERIFIED_PUBLIC_PHONE',
  VERIFIED_ORG_EMAIL: 'VERIFIED_ORG_EMAIL',
  VERIFIED_HOUSING_OR_PROGRAM_ROUTE: 'VERIFIED_HOUSING_OR_PROGRAM_ROUTE',
  VERIFIED_REFERRAL_OR_DEPARTMENT: 'VERIFIED_REFERRAL_OR_DEPARTMENT',
  NAMED_PERSON_NO_ROUTE: 'NAMED_PERSON_NO_ROUTE',
  ORGANIZATION_NO_ROUTE: 'ORGANIZATION_NO_ROUTE',
  ENRICHMENT_ON_DEMAND: 'ENRICHMENT_ON_DEMAND',
  NONE: 'NONE',
};

export const CONTACT_PERSONHOOD = {
  VERIFIED_NAMED_PERSON: 'VERIFIED_NAMED_PERSON',
  FUNCTIONAL_ORGANIZATION_ROUTE: 'FUNCTIONAL_ORGANIZATION_ROUTE',
  UNKNOWN_OR_ABSENT: 'UNKNOWN_OR_ABSENT',
};

function text(v) {
  return String(v || '').replace(/\s+/g, ' ').trim();
}

/** Org / desk / mailbox labels that must NOT count as a named person */
const NON_PERSON_NAME_RE =
  /^(general inquiries|unknown|n\/?a|tbd|none|null|functional entity)$|team\s*travel\s*source|travel\s*source|housing\s*team|\/\s*housing|housing\s*center|housing\s*desk|housing\s*bureau|conference\s*desk|meeting(s)?\s*desk|events?\s*desk|sales\s*office|front\s*desk|info@|support@|contact\s*us|association\s*office|north american headquarters|headquarters$|museum$|gallery$|cruises?\s*&?\s*events?$|^uss\s|^sparks\s|^flagship\s|^city\s*cruises|^deck\d+|^the\s+house\s+next\s+door|^nado\s+conference|^west\s+housing|^varsity\s+spirit/i;

const LOOKS_LIKE_PERSON_RE =
  /^[A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?(?:\s+[A-Z][a-z]+)+(?:\s+(?:Jr|Sr|II|III|IV)\.?)?$/;

/**
 * True only when the string is a plausible human name shape.
 * "WEST Housing Center" → false. "Sabrina Bracken" → true.
 * Shape alone is NOT enough for commercial verification — use
 * `verifyNamedPersonWithEvidence` when asserting a verified named person.
 */
export function isVerifiedNamedPerson(name) {
  const n = text(name);
  if (!n || n.length < 3) return false;
  if (NON_PERSON_NAME_RE.test(n)) return false;
  // Org / desk compound labels (slash paths, housing desks) are never people
  if (/[\/|]/.test(n)) return false;
  if (/\b(housing\s*(team|desk|center|bureau)|desk|bureau|inquiries)\b/i.test(n)) {
    return false;
  }
  // Chamber / institutional acronym shells
  if (/^(canaco|concanaco|coparmex|amexcap|anade|andellac|smne|amrob|varsity)\b/i.test(n)) {
    return false;
  }
  if (/\b(cdmx|mexico city|ciudad de m[eé]xico)\b/i.test(n) && n.split(/\s+/).length <= 3) {
    // "CANACO CDMX", "Endocrinología Mexico City" style labels
    if (!/^[A-Z][a-z]+\s+[A-Z][a-z]+$/.test(n)) return false;
  }
  // Reject scrape/role noise prefixes
  if (/^(administrativo|reconocimiento|design week|speakers?|endocrinolog|enlace|gerente general at)\b/i.test(n)) {
    return false;
  }
  if (/\b(inc|llc|ltd|corp|company|museum|gallery|cruises|events|center|desk|office|bureau|asociaci[oó]n|c[aá]mara)\b/i.test(n)) {
    if (!LOOKS_LIKE_PERSON_RE.test(n) && n.split(/\s+/).length <= 4) {
      if (!/^[A-Za-zÁÉÍÓÚÑáéíóúñ].*\s+[A-Za-zÁÉÍÓÚÑáéíóúñ]/.test(n) || NON_PERSON_NAME_RE.test(n)) return false;
    }
  }
  const parts = n.split(/\s+/).filter(Boolean);
  if (parts.length < 2) return false;
  if (parts.length > 5) return false; // likely scraped sentence
  if (parts.every((p) => p.length <= 1)) return false;
  if (/@/.test(n)) return false;
  if (/\d{4}/.test(n)) return false; // dates leaked into name
  return true;
}

/**
 * Named person requires name shape PLUS supporting evidence (role/org/source URL).
 * Two words alone are insufficient.
 */
export function verifyNamedPersonWithEvidence({
  name,
  organization,
  role,
  evidenceUrl,
  evidenceUrls,
  confidence,
} = {}) {
  const shapeOk = isVerifiedNamedPerson(name);
  const roleOk = Boolean(text(role));
  const orgOk = Boolean(text(organization));
  const urls = [];
  if (text(evidenceUrl)) urls.push(text(evidenceUrl));
  for (const u of evidenceUrls || []) {
    const v = typeof u === 'string' ? u : u?.url;
    if (text(v)) urls.push(text(v));
  }
  const sourceOk = urls.length > 0;
  const verified = shapeOk && sourceOk && (roleOk || orgOk);
  return {
    shapeOk,
    roleOk,
    orgOk,
    sourceOk,
    verified,
    confidence: confidence || (verified ? (roleOk && orgOk ? 'HIGH' : 'MEDIUM') : 'LOW'),
    name: text(name) || null,
    organization: text(organization) || null,
    role: text(role) || null,
    evidenceUrls: urls.slice(0, 5),
    personhood: !shapeOk
      ? classifyPersonhood(name)
      : verified
        ? CONTACT_PERSONHOOD.VERIFIED_NAMED_PERSON
        : CONTACT_PERSONHOOD.UNKNOWN_OR_ABSENT,
    note: verified
      ? null
      : shapeOk
        ? 'Name shape present but missing role/org or source evidence — treat as RESEARCH_CANDIDATE'
        : 'Not a verified named person',
  };
}

export function classifyPersonhood(name) {
  const n = text(name);
  if (!n || /^unknown$/i.test(n)) return CONTACT_PERSONHOOD.UNKNOWN_OR_ABSENT;
  if (isVerifiedNamedPerson(n)) return CONTACT_PERSONHOOD.VERIFIED_NAMED_PERSON;
  if (NON_PERSON_NAME_RE.test(n) || /\b(center|desk|office|bureau|museum|gallery|inquiries)\b/i.test(n)) {
    return CONTACT_PERSONHOOD.FUNCTIONAL_ORGANIZATION_ROUTE;
  }
  // Single-token or org-ish label
  return CONTACT_PERSONHOOD.FUNCTIONAL_ORGANIZATION_ROUTE;
}

function looksLikePersonalEmail(email) {
  const e = text(email).toLowerCase();
  if (!e || !e.includes('@')) return false;
  const local = e.split('@')[0];
  if (/^(info|support|contact|sales|events|housing|meetings|admin|office|hello|inquiries)$/i.test(local)) {
    return false;
  }
  return true;
}

function looksLikeOrgEmail(email) {
  const e = text(email).toLowerCase();
  if (!e || !e.includes('@')) return false;
  return !looksLikePersonalEmail(e) || /^(info|support|contact|sales|events|housing|meetings)@/i.test(e);
}

/**
 * Assess outreach routes independently of "has a name".
 * A name alone is NOT a route.
 */
export function assessOutreachRoutes(contact = {}, extras = {}) {
  const email = text(contact.email || extras.organizationContactEmail);
  const phone = text(contact.phone);
  const phoneDigits = phone.replace(/\D/g, '');
  // Reject date-shaped "phones" (Chapultepec gold-brief defect: phone="2025-07-17")
  const usablePhone =
    Boolean(phone) && phoneDigits.length >= 7 && !/^\d{4}-\d{2}-\d{2}$/.test(phone);

  const roleLabel = text(contact.title || contact.role);
  const forcedFunctional =
    /^functional(\s+entity)?$/i.test(roleLabel) ||
    NON_PERSON_NAME_RE.test(text(contact.name)) ||
    /[\/|]/.test(text(contact.name)) ||
    /\b(housing\s*team|team\s*travel|travel\s*source)\b/i.test(text(contact.name));

  const personEvidence = verifyNamedPersonWithEvidence({
    name: forcedFunctional ? null : contact.name,
    organization: contact.organization || extras.organizationName,
    role: forcedFunctional ? null : roleLabel,
    evidenceUrl: contact.evidence || contact.evidenceUrl || contact.sourceUrl,
    evidenceUrls: extras.evidenceUrls,
  });
  // Shape-only names are NOT verified persons — reserve VERIFIED for evidence-backed only
  let personhood;
  if (forcedFunctional) {
    personhood = CONTACT_PERSONHOOD.FUNCTIONAL_ORGANIZATION_ROUTE;
  } else if (personEvidence.verified) {
    personhood = CONTACT_PERSONHOOD.VERIFIED_NAMED_PERSON;
  } else if (!personEvidence.shapeOk) {
    personhood = classifyPersonhood(contact.name);
    if (personhood === CONTACT_PERSONHOOD.VERIFIED_NAMED_PERSON) {
      personhood = CONTACT_PERSONHOOD.FUNCTIONAL_ORGANIZATION_ROUTE;
    }
  } else {
    personhood = CONTACT_PERSONHOOD.UNKNOWN_OR_ABSENT; // RESEARCH_CANDIDATE shape
  }
  const kinds = [];

  if (email && looksLikePersonalEmail(email) && personEvidence.shapeOk) {
    kinds.push(OUTREACH_ROUTE_KIND.VERIFIED_DIRECT_EMAIL);
  } else if (email) {
    kinds.push(OUTREACH_ROUTE_KIND.VERIFIED_ORG_EMAIL);
  }

  if (usablePhone) kinds.push(OUTREACH_ROUTE_KIND.VERIFIED_PUBLIC_PHONE);

  const name = text(contact.name);
  if (
    personhood === CONTACT_PERSONHOOD.FUNCTIONAL_ORGANIZATION_ROUTE &&
    (email || usablePhone || /housing|meeting|events|desk|center|bureau|onpeak|procurement/i.test(name))
  ) {
    kinds.push(OUTREACH_ROUTE_KIND.VERIFIED_HOUSING_OR_PROGRAM_ROUTE);
  }

  if (extras.departmentRoute || extras.referralRoute) {
    kinds.push(OUTREACH_ROUTE_KIND.VERIFIED_REFERRAL_OR_DEPARTMENT);
  }

  // Official contact page URL without email/phone still counts as org approach
  if (text(contact.url) || text(extras.contactPageUrl)) {
    kinds.push(OUTREACH_ROUTE_KIND.VERIFIED_REFERRAL_OR_DEPARTMENT);
  }

  const hasUsableRoute = kinds.some((k) =>
    [
      OUTREACH_ROUTE_KIND.VERIFIED_DIRECT_EMAIL,
      OUTREACH_ROUTE_KIND.VERIFIED_PUBLIC_PHONE,
      OUTREACH_ROUTE_KIND.VERIFIED_ORG_EMAIL,
      OUTREACH_ROUTE_KIND.VERIFIED_HOUSING_OR_PROGRAM_ROUTE,
      OUTREACH_ROUTE_KIND.VERIFIED_REFERRAL_OR_DEPARTMENT,
    ].includes(k),
  );

  if (!hasUsableRoute && personEvidence.verified) {
    kinds.push(OUTREACH_ROUTE_KIND.NAMED_PERSON_NO_ROUTE);
  } else if (!hasUsableRoute && personhood === CONTACT_PERSONHOOD.FUNCTIONAL_ORGANIZATION_ROUTE) {
    kinds.push(OUTREACH_ROUTE_KIND.ORGANIZATION_NO_ROUTE);
  } else if (!hasUsableRoute) {
    kinds.push(OUTREACH_ROUTE_KIND.NONE);
  }

  const surfeEligible =
    personEvidence.shapeOk &&
    (personEvidence.verified || personEvidence.sourceOk) &&
    !kinds.includes(OUTREACH_ROUTE_KIND.VERIFIED_DIRECT_EMAIL);

  if (surfeEligible) kinds.push(OUTREACH_ROUTE_KIND.ENRICHMENT_ON_DEMAND);

  return {
    personhood,
    personEvidence,
    isVerifiedNamedPerson: personhood === CONTACT_PERSONHOOD.VERIFIED_NAMED_PERSON,
    isFunctionalOrganizationRoute: personhood === CONTACT_PERSONHOOD.FUNCTIONAL_ORGANIZATION_ROUTE,
    routeKinds: [...new Set(kinds)],
    hasCredibleOutreachRoute: hasUsableRoute,
    hasVerifiedNamedRelevantContact: personhood === CONTACT_PERSONHOOD.VERIFIED_NAMED_PERSON,
    surfeOnDemandEligible: surfeEligible,
    surfeAutoTriggerForbidden: true,
    surfePersistForbidden: true,
  };
}

const GENERIC_THESIS_RE =
  /dimension-level hotel-fit assessment|overall numeric score withheld|sits in the same commercial market|same commercial market \(/i;

/**
 * True when thesis is hotel-specific beyond generic co-location.
 */
export function isHotelSpecificThesis(thesis, { venue, roomDemand, distanceMiles, lodgingCatchment } = {}) {
  const t = text(thesis);
  if (!t) return false;
  if (GENERIC_THESIS_RE.test(t) && t.length < 220) return false;
  if (GENERIC_THESIS_RE.test(t) && !/(overflow|housing|room.?block|stay-to-play|SDCC|convention|metro|corridor|speaker|exhibitor|team lodging|meeting space|satellite)/i.test(t)) {
    return false;
  }
  if (venue || roomDemand || distanceMiles != null || lodgingCatchment) return true;
  return /(overflow|housing|room.?block|stay-to-play|SDCC|convention|metro|NIH|SoccerPlex|waterfront|North Harbor|speaker|exhibitor|visitor|counsel|client)/i.test(
    t,
  );
}

/**
 * Evidence-based gold-brief completeness — NOT portfolio metadata.
 */
export function assessGoldBriefCompleteness(fields = {}) {
  const checks = {
    verifiedOpportunityIdentity: Boolean(text(fields.organizationName) && text(fields.title || fields.id)),
    evidenceBackedActivity: Boolean(
      text(fields.hotelOpportunityThesis) ||
        text(fields.demandStatus) ||
        text(fields.roomDemandStatus) ||
        (fields.evidenceUrls && fields.evidenceUrls.length),
    ),
    relevantTiming: Boolean(text(fields.eventDateDisplay) || /ongoing|recurring|account/i.test(text(fields.title))),
    demandMechanism: Boolean(
      text(fields.roomDemandStatus) ||
        text(fields.roomDemandRationale) ||
        /guestroom|lodging|meeting|housing|room|catering|overflow|stay-to-play|visitor|exhibitor/i.test(
          text(fields.hotelOpportunityThesis) + text(fields.salesActionRecommended),
        ),
    ),
    hotelFitSpecific: isHotelSpecificThesis(fields.hotelOpportunityThesis, {
      venue: fields.eventLocationSummary,
      roomDemand: fields.roomDemandStatus,
      distanceMiles: fields.distanceMiles,
      lodgingCatchment: fields.lodgingCatchmentFit,
    }),
    factsVsInference: Boolean(
      text(fields.demandClassification) ||
        text(fields.roomDemandRationale) ||
        (fields.unknowns && fields.unknowns.length),
    ),
    contactsOrOrgApproach: Boolean(
      fields.hasVerifiedNamedRelevantContact ||
        fields.isFunctionalOrganizationRoute ||
        text(fields.primaryContact?.email) ||
        text(fields.primaryContact?.phone),
    ),
    outreachRoute: Boolean(fields.hasCredibleOutreachRoute),
    salesAction: Boolean(text(fields.salesActionRecommended) || text(fields.conversationOpener)),
    evidenceAndGaps: Boolean(
      (fields.evidenceUrls && fields.evidenceUrls.length) ||
        (fields.unknowns && fields.unknowns.length) ||
        text(fields.refreshRecommendation),
    ),
  };

  const passed = Object.values(checks).filter(Boolean).length;
  const missing = Object.entries(checks)
    .filter(([, v]) => !v)
    .map(([k]) => k);

  let status;
  if (passed >= 9 && checks.hotelFitSpecific && checks.outreachRoute && checks.salesAction) {
    status = GOLD_BRIEF_STATUS.COMPLETE_AND_ACTIONABLE;
  } else if (passed >= 7 && checks.verifiedOpportunityIdentity && checks.demandMechanism && checks.salesAction) {
    status = GOLD_BRIEF_STATUS.ACTIONABLE_WITH_DISCLOSED_GAPS;
  } else if (passed >= 4 && checks.verifiedOpportunityIdentity) {
    status = GOLD_BRIEF_STATUS.RESEARCH_CONTINUATION;
  } else {
    status = GOLD_BRIEF_STATUS.INSUFFICIENT_COMMERCIAL_EVIDENCE;
  }

  return {
    status,
    checks,
    passedCount: passed,
    missing,
    goldBriefComplete: status === GOLD_BRIEF_STATUS.COMPLETE_AND_ACTIONABLE,
    goldBriefActionable:
      status === GOLD_BRIEF_STATUS.COMPLETE_AND_ACTIONABLE ||
      status === GOLD_BRIEF_STATUS.ACTIONABLE_WITH_DISCLOSED_GAPS,
  };
}

/**
 * Opportunity product type — keep theses non-interchangeable.
 */
export function classifyDemandProductType(o = {}) {
  const seg = text(o.segment).toLowerCase();
  const title = text(o.title).toLowerCase();
  const type = text(o.demandType || o.opportunityType).toLowerCase();
  if (seg.includes('private events') || /partnership|preferred lodging/.test(title)) {
    return 'RECURRING_MARKET_DEMAND';
  }
  if (seg.includes('scaling') || /corporate account|scaling/.test(title + type)) {
    return 'CORPORATE_ACCOUNT_DEVELOPMENT';
  }
  if (/project|deployment|construction|extended stay/.test(seg + type + title)) {
    return 'SPECIFIC_CORPORATE_PROJECT';
  }
  if (/soccer|tournament|memorial|showcase|sports/.test(seg + title)) {
    return 'EVENT_GROUP_OPPORTUNITY';
  }
  return 'EVENT_GROUP_OPPORTUNITY';
}

/**
 * Improve generic co-location theses using verified store fields only.
 */
export function buildImprovedHotelThesis(o = {}, hotelLabel = 'this hotel') {
  const existing = text(o.hotelOpportunityThesis);
  const specific = isHotelSpecificThesis(existing, {
    venue: o.eventLocationSummary,
    roomDemand: o.roomDemandStatus,
    distanceMiles: o.distanceMiles,
    lodgingCatchment: o.lodgingCatchmentFit,
  });
  if (specific && !GENERIC_THESIS_RE.test(existing)) return { thesis: existing, improved: false, wasGeneric: false };

  const parts = [];
  const product = classifyDemandProductType(o);
  parts.push(`Demand product: ${product}.`);

  if (o.eventLocationSummary) parts.push(`Activity geography: ${text(o.eventLocationSummary)}.`);
  if (o.eventDateDisplay) parts.push(`Timing: ${text(o.eventDateDisplay)}.`);
  if (o.roomDemandStatus) {
    parts.push(
      `Demand mechanism: ${text(o.roomDemandStatus)}${o.roomDemandRationale ? ` — ${text(o.roomDemandRationale)}` : ''}.`,
    );
  }
  if (o.venueSourcingStatus) {
    parts.push(
      `Venue/housing sourcing: ${text(o.venueSourcingStatus)}${o.venueSourcingRationale ? ` (${text(o.venueSourcingRationale)})` : ''}.`,
    );
  }
  if (o.distanceMiles != null) parts.push(`Approx. distance to venue: ${o.distanceMiles} miles.`);
  if (o.lodgingCatchmentFit) parts.push(`Catchment fit: ${text(o.lodgingCatchmentFit)}.`);

  if (product === 'CORPORATE_ACCOUNT_DEVELOPMENT') {
    parts.push(
      `${hotelLabel} is positioned for corporate account development (visiting staff, client meetings, training nights) — not a confirmed near-term room block. No fabricated project volume.`,
    );
  } else if (product === 'RECURRING_MARKET_DEMAND') {
    parts.push(
      `${hotelLabel} can pursue a preferred-lodging / referral partnership for venue guests — complementary rooms near the non-hotel venue.`,
    );
  } else {
    parts.push(
      `${hotelLabel} is a credible overflow, speaker/staff, or catchment lodging option given verified location and facility profile — primary host status is not assumed.`,
    );
  }

  if (existing && GENERIC_THESIS_RE.test(existing)) {
    parts.push(`Prior generic note retained for provenance: ${existing.slice(0, 180)}`);
  }

  return {
    thesis: parts.join(' '),
    improved: true,
    wasGeneric: GENERIC_THESIS_RE.test(existing) || !existing,
    priorThesis: existing || null,
  };
}
