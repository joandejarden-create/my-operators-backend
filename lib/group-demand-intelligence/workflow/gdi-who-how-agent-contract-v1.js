/**
 * Dedicated WHO/HOW Exa Agent contract (Full E2E V3 Stage 3).
 *
 * Input = completed Stage 2 deep-research dossier (not rediscovery).
 * Output = verified buyer and/or up to two alternative entry points + status.
 *
 * Paid execution requires founder authorization; this module is contract +
 * fixture evaluation only unless a live researcher is injected.
 */

export const GDI_WHO_HOW_AGENT_CONTRACT_V1 = "gdi-who-how-agent-contract-v1";

export const WHO_HOW_STATUS = Object.freeze({
  VERIFIED_DIRECT_BUYER: "VERIFIED_DIRECT_BUYER",
  VERIFIED_BUYER_INFLUENCER: "VERIFIED_BUYER_INFLUENCER",
  VERIFIED_ENTRY_POINTS_2: "VERIFIED_ENTRY_POINTS_2",
  VERIFIED_ENTRY_POINT_1: "VERIFIED_ENTRY_POINT_1",
  ORGANIZATIONAL_ROUTE_ONLY: "ORGANIZATIONAL_ROUTE_ONLY",
  CONTACT_RESEARCH_REQUIRED: "CONTACT_RESEARCH_REQUIRED",
  NO_CREDIBLE_CONTACT_IDENTIFIED: "NO_CREDIBLE_CONTACT_IDENTIFIED",
});

export const WHO_HOW_PERSON_ROLE = Object.freeze({
  DIRECT_BUYER: "DIRECT_BUYER",
  BUYER_INFLUENCER: "BUYER_INFLUENCER",
  /** Alias: INFLUENCER (founder field name) */
  INFLUENCER: "INFLUENCER",
  ENTRY_POINT: "ENTRY_POINT",
  ENTRY_POINT_1: "ENTRY_POINT_1",
  ENTRY_POINT_2: "ENTRY_POINT_2",
  UNRELATED: "UNRELATED",
  UNKNOWN: "UNKNOWN",
});

/** Founder-facing contactRole vocabulary (Part A). */
export const CONTACT_ROLE = Object.freeze({
  DIRECT_BUYER: "DIRECT_BUYER",
  INFLUENCER: "INFLUENCER",
  ENTRY_POINT_1: "ENTRY_POINT_1",
  ENTRY_POINT_2: "ENTRY_POINT_2",
});

export const ENTRY_POINT_RANK = Object.freeze({
  ENTRY_POINT_1: "ENTRY_POINT_1",
  ENTRY_POINT_2: "ENTRY_POINT_2",
  NOT_FOUND: "NOT_FOUND",
});

/** Reject generic "why showing" justifications for alternative entry points. */
export const GENERIC_WHY_SHOWING_RE =
  /^(senior executive|industry professional|potential contact|key contact|decision maker|stakeholder|leader|executive|important person|relevant contact|good contact|possible contact)\.?$/i;

/** Prompt family id for Exa Agent adapters (must match allowlists when wired). */
export const WHO_HOW_AGENT_FAMILY = "WHO_HOW_RESOLUTION_V3";

export const WHO_HOW_PROMPT_CONTRACT_ID =
  "gdi-who-how-resolution-agent-contract-v1.1";

/**
 * Structured schema the Agent must return (and fixtures must satisfy).
 */
export const WHO_HOW_AGENT_OUTPUT_SCHEMA = Object.freeze({
  version: GDI_WHO_HOW_AGENT_CONTRACT_V1,
  requiredTopLevel: Object.freeze([
    "opportunityId",
    "organizationName",
    "whoHowStatus",
    "directBuyer",
    "entryPoints",
    "evidence",
    "researchContinuation",
  ]),
  personFields: Object.freeze([
    "contactRole",
    "fullName",
    "currentTitle",
    "organization",
    "identityVerified",
    "buyerAuthorityVerified",
    "relationshipToOpportunity",
    "whyWeAreShowingThisContact",
    "recommendedSalesApproach",
    "verifiedLinkedIn",
    "verifiedBusinessEmail",
    "verifiedBusinessPhone",
    "outreachChannelType",
    "sourceURLs",
    "lastVerifiedAt",
    "confidence",
    "missingEvidence",
  ]),
  entryPointRequiredFields: Object.freeze([
    "fullName",
    "currentTitle",
    "organization",
    "identityVerified",
    "relationshipToOpportunity",
    "whyWeAreShowingThisContact",
    "recommendedSalesApproach",
    "sourceURLs",
  ]),
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Validate opportunity-specific rationale for alternative entry points.
 * Must explain connection, routing value, and what sales should ask.
 */
export function evaluateWhyWeAreShowingThisContact(why, opts = {}) {
  const raw = text(why);
  const scan = text(opts.combinedText || why);
  const reasons = [];
  if (!raw) {
    reasons.push("MISSING_WHY_WE_ARE_SHOWING_THIS_CONTACT");
    return { ok: false, reasons, value: null };
  }
  if (GENERIC_WHY_SHOWING_RE.test(raw) || raw.length < 24) {
    reasons.push("GENERIC_OR_TOO_SHORT_WHY_SHOWING");
  }
  // Prefer answers that touch opportunity connection + routing + ask
  const touchesConnection =
    /meeting|event|housing|lodging|travel|tournament|conference|project|account|program|coordinator|director|planner|ops|operations|procurement|education/i.test(
      scan,
    );
  const touchesRouting =
    /route|introduc|connect|reach|ask|refer|housing|block|RFP|partner|sales|overflow|site.?selection/i.test(
      scan,
    );
  if (opts.requireRich === true) {
    if (!touchesConnection) reasons.push("WHY_MISSING_OPPORTUNITY_CONNECTION");
    if (!touchesRouting) reasons.push("WHY_MISSING_SALES_ROUTING_ASK");
  }
  return {
    ok: reasons.length === 0,
    reasons,
    value: raw,
    touchesConnection,
    touchesRouting,
  };
}

function looksLikePersonName(name) {
  const n = text(name);
  if (!n || n.length < 3) return false;
  if (/^(unknown|n\/a|tbd|none|null)$/i.test(n)) return false;
  if (!/[A-Za-z]/.test(n)) return false;
  // Require at least first + last token. Abbreviated surnames ("Suzanne K.")
  // still look like a person candidate but are NOT fully verified identities.
  const parts = n.split(/\s+/).filter(Boolean);
  return parts.length >= 2;
}

/** Full professional identity — rejects "Lily Y." / "Suzanne K." style surnames. */
function looksLikeFullyVerifiedPersonName(name) {
  const n = text(name);
  if (!looksLikePersonName(n)) return false;
  if (/\b[A-Za-z]\.$/.test(n)) return false;
  const parts = n.split(/\s+/).filter(Boolean);
  const last = parts[parts.length - 1];
  if (/^[A-Za-z]\.?$/.test(last)) return false;
  return true;
}

function isPersonLinkedInUrl(url) {
  return /^https?:\/\/([a-z0-9-]+\.)?linkedin\.com\/in\/[A-Za-z0-9_%-]+\/?/i.test(
    text(url),
  );
}

/** Prefer verifiedLinkedIn; fall back to person LinkedIn in sourceUrl. */
function resolveWhoHowLinkedIn(person = {}) {
  const direct = text(
    person.verifiedLinkedIn || person.linkedinUrl || person.linkedInUrl,
  );
  if (isPersonLinkedInUrl(direct)) return direct.replace(/\/$/, "");
  const src = text(person.sourceUrl || person.personSourceUrl);
  if (isPersonLinkedInUrl(src)) return src.replace(/\/$/, "");
  return null;
}

function isHttpUrl(url) {
  return /^https?:\/\//i.test(text(url));
}

/**
 * Build Agent input package from a Stage-2 dossier (no rediscovery fields).
 */
export function buildWhoHowAgentInputFromDossier(dossier = {}, opts = {}) {
  const opp = dossier.opportunity || dossier.after?.opportunity || dossier;
  const fqo = dossier.fqo || null;
  const competitive = dossier.competitive || null;
  return {
    version: GDI_WHO_HOW_AGENT_CONTRACT_V1,
    agentFamily: WHO_HOW_AGENT_FAMILY,
    promptContractId: WHO_HOW_PROMPT_CONTRACT_ID,
    mode: opts.mode || "DryRun",
    hotelId: opts.hotelId || opp.hotelId || null,
    opportunityId:
      opts.opportunityId ||
      opp.candidateId ||
      opp.opportunityId ||
      dossier.opportunityId ||
      null,
    organizationName: text(opp.organizationName || opp.organization),
    title: text(opp.title),
    segmentId: text(opp.segmentId || dossier.segmentId),
    demandSummary: text(
      opp.demandSummary || opp.hotelFitRationale || opp.title,
    ).slice(0, 1200),
    hotelFitRationale: text(opp.hotelFitRationale).slice(0, 800),
    competitiveClass: competitive?.classification || null,
    competitorHotel: competitive?.competitorHotel || null,
    priorNamedPerson: text(
      fqo?.gates?.NAMED_COMMERCIAL_PERSON?.name ||
        opp.primaryContact?.name ||
        opp.contactName,
    ),
    priorRole: text(
      fqo?.gates?.NAMED_COMMERCIAL_PERSON?.role ||
        opp.primaryContact?.role ||
        opp.primaryContact?.title,
    ),
    priorChannel:
      fqo?.gates?.PERSON_SPECIFIC_CHANNEL?.channels?.[0]?.value ||
      opp.primaryContact?.linkedinUrl ||
      null,
    evidenceUrls: Array.isArray(opp.evidenceUrls)
      ? opp.evidenceUrls.filter(isHttpUrl).slice(0, 12)
      : [],
    instructions: Object.freeze({
      doNotRediscoverDemand: true,
      findDirectBuyerFirst: true,
      ifNoVerifiedBuyerFindTwoEntryPoints: true,
      neverInventPeopleOrEmails: true,
      neverInferBuyerFromSeniorityAlone: true,
      separateIdentityFromBuyingAuthority: true,
    }),
  };
}

/**
 * Validate / normalize a WHO/HOW Agent (or fixture) output object.
 */
export function normalizeWhoHowAgentOutput(raw = {}, opts = {}) {
  const errors = [];
  const opportunityId = text(raw.opportunityId || opts.opportunityId);
  if (!opportunityId) errors.push("MISSING_OPPORTUNITY_ID");

  const directBuyer = normalizePerson(raw.directBuyer || raw.buyer, {
    roleHint: WHO_HOW_PERSON_ROLE.DIRECT_BUYER,
  });
  const influencer = normalizePerson(raw.buyerInfluencer || raw.influencer, {
    roleHint: WHO_HOW_PERSON_ROLE.BUYER_INFLUENCER,
  });

  const entryRaw = Array.isArray(raw.entryPoints)
    ? raw.entryPoints
    : [raw.entryPoint1, raw.entryPoint2].filter(Boolean);

  const entryPoints = [];
  for (let i = 0; i < Math.min(2, entryRaw.length); i++) {
    const p = normalizePerson(entryRaw[i], {
      roleHint: WHO_HOW_PERSON_ROLE.ENTRY_POINT,
      rank:
        i === 0
          ? ENTRY_POINT_RANK.ENTRY_POINT_1
          : ENTRY_POINT_RANK.ENTRY_POINT_2,
    });
    if (p && p.identityVerified) entryPoints.push(p);
  }

  // Pad NOT_FOUND slots for auditability when fewer than 2 verified.
  while (entryPoints.length < 2 && raw.requireEntryPointSlots !== false) {
    const rank =
      entryPoints.length === 0
        ? ENTRY_POINT_RANK.ENTRY_POINT_1
        : ENTRY_POINT_RANK.ENTRY_POINT_2;
    if (
      entryPoints.some((e) => e.rank === rank) ||
      (raw.entryPointsMissing || []).includes(rank)
    ) {
      break;
    }
    entryPoints.push({
      rank,
      status: ENTRY_POINT_RANK.NOT_FOUND,
      identityVerified: false,
      hotelBuyingAuthorityVerified: false,
      fullName: null,
      note: "NOT_FOUND",
    });
    break; // only document one NOT_FOUND sentinel when none found; see derive status
  }

  const verifiedEntries = entryPoints.filter(
    (e) => e.identityVerified === true && e.status !== ENTRY_POINT_RANK.NOT_FOUND,
  );

  let whoHowStatus = text(raw.whoHowStatus).toUpperCase() || null;
  if (!whoHowStatus || !WHO_HOW_STATUS[whoHowStatus]) {
    whoHowStatus = deriveWhoHowStatus({
      directBuyer,
      influencer,
      verifiedEntries,
      organizationalRouteOnly: raw.organizationalRouteOnly === true,
    });
  }

  // Guard: never promote entry point to direct buyer without authority flag.
  if (
    directBuyer?.identityVerified &&
    directBuyer.hotelBuyingAuthorityVerified !== true
  ) {
    // Downgrade to influencer or entry unless explicit authority.
    if (directBuyer.personRole === WHO_HOW_PERSON_ROLE.DIRECT_BUYER) {
      directBuyer.personRole = WHO_HOW_PERSON_ROLE.ENTRY_POINT;
      errors.push("DIRECT_BUYER_AUTHORITY_NOT_VERIFIED_DOWNGRADED");
    }
  }

  for (const ep of verifiedEntries) {
    if (ep.hotelBuyingAuthorityVerified === true) {
      errors.push("ENTRY_POINT_MUST_NOT_CLAIM_BUYING_AUTHORITY");
      ep.hotelBuyingAuthorityVerified = false;
    }
  }

  const researchContinuation =
    whoHowStatus === WHO_HOW_STATUS.NO_CREDIBLE_CONTACT_IDENTIFIED ||
    whoHowStatus === WHO_HOW_STATUS.CONTACT_RESEARCH_REQUIRED ||
    (whoHowStatus === WHO_HOW_STATUS.VERIFIED_ENTRY_POINT_1 &&
      verifiedEntries.length < 2);

  return {
    version: GDI_WHO_HOW_AGENT_CONTRACT_V1,
    opportunityId,
    organizationName: text(raw.organizationName || opts.organizationName),
    whoHowStatus,
    directBuyer:
      directBuyer?.identityVerified &&
      directBuyer.hotelBuyingAuthorityVerified === true
        ? { ...directBuyer, personRole: WHO_HOW_PERSON_ROLE.DIRECT_BUYER }
        : directBuyer?.identityVerified
          ? {
              ...directBuyer,
              personRole:
                directBuyer.personRole === WHO_HOW_PERSON_ROLE.BUYER_INFLUENCER
                  ? WHO_HOW_PERSON_ROLE.BUYER_INFLUENCER
                  : WHO_HOW_PERSON_ROLE.ENTRY_POINT,
              hotelBuyingAuthorityVerified: false,
            }
          : null,
    buyerInfluencer:
      influencer?.identityVerified &&
      influencer.hotelBuyingAuthorityVerified !== true
        ? { ...influencer, personRole: WHO_HOW_PERSON_ROLE.BUYER_INFLUENCER }
        : influencer?.identityVerified
          ? influencer
          : null,
    entryPoints: normalizeEntryPointPair(verifiedEntries, raw),
    evidence: Array.isArray(raw.evidence) ? raw.evidence : [],
    researchContinuation: researchContinuation === true,
    researchContinuationReason: researchContinuation
      ? text(raw.researchContinuationReason) || whoHowStatus
      : null,
    errors,
    at: new Date().toISOString(),
  };
}

function normalizeEntryPointPair(verifiedEntries, raw) {
  const out = [];
  const v0 = verifiedEntries[0] || null;
  const v1 = verifiedEntries[1] || null;
  if (v0) {
    out.push({ ...v0, rank: ENTRY_POINT_RANK.ENTRY_POINT_1 });
  } else {
    out.push({
      rank: ENTRY_POINT_RANK.ENTRY_POINT_1,
      status: ENTRY_POINT_RANK.NOT_FOUND,
      identityVerified: false,
      hotelBuyingAuthorityVerified: false,
      fullName: null,
      note: "NOT_FOUND",
    });
  }
  if (v1) {
    out.push({ ...v1, rank: ENTRY_POINT_RANK.ENTRY_POINT_2 });
  } else if (raw.skipSecondNotFound !== true) {
    out.push({
      rank: ENTRY_POINT_RANK.ENTRY_POINT_2,
      status: ENTRY_POINT_RANK.NOT_FOUND,
      identityVerified: false,
      hotelBuyingAuthorityVerified: false,
      fullName: null,
      note: "NOT_FOUND",
    });
  }
  return out;
}

function normalizePerson(p, { roleHint, rank } = {}) {
  if (!p || typeof p !== "object") return null;
  const fullName = text(p.fullName || p.name);
  const currentTitle = text(p.currentTitle || p.title || p.role || p.jobTitle);
  const organization = text(p.organization || p.organizationName);
  const sourceUrl = text(
    p.sourceUrl ||
      p.verificationSourceUrl ||
      (Array.isArray(p.sourceURLs) ? p.sourceURLs[0] : null),
  );
  const sourceURLs = Array.isArray(p.sourceURLs)
    ? p.sourceURLs.map(text).filter(isHttpUrl)
    : sourceUrl && isHttpUrl(sourceUrl)
      ? [sourceUrl]
      : [];
  const whyRaw =
    p.whyWeAreShowingThisContact ||
    p.whyCredibleRoute ||
    p.whyRelevant ||
    p.whyThisContact ||
    "";
  const isEntryHint =
    roleHint === WHO_HOW_PERSON_ROLE.ENTRY_POINT ||
    rank === ENTRY_POINT_RANK.ENTRY_POINT_1 ||
    rank === ENTRY_POINT_RANK.ENTRY_POINT_2;
  // Combine why + relationship so opportunity connection can live in either field.
  const whyCombined = [
    text(whyRaw),
    text(p.relationshipToOpportunity),
    text(p.recommendedSalesApproach),
  ]
    .filter(Boolean)
    .join(" — ");
  const whyEval = evaluateWhyWeAreShowingThisContact(
    whyRaw || whyCombined,
    {
      requireRich: isEntryHint,
      // When requireRich, also accept combined text for connection/routing signals
      combinedText: whyCombined,
    },
  );

  const unrelated =
    p.personRole === WHO_HOW_PERSON_ROLE.UNRELATED ||
    p.unrelatedToHotelSourcing === true ||
    p.commerciallyRelevant === false;

  const isEntry =
    roleHint === WHO_HOW_PERSON_ROLE.ENTRY_POINT ||
    rank === ENTRY_POINT_RANK.ENTRY_POINT_1 ||
    rank === ENTRY_POINT_RANK.ENTRY_POINT_2;

  // Entry points require non-generic whyWeAreShowingThisContact — never invent a filler.
  // Abbreviated surnames must never auto-pass as fully verified professional identity.
  const nameFullyVerified = looksLikeFullyVerifiedPersonName(fullName);
  let identityVerified =
    p.identityVerified === true &&
    nameFullyVerified &&
    (isHttpUrl(sourceUrl) || sourceURLs.length > 0) &&
    !unrelated;
  const missingEvidence = [];
  if (!looksLikePersonName(fullName)) missingEvidence.push("fullName");
  else if (!nameFullyVerified) missingEvidence.push("ABBREVIATED_OR_INCOMPLETE_SURNAME");
  if (!currentTitle) missingEvidence.push("currentTitle");
  if (!organization) missingEvidence.push("organization");
  if (!sourceURLs.length && !isHttpUrl(sourceUrl)) missingEvidence.push("sourceURLs");
  if (isEntry && !whyEval.ok) {
    identityVerified = false;
    missingEvidence.push(...whyEval.reasons);
  }
  if (isEntry && !text(p.relationshipToOpportunity)) {
    identityVerified = false;
    missingEvidence.push("relationshipToOpportunity");
  }
  if (isEntry && text(p.recommendedSalesApproach).length < 8) {
    identityVerified = false;
    missingEvidence.push("recommendedSalesApproach");
  }
  if (!nameFullyVerified && looksLikePersonName(fullName)) {
    identityVerified = false;
  }

  const buyerAuthorityVerified =
    p.buyerAuthorityVerified === true ||
    p.hotelBuyingAuthorityVerified === true;

  let contactRole = text(p.contactRole).toUpperCase() || null;
  if (!contactRole) {
    if (roleHint === WHO_HOW_PERSON_ROLE.DIRECT_BUYER) {
      contactRole = CONTACT_ROLE.DIRECT_BUYER;
    } else if (
      roleHint === WHO_HOW_PERSON_ROLE.BUYER_INFLUENCER ||
      roleHint === WHO_HOW_PERSON_ROLE.INFLUENCER
    ) {
      contactRole = CONTACT_ROLE.INFLUENCER;
    } else if (rank === ENTRY_POINT_RANK.ENTRY_POINT_1) {
      contactRole = CONTACT_ROLE.ENTRY_POINT_1;
    } else if (rank === ENTRY_POINT_RANK.ENTRY_POINT_2) {
      contactRole = CONTACT_ROLE.ENTRY_POINT_2;
    } else if (isEntry) {
      contactRole = CONTACT_ROLE.ENTRY_POINT_1;
    }
  }

  const liUrl = text(
    p.verifiedLinkedIn || p.linkedinUrl || p.linkedInUrl,
  );
  const verifiedLinkedIn = liUrl;
  const verifiedBusinessEmail = text(
    p.verifiedBusinessEmail || p.email || p.businessEmail,
  );
  const verifiedBusinessPhone = text(
    p.verifiedBusinessPhone || p.phone || p.directPhone,
  );
  let outreachChannelType = text(p.outreachChannelType).toUpperCase() || null;
  if (!outreachChannelType) {
    if (verifiedBusinessEmail) outreachChannelType = "PROFESSIONAL_EMAIL";
    else if (verifiedBusinessPhone) outreachChannelType = "PROFESSIONAL_DIRECT_PHONE";
    else if (verifiedLinkedIn && /linkedin\.com\/in\//i.test(verifiedLinkedIn)) {
      outreachChannelType = "PERSON_LINKEDIN";
    }
  }

  return {
    contactRole,
    fullName: fullName || null,
    currentTitle: currentTitle || null,
    title: currentTitle || null,
    organization: organization || null,
    responsibility: text(p.responsibility) || null,
    relationshipToOpportunity: text(p.relationshipToOpportunity) || null,
    whyWeAreShowingThisContact: whyEval.value,
    whyCredibleRoute: whyEval.value,
    recommendedSalesApproach: text(p.recommendedSalesApproach) || null,
    buyerAuthorityVerified,
    hotelBuyingAuthorityVerified: buyerAuthorityVerified,
    identityVerified,
    identityCompleteness: nameFullyVerified
      ? "FULL"
      : looksLikePersonName(fullName)
        ? "ABBREVIATED_SURNAME"
        : "INCOMPLETE",
    // Public profile URL field (not an OAuth client secret).
    verifiedLinkedIn: liUrl || null,
    verifiedBusinessEmail: verifiedBusinessEmail || null,
    verifiedBusinessPhone: verifiedBusinessPhone || null,
    outreachChannelType,
    sourceUrl: sourceUrl || sourceURLs[0] || null,
    sourceURLs,
    lastVerifiedAt: text(p.lastVerifiedAt) || new Date().toISOString(),
    confidence: text(p.confidence) || null,
    missingEvidence: [
      ...missingEvidence,
      ...(Array.isArray(p.missingEvidence) ? p.missingEvidence : []),
    ],
    personRole: unrelated
      ? WHO_HOW_PERSON_ROLE.UNRELATED
      : text(p.personRole) || roleHint || WHO_HOW_PERSON_ROLE.UNKNOWN,
    rank: rank || p.rank || null,
    status: identityVerified ? "VERIFIED" : "INVALID",
    commerciallyRelevant: unrelated ? false : p.commerciallyRelevant !== false,
    whyEval,
  };
}

export function deriveWhoHowStatus({
  directBuyer,
  influencer,
  verifiedEntries = [],
  organizationalRouteOnly = false,
} = {}) {
  if (
    directBuyer?.identityVerified &&
    directBuyer.hotelBuyingAuthorityVerified === true
  ) {
    return WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER;
  }
  if (influencer?.identityVerified) {
    return WHO_HOW_STATUS.VERIFIED_BUYER_INFLUENCER;
  }
  const n = verifiedEntries.filter((e) => e.identityVerified).length;
  if (n >= 2) return WHO_HOW_STATUS.VERIFIED_ENTRY_POINTS_2;
  if (n === 1) return WHO_HOW_STATUS.VERIFIED_ENTRY_POINT_1;
  if (organizationalRouteOnly) return WHO_HOW_STATUS.ORGANIZATIONAL_ROUTE_ONLY;
  if (n === 0) return WHO_HOW_STATUS.NO_CREDIBLE_CONTACT_IDENTIFIED;
  return WHO_HOW_STATUS.CONTACT_RESEARCH_REQUIRED;
}

/**
 * Map WHO/HOW result onto opportunity contact fields for FQO / drawer.
 * Does not invent channels.
 */
export function applyWhoHowResultToOpportunity(opportunity = {}, whoHow = {}) {
  const status = whoHow.whoHowStatus;
  const next = {
    ...opportunity,
    whoHowStatus: status,
    whoHowResolution: whoHow,
    whoHowResolvedAt: whoHow.at || new Date().toISOString(),
  };

  if (status === WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER && whoHow.directBuyer) {
    const b = whoHow.directBuyer;
    const linkedinUrl = resolveWhoHowLinkedIn(b);
    next.primaryContact = {
      ...(next.primaryContact || {}),
      name: b.fullName,
      role: b.currentTitle || b.title,
      title: b.currentTitle || b.title,
      organization: b.organization,
      whyThisContact:
        b.whyWeAreShowingThisContact ||
        b.whyCredibleRoute ||
        b.relationshipToOpportunity,
      whyWeAreShowingThisContact: b.whyWeAreShowingThisContact || null,
      recommendedContactAction: b.recommendedSalesApproach,
      personSourceUrl: b.sourceUrl,
      sourceUrl: b.sourceUrl || linkedinUrl || null,
      linkedinUrl,
      email: b.verifiedBusinessEmail || null,
      phone: b.verifiedBusinessPhone || null,
      contactRole: CONTACT_ROLE.DIRECT_BUYER,
      buyerVsEntry: "DIRECT_BUYER",
      buyerVsEntryLabel: "Direct lodging decision-maker",
      hotelBuyingAuthorityVerified: true,
      identityVerified: true,
      contactClass: "DIRECT_BUYER",
    };
    next.buyerVsEntry = "DIRECT_BUYER";
    next.buyerVsEntryLabel = "Direct lodging decision-maker";
  } else if (
    status === WHO_HOW_STATUS.VERIFIED_BUYER_INFLUENCER &&
    whoHow.buyerInfluencer
  ) {
    const b = whoHow.buyerInfluencer;
    const linkedinUrl = resolveWhoHowLinkedIn(b);
    next.primaryContact = {
      ...(next.primaryContact || {}),
      name: b.fullName,
      role: b.currentTitle || b.title,
      title: b.currentTitle || b.title,
      organization: b.organization,
      whyThisContact:
        b.whyWeAreShowingThisContact ||
        b.whyCredibleRoute ||
        b.relationshipToOpportunity,
      whyWeAreShowingThisContact: b.whyWeAreShowingThisContact || null,
      recommendedContactAction: b.recommendedSalesApproach,
      personSourceUrl: b.sourceUrl,
      sourceUrl: b.sourceUrl || linkedinUrl || null,
      linkedinUrl,
      email: b.verifiedBusinessEmail || null,
      phone: b.verifiedBusinessPhone || null,
      contactRole: CONTACT_ROLE.INFLUENCER,
      buyerVsEntry: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      buyerVsEntryLabel: "Buying influencer (authority not verified)",
      hotelBuyingAuthorityVerified: false,
      identityVerified: true,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    };
  } else {
    const ep1 = (whoHow.entryPoints || []).find(
      (e) =>
        e.rank === ENTRY_POINT_RANK.ENTRY_POINT_1 && e.identityVerified === true,
    );
    const ep2 = (whoHow.entryPoints || []).find(
      (e) =>
        e.rank === ENTRY_POINT_RANK.ENTRY_POINT_2 && e.identityVerified === true,
    );
    if (ep1) {
      next.primaryContact = {
        ...(next.primaryContact || {}),
        name: ep1.fullName,
        role: ep1.currentTitle || ep1.title,
        title: ep1.currentTitle || ep1.title,
        organization: ep1.organization,
        whyThisContact: ep1.whyWeAreShowingThisContact,
        whyWeAreShowingThisContact: ep1.whyWeAreShowingThisContact,
        recommendedContactAction: ep1.recommendedSalesApproach,
        personSourceUrl: ep1.sourceUrl,
        sourceUrl: ep1.sourceUrl || resolveWhoHowLinkedIn(ep1) || null,
        linkedinUrl: resolveWhoHowLinkedIn(ep1),
        email: ep1.verifiedBusinessEmail || null,
        phone: ep1.verifiedBusinessPhone || null,
        contactRole: CONTACT_ROLE.ENTRY_POINT_1,
        buyerVsEntry: "RELEVANT_COMMERCIAL_ENTRY_POINT",
        buyerVsEntryLabel: "Verified entry point",
        hotelBuyingAuthorityVerified: false,
        identityVerified: true,
        contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
        entryPointRank: ENTRY_POINT_RANK.ENTRY_POINT_1,
      };
      next.buyerVsEntry = "RELEVANT_COMMERCIAL_ENTRY_POINT";
      next.buyerVsEntryLabel = "Verified entry point";
    }
    if (ep2) {
      next.secondaryNamedContact = {
        name: ep2.fullName,
        role: ep2.currentTitle || ep2.title,
        title: ep2.currentTitle || ep2.title,
        organization: ep2.organization,
        whyThisContact: ep2.whyWeAreShowingThisContact,
        whyWeAreShowingThisContact: ep2.whyWeAreShowingThisContact,
        recommendedContactAction: ep2.recommendedSalesApproach,
        personSourceUrl: ep2.sourceUrl,
        sourceUrl: ep2.sourceUrl || resolveWhoHowLinkedIn(ep2) || null,
        linkedinUrl: resolveWhoHowLinkedIn(ep2),
        email: ep2.verifiedBusinessEmail || null,
        phone: ep2.verifiedBusinessPhone || null,
        contactRole: CONTACT_ROLE.ENTRY_POINT_2,
        buyerVsEntry: "RELEVANT_COMMERCIAL_ENTRY_POINT",
        buyerVsEntryLabel: "Verified entry point",
        hotelBuyingAuthorityVerified: false,
        identityVerified: true,
        contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
        entryPointRank: ENTRY_POINT_RANK.ENTRY_POINT_2,
      };
    }
  }

  next.whoHowDrawer = buildWhoHowDrawerPresentation(whoHow);
  return next;
}

/**
 * Drawer-facing structure:
 * Primary Lodging Decision-Maker + Alternative Entry Point 1/2.
 * whyWeAreShowingThisContact is surface-visible (not admin-only).
 */
export function buildWhoHowDrawerPresentation(whoHow = {}) {
  const status = whoHow.whoHowStatus;
  const buyerVerified =
    status === WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER && whoHow.directBuyer;

  function channelsFromPerson(p = {}) {
    const ch = Array.isArray(p.channels) ? [...p.channels] : [];
    if (p.verifiedBusinessEmail) {
      ch.push({
        channelType: "PROFESSIONAL_EMAIL",
        value: p.verifiedBusinessEmail,
      });
    }
    if (p.verifiedBusinessPhone) {
      ch.push({
        channelType: "PROFESSIONAL_DIRECT_PHONE",
        value: p.verifiedBusinessPhone,
      });
    }
    if (p.verifiedLinkedIn) {
      ch.push({ channelType: "PERSON_LINKEDIN", value: p.verifiedLinkedIn });
    }
    return ch;
  }

  const primary = buyerVerified
    ? {
        kind: "PRIMARY_LODGING_DECISION_MAKER",
        heading: "Primary Lodging Decision-Maker",
        verified: true,
        fullName: whoHow.directBuyer.fullName,
        title: whoHow.directBuyer.currentTitle || whoHow.directBuyer.title,
        organization: whoHow.directBuyer.organization,
        buyerRelationship: "Verified hotel / lodging decision-maker",
        verificationStatus: "IDENTITY_AND_AUTHORITY_VERIFIED",
        hotelBuyingAuthorityVerified: true,
        whyShowing:
          whoHow.directBuyer.whyWeAreShowingThisContact ||
          whoHow.directBuyer.whyCredibleRoute,
        whyWeAreShowingThisContact:
          whoHow.directBuyer.whyWeAreShowingThisContact || null,
        recommendedSalesApproach: whoHow.directBuyer.recommendedSalesApproach,
        sourceUrl: whoHow.directBuyer.sourceUrl,
        channels: channelsFromPerson(whoHow.directBuyer),
      }
    : {
        kind: "PRIMARY_LODGING_DECISION_MAKER",
        heading: "Primary Lodging Decision-Maker",
        verified: false,
        headline: "Hotel Decision-Maker Not Yet Verified",
        hotelBuyingAuthorityVerified: false,
      };

  function mapEntry(ep, label, heading) {
    if (!ep || ep.status === ENTRY_POINT_RANK.NOT_FOUND || !ep.identityVerified) {
      return {
        kind: label,
        heading,
        verified: false,
        badge: null,
        note: "NOT_FOUND",
      };
    }
    return {
      kind: label,
      heading,
      verified: true,
      badge: "VERIFIED ENTRY POINT",
      fullName: ep.fullName,
      title: ep.currentTitle || ep.title,
      organization: ep.organization,
      relationshipToOpportunity: ep.relationshipToOpportunity,
      whyShowing: ep.whyWeAreShowingThisContact,
      whyWeAreShowingThisContact: ep.whyWeAreShowingThisContact,
      recommendedSalesApproach: ep.recommendedSalesApproach,
      sourceUrl: ep.sourceUrl,
      hotelBuyingAuthorityVerified: false,
      authorityDisclaimer:
        "Identity verified — hotel-buying authority is NOT verified.",
      channels: channelsFromPerson(ep),
      contactRole: ep.contactRole || label,
    };
  }

  const entries = whoHow.entryPoints || [];
  const alt1 = mapEntry(
    entries.find((e) => e.rank === ENTRY_POINT_RANK.ENTRY_POINT_1),
    "ALTERNATIVE_ENTRY_POINT_1",
    "Alternative Entry Point 1",
  );
  const alt2 = mapEntry(
    entries.find((e) => e.rank === ENTRY_POINT_RANK.ENTRY_POINT_2),
    "ALTERNATIVE_ENTRY_POINT_2",
    "Alternative Entry Point 2",
  );

  return {
    version: GDI_WHO_HOW_AGENT_CONTRACT_V1,
    whoHowStatus: status,
    lastVerifiedAt: whoHow.at || null,
    primaryDecisionMaker: primary,
    alternativeContact1: alt1,
    alternativeContact2: alt2,
    showAlternativesWhenBuyerMissing: !buyerVerified,
  };
}

/**
 * Dry-run / fixture executor — never calls paid providers.
 */
export function executeWhoHowResolutionFixture(dossier, fixtureOutput, opts = {}) {
  const input = buildWhoHowAgentInputFromDossier(dossier, opts);
  const normalized = normalizeWhoHowAgentOutput(fixtureOutput, {
    opportunityId: input.opportunityId,
    organizationName: input.organizationName,
  });
  const opportunity = applyWhoHowResultToOpportunity(
    dossier.opportunity || dossier,
    normalized,
  );
  return {
    version: GDI_WHO_HOW_AGENT_CONTRACT_V1,
    mode: "fixture",
    paid: false,
    input,
    whoHow: normalized,
    opportunity,
    reused: opts.reused === true,
  };
}
