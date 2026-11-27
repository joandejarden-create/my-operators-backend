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
  ENTRY_POINT: "ENTRY_POINT",
  UNRELATED: "UNRELATED",
  UNKNOWN: "UNKNOWN",
});

export const ENTRY_POINT_RANK = Object.freeze({
  ENTRY_POINT_1: "ENTRY_POINT_1",
  ENTRY_POINT_2: "ENTRY_POINT_2",
  NOT_FOUND: "NOT_FOUND",
});

/** Prompt family id for Exa Agent adapters (must match allowlists when wired). */
export const WHO_HOW_AGENT_FAMILY = "WHO_HOW_RESOLUTION_V3";

export const WHO_HOW_PROMPT_CONTRACT_ID =
  "gdi-who-how-resolution-agent-contract-v1";

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
    "fullName",
    "title",
    "organization",
    "responsibility",
    "relationshipToOpportunity",
    "whyCredibleRoute",
    "hotelBuyingAuthorityVerified",
    "identityVerified",
    "sourceUrl",
    "confidence",
    "recommendedSalesApproach",
  ]),
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function looksLikePersonName(name) {
  const n = text(name);
  if (!n || n.length < 3) return false;
  if (/^(unknown|n\/a|tbd|none|null)$/i.test(n)) return false;
  if (!/[A-Za-z]/.test(n)) return false;
  // Require at least first + last token (allow abbreviated surname "Suzanne K.")
  const parts = n.split(/\s+/).filter(Boolean);
  return parts.length >= 2;
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
  const identityVerified =
    p.identityVerified === true &&
    looksLikePersonName(fullName) &&
    isHttpUrl(p.sourceUrl || p.verificationSourceUrl);
  const unrelated =
    p.personRole === WHO_HOW_PERSON_ROLE.UNRELATED ||
    p.unrelatedToHotelSourcing === true ||
    p.commerciallyRelevant === false;

  return {
    fullName: fullName || null,
    title: text(p.title || p.role || p.jobTitle) || null,
    organization: text(p.organization || p.organizationName) || null,
    responsibility: text(p.responsibility) || null,
    relationshipToOpportunity: text(p.relationshipToOpportunity) || null,
    whyCredibleRoute: text(p.whyCredibleRoute || p.whyRelevant) || null,
    recommendedSalesApproach: text(p.recommendedSalesApproach) || null,
    hotelBuyingAuthorityVerified: p.hotelBuyingAuthorityVerified === true,
    identityVerified: identityVerified && !unrelated,
    sourceUrl: text(p.sourceUrl || p.verificationSourceUrl) || null,
    confidence: text(p.confidence) || null,
    personRole: unrelated
      ? WHO_HOW_PERSON_ROLE.UNRELATED
      : text(p.personRole) || roleHint || WHO_HOW_PERSON_ROLE.UNKNOWN,
    rank: rank || p.rank || null,
    status: identityVerified && !unrelated ? "VERIFIED" : "INVALID",
    commerciallyRelevant: unrelated ? false : p.commerciallyRelevant !== false,
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
    next.primaryContact = {
      ...(next.primaryContact || {}),
      name: b.fullName,
      role: b.title,
      title: b.title,
      organization: b.organization,
      whyThisContact: b.whyCredibleRoute || b.relationshipToOpportunity,
      recommendedContactAction: b.recommendedSalesApproach,
      personSourceUrl: b.sourceUrl,
      buyerVsEntry: "DIRECT_BUYER",
      buyerVsEntryLabel: "Direct buyer",
      hotelBuyingAuthorityVerified: true,
      identityVerified: true,
      contactClass: "DIRECT_BUYER",
    };
    next.buyerVsEntry = "DIRECT_BUYER";
    next.buyerVsEntryLabel = "Direct buyer";
  } else if (
    status === WHO_HOW_STATUS.VERIFIED_BUYER_INFLUENCER &&
    whoHow.buyerInfluencer
  ) {
    const b = whoHow.buyerInfluencer;
    next.primaryContact = {
      ...(next.primaryContact || {}),
      name: b.fullName,
      role: b.title,
      title: b.title,
      organization: b.organization,
      whyThisContact: b.whyCredibleRoute || b.relationshipToOpportunity,
      recommendedContactAction: b.recommendedSalesApproach,
      personSourceUrl: b.sourceUrl,
      buyerVsEntry: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      buyerVsEntryLabel: "Buyer influencer (authority not verified)",
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
        role: ep1.title,
        title: ep1.title,
        organization: ep1.organization,
        whyThisContact: ep1.whyCredibleRoute || ep1.relationshipToOpportunity,
        recommendedContactAction: ep1.recommendedSalesApproach,
        personSourceUrl: ep1.sourceUrl,
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
        role: ep2.title,
        title: ep2.title,
        organization: ep2.organization,
        whyThisContact: ep2.whyCredibleRoute || ep2.relationshipToOpportunity,
        recommendedContactAction: ep2.recommendedSalesApproach,
        personSourceUrl: ep2.sourceUrl,
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
 * Drawer-facing structure: PRIMARY DECISION-MAKER + ALTERNATIVE CONTACT 1/2.
 */
export function buildWhoHowDrawerPresentation(whoHow = {}) {
  const status = whoHow.whoHowStatus;
  const buyerVerified =
    status === WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER && whoHow.directBuyer;

  const primary = buyerVerified
    ? {
        kind: "PRIMARY_DECISION_MAKER",
        verified: true,
        fullName: whoHow.directBuyer.fullName,
        title: whoHow.directBuyer.title,
        organization: whoHow.directBuyer.organization,
        buyerRelationship: "Verified hotel / lodging decision-maker",
        verificationStatus: "IDENTITY_AND_AUTHORITY_VERIFIED",
        hotelBuyingAuthorityVerified: true,
        whyShowing: whoHow.directBuyer.whyCredibleRoute,
        recommendedSalesApproach: whoHow.directBuyer.recommendedSalesApproach,
        sourceUrl: whoHow.directBuyer.sourceUrl,
        channels: whoHow.directBuyer.channels || [],
      }
    : {
        kind: "PRIMARY_DECISION_MAKER",
        verified: false,
        headline: "Hotel Decision-Maker Not Yet Verified",
        hotelBuyingAuthorityVerified: false,
      };

  function mapEntry(ep, label) {
    if (!ep || ep.status === ENTRY_POINT_RANK.NOT_FOUND || !ep.identityVerified) {
      return {
        kind: label,
        verified: false,
        badge: null,
        note: "NOT_FOUND",
      };
    }
    return {
      kind: label,
      verified: true,
      badge: "VERIFIED ENTRY POINT",
      fullName: ep.fullName,
      title: ep.title,
      organization: ep.organization,
      relationshipToOpportunity: ep.relationshipToOpportunity,
      whyShowing: ep.whyCredibleRoute,
      recommendedSalesApproach: ep.recommendedSalesApproach,
      sourceUrl: ep.sourceUrl,
      hotelBuyingAuthorityVerified: false,
      authorityDisclaimer:
        "Identity verified — hotel-buying authority is NOT verified.",
      channels: ep.channels || [],
    };
  }

  const entries = whoHow.entryPoints || [];
  const alt1 = mapEntry(
    entries.find((e) => e.rank === ENTRY_POINT_RANK.ENTRY_POINT_1),
    "ALTERNATIVE_CONTACT_1",
  );
  const alt2 = mapEntry(
    entries.find((e) => e.rank === ENTRY_POINT_RANK.ENTRY_POINT_2),
    "ALTERNATIVE_CONTACT_2",
  );

  // When influencer is primary reachability path, still show entry slots.
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
