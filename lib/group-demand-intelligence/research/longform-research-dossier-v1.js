/**
 * Long-form research dossier builder for GDI autonomous investigations.
 * Emits cited sections only from accepted evidence. Separate from customer publication gates.
 */

export const LONGFORM_DOSSIER_VERSION = "gdi-longform-research-dossier-v1";

function text(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function cite(claim) {
  if (!claim) return null;
  return {
    statement: text(claim.statement || claim.excerpt),
    url: claim.url || null,
    evidenceId: claim.evidenceId || null,
    accepted: claim.accepted !== false,
  };
}

function section(title, body, citations = []) {
  const accepted = citations.filter((item) => item && item.accepted !== false && text(item.statement));
  return {
    title,
    body: text(body),
    citations: accepted,
    unsupportedWithoutCitation: accepted.length === 0 && Boolean(text(body)),
  };
}

/**
 * Build a dossier from verified/accepted claims only.
 * Rejected or invented claims must appear under unsupportedHypotheses / unresolvedGaps.
 */
export function buildLongformResearchDossier({
  hotel = {},
  organization = {},
  objective = "",
  acceptedClaims = [],
  rejectedClaims = [],
  unresolvedGaps = [],
  recommendedSalesActions = [],
  researchDisposition = null,
  suitability = null,
  researchCompleteness = null,
  stoppingTrigger = null,
  verifiedFindings = null,
  supportedInferences = null,
  unverifiedClaims = null,
  contradictions = null,
  retrievalProvenance = null,
} = {}) {
  const accepted = (acceptedClaims || []).filter((item) => item && item.accepted !== false && text(item.statement || item.excerpt));
  const byTopic = (pattern) => accepted.filter((item) => pattern.test(`${item.statement || ""} ${item.excerpt || ""} ${item.topic || ""}`));

  const services = byTopic(/hotel sourcing|contract negotiation|incentive|programme|program|accommodation|delegate|production/i);
  const programmes = byTopic(/incentive|seven-day|five-night|programme|program|capital experience|rosewood|savoy/i);
  const placement = byTopic(/rosewood|hotel|accommodation|base|sourcing|contract/i);
  const contacts = byTopic(/@|contact|phone|020|decision|sourcing/i);
  const relationships = byTopic(/rosewood|savoy|hilton|bankside|relativity|meeting space|nobu/i);

  const nobuNamed = accepted.some((item) => /nobu/i.test(`${item.statement || ""} ${item.excerpt || ""}`));
  const openRfp = accepted.some((item) => /\brfp\b|request for proposal|accepting proposals/i.test(`${item.statement || ""} ${item.excerpt || ""}`));
  const roomBlock = accepted.some((item) => /room block|bedroom commitment|contracted \d+ rooms/i.test(`${item.statement || ""} ${item.excerpt || ""}`));

  const executive = [
    `${organization.name || "The organization"} is assessed for luxury group / incentive accommodation demand relevant to ${hotel.name || "the target hotel"}.`,
    suitability?.summary || (hotel.fit === "STRONG_TARGET_HOTEL_FIT"
      ? "Geographic and positioning fit supports account development, not an active hotel award."
      : "Hotel-specific fit remains to be established from cited evidence."),
    openRfp ? "Accepted evidence includes open-sourcing language." : "No accepted evidence establishes an open RFP.",
    roomBlock ? "Accepted evidence includes a bedroom commitment." : "No accepted evidence establishes a bedroom commitment for the target hotel.",
    nobuNamed ? "The target hotel is named in accepted evidence." : "The target hotel is not named in accepted evidence; do not invent a Nobu relationship.",
  ].join(" ");

  const dossier = {
    version: LONGFORM_DOSSIER_VERSION,
    generatedAt: new Date().toISOString(),
    publicationGate: "INTERNAL_RESEARCH_DOSSIER_ONLY",
    customerFacingPublicationEligible: false,
    prospectPublicationEligibilityChanged: false,
    objective: text(objective),
    hotel: {
      hotelId: hotel.hotelId || null,
      name: hotel.name || null,
      market: hotel.market || null,
      fit: hotel.fit || null,
    },
    organization: {
      name: organization.name || null,
      pathway: organization.pathway || null,
    },
    researchDisposition: researchDisposition || null,
    sections: {
      executiveCommercialAssessment: section("Executive commercial assessment", executive, accepted.slice(0, 6).map(cite)),
      organizationAndServices: section(
        "Organization and services",
        services.length
          ? services.map((item) => text(item.statement || item.excerpt)).join(" ")
          : "No accepted service description is held yet.",
        services.map(cite),
      ),
      documentedProgramsAndClientTypes: section(
        "Documented programs and client types",
        programmes.length
          ? programmes.map((item) => text(item.statement || item.excerpt)).join(" ")
          : "No accepted programme evidence is held yet.",
        programmes.map(cite),
      ),
      hotelPlacementAndAccommodationEvidence: section(
        "Hotel-placement and accommodation evidence",
        placement.length
          ? placement.map((item) => text(item.statement || item.excerpt)).join(" ")
          : "No accepted hotel-placement evidence is held yet.",
        placement.map(cite),
      ),
      relevantHotelAndClientRelationships: section(
        "Relevant hotel and client relationships",
        relationships.length
          ? relationships.map((item) => text(item.statement || item.excerpt)).join(" ")
          : "No accepted relationship evidence is held yet.",
        relationships.map(cite),
      ),
      contactsAndSourcingResponsibility: section(
        "Contacts and sourcing responsibility",
        contacts.length
          ? contacts.map((item) => text(item.statement || item.excerpt)).join(" ")
          : "No accepted contact route is held yet.",
        contacts.map(cite),
      ),
      suitabilityForNobu: section(
        "Suitability for Nobu",
        suitability?.detail
          || (hotel.fit === "STRONG_TARGET_HOTEL_FIT"
            ? "Central-London luxury incentive demand is commercially relevant to Nobu Portman Square. Relevance is not a booking, RFP, or named relationship."
            : "Suitability is not established."),
        accepted.filter((item) => /london|marylebone|luxury|incentive|nobu/i.test(`${item.statement || ""} ${item.excerpt || ""}`)).map(cite),
      ),
      competitiveConsiderations: section(
        "Competitive considerations",
        relationships.some((item) => /rosewood|savoy|hilton/i.test(`${item.statement || ""} ${item.excerpt || ""}`))
          ? "Accepted evidence names other London hotels in programme roles. Those are competitive or historical placements, not proof Nobu is excluded or selected."
          : "No competitive hotel placements are cited yet.",
        relationships.map(cite),
      ),
      futureCycleOrAccountDevelopment: section(
        "Future-cycle or account-development opportunities",
        researchDisposition === "BUYER_ENGAGEMENT_REQUIRED"
          ? "Public evidence supports an introduction. Buyer engagement is more appropriate than further public searching for a live room requirement."
          : researchDisposition === "SCHEDULED_REVISIT"
            ? "A later official announcement is the next useful public check."
            : "Account-development posture depends on unresolved public gaps listed below.",
        [],
      ),
      unsupportedHypothesesAndUnresolvedGaps: {
        title: "Unsupported hypotheses and unresolved gaps",
        unsupportedHypotheses: (rejectedClaims || []).map((item) => ({
          claim: text(item.claim || item.statement),
          reason: text(item.reason || "not_in_cited_source"),
        })),
        unresolvedGaps: (unresolvedGaps || []).map((item) => text(item)).filter(Boolean),
        citations: [],
      },
      recommendedSalesActions: {
        title: "Recommended sales actions",
        actions: (recommendedSalesActions || []).map((item) => text(item)).filter(Boolean),
        citations: contacts.map(cite),
      },
    },
    evidenceLedger: accepted.map((item, index) => ({
      evidenceId: item.evidenceId || `ev_${index + 1}`,
      statement: text(item.statement || item.excerpt),
      url: item.url || null,
      topic: item.topic || null,
      accepted: true,
    })),
    evidenceClassification: {
      verifiedFindings: (verifiedFindings || accepted).map((item) => text(item.statement || item.excerpt || item)).filter(Boolean),
      supportedInferences: (supportedInferences || []).map((item) => text(item)).filter(Boolean),
      unverifiedClaims: (unverifiedClaims || (rejectedClaims || []).map((item) => item.claim || item.statement)).map((item) => text(item)).filter(Boolean),
      contradictions: (contradictions || []).map((item) => text(item)).filter(Boolean),
      unansweredQuestions: (unresolvedGaps || []).map((item) => text(item)).filter(Boolean),
    },
    researchCompleteness: researchCompleteness || researchDisposition || null,
    stoppingTrigger: stoppingTrigger || null,
    retrievalProvenance: retrievalProvenance || null,
    salesActionSummary: {
      concise: (recommendedSalesActions || []).map((item) => text(item)).filter(Boolean)[0] || null,
      actions: (recommendedSalesActions || []).map((item) => text(item)).filter(Boolean),
      internalOnly: true,
      customerFacingFormatUnchanged: true,
    },
    provenanceRules: {
      materialClaimsRequireCitation: true,
      rejectedClaimsCannotEnterBody: true,
      inventingNobuRelationshipProhibited: true,
      inventingOpenRfpProhibited: true,
      separateFromCustomerPublicationGate: true,
    },
  };

  const bodyCitations = Object.values(dossier.sections)
    .flatMap((item) => item.citations || [])
    .filter(Boolean);
  const unresolvedCitation = bodyCitations.find((item) => {
    if (!text(item.statement)) return true;
    return !accepted.some((claim) => text(claim.statement || claim.excerpt) === text(item.statement));
  });

  return {
    dossier,
    citationAudit: {
      ok: !unresolvedCitation,
      unresolvedCitation: unresolvedCitation || null,
      acceptedClaimCount: accepted.length,
    },
  };
}

export function assertDossierCitationsResolve(dossierResult = {}) {
  const { dossier, citationAudit } = dossierResult;
  if (!dossier) return { ok: false, reason: "dossier_missing" };
  if (citationAudit?.ok !== true) return { ok: false, reason: "citation_unresolved", detail: citationAudit?.unresolvedCitation };
  const ledgerUrls = new Set((dossier.evidenceLedger || []).map((item) => item.url).filter(Boolean));
  for (const section of Object.values(dossier.sections || {})) {
    for (const citation of section.citations || []) {
      if (citation.url && !ledgerUrls.has(citation.url)) {
        return { ok: false, reason: "citation_url_not_in_ledger", url: citation.url };
      }
      const matched = (dossier.evidenceLedger || []).some((item) => item.statement === citation.statement);
      if (!matched) return { ok: false, reason: "citation_statement_not_in_ledger", statement: citation.statement };
    }
  }
  if (dossier.customerFacingPublicationEligible === true || dossier.prospectPublicationEligibilityChanged === true) {
    return { ok: false, reason: "publication_gate_touched" };
  }
  return { ok: true };
}
