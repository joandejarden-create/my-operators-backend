/**
 * Exa Agent → claim/source evidence → Supporting evidence drawer mapping V1.
 *
 * Every material claim must retain a provider URL through:
 *   Exa research → validation → CIB → canonical DTO → Supporting evidence section.
 *
 * Does not invent URLs. Does not expand the ten-section drawer structure.
 */
import {
  isPressPublisherNonCompanyRoute,
  isPressPublisherHost,
  isPressReleaseEvidenceUrl,
} from "../../press-publisher-entity-v1.js";

export const EXA_AGENT_SOURCE_EVIDENCE_V1 =
  "gdi-scaling-exa-agent-source-evidence-v1";

export const EVIDENCE_GROUP_IDS = Object.freeze([
  "company_identity_and_activity",
  "corporate_growth_and_operations",
  "hotel_demand_evidence",
  "hotel_specific_fit_and_geography",
  "buyer_contact_evidence",
  "commercial_sourcing",
]);

const EVIDENCE_GROUP_LABELS = Object.freeze({
  company_identity_and_activity: "Company identity and activity",
  corporate_growth_and_operations: "Corporate growth and operations",
  hotel_demand_evidence: "Hotel-demand evidence",
  hotel_specific_fit_and_geography: "Hotel-specific fit and geography",
  buyer_contact_evidence: "Buyer/contact evidence",
  commercial_sourcing: "Commercial sourcing",
});

const DRAWER_SECTION_TO_GROUP = Object.freeze({
  "1": "company_identity_and_activity",
  opportunity_overview: "company_identity_and_activity",
  companyIdentity: "company_identity_and_activity",
  "2": "corporate_growth_and_operations",
  what_is_happening: "corporate_growth_and_operations",
  activityAndChronology: "corporate_growth_and_operations",
  verifiedFacts: "corporate_growth_and_operations",
  "3": "hotel_demand_evidence",
  potential_business: "hotel_demand_evidence",
  hotelDemandEvidence: "hotel_demand_evidence",
  hotelDemandHypotheses: "hotel_demand_evidence",
  "4": "hotel_specific_fit_and_geography",
  why_this_hotel: "hotel_specific_fit_and_geography",
  hotelSpecificFit: "hotel_specific_fit_and_geography",
  "5": "corporate_growth_and_operations",
  why_act_now: "corporate_growth_and_operations",
  commercialTiming: "corporate_growth_and_operations",
  "6": "corporate_growth_and_operations",
  facts_and_uncertainties: "corporate_growth_and_operations",
  "7": "buyer_contact_evidence",
  who_to_contact: "buyer_contact_evidence",
  buyerCandidates: "buyer_contact_evidence",
  sourcingAccessibility: "buyer_contact_evidence",
  "8": "commercial_sourcing",
  recommended_next_step: "commercial_sourcing",
  recommendedCommercialAction: "commercial_sourcing",
  "9": "commercial_sourcing",
  supporting_evidence: "commercial_sourcing",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function stableSourceId(url, index = 0) {
  const u = text(url).toLowerCase().replace(/\/$/, "");
  let h = 0;
  for (let i = 0; i < u.length; i += 1) {
    h = (h * 31 + u.charCodeAt(i)) >>> 0;
  }
  return `src_${(h >>> 0).toString(16)}_${index}`;
}

function stableClaimId(claim, index = 0) {
  const c = text(claim).toLowerCase().slice(0, 120);
  let h = 0;
  for (let i = 0; i < c.length; i += 1) {
    h = (h * 31 + c.charCodeAt(i)) >>> 0;
  }
  return `clm_${(h >>> 0).toString(16)}_${index}`;
}

export function isValidHttpUrl(url = "") {
  try {
    const u = new URL(String(url));
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

export function isInternalOrCredentialUrl(url = "") {
  const u = text(url).toLowerCase();
  if (!u) return true;
  if (/api[_-]?key|token=|secret=|password=|authorization=/i.test(u)) return true;
  if (/localhost|127\.0\.0\.1|api\.exa\.ai|exa\.ai\/agent/i.test(u)) return true;
  return false;
}

export function shouldDisplayEvidenceUrl(url = "") {
  const u = text(url);
  if (!u || !isValidHttpUrl(u)) return false;
  if (isInternalOrCredentialUrl(u)) return false;
  if (isPressPublisherNonCompanyRoute(u)) return false;
  return true;
}

function classifySourceType(url, meta = {}) {
  if (meta.sourceType) return text(meta.sourceType);
  const u = text(url).toLowerCase();
  if (isPressPublisherHost(u) && isPressReleaseEvidenceUrl(u)) {
    return "press_release_distribution";
  }
  if (/linkedin\.com\/in\//i.test(u)) return "professional_profile";
  if (/\/jobs?|smartrecruiters|greenhouse|lever\.co|workday/i.test(u)) {
    return "job_posting";
  }
  if (/\.pdf($|\?)/i.test(u)) return "company_filing_or_pdf";
  if (/rolandberger\.com|official|fiestamericana|marriott|hilton/i.test(u)) {
    return "official_company_or_property_page";
  }
  return "web_source";
}

function inferGroupFromSections(drawerSections = []) {
  for (const s of drawerSections) {
    const key = text(s);
    if (DRAWER_SECTION_TO_GROUP[key]) return DRAWER_SECTION_TO_GROUP[key];
  }
  return "company_identity_and_activity";
}

/**
 * Normalize claim rows from Exa structured output into the Stage C claim contract.
 */
export function normalizeClaimsFromDossier(dossier = {}, opts = {}) {
  const company =
    text(dossier.companyIdentity?.legalOrTradingName) ||
    text(opts.companyName) ||
    null;
  const hotel =
    text(opts.hotelName) || text(opts.hotelDisplayName) || null;
  const rows = Array.isArray(dossier.claimToSourceMap)
    ? dossier.claimToSourceMap
    : [];
  const claims = [];
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i] || {};
    const claimText = text(row.claim);
    if (!claimText) continue;
    const sourceUrls = (Array.isArray(row.sourceUrls) ? row.sourceUrls : [])
      .map((u) => text(u))
      .filter((u) => shouldDisplayEvidenceUrl(u));
    const classification = text(row.classification || row.factClass) || "fact";
    const factClass = ["fact", "inference", "hypothesis"].includes(
      classification.toLowerCase(),
    )
      ? classification.toLowerCase()
      : sourceUrls.length
        ? "fact"
        : "hypothesis";
    claims.push({
      claimId: text(row.claimId) || stableClaimId(claimText, i),
      claimText,
      claimCategory: text(row.claimCategory) || inferGroupFromSections(row.drawerSections),
      targetCompany: company,
      relevantHotel: hotel,
      factClass,
      supportingSourceIds: [],
      supportingSourceUrls: sourceUrls,
      originalActivityDate: row.originalActivityDate || null,
      verificationStatus: sourceUrls.length
        ? text(row.verificationStatus) || "source_attributed"
        : "unsupported",
      materialLimitations: text(row.materialLimitations) || null,
      drawerSections: Array.isArray(row.drawerSections)
        ? row.drawerSections.map((s) => text(s)).filter(Boolean)
        : [],
    });
  }
  return claims;
}

/**
 * Normalize supportingSources + claim map into durable source records.
 */
export function normalizeSourcesFromDossier(dossier = {}, opts = {}) {
  const retrievedAt =
    text(opts.retrievedAt) || text(opts.verificationDate) || new Date().toISOString();
  const byUrl = new Map();
  const supporting = Array.isArray(dossier.supportingSources)
    ? dossier.supportingSources
    : [];

  for (let i = 0; i < supporting.length; i += 1) {
    const s = supporting[i] || {};
    const url = text(s.url);
    if (!shouldDisplayEvidenceUrl(url)) continue;
    const key = url.toLowerCase().replace(/\/$/, "");
    if (byUrl.has(key)) continue;
    byUrl.set(key, {
      sourceId: text(s.sourceId) || stableSourceId(url, i),
      title: text(s.title) || null,
      url,
      publisher: text(s.entityAttributed || s.publisher) || null,
      publicationDate: s.publishedDate || null,
      retrievalVerificationDate: retrievedAt,
      sourceType: classifySourceType(url, s),
      relevantCompanyEntity: text(s.entityAttributed) || null,
      claimIdsSupported: [],
      evidenceExcerptOrLocator: text(s.excerpt || s.relevance) || null,
      sourceRelevance: text(s.relevance) || null,
      verificationStatus: "provider_returned",
      establishes: text(s.relevance) || null,
      evidenceGroup: "company_identity_and_activity",
    });
  }

  const claims = normalizeClaimsFromDossier(dossier, opts);
  for (const claim of claims) {
    for (const url of claim.supportingSourceUrls) {
      const key = url.toLowerCase().replace(/\/$/, "");
      let src = byUrl.get(key);
      if (!src) {
        if (!shouldDisplayEvidenceUrl(url)) continue;
        src = {
          sourceId: stableSourceId(url, byUrl.size),
          title: null,
          url,
          publisher: null,
          publicationDate: null,
          retrievalVerificationDate: retrievedAt,
          sourceType: classifySourceType(url),
          relevantCompanyEntity: claim.targetCompany,
          claimIdsSupported: [],
          evidenceExcerptOrLocator: null,
          sourceRelevance: null,
          verificationStatus: "provider_returned",
          establishes: null,
          evidenceGroup: claim.claimCategory,
        };
        byUrl.set(key, src);
      }
      if (!src.claimIdsSupported.includes(claim.claimId)) {
        src.claimIdsSupported.push(claim.claimId);
      }
      const establishesBits = [src.establishes, claim.claimText].filter(Boolean);
      src.establishes = [...new Set(establishesBits)].join(" · ");
      src.evidenceGroup =
        DRAWER_SECTION_TO_GROUP[claim.drawerSections[0]] ||
        claim.claimCategory ||
        src.evidenceGroup;
      claim.supportingSourceIds.push(src.sourceId);
    }
  }

  return {
    version: EXA_AGENT_SOURCE_EVIDENCE_V1,
    claims,
    sources: [...byUrl.values()],
  };
}

/**
 * Build Supporting evidence drawer payload (still section id = supporting_evidence).
 * One row per unique URL; each lists what the source establishes.
 */
export function buildSupportingEvidenceDrawerItems(evidencePack = {}, opts = {}) {
  const sources = Array.isArray(evidencePack.sources) ? evidencePack.sources : [];
  const claims = Array.isArray(evidencePack.claims) ? evidencePack.claims : [];
  const claimById = new Map(claims.map((c) => [c.claimId, c]));

  const items = [];
  for (const s of sources) {
    if (!shouldDisplayEvidenceUrl(s.url)) continue;
    const supportedClaims = (s.claimIdsSupported || [])
      .map((id) => claimById.get(id))
      .filter(Boolean);
    const establishes =
      text(s.establishes) ||
      supportedClaims.map((c) => c.claimText).join(" · ") ||
      text(s.sourceRelevance) ||
      "Supporting evidence for research conclusions";
    const groupId =
      s.evidenceGroup && EVIDENCE_GROUP_LABELS[s.evidenceGroup]
        ? s.evidenceGroup
        : "company_identity_and_activity";
    items.push({
      sourceId: s.sourceId,
      title: text(s.title) || text(s.url),
      publisher: text(s.publisher) || text(s.relevantCompanyEntity) || null,
      publicationDate: s.publicationDate || null,
      retrievalVerificationDate: s.retrievalVerificationDate || null,
      url: s.url,
      establishes,
      sourceType: s.sourceType || null,
      evidenceGroup: groupId,
      evidenceGroupLabel: EVIDENCE_GROUP_LABELS[groupId],
      claimIdsSupported: [...(s.claimIdsSupported || [])],
      verificationStatus: s.verificationStatus || "provider_returned",
      historicallyVerified: opts.historicallyVerified === true,
      currentlyReachable: opts.currentlyReachable ?? null,
    });
  }

  // Group for presentation without adding top-level drawer sections
  const groups = EVIDENCE_GROUP_IDS.map((id) => ({
    id,
    label: EVIDENCE_GROUP_LABELS[id],
    sources: items.filter((it) => it.evidenceGroup === id),
  })).filter((g) => g.sources.length > 0);

  return {
    version: EXA_AGENT_SOURCE_EVIDENCE_V1,
    sources: items,
    groups,
    unsupportedMaterialClaims: claims.filter(
      (c) =>
        c.verificationStatus === "unsupported" ||
        !(c.supportingSourceIds || []).length,
    ),
  };
}

/**
 * Full pipeline: dossier → evidence pack → opportunity source fields + drawer items.
 */
export function mapDossierEvidenceToOpportunityFields(opportunity = {}, dossier = {}, opts = {}) {
  const pack = normalizeSourcesFromDossier(dossier, {
    companyName: opportunity.organizationName,
    hotelName: opts.hotelName || opts.hotelDisplayName,
    retrievedAt: opts.retrievedAt,
    verificationDate: opts.verificationDate,
  });
  const drawer = buildSupportingEvidenceDrawerItems(pack, opts);

  const existing = Array.isArray(opportunity.sources) ? opportunity.sources : [];
  const merged = [...existing];
  const seen = new Set(
    existing.map((s) => text(s.url || s).toLowerCase().replace(/\/$/, "")).filter(Boolean),
  );
  for (const s of drawer.sources) {
    const key = s.url.toLowerCase().replace(/\/$/, "");
    if (seen.has(key)) {
      // Enrich existing row in place when possible
      const idx = merged.findIndex(
        (x) => text(x.url || x).toLowerCase().replace(/\/$/, "") === key,
      );
      if (idx >= 0 && typeof merged[idx] === "object") {
        merged[idx] = {
          ...merged[idx],
          title: merged[idx].title || s.title,
          publisher: merged[idx].publisher || s.publisher,
          publishedDate: merged[idx].publishedDate || s.publicationDate,
          establishes: s.establishes,
          sourceId: s.sourceId,
          evidenceGroup: s.evidenceGroup,
          evidenceGroupLabel: s.evidenceGroupLabel,
          claimIdsSupported: s.claimIdsSupported,
          sourceType: s.sourceType,
          verificationStatus: s.verificationStatus,
        };
      }
      continue;
    }
    seen.add(key);
    merged.push({
      url: s.url,
      title: s.title,
      publisher: s.publisher,
      publishedDate: s.publicationDate,
      establishes: s.establishes,
      sourceId: s.sourceId,
      evidenceGroup: s.evidenceGroup,
      evidenceGroupLabel: s.evidenceGroupLabel,
      claimIdsSupported: s.claimIdsSupported,
      sourceType: s.sourceType,
      verificationStatus: s.verificationStatus,
      provider: "exa_agent",
    });
  }

  return {
    opportunity: {
      ...opportunity,
      sources: merged,
      exaAgentEvidencePack: pack,
      supportingEvidenceDrawer: drawer,
      exaAgentClaimToSourceMap: pack.claims,
    },
    evidencePack: pack,
    supportingEvidenceDrawer: drawer,
    mappingVersion: EXA_AGENT_SOURCE_EVIDENCE_V1,
  };
}

/**
 * Certify that material claims have source URLs before brief acceptance.
 */
export function certifyClaimSourceLinkage(evidencePack = {}) {
  const claims = Array.isArray(evidencePack.claims) ? evidencePack.claims : [];
  const sources = Array.isArray(evidencePack.sources) ? evidencePack.sources : [];
  const sourceIds = new Set(sources.map((s) => s.sourceId));
  const issues = [];
  for (const c of claims) {
    if (!(c.supportingSourceIds || []).length && !(c.supportingSourceUrls || []).length) {
      issues.push({
        severity: "P0",
        code: "claim_without_source",
        claimId: c.claimId,
        message: c.claimText,
      });
      continue;
    }
    for (const id of c.supportingSourceIds || []) {
      if (!sourceIds.has(id)) {
        issues.push({
          severity: "P0",
          code: "claim_source_id_missing",
          claimId: c.claimId,
          sourceId: id,
        });
      }
    }
  }
  const p0 = issues.filter((i) => i.severity === "P0");
  return {
    ok: p0.length === 0 || claims.length === 0,
    certifiable: p0.length === 0 && claims.length > 0 && sources.length > 0,
    issues,
    claimCount: claims.length,
    sourceCount: sources.length,
    version: EXA_AGENT_SOURCE_EVIDENCE_V1,
  };
}
