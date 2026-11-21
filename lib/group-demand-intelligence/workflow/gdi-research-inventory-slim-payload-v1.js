/**
 * GDI Slice 2A — research-inventory slim commercial payload V1.
 *
 * Allowlist-based commercial payload for GDI Hotel Research Opportunities.
 * Preserves identity, evidence URLs, contacts, fit, continuation, run economics,
 * and commercial brief inputs. Strips sales/hotel-private/publication/telemetry.
 *
 * Fail-closed on oversized payloads after bounded evidence caps.
 * Never silently drops contacts or evidence URLs that existed on the source.
 *
 * $0 / dry-run safe. No schema changes. No production writes.
 */

import {
  REQUIRED_COMMERCIAL_PAYLOAD_KEYS,
  extractCommercialPayloadSlice,
  HOTEL_PRIVATE_PAYLOAD_KEYS,
  stripHotelPrivateKeys,
} from "./gdi-commercial-payload-durability-v1.js";
import { SALES_OWNED_PAYLOAD_KEYS } from "./gdi-airtable-field-ownership-v1.js";

export const GDI_RESEARCH_INVENTORY_SLIM_PAYLOAD_V1 =
  "gdi-research-inventory-slim-payload-v1";

/** Keep aligned with RESEARCH_PAYLOAD_MAX_CHARS in research inventory store. */
export const SLIM_RESEARCH_PAYLOAD_MAX_CHARS = 95_000;

export const PUBLICATION_STRIP_KEYS = Object.freeze([
  "gdiPublicationPackageId",
  "gdiPublicationFingerprint",
  "gdiCustomerShareReady",
  "gdiPublishedAt",
  "customerPublicationReady",
  "publicationOccurred",
  "publicationPackageId",
  "shareToken",
]);

export const TELEMETRY_STRIP_KEYS = Object.freeze([
  "orsRaw",
  "orsTranscript",
  "firecrawlRaw",
  "exaRawHits",
  "retrievalLog",
  "plannerTrace",
  "ledgerEntries",
  "deepResearchEnrichment",
  "supervisorV2",
  "exaAgentRouting",
  "supportingEvidenceDrawer",
  "rawProviderPacks",
]);

const MAX_EVIDENCE = 40;
const MAX_STATEMENT = 280;

function text(v) {
  if (v == null) return "";
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
    return String(v).replace(/\s+/g, " ").trim();
  }
  return "";
}

function slimContact(c) {
  if (!c || typeof c !== "object") return null;
  const name = text(c.name || c.fullName || c.contactName);
  if (!name) return null;
  const phoneExtensions = Array.isArray(c.phoneExtensions)
    ? c.phoneExtensions.map((e) => text(e)).filter(Boolean).slice(0, 8)
    : text(c.phoneExtension)
      ? [text(c.phoneExtension)]
      : [];
  const linkedInUrl = text(c.linkedinUrl || c.linkedInUrl || "");
  const out = {
    name,
    role: text(c.role || c.title) || null,
    authorityBasis: text(c.authorityBasis || c.authority) || null,
    email: text(c.email || c.workEmail) || null,
    workEmail: text(c.workEmail || c.email) || null,
    phone: text(c.phone || c.directPhone || c.phoneNumber) || null,
    directPhone: text(c.directPhone || c.phone) || null,
    emailOwnership: c.emailOwnership || null,
    phoneOwnership: c.phoneOwnership || null,
    phoneType: text(c.phoneType) || null,
    phoneLabel: text(c.phoneLabel) || null,
    phoneKind: text(c.phoneKind) || null,
    sourceUrl: text(c.sourceUrl || c.channelSourceUrl || c.url) || null,
    channelSourceUrl: text(c.channelSourceUrl || c.sourceUrl) || null,
    verifiedAt: text(c.verifiedAt || c.verificationDate) || null,
    contactDataOrigin: text(c.contactDataOrigin) || null,
  };
  if (phoneExtensions.length) {
    out.phoneExtensions = phoneExtensions;
    out.phoneExtension = phoneExtensions[0];
  }
  if (/linkedin\.com\/in\//i.test(linkedInUrl)) {
    out.linkedinUrl = linkedInUrl;
    out.profileUrl = linkedInUrl;
  }
  if (text(c.liProfileMatchStatus)) {
    out.liProfileMatchStatus = text(c.liProfileMatchStatus);
  }
  // Contact Identity Preflight — public employer / readiness (never Surfe-derived)
  const employerName = text(c.employerName || c.organization || c.companyName);
  const employerDomain = text(c.employerDomain || c.companyDomain)
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0];
  if (employerName) out.employerName = employerName;
  if (employerDomain) out.employerDomain = employerDomain;
  if (text(c.identityReadiness)) out.identityReadiness = text(c.identityReadiness);
  if (c.linkedinTrustedForSurfe === true) out.linkedinTrustedForSurfe = true;
  if (text(c.identityVerifiedAt)) out.identityVerifiedAt = text(c.identityVerifiedAt);
  // Drop empty nulls
  for (const [k, v] of Object.entries(out)) {
    if (v == null || v === "") delete out[k];
  }
  return out;
}

function evidenceUrl(e) {
  if (typeof e === "string") return text(e);
  if (!e || typeof e !== "object") return "";
  return text(e.url || e.sourceUrl || e.href);
}

function slimEvidenceEntry(e) {
  if (typeof e === "string") {
    const url = text(e);
    return url ? { url } : null;
  }
  if (!e || typeof e !== "object") return null;
  const url = evidenceUrl(e);
  if (!url) return null;
  const statement = text(e.statement || e.claim || e.title || e.label).slice(
    0,
    MAX_STATEMENT,
  );
  return {
    url,
    statement: statement || undefined,
    provenance: text(e.provenance || e.sourceType || e.kind) || undefined,
    date: text(e.date || e.verifiedAt || e.publishedAt) || undefined,
    verificationDate:
      text(e.verificationDate || e.verifiedAt || e.checkedAt) || undefined,
  };
}

const MAX_FACTS = 80;
const MAX_REGISTER_SOURCES = 60;
const MAX_FACT_CLAIM = 420;

/**
 * Compact commercial fact catalog for Airtable researchPayloadJson.
 * Retains fact↔sourceIds edges required to reconstruct Brief L1/L2 + Sources.
 * Founder-authorized Slice 2A compatible extension (no new Airtable schema).
 */
export function slimCommercialFactCatalog(catalog) {
  if (!catalog || typeof catalog !== "object") return undefined;
  const factsIn = Array.isArray(catalog.facts) ? catalog.facts : [];
  const sourcesIn = Array.isArray(catalog.sources) ? catalog.sources : [];
  const facts = [];
  for (const f of factsIn) {
    if (!f || typeof f !== "object") continue;
    const claim = text(f.claim || f.statement || f.text).slice(0, MAX_FACT_CLAIM);
    if (!claim) continue;
    const sourceIds = Array.isArray(f.sourceIds)
      ? f.sourceIds.map((id) => text(id)).filter(Boolean).slice(0, 12)
      : undefined;
    facts.push({
      factId: text(f.factId || f.id) || undefined,
      claim,
      verificationStatus: text(f.verificationStatus || f.status) || undefined,
      destination: text(f.destination || f.level) || undefined,
      origin: text(f.origin) || undefined,
      sections: Array.isArray(f.sections)
        ? f.sections.map((s) => text(s)).filter(Boolean).slice(0, 8)
        : undefined,
      sourceIds: sourceIds?.length ? sourceIds : undefined,
      sourceUrls: Array.isArray(f.sourceUrls)
        ? f.sourceUrls.map((u) => text(u)).filter(Boolean).slice(0, 8)
        : undefined,
    });
    if (facts.length >= MAX_FACTS) break;
  }
  const sources = [];
  const seen = new Set();
  for (const s of sourcesIn) {
    if (!s || typeof s !== "object") continue;
    const url = evidenceUrl(s);
    const sourceId = text(s.sourceId || s.id);
    const key = sourceId || url;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    sources.push({
      sourceId: sourceId || undefined,
      url: url || undefined,
      title: text(s.title || s.label).slice(0, 180) || undefined,
      publisher: text(s.publisher || s.siteName).slice(0, 120) || undefined,
      verifiedAt: text(s.verifiedAt || s.verificationDate) || undefined,
      artifactRef: text(s.artifactRef).slice(0, 240) || undefined,
    });
    if (sources.length >= MAX_REGISTER_SOURCES) break;
  }
  if (!facts.length && !sources.length && !text(catalog.durableRef)) {
    return undefined;
  }
  return {
    version: text(catalog.version) || "gdi-commercial-fact-catalog-slim-v1",
    durableRef: text(catalog.durableRef) || undefined,
    facts: facts.length ? facts : undefined,
    sources: sources.length ? sources : undefined,
  };
}

/**
 * Compact commercial source register (section → sourceIds + source rows).
 */
export function slimCommercialSourceRegister(register) {
  if (!register || typeof register !== "object") return undefined;
  const sourcesIn = Array.isArray(register.sources) ? register.sources : [];
  const sources = [];
  const seen = new Set();
  for (const s of sourcesIn) {
    if (!s || typeof s !== "object") continue;
    const url = evidenceUrl(s);
    const sourceId = text(s.sourceId || s.id);
    const key = sourceId || url;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    sources.push({
      sourceId: sourceId || undefined,
      url: url || undefined,
      title: text(s.title || s.label).slice(0, 180) || undefined,
      publisher: text(s.publisher || s.siteName).slice(0, 120) || undefined,
      verifiedAt: text(s.verifiedAt || s.verificationDate) || undefined,
      artifactRef: text(s.artifactRef).slice(0, 240) || undefined,
      sections: Array.isArray(s.sections)
        ? s.sections.map((x) => text(x)).filter(Boolean).slice(0, 8)
        : undefined,
    });
    if (sources.length >= MAX_REGISTER_SOURCES) break;
  }
  const sectionSourceIds = {};
  const rawMap = register.sectionSourceIds;
  if (rawMap && typeof rawMap === "object") {
    for (const [section, ids] of Object.entries(rawMap)) {
      const sec = text(section);
      if (!sec || !Array.isArray(ids)) continue;
      const kept = ids.map((id) => text(id)).filter(Boolean).slice(0, 24);
      if (kept.length) sectionSourceIds[sec] = kept;
    }
  }
  if (!sources.length && !Object.keys(sectionSourceIds).length) {
    return undefined;
  }
  return {
    version: text(register.version) || "gdi-commercial-source-register-slim-v1",
    sources: sources.length ? sources : undefined,
    sectionSourceIds: Object.keys(sectionSourceIds).length
      ? sectionSourceIds
      : undefined,
  };
}

/**
 * Collect durable evidence URLs from opportunity fields.
 */
export function collectEvidenceUrls(opp = {}) {
  const urls = [];
  const push = (u) => {
    const t = text(u);
    if (t && !urls.includes(t)) urls.push(t);
  };
  for (const e of Array.isArray(opp.evidence) ? opp.evidence : []) {
    push(evidenceUrl(e));
  }
  for (const s of Array.isArray(opp.sources) ? opp.sources : []) {
    push(typeof s === "string" ? s : evidenceUrl(s));
  }
  for (const u of Array.isArray(opp.seedUrls) ? opp.seedUrls : []) push(u);
  for (const u of Array.isArray(opp.additionalEvidenceUrls)
    ? opp.additionalEvidenceUrls
    : []) {
    push(u);
  }
  push(opp.sourceUrl);
  return urls;
}

/**
 * Collect named contacts from opportunity fields.
 */
export function collectNamedContacts(opp = {}) {
  const names = [];
  const push = (n) => {
    const t = text(n);
    if (t && !names.includes(t)) names.push(t);
  };
  push(opp.primaryContactName);
  push(opp.primaryContact?.name);
  push(opp.secondaryContactName);
  push(opp.secondaryContact?.name);
  for (const c of Array.isArray(opp.contacts) ? opp.contacts : []) {
    push(c?.name);
  }
  for (const c of Array.isArray(opp.namedStakeholders) ? opp.namedStakeholders : []) {
    push(typeof c === "string" ? c : c?.name);
  }
  for (const n of Array.isArray(opp.salesReadyContactNames)
    ? opp.salesReadyContactNames
    : []) {
    push(n);
  }
  return names;
}

function isFilesystemOnlyRef(url) {
  const t = text(url);
  if (!t) return false;
  if (/^https?:\/\//i.test(t)) return false;
  if (/^file:/i.test(t)) return true;
  if (/^[A-Za-z]:[\\/]/.test(t)) return true;
  if (t.startsWith("data/group-demand-intelligence/")) return true;
  if (t.includes("opportunity-research-state/")) return true;
  return false;
}

/**
 * Build slim commercial research payload (allowlist).
 */
export function buildSlimResearchPayload(opp = {}, opts = {}) {
  const maxChars = Number(opts.maxChars) || SLIM_RESEARCH_PAYLOAD_MAX_CHARS;
  const maxEvidence = Number(opts.maxEvidence) || MAX_EVIDENCE;

  const stripped = stripHotelPrivateKeys({ ...opp });
  for (const k of SALES_OWNED_PAYLOAD_KEYS) delete stripped[k];
  for (const k of HOTEL_PRIVATE_PAYLOAD_KEYS) delete stripped[k];
  for (const k of PUBLICATION_STRIP_KEYS) delete stripped[k];
  for (const k of TELEMETRY_STRIP_KEYS) delete stripped[k];

  const evidenceRaw = [
    ...(Array.isArray(stripped.evidence) ? stripped.evidence : []),
    ...(Array.isArray(stripped.sources) ? stripped.sources : []),
  ]
    .map(slimEvidenceEntry)
    .filter(Boolean);
  const seenUrl = new Set();
  const evidence = [];
  const additionalEvidenceUrls = [];
  for (const e of evidenceRaw) {
    if (seenUrl.has(e.url)) continue;
    seenUrl.add(e.url);
    if (evidence.length < maxEvidence) {
      evidence.push(e);
    } else if (additionalEvidenceUrls.length < 80) {
      // Keep overflow URLs as compact strings so preservation checks pass
      // without bloating the primary evidence objects.
      additionalEvidenceUrls.push(e.url);
    }
  }

  const sources = [
    ...evidence.map((e) => e.url),
    ...additionalEvidenceUrls,
  ];
  const primaryContact =
    slimContact(stripped.primaryContact) ||
    (text(stripped.primaryContactName)
      ? {
          name: text(stripped.primaryContactName),
          role: text(stripped.primaryContact?.role) || null,
        }
      : null);
  const secondaryContact =
    slimContact(stripped.secondaryContact) ||
    (text(stripped.secondaryContactName)
      ? { name: text(stripped.secondaryContactName) }
      : null);
  const contacts = (Array.isArray(stripped.contacts) ? stripped.contacts : [])
    .map(slimContact)
    .filter(Boolean)
    .slice(0, 12);
  const namedStakeholders = (
    Array.isArray(stripped.namedStakeholders) ? stripped.namedStakeholders : []
  )
    .map((c) => (typeof c === "string" ? { name: text(c) } : slimContact(c)))
    .filter(Boolean)
    .slice(0, 12);

  const commercialSlice = extractCommercialPayloadSlice(stripped);
  const drawer = stripped.tenSectionDrawer;
  const drawerSlim =
    drawer && typeof drawer === "object"
      ? {
          sectionIds: Array.isArray(drawer.sectionIds)
            ? drawer.sectionIds.slice(0, 12)
            : undefined,
          salespersonPacket: drawer.salespersonPacket
            ? {
                headline: text(drawer.salespersonPacket.headline) || null,
                nextAction:
                  text(drawer.salespersonPacket.nextAction) || null,
              }
            : undefined,
        }
      : undefined;

  const payload = {
    slimPayloadVersion: GDI_RESEARCH_INVENTORY_SLIM_PAYLOAD_V1,
    id: stripped.id || stripped.opportunityId || null,
    opportunityId: stripped.opportunityId || stripped.id || null,
    hotelId: stripped.hotelId || null,
    organizationName: stripped.organizationName || stripped.company || null,
    organizationEntityId: stripped.organizationEntityId || null,
    dgGeneratorId: stripped.dgGeneratorId || null,
    dgProgramId: stripped.dgProgramId || null,
    dgSignalId: stripped.dgSignalId || null,
    title: stripped.title || stripped.displayTitle || stripped.programmeName || null,
    displayTitle: stripped.displayTitle || null,
    segment: stripped.segment || stripped.segmentId || stripped.researchSegment || null,
    researchSegment: stripped.researchSegment || null,
    demandMechanism: stripped.demandMechanism || stripped.opportunityType || null,
    opportunityType: stripped.opportunityType || null,
    eventStartDate: stripped.eventStartDate || null,
    eventEndDate: stripped.eventEndDate || null,
    demandThesis: stripped.demandThesis || null,
    hotelOpportunityThesis:
      stripped.hotelOpportunityThesis || stripped.whyThisHotel || null,
    whyThisHotel: stripped.whyThisHotel || null,
    whyNow: stripped.whyNow || null,
    whatIsHappening: stripped.whatIsHappening || null,
    priority: stripped.priority || null,
    disposition: stripped.disposition || stripped.internalDisposition || null,
    internalDisposition: stripped.internalDisposition || null,
    commercialEligibilityStatus: stripped.commercialEligibilityStatus || null,
    commercialQualificationV41: stripped.commercialQualificationV41
      ? {
          outcome: stripped.commercialQualificationV41.outcome || null,
          reasons: Array.isArray(stripped.commercialQualificationV41.reasons)
            ? stripped.commercialQualificationV41.reasons.slice(0, 8)
            : undefined,
        }
      : undefined,
    honestSalesBriefV41: stripped.honestSalesBriefV41
      ? {
          summary: text(stripped.honestSalesBriefV41.summary).slice(0, 600) || null,
          nextAction:
            text(stripped.honestSalesBriefV41.nextAction).slice(0, 280) || null,
        }
      : undefined,
    recommendedNextSalesAction: stripped.recommendedNextSalesAction || null,
    hotelFit: stripped.hotelFit || stripped.hotelFitSummary || null,
    hotelFitScore: stripped.hotelFitScore ?? null,
    partnerOrganizationName:
      stripped.partnerOrganizationName ||
      stripped.plannerCompany ||
      stripped.intermediaryName ||
      null,
    plannerCompany: stripped.plannerCompany || null,
    intermediaryName: stripped.intermediaryName || null,
    competitorHotel: stripped.competitorHotel || null,
    cmiCompetitorHotel: stripped.cmiCompetitorHotel || null,
    competitorVenueStatus: stripped.competitorVenueStatus || null,
    evidence,
    sources,
    additionalEvidenceUrls:
      additionalEvidenceUrls.length > 0 ? additionalEvidenceUrls : undefined,
    sourceUrl: stripped.sourceUrl || sources[0] || null,
    sourceSummary:
      text(stripped.sourceSummary) ||
      sources.slice(0, 5).join(" | ") ||
      null,
    // Founder-authorized fact↔source graph retention (Slice 2A compatible extension)
    commercialFactCatalog: slimCommercialFactCatalog(
      stripped.commercialFactCatalog,
    ),
    commercialSourceRegister: slimCommercialSourceRegister(
      stripped.commercialSourceRegister,
    ),
    sectionCitations: Array.isArray(stripped.sectionCitations)
      ? stripped.sectionCitations.slice(0, 16).map((c) => {
          if (!c || typeof c !== "object") return null;
          return {
            sectionId: text(c.sectionId || c.section) || undefined,
            sourceIds: Array.isArray(c.sourceIds)
              ? c.sourceIds.map((id) => text(id)).filter(Boolean).slice(0, 24)
              : undefined,
          };
        }).filter(Boolean)
      : undefined,
    primaryContact,
    secondaryContact,
    contacts,
    namedStakeholders,
    salesReadyContactNames: (
      Array.isArray(stripped.salesReadyContactNames)
        ? stripped.salesReadyContactNames
        : []
    )
      .map((n) => text(n))
      .filter(Boolean)
      .slice(0, 24),
    primaryContactName: primaryContact?.name || null,
    secondaryContactName: secondaryContact?.name || null,
    researchContinuation: stripped.researchContinuation || null,
    researchStopClass: stripped.researchStopClass || null,
    unresolvedQuestions: Array.isArray(stripped.unresolvedQuestions)
      ? stripped.unresolvedQuestions.slice(0, 12)
      : undefined,
    nextRefreshAt: stripped.nextRefreshAt || null,
    runId: stripped.runId || null,
    firstDiscoveredRunId: stripped.firstDiscoveredRunId || null,
    firstSeenRunId: stripped.firstSeenRunId || null,
    lastResearchedAt: stripped.lastResearchedAt || null,
    lastMaterialChangeAt: stripped.lastMaterialChangeAt || null,
    paidSpendUsd: stripped.paidSpendUsd ?? null,
    deepCostUsd: stripped.deepCostUsd ?? null,
    selection: stripped.selection || null,
    tenSectionDrawer: drawerSlim,
    filesystemOnlyEvidenceRefs: sources.filter(isFilesystemOnlyRef),
    ...Object.fromEntries(
      REQUIRED_COMMERCIAL_PAYLOAD_KEYS.filter((k) => commercialSlice[k] !== undefined).map(
        (k) => [k, commercialSlice[k]],
      ),
    ),
    commercialPayloadContractVersion:
      commercialSlice.commercialPayloadContractVersion,
  };

  // Remove undefined / empty noise
  for (const [k, v] of Object.entries(payload)) {
    if (v === undefined || v === null || v === "") delete payload[k];
    if (Array.isArray(v) && v.length === 0) delete payload[k];
  }

  let json = JSON.stringify(payload);
  let length = json.length;
  if (length > maxChars && evidence.length > 12) {
    payload.evidence = evidence.slice(0, 12);
    payload.sources = payload.evidence.map((e) => e.url);
    payload.sourceSummary = payload.sources.slice(0, 5).join(" | ");
    json = JSON.stringify(payload);
    length = json.length;
  }

  const sourceContactNames = collectNamedContacts(opp);
  const sourceEvidenceUrls = collectEvidenceUrls(opp).filter(
    (u) => !isFilesystemOnlyRef(u) || opts.allowFilesystemEvidence === true,
  );
  const payloadContactNames = collectNamedContacts(payload);
  const payloadEvidenceUrls = collectEvidenceUrls(payload);

  const missingContacts = sourceContactNames.filter(
    (n) => !payloadContactNames.some((p) => p.toLowerCase() === n.toLowerCase()),
  );
  const missingEvidence = sourceEvidenceUrls.filter(
    (u) => !payloadEvidenceUrls.includes(u),
  );

  if (length > maxChars) {
    return {
      ok: false,
      code: "RESEARCH_PAYLOAD_TOO_LARGE",
      error: `slim payload ${length} chars exceeds ${maxChars}; refuse truncate`,
      payload: null,
      json: null,
      length,
      preservation: {
        sourceContactNames,
        sourceEvidenceUrls,
        payloadContactNames,
        payloadEvidenceUrls,
        missingContacts,
        missingEvidence,
        filesystemOnlyEvidenceRefs: payload.filesystemOnlyEvidenceRefs || [],
      },
    };
  }

  if (missingContacts.length || missingEvidence.length) {
    return {
      ok: false,
      code: missingContacts.length
        ? "CONTACTS_LOST_IN_PAYLOAD"
        : "EVIDENCE_LOST_IN_PAYLOAD",
      error: "Essential contacts or evidence URLs disappeared during slim payload build",
      payload: null,
      json: null,
      length,
      preservation: {
        sourceContactNames,
        sourceEvidenceUrls,
        payloadContactNames,
        payloadEvidenceUrls,
        missingContacts,
        missingEvidence,
        filesystemOnlyEvidenceRefs: payload.filesystemOnlyEvidenceRefs || [],
      },
    };
  }

  return {
    ok: true,
    code: null,
    payload,
    json,
    length,
    preservation: {
      sourceContactNames,
      sourceEvidenceUrls,
      payloadContactNames,
      payloadEvidenceUrls,
      missingContacts: [],
      missingEvidence: [],
      filesystemOnlyEvidenceRefs: payload.filesystemOnlyEvidenceRefs || [],
    },
  };
}

/**
 * Assert a reconstructed payload retains essential commercial intelligence.
 */
export function assertSlimPayloadRoundTrip(sourceOpp, reconstructed) {
  const sourceContacts = collectNamedContacts(sourceOpp);
  const sourceEvidence = collectEvidenceUrls(sourceOpp).filter(
    (u) => /^https?:\/\//i.test(u),
  );
  const outContacts = collectNamedContacts(reconstructed || {});
  const outEvidence = collectEvidenceUrls(reconstructed || {});
  const missingContacts = sourceContacts.filter(
    (n) => !outContacts.some((p) => p.toLowerCase() === n.toLowerCase()),
  );
  const missingEvidence = sourceEvidence.filter((u) => !outEvidence.includes(u));
  return {
    ok: missingContacts.length === 0 && missingEvidence.length === 0,
    missingContacts,
    missingEvidence,
    identityOk:
      text(reconstructed?.opportunityId || reconstructed?.id) ===
        text(sourceOpp.opportunityId || sourceOpp.id) &&
      text(reconstructed?.hotelId) === text(sourceOpp.hotelId),
  };
}

export default {
  GDI_RESEARCH_INVENTORY_SLIM_PAYLOAD_V1,
  buildSlimResearchPayload,
  collectEvidenceUrls,
  collectNamedContacts,
  assertSlimPayloadRoundTrip,
  slimCommercialFactCatalog,
  slimCommercialSourceRegister,
};
