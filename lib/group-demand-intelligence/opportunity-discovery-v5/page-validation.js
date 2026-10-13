/**
 * Page-level validation — SERP is SIGNAL only; open the page before RESEARCH_LEAD→CANDIDATE.
 */

import {
  fetchCandidatePage,
  extractEvidenceFromPage,
} from "../candidate-completion-v2/page-research.js";

const PAGE_KIND_RE = [
  ["HOUSING", /\/(accommodation|housing|hotels?|hébergement|alojamiento|hotel-block)/i],
  ["RFP", /\b(rfp|tender|procurement|licitación|appel d['']offres)\b/i],
  ["REGISTRATION", /\b(register|registration|inscription)\b/i],
  ["AGENDA", /\b(agenda|program|programme|schedule)\b/i],
  ["CONTACT", /\/contact\b/i],
  ["ORGANIZER", /\b(about|organiser|organizer|secretariat)\b/i],
  ["ARCHIVE", /\b(past|archive|previous|édition|edicion)\b/i],
  ["OFFICIAL_EVENT", /\b(congress|conference|summit|meeting|tournament)\b/i],
];

function classifyPageKind(url, text) {
  const blob = `${url || ""} ${String(text || "").slice(0, 800)}`;
  for (const [kind, re] of PAGE_KIND_RE) {
    if (re.test(blob)) return kind;
  }
  return "GENERIC_PAGE";
}

/**
 * Validate a research lead by fetching its official source page.
 * Persists evidence; does not invent facts.
 */
export async function validateResearchLeadPage(lead = {}, opts = {}) {
  const url = String(lead.officialSource || lead.source || lead.url || "").trim();
  const result = {
    opportunityId: lead.id,
    hotelKey: lead.hotelKey,
    url,
    pageFetched: false,
    pageOk: false,
    pageKind: null,
    lodgingEvidence: null,
    eventStartDate: null,
    eventYear: null,
    futureCycleEvidenceState: null,
    organizationContactUrl: null,
    functionalContactEmail: null,
    organizerName: null,
    groupMotion: null,
    factsCount: 0,
    evidencePersisted: false,
    error: null,
    updates: {},
  };

  if (!url) {
    result.error = "NO_URL";
    return result;
  }

  const page = await fetchCandidatePage(url, { maxChars: opts.maxChars ?? 14000 });
  result.pageFetched = true;
  result.pageOk = page.ok === true;
  if (!page.ok) {
    result.error = page.error || "FETCH_FAILED";
    return result;
  }

  result.pageKind = classifyPageKind(page.url || url, page.text);
  const extracted = extractEvidenceFromPage(lead, page);
  const updates = extracted.updates || {};
  result.updates = updates;
  result.factsCount = (extracted.facts || []).length;
  result.evidencePersisted = result.factsCount > 0 || result.pageKind !== "GENERIC_PAGE";

  if (updates.lodgingEvidence) result.lodgingEvidence = updates.lodgingEvidence;
  if (updates.eventStartDate) result.eventStartDate = updates.eventStartDate;
  if (updates.eventYear) result.eventYear = updates.eventYear;
  if (updates.futureCycleEvidenceState) {
    result.futureCycleEvidenceState = updates.futureCycleEvidenceState;
  }
  if (updates.organizationContactUrl) result.organizationContactUrl = updates.organizationContactUrl;
  if (updates.functionalContactEmail) result.functionalContactEmail = updates.functionalContactEmail;

  // Organizer hint from title/H1-like early text
  const early = String(page.text || "").slice(0, 400);
  const orgLine = early.match(
    /\b((?:[A-Z][A-Za-z0-9&.\-']+(?:\s+[A-Z][A-Za-z0-9&.\-']+){1,5})\s+(?:Association|Society|Federation|Foundation|Congress|Conference|Agency|Events))\b/
  );
  if (orgLine) result.organizerName = orgLine[1];

  if (
    /congress|conference|summit|meeting|kickoff|retreat|tournament|incentive|training/i.test(
      `${lead.title || ""} ${page.text || ""}`.slice(0, 1200)
    )
  ) {
    result.groupMotion = lead.groupMotion || lead.opportunityType || "GROUP_PROGRAM";
  }

  result.sourceUrl = page.url || url;
  return result;
}

/**
 * Merge page evidence into lead object (deterministic facts only).
 */
export function applyPageEvidenceToLead(lead = {}, pageEvidence = {}) {
  const next = { ...lead };
  const u = pageEvidence.updates || {};
  if (u.eventStartDate) next.eventStartDate = u.eventStartDate;
  if (u.eventYear) next.eventYear = u.eventYear;
  if (u.futureCycleEvidenceState) next.futureCycleEvidenceState = u.futureCycleEvidenceState;
  if (u.lodgingEvidence) next.lodgingEvidence = u.lodgingEvidence;
  if (u.organizationContactUrl) next.organizationContactUrl = u.organizationContactUrl;
  if (u.functionalContactEmail) next.functionalContactEmail = u.functionalContactEmail;
  if (u.officialContactPath) next.officialContactPath = u.officialContactPath;
  if (pageEvidence.organizerName && !next.organizationName) {
    next.organizationName = pageEvidence.organizerName;
  }
  if (pageEvidence.groupMotion) next.groupMotion = pageEvidence.groupMotion;
  next.pageValidated = pageEvidence.pageOk === true;
  next.pageKind = pageEvidence.pageKind;
  next.pageValidationUrl = pageEvidence.sourceUrl || pageEvidence.url;
  next.sources = [
    ...(next.sources || []),
    {
      url: pageEvidence.sourceUrl || pageEvidence.url,
      kind: "page_level_validation_v5",
      pageKind: pageEvidence.pageKind,
      factsCount: pageEvidence.factsCount,
    },
  ];
  return next;
}
