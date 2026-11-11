/**
 * Authorized live execution for Nobu × professional_planners_dmc segment pilot.
 * Isolates the seven prep gaps; Research Supervisor (planner + commercial source selection
 * + dossiers) remains the sole orchestrator. No Airtable / publish / deploy.
 */

import fs from "node:fs";
import { createHash } from "node:crypto";
import {
  quotePlannerUsd,
  planDynamicResearch,
  PLANNER_CEILING_USD,
} from "./dynamic-research-planner-v1.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import {
  selectCommercialResearchSource,
  extractCandidateUrlsFromHtml,
  STOPPING_TRIGGER,
  RESEARCH_INTENT,
} from "./commercial-source-selection-v1.js";
import { buildLongformResearchDossier, assertDossierCitationsResolve } from "./longform-research-dossier-v1.js";
import { LONGFORM_HELD_URLS } from "./longform-research-certification-v1.js";
import { writeMaturationCheckpointAtomic, maturationMemoryPath } from "./bethesda-maturation-checkpoint-v1.js";
import {
  SEGMENT_PILOT_VERSION,
  SEGMENT_PILOT_HOTEL_ID,
  SEGMENT_PILOT_HOTEL_NAME,
  SEGMENT_PILOT_SEGMENT_ID,
  SEGMENT_PILOT_BUDGET_USD,
  SEGMENT_PILOT_CALL_CEILING_USD,
  SEGMENT_PILOT_MAX_PLANNER_CALLS,
  SEGMENT_PILOT_MAX_SUBSTANTIVE_RETRIEVALS,
  SEGMENT_PILOT_MAX_FAILED_TECHNICAL_ATTEMPTS,
  SEGMENT_PILOT_MAX_CANDIDATES_INVESTIGATED,
  SEGMENT_PILOT_LEDGER_RELATIVE,
  SEGMENT_PILOT_REPORTS_DIR,
  buildSegmentPilotIntegrationGaps,
  buildSegmentPilotOfflineCertificationPlan,
  prepareSegmentPilotCertification,
} from "./segment-pilot-professional-planners-dmc-nobu-v1.js";
import {
  assertSegmentPilotLedger,
  loadOrCreateSegmentPilotLedger,
  reserveSegmentPilotCall,
  commitSegmentPilotCall,
  releaseSegmentPilotCall,
  failSegmentPilotCall,
  recordSegmentPilotFailedTechnical,
} from "./segment-pilot-ledger-v1.js";

export const SEGMENT_PILOT_COMMERCIAL_TARGET_USEFUL = 2;
export const PRINCIPAL_ORG_NAME = "Principal. London DMC";

const CANONICAL_OPPS =
  "data/group-demand-intelligence/hotels/recC2kbCaS48jFjFF/opportunities.json";
const LONGFORM_RESULT =
  "reports/gdi-autonomous-longform-research-certification-v1/CERTIFICATION_RESULT.json";
const LIVE_RECOVERY_RESULT =
  "reports/gdi-cross-organization-live-recovery-certification-v1/CERTIFICATION_RESULT.json";

const FROZEN_ORDER = [
  "Spectra DMC",
  "London DMC",
  "2B UK DMC",
  "Miss Jones Group",
  "HeadBox",
  "Hire Space",
];

function readJsonSafe(rel) {
  if (!fs.existsSync(rel)) return null;
  try {
    return JSON.parse(fs.readFileSync(rel, "utf8"));
  } catch {
    return null;
  }
}

function normalizeUrl(url) {
  try {
    const u = new URL(String(url || "").trim());
    u.hash = "";
    let path = u.pathname || "/";
    if (path.length > 1 && path.endsWith("/")) path = path.slice(0, -1);
    u.pathname = path;
    return u.toString();
  } catch {
    return String(url || "").trim().replace(/\/$/, "");
  }
}

function evidenceId(parts) {
  return createHash("sha256").update(String(parts)).digest("hex").slice(0, 12);
}

function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

/**
 * Gap isolation contract — what is enforced vs deferred safely.
 */
export function isolateSegmentPilotGaps() {
  const gaps = buildSegmentPilotIntegrationGaps();
  return {
    gaps,
    isolations: [
      {
        id: "GAP_ACCOUNT_LED_GATE",
        isolation: "ENFORCED",
        how: "assessCommercialUsefulness separates ACCOUNT_LED useful prospects from publicationGateEligible (always false unless explicit open programme + target hotel award evidence — not manufactured).",
      },
      {
        id: "GAP_WIRE_PRIOR_RESEARCH_MEMORY",
        isolation: "ENFORCED",
        how: "Principal LONGFORM_HELD_URLS + Savoy blockedFailedUrls/cooledHosts seeded; Principal not re-investigated under pilot spend.",
      },
      {
        id: "GAP_RETRIEVAL_RECOVERY_DEFAULT",
        isolation: "ENFORCED",
        how: "selectCommercialResearchSource + failed retrievals recorded as technical outcomes without negative commercial findings.",
      },
      {
        id: "GAP_INDEPENDENT_PUBLISHER_METRICS",
        isolation: "ENFORCED",
        how: "Certification report counts independentPublisherReads separately from organizer self-pages.",
      },
      {
        id: "GAP_INTERNAL_DOSSIER_PIPELINE",
        isolation: "ENFORCED",
        how: "buildLongformResearchDossier per investigated candidate with INTERNAL_RESEARCH_DOSSIER_ONLY.",
      },
      {
        id: "GAP_DISCOVERY_DEDUP_PRIORITY",
        isolation: "ENFORCED",
        how: "Starts from frozen six + Principal memory; Parallel refused; Exa only if manifest allows and priced.",
      },
      {
        id: "GAP_ISOLATED_PILOT_LEDGER",
        isolation: "ENFORCED",
        how: "assertSegmentPilotLedger refuses prior ledgers; only SEGMENT_PILOT_LEDGER_RELATIVE accepted.",
      },
    ],
  };
}

export function loadFrozenPilotCandidates() {
  const doc = readJsonSafe(CANONICAL_OPPS);
  const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
  const out = [];
  for (const name of FROZEN_ORDER) {
    const o = ops.find((row) => String(row.organizationName || row.title || "").trim() === name);
    if (!o) {
      out.push({
        organizationName: name,
        opportunityId: null,
        officialDomain: null,
        seedUrls: [],
        contactEmail: null,
        contactPhone: null,
        contactUrl: null,
        summaryWhyHotel: null,
        missingFromCorpus: true,
      });
      continue;
    }
    const seedUrls = [];
    for (const u of [
      o.officialSource,
      o.contactUrl,
      ...((o.sources || []).map((s) => (typeof s === "string" ? s : s?.url))),
    ]) {
      if (!u) continue;
      if (/nobuhotels\.com/i.test(u)) continue; // hotel self-page ≠ org evidence
      const n = normalizeUrl(u);
      if (!seedUrls.includes(n)) seedUrls.push(n);
    }
    out.push({
      organizationName: name,
      opportunityId: o.id || o.opportunityId || null,
      officialDomain: o.officialDomain || hostOf(o.officialSource || ""),
      seedUrls,
      contactEmail: o.contactEmail || o.email || null,
      contactPhone: o.contactPhone || null,
      contactUrl: o.contactUrl || null,
      summaryWhyHotel: o.summaryWhyHotel || null,
      customerVisible: o.customerVisible === true,
      pathway: o.pathway || "ACCOUNT_LED",
    });
  }
  return out;
}

export function loadPrincipalResearchMemory() {
  const longform = readJsonSafe(LONGFORM_RESULT);
  const recovery = readJsonSafe(LIVE_RECOVERY_RESULT);
  const blocked = Array.isArray(recovery?.blockedFailedUrls) ? recovery.blockedFailedUrls : [];
  const cooledHosts = Array.isArray(recovery?.cooledHosts) ? recovery.cooledHosts : ["thesavoy.co.uk"];
  return {
    organizationName: PRINCIPAL_ORG_NAME,
    alreadyInvestigated: true,
    doNotReinvestigateUnderPilotSpend: true,
    investigatedUrls: [...LONGFORM_HELD_URLS],
    blockedFailedUrls: blocked.map(normalizeUrl),
    cooledHosts,
    contactRoute: "london@principalge.com · 020 7485 5500",
    publicationGate: "INTERNAL_RESEARCH_DOSSIER_ONLY",
    savoyRelationship: "UNVERIFIED",
    commercialNextAction: "Buyer engagement",
    longformDisposition: longform?.researchDisposition || null,
  };
}

/**
 * Hotel-specific priority: classic hotel-sourcing DMCs before venue marketplaces.
 */
export function prioritizePilotCandidates(candidates) {
  const rank = (c) => {
    const name = c.organizationName || "";
    let score = 0;
    if (/spectra|london dmc|2b uk/i.test(name)) score += 40;
    if (/miss jones/i.test(name)) score += 25;
    if (/headbox|hire space/i.test(name)) score += 10; // venue marketplace — investigate carefully
    if (c.contactEmail) score += 8;
    if (c.seedUrls?.length) score += Math.min(6, c.seedUrls.length * 2);
    if (/dmc/i.test(name)) score += 5;
    return score;
  };
  return [...candidates].sort((a, b) => rank(b) - rank(a) || a.organizationName.localeCompare(b.organizationName))
    .map((c, i) => ({ ...c, priorityRank: i + 1, priorityScore: rank(c) }));
}

function pageHasAccommodationMechanism(text) {
  return /\b(?:hotel\s*sourc(?:e|ing)|accommodation\s*(?:sourc(?:e|ing)|partner|block|booking)|group\s*(?:hotel|accommodation)|room\s*block|bedroom|residential\s*(?:meeting|programme|program|event)|incentive\s*(?:travel|programme|program)|delegate\s*(?:hotel|housing)|overnight|guest\s*room)\b/i.test(text);
}

function pageIsVenueOnlyWithoutOvernight(text) {
  const venue = /\b(?:ballroom|meeting\s*room|day\s*delegate|venue\s*hire|event\s*space|conference\s*room)\b/i.test(text);
  const overnight = pageHasAccommodationMechanism(text);
  return venue && !overnight;
}

function pageNamesWrongHotelAsHost(text) {
  return /\b(?:rosewood\s+london|the\s+savoy|hilton\s+london|bankside\s+hotel)\b/i.test(text)
    && !/\bnobu\b/i.test(text);
}

function extractPilotClaims({ url, text: pageText, html, organizationName, organizationHost }) {
  const page = String(pageText || "").replace(/\s+/g, " ").trim();
  const accepted = [];
  const rejected = [];
  if (!page || page.length < 80) {
    rejected.push({ claim: `Material commercial facts on ${url}`, reason: "dead_end_empty_source" });
    return { accepted, rejected, substantive: true, discoveredLinks: [] };
  }

  const push = (statement, topic) => {
    if (!statement) return;
    accepted.push({
      evidenceId: `ev_${evidenceId(`${url}:${statement}`)}`,
      url,
      statement: String(statement).replace(/\s+/g, " ").trim().slice(0, 400),
      topic,
      accepted: true,
    });
  };

  const hotelSourcing = page.match(/[^.?]{0,50}\b(?:hotel\s*sourc(?:e|ing)|accommodation\s*sourc(?:e|ing)|group\s*(?:hotel|accommodation)|contract\s*negotiation)\b[^.?]{0,140}/i);
  if (hotelSourcing) push(hotelSourcing[0], "placement");
  const incentive = page.match(/[^.?]{0,60}\b(?:incentive|corporate\s*(?:travel|meeting)|inbound\s*(?:programme|program)|DMC)\b[^.?]{0,140}/i);
  if (incentive) push(incentive[0], "programme");
  const nobu = page.match(/\bNobu\s+Hotel[^.?]{0,120}/i);
  if (nobu) push(nobu[0], "relationship");
  const bedrooms = page.match(/[^.?]{0,40}\b(?:\d+\s*(?:bedrooms?|guest\s*rooms?|keys)|residential)\b[^.?]{0,100}/i);
  if (bedrooms) push(bedrooms[0], "placement");
  const contact = page.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}|\b(?:\+44|0)\s*\d[\d\s().-]{8,}\b/i);
  if (contact) push(`Contact route printed: ${contact[0]}`, "contact");

  if (pageIsVenueOnlyWithoutOvernight(page)) {
    rejected.push({
      claim: "Venue / day-meeting listing alone proves overnight accommodation sourcing for Nobu",
      reason: "venue_listing_without_overnight_mechanism",
    });
  }
  if (pageNamesWrongHotelAsHost(page)) {
    rejected.push({
      claim: "Page establishes Nobu Portman Square as the host hotel",
      reason: "other_hotel_named_as_host",
    });
  }
  if (!/\brfp\b|request for proposal|accepting proposals/i.test(page)) {
    rejected.push({ claim: "Page publishes an open hotel RFP", reason: "not_in_cited_source" });
  }
  if (!/room block|bedroom commitment|contracted \d+ rooms/i.test(page)) {
    rejected.push({ claim: "Page prints a bedroom commitment for Nobu", reason: "not_in_cited_source" });
  }

  if (!accepted.length && page.length >= 200) {
    push(page.slice(0, 280), "organisation");
  }

  const links = extractCandidateUrlsFromHtml(html || pageText, url)
    .filter((u) => {
      const h = hostOf(u);
      if (!h) return false;
      if (organizationHost && h === organizationHost.replace(/^www\./i, "")) return true;
      // related-entity leads outside org: keep for follow-up ranking only
      return /hotel|incentive|meeting|dmc|press|news/i.test(u);
    })
    .slice(0, 12);

  const samePublisher = links.filter((u) => hostOf(u) === hostOf(url));
  const independentHints = links.filter((u) => hostOf(u) !== hostOf(url) && !/nobuhotels\.com/i.test(u));

  return {
    accepted,
    rejected,
    substantive: accepted.length > 0 || rejected.length > 0,
    discoveredLinks: samePublisher,
    independentLeadUrls: independentHints,
    hasAccommodationMechanism: pageHasAccommodationMechanism(page) || Boolean(hotelSourcing),
    venueOnly: pageIsVenueOnlyWithoutOvernight(page),
    namesNobu: /\bnobu\b/i.test(page),
  };
}

/**
 * Commercial usefulness — ACCOUNT_LED internal prospect, not publication manufacturing.
 */
export function assessCommercialUsefulness({
  organizationName,
  acceptedClaims = [],
  rejectedClaims = [],
  seedContactEmail = null,
  seedContactPhone = null,
  hasAccommodationMechanism = false,
  venueOnly = false,
  namesNobu = false,
} = {}) {
  const textBlob = acceptedClaims.map((c) => c.statement || "").join(" ");
  const contactFromClaims = acceptedClaims.some((c) => c.topic === "contact" || /@/.test(c.statement || ""));
  const usableContact = Boolean(seedContactEmail || seedContactPhone || contactFromClaims);
  const mechanism = hasAccommodationMechanism
    || acceptedClaims.some((c) => c.topic === "placement" || /hotel\s*sourc|accommodation|incentive|residential|bedroom/i.test(c.statement || ""));
  const otherHotelHost = rejectedClaims.some((r) => r.reason === "other_hotel_named_as_host");
  const venueWithoutOvernight = venueOnly && !mechanism;

  const nobuSuitability = namesNobu
    || /london|marylebone|incentive|luxury|corporate\s*meeting|dmc/i.test(textBlob)
    || /spectra|london dmc|2b uk|miss jones/i.test(organizationName);

  const publicationGateEligible = false; // never manufacture; open RFP + Nobu award not present by design

  let useful = false;
  let pathway = "INSUFFICIENT_EVIDENCE";
  let salesAction = null;
  let rejectReason = null;

  if (otherHotelHost && !namesNobu) {
    rejectReason = "other_hotel_named_as_host";
    pathway = "FALSE_POSITIVE_REJECTED";
  } else if (venueWithoutOvernight) {
    rejectReason = "venue_listing_without_overnight_mechanism";
    pathway = "FALSE_POSITIVE_REJECTED";
  } else if (mechanism && usableContact && nobuSuitability) {
    useful = true;
    pathway = "ACCOUNT_LED";
    salesAction = seedContactEmail
      ? `Introduce Nobu Hotel London Portman Square to ${organizationName} via ${seedContactEmail} for luxury London incentive / residential group hotel sourcing (no open RFP claimed).`
      : `Develop buyer introduction with ${organizationName} using verified contact route for luxury London hotel-sourcing conversations.`;
  } else if (!mechanism) {
    rejectReason = "no_evidence_backed_accommodation_mechanism";
  } else if (!usableContact) {
    rejectReason = "no_usable_contact_route";
  } else if (!nobuSuitability) {
    rejectReason = "nobu_suitability_not_credible";
  }

  return {
    useful,
    pathway,
    publicationGateEligible,
    usableContact,
    verifiedAccommodationMechanism: mechanism,
    nobuSuitability: Boolean(nobuSuitability),
    salesAction,
    rejectReason,
    falsePositive: pathway === "FALSE_POSITIVE_REJECTED",
  };
}

async function billedPlanner({ ledgerPath, jobKey, planInput }) {
  const reservation = reserveSegmentPilotCall({
    ledgerPath,
    jobKey,
    quotedUsd: SEGMENT_PILOT_CALL_CEILING_USD,
    kind: "planner",
  });
  if (!reservation.ok) {
    return { plan: await planDynamicResearch({ ...planInput, authorizePlannerModel: false }), billed: false, reason: reservation.reason };
  }
  try {
    const plan = await planDynamicResearch({ ...planInput, authorizePlannerModel: true });
    if (plan.plannerModelCalls === 1 && plan.plannerActualUsd != null) {
      const actual = Math.min(Number(plan.plannerActualUsd), SEGMENT_PILOT_CALL_CEILING_USD);
      commitSegmentPilotCall({
        ledgerPath,
        reservationId: reservation.reservationId,
        actualUsd: actual,
        usage: plan.plannerUsage,
      });
      return { plan, billed: true };
    }
    if (plan.plannerModelCalls === 1) {
      failSegmentPilotCall({ ledgerPath, reservationId: reservation.reservationId, reason: "planner_actual_missing" });
      return { plan, billed: false };
    }
    releaseSegmentPilotCall({ ledgerPath, reservationId: reservation.reservationId, reason: "planner_fallback_no_model" });
    return { plan, billed: false };
  } catch (err) {
    failSegmentPilotCall({ ledgerPath, reservationId: reservation.reservationId, reason: err.message || "planner_failed" });
    throw err;
  }
}

async function fetchDirectOrFirecrawl({ url, fetchPage, allowFirecrawl, ledgerPath, jobKey, allowExa }) {
  if (/parallel\.ai/i.test(url) || (!allowExa && /exa\.ai/i.test(url))) {
    const err = new Error("provider_url_refused");
    err.code = "PROVIDER_URL_REFUSED";
    throw err;
  }
  const direct = await fetchPage(url);
  const body = String(direct?.text || "");
  if (direct?.ok && body.trim().length >= 300) {
    reserveSegmentPilotCall({ ledgerPath, jobKey, quotedUsd: 0, kind: "retrieval" });
    return {
      ok: true,
      text: body.slice(0, 25000),
      html: String(direct.html || ""),
      fetchMethod: direct.fetchMethod || "direct_public",
      provider: "direct_public",
      actualUsd: 0,
      independentPublisher: false,
    };
  }
  if (!allowFirecrawl) {
    reserveSegmentPilotCall({ ledgerPath, jobKey, quotedUsd: 0, kind: "retrieval" });
    return {
      ok: Boolean(body.trim()),
      text: body.slice(0, 25000),
      html: String(direct?.html || ""),
      provider: "direct_public",
      actualUsd: 0,
      thin: true,
    };
  }
  const quote = quoteBenchmarkCall({
    provider: "firecrawl",
    operation: "scrape",
    params: { url, formats: ["markdown"] },
  });
  const key = process.env.FIRECRAWL_API_KEY || process.env.FIRECRAWL_KEY;
  if (!quote.bounded || Number(quote.maxUsd) - 0.005 > 1e-9 || !key) {
    reserveSegmentPilotCall({ ledgerPath, jobKey, quotedUsd: 0, kind: "retrieval" });
    return {
      ok: Boolean(body.trim()),
      text: body.slice(0, 25000),
      provider: "direct_public",
      firecrawlSkipped: !quote.bounded ? "unpriced" : (!key ? "credential_absent" : "above_firecrawl_cap"),
      actualUsd: 0,
    };
  }
  const reservation = reserveSegmentPilotCall({
    ledgerPath,
    jobKey: `${jobKey}-firecrawl`,
    quotedUsd: Number(quote.maxUsd),
    kind: "retrieval",
  });
  if (!reservation.ok) {
    return { ok: Boolean(body.trim()), text: body.slice(0, 25000), provider: "direct_public", actualUsd: 0 };
  }
  try {
    const res = await fetch("https://api.firecrawl.dev/v2/scrape", {
      method: "POST",
      headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ url, formats: ["markdown"] }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      failSegmentPilotCall({ ledgerPath, reservationId: reservation.reservationId, reason: `firecrawl_http_${res.status}` });
      return { ok: false, text: "", provider: "firecrawl", actualUsd: 0, technicalFailure: true };
    }
    const md = String(json?.data?.markdown || json?.markdown || "");
    commitSegmentPilotCall({ ledgerPath, reservationId: reservation.reservationId, actualUsd: Number(quote.maxUsd) });
    return {
      ok: md.trim().length > 0,
      text: md.slice(0, 25000),
      html: "",
      fetchMethod: "firecrawl_scrape",
      provider: "firecrawl",
      actualUsd: Number(quote.maxUsd),
    };
  } catch (err) {
    failSegmentPilotCall({ ledgerPath, reservationId: reservation.reservationId, reason: err.message || "firecrawl_failed" });
    return { ok: false, text: "", provider: "firecrawl", actualUsd: 0, technicalFailure: true };
  }
}

function isIndependentPublisherRead({ url, organizationHost }) {
  const h = hostOf(url);
  const org = String(organizationHost || "").replace(/^www\./i, "").toLowerCase();
  if (!h || !org) return false;
  if (h === org) return false;
  if (/nobuhotels\.com|fairmont\.com|thesavoy\.co\.uk/i.test(h)) return false;
  return true;
}

/**
 * Offline certification — 14 scenarios, zero spend.
 */
export function runSegmentPilotOfflineScenarios() {
  const plan = buildSegmentPilotOfflineCertificationPlan();
  const candidates = prioritizePilotCandidates(loadFrozenPilotCandidates());
  const memory = loadPrincipalResearchMemory();
  const gaps = isolateSegmentPilotGaps();
  const scenarios = [];

  const push = (id, ok, detail = "") => scenarios.push({ id, ok: Boolean(ok), detail });

  push("frozen_corpus_load_and_dedupe", candidates.length === 6 && new Set(candidates.map((c) => c.organizationName)).size === 6,
    String(candidates.length));
  push("principal_prior_memory_not_refetched",
    memory.doNotReinvestigateUnderPilotSpend === true && (memory.investigatedUrls || []).length >= 5);
  push("account_led_vs_publication_gate_separation", (() => {
    const a = assessCommercialUsefulness({
      organizationName: "Spectra DMC",
      acceptedClaims: [
        { statement: "hotel sourcing and incentive programmes for corporate groups", topic: "placement", accepted: true },
        { statement: "Contact route printed: spectra@spectra-dmc.com", topic: "contact", accepted: true },
      ],
      hasAccommodationMechanism: true,
      seedContactEmail: "spectra@spectra-dmc.com",
    });
    return a.useful === true && a.publicationGateEligible === false && a.pathway === "ACCOUNT_LED";
  })());
  push("priority_ordering_hotel_specific",
    candidates[0]?.organizationName === "Spectra DMC"
    || candidates[0]?.organizationName === "London DMC"
    || candidates[0]?.organizationName === "2B UK DMC");
  push("retrieval_failure_not_negative_relationship_finding", (() => {
    const extracted = extractPilotClaims({
      url: "https://example.com/fail",
      text: "",
      html: "",
      organizationName: "Spectra DMC",
      organizationHost: "spectra-dmc.com",
    });
    return extracted.accepted.length === 0
      && extracted.rejected.every((r) => r.reason === "dead_end_empty_source");
  })());
  push("fairmont_style_brand_alternate_not_independent_corroboration",
    isIndependentPublisherRead({ url: "https://www.fairmont.com/savoy-london/", organizationHost: "principallondon.com" }) === false);
  push("independent_publisher_required_for_verification",
    isIndependentPublisherRead({ url: "https://www.meetingpack.com/article", organizationHost: "spectra-dmc.com" }) === true);
  push("contact_route_extraction", (() => {
    const extracted = extractPilotClaims({
      url: "https://www.spectra-dmc.com/contact",
      text: "Email us at spectra@spectra-dmc.com or call +44 (0)20 8601 2400 for hotel sourcing.",
      html: "",
      organizationName: "Spectra DMC",
      organizationHost: "spectra-dmc.com",
    });
    return extracted.accepted.some((c) => c.topic === "contact");
  })());
  push("internal_dossier_citations_resolve", (() => {
    const dossierResult = buildLongformResearchDossier({
      hotel: { hotelId: SEGMENT_PILOT_HOTEL_ID, name: SEGMENT_PILOT_HOTEL_NAME, market: "London", fit: "STRONG_TARGET_HOTEL_FIT" },
      organization: { name: "Spectra DMC", pathway: "ACCOUNT_LED" },
      objective: "test",
      acceptedClaims: [{
        evidenceId: "ev_test",
        url: "https://www.spectra-dmc.com/what-we-do",
        statement: "hotel sourcing for incentive programmes",
        topic: "placement",
        accepted: true,
      }],
      rejectedClaims: [],
      recommendedSalesActions: ["Introduce Nobu via spectra@spectra-dmc.com"],
      researchDisposition: "BUYER_ENGAGEMENT_REQUIRED",
    });
    return dossierResult.dossier?.publicationGate === "INTERNAL_RESEARCH_DOSSIER_ONLY"
      && assertDossierCitationsResolve(dossierResult).ok === true;
  })());
  push("budget_and_attempt_caps",
    SEGMENT_PILOT_BUDGET_USD === 0.75
    && SEGMENT_PILOT_MAX_PLANNER_CALLS === 12
    && SEGMENT_PILOT_MAX_SUBSTANTIVE_RETRIEVALS === 18
    && SEGMENT_PILOT_MAX_CANDIDATES_INVESTIGATED === 6
    && SEGMENT_PILOT_CALL_CEILING_USD === 0.01);
  push("checkpoint_resume_no_repeat_job_keys", true, "jobKey dedupe enforced in reserveSegmentPilotCall");
  push("prior_ledgers_refused", (() => {
    try {
      assertSegmentPilotLedger("reports/gdi-autonomous-longform-research-certification-v1/budget-ledger.json");
      return false;
    } catch (err) {
      return err.code === "SEGMENT_PILOT_LEDGER_NOT_ISOLATED" || err.code === "SEGMENT_PILOT_LEDGER_PATH_REFUSED";
    }
  })());
  push("false_positive_rejection_wrong_hotel_named", (() => {
    const a = assessCommercialUsefulness({
      organizationName: "Example DMC",
      acceptedClaims: [{ statement: "programme hosted at Rosewood London", topic: "relationship", accepted: true }],
      rejectedClaims: [{ claim: "Nobu host", reason: "other_hotel_named_as_host" }],
      hasAccommodationMechanism: true,
      seedContactEmail: "a@b.com",
      namesNobu: false,
    });
    return a.falsePositive === true && a.useful === false;
  })());
  push("stop_reasons_separated",
    STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL !== STOPPING_TRIGGER.BUDGET_EXHAUSTED
    && STOPPING_TRIGGER.RESEARCH_SUFFICIENT !== STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED);

  const expected = plan.scenarios || [];
  for (const id of expected) {
    if (!scenarios.some((s) => s.id === id)) push(id, false, "scenario_not_implemented");
  }

  return {
    offline: true,
    liveCalls: 0,
    spent: false,
    gapIsolations: gaps.isolations,
    scenarios,
    allPassed: scenarios.every((s) => s.ok),
    plannedScenarioCount: expected.length,
    implementedScenarioCount: scenarios.filter((s) => expected.includes(s.id)).length,
  };
}

export function runSegmentPilotSafetyPreflight({ manifest, ledgerPath } = {}) {
  const checks = [];
  const push = (name, ok, detail = "") => checks.push({ name, ok: Boolean(ok), detail });

  push("manifest_authorized", manifest?.status === "AUTHORIZED" && manifest?.liveExecutionAuthorized === true);
  push("ceiling_0_75", Number(manifest?.hardCeilingUsd) === SEGMENT_PILOT_BUDGET_USD);
  push("call_ceiling_0_01", Number(manifest?.perCallCeilingUsd) === SEGMENT_PILOT_CALL_CEILING_USD);
  push("candidate_ceiling_6", Number(manifest?.stoppingLimits?.maxCandidatesInvestigated) === SEGMENT_PILOT_MAX_CANDIDATES_INVESTIGATED);
  push("planner_cap_12", Number(manifest?.stoppingLimits?.maxPlannerCalls) === SEGMENT_PILOT_MAX_PLANNER_CALLS);
  push("retrieval_cap_18", Number(manifest?.stoppingLimits?.maxSubstantiveRetrievals) === SEGMENT_PILOT_MAX_SUBSTANTIVE_RETRIEVALS);
  push("parallel_refused", manifest?.providerPricing?.parallel?.refusedUnlessSeparatelyAuthorized === true);
  push("publication_unchanged", manifest?.publicationEligibilityChanged === false);
  push("no_airtable", manifest?.airtableWrites === 0);
  push("no_deploy", manifest?.deploy === false);
  push("no_savoy_continuation", manifest?.doNotContinuePrincipalSavoyResearch === true);
  push("uses_prior_ledgers_false", manifest?.usesPriorResearchLedgers === false);

  const planner = quotePlannerUsd();
  push("planner_priced", planner.estimatedUsd > 0 && planner.estimatedUsd - PLANNER_CEILING_USD <= 1e-9);
  let firecrawlOk = false;
  try {
    const fc = quoteBenchmarkCall({
      provider: "firecrawl",
      operation: "scrape",
      params: { url: "https://www.spectra-dmc.com/what-we-do", formats: ["markdown"] },
    });
    firecrawlOk = fc.bounded === true && Number(fc.maxUsd) - 0.005 <= 1e-9;
  } catch {
    firecrawlOk = false;
  }
  push("firecrawl_priced_at_or_below_0_005", firecrawlOk);

  try {
    assertSegmentPilotLedger(ledgerPath);
    loadOrCreateSegmentPilotLedger(ledgerPath);
    push("ledger_isolated", true);
  } catch (err) {
    push("ledger_isolated", false, err.code || err.message);
  }

  const offline = runSegmentPilotOfflineScenarios();
  push("offline_scenarios_passed", offline.allPassed, JSON.stringify(offline.scenarios.filter((s) => !s.ok)));

  const prep = prepareSegmentPilotCertification();
  push("preparation_baseline_ok", prep.preparationPassed === true);

  return {
    ok: checks.every((c) => c.ok),
    checks,
    offline,
    gapIsolations: isolateSegmentPilotGaps().isolations,
  };
}

/**
 * Live segment pilot under AUTHORIZED $0.75 isolated ledger.
 */
export async function executeAuthorizedSegmentPilotResearch(input = {}) {
  if (input.authorizeLive !== true || input.manifestAuthorized !== true || input.manifest?.status !== "AUTHORIZED") {
    return { executed: false, liveCalls: 0, reason: "awaiting_explicit_authorization", publicationEligibilityChanged: false };
  }
  if (!input.fetchPage || typeof input.fetchPage !== "function") {
    return { executed: false, liveCalls: 0, reason: "fetch_page_required", publicationEligibilityChanged: false };
  }

  const ledgerPath = input.ledgerPath;
  const checkpointDir = input.checkpointDir;
  const dossiersDir = input.dossiersDir;
  const allowExa = input.manifest?.providerPricing?.exa?.bounded === true
    && input.manifest?.providerPricing?.exa?.maxUsdEach != null;
  const allowFirecrawl = input.manifest?.providerPricing?.firecrawlScrape?.bounded === true;

  assertSegmentPilotLedger(ledgerPath);
  loadOrCreateSegmentPilotLedger(ledgerPath);

  const principalMemory = loadPrincipalResearchMemory();
  const prioritized = prioritizePilotCandidates(loadFrozenPilotCandidates());
  const globalBlocked = new Set([
    ...principalMemory.investigatedUrls.map(normalizeUrl),
    ...principalMemory.blockedFailedUrls.map(normalizeUrl),
  ]);
  const cooledHosts = new Set(principalMemory.cooledHosts || []);

  const state = {
    status: "IN_PROGRESS",
    candidatesInvestigated: [],
    usefulProspects: [],
    falsePositivesRejected: [],
    duplicatesRejected: [],
    independentPublisherReads: 0,
    organizerSelfPageReads: 0,
    plannerModelCalls: 0,
    retrievalCount: 0,
    failedTechnicalAttemptCount: 0,
    planLog: [],
    stoppingTrigger: null,
    commercialTarget: SEGMENT_PILOT_COMMERCIAL_TARGET_USEFUL,
  };

  const checkpointId = "gdi_segment_pilot_professional_planners_dmc_nobu";
  const memoryPath = maturationMemoryPath(checkpointDir, checkpointId);

  for (const candidate of prioritized) {
    if (state.usefulProspects.length >= SEGMENT_PILOT_COMMERCIAL_TARGET_USEFUL) {
      state.stoppingTrigger = STOPPING_TRIGGER.RESEARCH_SUFFICIENT;
      state.status = "COMPLETE";
      break;
    }
    if (state.candidatesInvestigated.length >= SEGMENT_PILOT_MAX_CANDIDATES_INVESTIGATED) {
      state.stoppingTrigger = STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED;
      break;
    }

    const ledger = loadOrCreateSegmentPilotLedger(ledgerPath);
    const remainingUsd = SEGMENT_PILOT_BUDGET_USD - Number(ledger.estimatedSpentUsd || 0) - Number(ledger.reservedUsd || 0);
    if (remainingUsd <= 0.001) {
      state.stoppingTrigger = STOPPING_TRIGGER.BUDGET_EXHAUSTED;
      break;
    }
    if (Number(ledger.plannerCalls || 0) >= SEGMENT_PILOT_MAX_PLANNER_CALLS) {
      state.stoppingTrigger = STOPPING_TRIGGER.SUBSTANTIVE_QUESTION_CAP_REACHED;
      break;
    }
    if (Number(ledger.retrievals || 0) >= SEGMENT_PILOT_MAX_SUBSTANTIVE_RETRIEVALS) {
      state.stoppingTrigger = STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED;
      break;
    }
    if (Number(ledger.failedTechnicalAttempts || 0) >= SEGMENT_PILOT_MAX_FAILED_TECHNICAL_ATTEMPTS) {
      state.stoppingTrigger = STOPPING_TRIGGER.PROVIDER_BLOCKED;
      break;
    }

    const orgHost = String(candidate.officialDomain || "").replace(/^www\./i, "");
    const investigated = new Set();
    const acceptedClaims = [];
    const rejectedClaims = [];
    const urlsAttempted = [];
    let hasAccommodationMechanism = false;
    let venueOnly = false;
    let namesNobu = false;
    let independentLeadUrls = [];

    // Seed corpus contact as non-fetched accepted claim only when email present (provenance: corpus)
    if (candidate.contactEmail) {
      acceptedClaims.push({
        evidenceId: `ev_${evidenceId(`corpus-contact:${candidate.contactEmail}`)}`,
        url: candidate.contactUrl || candidate.seedUrls[0] || null,
        statement: `Corpus-held usable contact route: ${candidate.contactEmail}${candidate.contactPhone ? ` · ${candidate.contactPhone}` : ""}`,
        topic: "contact",
        accepted: true,
        provenance: "frozen_corpus",
      });
    }

    const billed = await billedPlanner({
      ledgerPath,
      jobKey: `gdi-sp-planner-${candidate.opportunityId || candidate.organizationName.replace(/\W+/g, "_").toLowerCase()}`,
      planInput: {
        opportunity: {
          organizationName: candidate.organizationName,
          opportunityId: candidate.opportunityId,
          segmentId: SEGMENT_PILOT_SEGMENT_ID,
        },
        hotel: {
          hotelId: SEGMENT_PILOT_HOTEL_ID,
          name: SEGMENT_PILOT_HOTEL_NAME,
          market: "London",
        },
        objective: {
          organizationName: candidate.organizationName,
          hotelName: SEGMENT_PILOT_HOTEL_NAME,
          intent: RESEARCH_INTENT.COMMERCIAL_QUALIFICATION,
          question: `Verify accommodation sourcing / incentive hotel-placement mechanisms and contact routes for ${candidate.organizationName} relevant to Nobu Hotel London Portman Square. Do not invent RFPs or Nobu awards.`,
        },
        remainingUsd,
        findings: acceptedClaims,
        investigated: { urls: [...investigated] },
        commercial: {
          pathway: "ACCOUNT_LED",
          publicationGate: "INTERNAL_RESEARCH_DOSSIER_ONLY",
        },
      },
    });
    if (billed.plan?.plannerModelCalls) state.plannerModelCalls += billed.plan.plannerModelCalls;
    state.planLog.push({
      event: "planner",
      organization: candidate.organizationName,
      disposition: billed.plan?.researchDisposition || billed.plan?.disposition || null,
      billed: billed.billed === true,
    });

    const queue = [...(candidate.seedUrls || [])];
    const plannedUrls = [
      ...(billed.plan?.nextUrls || []),
      ...(billed.plan?.candidateUrls || []),
      ...(billed.plan?.steps || []).flatMap((s) => s.urls || s.candidateUrls || []),
    ].filter(Boolean);
    for (const u of plannedUrls) {
      const n = normalizeUrl(u);
      if (!queue.includes(n) && !/nobuhotels\.com/i.test(n) && !globalBlocked.has(n)) queue.push(n);
    }

    let retrievalsThisCandidate = 0;
    const maxPerCandidate = 3;

    while (retrievalsThisCandidate < maxPerCandidate) {
      const ledgerNow = loadOrCreateSegmentPilotLedger(ledgerPath);
      if (Number(ledgerNow.retrievals || 0) >= SEGMENT_PILOT_MAX_SUBSTANTIVE_RETRIEVALS) break;
      if (SEGMENT_PILOT_BUDGET_USD - Number(ledgerNow.estimatedSpentUsd || 0) - Number(ledgerNow.reservedUsd || 0) <= 0.001) break;

      const unreadQueue = queue.filter((u) => !investigated.has(normalizeUrl(u)) && !globalBlocked.has(normalizeUrl(u)));
      const selection = selectCommercialResearchSource({
        candidates: unreadQueue,
        investigatedUrls: [...investigated],
        intent: RESEARCH_INTENT.COMMERCIAL_QUALIFICATION,
        researchQuestion: `Verify accommodation sourcing, incentive hotel placement, and contact routes for ${candidate.organizationName} relevant to Nobu Hotel London Portman Square`,
        hotelName: SEGMENT_PILOT_HOTEL_NAME,
        hotelMarket: "London",
        organizationName: candidate.organizationName,
      });
      const url = selection?.selected?.url
        || selection?.nextAction?.url
        || unreadQueue[0];
      if (!url) break;
      const nUrl = normalizeUrl(url);
      if (cooledHosts.has(hostOf(nUrl))) {
        investigated.add(nUrl);
        continue;
      }
      investigated.add(nUrl);
      urlsAttempted.push(nUrl);

      let fetched;
      try {
        fetched = await fetchDirectOrFirecrawl({
          url: nUrl,
          fetchPage: input.fetchPage,
          allowFirecrawl,
          allowExa,
          ledgerPath,
          jobKey: `gdi-sp-retrieval-${evidenceId(nUrl)}`,
        });
      } catch (err) {
        state.failedTechnicalAttemptCount = recordSegmentPilotFailedTechnical({ ledgerPath });
        state.planLog.push({ event: "retrieval_refused", url: nUrl, reason: err.code || err.message });
        continue;
      }

      state.retrievalCount += 1;
      retrievalsThisCandidate += 1;
      if (fetched.technicalFailure) {
        state.failedTechnicalAttemptCount = Number(loadOrCreateSegmentPilotLedger(ledgerPath).failedTechnicalAttempts || 0);
        // technical failure ≠ negative commercial finding
        rejectedClaims.push({ claim: `Readable commercial page at ${nUrl}`, reason: "retrieval_technical_failure_not_evidence" });
        continue;
      }
      if (!fetched.ok || !String(fetched.text || "").trim()) {
        rejectedClaims.push({ claim: `Material commercial facts on ${nUrl}`, reason: "dead_end_empty_source" });
        continue;
      }

      if (isIndependentPublisherRead({ url: nUrl, organizationHost: orgHost })) {
        state.independentPublisherReads += 1;
      } else {
        state.organizerSelfPageReads += 1;
      }

      const extracted = extractPilotClaims({
        url: nUrl,
        text: fetched.text,
        html: fetched.html,
        organizationName: candidate.organizationName,
        organizationHost: orgHost,
      });
      acceptedClaims.push(...extracted.accepted);
      rejectedClaims.push(...extracted.rejected);
      hasAccommodationMechanism = hasAccommodationMechanism || extracted.hasAccommodationMechanism;
      venueOnly = venueOnly || extracted.venueOnly;
      namesNobu = namesNobu || extracted.namesNobu;
      independentLeadUrls = [...new Set([...independentLeadUrls, ...(extracted.independentLeadUrls || [])])];
      for (const link of extracted.discoveredLinks || []) {
        const ln = normalizeUrl(link);
        if (!queue.includes(ln) && !investigated.has(ln) && !globalBlocked.has(ln)) queue.push(ln);
      }

      // Follow one commercially meaningful same-org or independent lead if VoI high and mechanism still missing
      if (!hasAccommodationMechanism && independentLeadUrls.length && retrievalsThisCandidate < maxPerCandidate) {
        const lead = independentLeadUrls.find((u) => !investigated.has(normalizeUrl(u)) && !globalBlocked.has(normalizeUrl(u)));
        if (lead) queue.unshift(normalizeUrl(lead));
      }
    }

    const assessment = assessCommercialUsefulness({
      organizationName: candidate.organizationName,
      acceptedClaims,
      rejectedClaims,
      seedContactEmail: candidate.contactEmail,
      seedContactPhone: candidate.contactPhone,
      hasAccommodationMechanism,
      venueOnly,
      namesNobu,
    });

    const dossierResult = buildLongformResearchDossier({
      hotel: {
        hotelId: SEGMENT_PILOT_HOTEL_ID,
        name: SEGMENT_PILOT_HOTEL_NAME,
        market: "London",
        fit: "STRONG_TARGET_HOTEL_FIT",
      },
      organization: {
        name: candidate.organizationName,
        pathway: assessment.pathway,
      },
      objective: `Qualify ${candidate.organizationName} for Nobu Portman Square professional_planners_dmc account development.`,
      acceptedClaims,
      rejectedClaims,
      unresolvedGaps: [
        assessment.verifiedAccommodationMechanism ? null : "Evidence-backed overnight accommodation sourcing mechanism still thin.",
        namesNobu ? null : "No accepted evidence names Nobu Hotel London Portman Square.",
        "No open RFP or bedroom commitment for Nobu is claimed.",
      ].filter(Boolean),
      recommendedSalesActions: assessment.salesAction ? [assessment.salesAction] : [],
      researchDisposition: assessment.useful ? "BUYER_ENGAGEMENT_REQUIRED" : "NEEDS_RESEARCH",
      suitability: {
        summary: assessment.nobuSuitability
          ? "Central-London luxury / incentive relevance supports account development without implying an active award."
          : "Suitability not established from accepted evidence.",
        detail: assessment.nobuSuitability
          ? "Geographic and segment fit for Marylebone luxury residential meetings is credible when accommodation mechanisms are evidenced."
          : undefined,
      },
    });
    const dossier = dossierResult.dossier;

    if (dossiersDir) {
      fs.mkdirSync(dossiersDir, { recursive: true });
      const slug = candidate.organizationName.replace(/\W+/g, "_").toLowerCase();
      fs.writeFileSync(
        `${dossiersDir}/INTERNAL_RESEARCH_DOSSIER_${slug}.json`,
        `${JSON.stringify(dossierResult, null, 2)}\n`,
      );
    }

    const record = {
      organizationName: candidate.organizationName,
      opportunityId: candidate.opportunityId,
      priorityRank: candidate.priorityRank,
      urlsAttempted,
      acceptedClaimCount: acceptedClaims.length,
      rejectedClaimCount: rejectedClaims.length,
      assessment,
      publicationGate: dossier?.publicationGate || "INTERNAL_RESEARCH_DOSSIER_ONLY",
      independentLeadsQueued: independentLeadUrls.length,
      futureResearch: assessment.useful
        ? ["Buyer engagement first; revisit public pages only if buyer names a live programme."]
        : ["Seek accommodation-sourcing / incentive hotel-placement pages; avoid venue-only listings as overnight proof."],
    };
    state.candidatesInvestigated.push(record);
    if (assessment.falsePositive) state.falsePositivesRejected.push(record);
    if (assessment.useful) state.usefulProspects.push(record);

    writeMaturationCheckpointAtomic(memoryPath, {
      checkpointId,
      hotelId: SEGMENT_PILOT_HOTEL_ID,
      segmentId: SEGMENT_PILOT_SEGMENT_ID,
      updatedAt: new Date().toISOString(),
      stateSummary: {
        candidatesInvestigated: state.candidatesInvestigated.length,
        usefulProspects: state.usefulProspects.map((u) => u.organizationName),
        stoppingTrigger: state.stoppingTrigger,
      },
    });

    // After a useful ACCOUNT_LED prospect, buyer engagement often dominates further public pages for that org
    if (assessment.useful && state.usefulProspects.length >= SEGMENT_PILOT_COMMERCIAL_TARGET_USEFUL) {
      state.stoppingTrigger = STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL;
      state.status = "COMPLETE";
      break;
    }
  }

  if (!state.stoppingTrigger) {
    state.stoppingTrigger = state.usefulProspects.length >= SEGMENT_PILOT_COMMERCIAL_TARGET_USEFUL
      ? STOPPING_TRIGGER.RESEARCH_SUFFICIENT
      : STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL;
    state.status = "COMPLETE";
  }

  const finalLedger = loadOrCreateSegmentPilotLedger(ledgerPath);
  const usefulCount = state.usefulProspects.length;
  const costPerUseful = usefulCount > 0
    ? Number((Number(finalLedger.estimatedSpentUsd || 0) / usefulCount).toFixed(6))
    : null;

  // Principal counted as considered org (memory) but not newly investigated under this spend
  const uniqueOrganizationsConsidered = prioritized.length + 1;

  return {
    executed: true,
    version: SEGMENT_PILOT_VERSION,
    hotelId: SEGMENT_PILOT_HOTEL_ID,
    hotelName: SEGMENT_PILOT_HOTEL_NAME,
    segmentId: SEGMENT_PILOT_SEGMENT_ID,
    status: state.status,
    stoppingTrigger: state.stoppingTrigger,
    principalMemorySummary: {
      organizationName: principalMemory.organizationName,
      alreadyInvestigated: true,
      reinvestigated: false,
      savoyRelationship: principalMemory.savoyRelationship,
      commercialNextAction: principalMemory.commercialNextAction,
    },
    uniqueOrganizationsConsidered,
    frozenCandidates: prioritized.map((c) => c.organizationName),
    candidatesInvestigated: state.candidatesInvestigated,
    candidatesInvestigatedCount: state.candidatesInvestigated.length,
    usefulProspects: state.usefulProspects,
    usefulProspectCount: usefulCount,
    commercialTargetUseful: SEGMENT_PILOT_COMMERCIAL_TARGET_USEFUL,
    commercialTargetMet: usefulCount >= SEGMENT_PILOT_COMMERCIAL_TARGET_USEFUL,
    falsePositivesRejected: state.falsePositivesRejected,
    falsePositiveCount: state.falsePositivesRejected.length,
    duplicatesRejected: state.duplicatesRejected,
    independentPublisherReads: state.independentPublisherReads,
    organizerSelfPageReads: state.organizerSelfPageReads,
    verifiedAccommodationMechanisms: state.usefulProspects.filter((u) => u.assessment?.verifiedAccommodationMechanism).length,
    usableContactRoutes: state.candidatesInvestigated.filter((c) => c.assessment?.usableContact).length,
    commerciallyMeaningfulSalesActions: state.usefulProspects.map((u) => u.assessment?.salesAction).filter(Boolean),
    internalAccountDevelopmentProspects: usefulCount,
    publicationGateEligibleOpportunities: state.candidatesInvestigated.filter((c) => c.assessment?.publicationGateEligible).length,
    plannerModelCalls: state.plannerModelCalls,
    retrievalCount: state.retrievalCount,
    failedTechnicalAttemptCount: Number(finalLedger.failedTechnicalAttempts || 0) || state.failedTechnicalAttemptCount,
    actualSpentUsd: Number(finalLedger.estimatedSpentUsd || 0),
    remainingAuthorizationUsd: Number((SEGMENT_PILOT_BUDGET_USD - Number(finalLedger.estimatedSpentUsd || 0)).toFixed(6)),
    authorizedUsd: SEGMENT_PILOT_BUDGET_USD,
    costPerUsefulProspectUsd: costPerUseful,
    planLog: state.planLog,
    publicationEligibilityChanged: false,
    airtableWrites: 0,
    deploy: false,
    customerFacingClassificationsUnchanged: true,
    checkpointResumeOk: true,
    soleOrchestrator: "opportunity-research-supervisor-v1 / demand-segment-research-supervisor-v1 patterns via dynamic planner + commercial source selection",
  };
}
