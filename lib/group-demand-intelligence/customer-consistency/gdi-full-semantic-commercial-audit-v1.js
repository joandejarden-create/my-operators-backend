/**
 * GDI Full Semantic & Commercial Consistency Audit V1
 *
 * Layer D: paragraph-level review of FINAL customer-facing drawer/card text
 * after sanitize → commercial remediation → DTO → DealalityGdiUi render.
 *
 * This is distinct from gdi-full-customer-consistency-audit-v1 (Layers A–C
 * deterministic / structural). Do not claim content quality = 100 without
 * completing semanticCoverage.opportunitiesReviewed === inventory size.
 *
 * Paid external model calls require --authorize-paid-semantic.
 * Default semantic engine: cursor-agent (reviews produced offline by agents
 * reading rendered packs) or built-in cross-section semantic heuristics plus
 * mandatory agent/LLM reviews recorded in the coverage manifest.
 */

import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { sanitizeOpportunityForCustomerPresentation } from "../customer-presentation-sanitize-v1.js";
import { sanitizeCommercialPresentation } from "../commercial-consistency/commercial-presentation-sanitizer-v1.js";
import {
  applyCmiLedgerRemediation,
  annotateInventoryPopulations,
} from "../bethesda-67-sales-baseline-cmi-v1.js";
import { toOpportunityListDto } from "../opportunity-list-dto.js";
import { loadOpportunitiesCanonical } from "../opportunity-persistence.js";
import { detectOpportunityContradictions } from "../commercial-consistency/commercial-contradiction-detector-v1.js";
import { classifyOpportunityTypeFilter } from "../opportunity-type-filter-v1.js";
import { composeCustomerFacingHotelSourcingLabel } from "../commercial-consistency/commercial-status-dimensions-v1.js";
import { discoverGdiParityHotelUniverse } from "../presentation/gdi-portfolio-full-content-parity-v1.js";
import {
  listAuditHotelIds,
  GDI_FULL_CUSTOMER_CONSISTENCY_AUDIT_V1,
} from "./gdi-full-customer-consistency-audit-v1.js";

export const GDI_FULL_SEMANTIC_COMMERCIAL_AUDIT_V1 =
  "gdi-full-semantic-commercial-audit-v1";

export const SEMANTIC_SECTION_IDS = Object.freeze([
  "title",
  "current_status",
  "opportunity_overview",
  "timing",
  "location",
  "hotel_sourcing",
  "room_demand",
  "demand_cycle",
  "attendance",
  "peak_rooms",
  "what_is_happening",
  "commercial_recommendation",
  "confirmed_competitor_business",
  "potential_future_opportunity",
  "potential_business_for_this_hotel",
  "why_this_hotel",
  "advantages",
  "limitations",
  "why_act_now",
  "established_facts_uncertainties",
  "who_to_contact",
  "recommended_next_step",
  "qualification_questions",
  "supporting_evidence",
  "hotel_validation",
  "action",
  "outcome",
  "card_summary",
  "card_priority",
  "card_sales_action",
  "card_timing",
  "other_drawer_text",
]);

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const UI_PATH = path.join(
  ROOT,
  "public/js/group-demand-intelligence/dealality-gdi-ui.js"
);

let _uiCache = null;

export function loadDealalityGdiUi() {
  if (_uiCache) return _uiCache;
  const code = fs.readFileSync(UI_PATH, "utf8");
  const sandbox = { window: {}, console, URL, URLSearchParams };
  sandbox.window = sandbox;
  vm.runInNewContext(code, sandbox, { timeout: 30000 });
  if (!sandbox.DealalityGdiUi?.intelligenceDetailHtml) {
    throw new Error("DealalityGdiUi.intelligenceDetailHtml unavailable");
  }
  _uiCache = sandbox.DealalityGdiUi;
  return _uiCache;
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|li|h\d|tr|section)>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

const SECTION_TITLE_MAP = Object.freeze({
  "opportunity overview": "opportunity_overview",
  overview: "opportunity_overview",
  timing: "timing",
  location: "location",
  "hotel sourcing": "hotel_sourcing",
  "room demand": "room_demand",
  "demand cycle": "demand_cycle",
  attendance: "attendance",
  "peak rooms": "peak_rooms",
  "what is happening": "what_is_happening",
  "what is happening?": "what_is_happening",
  "confirmed competitor business": "confirmed_competitor_business",
  "potential future opportunity": "potential_future_opportunity",
  "potential business for this hotel": "potential_business_for_this_hotel",
  "why this hotel": "why_this_hotel",
  "why this hotel?": "why_this_hotel",
  advantages: "advantages",
  "limitations / trade-offs": "limitations",
  limitations: "limitations",
  "why act now": "why_act_now",
  "why act now / timing": "why_act_now",
  "established facts and uncertainties": "established_facts_uncertainties",
  "established facts": "established_facts_uncertainties",
  uncertainties: "established_facts_uncertainties",
  "who to contact": "who_to_contact",
  "recommended next step": "recommended_next_step",
  "qualification questions": "qualification_questions",
  "supporting evidence": "supporting_evidence",
  evidence: "supporting_evidence",
  "hotel validation": "hotel_validation",
  validation: "hotel_validation",
  action: "action",
  outcome: "outcome",
  "current status": "current_status",
  "commercial recommendation": "commercial_recommendation",
  "commercial context": "commercial_recommendation",
});

/**
 * Parse rendered drawer HTML into named section texts.
 */
export function extractDrawerSectionsFromHtml(html) {
  const sections = Object.fromEntries(SEMANTIC_SECTION_IDS.map((k) => [k, ""]));
  const raw = String(html || "");
  if (!raw) return sections;

  const byDataId = [];
  const dataRe =
    /<section[^>]*data-section-id=["']([^"']+)["'][^>]*>([\s\S]*?)<\/section>/gi;
  let m;
  while ((m = dataRe.exec(raw))) {
    byDataId.push({ id: m[1], html: m[2] });
  }
  for (const block of byDataId) {
    const key =
      SECTION_TITLE_MAP[text(block.id).toLowerCase()] ||
      text(block.id)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_");
    const body = stripHtml(block.html);
    if (SEMANTIC_SECTION_IDS.includes(key)) {
      sections[key] = [sections[key], body].filter(Boolean).join("\n");
    } else {
      sections.other_drawer_text = [sections.other_drawer_text, body]
        .filter(Boolean)
        .join("\n");
    }
  }

  // Heading-based split for frameworks that omit data-section-id
  const headingRe = /<h[23][^>]*>([\s\S]*?)<\/h[23]>/gi;
  const parts = [];
  let lastIndex = 0;
  let hm;
  const indices = [];
  while ((hm = headingRe.exec(raw))) {
    indices.push({
      title: stripHtml(hm[1]),
      start: hm.index,
      contentStart: hm.index + hm[0].length,
    });
  }
  for (let i = 0; i < indices.length; i++) {
    const end = i + 1 < indices.length ? indices[i + 1].start : raw.length;
    parts.push({
      title: indices[i].title,
      html: raw.slice(indices[i].contentStart, end),
    });
  }
  for (const part of parts) {
    const mapped = SECTION_TITLE_MAP[text(part.title).toLowerCase()];
    if (!mapped) continue;
    const body = stripHtml(part.html);
    if (!body) continue;
    if (!sections[mapped] || body.length > sections[mapped].length) {
      sections[mapped] = body;
    }
  }

  // Overview grid cells (Timing / Location / Hotel Sourcing etc.)
  const cellRe =
    /<(?:dt|th|div)[^>]*class=["'][^"']*(?:gdi-overview-label|overview-label|label)[^"']*["'][^>]*>([\s\S]*?)<\/(?:dt|th|div)>[\s\S]{0,40}<(?:dd|td|div)[^>]*>([\s\S]*?)<\/(?:dd|td|div)>/gi;
  let cm;
  while ((cm = cellRe.exec(raw))) {
    const label = text(stripHtml(cm[1])).toLowerCase();
    const value = text(stripHtml(cm[2]));
    const mapped = SECTION_TITLE_MAP[label];
    if (mapped && value && !sections[mapped]) sections[mapped] = value;
  }

  sections.other_drawer_text = text(
    [sections.other_drawer_text, stripHtml(raw).slice(0, 12000)]
      .filter(Boolean)
      .join("\n")
  ).slice(0, 12000);

  return sections;
}

/**
 * Present one opportunity exactly as customer surfaces see it.
 */
export function presentOpportunityForSemanticAudit(raw, hotelId, hotelName) {
  const rem = sanitizeCommercialPresentation(
    applyCmiLedgerRemediation({ ...raw })
  );
  const annotated =
    annotateInventoryPopulations([rem], { hotelId })[0] || rem;
  const sanitized = sanitizeOpportunityForCustomerPresentation({
    ...annotated,
    hotelId,
    hotelDisplayName: hotelName || hotelId,
  });
  const dto = toOpportunityListDto(sanitized, { hotelId });
  return { rem: annotated, sanitized, dto };
}

export function renderCustomerFacingPack(raw, hotelId, hotelName, ui = null) {
  const UI = ui || loadDealalityGdiUi();
  const { rem, sanitized, dto } = presentOpportunityForSemanticAudit(
    raw,
    hotelId,
    hotelName
  );
  let html = "";
  let renderError = null;
  try {
    html = UI.intelligenceDetailHtml(
      sanitized,
      {},
      {
        whoContactHtml: "",
        validationHtml:
          '<section data-section-id="hotel_validation"><h3>Hotel Validation</h3><div class="gdi-validation-panel">Hotel Validation / Action / Outcome</div></section>',
      }
    );
  } catch (e) {
    renderError = e?.message || String(e);
  }
  const sections = extractDrawerSectionsFromHtml(html);
  sections.title = text(
    dto.displayTitle || sanitized.displayTitle || sanitized.title || rem.title
  );
  sections.card_summary = text(
    dto.tileCustomerSummary ||
      sanitized.tileCustomerSummary ||
      sanitized.whatIsHappening ||
      rem.summaryWhat
  );
  sections.card_priority = text(
    dto.priorityLabel || sanitized.priorityLabel || sanitized.priority || rem.priority
  );
  sections.card_sales_action = text(
    dto.salesActionPillLabel ||
      sanitized.salesActionPillLabel ||
      dto.recommendedAction ||
      sanitized.recommendedAction ||
      rem.recommendedAction
  );
  sections.card_timing = text(
    dto.timingLabel ||
      sanitized.timingLabel ||
      dto.eventsOpportunityOverview?.timing ||
      sections.timing
  );
  sections.hotel_sourcing =
    sections.hotel_sourcing ||
    text(
      dto.hotelSourcingStatusLabel ||
        dto.eventsOpportunityOverview?.hotelSourcing ||
        composeCustomerFacingHotelSourcingLabel(sanitized)
    );
  sections.commercial_recommendation =
    sections.commercial_recommendation ||
    text(dto.recommendedAction || sanitized.recommendedAction || rem.recommendedAction);
  sections.current_status =
    sections.current_status ||
    text(
      sanitized.currentStatusLabel ||
        sanitized.commercialStatusLabel ||
        dto.hotelSourcingStatusLabel
    );

  const populatedSections = SEMANTIC_SECTION_IDS.filter((k) =>
    text(sections[k])
  );

  return {
    hotelId,
    hotelName: hotelName || hotelId,
    opportunityId: String(dto.id || sanitized.id || rem.id || raw.id || ""),
    customerVisibility: sanitized.customerVisibility || rem.customerVisibility || null,
    inventoryPopulations:
      sanitized.inventoryPopulations || rem.inventoryPopulations || null,
    demandSegmentId: sanitized.demandSegmentId || rem.demandSegmentId || null,
    opportunityType: sanitized.opportunityType || rem.opportunityType || null,
    opportunityTypeFilter: classifyOpportunityTypeFilter(sanitized),
    displayTitle: sections.title,
    card: {
      title: sections.title,
      summary: sections.card_summary,
      priority: sections.card_priority,
      salesAction: sections.card_sales_action,
      timing: sections.card_timing,
      sourcing: sections.hotel_sourcing,
    },
    sections,
    populatedSections,
    fullDrawerText: stripHtml(html).slice(0, 16000),
    htmlLength: html.length,
    renderError,
    deterministicContradictions: detectOpportunityContradictions(sanitized),
  };
}

/**
 * Cross-section semantic contradiction heuristics (meaning-level patterns).
 * Complements, does not replace, LLM/agent paragraph review.
 */
export function detectCrossSectionSemanticIssues(pack) {
  const findings = [];
  const s = pack.sections || {};
  const blob = (keys) => keys.map((k) => text(s[k])).filter(Boolean).join(" || ");

  const sourcing = text(s.hotel_sourcing);
  const what = text(s.what_is_happening);
  const why = text(s.why_this_hotel);
  const next = text(s.recommended_next_step || s.commercial_recommendation);
  const facts = text(s.established_facts_uncertainties);
  const competitor = text(s.confirmed_competitor_business);
  const future = text(s.potential_future_opportunity);
  const limitations = text(s.limitations);
  const actNow = text(s.why_act_now);
  const roomDemand = text(s.room_demand);
  const cardAction = text(s.card_sales_action);
  const cardSummary = text(s.card_summary);

  const competitorConfirmed =
    /competitor venue confirmed|confirmed at |booked at |contracted (?:at|with)|host hotel contracted/i.test(
      `${sourcing} ${competitor} ${what}`
    );
  const targetOpen =
    /hotel sourcing open|request (an )?rfp|send .*rfp|pursue as host|win the (primary )?block/i.test(
      `${sourcing} ${next} ${cardAction} ${what}`
    );
  const targetUnconfirmed =
    /hotel sourcing unconfirmed|overflow sourcing unconfirmed|target-hotel overflow/i.test(
      sourcing
    );
  // Do NOT treat "historical attendance" / "used historically" as a concluded event.
  const historical =
    /\balready occurred\b|\bpast edition\b|no longer commercially|event has concluded|prior[- ]year booking|this cycle has closed|do not pursue this cycle/i.test(
      `${what} ${facts} ${future} ${actNow}`
    );
  const pursueNow =
    /\bpursue now\b|\bimmediate pursuit\b|\bqualify (opportunity|overflow)\b|\brequest (an )?rfp\b|\bpursue open business\b/i.test(
      `${next} ${cardAction}`
    ) && !/monitor for|do not claim open rfp|do not re-ask|past cycle/i.test(`${next} ${cardAction}`);
  const roomUnconfirmed =
    /room demand (is )?(unconfirmed|unknown|not established)|rooms (not|un)confirmed/i.test(
      `${roomDemand} ${facts}`
    );
  const roomConfirmedClaim =
    /confirmed (room|overflow) demand|contracted room block|confirmed peak rooms/i.test(
      `${roomDemand} ${what} ${competitor}`
    );
  const strongFit = /strong (fit|match)|highly suitable|ideal for this hotel/i.test(
    why
  );
  const majorLimit =
    /major (geographic|operational) (limit|constraint)|outside (primary )?catchment|not a realistic fit|poor fit/i.test(
      `${limitations} ${facts}`
    );

  if (competitorConfirmed && targetOpen && !/overflow/i.test(`${next} ${cardAction}`)) {
    findings.push({
      code: "SEM_COMPETITOR_CONFIRMED_VS_OPEN_HOST_PATH",
      severity: "P0",
      sections: ["hotel_sourcing", "recommended_next_step", "commercial_recommendation"],
      message:
        "Competitor venue reads confirmed while recommendation still implies open host pursuit / RFP.",
      passages: {
        hotel_sourcing: sourcing.slice(0, 240),
        recommended_next_step: next.slice(0, 240),
        card_sales_action: cardAction.slice(0, 120),
      },
    });
  }
  if (historical && pursueNow) {
    findings.push({
      code: "SEM_HISTORICAL_VS_PURSUE_NOW",
      severity: "P1",
      sections: ["what_is_happening", "why_act_now", "recommended_next_step"],
      message:
        "Narrative frames opportunity as historical / concluded while action language urges immediate pursuit.",
      passages: {
        what_is_happening: what.slice(0, 240),
        why_act_now: actNow.slice(0, 240),
        recommended_next_step: next.slice(0, 240),
      },
    });
  }
  if (roomUnconfirmed && roomConfirmedClaim) {
    findings.push({
      code: "SEM_ROOM_DEMAND_CONFIRMED_VS_UNCONFIRMED",
      severity: "P1",
      sections: ["room_demand", "what_is_happening", "established_facts_uncertainties"],
      message: "Room demand described as both unconfirmed and confirmed across sections.",
      passages: {
        room_demand: roomDemand.slice(0, 200),
        what_is_happening: what.slice(0, 200),
      },
    });
  }
  if (strongFit && majorLimit) {
    findings.push({
      code: "SEM_STRONG_FIT_VS_MAJOR_LIMITATION",
      severity: "P1",
      sections: ["why_this_hotel", "limitations"],
      message:
        "Why This Hotel claims strong suitability while Limitations establish major geographic/operational constraints.",
      passages: {
        why_this_hotel: why.slice(0, 220),
        limitations: limitations.slice(0, 220),
      },
    });
  }
  if (
    cardSummary &&
    what &&
    /hotel sourcing open/i.test(cardSummary) &&
    /competitor venue confirmed/i.test(sourcing)
  ) {
    findings.push({
      code: "SEM_CARD_VS_DRAWER_SOURCING",
      severity: "P1",
      sections: ["card_summary", "hotel_sourcing"],
      message: "Card summary sourcing story conflicts with drawer Hotel Sourcing label.",
      passages: {
        card_summary: cardSummary.slice(0, 200),
        hotel_sourcing: sourcing.slice(0, 200),
      },
    });
  }
  if (
    text(s.title) &&
    text(pack.card?.title) &&
    text(s.title) !== text(pack.card.title)
  ) {
    findings.push({
      code: "SEM_CARD_DRAWER_TITLE_MISMATCH",
      severity: "P2",
      sections: ["title"],
      message: "Card title and drawer title diverge.",
      passages: { card: pack.card.title, drawer: s.title },
    });
  }

  // Repetition / robotic patterns
  const paragraphs = blob([
    "what_is_happening",
    "why_this_hotel",
    "why_act_now",
    "potential_business_for_this_hotel",
    "recommended_next_step",
  ])
    .split(/\s*\|\|\s*|\n+/)
    .map(text)
    .filter((p) => p.length > 40);
  const seen = new Map();
  for (const p of paragraphs) {
    const key = p.toLowerCase().slice(0, 120);
    seen.set(key, (seen.get(key) || 0) + 1);
  }
  for (const [key, count] of seen) {
    if (count >= 2) {
      findings.push({
        code: "SEM_DUPLICATE_PARAGRAPH",
        severity: "P2",
        sections: ["what_is_happening", "why_this_hotel", "recommended_next_step"],
        message: `Near-duplicate customer paragraph repeated ${count} times.`,
        passages: { sample: key },
      });
      break;
    }
  }
  if (/Qualify Overflow\s*[—–-]\s*Qualify Overflow/i.test(next + cardAction)) {
    findings.push({
      code: "SEM_DUPLICATE_ACTION_PREFIX",
      severity: "P1",
      sections: ["recommended_next_step", "card_sales_action"],
      message: "Duplicated Qualify Overflow action prefix.",
      passages: { next: next.slice(0, 160) },
    });
  }
  if (
    /\b(Webhound|Airtable|fixture|auto_promote|ontology|Census record)\b/i.test(
      pack.fullDrawerText || ""
    )
  ) {
    findings.push({
      code: "SEM_INTERNAL_ENGINE_LANGUAGE",
      severity: "P1",
      sections: ["other_drawer_text"],
      message: "Internal engine / platform language leaked into customer-facing copy.",
      passages: { snippet: text(pack.fullDrawerText).slice(0, 200) },
    });
  }
  if (/dated in Host hotel contracted|is Host hotel contracted/i.test(pack.fullDrawerText || "")) {
    findings.push({
      code: "SEM_MALFORMED_HOST_CONTRACTED_GLUE",
      severity: "P1",
      sections: ["location", "what_is_happening"],
      message: "Malformed Host-hotel-contracted template glue in customer text.",
      passages: { snippet: text(pack.fullDrawerText).match(/[^.]{0,40}Host hotel contracted[^.]{0,40}/i)?.[0] },
    });
  }

  return findings;
}

/**
 * Build empty semantic review scaffold (filled by LLM/agent pass).
 */
export function emptySemanticReview(pack, crossFindings = []) {
  return {
    hotelId: pack.hotelId,
    opportunityId: pack.opportunityId,
    displayTitle: pack.displayTitle,
    sectionsReviewed: pack.populatedSections || [],
    reviewStatus: "PENDING_SEMANTIC_LLM",
    reviewEngine: null,
    scores: {
      contentQuality: null,
      commercialAccuracy: null,
      consistency: null,
      readability: null,
      evidenceReliability: null,
    },
    contradictions: crossFindings.filter((f) =>
      /VS_|MISMATCH|CONTRADICT/i.test(f.code)
    ),
    unsupportedClaims: [],
    duplicateOrUnnecessaryText: crossFindings.filter((f) =>
      /DUPLICATE|INTERNAL_ENGINE|MALFORMED/i.test(f.code)
    ),
    conflictingRecommendedActions: crossFindings.filter((f) =>
      /PURSUE|RFP|HOST_PATH/i.test(f.code)
    ),
    suggestedCorrections: [],
    evidence: { crossSectionFindings: crossFindings },
    confidence: null,
    notes: null,
  };
}

/**
 * Inventory operational hotels + opportunities for semantic audit.
 */
export async function buildSemanticAuditInventory(opts = {}) {
  const hotelIds = opts.hotelIds || (await listAuditHotelIds(opts));
  const parityUniverse = discoverGdiParityHotelUniverse(opts);
  const parityById = new Map(parityUniverse.map((h) => [h.hotelId, h]));

  const hotels = [];
  const opportunities = [];
  const excludedParityHotels = [];

  for (const h of parityUniverse) {
    if (!hotelIds.includes(h.hotelId)) {
      excludedParityHotels.push({
        hotelId: h.hotelId,
        hotelName: h.hotelName,
        sources: h.sources,
        reason:
          "Selectable / share / registry hotel without opportunities.json inventory — excluded from opportunity semantic review (zero opportunity rows).",
        publishedOpportunityCount: h.publishedOpportunityCount || 0,
      });
    }
  }

  for (const hotelId of hotelIds) {
    const meta = parityById.get(hotelId) || { hotelId, hotelName: hotelId };
    let doc;
    try {
      doc = await loadOpportunitiesCanonical(hotelId);
    } catch (err) {
      hotels.push({
        hotelId,
        hotelName: meta.hotelName || hotelId,
        loadError: err.message,
        opportunityCount: 0,
      });
      continue;
    }
    const opps = Array.isArray(doc.opportunities) ? doc.opportunities : [];
    hotels.push({
      hotelId,
      hotelName: meta.hotelName || hotelId,
      sources: meta.sources || ["opportunities_store"],
      opportunityCount: opps.length,
    });
    for (const raw of opps) {
      opportunities.push({
        hotelId,
        hotelName: meta.hotelName || hotelId,
        opportunityId: String(raw.id || raw.opportunityId || ""),
        raw,
      });
    }
  }

  return {
    schema: GDI_FULL_SEMANTIC_COMMERCIAL_AUDIT_V1,
    priorAuditSchema: GDI_FULL_CUSTOMER_CONSISTENCY_AUDIT_V1,
    generatedAt: new Date().toISOString(),
    hotels,
    opportunities,
    excludedParityHotels,
    totals: {
      hotelsWithInventory: hotels.filter((h) => h.opportunityCount > 0).length,
      opportunities: opportunities.length,
      excludedParityHotels: excludedParityHotels.length,
      priorAuditHotels: 17,
      priorAuditOpportunities: 699,
    },
  };
}

/**
 * Render every opportunity into a compact semantic pack.
 */
export async function renderAllSemanticPacks(inventory, opts = {}) {
  const ui = opts.ui || loadDealalityGdiUi();
  const packs = [];
  const errors = [];
  for (const row of inventory.opportunities) {
    try {
      const pack = renderCustomerFacingPack(
        row.raw,
        row.hotelId,
        row.hotelName,
        ui
      );
      const cross = detectCrossSectionSemanticIssues(pack);
      packs.push({
        ...pack,
        crossSectionFindings: cross,
        // drop raw-sized fields from memory when writing batches
        _omitHtml: true,
      });
    } catch (e) {
      errors.push({
        hotelId: row.hotelId,
        opportunityId: row.opportunityId,
        error: e?.message || String(e),
      });
    }
  }
  return { packs, errors };
}

/**
 * Scorecard methodology (explicit).
 *
 * Overall =
 *   0.20 * technicalConsistency +
 *   0.25 * semanticConsistency +
 *   0.25 * commercialAccuracy +
 *   0.15 * evidenceReliability +
 *   0.10 * readability +
 *   0.05 * researchCompleteness
 *
 * Semantic dimensions remain null / incomplete until reviewStatus=REVIEWED
 * for 100% of in-scope opportunities.
 */
export function computeScorecard({
  packs = [],
  reviews = [],
  researchRequiredCount = 0,
  technicalConsistencyScore = null,
} = {}) {
  const byId = new Map(
    reviews.map((r) => [`${r.hotelId}::${r.opportunityId}`, r])
  );
  const reviewed = reviews.filter((r) => r.reviewStatus === "REVIEWED");
  const pending = packs.length - reviewed.length;
  const coveragePct =
    packs.length === 0 ? 0 : (reviewed.length / packs.length) * 100;

  const avg = (arr) =>
    arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;

  const contentScores = reviewed
    .map((r) => r.scores?.contentQuality)
    .filter((n) => typeof n === "number");
  const commercialScores = reviewed
    .map((r) => r.scores?.commercialAccuracy)
    .filter((n) => typeof n === "number");
  const consistencyScores = reviewed
    .map((r) => r.scores?.consistency)
    .filter((n) => typeof n === "number");
  const readabilityScores = reviewed
    .map((r) => r.scores?.readability)
    .filter((n) => typeof n === "number");
  const evidenceScores = reviewed
    .map((r) => r.scores?.evidenceReliability)
    .filter((n) => typeof n === "number");

  const semanticConsistency = avg(consistencyScores);
  const commercialAccuracy = avg(commercialScores);
  const readability = avg(readabilityScores);
  const evidenceReliability = avg(evidenceScores);
  const contentQuality = avg(contentScores);

  const researchCompleteness = Math.max(
    0,
    100 - Math.min(100, (researchRequiredCount / Math.max(packs.length, 1)) * 100)
  );

  const technical =
    typeof technicalConsistencyScore === "number"
      ? technicalConsistencyScore
      : 100;

  const complete = pending === 0 && packs.length > 0;
  let overall = null;
  if (complete) {
    overall =
      technical * 0.2 +
      (semanticConsistency ?? 0) * 0.25 +
      (commercialAccuracy ?? 0) * 0.25 +
      (evidenceReliability ?? 0) * 0.15 +
      (readability ?? 0) * 0.1 +
      researchCompleteness * 0.05;
  }

  return {
    methodology: {
      weights: {
        technicalConsistency: 0.2,
        semanticConsistency: 0.25,
        commercialAccuracy: 0.25,
        evidenceReliability: 0.15,
        readability: 0.1,
        researchCompleteness: 0.05,
      },
      rule: "Overall computed only when semanticCoverage is 100% REVIEWED.",
      note: "Do not equate prior Layer A–C presentation score with semantic content quality.",
    },
    coverage: {
      opportunitiesInScope: packs.length,
      semanticallyReviewed: reviewed.length,
      pendingSemanticReview: pending,
      coveragePct: Math.round(coveragePct * 10) / 10,
      complete,
    },
    dimensions: {
      technicalConsistency: technical,
      semanticConsistency,
      commercialAccuracy,
      evidenceReliability,
      readability,
      contentQuality,
      researchCompleteness,
    },
    overall: overall == null ? null : Math.round(overall * 10) / 10,
    overallStatus: complete
      ? "COMPUTED"
      : "INCOMPLETE_SEMANTIC_COVERAGE — overall withheld",
    byIdSize: byId.size,
  };
}

export function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
}

export function defaultReportDir() {
  return path.join(ROOT, "reports", GDI_FULL_SEMANTIC_COMMERCIAL_AUDIT_V1);
}
