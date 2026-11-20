/**
 * Brand Explorer — Dealality Insight advisory quality.
 *
 * Separates internal research/HPC/certification metadata from customer-facing
 * Dealality assessment copy. Production UI must never expose internal language.
 *
 * Contract: BE_DEALALITY_INSIGHT_ADVISORY_V1
 */

export const BE_DEALALITY_INSIGHT_ADVISORY_V1 = "BE_DEALALITY_INSIGHT_ADVISORY_V1";

export const DEALALITY_ASSESSMENT_SECTIONS = Object.freeze([
  {
    id: "overall",
    slotKey: "insight.assessment.overall",
    title: "Overall Brand Assessment",
    required: true,
  },
  {
    id: "opportunity",
    slotKey: "insight.assessment.opportunity",
    title: "Where We See the Greatest Opportunity",
    required: true,
  },
  {
    id: "advantages",
    slotKey: "insight.assessment.advantages",
    title: "Principal Competitive Advantages",
    required: true,
  },
  {
    id: "fit",
    slotKey: "insight.assessment.fit",
    title: "Ideal Owner, Asset & Operator Fit",
    required: true,
  },
  {
    id: "commercial",
    slotKey: "insight.assessment.commercial",
    title: "Commercial & Investment Considerations",
    required: true,
  },
  {
    id: "perspective",
    slotKey: "insight.assessment.perspective",
    title: "Dealality Perspective",
    required: true,
  },
]);

/**
 * Slots that may appear in customer-facing Brand Explorer panels.
 * Broad enough to cover all ten atelier tabs; internal research dumps never publish here.
 */
export const CUSTOMER_FACING_ADVISORY_SLOT_RE =
  /^(?:insight\.|overview\.|valueOwners\.|commercial\.|loyalty\.|footprint\.|materials\.|economics\.|operations\.|standards\.|metric\.)/i;

export const DEALALITY_INSIGHT_FAIL = Object.freeze({
  INTERNAL_LANGUAGE: "internal_technical_language_leakage",
  GENERIC_MARKETING: "generic_marketing_copy",
  UNSUPPORTED_CLAIM: "unsupported_factual_claim",
  MISSING_COMMERCIAL_INTERPRETATION: "missing_commercial_interpretation",
  MISSING_FIT_ANALYSIS: "missing_asset_owner_suitability_analysis",
  EMPTY_SECTION: "empty_advisory_section",
  REPETITIVE: "repetitive_content",
  TITLE_CASE: "incorrect_title_case",
});

/**
 * Semantic + lexical detectors for internal research / pipeline language.
 * Keyword alone is insufficient — patterns cover operator instructions and system states.
 */
export const INTERNAL_CUSTOMER_LANGUAGE_RES = Object.freeze([
  /\bHPC\s+Open\s+hotels\s+matched\b/i,
  /\bRegional\s+HPC\s+counts\b/i,
  /\bHPC\s+(?:open\s+)?(?:footprint|inventory|counts?|rows?)\b/i,
  /\bkeys\s*=\s*\d+/i,
  /\bcertState\b/i,
  /\bLIVE_UNVERIFIED_INVENTORY\b/i,
  /\bsteward[- ]confirmed\b/i,
  /\bkeep\s+narrative\s+HPC[- ]qualified\b/i,
  /\bnamed\s+openings\s+need\s+steward\b/i,
  /\bno\s+fabricated\s+property\s+names\b/i,
  /\bverify\s+certState\s+before\s+customer\s+publish\b/i,
  /\bnot verified in this pack\b/i,
  /\bdo not infer brand-wide totals\b/i,
  /\bCoverage limited\b/i,
  /\bRESEARCH_REQUIRED\b/,
  /\bfield\s+mapping\b/i,
  /\bphase2[a-z]?\b/i,
  /\bslotKey\b/i,
  /\bTODO_RESEARCH\b/i,
  /\bfixture\b/i,
  /\bcandidate\s+overlay\b/i,
  /\binternal\s+preview\b/i,
  /\bdiligence\s+framing\s+only\b/i,
  /\bFocus\s+this\s+card\b/i,
  /\bEvidence-grounded\s+reconstruction\b/i,
  /\bBrand\s+Basics\s+[“"]?Profile\s+Analysis/i,
  /\brec[A-Za-z0-9]{10,}\b/, // raw Airtable record ids in customer prose
]);

export const GENERIC_MARKETING_RES = Object.freeze([
  /^world[- ]class\b/i,
  /^best[- ]in[- ]class\b/i,
  /\bunparalleled\s+guest\s+experience\b/i,
  /\bredefining\s+hospitality\b/i,
  /\bseamless\s+excellence\b/i,
]);

function nz(s) {
  return s == null ? "" : String(s).trim();
}

export function isCustomerFacingAdvisorySlot(slotKey) {
  return CUSTOMER_FACING_ADVISORY_SLOT_RE.test(nz(slotKey));
}

export function detectInternalCustomerLanguage(text) {
  const t = nz(text);
  if (!t) return [];
  const hits = [];
  for (const re of INTERNAL_CUSTOMER_LANGUAGE_RES) {
    if (re.test(t)) hits.push(re.source);
  }
  return hits;
}

export function containsInternalCustomerLanguage(text) {
  return detectInternalCustomerLanguage(text).length > 0;
}

export function detectGenericMarketingCopy(text) {
  const t = nz(text);
  if (!t) return [];
  return GENERIC_MARKETING_RES.filter((re) => re.test(t)).map((re) => re.source);
}

/**
 * True when a candidate body is internal metadata masquerading as editorial.
 * Longer dumps must not win over shorter customer-safe CURRENT copy.
 */
export function isInternalLanguageRegression(candidateText, currentText = "") {
  const candHits = detectInternalCustomerLanguage(candidateText);
  if (!candHits.length) return false;
  // Any internal language in a customer-facing candidate is a regression
  // relative to clean current — and is never publishable even if current is empty.
  void currentText;
  return true;
}

function jaccard(a, b) {
  const ta = new Set(
    nz(a)
      .toLowerCase()
      .split(/\W+/)
      .filter((w) => w.length > 3),
  );
  const tb = new Set(
    nz(b)
      .toLowerCase()
      .split(/\W+/)
      .filter((w) => w.length > 3),
  );
  if (!ta.size || !tb.size) return 0;
  let inter = 0;
  for (const w of ta) if (tb.has(w)) inter += 1;
  return inter / (ta.size + tb.size - inter);
}

/**
 * Curated RIGHT-only advisory packs (proposal overlay — not Airtable writes).
 * Brand-specific analysis is allowed; enforcement logic is universal.
 */
export const DEALALITY_ADVISORY_PACKS = Object.freeze({
  curio: Object.freeze({
    brandId: "receQkxgjlezsc1xg",
    matchNameRe: /curio\s+collection/i,
    parentFamily: "Hilton Worldwide",
    blocks: Object.freeze([
      {
        slotKey: "footprint.editorial",
        title: "Dealality View on Market Presence",
        body:
          "Curio reads as a conversion-weighted upper-upscale soft collection: owners keep a distinctive property story while plugging into Hilton Honors demand and Hilton commercial systems. Treat regional presence as directional for underwriting—not as a substitute for market-level competitive-set work. The most useful footprint signal for owners is whether the asset’s positioning, F&B scope, and design narrative can sustain Curio’s collection standard in that corridor.",
      },
      {
        slotKey: "footprint.editorial_bullets",
        title: "Market Presence Themes",
        body:
          "Conversion and repositioning opportunities in character-rich urban and destination assets\nCulinary-forward F&B and design narrative as operating differentiators—not optional cosmetics\nHilton Honors member demand as a distribution advantage when the property product earns the rate\nFlexible design story relative to hard brands—still with QA, brand standards, and commercial discipline",
      },
      {
        slotKey: "insight.summary",
        title: "Dealality Summary",
        body:
          "Curio Collection by Hilton is most compelling when an owner needs upper-upscale soft-collection positioning with retained property individuality and Hilton Honors distribution—not a limited-service reflag. Underwrite F&B operating model, design PIP, and conversion economics explicitly. It is weaker where ADR cannot support culinary and design investment, or where the operator lacks lifestyle calibration.",
      },
      {
        slotKey: "insight.assessment.overall",
        title: "Overall Brand Assessment",
        body:
          "Curio sits in Hilton’s upper-upscale soft-collection lane: conversion-friendly, design-flexible, and commercially tied to Hilton Honors. For owners, attractiveness comes from combining a distinctive asset story with system distribution—not from standardizing the product into a hard-brand template. The brand is worth shortlisting when the property already has (or can earn) a credible lifestyle narrative and the market will pay for that positioning.",
      },
      {
        slotKey: "insight.assessment.opportunity",
        title: "Where We See the Greatest Opportunity",
        body:
          "Greatest owner value tends to appear in character-rich conversions, premium urban repositioning, destination and experiential resorts, and heritage or adaptive-reuse assets where individuality is the commercial thesis. Opportunity is strongest when distribution lift and design credibility both matter—and weaker when the asset lacks a story the collection can amplify.",
      },
      {
        slotKey: "insight.assessment.advantages",
        title: "Principal Competitive Advantages",
        body:
          "Relative to peer soft collections, Curio’s practical advantages are Hilton Honors demand interoperability, Hilton commercial and reservations infrastructure, and conversion latitude that preserves property identity. Those advantages matter most to owners who need member-driven demand and system reliability without surrendering the asset’s distinctive positioning.",
      },
      {
        slotKey: "insight.assessment.fit",
        title: "Ideal Owner, Asset & Operator Fit",
        body:
          "Best fit: sponsors underwriting upper-upscale lifestyle economics; assets with authentic public spaces, design narrative, and F&B complexity; operators with branded or lifestyle experience and reporting discipline. Poor fit: assets without identity, markets that will not reward premium positioning, operators expecting total design freedom without QA guardrails, or structures that treat Curio like select-service economics.",
      },
      {
        slotKey: "insight.assessment.commercial",
        title: "Commercial & Investment Considerations",
        body:
          "Owners should evaluate Hilton Honors contribution versus retail/OTA mix, affiliation and program costs in current disclosure, design and F&B PIP capital, labor intensity of culinary-forward operations, and competitive set versus other Hilton soft brands in-market. Do not invent fee schedules or ROI claims—confirm commercial terms and ramp assumptions in the deal file before capital allocation.",
      },
      {
        slotKey: "insight.assessment.perspective",
        title: "Dealality Perspective",
        body:
          "Curio is a compelling choice when individuality plus Hilton system access is the investment thesis, and when the owner can fund and operate the product standard that thesis requires. The conditions that matter most are market rate support, honest conversion capital, operator capability, and clear commercial agreement economics—not brochure geography alone.",
      },
    ]),
  }),
  ascend: Object.freeze({
    brandId: "reclkgOzvAcBheUSo",
    matchNameRe: /ascend\s+hotel\s+collection/i,
    parentFamily: "Choice Hotels",
    blocks: Object.freeze([
      {
        slotKey: "footprint.editorial",
        title: "Dealality View on Market Presence",
        body:
          "Ascend Hotel Collection is best read as Choice’s upscale soft-collection path for distinctive conversions and independent-minded assets. Market presence should inform corridor diligence, not replace property-level competitive-set analysis. Owners should judge whether the asset’s identity and service model can earn Ascend’s collection positioning in that market.",
      },
      {
        slotKey: "footprint.editorial_bullets",
        title: "Market Presence Themes",
        body:
          "Soft-collection conversions that preserve local character\nChoice Privileges member demand as a distribution lever when product quality holds\nFlexible design narrative versus hard-brand prototypes\nOwner diligence still required on fees, standards, and competitive set in-market",
      },
      {
        slotKey: "insight.summary",
        title: "Dealality Summary",
        body:
          "Ascend Hotel Collection fits owners who want soft-collection individuality with Choice distribution and loyalty reach—not a highly standardized hard brand. Underwrite conversion capital, local character, and commercial terms explicitly. It is less suitable where the asset cannot support an upscale soft-collection guest promise.",
      },
      {
        slotKey: "insight.assessment.overall",
        title: "Overall Brand Assessment",
        body:
          "Ascend offers an upscale soft-collection affiliation for properties that need retained character with Choice Hotels distribution and Choice Privileges participation. Attractiveness depends on whether the asset’s story and operating model can sustain collection positioning—and whether Choice system access materially improves the owner’s demand and channel mix.",
      },
      {
        slotKey: "insight.assessment.opportunity",
        title: "Where We See the Greatest Opportunity",
        body:
          "Opportunity concentrates in conversions and repositionings where local character is the commercial edge: boutique urban assets, adaptive reuse, and distinctive regional hotels that benefit from loyalty and reservations reach without a hard-brand prototype. Value is weaker when the property lacks a differentiating story.",
      },
      {
        slotKey: "insight.assessment.advantages",
        title: "Principal Competitive Advantages",
        body:
          "Practical advantages include soft-collection design latitude, Choice distribution infrastructure, and Choice Privileges member demand. These matter most to owners who need system reach while keeping a property-specific guest narrative—and who can meet collection standards without over-capitalizing relative to market ADR.",
      },
      {
        slotKey: "insight.assessment.fit",
        title: "Ideal Owner, Asset & Operator Fit",
        body:
          "Best fit: owners seeking soft-brand flexibility; assets with authentic identity; operators comfortable with upscale service expectations and brand QA. Poor fit: commodity boxes without story, markets that will not support soft-collection pricing, or operators expecting hard-brand plug-and-play simplicity.",
      },
      {
        slotKey: "insight.assessment.commercial",
        title: "Commercial & Investment Considerations",
        body:
          "Evaluate loyalty contribution, affiliation economics in current disclosure, conversion PIP scope, operating complexity versus ADR support, and competitive alternatives in the soft-collection set. Confirm fees and standards in deal materials—do not rely on generic marketing claims or invented performance lifts.",
      },
      {
        slotKey: "insight.assessment.perspective",
        title: "Dealality Perspective",
        body:
          "Ascend is most compelling when local character plus Choice system access is the thesis, and when capital and operating plans match that promise. The decisive conditions are product authenticity, market rate support, operator capability, and clear commercial terms.",
      },
    ]),
  }),
});

export function resolveAdvisoryPack(brandOrMeta = {}) {
  const id = nz(brandOrMeta.id || brandOrMeta.brandId || brandOrMeta.recordId);
  const name = nz(brandOrMeta.name || brandOrMeta.brandName);
  for (const pack of Object.values(DEALALITY_ADVISORY_PACKS)) {
    if (id && pack.brandId === id) return pack;
    if (name && pack.matchNameRe.test(name)) return pack;
  }
  return null;
}

function upsertBlock(list, next) {
  const key = nz(next.slotKey);
  const idx = list.findIndex((b) => nz(b.slotKey) === key);
  if (idx >= 0) {
    list[idx] = { ...list[idx], ...next, slotKey: key };
  } else {
    list.push({ ...next, slotKey: key, sort: list.length + 1 });
  }
}

/**
 * Apply advisory quality to proposed blocks:
 * - Strip internal language from customer-facing slots
 * - Upsert curated assessment sections when a pack resolves
 * - Preserve research evidence separately via returned internalEvidence[]
 */
export function applyDealalityInsightAdvisoryToBlocks(blocks, opts = {}) {
  const list = Array.isArray(blocks) ? blocks.map((b) => ({ ...b })) : [];
  const changed = [];
  const internalEvidence = [];
  const pack = resolveAdvisoryPack(opts.brand || opts);

  for (const b of list) {
    const slotKey = nz(b.slotKey || b.slot);
    if (!isCustomerFacingAdvisorySlot(slotKey)) continue;
    const body = nz(b.body);
    const hits = detectInternalCustomerLanguage(`${nz(b.title)}\n${body}`);
    if (!hits.length) continue;
    internalEvidence.push({
      slotKey,
      rejectedBody: body,
      hits,
      note: "Moved out of customer-facing copy; retain in Issues & Research Gaps / internal evidence",
    });
    b.body = "";
    b.title = nz(b.title).replace(/editorial/i, "Advisory").trim() || b.title;
    b.__dealalityInsightAdvisory = {
      action: "stripped_internal_language",
      hits,
    };
    changed.push({
      slotKey,
      reason: DEALALITY_INSIGHT_FAIL.INTERNAL_LANGUAGE,
      action: "stripped_internal_language",
    });
  }

  if (pack) {
    for (const row of pack.blocks) {
      const prev = list.find((b) => nz(b.slotKey) === row.slotKey);
      const prevBody = nz(prev && prev.body);
      if (prevBody === row.body && nz(prev && prev.title) === row.title) continue;
      const isAssessment = /^insight\.assessment\./.test(row.slotKey);
      const prevInternal = containsInternalCustomerLanguage(prevBody);
      const prevMetaNote =
        /Brand Footprint-scoped|Absence in one region is not a global zero|HPC Open hotels matched/i.test(
          prevBody,
        );
      const prevWeak = !prevBody || prevInternal || prevMetaNote || prevBody.length < 80;
      // Always populate structured assessment sections on the proposed profile.
      // For summary / footprint editorial, replace only when current is weak or internal.
      if (!isAssessment && !prevWeak) continue;
      upsertBlock(list, {
        ...row,
        __dealalityInsightAdvisory: {
          action: "applied_advisory_pack",
          packBrandId: pack.brandId,
          previousBody: prevBody || null,
        },
      });
      changed.push({
        slotKey: row.slotKey,
        reason: "advisory_pack_applied",
        action: "applied_advisory_pack",
        from: prevBody || null,
        to: row.body,
      });
    }
  }

  return {
    contract: BE_DEALALITY_INSIGHT_ADVISORY_V1,
    blocks: list,
    changed,
    changedCount: changed.length,
    internalEvidence,
    packBrandId: pack ? pack.brandId : null,
  };
}

export function validateDealalityInsightAdvisory(blocks, opts = {}) {
  const list = Array.isArray(blocks) ? blocks : [];
  const failures = [];
  const bySlot = new Map();
  for (const b of list) {
    const slotKey = nz(b.slotKey || b.slot);
    if (!slotKey) continue;
    bySlot.set(slotKey, b);
    if (!isCustomerFacingAdvisorySlot(slotKey)) continue;
    const blob = `${nz(b.title)}\n${nz(b.body)}`;
    for (const hit of detectInternalCustomerLanguage(blob)) {
      failures.push({
        code: DEALALITY_INSIGHT_FAIL.INTERNAL_LANGUAGE,
        slotKey,
        detail: hit,
      });
    }
    for (const hit of detectGenericMarketingCopy(blob)) {
      failures.push({
        code: DEALALITY_INSIGHT_FAIL.GENERIC_MARKETING,
        slotKey,
        detail: hit,
      });
    }
  }

  if (opts.requireAssessmentStructure) {
    for (const sec of DEALALITY_ASSESSMENT_SECTIONS) {
      const b = bySlot.get(sec.slotKey);
      const body = nz(b && b.body);
      if (!body || body.length < 80) {
        failures.push({
          code: DEALALITY_INSIGHT_FAIL.EMPTY_SECTION,
          slotKey: sec.slotKey,
          detail: sec.title,
        });
      } else if (
        sec.id === "commercial" &&
        !/(owner|fee|capital|loyalty|distribution|ADR|PIP|operator|commercial)/i.test(body)
      ) {
        failures.push({
          code: DEALALITY_INSIGHT_FAIL.MISSING_COMMERCIAL_INTERPRETATION,
          slotKey: sec.slotKey,
          detail: "commercial section lacks owner/commercial vocabulary",
        });
      } else if (
        sec.id === "fit" &&
        !/(owner|asset|operator|fit|suitable|conversion|sponsor)/i.test(body)
      ) {
        failures.push({
          code: DEALALITY_INSIGHT_FAIL.MISSING_FIT_ANALYSIS,
          slotKey: sec.slotKey,
          detail: "fit section lacks owner/asset/operator framing",
        });
      }
    }
  }

  // Near-duplicate assessment bodies
  const assessmentBodies = DEALALITY_ASSESSMENT_SECTIONS.map((s) =>
    nz(bySlot.get(s.slotKey) && bySlot.get(s.slotKey).body),
  ).filter(Boolean);
  for (let i = 0; i < assessmentBodies.length; i++) {
    for (let j = i + 1; j < assessmentBodies.length; j++) {
      if (jaccard(assessmentBodies[i], assessmentBodies[j]) >= 0.85) {
        failures.push({
          code: DEALALITY_INSIGHT_FAIL.REPETITIVE,
          slotKey: DEALALITY_ASSESSMENT_SECTIONS[i].slotKey,
          detail: `near_duplicate_of_${DEALALITY_ASSESSMENT_SECTIONS[j].slotKey}`,
        });
      }
    }
  }

  return {
    contract: BE_DEALALITY_INSIGHT_ADVISORY_V1,
    ok: failures.length === 0,
    pass: failures.length === 0,
    failures,
    failureCount: failures.length,
  };
}

/** Browser/atelier gate: never return internal language to customer DOM. */
export function sanitizeCustomerFacingText(text) {
  const t = nz(text);
  if (!t) return "";
  if (containsInternalCustomerLanguage(t)) return "";
  return t;
}
