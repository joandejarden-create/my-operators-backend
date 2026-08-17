/**
 * v40B — Post-apply copy quality patterns (forbidden + mechanical + brand signals).
 * Read-only helpers for founder review packets.
 */

export const V40B_VERSION = "v40b";

/** Visible forbidden language after v40 scrub (owner-facing). */
export const V40B_FORBIDDEN_LANGUAGE = Object.freeze([
  { id: "loi", re: /\bLOI\b/, label: "LOI" },
  { id: "fdd", re: /\bFDD\b/, label: "FDD" },
  { id: "item_19", re: /\bItem\s*19\b/i, label: "Item 19" },
  { id: "franchise_disclosure", re: /\bfranchise disclosure\b/i, label: "franchise disclosure" },
  { id: "fee_stack", re: /\bfee stack\b/i, label: "fee stack" },
  { id: "net_contribution", re: /\bnet contribution\b/i, label: "net contribution" },
  { id: "raw_url", re: /https?:\/\/\S+/i, label: "raw URLs" },
  { id: "sources_block", re: /\bSources:\s*/i, label: "Sources:" },
  { id: "source_line", re: /\bSource:\s*/i, label: "Source:" },
  { id: "disclosure_document", re: /\bdisclosure document\b/i, label: "disclosure document" },
  { id: "performance_representation", re: /\bperformance representation\b/i, label: "performance representation" },
  { id: "adr", re: /\bADR\b/, label: "ADR" },
  { id: "revpar", re: /\bRevPAR\b/, label: "RevPAR" },
]);

/** Mechanical / robotic scrub artifacts. */
export const V40B_MECHANICAL_PATTERNS = Object.freeze([
  {
    id: "owner_economics_awkward",
    re: /\bowner economics after brand-related costs\b/i,
    severity: "medium",
    note: "Mechanical replacement of “net contribution” — often unnatural in context",
  },
  {
    id: "participation_cost_categories",
    re: /\bparticipation cost categories\b/i,
    severity: "medium",
    note: "Mechanical replacement of “fee stack” — may be vague without supporting detail",
  },
  {
    id: "franchise_disclosure_materials",
    re: /\bfranchise disclosure materials\b/i,
    severity: "high",
    note: "Scrub left FDD-adjacent phrasing; still fails founder “no franchise disclosure” bar",
  },
  {
    id: "letter_of_intent_boilerplate",
    re: /\bletter of intent or commercial proposal\b/i,
    severity: "medium",
    note: "Mechanical LOI rewrite — usually not useful for owners on this page",
  },
  {
    id: "generic_diligence_boilerplate",
    re: /\bConfirm (participation costs|agreement terms|participation categories|[^.]{0,80}) directly during brand engagement/i,
    severity: "medium",
    note: "Repeated generic diligence boilerplate from scrubber placeholders",
  },
  {
    id: "orientation_only_boilerplate",
    re: /\borientation only[—-]not commercial terms or a forecast\b/i,
    severity: "low",
    note: "Generic scrub placeholder when a field was emptied",
  },
  {
    id: "public_franchise_performance",
    re: /\bpublic franchise performance disclosures\b/i,
    severity: "medium",
    note: "Item 19 rewrite still sounds franchise/legal rather than owner-useful",
  },
  {
    id: "average_daily_rate_stuffed",
    re: /\baverage daily rate\b/i,
    severity: "low",
    note: "ADR expanded in place — check whether sentence still reads naturally",
  },
  {
    id: "revpar_expanded",
    re: /\brevenue per available room\b/i,
    severity: "low",
    note: "RevPAR expanded in place — check whether sentence still reads naturally",
  },
]);

export const V40B_BRAND_COPY_PROFILES = Object.freeze({
  "everhome-suites": {
    brandName: "Everhome Suites",
    expectedSignals: [
      { id: "extended_stay", re: /\bextended[- ]stay\b/i, label: "extended-stay operating model" },
      { id: "longer_stay", re: /\b(longer[- ]stay|extended stay|week(?:ly|s)|month(?:ly|s))\b/i, label: "longer-stay demand" },
      {
        id: "room_product",
        re: /\b(kitchen|suite|labor|housekeeping|room product|operating model)\b/i,
        label: "room product / kitchen / labor model cues",
      },
    ],
    avoidSignals: [
      { id: "generic_franchise_boilerplate", re: /\b(franchise disclosure|FDD|Item\s*19|fee stack)\b/i, label: "generic franchise boilerplate" },
    ],
  },
  kimpton: {
    brandName: "Kimpton Hotels",
    expectedSignals: [
      { id: "lifestyle", re: /\b(lifestyle|boutique|experience[- ]led|design[- ]led|individual)\b/i, label: "lifestyle / boutique / experience-led" },
      { id: "fb_ops", re: /\b(F&B|food and beverage|restaurant|bar|operating complexity|ops)\b/i, label: "F&B / operating complexity" },
    ],
    avoidSignals: [
      { id: "fdd_style", re: /\b(FDD|Item\s*19|franchise disclosure|fee stack)\b/i, label: "FDD-style language" },
    ],
    optionalSignals: [
      { id: "ihg_context", re: /\bIHG\b/, label: "IHG context (only if owner-useful)" },
    ],
  },
  "radisson-individuals-by-choice": {
    brandName: "Radisson Individuals by Choice",
    expectedSignals: [
      { id: "soft_brand", re: /\b(soft[- ]brand|collection|conversion|individuals)\b/i, label: "soft-brand / collection / conversion fit" },
      { id: "owner_flex", re: /\b(flexib|owner|Choice|conversion)\b/i, label: "owner flexibility / Choice system" },
    ],
    avoidSignals: [
      { id: "fee_loi_boilerplate", re: /\b(fee stack|LOI|Item\s*19|FDD)\b/i, label: "fee-stack or LOI boilerplate" },
    ],
  },
  "hotel-indigo": {
    brandName: "Hotel Indigo",
    expectedSignals: [
      {
        id: "neighborhood_lifestyle",
        re: /\b(neighborhood|lifestyle|local discovery|boutique)\b/i,
        label: "neighborhood / lifestyle / local discovery",
      },
      { id: "ihg_context", re: /\b(IHG|Hotel Indigo)\b/i, label: "Hotel Indigo / IHG context" },
    ],
    avoidSignals: [
      { id: "fdd_style", re: /\b(FDD|Item\s*19|franchise disclosure|fee stack)\b/i, label: "FDD-style language" },
    ],
  },
  "mgallery-collection": {
    brandName: "MGallery Collection",
    expectedSignals: [
      {
        id: "collection_character",
        re: /\b(collection|local character|MGallery|Accor)\b/i,
        label: "collection / local character / Accor framing",
      },
      {
        id: "conversion_fit",
        re: /\b(conversion|reposition|brand standards|owner)\b/i,
        label: "conversion / repositioning / standards diligence",
      },
    ],
    avoidSignals: [
      { id: "fdd_style", re: /\b(FDD|Item\s*19|franchise disclosure|fee stack)\b/i, label: "FDD-style language" },
    ],
  },
  "small-luxury-hotels-of-the-world": {
    brandName: "Small Luxury Hotels of the World",
    expectedSignals: [
      {
        id: "independent_consortium",
        re: /\b(independent|consortium|affiliation|membership|SLH)\b/i,
        label: "independent consortium / membership framing",
      },
      {
        id: "luxury_quality",
        re: /\b(luxury|quality|membership|owner)\b/i,
        label: "luxury / quality / membership diligence",
      },
    ],
    avoidSignals: [
      { id: "fdd_style", re: /\b(FDD|Item\s*19|franchise disclosure|fee stack)\b/i, label: "FDD-style language" },
    ],
  },
});

export const V40B_DEFAULT_BRANDS = Object.freeze([
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
]);

function nz(v) {
  return v == null ? "" : String(v);
}

export function scanForbiddenLanguage(text) {
  const blob = nz(text);
  const hits = [];
  for (const rule of V40B_FORBIDDEN_LANGUAGE) {
    const m = blob.match(rule.re);
    if (m) {
      hits.push({
        id: rule.id,
        label: rule.label,
        snippet: m[0].slice(0, 80),
      });
    }
  }
  return hits;
}

export function scanMechanicalCopy(text) {
  const blob = nz(text);
  const hits = [];
  for (const rule of V40B_MECHANICAL_PATTERNS) {
    const m = blob.match(rule.re);
    if (m) {
      hits.push({
        id: rule.id,
        severity: rule.severity,
        note: rule.note,
        snippet: m[0].slice(0, 120),
      });
    }
  }
  return hits;
}

export function evaluateBrandCopySignals(brandSlug, text) {
  const profile = V40B_BRAND_COPY_PROFILES[brandSlug];
  if (!profile) {
    return { brandSlug, expected: [], avoid: [], optional: [], pass: false, missingExpected: ["no_profile"] };
  }
  const blob = nz(text);
  const expected = profile.expectedSignals.map((s) => ({
    id: s.id,
    label: s.label,
    present: s.re.test(blob),
  }));
  const avoid = (profile.avoidSignals || []).map((s) => ({
    id: s.id,
    label: s.label,
    present: s.re.test(blob),
  }));
  const optional = (profile.optionalSignals || []).map((s) => ({
    id: s.id,
    label: s.label,
    present: s.re.test(blob),
  }));
  const missingExpected = expected.filter((e) => !e.present).map((e) => e.id);
  const avoidHits = avoid.filter((a) => a.present).map((a) => a.id);
  return {
    brandSlug,
    brandName: profile.brandName,
    expected,
    avoid,
    optional,
    missingExpected,
    avoidHits,
    pass: missingExpected.length === 0 && avoidHits.length === 0,
  };
}

/**
 * Detect repeated diligence boilerplate across rows (same sentence stem ≥3 times).
 */
export function detectRepeatedBoilerplate(texts = []) {
  const counts = new Map();
  for (const t of texts) {
    const blob = nz(t);
    const stems = blob.match(
      /Confirm [^.]{20,160}directly during brand engagement[^.]{0,80}\./gi
    );
    if (!stems) continue;
    for (const s of stems) {
      const key = s.replace(/\s+/g, " ").trim().toLowerCase();
      counts.set(key, (counts.get(key) || 0) + 1);
    }
  }
  return [...counts.entries()]
    .filter(([, n]) => n >= 3)
    .map(([text, count]) => ({ text: text.slice(0, 160), count }))
    .sort((a, b) => b.count - a.count);
}

/**
 * Flag rows that became vague after scrub (very short / placeholder-only).
 */
export function isVagueAfterScrub(text) {
  const t = nz(text).trim();
  if (!t) return true;
  if (t.length < 40) return true;
  if (/^Confirm .{10,120}during brand engagement/i.test(t) && t.length < 220) return true;
  if (/orientation only/i.test(t) && t.length < 280) return true;
  return false;
}
