/**
 * Hotel News & Articles from Market Alerts (canonical).
 * Matching is property-specific — brand/operator-only articles stay out of HE.
 * No parallel news store. Association key: Tags `hpc:<recId>` (+ optional text field).
 */

import { canonicalizeSourceUrl } from "../market-alerts-dedupe.js";

export const HOTEL_NEWS_FROM_MARKET_ALERTS_V1 = "hotel-news-from-market-alerts-v1";
export const HPC_TAG_PREFIX = "hpc:";
/** Optional Airtable multiline field — designed; apply only via authorized ensure/write. */
export const MAP_HOTEL_ASSOC = Object.freeze({
  hpcHotelRecordIds: "HPC Hotel Record IDs",
});
/** Subset of Market Alerts intelligence fields used for hotel matching (canonical names). */
export const MAP_INTEL = Object.freeze({
  hotelProject: "Hotel / Project",
  brandInvolved: "Brand Involved",
  operatorInvolved: "Operator Involved",
  ownerDeveloper: "Owner / Developer",
});

export const HOTEL_ARTICLE_MATCH = Object.freeze({
  HOTEL_SPECIFIC: "HOTEL_SPECIFIC",
  MULTI_HOTEL_SPECIFIC: "MULTI_HOTEL_SPECIFIC",
  ORGANIZATION_CONTEXT_ONLY: "ORGANIZATION_CONTEXT_ONLY",
  AMBIGUOUS_MATCH: "AMBIGUOUS_MATCH",
  UNRELATED: "UNRELATED",
});

export const HE_NEWS_VISIBLE_MATCHES = new Set([
  HOTEL_ARTICLE_MATCH.HOTEL_SPECIFIC,
  HOTEL_ARTICLE_MATCH.MULTI_HOTEL_SPECIFIC,
]);

const F_ALERT = Object.freeze({
  title: "Title",
  dedupeId: "Dedupe ID",
  summary: "Summary",
  sourceName: "Source Name",
  sourceUrl: "Source URL",
  publishedAt: "Published At",
  category: "Category",
  tags: "Tags",
});

function text(v) {
  const s = String(v == null ? "" : v).trim();
  return s || null;
}

function normName(v) {
  return String(v || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\b(the|a|an|hotel|resort|inn|suites?|and)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function includesPhrase(haystack, phrase) {
  const h = normName(haystack);
  const p = normName(phrase);
  if (!h || !p || p.length < 4) return false;
  if (h === p || h.includes(p)) return true;
  // Word-boundary-ish: require contiguous token sequence
  const re = new RegExp(`(?:^|\\s)${escapeRegExp(p)}(?:\\s|$)`);
  return re.test(h);
}

function isHpcId(id) {
  return /^rec[a-zA-Z0-9]{14}$/.test(String(id || "").trim());
}

/**
 * @param {string|string[]|null|undefined} tags
 * @returns {string[]}
 */
export function parseHpcIdsFromTags(tags) {
  const list = Array.isArray(tags) ? tags : tags ? [tags] : [];
  const out = [];
  for (const t of list) {
    const s = String(t || "").trim();
    const m = s.match(/^hpc:(rec[a-zA-Z0-9]{14})$/i);
    if (m) out.push(m[1]);
  }
  return [...new Set(out)];
}

/**
 * @param {string|null|undefined} raw
 * @returns {string[]}
 */
export function parseHpcIdsFromAssocField(raw) {
  const s = String(raw || "");
  const matches = s.match(/rec[a-zA-Z0-9]{14}/g) || [];
  return [...new Set(matches)];
}

export function hpcAssociationTag(hpcId) {
  if (!isHpcId(hpcId)) return null;
  return `${HPC_TAG_PREFIX}${hpcId}`;
}

/**
 * Idempotent tag merge for Market Alerts ↔ HPC association.
 * Does not invent articles; only proposes Tag updates.
 */
export function planHpcHotelAssociation(existingTags, hpcId) {
  const tag = hpcAssociationTag(hpcId);
  if (!tag) {
    return { ok: false, reason: "invalid_hpc_id", nextTags: existingTags || [], changed: false };
  }
  const tags = Array.isArray(existingTags) ? existingTags.slice() : [];
  const already = tags.some((t) => String(t).toLowerCase() === tag.toLowerCase());
  if (already) {
    return { ok: true, reason: "already_associated", nextTags: tags, changed: false, tag };
  }
  tags.push(tag);
  return { ok: true, reason: "associate", nextTags: tags, changed: true, tag };
}

/**
 * Build identity bundle for matching.
 * @param {object} hotel
 */
export function buildHotelNewsIdentity(hotel = {}) {
  const hpcId =
    text(hotel.id) ||
    text(hotel.recordId) ||
    text(hotel.airtable_record_id) ||
    text(hotel.hpcId) ||
    null;
  const name = text(hotel.name) || text(hotel.hotel_name) || null;
  const aliases = []
    .concat(hotel.aliases || [], hotel.former_names || [], hotel.formerNames || [])
    .map((a) => text(a))
    .filter(Boolean);
  if (name && /marriott/i.test(name) && /bethesda/i.test(name)) {
    aliases.push("Bethesda Marriott Hotel", "Marriott Bethesda");
  }
  const brand = text(hotel.brand) || text(hotel.affiliation) || null;
  const operator = text(hotel.managementCompany) || text(hotel.operator) || null;
  const owners = []
    .concat(
      hotel.economicOwner ? [hotel.economicOwner] : [],
      hotel.economic_owner ? [hotel.economic_owner] : [],
      Array.isArray(hotel.sponsors) ? hotel.sponsors : [],
      Array.isArray(hotel.ownerNames) ? hotel.ownerNames : []
    )
    .map((a) => text(a))
    .filter(Boolean);
  const city = text(hotel.city) || text(hotel.market) || null;
  const address = text(hotel.address) || text(hotel.street) || null;
  return {
    hpcId: isHpcId(hpcId) ? hpcId : null,
    name,
    aliases: [...new Set(aliases)],
    brand,
    operator,
    owners: [...new Set(owners)],
    city,
    address,
    nameKeys: [name, ...aliases].filter(Boolean),
  };
}

function articleBlob(fields = {}) {
  return {
    title: text(fields[F_ALERT.title] || fields.title) || "",
    summary: text(fields[F_ALERT.summary] || fields.summary) || "",
    hotelProject: text(fields[MAP_INTEL.hotelProject] || fields.hotelProject) || "",
    brandInvolved: text(fields[MAP_INTEL.brandInvolved] || fields.brandInvolved) || "",
    operatorInvolved: text(fields[MAP_INTEL.operatorInvolved] || fields.operatorInvolved) || "",
    ownerDeveloper: text(fields[MAP_INTEL.ownerDeveloper] || fields.ownerDeveloper) || "",
    tags: fields[F_ALERT.tags] || fields.tags || [],
    assocField: fields[MAP_HOTEL_ASSOC.hpcHotelRecordIds] || "",
    sourceUrl: text(fields[F_ALERT.sourceUrl] || fields.sourceUrl) || "",
    sourceName: text(fields[F_ALERT.sourceName] || fields.sourceName) || "",
    publishedAt: fields[F_ALERT.publishedAt] || fields.publishedAt || null,
    category: text(fields[F_ALERT.category] || fields.category) || "",
    dedupeId: text(fields[F_ALERT.dedupeId] || fields.dedupeId) || "",
  };
}

function nameMentionedIn(identity, ...parts) {
  const blob = parts.filter(Boolean).join(" \n ");
  if (!blob) return false;
  return identity.nameKeys.some((n) => includesPhrase(blob, n));
}

function orgOnlySignals(identity, a) {
  const brandHit =
    identity.brand &&
    (includesPhrase(a.brandInvolved, identity.brand) ||
      includesPhrase(a.title, identity.brand) ||
      includesPhrase(a.summary, identity.brand));
  const opHit =
    identity.operator &&
    (includesPhrase(a.operatorInvolved, identity.operator) ||
      includesPhrase(a.title, identity.operator) ||
      includesPhrase(a.summary, identity.operator));
  const ownerHit = (identity.owners || []).some(
    (o) =>
      includesPhrase(a.ownerDeveloper, o) ||
      includesPhrase(a.title, o) ||
      includesPhrase(a.summary, o)
  );
  return Boolean(brandHit || opHit || ownerHit);
}

/**
 * Classify article relevance to one hotel identity.
 * @returns {{ classification: string, reasons: string[], matchedNames: string[] }}
 */
export function classifyHotelArticleRelevance(hotelOrIdentity, fieldsOrArticle) {
  const identity =
    hotelOrIdentity && hotelOrIdentity.nameKeys
      ? hotelOrIdentity
      : buildHotelNewsIdentity(hotelOrIdentity);
  const a =
    fieldsOrArticle && fieldsOrArticle.title !== undefined && fieldsOrArticle.hotelProject !== undefined
      ? fieldsOrArticle
      : articleBlob(fieldsOrArticle || {});

  const reasons = [];
  const matchedNames = [];

  if (identity.hpcId) {
    const fromTags = parseHpcIdsFromTags(a.tags);
    const fromField = parseHpcIdsFromAssocField(a.assocField);
    if (fromTags.includes(identity.hpcId) || fromField.includes(identity.hpcId)) {
      reasons.push("canonical_hpc_association");
      return {
        classification: HOTEL_ARTICLE_MATCH.HOTEL_SPECIFIC,
        reasons,
        matchedNames: identity.name ? [identity.name] : [],
      };
    }
  }

  const projectHit = identity.nameKeys.some((n) => {
    if (includesPhrase(a.hotelProject, n)) {
      matchedNames.push(n);
      return true;
    }
    return false;
  });
  const titleHit = identity.nameKeys.some((n) => {
    if (includesPhrase(a.title, n)) {
      matchedNames.push(n);
      return true;
    }
    return false;
  });
  const summaryHit = identity.nameKeys.some((n) => {
    if (includesPhrase(a.summary, n)) {
      matchedNames.push(n);
      return true;
    }
    return false;
  });

  const explicitProperty =
    projectHit || titleHit || (summaryHit && (projectHit || titleHit || Boolean(a.hotelProject)));

  // Summary-only weak name hit without Hotel/Project → ambiguous unless city+name co-occur tightly
  if (!projectHit && !titleHit && summaryHit) {
    const cityOk = identity.city && includesPhrase(a.summary, identity.city);
    if (cityOk && identity.name && includesPhrase(a.summary, identity.name)) {
      reasons.push("summary_name_city");
      return {
        classification: HOTEL_ARTICLE_MATCH.HOTEL_SPECIFIC,
        reasons,
        matchedNames: [...new Set(matchedNames)],
      };
    }
    reasons.push("summary_only_weak");
    return {
      classification: HOTEL_ARTICLE_MATCH.AMBIGUOUS_MATCH,
      reasons,
      matchedNames: [...new Set(matchedNames)],
    };
  }

  if (explicitProperty) {
    reasons.push(projectHit ? "hotel_project_field" : "title_name");
    // Multi-hotel: Hotel/Project or title lists multiple distinct hotel-like names — keep MULTI when
    // more than one strong property token besides focus (heuristic: " and " / ";" with hotel words).
    const multiHint =
      /\b(and|,|;)\b.{0,40}\b(hotel|resort|inn)\b/i.test(`${a.hotelProject} ${a.title}`) ||
      /\b(hotels|properties|portfolio assets)\b/i.test(`${a.title} ${a.summary}`);
    return {
      classification: multiHint
        ? HOTEL_ARTICLE_MATCH.MULTI_HOTEL_SPECIFIC
        : HOTEL_ARTICLE_MATCH.HOTEL_SPECIFIC,
      reasons,
      matchedNames: [...new Set(matchedNames)],
    };
  }

  if (orgOnlySignals(identity, a)) {
    reasons.push("brand_or_operator_only");
    return {
      classification: HOTEL_ARTICLE_MATCH.ORGANIZATION_CONTEXT_ONLY,
      reasons,
      matchedNames: [],
    };
  }

  return {
    classification: HOTEL_ARTICLE_MATCH.UNRELATED,
    reasons: ["no_property_signal"],
    matchedNames: [],
  };
}

function relevanceSummary(identity, a, classification) {
  if (a.hotelProject && identity.name && includesPhrase(a.hotelProject, identity.name)) {
    return `Market Alerts links this coverage to ${identity.name} via Hotel / Project.`;
  }
  if (nameMentionedIn(identity, a.title)) {
    return `${identity.name} is named in the article headline.`;
  }
  if (classification === HOTEL_ARTICLE_MATCH.MULTI_HOTEL_SPECIFIC) {
    return `Property-specific coverage that includes ${identity.name} among named hotels.`;
  }
  return `Property-relevant Market Alerts coverage for ${identity.name || "this hotel"}.`;
}

/**
 * Map Airtable/alert record → HE news card (no internal match enums in UI payload labels).
 */
export function toHotelNewsCard(record, identity, classification, reasons) {
  const fields = record.fields || record;
  const a = articleBlob(fields);
  const id = record.id || fields.id || null;
  const url = a.sourceUrl || "";
  return {
    id,
    dedupeKey: a.dedupeId || canonicalizeSourceUrl(url) || id,
    headline: a.title || "Untitled",
    publisher: a.sourceName || "—",
    publishedAt: a.publishedAt || null,
    category: a.category || null,
    relevanceSummary: relevanceSummary(identity, a, classification),
    url,
    matchClass: classification,
    matchReasons: reasons || [],
    canonicalSource: "MarketAlerts",
  };
}

/**
 * Select visible hotel news from Market Alerts rows.
 * Dedupes by URL/Dedupe ID. Sorts by publishedAt desc.
 */
export function selectHotelNewsArticles(hotel, records = [], opts = {}) {
  const identity = buildHotelNewsIdentity(hotel);
  const limit = Math.min(Math.max(Number(opts.limit) || 5, 1), 50);
  const includeHistorical = opts.includeHistorical !== false;
  const maxAgeDays = opts.maxAgeDays == null ? null : Number(opts.maxAgeDays);
  const now = opts.now || Date.now();

  const seen = new Set();
  const cards = [];

  for (const rec of records) {
    const fields = rec.fields || rec;
    const verdict = classifyHotelArticleRelevance(identity, fields);
    if (!HE_NEWS_VISIBLE_MATCHES.has(verdict.classification)) continue;

    const card = toHotelNewsCard(rec, identity, verdict.classification, verdict.reasons);
    if (!card.url && !card.headline) continue;
    const key = card.dedupeKey || card.id;
    if (key && seen.has(key)) continue;
    if (key) seen.add(key);

    if (!includeHistorical && maxAgeDays != null && card.publishedAt) {
      const t = new Date(card.publishedAt).getTime();
      if (Number.isFinite(t) && now - t > maxAgeDays * 86400000) {
        // Keep ownership / renovation historical categories even when old
        const cat = String(card.category || "");
        const histKeep = /Deal|Capital|Brand|Supply/i.test(cat);
        if (!histKeep) continue;
      }
    }

    cards.push(card);
  }

  cards.sort((a, b) => {
    const ta = a.publishedAt ? new Date(a.publishedAt).getTime() : 0;
    const tb = b.publishedAt ? new Date(b.publishedAt).getTime() : 0;
    return tb - ta;
  });

  const total = cards.length;
  return {
    version: HOTEL_NEWS_FROM_MARKET_ALERTS_V1,
    hotelId: identity.hpcId,
    hotelName: identity.name,
    canonicalSource: "MarketAlerts",
    total,
    articles: cards.slice(0, limit),
    hasMore: total > limit,
    visible: total > 0,
  };
}

export { F_ALERT as HOTEL_NEWS_ALERT_FIELDS, MAP_INTEL as HOTEL_NEWS_INTEL_FIELDS };
