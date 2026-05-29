import crypto from "crypto";
import Airtable from "airtable";
import {
  decodeHtmlEntities,
  decodeHtmlEntitiesPreserveWhitespace,
} from "../../lib/decode-html-entities.js";

/** @typedef {import("../market-alerts-news.js").fetchMarketAlertsRssItems} fetchMarketAlertsRssItems */

export const MAP_ALERT = {
  title: "Title",
  dedupeId: "Dedupe ID",
  summary: "Summary",
  sourceName: "Source Name",
  sourceUrl: "Source URL",
  publishedAt: "Published At",
  category: "Category",
  regionGroup: "Region Group",
  priority: "Priority",
  tags: "Tags",
};

export const ALLOWED_CATEGORIES = [
  "Deals",
  "Capital",
  "Brand",
  "Supply",
  "Demand",
  "Loyalty",
  "Risk",
];

export const ALLOWED_REGION_GROUPS = [
  "Global",
  "Europe",
  "Asia Pacific",
  "Caribbean",
  "Latin America",
  "Other",
];

export const ALLOWED_PRIORITIES = ["Low", "Medium", "High"];

const CATEGORY_RULES = [
  { category: "Deals", re: /\b(acquisition|merger|sold|sale of|deal\b|transaction|portfolio sale)\b/i },
  { category: "Capital", re: /\b(funding|financ|investment|lender|loan|reit|bond|capital raise|refinanc)\b/i },
  { category: "Brand", re: /\b(franchise|rebrand|brand launch|soft brand|flagged|mgallery|marriott|hilton|accor|hyatt|ihg|choice hotels)\b/i },
  { category: "Supply", re: /\b(pipeline|construction|opening|opens\b|new hotel|rooms in construction|groundbreaking|development)\b/i },
  { category: "Demand", re: /\b(revpar|occupancy|adr|demand|booking|traveler|performance|arrivals)\b/i },
  { category: "Loyalty", re: /\b(loyalty|rewards|bonvoy|points program|frequent guest)\b/i },
  { category: "Risk", re: /\b(lawsuit|regulat|strike|downturn|bankrupt|sanction|risk\b|recall)\b/i },
];

const REGION_RULES = [
  { region: "Europe", re: /\b(europe|european|uk\b|united kingdom|germany|france|spain|italy|amsterdam)\b/i },
  { region: "Asia Pacific", re: /\b(asia|pacific|china|india|japan|australia|vietnam|singapore|korea|beijing|mykonos)\b/i },
  { region: "Caribbean", re: /\b(caribbean|aruba|bahamas|jamaica|puerto rico|cancun|dominican)\b/i },
  { region: "Latin America", re: /\b(latin america|cala\b|mexico|brazil|argentina|chile|colombia|peru|guatemala|panama)\b/i },
];

/** SHA-256 of canonical source URL (matches existing MarketAlerts rows). */
export function rssItemDedupeId(item) {
  const url = (item.link || item.sourceUrl || "").trim();
  if (url) return crypto.createHash("sha256").update(url).digest("hex");
  const title = (item.title || "").trim().toLowerCase();
  return crypto.createHash("sha256").update(title).digest("hex");
}

export function inferCategory(item) {
  const text = `${item.title || ""} ${item.summary || ""}`.trim();
  for (const { category, re } of CATEGORY_RULES) {
    if (re.test(text)) return category;
  }
  if ((item.source || "").includes("Openings")) return "Supply";
  return "Demand";
}

export function inferRegionGroup(item) {
  const text = `${item.title || ""} ${item.summary || ""} ${item.link || ""}`.trim();
  for (const { region, re } of REGION_RULES) {
    if (re.test(text)) return region;
  }
  return "Global";
}

export function parsePublishedAtIso(pubDate) {
  if (!pubDate) return new Date().toISOString();
  const d = new Date(pubDate);
  if (Number.isNaN(d.getTime())) return new Date().toISOString();
  return d.toISOString();
}

/** Normalize display text before Airtable write or publish. */
export function sanitizeMarketAlertText(text, { preserveWhitespace = false } = {}) {
  const raw = (text || "").trim();
  if (!raw) return "";
  return preserveWhitespace
    ? decodeHtmlEntitiesPreserveWhitespace(raw)
    : decodeHtmlEntities(raw);
}

export function mapRssItemToAirtableFields(item) {
  const sourceUrl = (item.link || "").trim();
  const fields = {
    [MAP_ALERT.title]: sanitizeMarketAlertText(item.title || "(No title)").slice(0, 500),
    [MAP_ALERT.dedupeId]: rssItemDedupeId(item),
    [MAP_ALERT.summary]: sanitizeMarketAlertText(item.summary, { preserveWhitespace: true }).slice(
      0,
      10000
    ),
    [MAP_ALERT.sourceName]: sanitizeMarketAlertText(item.source || "RSS").slice(0, 200),
    [MAP_ALERT.sourceUrl]: sourceUrl.slice(0, 1000),
    [MAP_ALERT.publishedAt]: parsePublishedAtIso(item.pubDate),
    [MAP_ALERT.category]: inferCategory(item),
    [MAP_ALERT.regionGroup]: inferRegionGroup(item),
    [MAP_ALERT.priority]: "Medium",
    [MAP_ALERT.tags]: ["RSS"],
  };
  return fields;
}

export function validateAlertFields(fields) {
  const errors = [];
  if (!fields[MAP_ALERT.title]) errors.push("Title is required");
  if (!fields[MAP_ALERT.dedupeId]) errors.push("Dedupe ID is required");
  if (!ALLOWED_CATEGORIES.includes(fields[MAP_ALERT.category])) {
    errors.push(`Invalid Category: ${fields[MAP_ALERT.category]}`);
  }
  if (!ALLOWED_REGION_GROUPS.includes(fields[MAP_ALERT.regionGroup])) {
    errors.push(`Invalid Region Group: ${fields[MAP_ALERT.regionGroup]}`);
  }
  if (!ALLOWED_PRIORITIES.includes(fields[MAP_ALERT.priority])) {
    errors.push(`Invalid Priority: ${fields[MAP_ALERT.priority]}`);
  }
  const pub = fields[MAP_ALERT.publishedAt];
  if (pub && Number.isNaN(new Date(pub).getTime())) {
    errors.push("Published At is not a valid date");
  }
  return { ok: errors.length === 0, errors };
}

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) return null;
  return new Airtable({ apiKey }).base(baseId);
}

export async function loadExistingDedupeIds(tableName) {
  const base = getBase();
  if (!base) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required");

  const ids = new Set();
  await base(tableName)
    .select({
      fields: [MAP_ALERT.dedupeId],
      pageSize: 100,
    })
    .eachPage((records, next) => {
      records.forEach((r) => {
        const v = r.fields[MAP_ALERT.dedupeId];
        if (v) ids.add(String(v));
      });
      next();
    });
  return ids;
}

const AIRTABLE_CREATE_BATCH = 10;

/**
 * @param {object} opts
 * @param {Array<object>} opts.items RSS items from fetchMarketAlertsRssItems
 * @param {boolean} [opts.dryRun]
 * @param {string} [opts.tableName]
 */
export async function syncRssItemsToAirtable({ items, dryRun = false, tableName }) {
  const table = tableName || process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  const base = getBase();
  if (!base) {
    return {
      ok: false,
      error: "AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required",
      validation: { ok: false, errors: ["Airtable not configured"] },
    };
  }

  const existingDedupe = await loadExistingDedupeIds(table);
  const toCreate = [];
  const skipped = [];
  const invalid = [];

  for (const item of items) {
    const fields = mapRssItemToAirtableFields(item);
    const validation = validateAlertFields(fields);
    if (!validation.ok) {
      invalid.push({ title: fields[MAP_ALERT.title], errors: validation.errors });
      continue;
    }
    if (existingDedupe.has(fields[MAP_ALERT.dedupeId])) {
      skipped.push({ title: fields[MAP_ALERT.title], dedupeId: fields[MAP_ALERT.dedupeId] });
      continue;
    }
    existingDedupe.add(fields[MAP_ALERT.dedupeId]);
    toCreate.push(fields);
  }

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      table,
      fetched: items.length,
      wouldCreate: toCreate.length,
      skipped: skipped.length,
      invalid: invalid.length,
      preview: toCreate.slice(0, 5).map((f) => ({
        title: f[MAP_ALERT.title],
        category: f[MAP_ALERT.category],
        regionGroup: f[MAP_ALERT.regionGroup],
        publishedAt: f[MAP_ALERT.publishedAt],
        sourceUrl: f[MAP_ALERT.sourceUrl],
      })),
      fieldMapping: MAP_ALERT,
    };
  }

  const created = [];
  const createErrors = [];

  for (let i = 0; i < toCreate.length; i += AIRTABLE_CREATE_BATCH) {
    const batch = toCreate.slice(i, i + AIRTABLE_CREATE_BATCH).map((fields) => ({ fields }));
    try {
      const records = await base(table).create(batch, { typecast: true });
      records.forEach((r) => {
        created.push({ id: r.id, title: r.fields[MAP_ALERT.title] });
      });
    } catch (err) {
      createErrors.push({
        batchStart: i,
        message: err.message || String(err),
      });
      if (process.env.NODE_ENV !== "production") {
        console.error("[market-alerts-rss-sync] batch create failed:", err.message);
      }
    }
  }

  return {
    ok: createErrors.length === 0,
    dryRun: false,
    table,
    fetched: items.length,
    created: created.length,
    skipped: skipped.length,
    invalid: invalid.length,
    createErrors,
    createdSample: created.slice(0, 5),
    fieldMapping: MAP_ALERT,
  };
}
