/**
 * Related Market Alerts correlation by Entity Key (V1 helpers).
 */
import { inferCorrelationEntityKey } from "./market-alerts-entity-extract.js";
const DEFAULT_WINDOW_DAYS = 90;

/**
 * @param {string|null|undefined} entityKey
 * @param {Array<{ id: string, entityKey?: string|null, title?: string, publishedAt?: string|null, eventType?: string|null }>} candidates
 * @param {{ excludeId?: string, windowDays?: number, now?: Date }} [opts]
 * @returns {Array<object>}
 */
export function findRelatedAlertsByEntityKey(entityKey, candidates = [], opts = {}) {
  const key = String(entityKey || "").trim();
  if (!key) return [];

  const excludeId = opts.excludeId || null;
  const windowDays = opts.windowDays ?? DEFAULT_WINDOW_DAYS;
  const now = opts.now || new Date();
  const cutoff = now.getTime() - windowDays * 24 * 60 * 60 * 1000;

  return candidates
    .filter((c) => {
      if (!c || c.id === excludeId) return false;
      if (String(c.entityKey || "").trim() !== key) return false;
      if (!c.publishedAt) return true;
      const t = new Date(c.publishedAt).getTime();
      if (!Number.isFinite(t)) return true;
      return t >= cutoff;
    })
    .sort((a, b) => {
      const ta = a.publishedAt ? new Date(a.publishedAt).getTime() : 0;
      const tb = b.publishedAt ? new Date(b.publishedAt).getTime() : 0;
      return tb - ta;
    })
    .slice(0, 5);
}

/**
 * @param {Array<{ title?: string, eventType?: string|null }>} related
 * @returns {string|null}
 */
export function buildRelatedSummary(related = []) {
  if (!related.length) return null;
  const bits = related.slice(0, 3).map((r) => {
    const et = r.eventType ? `${r.eventType}: ` : "";
    const title = String(r.title || "Related alert").trim();
    return `${et}${title}`.slice(0, 120);
  });
  return `Related recent coverage: ${bits.join(" · ")}`;
}

/**
 * Watching list for drawer — factual context chips from entities.
 * @param {object} entities
 * @returns {string[]}
 */
export function buildWatchingList(entities = {}) {
  const out = [];
  if (entities.hotelProject) out.push(`Hotel / project: ${entities.hotelProject}`);
  if (entities.rooms) out.push(`Rooms: ${entities.rooms}`);
  if (entities.brandInvolved) out.push(`Brand: ${entities.brandInvolved}`);
  if (entities.operatorInvolved) out.push(`Operator: ${entities.operatorInvolved}`);
  if (entities.ownerDeveloper) out.push(`Owner / developer: ${entities.ownerDeveloper}`);
  if (entities.assetProjectStage && entities.assetProjectStage !== "Unknown") {
    out.push(`Stage: ${entities.assetProjectStage}`);
  }
  return out;
}

/**
 * Feed-level dedupe: one Worth Reviewing card per entity key (most recent wins).
 * Does not delete Airtable rows — only suppresses duplicate cards in API responses.
 * @param {Array<{ intelligence?: { entities?: { entityKey?: string|null }, eventType?: string|null }, fields?: Record<string, unknown> }>} items
 * @param {{ windowDays?: number }} [opts]
 */
function resolveFeedCorrelationKey(item = {}) {
  const intel = item.intelligence || {};
  const entities = intel.entities || {};
  const fields = item.fields || {};
  const title = String(fields["Title"] || fields.title || "").trim();
  const summary = String(fields["Summary"] || fields.summary || "").trim();
  const text = `${title} ${summary}`.trim();
  const eventType = intel.eventType || intel.event?.eventType || null;

  const recomputed = inferCorrelationEntityKey({
    text,
    eventType,
    hotelProject: entities.hotelProject || null,
    brandInvolved: entities.brandInvolved || null,
    ownerDeveloper: entities.ownerDeveloper || null,
    rooms: entities.rooms ?? null,
  });

  return String(recomputed || entities.entityKey || "").trim();
}

export function dedupeFeedItemsByEntityKey(items = [], opts = {}) {
  const windowDays = opts.windowDays ?? 14;
  const cutoff = Date.now() - windowDays * 24 * 60 * 60 * 1000;
  const seen = new Set();
  const out = [];

  for (const item of items) {
    const key = resolveFeedCorrelationKey(item);
    if (!key) {
      out.push(item);
      continue;
    }
    const pubRaw =
      item?.fields?.["Published At"] ||
      item?.fields?.publishedAt ||
      item?.publishedAt ||
      null;
    if (pubRaw) {
      const t = new Date(pubRaw).getTime();
      if (Number.isFinite(t) && t < cutoff) {
        out.push(item);
        continue;
      }
    }
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}
