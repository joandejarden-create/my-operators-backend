/**
 * Rooms candidate corroboration.
 * Held Medium candidates become an active queue — HIGH only via first-party
 * or two independent non-syndicated sources. Never HBX / Cvent-only / beds.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import { MAP_ROOMS } from "./production-census-rooms-keys-queue.js";
import {
  extractRoomsKeysFromOfficialHtml,
  selectBestRoomsHit,
  isFalsePositiveRoomCount,
} from "./production-census-rooms-keys-extractor.js";
import {
  assertRoomsSourcePolicy,
  classifyNullFill,
} from "./property-fundamentals-enrichment-v1.js";
import { hostFromUrl, isForbiddenHost } from "./official-domain-crawler-v1.js";
import { MAP_ROOMS_SOURCE_TYPE } from "./census-secondary-hotel-data-policy.js";
import { isIdentityHigh } from "./property-outward-brand-enrichment-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const ROOMS_CORROBORATION_VERSION = "rooms-candidate-corroboration-v1";

export const ROOMS_QUEUE_FP = path.join(
  ROOT,
  "data/research-engine-v2/overnight-census-enrichment/rooms-corroboration-queue.json"
);
export const LEGACY_ROOMS_CANDIDATES_FP = path.join(
  ROOT,
  "data/research-engine-v2/property-fundamentals-enrichment/last-rooms-candidates.json"
);

const BEDS_PLAZAS_RE =
  /\b(leitos?|camas?|beds?|plazas?|pax|guest\s*capacity|capacidad(?:\s+de)?\s+huespedes|apartments?|residences?)\b/i;
const GUESTROOM_RE =
  /\b(habitaciones?|guest\s*rooms?|guestrooms?|keys?|cuartos?|\buh\b|rooms?\s*\/\s*keys?)\b/i;

export function rejectNonGuestroomSemantics(text, count) {
  const t = String(text || "");
  if (count == null || !Number.isFinite(Number(count))) {
    return { ok: false, reason: "not_a_number" };
  }
  if (BEDS_PLAZAS_RE.test(t) && !GUESTROOM_RE.test(t)) {
    return { ok: false, reason: "beds_plazas_or_capacity_not_rooms" };
  }
  if (isFalsePositiveRoomCount(t, Number(count), "rooms")) {
    return { ok: false, reason: "false_positive_room_count" };
  }
  return { ok: true };
}

export function sameUpstreamSource(urlA, urlB) {
  const a = hostFromUrl(urlA);
  const b = hostFromUrl(urlB);
  if (!a || !b) return false;
  if (a === b) return true;
  const root = (h) => h.split(".").slice(-2).join(".");
  return root(a) === root(b);
}

export function sourcesAreIndependent(urlA, urlB) {
  if (!urlA || !urlB) return false;
  if (isForbiddenHost(urlA) || isForbiddenHost(urlB)) return false;
  return !sameUpstreamSource(urlA, urlB);
}

export function loadRoomsCorroborationQueue() {
  /** @type {object[]} */
  let items = [];
  try {
    if (fs.existsSync(ROOMS_QUEUE_FP)) {
      const json = JSON.parse(fs.readFileSync(ROOMS_QUEUE_FP, "utf8"));
      items = Array.isArray(json) ? json : json.items || [];
    }
  } catch {
    items = [];
  }
  try {
    if (!items.length && fs.existsSync(LEGACY_ROOMS_CANDIDATES_FP)) {
      const legacy = JSON.parse(fs.readFileSync(LEGACY_ROOMS_CANDIDATES_FP, "utf8"));
      items = (Array.isArray(legacy) ? legacy : []).map((c) => ({
        id: c.id,
        rooms: c.rooms,
        source_url: c.source_url,
        source_kind: "held_medium_candidate",
        status: "PENDING",
      }));
    }
  } catch {
    // ignore
  }
  return items;
}

export function saveRoomsCorroborationQueue(items) {
  fs.mkdirSync(path.dirname(ROOMS_QUEUE_FP), { recursive: true });
  fs.writeFileSync(
    ROOMS_QUEUE_FP,
    JSON.stringify({ updated_at: new Date().toISOString(), items }, null, 2)
  );
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

export function evaluateRoomsCorroboration(candidate, pageExtract, opts = {}) {
  const policy = assertRoomsSourcePolicy({
    source_kind: pageExtract?.source_kind,
    from_hbx_rooms_array: pageExtract?.from_hbx_rooms_array === true,
    from_cvent_only: pageExtract?.from_cvent_only === true,
  });
  if (!policy.ok) {
    return { ok: false, reason: policy.blockers[0], class: "REJECTED_POLICY" };
  }
  const count = Number(pageExtract?.count);
  if (!Number.isFinite(count) || count <= 0) {
    return { ok: false, reason: "no_page_rooms" };
  }
  const semantics = rejectNonGuestroomSemantics(
    pageExtract?.context || pageExtract?.html || "",
    count
  );
  if (!semantics.ok) return { ok: false, reason: semantics.reason };

  const candCount = Number(candidate?.rooms);
  const pageUrl = pageExtract?.source_url;
  const candUrl = candidate?.source_url;
  const firstParty =
    pageExtract?.source_kind === "official_html" ||
    pageExtract?.source_kind === "official_property_page" ||
    pageExtract?.source_kind === "official_factsheet";

  if (firstParty && pageExtract.confidence === "High") {
    return {
      ok: true,
      class: "ROOMS_HIGH_FIRST_PARTY",
      rooms: count,
      source_url: pageUrl,
    };
  }

  if (
    Number.isFinite(candCount) &&
    candCount === count &&
    sourcesAreIndependent(candUrl, pageUrl) &&
    !isForbiddenHost(candUrl) &&
    !isForbiddenHost(pageUrl)
  ) {
    return {
      ok: true,
      class: "ROOMS_HIGH_TWO_SOURCE",
      rooms: count,
      source_url: pageUrl,
      corroborating_url: candUrl,
    };
  }

  if (sameUpstreamSource(candUrl, pageUrl)) {
    return { ok: false, reason: "syndicated_same_upstream", class: "NOT_INDEPENDENT" };
  }
  return { ok: false, reason: "insufficient_corroboration" };
}

export function buildRoomsHighPatch(fields, evaluation) {
  if (!evaluation?.ok) return { ok: false, reason: evaluation?.reason };
  const fill = classifyNullFill(fields[MAP_ROOMS.roomsKeys], evaluation.rooms);
  if (!fill.write) {
    return { ok: false, reason: fill.class === "CONFLICT_REVIEW" ? "conflict" : "already_populated" };
  }
  return {
    ok: true,
    patch: {
      [MAP_ROOMS.roomsKeys]: evaluation.rooms,
      [MAP_ROOMS.confidenceExisting]: "High",
      [MAP_ROOMS.sourceUrlExisting]: evaluation.source_url,
      [MAP_FIRST_PASS.lastReviewed]: todayIsoDate(),
      [MAP_FIRST_PASS.enrichmentStatus]: "Partial",
    },
  };
}

/**
 * @param {{
 *   censusRecords?: object[],
 *   fetchPageFn?: Function,
 *   maxProperties?: number,
 *   log?: Function,
 * }} opts
 */
export function rankRoomsCorroborationQueue(queue, recordsById = new Map()) {
  return [...(queue || [])].sort((a, b) => {
    const recA = recordsById.get(a.id);
    const recB = recordsById.get(b.id);
    const scoreOf = (cand, rec) => {
      if (!rec) return 0;
      const f = rec.fields || {};
      let s = Number(cand.rooms || cand.count || 0) > 0 ? 1 : 0;
      const url = f[MAP_FIRST_PASS.officialUrl] || f[MAP_FIRST_PASS.sourceUrl];
      if (url && !isForbiddenHost(url)) s += 3;
      if (isIdentityHigh(f)) s += 2;
      if (cand.status === "PENDING") s += 0.5;
      return s;
    };
    return scoreOf(b, recB) - scoreOf(a, recA);
  });
}

export async function runRoomsCandidateCorroboration(opts = {}) {
  const log = opts.log || (() => {});
  const records = opts.censusRecords || [];
  const byId = new Map(records.map((r) => [r.id, r]));
  const queue = loadRoomsCorroborationQueue();
  const fetchPageFn = opts.fetchPageFn;
  const maxProperties = Number(opts.maxProperties || 40);
  const proposals = [];
  let firstParty = 0;
  let twoSource = 0;
  let remaining = 0;
  let researched = 0;

  const pending = rankRoomsCorroborationQueue(
    queue.filter((c) => c.status !== "RESOLVED_HIGH" && c.status !== "REJECTED"),
    byId
  );
  for (const cand of pending.slice(0, maxProperties)) {
    const rec = byId.get(cand.id);
    if (!rec) {
      cand.status = "NOT_APPLICABLE";
      continue;
    }
    if (!isBlank(rec.fields?.[MAP_ROOMS.roomsKeys])) {
      cand.status = "ALREADY_POPULATED";
      continue;
    }
    const url =
      rec.fields?.[MAP_FIRST_PASS.officialUrl] ||
      rec.fields?.[MAP_FIRST_PASS.sourceUrl];
    if (!url || isForbiddenHost(url)) {
      cand.status = "NO_OFFICIAL_URL";
      remaining += 1;
      continue;
    }
    researched += 1;
    if (!fetchPageFn) {
      remaining += 1;
      continue;
    }
    try {
      const page = await fetchPageFn(url, rec);
      const html = page?.html || page?.text || "";
      const hits = extractRoomsKeysFromOfficialHtml(html, {
        url,
        propertyName: rec.fields?.[MAP_FIRST_PASS.propertyName],
      });
      const list = Array.isArray(hits) ? hits : hits?.hits || [];
      const best = selectBestRoomsHit(list);
      const extract = best
        ? {
            count: best.count,
            confidence: best.confidence,
            source_kind: "official_html",
            source_url: url,
            from_hbx_rooms_array: false,
            from_cvent_only: false,
            context: html.slice(0, 4000),
          }
        : null;
      const ev = evaluateRoomsCorroboration(cand, extract);
      if (ev.ok) {
        const built = buildRoomsHighPatch(rec.fields || {}, ev);
        if (built.ok) {
          proposals.push({ id: rec.id, fields: built.patch, class: ev.class });
          cand.status = "RESOLVED_HIGH";
          if (ev.class === "ROOMS_HIGH_FIRST_PARTY") firstParty += 1;
          if (ev.class === "ROOMS_HIGH_TWO_SOURCE") twoSource += 1;
        } else {
          cand.status = built.reason === "conflict" ? "CONFLICT" : "PENDING";
          remaining += 1;
        }
      } else {
        cand.status = ev.reason === "syndicated_same_upstream" ? "NOT_INDEPENDENT" : "PENDING";
        remaining += 1;
      }
    } catch (err) {
      cand.status = "SOURCE_BLOCKED";
      remaining += 1;
      log(`[rooms-corr] ${cand.id} ${String(err?.message || err).slice(0, 120)}`);
    }
  }

  remaining += pending.slice(maxProperties).filter((c) => c.status !== "RESOLVED_HIGH").length;
  saveRoomsCorroborationQueue(queue);
  log(
    `[rooms-corr] researched=${researched} first_party=${firstParty} two_source=${twoSource} remaining=${remaining}`
  );
  return {
    ok: true,
    version: ROOMS_CORROBORATION_VERSION,
    ROOM_CANDIDATES_BEFORE: queue.length,
    ROOM_CANDIDATES_CORROBORATED: firstParty + twoSource,
    ROOM_CANDIDATES_REMAINING: remaining,
    ROOMS_HIGH_FROM_FIRST_PARTY: firstParty,
    ROOMS_HIGH_FROM_TWO_SOURCE_CORROBORATION: twoSource,
    proposals,
    exhausted: remaining === 0 || researched === 0,
  };
}

void MAP_ROOMS_SOURCE_TYPE;
void todayIsoDate;
