/**
 * Match governed secondary Rooms sources (tourism-board / government open data)
 * onto existing Hotel Property Census records. Field-completion only — no inserts.
 *
 * Wave 2: stricter brand/location-aware fuzzy matching for Colombia RNT remainders.
 */

import {
  nameSimilarity,
  normalizeKey,
  citiesMatch,
} from "../independent-census/match-current-census.js";
import {
  buildColombiaRntSourceUrl,
  mapColombiaRntRowToCensusCandidate,
  parseColombiaRntRooms,
  MAP_COLOMBIA_RNT,
} from "./colombia-rnt-open-data-adapter.js";
import {
  ROOMS_EVIDENCE_TIER,
  buildRoomsProvenanceNotes,
  resolveRoomsConfidenceForSource,
  resolveRoomsSourceTypeForAirtable,
} from "./census-secondary-hotel-data-policy.js";
import { mapEvidenceTierCodeToSelect } from "./production-census-rooms-evidence-tier-schema.js";

export const ROOMS_SECONDARY_MATCH_VERSION = "census-rooms-secondary-match-v2";

const MIN_SIM_STRICT = 0.72;
const MIN_GAP = 0.08;
const STOP = new Set([
  "hotel",
  "hoteles",
  "the",
  "and",
  "del",
  "de",
  "la",
  "las",
  "los",
  "el",
  "san",
  "santa",
  "by",
  "a",
  "member",
  "of",
  "collection",
  "grand",
  "express", // kept in brandTokens separately
]);

const LOCATION_MARKERS = [
  "manga",
  "bocagrande",
  "buenavista",
  "airport",
  "aeropuerto",
  "norte",
  "sur",
  "centro",
  "parque",
  "calle",
  "zona",
  "morros",
  "chia",
  "chia",
];

function stripNoise(name) {
  return String(name || "")
    .replace(
      /\b(s\.?\s*a\.?s?\.?|ltda\.?|inc\.?|llc|corp\.?|hotel|hoteles|the)\b/gi,
      " "
    )
    .replace(/[™®]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokenSet(text, { keepExpress = false } = {}) {
  const stop = new Set(STOP);
  if (keepExpress) stop.delete("express");
  return new Set(
    normalizeKey(text)
      .split(/[^a-z0-9]+/)
      .filter((t) => t.length > 2 && !stop.has(t))
  );
}

function censusNames(fields = {}) {
  return [fields["Canonical Property Name"], fields["Property Name"]]
    .map((n) => String(n || "").trim())
    .filter(Boolean);
}

function scoreNamePair(censusName, sourceName) {
  return Math.max(
    nameSimilarity(censusName, sourceName),
    nameSimilarity(stripNoise(censusName), stripNoise(sourceName)),
    nameSimilarity(censusName, stripNoise(sourceName)),
    nameSimilarity(stripNoise(censusName), sourceName)
  );
}

function brandTokens(fields = {}) {
  const brand = String(fields["Current Brand"] || "");
  const family = String(fields["Brand Family"] || "");
  const raw = tokenSet(`${brand} ${family}`, { keepExpress: true });
  // Drop ultra-generic family-only tokens that collide across sub-brands
  for (const g of ["marriott", "hilton", "ihg", "accor", "wyndham", "choice", "hotels", "international", "resorts"]) {
    raw.delete(g);
  }
  return raw;
}

function locationTokens(text) {
  const n = normalizeKey(text);
  return LOCATION_MARKERS.filter((m) => n.includes(m));
}

function hasLocationConflict(censusText, sourceText) {
  const a = locationTokens(censusText);
  const b = locationTokens(sourceText);
  if (!a.length || !b.length) return false;
  return a.some((t) => !b.includes(t)) || b.some((t) => !a.includes(t));
}

function overlapCount(a, b) {
  let n = 0;
  for (const t of a) if (b.has(t)) n += 1;
  return n;
}

/**
 * Ranked Colombia RNT candidates for one census record (Wave 2 fuzzy).
 * @param {object} fields
 * @param {Record<string, unknown>[]} rntRows
 */
export function rankColombiaRntCandidates(fields, rntRows = []) {
  const city = fields.City || "";
  const names = censusNames(fields);
  if (!names.length || !city) return [];

  const brandTok = brandTokens(fields);
  /** @type {Array<object>} */
  const ranked = [];

  for (const row of rntRows) {
    const muni = row?.[MAP_COLOMBIA_RNT.municipio] || "";
    if (
      citiesMatch(city, muni) !== true &&
      normalizeKey(city) !== normalizeKey(muni)
    ) {
      continue;
    }
    const sourceName = String(row?.[MAP_COLOMBIA_RNT.razonSocial] || "").trim();
    if (!sourceName) continue;

    let bestSim = 0;
    let matchedName = names[0];
    for (const n of names) {
      const sim = scoreNamePair(n, sourceName);
      if (sim > bestSim) {
        bestSim = sim;
        matchedName = n;
      }
    }
    if (bestSim < 0.55) continue;

    const srcTok = tokenSet(sourceName, { keepExpress: true });
    const cenTok = tokenSet(matchedName, { keepExpress: true });
    const inter = overlapCount(cenTok, srcTok);
    const brandHits = overlapCount(brandTok, srcTok);
    const sourceShort = srcTok.size <= 1 || (srcTok.size <= 2 && inter < 2);
    const locConflict = hasLocationConflict(
      `${matchedName} ${fields.Address || ""}`,
      sourceName
    );
    const roomsParsed = parseColombiaRntRooms(
      row?.[MAP_COLOMBIA_RNT.habitaciones]
    );

    // Composite score: prefer distinctive token overlap + brand hits
    const score =
      bestSim +
      Math.min(inter, 4) * 0.04 +
      brandHits * 0.08 -
      (sourceShort ? 0.2 : 0) -
      (locConflict ? 0.5 : 0);

    ranked.push({
      sim: bestSim,
      score,
      row,
      matched_name: matchedName,
      source_name: sourceName,
      inter,
      brand_hits: brandHits,
      source_short: sourceShort,
      location_conflict: locConflict,
      source_tokens: srcTok.size,
      rooms_parse: roomsParsed,
      codigo_rnt: String(row?.[MAP_COLOMBIA_RNT.codigoRnt] || ""),
    });
  }

  ranked.sort((a, b) => b.score - a.score || b.sim - a.sim);
  return ranked;
}

/**
 * Wave 2 safe fuzzy accept rules for Colombia RNT.
 * @param {object} best
 * @param {object|null} second
 */
export function isSafeColombiaRntFuzzyMatch(best, second = null) {
  if (!best) return { ok: false, reason: "no_candidate" };
  if (best.location_conflict) {
    return { ok: false, reason: "location_token_conflict" };
  }
  if (best.source_short && best.inter < 3) {
    return { ok: false, reason: "source_name_too_generic" };
  }
  if (best.rooms_parse?.hold || !best.rooms_parse?.ok || best.rooms_parse.rooms == null) {
    return { ok: false, reason: "rnt_rooms_unparseable_or_hold" };
  }

  // Ignore short/generic seconds for gap
  const secondUseful =
    second && !second.source_short && second.score > best.score - 0.5
      ? second
      : null;
  const gap = best.score - (secondUseful?.score || 0);

  const brandOk = best.brand_hits >= 1 || best.inter >= 3;
  if (!brandOk) return { ok: false, reason: "brand_or_distinctive_tokens_insufficient" };

  const high =
    best.sim >= 0.85 && best.inter >= 2 && (gap >= MIN_GAP || !secondUseful);
  const mediumSafe =
    best.sim >= 0.7 &&
    best.inter >= 3 &&
    best.brand_hits >= 1 &&
    !best.source_short &&
    (gap >= MIN_GAP || !secondUseful);
  const fuzzySafe =
    best.sim >= 0.62 &&
    best.brand_hits >= 2 &&
    best.inter >= 3 &&
    !best.source_short &&
    (gap >= MIN_GAP || !secondUseful);

  if (high || mediumSafe || fuzzySafe) {
    return { ok: true, reason: high ? "high" : mediumSafe ? "medium_safe" : "fuzzy_brand_safe" };
  }
  if (secondUseful && gap < MIN_GAP) {
    return { ok: false, reason: "ambiguous_rnt_match" };
  }
  return { ok: false, reason: "below_safe_threshold" };
}

/**
 * Best unique RNT row for a census record.
 * @param {object} fields
 * @param {Record<string, unknown>[]} rntRows
 * @param {{ fuzzy?: boolean }} [opts] — Wave 2 fuzzy=true uses safer composite rules
 */
export function matchCensusToColombiaRntRooms(fields, rntRows = [], opts = {}) {
  const country = String(fields.Country || "");
  if (!/^colombia$/i.test(country)) {
    return { ok: false, reason: "country_not_colombia" };
  }
  const city = fields.City || "";
  if (!city) return { ok: false, reason: "census_city_blank" };
  const names = censusNames(fields);
  if (!names.length) return { ok: false, reason: "census_name_blank" };

  const ranked = rankColombiaRntCandidates(fields, rntRows);
  if (!ranked.length) return { ok: false, reason: "no_city_name_match" };

  const best = ranked[0];
  const second = ranked[1] || null;

  if (opts.fuzzy) {
    const gate = isSafeColombiaRntFuzzyMatch(best, second);
    if (!gate.ok) {
      return {
        ok: false,
        reason: gate.reason,
        best_sim: best.sim,
        best_name: best.source_name,
        second_name: second?.source_name || null,
        second_sim: second?.sim ?? null,
        steward_candidate: {
          census_name: best.matched_name,
          rnt_name: best.source_name,
          sim: best.sim,
          rooms: best.rooms_parse?.rooms ?? null,
          codigo_rnt: best.codigo_rnt,
        },
      };
    }
  } else {
    // Wave 1 strict path
    if (best.sim < MIN_SIM_STRICT) {
      return { ok: false, reason: "below_min_sim", best_sim: best.sim };
    }
    const secondUseful = second && !second.source_short ? second : null;
    const gap = best.sim - (secondUseful?.sim || 0);
    if (secondUseful && gap < MIN_GAP) {
      return {
        ok: false,
        reason: "ambiguous_rnt_match",
        best_sim: best.sim,
        second_sim: secondUseful.sim,
      };
    }
    if (best.location_conflict) {
      return { ok: false, reason: "location_token_conflict" };
    }
    if (!best.rooms_parse?.ok || best.rooms_parse.rooms == null || best.rooms_parse.hold) {
      return {
        ok: false,
        reason: best.rooms_parse?.hold
          ? "rnt_rooms_sanity_hold"
          : "rnt_rooms_unparseable",
      };
    }
  }

  const rooms = best.rooms_parse.rooms;
  const codigo = best.codigo_rnt;
  const sourceUrl = buildColombiaRntSourceUrl(codigo);
  const category = "tourism_board_convention_bureau_destination_authority";
  const evidenceTier = ROOMS_EVIDENCE_TIER.SECONDARY_TOURISM_BOARD;

  return {
    ok: true,
    adapter: "colombia_rnt",
    category,
    is_official: false,
    rooms,
    source_url: sourceUrl,
    source_type_airtable: resolveRoomsSourceTypeForAirtable({
      is_official: false,
      category,
    }),
    confidence: resolveRoomsConfidenceForSource({
      is_official: false,
      category,
    }),
    evidence_tier: evidenceTier,
    evidence_tier_select: mapEvidenceTierCodeToSelect(evidenceTier),
    match_sim: +best.sim.toFixed(3),
    matched_census_name: best.matched_name,
    matched_source_name: best.source_name,
    codigo_rnt: codigo,
    notes: buildRoomsProvenanceNotes({
      evidence_tier: evidenceTier,
      category,
      adapter: opts.fuzzy ? "colombia_rnt_fuzzy_v2" : "colombia_rnt",
      match_sim: +best.sim.toFixed(3),
      note: `rnt=${best.source_name}`,
    }),
  };
}

/**
 * Build a writable rooms patch from a secondary match (or conflict hold).
 * @param {object} fields
 * @param {object} match
 * @param {{ today?: string, roomsEvidenceTierFieldExists?: boolean }} [opts]
 */
export function buildSecondaryRoomsPatch(fields, match, opts = {}) {
  const today = opts.today || new Date().toISOString().slice(0, 10);
  const existing = fields?.["Rooms / Keys"];
  const existingBlank = existing == null || String(existing).trim() === "";
  const tierField = opts.roomsEvidenceTierFieldExists === true;

  if (!match?.ok) {
    return { ok: false, reason: match?.reason || "no_match", patch: null };
  }

  const tierSelect =
    match.evidence_tier_select ||
    mapEvidenceTierCodeToSelect(match.evidence_tier) ||
    null;

  if (!existingBlank && Number(existing) !== Number(match.rooms)) {
    /** @type {Record<string, unknown>} */
    const patch = {
      "Rooms Confidence": "Hold",
      "Rooms Source Type": resolveRoomsSourceTypeForAirtable({ conflict: true }),
      "Rooms Reviewed Date": today,
      "Rooms Notes": buildRoomsProvenanceNotes({
        evidence_tier: ROOMS_EVIDENCE_TIER.CONFLICT_HOLD,
        category: match.category,
        adapter: match.adapter,
        match_sim: match.match_sim,
        note: `existing=${existing} candidate=${match.rooms} url=${match.source_url}`,
      }),
      "Human Review Required": true,
    };
    if (tierField) {
      patch["Rooms Evidence Tier"] = mapEvidenceTierCodeToSelect(
        ROOMS_EVIDENCE_TIER.CONFLICT_HOLD
      );
    }
    return {
      ok: false,
      reason: "rooms_conflict_steward",
      conflict: true,
      existing: Number(existing),
      candidate: match.rooms,
      patch,
      write_rooms_value: false,
    };
  }

  if (!existingBlank && Number(existing) === Number(match.rooms)) {
    const hasUrl = Boolean(String(fields["Rooms Source URL"] || "").trim());
    const hasTier = Boolean(String(fields["Rooms Evidence Tier"] || "").trim());
    if (hasUrl && (!tierField || hasTier)) {
      return { ok: false, reason: "rooms_already_complete", patch: null };
    }
    /** @type {Record<string, unknown>} */
    const patch = {
      "Rooms Source URL": match.source_url,
      "Rooms Source Type": match.source_type_airtable,
      "Rooms Confidence": match.confidence,
      "Rooms Reviewed Date": today,
      "Rooms Notes": match.notes,
      "Last Reviewed Date": today,
    };
    if (tierField && tierSelect) patch["Rooms Evidence Tier"] = tierSelect;
    return {
      ok: true,
      write_rooms_value: false,
      provenance_backfill: true,
      patch,
      evidence_tier: match.evidence_tier,
      category: match.category,
      adapter: match.adapter,
    };
  }

  /** @type {Record<string, unknown>} */
  const patch = {
    "Rooms / Keys": match.rooms,
    "Rooms Confidence": match.confidence,
    "Rooms Source URL": match.source_url,
    "Rooms Source Type": match.source_type_airtable,
    "Rooms Reviewed Date": today,
    "Rooms Notes": match.notes,
    "Last Reviewed Date": today,
    "Enrichment Status": "Partial",
  };
  if (tierField && tierSelect) patch["Rooms Evidence Tier"] = tierSelect;

  return {
    ok: true,
    write_rooms_value: true,
    patch,
    evidence_tier: match.evidence_tier,
    category: match.category,
    adapter: match.adapter,
  };
}

/**
 * @param {Record<string, unknown>} row
 */
export function rntRowPreview(row) {
  const c = mapColombiaRntRowToCensusCandidate(row, { dryRun: true });
  return {
    codigo_rnt: c.codigo_rnt,
    name: c.fields?.[MAP_COLOMBIA_RNT.propertyName],
    city: c.fields?.[MAP_COLOMBIA_RNT.city],
    rooms: c.fields?.[MAP_COLOMBIA_RNT.roomsKeys] ?? null,
  };
}
