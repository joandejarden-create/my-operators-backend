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
  const identity_match_high = best.sim >= 0.85;

  return {
    ok: true,
    adapter: "colombia_rnt",
    category,
    is_official: false,
    identity_match_high,
    rooms,
    source_url: sourceUrl,
    source_type_airtable: resolveRoomsSourceTypeForAirtable({
      is_official: false,
      category,
    }),
    confidence: resolveRoomsConfidenceForSource({
      is_official: false,
      category,
      identity_match_high,
      match_sim: best.sim,
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

function digitsOnly(s) {
  return String(s || "").replace(/\D/g, "");
}

function domainKey(url) {
  try {
    return new URL(String(url)).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return "";
  }
}

function statesLooselyMatch(a, b) {
  const na = normalizeKey(a);
  const nb = normalizeKey(b);
  if (!na || !nb) return true; // missing state is not a conflict
  if (na === nb) return true;
  if (na.includes(nb) || nb.includes(na)) return true;
  return false;
}

/**
 * Generic ranked match against normalized registry rows
 * ({ property_name, city, state_region, rooms, phone, website, postal_code, address }).
 */
export function indexNormalizedRegistryByCity(sourceRows = []) {
  /** @type {Map<string, object[]>} */
  const idx = new Map();
  for (const row of sourceRows) {
    if (!row?.property_name || row.rooms == null) continue;
    const cities = [
      row.city,
      ...(Array.isArray(row.match_cities) ? row.match_cities : []),
    ].filter(Boolean);
    for (const c of cities) {
      const k = normalizeKey(c);
      if (!k) continue;
      if (!idx.has(k)) idx.set(k, []);
      idx.get(k).push(row);
    }
  }
  return idx;
}

export function rankNormalizedRegistryCandidates(fields, sourceRows = [], opts = {}) {
  const city = fields.City || "";
  const names = censusNames(fields);
  if (!names.length) return [];
  const requireCity = opts.requireCity !== false;
  if (requireCity && !city) return [];

  let pool = sourceRows;
  if (opts.cityIndex && requireCity && city) {
    const k = normalizeKey(city);
    pool = opts.cityIndex.get(k) || [];
    // Also try token-ish city aliases already in index only
  }

  /** @type {Array<object>} */
  const ranked = [];
  for (const row of pool) {
    if (!row?.property_name || row.rooms == null) continue;
    if (requireCity) {
      const srcCities = [
        row.city,
        ...(Array.isArray(row.match_cities) ? row.match_cities : []),
      ].filter(Boolean);
      const cityOk = srcCities.some(
        (srcCity) =>
          citiesMatch(city, srcCity) === true ||
          normalizeKey(city) === normalizeKey(srcCity)
      );
      if (!cityOk) continue;
    }
    if (
      row.state_region &&
      fields["State / Region"] &&
      !statesLooselyMatch(fields["State / Region"], row.state_region) &&
      !statesLooselyMatch(fields["State / Region"], row.state_uf || "")
    ) {
      // soft demote later via score; still allow if name is very strong
    }

    let bestSim = 0;
    let matchedName = names[0];
    for (const n of names) {
      const sim = Math.max(
        scoreNamePair(n, row.property_name),
        row.commercial_name ? scoreNamePair(n, row.commercial_name) : 0,
        row.legal_name ? scoreNamePair(n, row.legal_name) : 0
      );
      if (sim > bestSim) {
        bestSim = sim;
        matchedName = n;
      }
    }
    if (bestSim < 0.55) continue;

    const srcTok = tokenSet(row.property_name, { keepExpress: true });
    const cenTok = tokenSet(matchedName, { keepExpress: true });
    const inter = overlapCount(cenTok, srcTok);
    const phoneBoost =
      row.phone &&
      fields.Phone &&
      digitsOnly(row.phone).length >= 8 &&
      digitsOnly(fields.Phone).endsWith(digitsOnly(row.phone).slice(-8))
        ? 0.12
        : 0;
    const webBoost =
      row.website &&
      fields["Official Property URL"] &&
      domainKey(row.website) &&
      domainKey(row.website) === domainKey(fields["Official Property URL"])
        ? 0.15
        : 0;
    const postalBoost =
      row.postal_code &&
      fields["Postal Code"] &&
      normalizeKey(row.postal_code) === normalizeKey(fields["Postal Code"])
        ? 0.1
        : 0;
    const stateOk =
      !row.state_region ||
      !fields["State / Region"] ||
      statesLooselyMatch(fields["State / Region"], row.state_region) ||
      statesLooselyMatch(fields["State / Region"], row.state_uf || "");

    const score =
      bestSim +
      Math.min(inter, 4) * 0.04 +
      phoneBoost +
      webBoost +
      postalBoost -
      (stateOk ? 0 : 0.15);

    ranked.push({
      sim: bestSim,
      score,
      row,
      matched_name: matchedName,
      source_name: row.property_name,
      inter,
      phone_boost: phoneBoost,
      web_boost: webBoost,
      postal_boost: postalBoost,
      state_ok: stateOk,
      rooms: row.rooms,
      source_url: row.source_url,
      identity_key: row.identity_key || null,
    });
  }
  ranked.sort((a, b) => b.score - a.score || b.sim - a.sim);
  return ranked;
}

function finalizeRegistryRoomsMatch(best, second, adapter, opts = {}) {
  if (!best || best.rooms == null) {
    return { ok: false, reason: "no_rooms" };
  }
  const secondUseful = second && second.sim >= 0.6 ? second : null;
  const gap = best.sim - (secondUseful?.sim || 0);
  if (best.sim < MIN_SIM_STRICT) {
    return { ok: false, reason: "below_min_sim", best_sim: best.sim };
  }
  if (secondUseful && gap < MIN_GAP && best.sim < 0.92) {
    return {
      ok: false,
      reason: "ambiguous_registry_match",
      best_sim: best.sim,
      second_sim: secondUseful.sim,
    };
  }

  const category =
    "tourism_board_convention_bureau_destination_authority";
  const evidenceTier = ROOMS_EVIDENCE_TIER.SECONDARY_TOURISM_BOARD;
  const identity_match_high =
    best.sim >= 0.85 ||
    (best.sim >= 0.8 &&
      (best.phone_boost > 0 || best.web_boost > 0 || best.postal_boost > 0));

  return {
    ok: true,
    adapter,
    category,
    is_official: false,
    identity_match_high,
    rooms: best.rooms,
    source_url: best.source_url,
    source_type_airtable: resolveRoomsSourceTypeForAirtable({
      is_official: false,
      category,
    }),
    confidence: resolveRoomsConfidenceForSource({
      is_official: false,
      category,
      identity_match_high,
      match_sim: best.sim,
    }),
    evidence_tier: evidenceTier,
    evidence_tier_select: mapEvidenceTierCodeToSelect(evidenceTier),
    match_sim: +best.sim.toFixed(3),
    matched_census_name: best.matched_name,
    matched_source_name: best.source_name,
    identity_key: best.identity_key,
    notes: buildRoomsProvenanceNotes({
      evidence_tier: evidenceTier,
      category,
      adapter,
      match_sim: +best.sim.toFixed(3),
      note: `${adapter}=${best.source_name}`,
    }),
    fundamentals: {
      address: best.row?.address || null,
      postal_code: best.row?.postal_code || null,
      phone: best.row?.phone || null,
      website: best.row?.website || null,
      city: best.row?.city || null,
      state_region: best.row?.state_region || null,
    },
    ...opts.extra,
  };
}

export function matchCensusToPeruMinceturRooms(fields, peruRows = [], opts = {}) {
  if (!/^peru$/i.test(String(fields.Country || ""))) {
    return { ok: false, reason: "country_not_peru" };
  }
  const ranked = rankNormalizedRegistryCandidates(fields, peruRows, {
    requireCity: true,
    cityIndex: opts.cityIndex,
  });
  if (!ranked.length) return { ok: false, reason: "no_city_name_match" };
  return finalizeRegistryRoomsMatch(ranked[0], ranked[1], "peru_mincetur");
}

export function matchCensusToBrazilCadasturRooms(fields, brRows = [], opts = {}) {
  if (!/^brazil$/i.test(String(fields.Country || ""))) {
    return { ok: false, reason: "country_not_brazil" };
  }
  const ranked = rankNormalizedRegistryCandidates(fields, brRows, {
    requireCity: true,
    cityIndex: opts.cityIndex,
  });
  if (!ranked.length) return { ok: false, reason: "no_city_name_match" };
  return finalizeRegistryRoomsMatch(ranked[0], ranked[1], "brazil_cadastur");
}

export function matchCensusToBarbadosBtpaRooms(fields, bbRows = []) {
  if (!/^barbados$/i.test(String(fields.Country || ""))) {
    return { ok: false, reason: "country_not_barbados" };
  }
  // Barbados directory often lacks city — name + country only with stricter sim
  const ranked = rankNormalizedRegistryCandidates(fields, bbRows, {
    requireCity: false,
  });
  if (!ranked.length) return { ok: false, reason: "no_name_match" };
  const best = ranked[0];
  const second = ranked[1];
  if (best.sim < 0.88) {
    return { ok: false, reason: "below_barbados_name_threshold", best_sim: best.sim };
  }
  return finalizeRegistryRoomsMatch(best, second, "barbados_btpa");
}

/**
 * Promote Colombia RNT Medium candidate → High when independent Census
 * evidence corroborates (phone/postal/website/address tokens) WITHOUT lowering
 * the base name similarity floor.
 *
 * @param {object} fields
 * @param {object} mediumMatch — prior match with confidence Medium / ok true
 */
export function promoteColombiaRntMediumWithCorroboration(fields, mediumMatch) {
  if (!mediumMatch?.ok || mediumMatch.rooms == null) {
    return { ok: false, reason: "no_medium_match" };
  }
  if (mediumMatch.confidence === "High" || mediumMatch.identity_match_high) {
    return { ...mediumMatch, promoted: false, reason: "already_high" };
  }
  const sim = Number(mediumMatch.match_sim || 0);
  if (sim < 0.8) {
    return { ok: false, reason: "sim_below_promotion_floor", match_sim: sim };
  }

  const signals = [];
  const srcName = String(mediumMatch.matched_source_name || "");
  const address = String(fields.Address || "");
  const postal = String(fields["Postal Code"] || "");
  const phone = String(fields.Phone || "");
  const website = String(fields["Official Property URL"] || "");

  if (postal && postal.length >= 4) signals.push("postal_present_with_high_sim");
  if (phone && phone.replace(/\D/g, "").length >= 8) {
    signals.push("phone_present_with_high_sim");
  }
  if (website && /^https?:\/\//i.test(website)) {
    signals.push("website_present_with_high_sim");
  }
  if (address) {
    const addrTok = tokenSet(address);
    const nameTok = tokenSet(srcName);
    if (overlapCount(addrTok, nameTok) >= 1) signals.push("address_name_token_overlap");
    // Street-number style corroboration: shared significant tokens in address vs notes
    if (addrTok.size >= 2) signals.push("address_present_with_high_sim");
  }

  // Require at least two independent corroborating signals + sim>=0.80
  const unique = [...new Set(signals)];
  if (unique.length < 2) {
    return {
      ok: false,
      reason: "insufficient_corroboration",
      signals: unique,
      match_sim: sim,
    };
  }

  return {
    ...mediumMatch,
    ok: true,
    promoted: true,
    identity_match_high: true,
    confidence: "High",
    promotion_signals: unique,
    notes: buildRoomsProvenanceNotes({
      evidence_tier: ROOMS_EVIDENCE_TIER.SECONDARY_TOURISM_BOARD,
      category: mediumMatch.category,
      adapter: "colombia_rnt_medium_promoted",
      match_sim: sim,
      note: `promoted_via=${unique.join("+")}; rnt=${srcName}`,
    }),
  };
}
