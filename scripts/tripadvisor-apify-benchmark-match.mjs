#!/usr/bin/env node
/**
 * Match Tripadvisor Actor outputs to Dealality census samples — READ ONLY.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const DIR = path.join(ROOT, "data/hotel-intelligence/tripadvisor-apify-benchmark-v1");
const REPORT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/tripadvisor-apify-benchmark-v1"
);

function fold(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokens(s) {
  return fold(s)
    .split(/\s+/)
    .filter((t) => t.length > 2)
    .filter(
      (t) =>
        !["hotel", "hotels", "the", "and", "spa", "resort", "resorts", "by"].includes(
          t
        )
    );
}

function nameSimilarity(a, b) {
  const na = fold(a);
  const nb = fold(b);
  if (!na || !nb) return 0;
  if (na === nb) return 1;
  if (na.includes(nb) || nb.includes(na)) return 0.92;
  const ta = new Set(tokens(a));
  const tb = new Set(tokens(b));
  if (!ta.size || !tb.size) return 0;
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter += 1;
  return inter / Math.max(ta.size, tb.size);
}

function haversineKm(lat1, lon1, lat2, lon2) {
  if ([lat1, lon1, lat2, lon2].some((v) => v == null || !Number.isFinite(Number(v))))
    return null;
  const toRad = (d) => (Number(d) * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const x =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(x));
}

function isHotelItem(it) {
  const type = String(it.type || "").toUpperCase();
  const cat = String(it.category || "").toLowerCase();
  if (["RESTAURANT", "ATTRACTION", "VACATION_RENTAL", "ACTIVITY"].includes(type))
    return false;
  if (["restaurant", "attraction", "vacation_rental"].includes(cat)) return false;
  if (type === "HOTEL" || cat === "hotel") return true;
  // some hotel rows may use lodging subtypes
  if (/hotel|resort|inn|lodge|hostel|boutique/i.test(String(it.name || ""))) {
    return type !== "RESTAURANT";
  }
  return false;
}

function usableRoomCount(it) {
  const t = Number(it?.numberOfRooms);
  if (!Number.isFinite(t) || t <= 0) return false;
  // Tripadvisor sometimes emits 1 for mega-resorts while description cites thousands of rooms.
  const reviews = Number(it?.numberOfReviews) || 0;
  const desc = String(it?.description || "");
  const descRooms = desc.match(/(\d{2,5})\s*(?:room|suite|key)/i);
  if (t <= 2 && (reviews >= 500 || (descRooms && Number(descRooms[1]) >= 50))) {
    return false;
  }
  return true;
}

function classifyRooms(dealalityRooms, taRooms, taItem) {
  if (!usableRoomCount({ numberOfRooms: taRooms, ...taItem })) return "MISSING";
  const d = Number(dealalityRooms);
  const t = Number(taRooms);
  if (!Number.isFinite(d)) return "MISSING";
  if (d === t) return "EXACT";
  const diff = Math.abs(d - t);
  const pct = diff / Math.max(d, 1);
  if (diff <= 5 || pct <= 0.05) return "NEAR_MATCH";
  return "CONFLICT";
}

const COUNTRY_ALIASES = {
  "dominican republic": ["dominican republic", "caribbean", "dominicana"],
  caribbean: ["caribbean", "dominican republic"],
  "costa rica": ["costa rica"],
  colombia: ["colombia"],
  mexico: ["mexico", "méxico"],
  panama: ["panama", "panamá"],
  brazil: ["brazil", "brasil"],
  argentina: ["argentina"],
  chile: ["chile"],
  peru: ["peru", "perú"],
  ecuador: ["ecuador"],
  jamaica: ["jamaica"],
  "puerto rico": ["puerto rico"],
  aruba: ["aruba"],
  bahamas: ["bahamas"],
  barbados: ["barbados"],
  belize: ["belize"],
  guatemala: ["guatemala"],
  honduras: ["honduras"],
  nicaragua: ["nicaragua"],
  "el salvador": ["el salvador"],
  venezuela: ["venezuela"],
  uruguay: ["uruguay"],
  bolivia: ["bolivia"],
  paraguay: ["paraguay"],
  suriname: ["suriname"],
  guyana: ["guyana"],
  "trinidad and tobago": ["trinidad", "tobago"],
  curacao: ["curacao", "curaçao"],
  "sint maarten": ["sint maarten", "st maarten", "saint martin"],
  "saint lucia": ["saint lucia", "st lucia"],
  "cayman islands": ["cayman", "grand cayman"],
  "turks and caicos islands": ["turks", "caicos"],
  "u.s. virgin islands": ["virgin islands", "st thomas", "st croix"],
  "british virgin islands": ["virgin islands", "virgin gorda", "peter island"],
};

function countryCompatible(dealalityCountry, taItem) {
  const dc = fold(dealalityCountry);
  if (!dc) return true;
  const taCountry = fold(taItem?.addressObj?.country || "");
  const loc = fold(
    [taItem?.locationString, taItem?.address, taCountry].filter(Boolean).join(" ")
  );
  // Reject clear US false positives for non-US Dealality hotels
  if (
    dc !== "united states" &&
    (taCountry === "united states" || /\bunited states\b|\busa\b|, (ga|nd|tx|fl|ca|ny)\b/.test(loc))
  ) {
    return false;
  }
  const aliases = COUNTRY_ALIASES[dc] || [dc.split(" ")[0]];
  if (aliases.some((a) => loc.includes(fold(a)))) return true;
  // soft pass when geo is close (lat/lng available)
  return null; // undecided — let geo decide
}

function bestMatch(hotel, pool) {
  let best = null;
  const lat = hotel.lat ?? hotel.latitude ?? null;
  const lng = hotel.lng ?? hotel.longitude ?? null;
  for (const it of pool) {
    if (!isHotelItem(it)) continue;
    const compat = countryCompatible(hotel.country, it);
    if (compat === false) continue;
    const sim = nameSimilarity(hotel.name, it.name);
    if (sim < 0.55) continue;
    const km = haversineKm(lat, lng, it.latitude, it.longitude);
    // Hard reject distant geography when coords exist
    if (km != null && km > 50) continue;
    // If country undecided and no useful geo, require stronger name overlap
    if (compat === null && km == null && sim < 0.85) continue;
    let score = sim;
    if (km != null && km <= 1) score += 0.15;
    else if (km != null && km <= 3) score += 0.1;
    else if (km != null && km <= 10) score += 0.05;
    if (compat === true) score += 0.08;
    if (!best || score > best.score) best = { item: it, score, sim, km };
  }
  if (!best) return null;
  // Minimum acceptance after scoring
  if (best.score < 0.72) return null;
  let confidence = "low";
  if (best.score >= 0.95 || (best.sim >= 0.9 && best.km != null && best.km <= 3))
    confidence = "high";
  else if (best.score >= 0.82) confidence = "medium";
  best.confidence = confidence;
  return best;
}

function loadPool() {
  const poolPath = path.join(DIR, "ta-pool.json");
  if (!fs.existsSync(poolPath)) return [];
  const raw = JSON.parse(fs.readFileSync(poolPath, "utf8"));
  return Array.isArray(raw.items) ? raw.items : [];
}

function evaluatePhase(hotels, pool, phase) {
  const rows = [];
  for (const h of hotels) {
    const m = bestMatch(h, pool);
    if (!m) {
      rows.push({
        phase,
        dealality_record_id: h.record_id,
        dealality_name: h.name,
        dealality_country: h.country,
        dealality_city: h.city,
        dealality_rooms: h.rooms ?? null,
        tripadvisor_id: null,
        tripadvisor_name: null,
        tripadvisor_numberOfRooms: null,
        tripadvisor_url: null,
        website: null,
        email: null,
        phone: null,
        match_confidence: "none",
        match_score: 0,
        room_comparison: "NO_MATCH",
        false_match_risk: false,
      });
      continue;
    }
    const it = m.item;
    const roomCmp =
      phase === "known"
        ? classifyRooms(h.rooms, it.numberOfRooms, it)
        : "MISSING";
    const usableRooms = usableRoomCount(it);
    const likelyFalse =
      m.confidence === "low" ||
      (m.km != null && m.km > 25) ||
      (m.sim < 0.7 && (m.km == null || m.km > 5)) ||
      // Sister-brand / truncated-name collisions: high name overlap but not identity-safe
      (m.sim < 0.92 &&
        phase === "known" &&
        classifyRooms(h.rooms, it.numberOfRooms, it) === "CONFLICT" &&
        Math.abs(Number(h.rooms) - Number(it.numberOfRooms)) /
          Math.max(Number(h.rooms), 1) >
          0.2);
    rows.push({
      phase,
      dealality_record_id: h.record_id,
      dealality_name: h.name,
      dealality_country: h.country,
      dealality_city: h.city,
      dealality_rooms: h.rooms ?? null,
      tripadvisor_id: it.id || null,
      tripadvisor_name: it.name || null,
      tripadvisor_numberOfRooms:
        it.numberOfRooms != null ? Number(it.numberOfRooms) : null,
      tripadvisor_url: it.webUrl || null,
      website: it.website || null,
      email: it.email || null,
      phone: it.phone || null,
      hotelClass: it.hotelClass || null,
      type: it.type || null,
      category: it.category || null,
      match_confidence: m.confidence,
      match_score: Math.round(m.score * 1000) / 1000,
      name_similarity: Math.round(m.sim * 1000) / 1000,
      geo_km: m.km != null ? Math.round(m.km * 100) / 100 : null,
      room_comparison:
        phase === "known"
          ? roomCmp
          : usableRooms
            ? "RECOVERED"
            : "MISSING",
      false_match_risk: likelyFalse,
    });
  }
  return rows;
}

function summarize(knownRows, missRows, cost) {
  const matched = (rows) => rows.filter((r) => r.room_comparison !== "NO_MATCH");
  const kMatched = matched(knownRows);
  const mMatched = matched(missRows);
  const n = knownRows.length || 1;
  const mn = missRows.length || 1;
  const exact = knownRows.filter((r) => r.room_comparison === "EXACT").length;
  const near = knownRows.filter((r) => r.room_comparison === "NEAR_MATCH").length;
  const conflict = knownRows.filter((r) => r.room_comparison === "CONFLICT").length;
  const missingRooms = knownRows.filter((r) => r.room_comparison === "MISSING").length;
  const noMatch = knownRows.filter((r) => r.room_comparison === "NO_MATCH").length;
  const roomsOnMatch = kMatched.filter(
    (r) => r.tripadvisor_numberOfRooms != null
  ).length;
  const recovered = missRows.filter((r) => r.room_comparison === "RECOVERED").length;
  const falseMatch = knownRows.filter(
    (r) => r.false_match_risk && r.room_comparison !== "NO_MATCH"
  ).length;
  const allMatched = [...kMatched, ...mMatched];
  const websiteCov =
    allMatched.filter((r) => r.website).length / Math.max(allMatched.length, 1);
  const emailCov =
    allMatched.filter((r) => r.email).length / Math.max(allMatched.length, 1);
  const phoneCov =
    allMatched.filter((r) => r.phone).length / Math.max(allMatched.length, 1);

  const recoveredCount = recovered;
  const costPerRecovered =
    recoveredCount > 0 && cost?.total_usd != null
      ? Math.round((cost.total_usd / recoveredCount) * 10000) / 10000
      : null;

  return {
    tripadvisor_property_match_rate_known:
      Math.round((1000 * kMatched.length) / n) / 10,
    tripadvisor_property_match_rate_missing:
      Math.round((1000 * mMatched.length) / mn) / 10,
    tripadvisor_property_match_rate_overall:
      Math.round(
        (1000 * (kMatched.length + mMatched.length)) /
          (knownRows.length + missRows.length || 1)
      ) / 10,
    numberOfRooms_coverage_among_matches:
      Math.round((1000 * roomsOnMatch) / Math.max(kMatched.length, 1)) / 10,
    exact_accuracy_pct: Math.round((1000 * exact) / n) / 10,
    exact_among_matched_pct:
      Math.round((1000 * exact) / Math.max(kMatched.length, 1)) / 10,
    near_match_rate_pct: Math.round((1000 * near) / n) / 10,
    conflict_rate_pct: Math.round((1000 * conflict) / n) / 10,
    missing_rooms_on_matched_pct:
      Math.round((1000 * missingRooms) / Math.max(kMatched.length, 1)) / 10,
    no_match_rate_pct: Math.round((1000 * noMatch) / n) / 10,
    false_match_rate_pct: Math.round((1000 * falseMatch) / n) / 10,
    room_count_recovery_rate_missing_pct:
      Math.round((1000 * recovered) / mn) / 10,
    website_coverage_among_matches_pct: Math.round(websiteCov * 1000) / 10,
    email_coverage_among_matches_pct: Math.round(emailCov * 1000) / 10,
    phone_coverage_among_matches_pct: Math.round(phoneCov * 1000) / 10,
    counts: {
      known: knownRows.length,
      missing: missRows.length,
      known_matched: kMatched.length,
      missing_matched: mMatched.length,
      exact,
      near,
      conflict,
      missing_rooms_field: missingRooms,
      no_match: noMatch,
      recovered_rooms: recovered,
      low_confidence_matches: falseMatch,
    },
    cost: cost || null,
    cost_per_successfully_recovered_room_count: costPerRecovered,
  };
}

function main() {
  const samples = JSON.parse(
    fs.readFileSync(path.join(DIR, "samples.json"), "utf8")
  );
  const pool = loadPool();
  const knownRows = evaluatePhase(samples.phase2_known, pool, "known");
  const missRows = evaluatePhase(samples.phase3_missing, pool, "missing");
  const costPath = path.join(DIR, "cost-estimate.json");
  const cost = fs.existsSync(costPath)
    ? JSON.parse(fs.readFileSync(costPath, "utf8"))
    : null;
  const metrics = summarize(knownRows, missRows, cost);

  fs.mkdirSync(REPORT_DIR, { recursive: true });
  fs.writeFileSync(
    path.join(DIR, "phase2-known-results.json"),
    JSON.stringify({ rows: knownRows }, null, 2)
  );
  fs.writeFileSync(
    path.join(DIR, "phase3-missing-results.json"),
    JSON.stringify({ rows: missRows }, null, 2)
  );
  fs.writeFileSync(
    path.join(DIR, "metrics.json"),
    JSON.stringify(metrics, null, 2)
  );
  fs.writeFileSync(
    path.join(REPORT_DIR, "metrics.json"),
    JSON.stringify(metrics, null, 2)
  );

  console.log(
    JSON.stringify(
      {
        pool_size: pool.length,
        pool_hotels: pool.filter(isHotelItem).length,
        metrics,
      },
      null,
      2
    )
  );
}

main();
