/**
 * Independent verification waterfall v2 for Tripadvisor room-count candidates.
 *
 * Order:
 *  1) Official hotel/brand site path crawl (+ fact-sheet PDFs)
 *  2) Structured APIs (Hotelbeds) when code available
 *  3) Existing room-count research (SerpApi + page fetches)
 *  4) Extra approved observations hook
 *
 * Tripadvisor is NEVER an independent confirming source.
 * OTA↔OTA or Tripadvisor↔Hotelbeds agreement alone ≠ VERIFIED_MULTI_SOURCE.
 */

import { researchHotelRoomCount } from "../room-count-research/research.js";
import { SOURCE_CATEGORIES } from "../room-count-research/trust.js";
import { isFetchEligibleUrl, classifySourceUrl } from "../room-count-research/trust.js";
import { ROOMS_VERIFICATION_STATUS } from "./constants.js";
import { verifyOfficialWebsiteRoomCount } from "./official-site-verify.js";
import {
  annotateIndependence,
  assessMultiSourceIndependence,
} from "./independence.js";
import { classifyRoomConflict } from "./conflicts.js";

export const TRIPADVISOR_VERIFY_VERSION = "tripadvisor-verify-v2";

function nearEqual(a, b) {
  const x = Number(a);
  const y = Number(b);
  if (!Number.isFinite(x) || !Number.isFinite(y)) return false;
  if (x === y) return true;
  const diff = Math.abs(x - y);
  return diff <= 5 || diff / Math.max(x, 1) <= 0.05;
}

function isOfficialCategory(cat) {
  return [
    SOURCE_CATEGORIES.OFFICIAL_HOTEL,
    SOURCE_CATEGORIES.OFFICIAL_BRAND,
    SOURCE_CATEGORIES.OFFICIAL_OWNER,
    SOURCE_CATEGORIES.OFFICIAL_OPERATOR,
  ].includes(cat);
}

function tierOf(obs) {
  if (isOfficialCategory(obs.source_category)) return 1;
  if (
    /Tourism Authority|Convention Bureau|Destination Marketing|Official Owner/i.test(
      String(obs.source_category || "")
    )
  ) {
    return 2;
  }
  if (/Historic Press Release|News/i.test(String(obs.source_category || ""))) return 3;
  if (obs.provider === "hotelbeds") return 2; // structured but independence-gated
  if (/tripadvisor|booking|expedia|hotels\.com|agoda|google/i.test(String(obs.url || "")))
    return 4;
  return 3;
}

/**
 * @param {object} hotel
 * @param {number} taRooms
 * @param {object} [opts]
 */
export async function verifyTripadvisorRoomCandidate(hotel, taRooms, opts = {}) {
  const t0 = Date.now();
  const candidate = Number(taRooms);
  /** @type {Array<object>} */
  let independent = [];
  const steps = [];
  let official_website_room_count_found = false;
  let official_pdf_factsheet_found = false;
  let secondary_source_verifications = 0;
  let serpapi_searches = 0;
  let pages_fetched = 0;

  // --- 1) Official website path crawl ---
  const websites = [
    ...new Set(
      [hotel.website, ...(hotel.websites || [])]
        .map((u) => String(u || "").trim())
        .filter(Boolean)
    ),
  ].filter((u) => !/tripadvisor\./i.test(u));

  for (const website of websites.slice(0, 2)) {
    if (!isFetchEligibleUrl(website)) continue;
    steps.push({ step: "official_site_crawl", url: website });
    const official = await verifyOfficialWebsiteRoomCount(website, {
      hotelName: hotel.name,
      brand: hotel.brand,
      maxPages: opts.officialMaxPages ?? 8,
      stopOnHit: opts.officialStopOnHit !== false,
      timeoutMs: opts.timeoutMs,
    });
    pages_fetched += official.pages_fetched || 0;
    steps.push(...(official.steps || []));
    if (official.official_website_room_count_found)
      official_website_room_count_found = true;
    if (official.official_pdf_factsheet_found) official_pdf_factsheet_found = true;
    for (const ev of official.evidence || []) independent.push(ev);
  }

  // --- 2) Hotelbeds structured API ---
  if (opts.hotelbedsProvider && (hotel.hbx_hotel_code || hotel.external_ids?.hbx_hotel_code)) {
    const code = hotel.hbx_hotel_code || hotel.external_ids.hbx_hotel_code;
    steps.push({ step: "hotelbeds_get", code });
    try {
      const res = await opts.hotelbedsProvider.getHotel(String(code));
      const rooms = res?.hotel?.room_count;
      if (rooms != null && Number(rooms) > 0) {
        secondary_source_verifications += 1;
        independent.push({
          value: Number(rooms),
          source_category: SOURCE_CATEGORIES.TRUSTED_DIRECTORY,
          url: null,
          quote: `hotelbeds roomsNumber=${rooms}`,
          evidence_text: `hotelbeds roomsNumber=${rooms}`,
          provider: "hotelbeds",
          source_provider: "hotelbeds",
          retrieval_timestamp: new Date().toISOString(),
          extraction_method: "hotelbeds_roomsNumber",
          confidence: "Medium",
          is_official_hotel_site: false,
          is_official_brand_site: false,
          is_pdf_factsheet: false,
        });
      }
    } catch (err) {
      steps.push({
        step: "hotelbeds_error",
        message: String(err?.message || err).slice(0, 120),
      });
    }
  } else {
    steps.push({ step: "hotelbeds_skip", reason: "no_hbx_code" });
  }

  // StayingAPI / SerpApi Google Hotels / GIATA Drive: room_count not supported — skip
  steps.push({
    step: "provider_capability_skip",
    providers: ["stayingapi", "serpapi_google_hotels", "giata_drive"],
    reason: "room_count_not_supported_or_firewalled",
  });

  // --- 3) Search-based room-count research (existing infrastructure) ---
  const needSearch =
    opts.allowRoomCountResearch !== false &&
    (opts.forceSearch === true ||
      !independent.some((o) => isOfficialCategory(o.source_category) && nearEqual(o.value, candidate)));

  if (needSearch) {
    steps.push({ step: "room_count_research" });
    try {
      const research = await researchHotelRoomCount(
        {
          hotel_id: hotel.record_id || hotel.hotel_id,
          hotel_name: hotel.name,
          city: hotel.city,
          country: hotel.country,
          brand: hotel.brand,
          website: hotel.website || websites[0] || null,
        },
        {
          env: opts.env,
          maxSearches: opts.maxSearches ?? 3,
          maxPageFetches: opts.maxPageFetches ?? 4,
          allowSerpapi: opts.allowSerpapi !== false,
        }
      );
      serpapi_searches += Number(research.metrics?.searches || 0);
      pages_fetched += Number(research.metrics?.pages_fetched || 0);
      for (const obs of research.observations || []) {
        if (obs.rejected) continue;
        const url = String(obs.url || "").toLowerCase();
        if (url.includes("tripadvisor.")) continue;
        // Snippets alone: keep but mark lower confidence; page fetches preferred
        const fromSnippet = String(obs.step || "").includes("snippet");
        independent.push({
          value: Number(obs.value),
          source_category: obs.source_category,
          url: obs.url,
          quote: obs.quote,
          evidence_text: obs.quote,
          provider: "room_count_research",
          source_provider: "room_count_research",
          retrieval_timestamp: obs.observed_at || new Date().toISOString().slice(0, 10),
          extraction_method: obs.method || obs.step,
          confidence: fromSnippet ? "Medium" : obs.confidence_label || "High",
          snippet_only: fromSnippet,
          is_official_hotel_site: isOfficialCategory(obs.source_category),
          is_official_brand_site: obs.source_category === SOURCE_CATEGORIES.OFFICIAL_BRAND,
          is_pdf_factsheet: /\.pdf(\?|#|$)/i.test(url),
          step: obs.step,
        });
        if (!fromSnippet && nearEqual(obs.value, candidate)) {
          secondary_source_verifications += 1;
        }
      }
      steps.push({
        step: "room_count_research_done",
        status: research.research_status,
        candidate: research.candidate_room_count,
        metrics: research.metrics || null,
      });
    } catch (err) {
      steps.push({
        step: "room_count_research_error",
        message: String(err?.message || err).slice(0, 120),
      });
    }
  }

  // --- 4) Extra hook ---
  if (typeof opts.extraObservations === "function") {
    const extra = await opts.extraObservations(hotel, candidate);
    for (const o of extra || []) independent.push(o);
  }

  independent = annotateIndependence(independent);

  // Drop pure Tier-4 OTA hits from "agreeing independent" for promotion (keep in observations)
  const promotionPool = independent.filter((o) => tierOf(o) <= 3 || isOfficialCategory(o.source_category));

  const agreeing = promotionPool.filter((o) => nearEqual(o.value, candidate));
  const conflicting = promotionPool.filter((o) => !nearEqual(o.value, candidate));
  const officialAgree = agreeing.filter((o) => isOfficialCategory(o.source_category));
  const officialConflict = conflicting.filter((o) => isOfficialCategory(o.source_category));

  const officialValues = [
    ...new Set(
      independent
        .filter((o) => isOfficialCategory(o.source_category))
        .map((o) => Number(o.value))
        .filter((n) => Number.isFinite(n))
    ),
  ];
  const bestOfficial =
    officialValues.find((v) => nearEqual(v, candidate)) ??
    officialValues[0] ??
    null;

  let conflict_analysis = null;
  if (
    Number.isFinite(candidate) &&
    bestOfficial != null &&
    !nearEqual(candidate, bestOfficial)
  ) {
    conflict_analysis = classifyRoomConflict({
      tripadvisor_rooms: candidate,
      official_rooms: bestOfficial,
      tripadvisor_name: hotel.tripadvisor_name || null,
      dealality_name: hotel.name,
      evidence_quotes: independent.map((o) => o.quote || o.evidence_text).filter(Boolean),
      sister_collision: Boolean(opts.sisterCollision),
    });
  }

  const multiAssessment = assessMultiSourceIndependence(agreeing);

  let rooms_verification_status = ROOMS_VERIFICATION_STATUS.CANDIDATE_SINGLE_SOURCE;
  let rooms_confidence = 0.55;

  // PRIMARY: official hotel/brand explicitly states inventory matching TA (or we prefer official on conflict separately)
  if (officialAgree.length >= 1 && officialConflict.length === 0) {
    rooms_verification_status = ROOMS_VERIFICATION_STATUS.VERIFIED_PRIMARY_SOURCE;
    rooms_confidence = 0.93;
  } else if (officialConflict.length >= 1) {
    // Credible official disagrees with Tripadvisor candidate
    rooms_verification_status = ROOMS_VERIFICATION_STATUS.CONFLICT_REVIEW_REQUIRED;
    rooms_confidence = 0.45;
  } else if (multiAssessment.status_hint === "VERIFIED_MULTI_SOURCE" && multiAssessment.independent) {
    rooms_verification_status = ROOMS_VERIFICATION_STATUS.VERIFIED_MULTI_SOURCE;
    rooms_confidence = multiAssessment.independence_confidence || 0.88;
  } else if (multiAssessment.status_hint === "SOURCE_INDEPENDENCE_UNCERTAIN") {
    rooms_verification_status = ROOMS_VERIFICATION_STATUS.SOURCE_INDEPENDENCE_UNCERTAIN;
    rooms_confidence = multiAssessment.independence_confidence || 0.5;
  } else if (conflicting.length > 0 && agreeing.length === 0) {
    rooms_verification_status = ROOMS_VERIFICATION_STATUS.CONFLICT_REVIEW_REQUIRED;
    rooms_confidence = 0.4;
  } else if (agreeing.length === 0 && independent.length === 0) {
    rooms_verification_status = ROOMS_VERIFICATION_STATUS.CANDIDATE_SINGLE_SOURCE;
    rooms_confidence = 0.55;
  } else if (agreeing.length >= 1 && !multiAssessment.independent) {
    // Corroboration exists but not independence-safe
    rooms_verification_status =
      multiAssessment.status_hint === "SOURCE_INDEPENDENCE_UNCERTAIN"
        ? ROOMS_VERIFICATION_STATUS.SOURCE_INDEPENDENCE_UNCERTAIN
        : ROOMS_VERIFICATION_STATUS.CANDIDATE_SINGLE_SOURCE;
    rooms_confidence = multiAssessment.independence_confidence || 0.55;
  }

  // Prefer official value for audit display when conflict — never overwrite TA provenance
  const preferred_verified_rooms =
    rooms_verification_status === ROOMS_VERIFICATION_STATUS.VERIFIED_PRIMARY_SOURCE
      ? officialAgree[0]?.value ?? candidate
      : rooms_verification_status === ROOMS_VERIFICATION_STATUS.VERIFIED_MULTI_SOURCE
        ? candidate
        : rooms_verification_status === ROOMS_VERIFICATION_STATUS.CONFLICT_REVIEW_REQUIRED &&
            conflict_analysis?.prefer === "official" &&
            bestOfficial != null
          ? bestOfficial
          : null;

  return {
    version: TRIPADVISOR_VERIFY_VERSION,
    rooms_candidate: candidate,
    preferred_verified_rooms,
    rooms_verification_status,
    rooms_confidence,
    independent_observations: independent,
    agreeing,
    conflicting,
    multi_source_independence: multiAssessment,
    conflict_analysis,
    official_website_room_count_found,
    official_pdf_factsheet_found,
    secondary_source_verifications,
    cost_signals: {
      serpapi_searches,
      pages_fetched,
    },
    steps,
    elapsed_ms: Date.now() - t0,
  };
}

export { classifySourceUrl, nearEqual };
