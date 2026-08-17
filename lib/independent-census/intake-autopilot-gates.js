/**
 * Census Intake Autopilot gates — deterministic validation for Independent +
 * known-brand Hotel Property Census inserts.
 *
 * Decisions: auto_insert | auto_enrich_only | steward_hold | reject
 * Writes: never from this module (plan/controlled/apply elsewhere).
 * Dedupe SoT: Hotel Property Census only (legacy Hotel Census forbidden).
 */

import { websiteHost } from "./match-current-census.js";
import { ROUTE_BUCKETS } from "./brand-exclusion-audit.js";
import { isHostelOrHostalProperty } from "./intake-census-field-normalize.js";
import { isBrandHomepageOfficialUrl } from "./official-property-url-quality.js";

export { isBrandHomepageOfficialUrl } from "./official-property-url-quality.js";

export const INTAKE_AUTOPILOT_GATES_VERSION = "census-intake-autopilot-gates-v1";

export const INTAKE_DECISIONS = Object.freeze({
  AUTO_INSERT: "auto_insert",
  AUTO_ENRICH_ONLY: "auto_enrich_only",
  STEWARD_HOLD: "steward_hold",
  REJECT: "reject",
});

/** Hosts that must not be Official Property URL for High auto_insert. */
export const WEBSITE_DENYLIST_HOSTS = Object.freeze([
  "booking.com",
  "expedia.com",
  "hotels.com",
  "agoda.com",
  "tripadvisor.com",
  "tripadvisor.es",
  "airbnb.com",
  "vrbo.com",
  "kayak.com",
  "trivago.com",
  "hotelbeds.com",
  "instagram.com",
  "facebook.com",
  "fb.com",
  "m.facebook.com",
  "twitter.com",
  "x.com",
  "tiktok.com",
  "youtube.com",
  "google.com",
  "maps.google.com",
  "goo.gl",
  "bit.ly",
]);

/** Weak / non-property hosts — steward unless strong second source. */
export const WEBSITE_WEAK_HOSTS = Object.freeze([
  "book.direct",
  "synxis.com",
  "cloudbeds.com",
  "mews.com",
  "reservhotel.com",
]);

export const INTAKE_GATE_CONFIG = Object.freeze({
  independent_min_quality_high: 70,
  independent_min_quality_with_wikidata: 55,
  known_brand_min_quality: 55,
  require_http_website_for_auto_insert: true,
  city_unknown_allowed_for_known_brand_hr: true,
});

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase();
}

function hostOf(url) {
  return websiteHost(url) || "";
}

export function isDeniedWebsite(url) {
  const host = hostOf(url);
  if (!host) return false;
  return WEBSITE_DENYLIST_HOSTS.some(
    (d) => host === d || host.endsWith(`.${d}`)
  );
}

export function isWeakWebsite(url) {
  const host = hostOf(url);
  if (!host) return false;
  return WEBSITE_WEAK_HOSTS.some((d) => host === d || host.endsWith(`.${d}`));
}

function hasUsableWebsite(url) {
  const raw = String(url || "").trim();
  if (!raw) return false;
  if (isDeniedWebsite(raw)) return false;
  try {
    const withProto = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
    const u = new URL(withProto);
    return Boolean(u.hostname);
  } catch {
    return false;
  }
}

function cityOk(city) {
  const c = String(city || "").trim();
  if (!c) return false;
  if (/^unknown$/i.test(c)) return false;
  return true;
}

function countryOk(country) {
  const c = String(country || "").trim();
  return Boolean(c) && !/^unknown$/i.test(c) && c.length > 1;
}

/**
 * Evaluate one dual-lane intake payload (or candidate row).
 *
 * @param {object} row
 * @param {{
 *   lane?: string,
 *   intake_class?: string,
 *   hpc_recommended_action?: string,
 *   hpc_match_confidence?: string,
 *   route?: string,
 *   quality_score?: number|null,
 *   wikidata_match_confidence?: string,
 *   sanitized_payload_preview?: object,
 *   fields?: object,
 * }} [opts] — overrides when not nested on row
 */
export function evaluateIntakeAutopilotGate(row, opts = {}) {
  const fields =
    opts.fields ||
    row.sanitized_payload_preview ||
    row.fields ||
    row;
  const lane = opts.lane || row.lane || "";
  const intakeClass = opts.intake_class || row.intake_class || "";
  const hpcAction =
    opts.hpc_recommended_action ||
    row.hpc_recommended_action ||
    row.hpcRecommendedAction ||
    "";
  const hpcConf =
    opts.hpc_match_confidence ||
    row.hpc_match_confidence ||
    row.hpcMatchConfidence ||
    "";
  const route = opts.route || row.route || "";
    const quality =
      opts.quality_score ??
      row.quality_score ??
      row.qualityScore ??
      fields.qualityScore ??
      null;
    const wdConf = norm(
      opts.wikidata_match_confidence ||
        row.wikidata_match_confidence ||
        row.wikidataMatchConfidence ||
        ""
    );

  const name = String(fields["Property Name"] || fields.rawHotelName || "").trim();
  const country = String(fields.Country || fields.rawCountry || "").trim();
  const city = String(fields.City || fields.resolvedCity || fields.rawCity || "").trim();
  const website = String(
    fields["Official Property URL"] ||
      fields.resolvedWebsite ||
      fields.rawWebsite ||
      ""
  ).trim();
  const brand = String(fields["Current Brand"] || fields.matchedBrand || "").trim();
  const affiliation = String(fields["Affiliation Status"] || "").trim();

  /** @type {string[]} */
  const reasons = [];
  /** @type {string[]} */
  const checks_passed = [];

  // --- Hard rejects ---
  if (!name) {
    return decision(INTAKE_DECISIONS.REJECT, "Low", true, ["missing_property_name"], [], {
      lane,
      intakeClass,
    });
  }
  checks_passed.push("has_name");

  if (isHostelOrHostalProperty(name)) {
    return decision(
      INTAKE_DECISIONS.REJECT,
      "Low",
      true,
      ["hostel_or_hostal_out_of_scope"],
      checks_passed,
      { lane, intakeClass }
    );
  }
  checks_passed.push("not_hostel_or_hostal");

  if (!countryOk(country)) {
    return decision(INTAKE_DECISIONS.REJECT, "Low", true, ["missing_or_invalid_country"], checks_passed, {
      lane,
      intakeClass,
    });
  }
  checks_passed.push("has_country");

  if (isDeniedWebsite(website)) {
    return decision(
      INTAKE_DECISIONS.REJECT,
      "Hold",
      true,
      ["official_url_denylisted_ota_or_social"],
      checks_passed,
      { lane, intakeClass }
    );
  }

  // --- HPC dedupe (production SoT only) ---
  if (hpcAction === "likely_existing") {
    return decision(
      INTAKE_DECISIONS.AUTO_ENRICH_ONLY,
      "High",
      false,
      ["hpc_likely_existing_no_insert"],
      [...checks_passed, "hpc_dedupe"],
      { lane, intakeClass, hpc_record_hint: row.hpc_matched_record_id || row.hpcMatchedRecordId }
    );
  }
  if (hpcAction === "possible_duplicate_review") {
    return decision(
      INTAKE_DECISIONS.STEWARD_HOLD,
      "Hold",
      true,
      ["hpc_possible_duplicate_review"],
      checks_passed,
      { lane, intakeClass }
    );
  }
  if (hpcAction === "skip_missing_name") {
    return decision(INTAKE_DECISIONS.REJECT, "Low", true, ["hpc_skip_missing_name"], checks_passed, {
      lane,
      intakeClass,
    });
  }
  checks_passed.push("hpc_not_duplicate");

  // Steward brand-tag noise
  if (
    intakeClass === "steward_brand_tag_review" ||
    route === ROUTE_BUCKETS.POSSIBLE_BRANDED
  ) {
    return decision(
      INTAKE_DECISIONS.STEWARD_HOLD,
      "Hold",
      true,
      ["steward_unresolved_osm_brand_tag"],
      checks_passed,
      { lane, intakeClass }
    );
  }

  const isIndependent =
    lane === "independent_unaffiliated" ||
    affiliation === "Independent" ||
    intakeClass === "independent_l1_promote";

  const isKnownBrand =
    lane === "known_brand_census_intake" ||
    intakeClass === "known_chain_census_backlog_not_active_setup" ||
    intakeClass === "active_or_soft_brand_census_plus_autopilot";

  // Independent must not carry a resolved chain brand
  if (isIndependent && brand && !/^independent$/i.test(brand)) {
    return decision(
      INTAKE_DECISIONS.STEWARD_HOLD,
      "Hold",
      true,
      ["independent_lane_has_non_independent_brand"],
      checks_passed,
      { lane, intakeClass }
    );
  }

  const websiteOk = hasUsableWebsite(website);
  const weakSite = isWeakWebsite(website);
  const brandHomepage = isBrandHomepageOfficialUrl(website);
  const hasCity = cityOk(city);
  const wdStrong = wdConf === "high" || wdConf === "medium";
  const q = typeof quality === "number" ? quality : null;

  if (
    INTAKE_GATE_CONFIG.require_http_website_for_auto_insert &&
    (!websiteOk || brandHomepage)
  ) {
    return decision(
      INTAKE_DECISIONS.STEWARD_HOLD,
      "Medium",
      true,
      [
        brandHomepage
          ? "official_url_is_brand_homepage_not_property_page"
          : "missing_official_property_url",
      ],
      checks_passed,
      { lane, intakeClass }
    );
  }
  checks_passed.push("has_non_denylist_website");

  if (weakSite && !wdStrong && !(q != null && q >= 70)) {
    return decision(
      INTAKE_DECISIONS.STEWARD_HOLD,
      "Medium",
      true,
      ["weak_booking_engine_host_needs_corroboration"],
      checks_passed,
      { lane, intakeClass }
    );
  }
  if (!weakSite) checks_passed.push("website_not_weak_or_corroborated");

  // --- Independent High gate ---
  if (isIndependent) {
    if (!hasCity) {
      return decision(
        INTAKE_DECISIONS.STEWARD_HOLD,
        "Medium",
        true,
        ["independent_missing_city"],
        checks_passed,
        { lane, intakeClass }
      );
    }
    checks_passed.push("has_city");

    const qualityHigh =
      (q != null && q >= INTAKE_GATE_CONFIG.independent_min_quality_high) ||
      (wdStrong &&
        q != null &&
        q >= INTAKE_GATE_CONFIG.independent_min_quality_with_wikidata) ||
      (wdConf === "high");

    if (!qualityHigh) {
      return decision(
        INTAKE_DECISIONS.STEWARD_HOLD,
        "Medium",
        true,
        ["independent_quality_below_auto_insert"],
        checks_passed,
        { lane, intakeClass, quality: q, wikidata: wdConf }
      );
    }
    checks_passed.push("independent_quality_or_wikidata");

    return decision(
      INTAKE_DECISIONS.AUTO_INSERT,
      "High",
      false,
      reasons.length ? reasons : ["independent_gates_passed"],
      checks_passed,
      {
        lane,
        intakeClass,
        human_review_required: false,
        enrichment_priority: "High",
      }
    );
  }

  // --- Known brand gates ---
  if (isKnownBrand) {
    if (!brand || /^brand-unconfirmed$/i.test(brand)) {
      return decision(
        INTAKE_DECISIONS.STEWARD_HOLD,
        "Hold",
        true,
        ["known_brand_missing_resolved_brand"],
        checks_passed,
        { lane, intakeClass }
      );
    }
    checks_passed.push("has_resolved_brand");

    if (
      q != null &&
      q < INTAKE_GATE_CONFIG.known_brand_min_quality &&
      !wdStrong
    ) {
      return decision(
        INTAKE_DECISIONS.STEWARD_HOLD,
        "Medium",
        true,
        ["known_brand_quality_below_threshold"],
        checks_passed,
        { lane, intakeClass }
      );
    }
    checks_passed.push("known_brand_quality_ok");

    const backlog =
      intakeClass === "known_chain_census_backlog_not_active_setup";
    const activeSoft =
      intakeClass === "active_or_soft_brand_census_plus_autopilot";

    // City: Active/soft prefer city; backlog may insert with Unknown + HR
    if (!hasCity) {
      if (
        backlog &&
        INTAKE_GATE_CONFIG.city_unknown_allowed_for_known_brand_hr
      ) {
        checks_passed.push("city_unknown_allowed_with_hr");
        return decision(
          INTAKE_DECISIONS.AUTO_INSERT,
          "Medium",
          true,
          ["known_chain_backlog_insert_city_unknown_hr"],
          checks_passed,
          {
            lane,
            intakeClass,
            human_review_required: true,
            enrichment_priority: "High",
            identity_confidence: "Medium",
          }
        );
      }
      return decision(
        INTAKE_DECISIONS.STEWARD_HOLD,
        "Medium",
        true,
        ["known_brand_missing_city"],
        checks_passed,
        { lane, intakeClass }
      );
    }
    checks_passed.push("has_city");

    if (activeSoft) {
      return decision(
        INTAKE_DECISIONS.AUTO_INSERT,
        "High",
        false,
        ["active_or_soft_brand_gates_passed"],
        checks_passed,
        {
          lane,
          intakeClass,
          human_review_required: false,
          enrichment_priority: "High",
          queue_autopilot_enrichment: true,
        }
      );
    }

    // Known chain backlog with city+website
    return decision(
      INTAKE_DECISIONS.AUTO_INSERT,
      "High",
      true,
      ["known_chain_backlog_gates_passed_hr"],
      checks_passed,
      {
        lane,
        intakeClass,
        human_review_required: true,
        enrichment_priority: "High",
        queue_autopilot_enrichment: false,
        brand_setup_active: false,
      }
    );
  }

  return decision(
    INTAKE_DECISIONS.STEWARD_HOLD,
    "Hold",
    true,
    ["unclassified_intake_row"],
    checks_passed,
    { lane, intakeClass }
  );
}

function decision(decisionName, identityConfidence, humanReview, reasons, checks_passed, meta = {}) {
  const writable =
    decisionName === INTAKE_DECISIONS.AUTO_INSERT &&
    (identityConfidence === "High" ||
      (identityConfidence === "Medium" && meta.human_review_required === true));

  return {
    version: INTAKE_AUTOPILOT_GATES_VERSION,
    decision: decisionName,
    identity_confidence: identityConfidence,
    human_review_required:
      meta.human_review_required != null
        ? meta.human_review_required
        : humanReview,
    production_writable_insert: writable && decisionName === INTAKE_DECISIONS.AUTO_INSERT,
    reasons: [...reasons],
    checks_passed: [...checks_passed],
    lane: meta.lane || "",
    intake_class: meta.intakeClass || "",
    enrichment_priority: meta.enrichment_priority || null,
    queue_autopilot_enrichment: Boolean(meta.queue_autopilot_enrichment),
    meta,
  };
}

/**
 * Run gates over dual-lane plan payloads.
 * @param {{ independent_payloads?: object[], known_brand_payloads?: object[] }} dualLanePlan
 * @param {{ hpcBySourceId?: Map<string, object>, qualityBySourceId?: Map<string, number>, wikidataBySourceId?: Map<string, object> }} [ctx]
 */
export function planIntakeAutopilot(dualLanePlan, ctx = {}) {
  const hpcById = ctx.hpcBySourceId || new Map();
  const qualityById = ctx.qualityBySourceId || new Map();
  const wdById = ctx.wikidataBySourceId || new Map();

  const rows = [];
  const counts = {
    auto_insert: 0,
    auto_insert_human_review: 0,
    auto_insert_no_hr: 0,
    auto_enrich_only: 0,
    steward_hold: 0,
    reject: 0,
    production_writable_insert: 0,
  };
  const reasonCounts = new Map();

  const all = [
    ...(dualLanePlan.independent_payloads || []).map((r) => ({
      ...r,
      lane: r.lane || "independent_unaffiliated",
    })),
    ...(dualLanePlan.known_brand_payloads || []).map((r) => ({
      ...r,
      lane: r.lane || "known_brand_census_intake",
    })),
  ];

  for (const row of all) {
    const sid = String(row.source_record_id || row.sourceRecordId || "");
    const hpc = hpcById.get(sid);
    const wd = wdById.get(sid);
    const gate = evaluateIntakeAutopilotGate(row, {
      hpc_recommended_action:
        row.hpc_recommended_action || hpc?.recommendedAction || "",
      hpc_match_confidence: row.hpc_match_confidence || hpc?.matchConfidence || "",
      quality_score:
        qualityById.has(sid) ? qualityById.get(sid) : row.quality_score ?? null,
      wikidata_match_confidence:
        row.wikidata_match_confidence || wd?.matchConfidence || "",
    });

    counts[gate.decision] = (counts[gate.decision] || 0) + 1;
    if (gate.decision === INTAKE_DECISIONS.AUTO_INSERT) {
      if (gate.human_review_required) counts.auto_insert_human_review += 1;
      else counts.auto_insert_no_hr += 1;
    }
    if (gate.production_writable_insert) counts.production_writable_insert += 1;

    for (const reason of gate.reasons) {
      reasonCounts.set(reason, (reasonCounts.get(reason) || 0) + 1);
    }

    rows.push({
      source_record_id: sid,
      property_name: row.sanitized_payload_preview?.["Property Name"] || "",
      current_brand: row.sanitized_payload_preview?.["Current Brand"] || "",
      city: row.sanitized_payload_preview?.City || "",
      lane: gate.lane,
      intake_class: gate.intake_class,
      quality_score: qualityById.has(sid)
        ? qualityById.get(sid)
        : row.quality_score ?? row.qualityScore ?? null,
      wikidata_match_confidence:
        row.wikidata_match_confidence || wd?.matchConfidence || "",
      hpc_recommended_action:
        row.hpc_recommended_action || hpc?.recommendedAction || "",
      ...gate,
      payload: row.sanitized_payload_preview || null,
    });
  }

  const topReasons = [...reasonCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 30)
    .map(([reason, count]) => ({ reason, count }));

  return {
    version: INTAKE_AUTOPILOT_GATES_VERSION,
    generated_at: new Date().toISOString(),
    airtable_writes: false,
    legacy_hotel_census_used: false,
    dedupe_source_of_truth: "Hotel Property Census",
    input_count: all.length,
    counts,
    top_reasons: topReasons,
    auto_insert_sample: rows
      .filter((r) => r.decision === INTAKE_DECISIONS.AUTO_INSERT)
      .slice(0, 40),
    steward_sample: rows
      .filter((r) => r.decision === INTAKE_DECISIONS.STEWARD_HOLD)
      .slice(0, 40),
    reject_sample: rows
      .filter((r) => r.decision === INTAKE_DECISIONS.REJECT)
      .slice(0, 20),
    enrich_only_sample: rows
      .filter((r) => r.decision === INTAKE_DECISIONS.AUTO_ENRICH_ONLY)
      .slice(0, 20),
    rows,
  };
}
