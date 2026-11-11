/**
 * ADP_INDEPENDENT_UNPROMPTED_CORE_UNIVERSE_V1
 *
 * Builds a Core scenario universe of exactly TARGET scenarios that all pass
 * ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1. Contaminated / assisted
 * prompts from the default pack are replaced (not dropped) so coverage stays full.
 */

import { buildScenarioUniverse } from "./scenario-registry.js";
import {
  assessIndependentDiscoveryEligibility,
  ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1,
} from "../certification/adp-unprompted-independent-discovery-standard-v1.js";

export const ADP_INDEPENDENT_UNPROMPTED_CORE_UNIVERSE_V1 =
  "ADP_INDEPENDENT_UNPROMPTED_CORE_UNIVERSE_V1";
export const INDEPENDENT_CORE_TARGET = 63;

function place(profile) {
  const city = String(profile?.city || profile?.market || "").trim() || "the destination";
  const market = String(profile?.market || city).trim();
  const sub = String(profile?.submarket || "").trim();
  return { city, market, sub, placeLabel: sub ? `${sub}, ${city}` : city };
}

/** Demand-bank replacements — never name subject hotel or brand. */
export function buildIndependentReplacementBank(profile) {
  const { city, market, sub, placeLabel } = place(profile);
  const pid = String(profile?.propertyId || "property");
  const attrs = new Set((profile?.attributes || []).map(String));
  const isAi = attrs.has("all_inclusive") || /all.?inclusive/i.test(profile?.propertyType || "");
  const isConvention =
    attrs.has("near_convention_center") ||
    /expo|convention/i.test(sub) ||
    /expo|convention/i.test(market);
  const isBeach =
    attrs.has("beachfront") ||
    attrs.has("beach_access") ||
    attrs.has("resort") ||
    /beach|hotel zone|zona hotelera|uvero/i.test(`${sub} ${market}`);

  const bank = [];
  const push = (intent, frame, query) => {
    bank.push({
      scenarioId: null, // assigned later
      intent,
      frame,
      query,
      source: "independent_replacement",
      propertyId: pid,
      layer: "independent_replacement",
    });
  };

  // Shared destination demand
  push("business", "best_for", `Best hotels in ${city} for a midweek corporate business trip`);
  push("business", "recommend", `Recommend hotels in ${placeLabel} for international business travelers`);
  push("leisure", "best_for", `Best hotels in ${city} for a weekend leisure break`);
  push("leisure", "alternatives", `What are good hotel alternatives in ${placeLabel} for a short leisure stay`);
  push("family", "best_for", `Best family-friendly hotels in ${city} for a multi-night stay`);
  push("couples", "best_for", `Best hotels in ${placeLabel} for a couples getaway`);
  // Value is a decision frame, not a TRAVELER_INTENTS demand segment — keep leisure intent.
  push("leisure", "compare", `Compare good-value upscale hotels in ${city} for a 3-night stay`);
  // Universal taxonomy: Group + Meetings collapse to TRAVELER_INTENTS.GROUP_MEETING ("group_meeting").
  // Do not invent parallel "group" / "meetings" customer categories.
  push("group_meeting", "best_for", `Best hotels in ${city} for a small group leisure trip`);
  push("business", "where_to_stay", `Where should I stay in ${placeLabel} for client meetings and evening work`);
  push("leisure", "where_to_stay", `Where to stay in ${city} for a first-time leisure visit`);

  if (isConvention) {
    // CRITICAL: use destination city/place — never hardcode another market (e.g. Guadalajara Expo).
    // Cross-market convention templates contaminate Core competitive extraction and rankings.
    push(
      "group_meeting",
      "best_for",
      `Best hotels near the main convention center in ${city} for attendees who need meeting space`
    );
    push(
      "business",
      "recommend",
      `Recommend hotels in ${city} for exhibitors attending a trade show at the convention center`
    );
    push(
      "group_meeting",
      "best_for",
      `Best ${city} hotels for a corporate group attending a major convention or exhibition`
    );
    push(
      "group_meeting",
      "where_to_stay",
      `Where to stay in ${placeLabel} for a multi-day industry exhibition with evening networking`
    );
    push(
      "business",
      "compare",
      `Compare full-service hotels in ${city} convenient to the convention district for international delegates`
    );
    push(
      "group_meeting",
      "best_for",
      `Best hotels in ${city} for hosting a 50–100 person offsite near the convention district`
    );
    push(
      "business",
      "alternatives",
      `Hotel options in ${placeLabel} for business travelers visiting a convention and nearby business districts`
    );
  }

  if (isBeach && !isAi) {
    push("leisure", "best_for", `Best hotels in ${placeLabel} for a beach vacation`);
    push("couples", "recommend", `Recommend hotels in ${city} Hotel Zone for a couples beach holiday`);
    push("family", "best_for", `Best family hotels near the beach in ${city}`);
    push("leisure", "compare", `Compare lagoon-view and beach-access hotels in ${placeLabel}`);
    push("leisure", "best_for", `Best value hotels in ${city} Hotel Zone for a 4-night leisure stay`);
    push("leisure", "alternatives", `Alternatives to large resort brands in ${city} for a stylish leisure stay`);
    push("couples", "where_to_stay", `Where to stay in ${placeLabel} for sunset views and easy beach access`);
    push("leisure", "best_for", `Best hotels in ${city} for a short international leisure break`);
  }

  if (isAi) {
    push(
      "leisure",
      "best_for",
      `Best all-inclusive resorts in ${city} for a week-long family vacation`
    );
    push(
      "family",
      "recommend",
      `Recommend all-inclusive resorts in ${placeLabel} with kids clubs and water activities`
    );
    push(
      "couples",
      "best_for",
      `Best all-inclusive resorts in ${city} for a couples celebration trip`
    );
    push(
      "group",
      "best_for",
      `Best all-inclusive resorts in ${city} for a multi-family group leisure trip`
    );
    push(
      "leisure",
      "compare",
      `Compare large all-inclusive beach resorts in ${placeLabel} on dining variety and activities`
    );
    push(
      "value",
      "best_for",
      `Best-value all-inclusive resorts in ${city} for international travelers`
    );
    push(
      "leisure",
      "where_to_stay",
      `Where to stay all-inclusive in ${placeLabel} for a celebration with friends`
    );
    push(
      "family",
      "alternatives",
      `All-inclusive resort alternatives in ${city} for families wanting a water park and beach`
    );
  }

  // Pad with additional destination variants
  const pads = [
    `Hotels in ${city} good for extended business stays with reliable Wi-Fi and a desk`,
    `Best hotels in ${placeLabel} for travelers who want on-site dining and a pool`,
    `Recommend hotels in ${city} suitable for mixed business and leisure travel`,
    `Where should a solo international traveler stay in ${placeLabel}`,
    `Best hotels in ${city} for a quiet adult leisure stay`,
    `Compare upscale hotels in ${placeLabel} for weekend leisure`,
    `Hotels in ${city} convenient for airport arrivals and a two-night stay`,
    `Best hotels in ${placeLabel} for travelers prioritizing spa and wellness`,
    `Recommend hotels in ${city} for a social weekend with friends`,
    `Where to stay in ${city} when you want walkable dining and nightlife nearby`,
    `Best hotels in ${placeLabel} for a workation with morning meetings and afternoon leisure`,
    `Hotels in ${city} that work well for a small celebration dinner and overnight guests`,
  ];
  for (const q of pads) {
    push("leisure", "best_for", q);
  }
  return bank;
}

function promptOf(s) {
  return String(s?.query || s?.prompt || "").trim();
}

/**
 * @returns {{ ok: boolean, scenarios: object[], audit: object }}
 */
export function buildIndependentUnpromptedCoreUniverse(profile, opts = {}) {
  const target = Number(opts.target || INDEPENDENT_CORE_TARGET);
  const base = buildScenarioUniverse(profile);
  const keptFromBase = [];
  const replaced = [];
  const failed = [];

  for (const s of base) {
    const a = assessIndependentDiscoveryEligibility(promptOf(s), profile);
    if (a.independentlyEligible) {
      keptFromBase.push({
        ...s,
        independentUnpromptedEligible: true,
        unpromptedStandard: ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1,
      });
    } else {
      failed.push({
        scenarioId: s.scenarioId || s.id,
        exclusions: a.exclusions,
        eligibilityClass: a.eligibilityClass,
        contaminationHits: a.contamination?.hits || [],
        query: promptOf(s).slice(0, 160),
      });
    }
  }

  const kept = [...keptFromBase];
  const bank = buildIndependentReplacementBank(profile);
  let bankIdx = 0;
  let replSeq = 1;
  const usedQueries = new Set(kept.map((s) => promptOf(s).toLowerCase()));

  while (kept.length < target && bankIdx < bank.length * 4) {
    const candidate = { ...bank[bankIdx % bank.length] };
    bankIdx += 1;
    if (bankIdx > bank.length) {
      candidate.query = `${candidate.query} for a ${2 + (bankIdx % 5)}-night stay`;
    }
    const q = promptOf(candidate);
    if (!q || usedQueries.has(q.toLowerCase())) continue;
    const a = assessIndependentDiscoveryEligibility(q, profile);
    if (!a.independentlyEligible) continue;
    const scenarioId = `indep_repl_${String(profile.propertyId || "p")
      .replace(/^adp_/, "")
      .slice(0, 28)}_${String(replSeq).padStart(2, "0")}`;
    replSeq += 1;
    usedQueries.add(q.toLowerCase());
    const row = {
      ...candidate,
      scenarioId,
      query: q,
      prompt: q,
      independentUnpromptedEligible: true,
      unpromptedStandard: ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1,
      replaces: failed[replaced.length]?.scenarioId || null,
    };
    kept.push(row);
    replaced.push({
      newScenarioId: scenarioId,
      replacedScenarioId: row.replaces,
      query: q.slice(0, 160),
    });
  }

  const final = kept.slice(0, target);
  const reAuditFail = [];
  for (const s of final) {
    const a = assessIndependentDiscoveryEligibility(promptOf(s), profile);
    if (!a.independentlyEligible) {
      reAuditFail.push({ scenarioId: s.scenarioId, exclusions: a.exclusions });
    }
  }

  return {
    ok: final.length === target && reAuditFail.length === 0,
    contract: ADP_INDEPENDENT_UNPROMPTED_CORE_UNIVERSE_V1,
    standard: ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1,
    propertyId: profile?.propertyId || null,
    target,
    scenarios: final,
    audit: {
      baseTotal: base.length,
      keptFromBase: keptFromBase.length,
      failedFromBase: failed.length,
      replacementsAdded: replaced.length,
      finalCount: final.length,
      reAuditFailures: reAuditFail,
      failedSample: failed.slice(0, 25),
      replacements: replaced,
    },
  };
}
