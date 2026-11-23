/**
 * Config-driven leisure module selection from verified hotel characteristics.
 * Examples (LaLuna / Memmo / Condesa) are configuration — not live research auth.
 */

import {
  LEISURE_MODULE,
  LEISURE_MODULES_ORDERED,
  getLeisureModule,
} from "./gdi-leisure-module-registry-v1.js";

export const GDI_LEISURE_HOTEL_MODULE_SELECTION_V1 =
  "gdi-leisure-hotel-module-selection-v1";

/** Named profile presets — keyed by HPC id when known; else by classification tags. */
export const LEISURE_HOTEL_PROFILE_PRESETS = Object.freeze({
  // LaLuna Grenada — leisure resort (prioritize distribution / attractions / events)
  recaokE8psIbuVeuH: Object.freeze({
    label: "LaLuna Grenada",
    modules: Object.freeze([
      LEISURE_MODULE.DISTRIBUTION.id,
      LEISURE_MODULE.ATTRACTIONS.id,
      LEISURE_MODULE.EVENTS.id,
      LEISURE_MODULE.DESTINATION.id,
      LEISURE_MODULE.SPORTS.id,
    ]),
    rationale:
      "Prioritize leisure distribution, attractions/experiences, and events; then destination + selected sports",
  }),
  // Memmo Príncipe Real Lisbon (prioritize distribution / attractions / events)
  rec1t35e3sPerzxYu: Object.freeze({
    label: "Memmo Príncipe Real Lisbon",
    modules: Object.freeze([
      LEISURE_MODULE.DISTRIBUTION.id,
      LEISURE_MODULE.ATTRACTIONS.id,
      LEISURE_MODULE.EVENTS.id,
      LEISURE_MODULE.DESTINATION.id,
    ]),
    rationale:
      "Prioritize leisure distribution, attractions/experiences, and cultural events; then destination demand",
  }),
  // Condesa DF Mexico City — relevant urban leisure modules
  rece0NdV9FCnaFlVS: Object.freeze({
    label: "Condesa DF Mexico City",
    modules: Object.freeze([
      LEISURE_MODULE.EVENTS.id,
      LEISURE_MODULE.ATTRACTIONS.id,
      LEISURE_MODULE.DISTRIBUTION.id,
      LEISURE_MODULE.UNIVERSITIES.id,
    ]),
    rationale:
      "Relevant Condesa modules: cultural events, urban attractions, leisure partnerships, universities/VFR",
  }),
});

/**
 * @param {object} hotelProfile
 * @param {object} [opts]
 * @param {string[]} [opts.forceModules] — explicit module list (overrides auto)
 * @param {string[]} [opts.disableModules]
 */
export function selectLeisureModulesForHotel(hotelProfile = {}, opts = {}) {
  const hotelId = String(hotelProfile.hotelId || hotelProfile.id || "").trim();
  const disable = new Set(
    (opts.disableModules || []).map((m) => getLeisureModule(m)?.id).filter(Boolean),
  );

  if (Array.isArray(opts.forceModules) && opts.forceModules.length) {
    const forced = opts.forceModules
      .map((m) => getLeisureModule(m))
      .filter(Boolean)
      .map((m) => m.id)
      .filter((id) => !disable.has(id));
    return {
      hotelId: hotelId || null,
      source: "force_modules",
      moduleIds: forced,
      rationale: "Explicit module list from orchestrator / PowerShell",
    };
  }

  const preset = LEISURE_HOTEL_PROFILE_PRESETS[hotelId];
  if (preset) {
    return {
      hotelId,
      source: "hpc_preset",
      moduleIds: preset.modules.filter((id) => !disable.has(id)),
      rationale: preset.rationale,
      label: preset.label,
    };
  }

  const tags = collectTags(hotelProfile);
  const scored = LEISURE_MODULES_ORDERED.map((m) => ({
    id: m.id,
    score: scoreModule(m.id, tags),
  }))
    .filter((r) => r.score > 0 && !disable.has(r.id))
    .sort((a, b) => b.score - a.score);

  // Always allow at least destination+events for leisure-capable hotels; empty if no leisure signal
  let moduleIds = scored.map((r) => r.id);
  if (!moduleIds.length && tags.has("leisure_capable")) {
    moduleIds = [LEISURE_MODULE.EVENTS.id, LEISURE_MODULE.DESTINATION.id];
  }

  return {
    hotelId: hotelId || null,
    source: "characteristic_heuristic",
    moduleIds,
    rationale: `Derived from tags: ${[...tags].join(", ") || "none"}`,
    scores: scored,
  };
}

function collectTags(profile) {
  const tags = new Set();
  const blob = JSON.stringify(profile).toLowerCase();
  const classification = String(
    profile.classification || profile.hotelClass || profile.propertyType || "",
  ).toLowerCase();

  if (/leisure|resort|boutique|villa|beach|island/.test(blob + classification)) {
    tags.add("leisure_capable");
  }
  if (/resort|beach|island|villa|destination/.test(blob)) {
    tags.add("destination_resort");
    tags.add("distribution");
    tags.add("attractions");
    tags.add("sports");
  }
  if (/urban|city|boutique|downtown/.test(blob)) {
    tags.add("urban");
    tags.add("events");
    tags.add("attractions");
    tags.add("distribution");
  }
  if (/university|campus|student|academic/.test(blob)) {
    tags.add("universities");
  }
  if (/mexico\s*city|lisbon|grenad|cultural/.test(blob)) {
    tags.add("events");
  }
  if (profile.meetingRooms > 0 || /meeting/.test(blob)) {
    tags.add("events");
  }
  if (profile.commercialPriorities) {
    const p = JSON.stringify(profile.commercialPriorities).toLowerCase();
    if (/leisure|leisure.?distribution|dmc|attraction/.test(p)) {
      tags.add("distribution");
      tags.add("attractions");
      tags.add("leisure_capable");
    }
    if (/festival|concert|cultural/.test(p)) tags.add("events");
    if (/sport|tournament|regatta/.test(p)) tags.add("sports");
    if (/vfr|graduation|alumni|university/.test(p)) tags.add("universities");
    if (/shoulder|seasonality|occasion/.test(p)) tags.add("destination");
  }
  return tags;
}

function scoreModule(moduleId, tags) {
  const map = {
    [LEISURE_MODULE.EVENTS.id]: ["events", "urban", "leisure_capable"],
    [LEISURE_MODULE.SPORTS.id]: ["sports", "destination_resort"],
    [LEISURE_MODULE.ATTRACTIONS.id]: ["attractions", "destination_resort", "urban"],
    [LEISURE_MODULE.UNIVERSITIES.id]: ["universities", "urban"],
    [LEISURE_MODULE.DESTINATION.id]: [
      "destination",
      "destination_resort",
      "leisure_capable",
    ],
    [LEISURE_MODULE.DISTRIBUTION.id]: [
      "distribution",
      "destination_resort",
      "urban",
      "leisure_capable",
    ],
  };
  const keys = map[moduleId] || [];
  let score = 0;
  for (const k of keys) {
    if (tags.has(k)) score += 1;
  }
  return score;
}
