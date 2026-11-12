/**
 * Hotel-adaptive specialist hunter selection for weekly Full E2E supervisor.
 * Uses the hotel's own Commercial Priorities V3 / saved profile — never copies
 * another hotel's preferences. Does not run all ten modules by default.
 */

import {
  SPECIALIST_HUNTER,
  SPECIALIST_HUNTER_META,
  FIRST_WAVE_HUNTERS,
  SECOND_WAVE_HUNTERS,
  THIRD_WAVE_HUNTERS,
  HUNTER_WAVE,
} from "./specialist-hunter-contract-v1.js";

const SEGMENT_HINTS = Object.freeze({
  government: SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT,
  public_sector: SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT,
  procurement: SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT,
  association: SPECIALIST_HUNTER.ASSOCIATION_ROTATION,
  rotation: SPECIALIST_HUNTER.ASSOCIATION_ROTATION,
  corporate: SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS,
  meeting: SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS,
  training: SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS,
  medical: SPECIALIST_HUNTER.CLINICAL_TRIALS_MEDICAL,
  clinical: SPECIALIST_HUNTER.CLINICAL_TRIALS_MEDICAL,
  pharma: SPECIALIST_HUNTER.CLINICAL_TRIALS_MEDICAL,
  project: SPECIALIST_HUNTER.PROJECT_WORKFORCE,
  construction: SPECIALIST_HUNTER.PROJECT_WORKFORCE,
  infrastructure: SPECIALIST_HUNTER.PROJECT_WORKFORCE,
  relocation: SPECIALIST_HUNTER.PROJECT_WORKFORCE,
  sports: SPECIALIST_HUNTER.SPORTS_HOUSING,
  tournament: SPECIALIST_HUNTER.SPORTS_HOUSING,
  race: SPECIALIST_HUNTER.SPORTS_HOUSING,
  university: SPECIALIST_HUNTER.UNIVERSITY_DEMAND,
  education: SPECIALIST_HUNTER.UNIVERSITY_DEMAND,
  campus: SPECIALIST_HUNTER.UNIVERSITY_DEMAND,
  legal: SPECIALIST_HUNTER.LEGAL_REGULATORY,
  regulatory: SPECIALIST_HUNTER.LEGAL_REGULATORY,
  emergency: SPECIALIST_HUNTER.EMERGENCY_DISPLACEMENT,
  fema: SPECIALIST_HUNTER.EMERGENCY_DISPLACEMENT,
  displacement: SPECIALIST_HUNTER.EMERGENCY_DISPLACEMENT,
  crew: SPECIALIST_HUNTER.CREW_PRODUCTION,
  production: SPECIALIST_HUNTER.CREW_PRODUCTION,
  entertainment: SPECIALIST_HUNTER.CREW_PRODUCTION,
});

function text(v) {
  return String(v || "").toLowerCase();
}

function collectPriorityBlob(commercialPriorities = {}) {
  const cp = commercialPriorities || {};
  return [
    ...(cp.targetSegments || []),
    ...(cp.demandAnchorFocus || []),
    ...(cp.priorityChips || []),
    ...(cp.needPeriods || []).map((n) => n?.label || n?.segment || ""),
    cp.notes || "",
    cp.weekdayFocus ? "weekday corporate meeting training" : "",
    cp.weekendFocus ? "weekend sports university" : "",
    cp.guestroomPriority ? "guestroom lodging housing" : "",
    cp.cateringPriority ? "catering meeting" : "",
  ]
    .map(text)
    .join(" ");
}

/**
 * @param {object} opts
 * @param {string} opts.hotelId
 * @param {object|null} opts.commercialPriorities - THIS hotel's HCP V3 / saved profile only
 * @param {string[]} [opts.knownAccountSegments]
 * @param {object} [opts.priorHunterYield] - hunterId -> { qualifiedIncremental }
 * @param {string} [opts.wavePolicy] - FIRST | FIRST_SECOND | ALL_IMPLEMENTED
 * @param {number} [opts.maxHunters]
 */
export function selectSpecialistHuntersForHotel(opts = {}) {
  const hotelId = String(opts.hotelId || "").trim();
  const cp = opts.commercialPriorities || null;
  const wavePolicy = opts.wavePolicy || "FIRST";
  const maxHunters = Math.max(1, Number(opts.maxHunters) || 5);
  const blob = collectPriorityBlob(cp || {});

  const scored = new Map();

  function bump(hunterId, weight, reason) {
    if (!hunterId || !SPECIALIST_HUNTER_META[hunterId]) return;
    const prev = scored.get(hunterId) || { score: 0, reasons: [] };
    prev.score += weight;
    prev.reasons.push(reason);
    scored.set(hunterId, prev);
  }

  // Baseline: first-wave always eligible at low weight when hotel has any CP
  for (const id of FIRST_WAVE_HUNTERS) {
    bump(id, cp ? 1 : 0.5, "first_wave_default");
  }

  for (const [hint, hunterId] of Object.entries(SEGMENT_HINTS)) {
    if (blob.includes(hint)) {
      bump(hunterId, 3, `cp_hint:${hint}`);
    }
  }

  for (const seg of opts.knownAccountSegments || []) {
    const s = text(seg);
    for (const [hint, hunterId] of Object.entries(SEGMENT_HINTS)) {
      if (s.includes(hint)) bump(hunterId, 2, `known_account:${hint}`);
    }
  }

  const yieldMap = opts.priorHunterYield || {};
  for (const [hunterId, row] of Object.entries(yieldMap)) {
    const q = Number(row?.qualifiedIncremental) || 0;
    if (q > 0) bump(hunterId, Math.min(5, q), "prior_yield");
    if (row?.duplicateRate > 0.7) bump(hunterId, -2, "high_dup_rate");
  }

  let allowedWaves = new Set([HUNTER_WAVE.FIRST]);
  if (wavePolicy === "FIRST_SECOND") {
    allowedWaves = new Set([HUNTER_WAVE.FIRST, HUNTER_WAVE.SECOND]);
  } else if (wavePolicy === "ALL" || wavePolicy === "ALL_IMPLEMENTED") {
    allowedWaves = new Set([
      HUNTER_WAVE.FIRST,
      HUNTER_WAVE.SECOND,
      HUNTER_WAVE.THIRD,
    ]);
  }

  const ranked = [...scored.entries()]
    .map(([hunterId, v]) => ({
      hunterId,
      label: SPECIALIST_HUNTER_META[hunterId].label,
      wave: SPECIALIST_HUNTER_META[hunterId].wave,
      capability: SPECIALIST_HUNTER_META[hunterId].capability,
      score: v.score,
      reasons: v.reasons,
      implemented: FIRST_WAVE_HUNTERS.includes(hunterId),
    }))
    .filter((r) => allowedWaves.has(r.wave))
    .sort((a, b) => b.score - a.score || a.hunterId.localeCompare(b.hunterId));

  const selected = ranked.filter((r) => r.score > 0).slice(0, maxHunters);

  return {
    schema: "gdi-hotel-adaptive-hunter-selection-v1",
    hotelId: hotelId || null,
    wavePolicy,
    maxHunters,
    commercialPrioritiesSource: cp?.source || (cp ? "provided" : "none"),
    copiedFromOtherHotel: false,
    selectedHunterIds: selected.map((s) => s.hunterId),
    selected,
    deferred: ranked.filter((r) => !selected.find((s) => s.hunterId === r.hunterId)),
    secondWaveAvailable: SECOND_WAVE_HUNTERS,
    thirdWaveAvailable: THIRD_WAVE_HUNTERS,
    safeguard: {
      runAllTen: false,
      requiresHotelOwnProfile: true,
    },
  };
}
