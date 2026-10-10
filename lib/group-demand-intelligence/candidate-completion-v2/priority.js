/**
 * Deterministic completion priority for V3 / discovery candidates.
 * P0 = high chance page-level research can close to useful.
 */

export const COMPLETION_PRIORITY = Object.freeze({
  P0_HIGH_COMPLETION_POTENTIAL: "P0_HIGH_COMPLETION_POTENTIAL",
  P1_MEDIUM_COMPLETION_POTENTIAL: "P1_MEDIUM_COMPLETION_POTENTIAL",
  P2_LOW_DEFER: "P2_LOW_DEFER",
});

const NOISE_RE =
  /\b(booking\.com|expedia|tripadvisor|trivago|indeed\.|jobs?\.|vol pas cher|jettours|airbnb|vrbo|wikipedia\.org\/wiki\/20\d{2}\b)\b/i;
const LODGING_HINT_RE =
  /\b(room block|host hotel|housing|accommodation|hébergement|alojamiento|hotel block|preferred hotel|overflow|lodging|prestations hôtelières|hébergement officiel)\b/i;
const PROCUREMENT_RE =
  /\b(rfp|tender|licitación|appel d['']offres|procurement|marché public|solicitation|marché de prestations)\b/i;
const HOUSING_URL_RE = /\/(accommodation|housing|hotels?|lodging|hébergement|alojamiento)/i;
const FUTURE_RE = /\b(202[6-9]|203[0-2]|upcoming|next year|annual|édition|edicion)\b/i;
const TRAVEL_RE =
  /\b(overnight|multi-?day|delegation|travelling|traveling|congress|conference|summit|kickoff|retreat|congrès)\b/i;
const CONTACT_RE = /\/(contact|about|team|secretariat|procurement)/i;
const DIRECTORY_RE =
  /\b(indeed|linkedin\.com\/posts|govoyages|jettours|tenderlift\.ch\/fr\/appels-offres\/[^/]+\/construction)\b/i;

function scoreCandidate(c = {}) {
  const title = String(c.title || "");
  const org = String(c.organizationName || c.company || "");
  const url = String(c.officialSource || c.discoverySource || c.url || "");
  // Do not score thesis boilerplate ("lodging demand") — only title/org/snippet/URL
  const blob = `${title} ${org} ${c.summaryWhat || ""} ${url}`;
  let score = 0;
  const signals = [];

  if (NOISE_RE.test(blob) || DIRECTORY_RE.test(blob)) {
    return { score: -10, signals: ["noise_directory_or_ota"], priority: COMPLETION_PRIORITY.P2_LOW_DEFER };
  }

  let lodgingSignal = false;
  if (c.lodgingEvidence?.housingPageFound || c.lodgingEvidence?.roomBlockMentioned) {
    score += 4;
    lodgingSignal = true;
    signals.push("lodging_evidence_stamped");
  } else if (LODGING_HINT_RE.test(blob) || HOUSING_URL_RE.test(url)) {
    score += 3;
    lodgingSignal = true;
    signals.push("lodging_hint");
  }

  if (HOUSING_URL_RE.test(url)) {
    score += 2;
    lodgingSignal = true;
    signals.push("housing_url");
  }

  const isProcurement =
    PROCUREMENT_RE.test(blob) ||
    /ProcurementScout/i.test(String(c.scoutFamily || c.discoveryMeta?.scoutFamily || ""));
  if (isProcurement) {
    score += 2;
    signals.push("procurement_rfp_signal");
  }

  // Named organizer: real org name, not title echo / URL host
  const orgOk =
    org &&
    org.length > 4 &&
    !/^https?/i.test(org) &&
    org.split(/\s+/).length >= 2 &&
    org.toLowerCase() !== title.toLowerCase().slice(0, org.length) &&
    !/^(appel|post de|emplois|vol pas|https|marché|64 appels)/i.test(org);
  if (orgOk) {
    score += 2;
    signals.push("named_organizer");
  }

  if (c.eventStartDate || FUTURE_RE.test(blob)) {
    score += 2;
    signals.push("future_timing_signal");
  }

  if (/annual|rotat|series|édition|congress|congrès|biennale|assembly/i.test(blob)) {
    score += 1;
    signals.push("repeat_series_signal");
  }

  if (c.venueStatus && !/^UNKNOWN$/i.test(String(c.venueStatus))) {
    score += 1;
    signals.push("known_venue");
  }

  const fit = c.hotelFitScore ?? c.hotelFit;
  if (fit != null && Number(fit) >= 50) {
    score += 1;
    signals.push("hotel_fit");
  }

  if (c.primaryContact || c.organizationContactUrl || CONTACT_RE.test(url)) {
    score += 2;
    signals.push("public_contact_route");
  }

  if (TRAVEL_RE.test(blob)) {
    score += 1;
    signals.push("overnight_travel_language");
  }

  // Priority bands — lodging-bearing candidates preferred for P0
  let priority = COMPLETION_PRIORITY.P2_LOW_DEFER;
  if (lodgingSignal && score >= 5) {
    priority = COMPLETION_PRIORITY.P0_HIGH_COMPLETION_POTENTIAL;
  } else if (lodgingSignal && score >= 3) {
    priority = COMPLETION_PRIORITY.P1_MEDIUM_COMPLETION_POTENTIAL;
  } else if (
    !lodgingSignal &&
    score >= 6 &&
    (isProcurement || signals.includes("repeat_series_signal") || signals.includes("future_timing_signal"))
  ) {
    // Strong non-lodging public entity/series: medium — one targeted lodging hunt
    priority = COMPLETION_PRIORITY.P1_MEDIUM_COMPLETION_POTENTIAL;
  } else if (score >= 5) {
    priority = COMPLETION_PRIORITY.P1_MEDIUM_COMPLETION_POTENTIAL;
  } else {
    priority = COMPLETION_PRIORITY.P2_LOW_DEFER;
  }

  // Cap pure noise-adjacent low scores
  if (score < 3) priority = COMPLETION_PRIORITY.P2_LOW_DEFER;

  return { score, signals, priority };
}

/**
 * @param {object[]} candidates
 * @returns {{ queue, tallies }}
 */
export function buildGdiCompletionPriority(candidates = []) {
  const queue = (candidates || []).map((c) => {
    const ranked = scoreCandidate(c);
    return {
      ...c,
      completionScore: ranked.score,
      completionSignals: ranked.signals,
      completionPriority: ranked.priority,
    };
  });

  queue.sort((a, b) => {
    const order = {
      [COMPLETION_PRIORITY.P0_HIGH_COMPLETION_POTENTIAL]: 0,
      [COMPLETION_PRIORITY.P1_MEDIUM_COMPLETION_POTENTIAL]: 1,
      [COMPLETION_PRIORITY.P2_LOW_DEFER]: 2,
    };
    const d = order[a.completionPriority] - order[b.completionPriority];
    if (d !== 0) return d;
    return (b.completionScore || 0) - (a.completionScore || 0);
  });

  const tallies = {
    P0: queue.filter((c) => c.completionPriority === COMPLETION_PRIORITY.P0_HIGH_COMPLETION_POTENTIAL).length,
    P1: queue.filter((c) => c.completionPriority === COMPLETION_PRIORITY.P1_MEDIUM_COMPLETION_POTENTIAL).length,
    P2: queue.filter((c) => c.completionPriority === COMPLETION_PRIORITY.P2_LOW_DEFER).length,
    total: queue.length,
  };

  return { queue, tallies };
}
