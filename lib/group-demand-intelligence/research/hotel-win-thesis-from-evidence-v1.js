/**
 * Opportunity-specific hotel win / path thesis from research evidence.
 *
 * Replaces generic facility/corridor boilerplate as the sole commercial fit claim.
 * Does not invent room blocks, RFPs, or named buyers.
 * Scaling Companies must not inherit Association host/overflow / delegate-lodging copy.
 */
import { isScalingCompaniesSegment } from "../scaling-drawer-presentation-contract-v1.js";

export const HOTEL_WIN_THESIS_FROM_EVIDENCE_V1 =
  "gdi-hotel-win-thesis-from-evidence-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hotelLabel(hotel = {}, opportunity = {}, action = {}) {
  return (
    text(action.hotelName) ||
    text(hotel.displayName || hotel.name) ||
    text(opportunity.hotelDisplayName || opportunity.hotelName) ||
    "this hotel"
  );
}

function targetLabel(opportunity = {}, action = {}) {
  return (
    text(action.targetEntity) ||
    text(opportunity.eventName || opportunity.title || opportunity.organizationName) ||
    "this activity"
  );
}

/**
 * Infer lodging path class from bilingual evidence (EN/ES).
 */
export function classifyLodgingPathFromEvidence(blob = "") {
  const b = text(blob);
  const negatesHost =
    /sin\s+hotel\s+sede|no\s+hotel\s+sede|hotel\s+sede\s+(?:no\s+)?publicad|host\s+hotel\s+(?:not|un)confirm|venue\s+hotel\s+unconfirm|no\s+official\s+host/i.test(
      b,
    );
  if (/stay[- ]?to[- ]?play|housing bureau|hoteles oficiales|alojamiento oficial|official hotel block/i.test(b)) {
    return "OFFICIAL_HOUSING_OR_STAY_TO_PLAY";
  }
  if (/overflow|hoteles?\s+de\s+apoyo|alternate hotel|recommended hotel list/i.test(b)) {
    return "OVERFLOW_OR_RECOMMENDED";
  }
  if (
    !negatesHost &&
    /host hotel|sede oficial|hotel sede|headquarters hotel|venue hotel/i.test(b)
  ) {
    return "PRIMARY_HOST_OR_SEDE";
  }
  if (/room block|bloque de habitaciones|hospedaje|alojamiento|housing bureau|official lodging/i.test(b)) {
    return "LODGING_SIGNAL_UNSTRUCTURED";
  }
  // Bare "lodging"/"housing" in English sales prose is too weak alone — prefer programme class.
  if (/congreso|congress|symposium|jornada|foro|cumbre|summit|expo|tournament|conference/i.test(b)) {
    return "PROGRAMME_ONLY_NO_HOUSING_PROOF";
  }
  if (/\blodging\b|\bhousing\b/i.test(b)) {
    return "LODGING_SIGNAL_UNSTRUCTURED";
  }
  return "INSUFFICIENT_LODGING_SIGNAL";
}

/**
 * Extract short prior-host / competitor hotel mentions (heuristic, evidence-bound).
 */
export function extractCompetitorHotelMentions(
  blob = "",
  { limit = 3, excludeHotelName = "" } = {},
) {
  const b = text(blob);
  const exclude = text(excludeHotelName).toLowerCase();
  const excludeTokens = exclude
    ? exclude
        .replace(/grand\s+/g, "")
        .split(/\s+/)
        .filter((t) => t.length > 3)
    : [];
  const patterns = [
    /\b(?:Hilton|Marriott|Hyatt|Westin|Sheraton|InterContinental|Fiesta Americana|Live Aqua|Sofitel|Four Seasons|St\.?\s*Regis|JW Marriott|Crystal Gateway Marriott)[^\n.,;]{0,40}/gi,
    /\bHotel\s+[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ' -]{2,40}/g,
  ];
  const found = [];
  const seen = new Set();
  for (const re of patterns) {
    const m = b.match(re) || [];
    for (const raw of m) {
      const t = text(raw).replace(/\s+/g, " ");
      if (t.length < 8 || t.length > 60) continue;
      const key = t.toLowerCase();
      if (seen.has(key)) continue;
      // Never treat the subject hotel / its brand page as a "competitor".
      if (exclude && (key.includes(exclude) || exclude.includes(key.slice(0, 24)))) {
        continue;
      }
      if (
        excludeTokens.length >= 2 &&
        excludeTokens.filter((tok) => key.includes(tok)).length >= 2
      ) {
        continue;
      }
      if (/chapultepec/i.test(key) && /fiesta\s*americana/i.test(exclude)) {
        continue;
      }
      seen.add(key);
      found.push(t);
      if (found.length >= limit) return found;
    }
  }
  return found;
}

function marketCorridorNote(hotelName, blob) {
  const geoMx =
    /ciudad de m[eé]xico|mexico city|cdmx|polanco|reforma|chapultepec|ángel de la independencia/i.test(
      blob,
    );
  const geoUsDm =
    /bethesda|washington|arlington|dmv|national mall|crystal city|montgomery/i.test(blob);
  if (geoMx) {
    return `${hotelName} is in the Chapultepec / Reforma / Polanco corridor of Mexico City — geography is relevant only if this edition’s attendees or programme create lodging or meetings demand there`;
  }
  if (geoUsDm) {
    return `${hotelName} can be evaluated against DMV geography and Metro access for this activity — only if lodging path is still open`;
  }
  return null;
}

function programmeSectorHint(blob, opportunity = {}) {
  const segment = text(opportunity.researchSegment || opportunity.segment);
  if (/scaling/i.test(segment)) {
    return "corporate expansion / local growth signals";
  }
  // Prefer opportunity title/org over contaminated cross-source packs.
  const focus = `${opportunity.title || ""} ${opportunity.organizationName || ""} ${opportunity.eventName || ""}`;
  const scope = text(focus) || text(blob);
  if (/m[eé]dic|salud|hospital|clinical|AHIMA|endocrin|nutrici[oó]n|genetics/i.test(scope)) {
    return "healthcare / professional advocacy programme signals";
  }
  if (/legal|abogad|law |bar association|ANADE/i.test(scope)) {
    return "legal / professional association signals";
  }
  if (/robot|AMRob|COMRob|microscop|engineering|STEM/i.test(scope)) {
    return "technical / STEM association signals";
  }
  if (/cooperativ|capital|AMECAP|AMEXCAP|CANACO|c[aá]mara|finance|fintech/i.test(scope)) {
    return "cooperative / capital / business programme signals";
  }
  if (/soccer|tournament|sports|cup\b/i.test(scope)) return "sports / tournament housing signals";
  if (/scaling|opened (an )?office|hiring|expansion|Weber|Globant|Roland Berger/i.test(scope)) {
    return "corporate expansion / local growth signals";
  }
  return "published programme activity";
}

const STALE_CORRIDOR_RE =
  /sits in the Chapultepec\s*\/\s*Reforma\s*\/\s*Polanco corridor used by Mexico City medical and association programmes|used by Mexico City medical and association programmes|Programme signals support mid-size meeting \/ speaker \/ delegate lodging demand rather than generic city facilities alone/gi;

export function scrubStaleChapultepecCorridorTemplate(value) {
  return text(String(value || "").replace(STALE_CORRIDOR_RE, " ")).replace(/\s{2,}/g, " ");
}

/**
 * Build opportunity-specific win thesis parts from evidence text.
 */
export function buildHotelWinThesisFromEvidence({
  opportunity = {},
  hotel = {},
  action = {},
  evidenceText = "",
} = {}) {
  const hotelName = hotelLabel(hotel, opportunity, action);
  const target = targetLabel(opportunity, action);
  const blob = scrubStaleChapultepecCorridorTemplate(evidenceText).slice(0, 24000);
  const path = classifyLodgingPathFromEvidence(blob);
  const competitors = extractCompetitorHotelMentions(blob, {
    excludeHotelName: hotelName,
  });
  const geo = marketCorridorNote(hotelName, blob);
  const sector = programmeSectorHint(blob, opportunity);
  const lodging =
    /hospedaje|alojamiento|room block|bloque de habitaciones|housing bureau|official lodging|hoteles oficiales/i.test(
      blob,
    );
  const programme =
    /congreso|congress|symposium|jornada|foro|cumbre|summit|expo|tournament|conference/i.test(
      blob,
    );

  const parts = [];
  const limitations = [];
  const scaling =
    isScalingCompaniesSegment(opportunity) ||
    /corporate expansion \/ local growth/i.test(sector);

  if (scaling) {
    parts.push(
      `${sector} for ${target} support a hypothesis of visiting-staff lodging, training stays, and compact corporate meetings near the documented activity — not proof of a contracted room block or preferred-hotel agreement`,
    );
    limitations.push("Local expansion / presence ≠ confirmed guestroom demand");
    if (geo) {
      parts.push(
        geo.replace(
          /only if this edition’s attendees or programme create lodging or meetings demand there/i,
          "relevant if project teams, visiting leadership, or training create overnight stays near that geography",
        ).replace(
          /only if lodging path is still open/i,
          "relevant if corporate travelers overnight near that geography",
        ),
      );
    }
    if (competitors.length) {
      parts.push(
        `Other hotels mentioned in sources: ${competitors.join("; ")} — competitive context only`,
      );
    } else {
      limitations.push("Existing lodging pattern for this company not yet identified");
    }
    limitations.push(
      "Confirm active local operations and travel ownership before pitching surge demand",
    );
  } else if (path === "PRIMARY_HOST_OR_SEDE") {
    parts.push(
      `Evidence suggests a primary host / sede hotel path for ${target}; ${hotelName} should be assessed as host, preferred, or independent alternate — not assumed winner`,
    );
  } else if (path === "OVERFLOW_OR_RECOMMENDED") {
    parts.push(
      `Hotel path looks like overflow / recommended lodging rather than sole host for ${target}; ${hotelName} competes if primary inventory, rate, or location leaves a gap`,
    );
  } else if (path === "OFFICIAL_HOUSING_OR_STAY_TO_PLAY") {
    parts.push(
      `Official housing / stay-to-play structure is signaled for ${target}; ${hotelName} needs inclusion on the housing list or preferred rates — not a cold “host venue” pitch alone`,
    );
  } else if (path === "LODGING_SIGNAL_UNSTRUCTURED") {
    parts.push(
      `Public materials reference lodging/housing for ${target} without a confirmed exclusive host; ${hotelName} can open a sourcing conversation once organizer/PCO ownership is clear`,
    );
  } else if (programme) {
    parts.push(
      `${sector} for ${target} support a hypothesis of mid-size meetings, speakers, or delegate lodging — not proof of an open room block`,
    );
    limitations.push("Programme existence ≠ contracted hotel demand");
  } else {
    limitations.push("Insufficient lodging or programme evidence to assert property fit");
  }

  if (!scaling) {
    if (competitors.length) {
      parts.push(
        `Competing or prior hotel pattern mentioned in sources: ${competitors.join("; ")} — use as competitive context, not as proof ${hotelName} is selected`,
      );
    } else {
      limitations.push("Prior host / official hotel not yet identified in retrieved text");
    }

    if (geo) parts.push(geo);
    if (lodging && !parts.some((p) => /lodging|housing|hospedaje/i.test(p))) {
      parts.push(`Lodging/venue language appears in public materials tied to ${target}`);
    }

    limitations.push("Verify dates, city edition, and sourcing status before pitching contracted space");
  }

  const rationale =
    parts.length > 0
      ? `${parts.join(". ")}. ${limitations.map((l) => `Limit: ${l}`).join(" ")}`
      : `Opportunity-specific hotel fit for ${hotelName} vs ${target} is not yet evidenced — do not substitute generic facility lists for a win thesis.`;

  const isGenericOnly =
    parts.length <= 1 &&
    /corridor of Mexico City|DMV geography/i.test(rationale) &&
    !competitors.length &&
    path === "PROGRAMME_ONLY_NO_HOUSING_PROOF";

  return {
    version: HOTEL_WIN_THESIS_FROM_EVIDENCE_V1,
    lodgingPathClass: path,
    competitorMentions: competitors,
    rationale: text(rationale),
    limitations,
    commerciallySpecific: !isGenericOnly && parts.length > 0 && path !== "INSUFFICIENT_LODGING_SIGNAL",
    sectorHint: sector,
  };
}

/**
 * Apply thesis fields onto an opportunity (non-destructive to sources).
 */
export function applyHotelWinThesisToOpportunity(opportunity = {}, thesis = {}) {
  if (!thesis?.rationale) return opportunity;
  const next = { ...opportunity };
  next.hotelFitRationale = thesis.rationale;
  next.propertyFitNote = thesis.rationale;
  next.hotelOpportunityThesis = thesis.rationale;
  next.whyThisHotel = thesis.rationale;
  next.hotelWinThesis = thesis.rationale;
  next.chapultepecFit = thesis.rationale;
  next.lodgingPathClass = thesis.lodgingPathClass || null;
  next.hotelWinThesisMeta = {
    version: thesis.version,
    lodgingPathClass: thesis.lodgingPathClass,
    competitorMentions: thesis.competitorMentions || [],
    commerciallySpecific: thesis.commerciallySpecific === true,
    at: new Date().toISOString(),
  };
  // Scrub stale corridor templates so gold-brief assembly cannot re-append them.
  next.localConnection = scrubStaleChapultepecCorridorTemplate(next.localConnection) || null;
  if (/medical and association programmes|sits in the Chapultepec/i.test(String(next.localConnection || ""))) {
    next.localConnection = `Lodging path class ${thesis.lodgingPathClass}; see hotel win thesis`;
  }
  if (next.commercialIntelligenceBrief) {
    next.commercialIntelligenceBrief = {
      ...next.commercialIntelligenceBrief,
      whyThisHotel: thesis.rationale,
      hotelFitRationale: thesis.rationale,
    };
  }
  if (next.associationEventsGoldBrief) {
    next.associationEventsGoldBrief = {
      ...next.associationEventsGoldBrief,
      whyThisHotel: thesis.rationale,
    };
  }
  return next;
}
