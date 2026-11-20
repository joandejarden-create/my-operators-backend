/**
 * Hotel Explorer executive commercial description rubric (reusable).
 * Owner / operator / asset-manager / investor voice — never invents performance.
 *
 * Version: hotel-commercial-description-v2
 */

export const HOTEL_COMMERCIAL_DESCRIPTION_VERSION = "hotel-commercial-description-v2";

const UNKNOWN_RE = /unknown/i;
const MECHANICAL_PHRASES = [
  /the physical product comprises/i,
  /on the physical-product side/i,
  /it sits in/i,
  /day-to-day operations are managed by/i,
  /relevant demand context includes/i,
  /ownership intelligence identifies/i,
];

function clean(v) {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s || UNKNOWN_RE.test(s)) return null;
  return s;
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function fmt(n) {
  return Math.round(n).toLocaleString("en-US");
}

function articleFor(phrase) {
  return /^[aeiou]/i.test(String(phrase || "").trim()) ? "an" : "a";
}

function amenitySummary(hotel) {
  const list = [];
  const raw = hotel.amenitiesDisplay || hotel.amenities || [];
  if (Array.isArray(raw)) {
    raw.forEach((a) => {
      const label = clean(typeof a === "string" ? a : a && (a.label || a.name || a.id));
      if (label && !/unknown|token|_/i.test(label)) list.push(label);
    });
  } else if (typeof raw === "string" && raw.trim()) {
    raw.split(/[;,|]/).forEach((p) => {
      const label = clean(p);
      if (label) list.push(label);
    });
  }
  return [...new Set(list)].slice(0, 6);
}

function demandGenerators(hotel) {
  const list = [];
  const raw =
    hotel.demandGenerators ||
    hotel.nearbyDemand ||
    (hotel.overviewEnrichment && hotel.overviewEnrichment.demandGenerators) ||
    [];
  if (Array.isArray(raw)) {
    raw.forEach((d) => {
      const name = clean(typeof d === "string" ? d : d && (d.name || d.label));
      if (name) list.push(name);
    });
  }
  [
    hotel.primaryDemandDriver,
    hotel.airportName,
    hotel.conventionCenterName,
  ]
    .map(clean)
    .filter(Boolean)
    .forEach((s) => {
      if (!list.includes(s)) list.push(s);
    });
  return list.slice(0, 5);
}

function yearPhrase(hotel) {
  const built = clean(hotel.yearBuilt || hotel.year_built);
  const reno = clean(hotel.yearRenovated || hotel.lastRenovated || hotel.last_renovated);
  if (built && reno) return `Originally developed in ${built} and last renovated in ${reno}`;
  if (built) return `Originally developed in ${built}`;
  if (reno) return `Last renovated in ${reno}`;
  return null;
}

/**
 * Detect thin / mechanical / keyword-stuffed descriptions that must not ship.
 */
export function isLowQualityCommercialDescription(text, hotel = {}) {
  const s = String(text || "").trim();
  if (!s) return true;
  if (/hotel\s+hotel/i.test(s)) return true;
  if (/signals\s*:/i.test(s)) return true;
  if (/_[a-z0-9]+_/i.test(s)) return true;
  if (/\brec[A-Za-z0-9]{10,}\b/.test(s)) return true;
  if (/\bdle_[A-Z0-9]+\b/i.test(s)) return true;
  if (/\b\d+\s+keys\b/i.test(s) && s.length < 120) return true;
  if (/^(OPEN|PIPELINE|CLOSED)\b/i.test(s) && s.length < 80) return true;
  const commas = (s.match(/,/g) || []).length;
  if (commas >= 6 && s.split(".").length <= 2) return true;
  if (/serving\s+.+\s+—\s+signals/i.test(s)) return true;
  for (const re of MECHANICAL_PHRASES) {
    if (re.test(s)) return true;
  }
  // Too short for an executive overview when the hotel has rich facts
  const rich =
    num(hotel.rooms) ||
    num(hotel.totalMeetingSpace) ||
    clean(hotel.brand) ||
    clean(hotel.managementCompany);
  if (rich && s.length < 420) return true;
  const paras = s.split(/\n\n+/).filter((p) => p.trim().length > 40);
  if (rich && paras.length < 2 && s.split(/\.\s+/).length < 5) return true;
  return false;
}

/**
 * Build multi-paragraph executive asset summary from verified / present fields only.
 * Returns text with paragraphs separated by blank lines.
 */
export function buildCommercialDescription(hotel = {}, opts = {}) {
  const name = clean(hotel.name) || "This hotel";
  const brand = clean(hotel.brand);
  const scaleRaw = clean(hotel.chainScale) || clean(hotel.propertyType);
  const scale = scaleRaw ? scaleRaw.replace(/\s+Chain$/i, "") : null;
  const scaleLc = scale ? scale.toLowerCase() : null;
  const city = clean(hotel.city);
  const state = clean(hotel.state);
  const country = clean(hotel.country);
  const market = clean(hotel.market);
  const submarket = clean(hotel.submarket) || clean(hotel.effectiveSubmarket);
  const locationType = clean(hotel.locationType);
  const rooms = num(hotel.rooms);
  const meetingSq = num(hotel.totalMeetingSpace);
  const meetingRooms = num(hotel.meetingRooms || hotel.meetingRoomCount);
  const largestName =
    clean(hotel.largestMeetingRoomName) ||
    clean(
      hotel.meetingSpaceProvenance &&
        hotel.meetingSpaceProvenance.largestRoom &&
        hotel.meetingSpaceProvenance.largestRoom.name
    );
  const largestSq =
    num(hotel.largestMeetingRoomSqFt) ||
    num(
      hotel.meetingSpaceProvenance &&
        hotel.meetingSpaceProvenance.largestRoom &&
        hotel.meetingSpaceProvenance.largestRoom.sqFt
    );
  const operator = clean(hotel.managementCompany) || clean(hotel.operator);
  const opType = clean(hotel.operationType);
  const ownership = opts.ownership || hotel.ownershipContext || null;
  const economicOwner =
    clean(ownership && (ownership.economicOwnerName || ownership.economic_owner)) ||
    clean(hotel.economicOwner);
  const legalOwner =
    clean(ownership && (ownership.legalOwnerName || ownership.legal_owner)) ||
    clean(hotel.legalOwner);
  const ownershipConfidence = clean(
    (ownership && (ownership.confidence || ownership.preview_state)) || hotel.ownershipConfidence
  );
  const ownershipProposed =
    /proposed|staged/i.test(String(ownershipConfidence || "")) ||
    (ownership && ownership.preview_state && /PROPOSED|STAGED/i.test(String(ownership.preview_state)));
  const ownershipUsable =
    ownershipProposed ||
    (ownership &&
      (ownership.verified === true ||
        ownership.confidence === "HIGH" ||
        ownership.confidence === "MEDIUM" ||
        ownership.preview_state === "LIVE"));
  const demand = demandGenerators(hotel);
  const amenities = amenitySummary(hotel);
  const years = yearPhrase(hotel);
  const paragraphs = [];

  // 1) Asset positioning
  {
    let p = `${name} is`;
    if (scaleLc) p += ` ${articleFor(scaleLc)} ${scaleLc}`;
    else p += " a";
    p += " lodging asset";
    if (brand) p += ` affiliated with ${brand}`;
    if (city) {
      p += ` in ${city}`;
      const geo = [state, country]
        .filter((x) => x && String(x).toLowerCase() !== String(city).toLowerCase())
        .join(", ");
      if (geo) p += `, ${geo}`;
    }
    p += ".";
    if (rooms || brand || scale) {
      p += " From an owner and asset-management perspective, the hotel is positioned as";
      if (scaleLc) p += ` ${articleFor(scaleLc)} ${scaleLc}`;
      else p += " a";
      p += " product";
      if (rooms) p += ` with a ${fmt(rooms)}-key inventory`;
      if (brand) p += ` under the ${brand} flag`;
      p += ", oriented to a mix of transient and group demand rather than a narrowly specialized format.";
    }
    paragraphs.push(p);
  }

  // 2) Location and market
  {
    const bits = [];
    if (submarket) bits.push(`the ${submarket} corridor`);
    else if (market) bits.push(`the ${market} market`);
    if (locationType) bits.push(`${locationType.toLowerCase()} setting`);
    if (bits.length || demand.length) {
      let p = "";
      if (bits.length) {
        p = `Commercially, the hotel anchors ${bits.join(" within a ")}.`;
      } else {
        p = "Location context is still being assembled for this asset.";
      }
      if (demand.length) {
        p +=
          " Documented demand environments that are relevant to commercial diligence include " +
          (demand.length === 1
            ? demand[0]
            : `${demand.slice(0, -1).join(", ")}, and ${demand[demand.length - 1]}`) +
          ". These are contextual drivers, not proof of captured market share or realized occupancy.";
      }
      paragraphs.push(p);
    }
  }

  // 3) Physical product
  {
    const parts = [];
    if (rooms) parts.push(`${fmt(rooms)} guestrooms`);
    if (meetingSq) {
      let m = `${fmt(meetingSq)} sq ft of meeting and event space`;
      if (meetingRooms) m += ` across ${fmt(meetingRooms)} published rooms`;
      if (largestName && largestSq) m += `, anchored by ${largestName} (${fmt(largestSq)} sq ft)`;
      else if (largestName) m += `, with ${largestName} as the largest published room`;
      parts.push(m);
    }
    if (amenities.length) {
      parts.push(
        "guest-facing amenities such as " +
          (amenities.length === 1
            ? amenities[0]
            : `${amenities.slice(0, -1).join(", ")}, and ${amenities[amenities.length - 1]}`)
      );
    }
    if (parts.length) {
      let p =
        "Published inventory includes " +
        (parts.length === 1
          ? parts[0]
          : parts.length === 2
            ? `${parts[0]} and ${parts[1]}`
            : `${parts.slice(0, -1).join("; ")}; and ${parts[parts.length - 1]}`) +
        ".";
      p +=
        " Together these attributes support both individual transient stays and structured group / meeting business, subject to competitive set and sales execution that are outside this summary.";
      paragraphs.push(p);
    }
  }

  // 4) Ownership and operation
  {
    const bits = [];
    if (operator) {
      let op = `${operator} provides hotel management`;
      if (opType) op += ` under a ${opType.toLowerCase()}`;
      if (brand) op += `, with brand standards and distribution tied to ${brand}`;
      bits.push(op);
    } else if (brand) {
      bits.push(`Brand affiliation is ${brand}`);
    }
    if (ownershipUsable && (economicOwner || legalOwner)) {
      if (ownershipProposed) {
        if (legalOwner && economicOwner && legalOwner !== economicOwner) {
          bits.push(
            `staged ownership research proposes ${legalOwner} as legal property owner and ${economicOwner} as economic sponsor (not yet Airtable-persisted; ultimate beneficial owner unresolved)`
          );
        } else if (economicOwner) {
          bits.push(
            `staged ownership research proposes ${economicOwner} as economic sponsor (not yet Airtable-persisted; ultimate beneficial owner unresolved)`
          );
        } else if (legalOwner) {
          bits.push(
            `staged ownership research proposes ${legalOwner} as legal property owner (not yet Airtable-persisted)`
          );
        }
      } else if (legalOwner && economicOwner && legalOwner !== economicOwner) {
        bits.push(
          `verified ownership intelligence associates legal title with ${legalOwner} and economic sponsorship with ${economicOwner}`
        );
      } else if (economicOwner) {
        bits.push(`verified ownership intelligence associates economic sponsorship with ${economicOwner}`);
      } else if (legalOwner) {
        bits.push(`verified ownership intelligence associates legal title with ${legalOwner}`);
      }
    }
    if (bits.length) {
      paragraphs.push(
        "From an ownership and operating standpoint, " +
          bits.join(". ") +
          ". Proposed or medium-confidence relationships are shown as research posture, not as confirmed closing-table facts."
      );
    }
  }

  // 5) Asset considerations + commercial interpretation
  {
    let p = "";
    if (years) {
      p += `${years}. Construction and renovation history should be validated against capital-plan evidence before underwriting assumptions. `;
    } else {
      p +=
        "Year built and last renovation are not yet independently verified in Dealality for this asset; capital-plan timing should not be inferred from branding alone. ";
    }
    p +=
      "Commercially, the verified product attributes — ";
    const attrs = [];
    if (scaleLc) attrs.push(scaleLc + " positioning");
    if (rooms) attrs.push(`${fmt(rooms)}-key scale`);
    if (meetingSq) attrs.push("material meeting inventory");
    if (operator || brand) attrs.push("branded / managed distribution");
    p +=
      (attrs.length ? attrs.join(", ") : "available inventory and affiliation facts") +
      " — frame a conventional upper-mid to upper-scale business mix of corporate transient, group meetings, and leisure where the destination supports it. ";
    p +=
      "This overview does not estimate occupancy, ADR, RevPAR, or demand capture; those require performance data and competitive-set analysis beyond the verified property profile.";
    paragraphs.push(p.trim());
  }

  const text = paragraphs.join("\n\n").replace(/[ \t]+\n/g, "\n").trim();
  const wordCount = text.split(/\s+/).filter(Boolean).length;
  return {
    version: HOTEL_COMMERCIAL_DESCRIPTION_VERSION,
    text,
    paragraphs,
    wordCount,
    lowQuality: isLowQualityCommercialDescription(text, hotel),
    usedFields: {
      brand: Boolean(brand),
      scale: Boolean(scale),
      rooms: Boolean(rooms),
      meeting: Boolean(meetingSq),
      operator: Boolean(operator),
      demand: demand.length,
      amenities: amenities.length,
      ownership: Boolean(ownershipUsable && (economicOwner || legalOwner)),
    },
  };
}

export function resolveCommercialDescription(hotel = {}, opts = {}) {
  const fromAirtable = clean(hotel.hotelDescription) || clean(hotel.description);
  if (fromAirtable && !isLowQualityCommercialDescription(fromAirtable, hotel)) {
    return {
      version: HOTEL_COMMERCIAL_DESCRIPTION_VERSION,
      text: fromAirtable,
      source: "airtable",
      lowQuality: false,
    };
  }
  const positioning = clean(
    hotel.overviewEnrichment &&
      hotel.overviewEnrichment.positioning &&
      hotel.overviewEnrichment.positioning.text
  );
  if (positioning && !isLowQualityCommercialDescription(positioning, hotel)) {
    return {
      version: HOTEL_COMMERCIAL_DESCRIPTION_VERSION,
      text: positioning,
      source: "overview_enrichment",
      lowQuality: false,
    };
  }
  const built = buildCommercialDescription(hotel, opts);
  if (!built.lowQuality && built.text) {
    return { ...built, source: "rubric_v2" };
  }
  // If rubric marked low-quality only for length on sparse hotels, still return text
  if (built.text && built.wordCount >= 80) {
    return { ...built, source: "rubric_v2_sparse", lowQuality: false };
  }
  return {
    version: HOTEL_COMMERCIAL_DESCRIPTION_VERSION,
    text: "Property description is not yet available.",
    source: "unavailable",
    lowQuality: true,
  };
}
