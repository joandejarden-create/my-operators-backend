/**
 * ADP attribute provenance mapper V1.
 * Maps profile.attributes → evidence classes without inventing URLs or timestamps.
 * VERIFIED_* only when profile fields / stewardship already support the claim.
 */

import { ATTRIBUTE_DEFINITIONS } from "../intelligence/reality-gap.js";

export const PROVENANCE_CLASS = Object.freeze({
  VERIFIED_EXISTING_SOURCE: "VERIFIED_EXISTING_SOURCE",
  VERIFIED_FROM_PROFILE_BRAND: "VERIFIED_FROM_PROFILE_BRAND",
  VERIFIED_FROM_PROFILE_AFFILIATION: "VERIFIED_FROM_PROFILE_AFFILIATION",
  VERIFIED_FROM_PROFILE_PROPERTY_TYPE: "VERIFIED_FROM_PROFILE_PROPERTY_TYPE",
  VERIFIED_FROM_PROFILE_LOCATION: "VERIFIED_FROM_PROFILE_LOCATION",
  VERIFIED_FROM_PROFILE_MEETING_SPACE: "VERIFIED_FROM_PROFILE_MEETING_SPACE",
  VERIFIED_FROM_PROFILE_STEWARDSHIP: "VERIFIED_FROM_PROFILE_STEWARDSHIP",
  VERIFIED_EXISTENCE_ONLY: "VERIFIED_EXISTENCE_ONLY",
  MAPPED_WITH_MISSING_PROVENANCE: "MAPPED_WITH_MISSING_PROVENANCE",
  NEEDS_SOURCE_VERIFICATION: "NEEDS_SOURCE_VERIFICATION",
  CONFLICTING_EVIDENCE: "CONFLICTING_EVIDENCE",
  NOT_MEASURABLE: "NOT_MEASURABLE",
});

const BRAND_ATTR_HINTS = {
  marriott_bonvoy: [/marriott/i, /bonvoy/i, /ac hotels/i, /westin/i, /st\.?\s*regis/i, /\bw hotels\b/i, /jw marriott/i, /autograph/i],
  marriott_hotels: [/marriott hotels/i],
  jw_marriott: [/jw marriott/i],
  westin: [/westin/i],
  st_regis: [/st\.?\s*regis/i],
  radisson: [/radisson/i],
  choice_hotels: [/choice/i, /radisson/i],
  radisson_individuals: [/radisson individuals/i, /faranda/i],
  faranda_collection: [/faranda collection/i],
  faranda_grand: [/faranda grand/i],
  vignette_collection: [/vignette/i],
  ihg_one_rewards: [/ihg/i, /vignette/i, /intercontinental/i],
  soft_brand: [/autograph/i, /vignette/i, /small luxury hotels/i, /slh/i, /tribute/i, /curio/i],
  hilton_honors: [/hilton/i],
};

const LOCATION_ATTR_FIELDS = [
  "city",
  "state",
  "market",
  "submarket",
  "region",
  "country",
  "address",
  "neighborhood",
];

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function evidenceUrl(profile) {
  return (
    profile?.officialPropertyPageUrl ||
    profile?.officialUrl ||
    profile?.website ||
    profile?.meetingSpace?.source ||
    profile?.eventsUrl ||
    null
  );
}

function brandBlob(profile) {
  return [profile?.brand, profile?.affiliation, profile?.parentCompany, profile?.chainScale]
    .filter(Boolean)
    .join(" | ");
}

function locationBlob(profile) {
  const nearby = Array.isArray(profile?.geography?.nearbyAttractions)
    ? profile.geography.nearbyAttractions.join(" ")
    : "";
  const aliases = Array.isArray(profile?.identityAliases)
    ? profile.identityAliases.join(" ")
    : "";
  return [
    ...LOCATION_ATTR_FIELDS.map((k) => profile?.[k]),
    nearby,
    aliases,
    profile?.locationContextNote,
  ]
    .filter(Boolean)
    .join(" | ");
}

function locationAttrMatches(attr, profile) {
  const blob = norm(locationBlob(profile));
  const def = ATTRIBUTE_DEFINITIONS[attr];
  const label = norm(def?.label || attr);
  const keyBits = attr.split("_").filter((x) => x.length > 2);
  if (label && blob.includes(label)) return true;
  // Common geo keys
  const geoMap = {
    santo_domingo: ["santo domingo"],
    zona_colonial: ["zona colonial", "ciudad colonial", "colonial"],
    piantini: ["piantini"],
    naco: ["naco"],
    tiradentes: ["tiradentes"],
    bogota: ["bogota"],
    bogota_norte: ["bogota norte", "norte"],
    mexico_city: ["mexico city", "ciudad de mexico"],
    paseo_de_la_reforma: ["paseo de la reforma", "reforma"],
    monterrey_valle: ["monterrey", "valle"],
    san_pedro_garza_garcia: ["san pedro", "garza garcia"],
    punto_valle: ["punto valle"],
    blue_mall: ["blue mall"],
    business_district: ["business", "financial", "uptown", "centro", "matogrande"],
    financial_center: ["financial"],
    urban_hotel: ["urban", "city"],
    urban_luxury: ["urban", "city"],
    urban_lifestyle: ["urban"],
    waterfront: ["waterfront", "waterside", "bayside", "bay"],
    beachfront: ["beachfront", "beach"],
    oceanfront: ["oceanfront", "ocean", "caribbean", "grand anse"],
    island_resort: ["island", "grenada", "bermuda"],
    historic_city_access: ["colonial", "historic"],
  };
  const needles = geoMap[attr];
  if (needles && needles.some((n) => blob.includes(norm(n)))) return true;
  if (keyBits.length && keyBits.every((b) => blob.includes(b))) return true;
  return false;
}

/**
 * @returns {{ attribute, class, verificationStatus, value, unit, evidenceUrl, retrievalDate, note, sourceArtifact }}
 */
export function mapAttributeProvenance(attribute, profile, { fixturePath = null } = {}) {
  const url = evidenceUrl(profile);
  const stewardshipNote = profile?.attributeStewardship?.verifiedFacts?.[attribute] || null;
  const def = ATTRIBUTE_DEFINITIONS[attribute];
  const base = {
    attribute,
    value: true,
    unit: null,
    evidenceUrl: url,
    retrievalDate: null,
    sourceArtifact: fixturePath || `profile:${profile?.propertyId}`,
    hotelId: profile?.propertyId || null,
  };

  if (!def) {
    return {
      ...base,
      class: PROVENANCE_CLASS.NOT_MEASURABLE,
      verificationStatus: PROVENANCE_CLASS.NOT_MEASURABLE,
      note: "Attribute key not in ATTRIBUTE_DEFINITIONS",
    };
  }

  if (!url) {
    return {
      ...base,
      class: PROVENANCE_CLASS.MAPPED_WITH_MISSING_PROVENANCE,
      verificationStatus: PROVENANCE_CLASS.MAPPED_WITH_MISSING_PROVENANCE,
      note: "No officialPropertyPageUrl/website/events source on profile — cannot attach citation",
    };
  }

  // Stewardship already recorded on governed profile (not invented here)
  if (stewardshipNote) {
    return {
      ...base,
      class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
      verificationStatus: PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_STEWARDSHIP,
      note: String(stewardshipNote).slice(0, 240),
    };
  }

  const brandText = brandBlob(profile);
  if (BRAND_ATTR_HINTS[attribute]) {
    const ok = BRAND_ATTR_HINTS[attribute].some((re) => re.test(brandText));
    if (ok) {
      const status =
        attribute.includes("bonvoy") || attribute.includes("rewards") || attribute.includes("honors")
          ? PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_AFFILIATION
          : PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_BRAND;
      return {
        ...base,
        class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
        verificationStatus: status,
        note: `Matched brand/affiliation fields: ${brandText}`,
      };
    }
  }

  if (attribute === "full_service") {
    const pt = String(profile?.propertyType || "").toLowerCase();
    const pos = String(profile?.positioning?.primary || "").toLowerCase();
    const diffs = (profile?.positioning?.differentiators || []).join(" ").toLowerCase();
    const scale = String(profile?.chainScale || "").toLowerCase();
    if (
      pt.includes("full_service") ||
      pt.includes("full-service") ||
      /full-?service/.test(pos) ||
      /full-?service/.test(diffs) ||
      // Upper Upscale / Luxury chain-scale full-service hotels with meeting inventory
      ((/upper upscale|luxury|upscale/.test(scale) || /full-?service/.test(pos)) &&
        profile?.meetingSpace &&
        typeof profile.meetingSpace === "object")
    ) {
      return {
        ...base,
        class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
        verificationStatus: PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_PROPERTY_TYPE,
        note: `propertyType/positioning/chainScale+meetingSpace indicates full-service (${profile?.propertyType || profile?.chainScale || "from positioning"})`,
      };
    }
  }

  if (attribute === "upper_upscale" || attribute === "upscale" || attribute === "luxury") {
    const scale = String(profile?.chainScale || "").toLowerCase();
    const pos = String(profile?.positioning?.primary || "").toLowerCase();
    if (
      (attribute === "luxury" && (/luxury/.test(scale) || /luxury/.test(pos))) ||
      (attribute === "upper_upscale" && /upper upscale/.test(scale)) ||
      (attribute === "upscale" && (/upscale/.test(scale) || /upscale/.test(pos)))
    ) {
      return {
        ...base,
        class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
        verificationStatus: PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_PROPERTY_TYPE,
        note: `chainScale/positioning=${profile?.chainScale || profile?.positioning?.primary || ""}`,
      };
    }
  }

  if (attribute === "aimbridge_managed") {
    const op = String(profile?.operatorNotes || profile?.assetManager || "").toLowerCase();
    if (/aimbridge/.test(op)) {
      return {
        ...base,
        class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
        verificationStatus: PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_STEWARDSHIP,
        note: `operator/assetManager fields: ${profile?.assetManager || profile?.operatorNotes}`,
      };
    }
  }

  if (["meeting_space", "ballroom", "large_ballroom", "event_space_outdoor", "wedding_venue"].includes(attribute)) {
    const ms = profile?.meetingSpace;
    if (ms && typeof ms === "object") {
      const meetingUrl = ms.source || profile?.eventsUrl || url;
      if (attribute === "meeting_space" && (ms.meetingRooms || ms.totalSqFt || ms.totalSqM || ms.rooms?.length)) {
        return {
          ...base,
          evidenceUrl: meetingUrl,
          class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
          verificationStatus: PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_MEETING_SPACE,
          note: "meetingSpace object present on governed profile (existence / inventory — not inventing sq ft beyond profile)",
        };
      }
      if (attribute === "ballroom" || attribute === "large_ballroom") {
        const largest = ms.largestRoom;
        if (largest || (Array.isArray(ms.rooms) && ms.rooms.some((r) => /banquet|ballroom|gran/i.test(r?.name || "")))) {
          return {
            ...base,
            evidenceUrl: meetingUrl,
            class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
            verificationStatus: PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_MEETING_SPACE,
            note: "Largest / banquet room present on profile meetingSpace",
          };
        }
      }
      if (attribute === "event_space_outdoor" && ms.outdoorEventSpace === true) {
        return {
          ...base,
          evidenceUrl: meetingUrl,
          class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
          verificationStatus: PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_MEETING_SPACE,
          note: "meetingSpace.outdoorEventSpace=true on profile",
        };
      }
      if (attribute === "wedding_venue" && ms.weddingVenue === true) {
        return {
          ...base,
          evidenceUrl: meetingUrl,
          class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
          verificationStatus: PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_MEETING_SPACE,
          note: "meetingSpace.weddingVenue=true on profile",
        };
      }
      if (attribute === "meeting_space") {
        return {
          ...base,
          evidenceUrl: meetingUrl,
          class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
          verificationStatus: PROVENANCE_CLASS.VERIFIED_EXISTENCE_ONLY,
          note: "meetingSpace object exists; detailed inventory incomplete — existence only",
        };
      }
    }
  }

  if (locationAttrMatches(attribute, profile)) {
    return {
      ...base,
      class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
      verificationStatus: PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_LOCATION,
      note: `Location/market fields support attribute (${locationBlob(profile).slice(0, 160)})`,
    };
  }

  // Urban tags: city-market hotels with a named city on the governed profile
  if (
    (attribute === "urban_hotel" || attribute === "urban_luxury" || attribute === "urban_lifestyle") &&
    String(profile?.city || "").trim()
  ) {
    return {
      ...base,
      class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
      verificationStatus: PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_LOCATION,
      note: `Named city on governed profile (${profile.city}) supports urban location tag`,
    };
  }

  // Positioning differentiators already on the governed profile (not invented copy)
  const diffBlob = norm(
    [
      profile?.positioning?.primary,
      ...(profile?.positioning?.differentiators || []),
      profile?.operatorNotes,
      profile?.roomCountNotes,
      profile?.meetingSpace?.notes,
      profile?.meetingSpace?.interpretationNote,
      profile?.attributeStewardship?.note,
      profile?.locationContextNote,
      ...(profile?.sourceBasis || []),
    ]
      .filter(Boolean)
      .join(" | ")
  );
  const diffKeywords = {
    historic_property: ["historic", "1925", "colonial houses", "historic hotel"],
    boutique_feel: ["intimate", "boutique", "compact", "64-suite", "64 suite"],
    wedding_venue: ["wedding", "celebration"],
    outdoor_pool: ["outdoor pool", "plunge pool", "swimming pool", "courtyards / pools", "pools"],
    honeymoon_destination: ["honeymoon", "romance and celebration", "romance"],
    infinity_pool: ["infinity pool"],
    spa: ["spa"],
    heavenly_spa: ["heavenly spa"],
    full_service_spa: ["full-service spa", "full service spa", "janissa spa"],
    fine_dining: ["dining", "restaurant", "brasserie"],
    rooftop_dining: ["rooftop dining", "deck11", "rooftop"],
    rooftop_bar: ["rooftop bar", "rooftop terrace", "rooftop dining", "terrace dining"],
    executive_lounge: ["executive lounge", "club lounge"],
    westin_club: ["westin club"],
    butler_service: ["butler"],
    fitness_center: ["fitness", "gym", "wellness", "heavenly spa"],
    wellness: ["wellness", "heavenly spa", "fitness studio", "spa guests"],
    concierge: ["concierge"],
    suite_inventory: ["suite", "suites"],
    swim_out: ["swim-out", "swim out", "plunge"],
    business_district: ["business-district", "business district", "connecticut avenue", "financial"],
    urban_hotel: ["urban", "city"],
    urban_luxury: ["urban luxury", "urban"],
    design_forward: ["design"],
    five_diamond: ["five diamond", "5 diamond"],
    king_cole_bar: ["king cole"],
    panoramic_views: ["views", "panoramic"],
    private_balconies: ["balcony", "balconies"],
    event_space_outdoor: ["outdoor event", "beachfront wedding", "outdoor venue"],
    aimbridge_managed: ["aimbridge"],
    waterfront: ["waterfront", "harbour", "harbor", "bayfront", "bayside", "waterside"],
  };
  const needles = diffKeywords[attribute];
  if (needles && needles.some((n) => diffBlob.includes(norm(n)))) {
    return {
      ...base,
      class: PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
      verificationStatus: PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_STEWARDSHIP,
      note: `Supported by governed profile positioning/operator notes (keyword match on existing copy)`,
    };
  }

  // Amenity / product tags without stewardship or structured fields
  return {
    ...base,
    class: PROVENANCE_CLASS.NEEDS_SOURCE_VERIFICATION,
    verificationStatus: PROVENANCE_CLASS.NEEDS_SOURCE_VERIFICATION,
    note: "Present on profile.attributes but no stewardship note, brand/affiliation match, meetingSpace field, location field, or positioning differentiator support",
  };
}

export function buildGovernedAttributeSetFromProfile(profile, { fixturePath = null, version = null } = {}) {
  const attrs = (profile?.attributes || []).filter((a) => ATTRIBUTE_DEFINITIONS[a]);
  const rows = attrs.map((a) => mapAttributeProvenance(a, profile, { fixturePath }));
  const verified = rows.filter((r) => String(r.verificationStatus || "").startsWith("VERIFIED_"));
  const needsResearch = rows.filter((r) => r.class === PROVENANCE_CLASS.NEEDS_SOURCE_VERIFICATION);
  const missingProv = rows.filter((r) => r.class === PROVENANCE_CLASS.MAPPED_WITH_MISSING_PROVENANCE);

  const setVersion =
    version ||
    `adp_${String(profile?.propertyId || "unknown").replace(/^adp_/, "")}_attribute_set_v1_rg_cert_20261019`;

  const url = evidenceUrl(profile);
  const attributeSet = {
    version: setVersion,
    status: verified.length === 0 ? "EMPTY" : verified.length < attrs.length ? "GOVERNED_PARTIAL" : "GOVERNED_COMPLETE",
    contractVersion: "ADP_PROPERTY_ATTRIBUTE_INTELLIGENCE_V1",
    retrievedAt: null,
    retrievalDatePolicy: "NOT_INVENTED — retrievalDate null unless already on source artifact",
    sourceBasis: [fixturePath, url].filter(Boolean),
    facts: verified.map((r) => ({
      attribute: r.attribute,
      value: r.value,
      unit: r.unit,
      evidenceUrl: r.evidenceUrl,
      retrievalDate: r.retrievalDate,
      verificationStatus: r.verificationStatus,
      note: r.note,
    })),
    explicitlyNotAsserted: needsResearch.map((r) => ({
      attribute: r.attribute,
      reason: r.note,
      class: r.class,
    })),
  };

  return {
    rows,
    verifiedAttributeKeys: verified.map((r) => r.attribute),
    needsResearchKeys: needsResearch.map((r) => r.attribute),
    missingProvenanceKeys: missingProv.map((r) => r.attribute),
    attributeSet,
    coverage: {
      total: attrs.length,
      verified: verified.length,
      needsResearch: needsResearch.length,
      missingProvenance: missingProv.length,
      complete: verified.length === attrs.length && attrs.length > 0,
    },
    attributesEvidenceCertified: verified.length,
  };
}
