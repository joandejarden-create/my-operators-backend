/**
 * Hotel Explorer Overview enrichment (hotel-agnostic).
 *
 * Reuses Hotel Intelligence / ADP property profiles and
 * selectClientSafeVerifiedAttributes. Does not create a competing
 * attribute model. Does not write Airtable.
 *
 * Verification for Overview display requires:
 * - Active / non-rejected lifecycle
 * - HIGH|MEDIUM confidence (necessary but not sufficient)
 * - Acceptable evidence (URL or typed source + name)
 * - Matching HPC hotel identity when present
 * - Not Reality Gap recognition-only / inferred suitability / unsupported
 */

import { ATTRIBUTE_DEFINITIONS } from "../ai-demand-positioning/intelligence/reality-gap.js";
import { buildAdpHotelAttributes } from "./adp-attributes/build-adp-hotel-attributes.js";
import { buildHotelIntelligenceProfile } from "./adp-attributes/build-hotel-intelligence-profile.js";
import {
  loadOverviewPropertyProfile,
  resolveAdpPropertyIdForCensusRecord,
} from "./hotel-explorer-overview-enrichment-profile-loader-v1.js";
import { selectClientSafeVerifiedAttributes } from "./radar-hotel-explorer-progressive-enrichment-v1.js";

export const HOTEL_EXPLORER_OVERVIEW_ENRICHMENT_VERSION =
  "hotel_explorer_overview_enrichment_v1";

const REC_ID_RE = /^rec[A-Za-z0-9]{14}$/;

const AMENITY_PRESENTATION_CATEGORIES = new Set([
  "Amenity",
  "Meeting / Group",
  "Segment Relevance",
]);

const IDENTITY_LIKE_ATTRIBUTE_NAMES = new Set([
  "hotel name",
  "property identity key",
  "official property url",
  "official events url",
  "brand",
  "address",
  "city",
  "state",
  "postal code",
  "country",
  "market",
  "submarket",
  "rooms",
]);

function blankish(value) {
  const s = String(value == null ? "" : value).trim();
  if (!s) return true;
  const lower = s.toLowerCase();
  return (
    lower === "unknown" ||
    lower === "unknown brand" ||
    lower === "unknown hotel" ||
    lower === "—" ||
    lower === "-"
  );
}

function profileUnverifiedThemes(profile = {}) {
  const notes = profile.notes || {};
  const blocked = new Set();
  for (const [theme, text] of Object.entries(notes)) {
    if (/unverified/i.test(String(text || ""))) {
      blocked.add(String(theme).toLowerCase());
    }
  }
  return blocked;
}

function attributeTouchesUnverifiedTheme(key, blockedThemes) {
  const k = String(key || "").toLowerCase();
  if (!k) return false;
  if (blockedThemes.has("parking") && /park/.test(k)) return true;
  if (blockedThemes.has("petpolicy") && /pet/.test(k)) return true;
  if (blockedThemes.has("accessibility") && /accessib/.test(k)) return true;
  return false;
}

function labelForAttributeKey(key) {
  const raw = String(key || "").trim();
  if (!raw) return null;
  const def = ATTRIBUTE_DEFINITIONS[raw];
  if (def?.label) return def.label;
  if (/^[a-z0-9_]+$/i.test(raw)) {
    return raw
      .split("_")
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ");
  }
  return raw;
}

function isAmenityPresentationCandidate(attr) {
  const category = String(attr.attributeCategory || attr.category || "").trim();
  if (category && !AMENITY_PRESENTATION_CATEGORIES.has(category)) return false;
  const name = String(attr.attributeName || attr.name || attr.key || "")
    .trim()
    .toLowerCase();
  if (IDENTITY_LIKE_ATTRIBUTE_NAMES.has(name)) return false;
  if (/^demand node:/i.test(name)) return false;
  if (/^event space:/i.test(name) && /\{/.test(String(attr.attributeValue || ""))) {
    return false;
  }
  if (/^total meeting space|^meeting room count|^largest /i.test(name)) return false;
  return true;
}

function formatMeetingSpaceDisplay(meetingSpace) {
  if (!meetingSpace || typeof meetingSpace !== "object") return null;
  const parts = [];
  if (Number.isFinite(Number(meetingSpace.totalSqFt))) {
    parts.push(`${Number(meetingSpace.totalSqFt).toLocaleString()} sq ft`);
  }
  if (Number.isFinite(Number(meetingSpace.meetingRooms))) {
    parts.push(`${Number(meetingSpace.meetingRooms)} rooms`);
  }
  if (meetingSpace.largestRoom?.name) {
    const largest = [meetingSpace.largestRoom.name];
    if (Number.isFinite(Number(meetingSpace.largestRoom.sqFt))) {
      largest.push(`${Number(meetingSpace.largestRoom.sqFt).toLocaleString()} sq ft`);
    }
    parts.push(`largest: ${largest.join(", ")}`);
  }
  return parts.length ? parts.join(" · ") : null;
}

/**
 * Evidence-backed Overview attribute selection.
 * Confidence alone never verifies.
 */
export function selectEvidenceBackedOverviewAttributes(
  attributes = [],
  { audience = "internal", expectedHpcHotelId = null, blockedThemes = new Set() } = {}
) {
  const suppressed = [];
  const candidates = [];
  for (const attr of Array.isArray(attributes) ? attributes : []) {
    if (!attr || typeof attr !== "object") continue;
    const key = attr.attributeKey || attr.attributeName || attr.name || null;
    if (attributeTouchesUnverifiedTheme(key, blockedThemes)) {
      suppressed.push({ key, reason: "profile_theme_unverified" });
      continue;
    }
    if (attr.factKind === "POSITIONING") {
      suppressed.push({ key, reason: "positioning_not_amenity_fact" });
      continue;
    }
    const name = String(attr.attributeName || "").trim();
    if (
      attr.requireGovernedKey &&
      /^[a-z0-9_]+$/i.test(name) &&
      !ATTRIBUTE_DEFINITIONS[name]
    ) {
      suppressed.push({ key: name, reason: "ungoverned_attribute_key" });
      continue;
    }
    candidates.push(attr);
  }

  const selected = selectClientSafeVerifiedAttributes(candidates, {
    audience,
    evidenceRequired: true,
    expectedHpcHotelId,
  });

  return { selected, suppressed };
}

function annotateProfileAttributeRows(attrs, profile, hpcHotelId) {
  const officialUrl =
    profile?.officialPropertyPageUrl ||
    profile?.website ||
    profile?.sourceGovernance?.officialPropertyPageUrl ||
    null;
  const eventsUrl =
    profile?.meetingSpace?.source ||
    profile?.sourceGovernance?.eventsUrl ||
    null;

  return (Array.isArray(attrs) ? attrs : []).map((attr) => {
    const name = String(attr.attributeName || "").trim();
    const isSnakeTag = /^[a-z0-9]+(?:_[a-z0-9]+)+$/i.test(name);
    const isGovernedTag = Boolean(ATTRIBUTE_DEFINITIONS[name]);
    const sourceUrl = attr.sourceUrl || eventsUrl || officialUrl || null;
    return {
      ...attr,
      hpcHotelId: attr.hpcHotelId || hpcHotelId,
      censusRecordId: attr.censusRecordId || hpcHotelId,
      sourceUrl,
      sourceName: attr.sourceName || "ADP property profile",
      factKind: attr.factKind || "VERIFIED_FACT",
      // Only Reality Gap–style snake_case tags must be governed vocabulary.
      // Title-case commercial rows are filtered by amenity-presentation rules.
      requireGovernedKey: isSnakeTag || isGovernedTag,
    };
  });
}

/**
 * Build Overview enrichment for a census hotel id.
 * @param {string} censusRecordId
 * @param {{ hotelDto?: object, audience?: string, skipLiveHpc?: boolean, skipHiAirtable?: boolean }} [opts]
 */
export async function buildHotelExplorerOverviewEnrichment(censusRecordId, opts = {}) {
  const id = String(censusRecordId || "").trim();
  const audience = opts.audience || "internal";
  const empty = {
    version: HOTEL_EXPLORER_OVERVIEW_ENRICHMENT_VERSION,
    ok: false,
    censusRecordId: REC_ID_RE.test(id) ? id : null,
    adpPropertyId: null,
    parentCompanyOverlay: null,
    positioning: null,
    meetingSpace: null,
    verifiedAmenities: [],
    suppressed: [],
    notes: [],
  };

  if (!REC_ID_RE.test(id)) {
    return { ...empty, error: "invalid_census_record_id" };
  }

  const adpPropertyId = resolveAdpPropertyIdForCensusRecord(id);
  const loaded = loadOverviewPropertyProfile(adpPropertyId);
  const fixtureProfile = loaded?.profile || null;

  const govCensusId = String(
    fixtureProfile?.sourceGovernance?.censusRecordId || ""
  ).trim();
  if (govCensusId && govCensusId !== id) {
    return {
      ...empty,
      ok: true,
      adpPropertyId,
      error: "identity_mismatch",
      notes: [`profile_census_id=${govCensusId}`],
    };
  }

  const profileResult = await buildHotelIntelligenceProfile(id, {
    skipLiveHpc: opts.skipLiveHpc !== false,
    skipHiAirtable: opts.skipHiAirtable !== false,
  });

  if (!profileResult?.ok && !fixtureProfile) {
    return {
      ...empty,
      ok: true,
      adpPropertyId,
      notes: ["no_hotel_intelligence_profile"],
    };
  }

  const attrBuild = await buildAdpHotelAttributes(id, {
    skipLiveHpc: opts.skipLiveHpc !== false,
    skipHiAirtable: opts.skipHiAirtable !== false,
    profile: profileResult?.ok ? profileResult : undefined,
  });

  const blockedThemes = profileUnverifiedThemes(fixtureProfile || {});
  const hotelDto = opts.hotelDto || {};

  const parentFromProfile = fixtureProfile?.parentCompany || null;
  let parentCompanyOverlay = null;
  if (
    blankish(hotelDto.parentCompany) &&
    parentFromProfile &&
    !blankish(parentFromProfile)
  ) {
    parentCompanyOverlay = {
      value: parentFromProfile,
      factKind: "VERIFIED_FACT",
      confidence: "HIGH",
      sourceUrl:
        fixtureProfile?.officialPropertyPageUrl ||
        fixtureProfile?.website ||
        null,
      sourceType: "Hotel Commercial Profile",
      provenance: "adp_property_profile_parent_company",
    };
  }

  const positioningText =
    fixtureProfile?.positioning?.primary ||
    fixtureProfile?.positioning?.statement ||
    null;
  let positioning = null;
  if (positioningText && blankish(hotelDto.hotelDescription)) {
    positioning = {
      text: String(positioningText).trim(),
      factKind: "POSITIONING",
      confidence: "MEDIUM",
      sourceUrl:
        fixtureProfile?.officialPropertyPageUrl ||
        fixtureProfile?.website ||
        null,
      provenance: "adp_property_profile_positioning",
    };
  }

  const meeting =
    profileResult?.commercialProfile?.meetingSpace ||
    fixtureProfile?.meetingSpace ||
    null;
  let meetingSpace = null;
  const meetingConf = String(meeting?.confidence || "").toUpperCase();
  const meetingSource =
    meeting?.source || fixtureProfile?.sourceGovernance?.eventsUrl || null;
  if (
    meeting &&
    (meetingConf === "HIGH" || meetingConf === "MEDIUM") &&
    /^https?:\/\//i.test(String(meetingSource || "")) &&
    (meeting.totalSqFt != null || meeting.meetingRooms != null)
  ) {
    const displayValue = formatMeetingSpaceDisplay(meeting);
    if (displayValue) {
      meetingSpace = {
        displayValue,
        totalSqFt: meeting.totalSqFt ?? null,
        meetingRooms: meeting.meetingRooms ?? null,
        largestRoom: meeting.largestRoom || null,
        confidence: meetingConf,
        sourceUrl: meetingSource,
        factKind: "VERIFIED_FACT",
        provenance: "hotel_intelligence_meeting_space",
      };
    }
  }

  const annotated = annotateProfileAttributeRows(
    attrBuild?.attributes || [],
    fixtureProfile,
    id
  );

  const { selected, suppressed } = selectEvidenceBackedOverviewAttributes(annotated, {
    audience,
    expectedHpcHotelId: id,
    blockedThemes,
  });

  const verifiedAmenities = [];
  const seenLabels = new Set();
  for (const attr of selected) {
    const row = annotated.find(
      (a) =>
        (a.attributeKey || a.attributeName) === attr.key ||
        a.attributeName === attr.name
    );
    if (row && !isAmenityPresentationCandidate(row)) continue;
    const key = String(attr.name || attr.key || "").trim();
    if (!key) continue;
    const label = ATTRIBUTE_DEFINITIONS[key]
      ? ATTRIBUTE_DEFINITIONS[key].label
      : labelForAttributeKey(key);
    const labelKey = String(label).toLowerCase();
    if (seenLabels.has(labelKey)) continue;
    seenLabels.add(labelKey);
    verifiedAmenities.push({
      id: null,
      key,
      label,
      value: attr.value,
      confidence: attr.confidence,
      sourceUrl: attr.sourceUrl,
      factKind: attr.factKind || "VERIFIED_FACT",
      provenance: attr.provenance,
    });
  }

  return {
    version: HOTEL_EXPLORER_OVERVIEW_ENRICHMENT_VERSION,
    ok: true,
    censusRecordId: id,
    adpPropertyId: adpPropertyId || profileResult?.identity?.adpPropertyId || null,
    parentCompanyOverlay,
    positioning,
    meetingSpace,
    verifiedAmenities,
    suppressed,
    notes: [
      "evidence_and_identity_gated",
      "reality_gap_recognition_is_not_existence",
      "no_airtable_writes",
    ],
  };
}

/**
 * Apply enrichment onto a brand-presence hotel DTO (display only).
 */
export function applyOverviewEnrichmentToHotel(hotel, enrichment) {
  if (!hotel || typeof hotel !== "object") return hotel;
  const next = { ...hotel };
  next.overviewEnrichment = enrichment || null;
  if (!enrichment?.ok) return next;

  if (
    blankish(next.parentCompany) &&
    enrichment.parentCompanyOverlay?.value &&
    enrichment.parentCompanyOverlay.factKind === "VERIFIED_FACT"
  ) {
    next.parentCompany = enrichment.parentCompanyOverlay.value;
    next.parentCompanyProvenance = enrichment.parentCompanyOverlay;
  }

  if (
    blankish(next.hotelDescription) &&
    enrichment.positioning?.text &&
    enrichment.positioning.factKind === "POSITIONING"
  ) {
    next.hotelDescription = enrichment.positioning.text;
    next.descriptionFactKind = "POSITIONING";
    next.descriptionProvenance = enrichment.positioning;
  }

  if (
    (next.totalMeetingSpace == null || !Number.isFinite(Number(next.totalMeetingSpace))) &&
    enrichment.meetingSpace?.totalSqFt != null
  ) {
    next.totalMeetingSpace = enrichment.meetingSpace.totalSqFt;
  }
  if (enrichment.meetingSpace?.displayValue) {
    next.meetingSpaceDisplay = enrichment.meetingSpace.displayValue;
    next.meetingSpaceProvenance = enrichment.meetingSpace;
  }

  const verified = Array.isArray(enrichment.verifiedAmenities)
    ? enrichment.verifiedAmenities
    : [];
  if (verified.length) {
    next.verifiedAmenitiesDisplay = verified.map((a) => ({
      id: a.id || null,
      label: a.label,
      key: a.key,
      confidence: a.confidence,
      sourceUrl: a.sourceUrl,
      factKind: a.factKind,
    }));
  }

  return next;
}
