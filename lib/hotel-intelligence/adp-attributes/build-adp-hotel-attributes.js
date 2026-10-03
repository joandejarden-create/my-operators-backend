/**
 * buildAdpHotelAttributes(hpcHotelId)
 *
 * Derives Hotel ADP Attributes rows from Hotel Intelligence Profile.
 * Marks which attributes ADP currently consumes and how.
 */

import {
  HOTEL_ADP_ATTRIBUTE_VERSION,
  HOTEL_ADP_ATTRIBUTES_SCHEMA_VERSION,
  buildAttributeDedupeKey,
  VAL_ADP_USE_TYPE,
} from "./field-map.js";
import { buildHotelIntelligenceProfile } from "./build-hotel-intelligence-profile.js";

function normValue(v) {
  if (v == null) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v).trim();
}

function row(base, partial) {
  const attributeName = partial.attributeName;
  const attributeVersion = partial.attributeVersion || HOTEL_ADP_ATTRIBUTE_VERSION;
  const dedupeKey = buildAttributeDedupeKey(
    base.hpcHotelId,
    attributeName,
    attributeVersion,
    partial.attributeCategory
  );
  const useTypes = Array.isArray(partial.adpUseType)
    ? partial.adpUseType.filter((t) => VAL_ADP_USE_TYPE.includes(t))
    : partial.adpUseType
      ? [partial.adpUseType].filter((t) => VAL_ADP_USE_TYPE.includes(t))
      : [];
  return {
    attributeKey: dedupeKey,
    dedupeKey,
    hpcHotelId: base.hpcHotelId,
    dealalityHotelId: base.dealalityHotelId,
    adpPropertyId: base.adpPropertyId,
    hotelName: base.hotelName,
    attributeCategory: partial.attributeCategory,
    attributeName,
    attributeValue: normValue(partial.attributeValue),
    normalizedValue: normValue(
      partial.normalizedValue != null ? partial.normalizedValue : partial.attributeValue
    ),
    usedInAdp: partial.usedInAdp === true,
    adpUseType: useTypes,
    sourceType: partial.sourceType,
    sourceRecordId: partial.sourceRecordId || null,
    sourceName: partial.sourceName || null,
    sourceUrl: partial.sourceUrl || null,
    confidence: partial.confidence || "MEDIUM",
    effectiveFrom: partial.effectiveFrom || new Date().toISOString().slice(0, 10),
    effectiveTo: partial.effectiveTo || null,
    attributeVersion,
    lastVerifiedAt: partial.lastVerifiedAt || new Date().toISOString(),
    active: partial.active !== false,
    notes: partial.notes || null,
    schemaVersion: HOTEL_ADP_ATTRIBUTES_SCHEMA_VERSION,
  };
}

/**
 * Generic ADP consumption map by attribute name pattern (hotel-agnostic).
 */
function classifyAdpUse(attributeName) {
  const n = String(attributeName || "").toLowerCase();
  if (/rooms|keys|room count/.test(n)) {
    return {
      usedInAdp: true,
      adpUseType: ["Prompt Context", "Recommendation Context"],
      notes: "Room inventory used in property context and group-size framing.",
    };
  }
  if (/meeting room count|total meeting|largest meeting|ballroom|event capacity/.test(n)) {
    return {
      usedInAdp: true,
      adpUseType: ["Prompt Context", "Query Generation", "Recommendation Context"],
      notes: "Meeting inventory drives group_meeting scenarios and meeting-space attributes.",
    };
  }
  if (/city|state|market|submarket|address|postal|location|pooks|bethesda_md|times square|boca/.test(n)) {
    return {
      usedInAdp: true,
      adpUseType: ["Query Generation", "Prompt Context", "Exclusion Logic"],
      notes: "Geography used in scenario generation and confusable-entity exclusion.",
    };
  }
  if (/brand|affiliation|chain scale|marriott_hotels|hilton|hyatt|curio|renaissance/.test(n)) {
    return {
      usedInAdp: true,
      adpUseType: ["Prompt Context", "Recommendation Context"],
      notes: "Brand/affiliation used in prompt context and peer framing.",
    };
  }
  if (/nih|walter reed|fda|metro|strathmore|downtown|zoo|university|federal|demand node|demand anchor/.test(n)) {
    return {
      usedInAdp: true,
      adpUseType: ["Query Generation", "Demand Family Selection", "Prompt Context"],
      notes: "Demand node proximity informs query generation and demand-family selection.",
    };
  }
  if (/meeting_space|ballroom|m_club|fitness|pool|spa|peloton|bonvoy|amenity/.test(n)) {
    return {
      usedInAdp: true,
      adpUseType: ["Query Generation", "Scoring", "Prompt Context"],
      notes: "Attribute dictionary / scenario eligibility and measurement.",
    };
  }
  if (/official.*url|events url|website/.test(n)) {
    return {
      usedInAdp: true,
      adpUseType: ["Reporting Only", "Prompt Context"],
      notes: "Official URLs used for provenance and research; limited direct prompt injection.",
    };
  }
  if (/seasonality|need period|compression/.test(n)) {
    return {
      usedInAdp: false,
      adpUseType: ["Reporting Only"],
      notes: "Present in Hotel Intelligence; ADP does not yet consume seasonality/need periods in live prompts.",
    };
  }
  return {
    usedInAdp: false,
    adpUseType: ["Reporting Only"],
    notes: "Available in Hotel Intelligence; not currently wired into ADP consumption paths.",
  };
}

function withUse(partial) {
  const use = classifyAdpUse(partial.attributeName);
  // Prefer explicit overrides from caller
  return {
    ...use,
    ...partial,
    usedInAdp: partial.usedInAdp != null ? partial.usedInAdp : use.usedInAdp,
    adpUseType: partial.adpUseType || use.adpUseType,
    notes: [use.notes, partial.notes].filter(Boolean).join(" "),
  };
}

/**
 * @param {string} hpcHotelId
 * @param {{ skipLiveHpc?: boolean, profile?: object }} [opts]
 */
export async function buildAdpHotelAttributes(hpcHotelId, opts = {}) {
  const profile =
    opts.profile || (await buildHotelIntelligenceProfile(hpcHotelId, opts));
  if (!profile?.ok) {
    return {
      ok: false,
      error: profile?.error || "profile_failed",
      hotelId: hpcHotelId,
      attributes: [],
    };
  }

  const id = profile.identity || {};
  const base = {
    hpcHotelId: id.hpcHotelId,
    dealalityHotelId: id.dealalityHotelId,
    adpPropertyId: id.adpPropertyId,
    hotelName: id.hotelName,
  };
  const cp = profile.commercialProfile || {};
  const attrs = [];

  const push = (partial) => attrs.push(row(base, withUse(partial)));

  // Identity
  push({
    attributeCategory: "Identity",
    attributeName: "Hotel Name",
    attributeValue: id.hotelName,
    sourceType: id.source === "HPC" ? "HPC" : "Hotel Commercial Profile",
    sourceRecordId: id.hpcHotelId,
    sourceName: "Hotel Property Census / profile",
    sourceUrl: id.officialPropertyUrl,
    confidence: "HIGH",
    usedInAdp: true,
    adpUseType: ["Prompt Context", "Query Generation", "Exclusion Logic"],
  });
  push({
    attributeCategory: "Positioning",
    attributeName: "Brand",
    attributeValue: id.brand,
    sourceType: id.source === "HPC" ? "HPC" : "Hotel Commercial Profile",
    sourceRecordId: id.hpcHotelId,
    sourceUrl: id.officialPropertyUrl,
    confidence: "HIGH",
    usedInAdp: true,
    adpUseType: ["Prompt Context", "Query Generation", "Recommendation Context"],
    notes:
      "Consumed via propertyProfile.brand in generic-profile-scenarios / scenario-registry.",
  });
  if (id.identityKey) {
    push({
      attributeCategory: "Identity",
      attributeName: "Property Identity Key",
      attributeValue: id.identityKey,
      sourceType: "HPC",
      sourceRecordId: id.hpcHotelId,
      confidence: "HIGH",
      usedInAdp: true,
      adpUseType: ["Exclusion Logic", "Reporting Only"],
      notes: "Canonical identity key; ADP uses for census linkage / confusable exclusion.",
    });
  }

  // Location
  for (const [name, value, cat] of [
    ["Address", id.address, "Location"],
    ["City", id.city, "Location"],
    ["State", id.state, "Location"],
    ["Postal Code", id.postalCode, "Location"],
    ["Country", id.country, "Location"],
    ["Market", cp.market, "Location"],
    ["Submarket", cp.submarket, "Location"],
  ]) {
    if (value == null || value === "") continue;
    push({
      attributeCategory: cat,
      attributeName: name,
      attributeValue: value,
      sourceType: id.source === "HPC" ? "HPC" : "Hotel Commercial Profile",
      sourceRecordId: id.hpcHotelId,
      sourceUrl: id.officialPropertyUrl,
      confidence: "HIGH",
    });
  }

  // Commercial / rooms
  if (cp.rooms != null) {
    push({
      attributeCategory: "Commercial",
      attributeName: "Rooms",
      attributeValue: cp.rooms,
      normalizedValue: Number(cp.rooms),
      sourceType: cp.sourceType || "Hotel Commercial Profile",
      sourceRecordId: cp.sourceRecordId,
      sourceName: cp.sourceName,
      sourceUrl: cp.sourceUrl,
      confidence: cp.roomCountConfidence || "MEDIUM",
      usedInAdp: true,
      adpUseType: ["Prompt Context", "Recommendation Context"],
    });
  }

  // Meeting / group
  const ms = cp.meetingSpace || {};
  if (ms.meetingRooms != null) {
    push({
      attributeCategory: "Meeting / Group",
      attributeName: "Meeting Room Count",
      attributeValue: ms.meetingRooms,
      normalizedValue: Number(ms.meetingRooms),
      sourceType: "Hotel Commercial Profile",
      sourceRecordId: cp.sourceRecordId,
      sourceUrl: ms.source || cp.eventsUrl,
      confidence: ms.confidence || "MEDIUM",
      usedInAdp: true,
      adpUseType: ["Prompt Context", "Query Generation"],
    });
  }
  if (ms.totalSqFt != null) {
    push({
      attributeCategory: "Meeting / Group",
      attributeName: "Total Meeting Space Sq Ft",
      attributeValue: ms.totalSqFt,
      normalizedValue: Number(ms.totalSqFt),
      sourceType: "Hotel Commercial Profile",
      sourceRecordId: cp.sourceRecordId,
      sourceUrl: ms.source || cp.eventsUrl,
      confidence: ms.confidence || "MEDIUM",
      usedInAdp: true,
      adpUseType: ["Prompt Context", "Query Generation", "Recommendation Context"],
      notes: "Group demand / meeting-space context for ADP scenarios.",
    });
  }
  if (ms.largestRoom?.sqFt != null) {
    push({
      attributeCategory: "Meeting / Group",
      attributeName: "Largest Meeting Space Sq Ft",
      attributeValue: ms.largestRoom.sqFt,
      normalizedValue: Number(ms.largestRoom.sqFt),
      sourceType: "Hotel Event Space",
      sourceRecordId: cp.sourceRecordId,
      sourceUrl: ms.source,
      confidence: ms.confidence || "MEDIUM",
      usedInAdp: true,
      adpUseType: ["Prompt Context", "Query Generation"],
    });
  }
  if (ms.largestRoom?.capacity != null) {
    push({
      attributeCategory: "Meeting / Group",
      attributeName: "Largest Event Capacity",
      attributeValue: ms.largestRoom.capacity,
      normalizedValue: Number(ms.largestRoom.capacity),
      sourceType: "Hotel Event Space",
      sourceRecordId: cp.sourceRecordId,
      sourceUrl: ms.source,
      confidence: ms.confidence || "MEDIUM",
      usedInAdp: true,
      adpUseType: ["Prompt Context", "Recommendation Context"],
    });
  }
  if (ms.largestRoom?.name) {
    push({
      attributeCategory: "Meeting / Group",
      attributeName: "Largest Meeting Space Name",
      attributeValue: ms.largestRoom.name,
      sourceType: "Hotel Event Space",
      sourceRecordId: cp.sourceRecordId,
      sourceUrl: ms.source,
      confidence: ms.confidence || "MEDIUM",
      usedInAdp: true,
      adpUseType: ["Prompt Context", "Query Generation"],
    });
  }
  if (cp.eventsUrl) {
    push({
      attributeCategory: "Meeting / Group",
      attributeName: "Official Events URL",
      attributeValue: cp.eventsUrl,
      sourceType: "Hotel Commercial Profile",
      sourceUrl: cp.eventsUrl,
      confidence: "HIGH",
    });
  }
  if (id.officialPropertyUrl) {
    push({
      attributeCategory: "Identity",
      attributeName: "Official Property URL",
      attributeValue: id.officialPropertyUrl,
      sourceType: id.source === "HPC" ? "HPC" : "Hotel Commercial Profile",
      sourceUrl: id.officialPropertyUrl,
      confidence: "HIGH",
    });
  }

  // Positioning / amenity attributes from profile attribute tags
  // Any tag on propertyProfile.attributes is ADP-consumed (scenario gen, reality-gap, response parse).
  for (const tag of cp.attributes || []) {
    const isDemandish = /nih|metro|dc_|pooks|downtown|medical|federal/.test(String(tag));
    push({
      attributeCategory: isDemandish ? "Segment Relevance" : "Amenity",
      attributeName: String(tag),
      attributeValue: true,
      normalizedValue: "true",
      sourceType: "Hotel Commercial Profile",
      sourceRecordId: cp.sourceRecordId,
      sourceUrl: cp.sourceUrl,
      confidence: "MEDIUM",
      usedInAdp: true,
      adpUseType: isDemandish
        ? ["Query Generation", "Demand Family Selection", "Prompt Context"]
        : ["Query Generation", "Scoring", "Prompt Context"],
      notes:
        "On propertyProfile.attributes — consumed by generic-profile-scenarios, reality-gap, and response-parser.",
    });
  }

  // Demand nodes
  for (const node of profile.demandNodes || []) {
    push({
      attributeCategory: "Demand Node",
      attributeName: `Demand Node: ${node.name}`,
      attributeValue: node.name,
      normalizedValue: String(node.name).toLowerCase(),
      sourceType: node.sourceType || "Hotel Demand Node",
      sourceName: node.sourceName || "Hotel Demand Node",
      sourceUrl: node.sourceUrl || null,
      confidence: node.confidence || "MEDIUM",
      usedInAdp: true,
      adpUseType: ["Query Generation", "Demand Family Selection", "Prompt Context"],
      notes: `Type=${node.type || "Other"}. Demand-node proximity for ADP query/demand-family selection.`,
    });
  }

  // Event spaces (beyond largest)
  for (const space of profile.eventSpaces || []) {
    push({
      attributeCategory: "Meeting / Group",
      attributeName: `Event Space: ${space.name}`,
      attributeValue: JSON.stringify({
        sqFt: space.sqFt,
        type: space.type,
        capacities: space.capacities,
      }),
      sourceType: "Hotel Event Space",
      sourceRecordId: space.sourceRecordId,
      sourceUrl: space.sourceUrl,
      confidence: space.confidence || "MEDIUM",
      usedInAdp: true,
      adpUseType: ["Prompt Context", "Recommendation Context"],
    });
  }

  // Seasonality / need periods (usually unused by ADP today)
  for (const s of profile.seasonality || []) {
    push({
      attributeCategory: "Seasonality",
      attributeName: s.label || s.seasonLabel || "Seasonality Period",
      attributeValue: s.description || JSON.stringify(s),
      sourceType: "Hotel Seasonality",
      sourceUrl: s.sourceUrl,
      confidence: s.confidence || "LOW",
      usedInAdp: false,
      adpUseType: ["Reporting Only"],
      notes: "Public seasonality available; ADP does not currently inject into prompts.",
    });
  }
  const needPeriods = profile.needPeriods || [];
  if (needPeriods.length === 0) {
    push({
      attributeCategory: "Need Period",
      attributeName: "Need Period",
      attributeValue: "NOT_PROVIDED",
      normalizedValue: "not_provided",
      sourceType: "Model Derived",
      confidence: "UNVERIFIED",
      usedInAdp: false,
      adpUseType: ["Reporting Only"],
      notes:
        "Hotel-supplied need periods not provided. Public/market seasonality is separate. Not an ADP production blocker; ADP does not consume need periods today.",
    });
  } else {
    for (const n of needPeriods) {
      push({
        attributeCategory: "Need Period",
        attributeName: n.label || "Need Period",
        attributeValue: n.description || JSON.stringify(n),
        sourceType: n.hotelSupplied ? "Hotel Supplied" : "Hotel Seasonality",
        sourceUrl: n.sourceUrl,
        confidence: n.confidence || "LOW",
        usedInAdp: false,
        adpUseType: ["Reporting Only"],
        notes: n.hotelSupplied
          ? "Hotel-supplied need period — distinguishable from public seasonality; not yet ADP-consumed."
          : "Public need period — not yet ADP-consumed.",
      });
    }
  }

  // Drop empty values
  const filtered = attrs.filter((a) => a.attributeValue !== "" && a.attributeValue != null);

  // Critical gap flags
  const names = new Set(filtered.map((a) => a.attributeName));
  const missingCritical = [];
  for (const req of [
    "Rooms",
    "Total Meeting Space Sq Ft",
    "Meeting Room Count",
    "City",
    "Brand",
  ]) {
    if (!names.has(req)) missingCritical.push(req);
  }

  return {
    ok: true,
    hotelId: base.hpcHotelId,
    adpPropertyId: base.adpPropertyId,
    hotelName: base.hotelName,
    attributeVersion: HOTEL_ADP_ATTRIBUTE_VERSION,
    attributes: filtered,
    counts: {
      total: filtered.length,
      usedInAdp: filtered.filter((a) => a.usedInAdp).length,
      inactiveCandidate: 0,
      byCategory: filtered.reduce((acc, a) => {
        acc[a.attributeCategory] = (acc[a.attributeCategory] || 0) + 1;
        return acc;
      }, {}),
    },
    missingCritical,
    profileCompleteness: profile.completeness,
    generatedAt: new Date().toISOString(),
  };
}

export { classifyAdpUse };
