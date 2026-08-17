/**
 * Brand Explorer image role taxonomy + role-match evaluation.
 *
 * Distinct images are necessary but not sufficient — captions/roles must
 * match visual/metadata evidence (Accor media type codes, SLH filename cues, etc.).
 */
import { buildImageIdentity } from "./brand-explorer-image-uniqueness.js";

export const IMAGE_ROLE_MATCH_VERSION = "image-role-match-v1";

/** Canonical role keys used by match logic. */
export const IMAGE_ROLES = Object.freeze({
  exterior_arrival: "exterior_arrival",
  guest_room_suite: "guest_room_suite",
  public_space_lobby: "public_space_lobby",
  food_beverage_experience: "food_beverage_experience",
  design_detail: "design_detail",
  property_setting: "property_setting",
  wellness_pool_spa: "wellness_pool_spa",
  meeting_event: "meeting_event",
  neighborhood_context: "neighborhood_context",
  unknown: "unknown",
});

/** Owner-facing caption prefixes for standard gallery slots. */
export const GALLERY_ROLE_CAPTIONS = Object.freeze({
  [IMAGE_ROLES.exterior_arrival]: "Exterior / Arrival",
  [IMAGE_ROLES.guest_room_suite]: "Guest Room / Suite",
  [IMAGE_ROLES.public_space_lobby]: "Public Space / Lobby",
  [IMAGE_ROLES.food_beverage_experience]: "F&B / Bar / Restaurant / Local Experience",
  [IMAGE_ROLES.design_detail]: "Design Detail / Interior Detail",
  [IMAGE_ROLES.property_setting]: "Property Setting / Destination Context",
  [IMAGE_ROLES.wellness_pool_spa]: "Wellness / Pool / Spa",
  [IMAGE_ROLES.meeting_event]: "Meeting / Event Space",
  [IMAGE_ROLES.neighborhood_context]: "Neighborhood / Local Context",
});

/** Default six-slot role sequence for gallery rematerialization. */
export const DEFAULT_GALLERY_ROLE_SEQUENCE = Object.freeze([
  IMAGE_ROLES.exterior_arrival,
  IMAGE_ROLES.guest_room_suite,
  IMAGE_ROLES.public_space_lobby,
  IMAGE_ROLES.food_beverage_experience,
  IMAGE_ROLES.design_detail,
  IMAGE_ROLES.property_setting,
]);

/**
 * Accor DAM media-type → visual category.
 * ho = hotel exterior/overview (never room/F&B by code alone).
 */
export const ACCOR_TYPE_TO_CATEGORY = Object.freeze({
  ho: IMAGE_ROLES.exterior_arrival,
  ro: IMAGE_ROLES.guest_room_suite,
  su: IMAGE_ROLES.guest_room_suite,
  ba: IMAGE_ROLES.food_beverage_experience,
  re: IMAGE_ROLES.food_beverage_experience,
  bar: IMAGE_ROLES.food_beverage_experience,
  br: IMAGE_ROLES.food_beverage_experience,
  bab: IMAGE_ROLES.food_beverage_experience,
  rsr: IMAGE_ROLES.food_beverage_experience,
  sp: IMAGE_ROLES.wellness_pool_spa,
  sw: IMAGE_ROLES.wellness_pool_spa,
  fi: IMAGE_ROLES.wellness_pool_spa,
  me: IMAGE_ROLES.meeting_event,
  sm: IMAGE_ROLES.public_space_lobby,
  wd: IMAGE_ROLES.meeting_event,
  lobby: IMAGE_ROLES.public_space_lobby,
});

/**
 * Accor DAM type token → category. Supports compound codes (roqec, bab001, rsr001)
 * where the type is a prefix of the known Accor media family, not only exact 2-letter codes.
 */
export function resolveAccorTypeCategory(typeToken = "") {
  const type = nz(typeToken).toLowerCase();
  if (!type) return IMAGE_ROLES.unknown;
  if (ACCOR_TYPE_TO_CATEGORY[type]) return ACCOR_TYPE_TO_CATEGORY[type];
  if (type.startsWith("ho")) return IMAGE_ROLES.exterior_arrival;
  if (type.startsWith("ro") || type.startsWith("su")) return IMAGE_ROLES.guest_room_suite;
  if (
    type.startsWith("ba") ||
    type.startsWith("re") ||
    type.startsWith("br") ||
    type.startsWith("rsr")
  ) {
    return IMAGE_ROLES.food_beverage_experience;
  }
  if (type.startsWith("sp") || type.startsWith("sw") || type.startsWith("fi")) {
    return IMAGE_ROLES.wellness_pool_spa;
  }
  if (type.startsWith("me") || type.startsWith("wd")) return IMAGE_ROLES.meeting_event;
  if (type.startsWith("sm") || type.includes("lobby") || type.startsWith("lb")) {
    return IMAGE_ROLES.public_space_lobby;
  }
  return IMAGE_ROLES.unknown;
}

function nz(v) {
  return v == null ? "" : String(v).trim();
}

/**
 * Parse assigned role from presentation title / caption.
 */
export function parseAssignedRoleFromCaption(title = "") {
  const t = nz(title).toLowerCase();
  if (/guest room|suite|bedroom|in-room/i.test(t) && !/meeting/i.test(t)) {
    return IMAGE_ROLES.guest_room_suite;
  }
  if (/f\s*&\s*b|f&b|dining|restaurant|bar\b|local experience|culinary/i.test(t)) {
    return IMAGE_ROLES.food_beverage_experience;
  }
  if (/exterior|arrival|facade|façade|entrance|drive/i.test(t)) {
    return IMAGE_ROLES.exterior_arrival;
  }
  if (/public space|lobby|lounge|reception|atrium/i.test(t)) {
    return IMAGE_ROLES.public_space_lobby;
  }
  if (/design detail|interior detail|materiality|architecture detail/i.test(t)) {
    return IMAGE_ROLES.design_detail;
  }
  if (/property setting|destination|neighborhood|context|landscape|surround/i.test(t)) {
    return IMAGE_ROLES.property_setting;
  }
  if (/wellness|pool|spa|fitness/i.test(t)) {
    return IMAGE_ROLES.wellness_pool_spa;
  }
  if (/meeting|event|ballroom|conference/i.test(t)) {
    return IMAGE_ROLES.meeting_event;
  }
  return IMAGE_ROLES.unknown;
}

/**
 * Detect visual category from URL / filename / source cues (no inventing).
 */
export function detectVisualCategory({
  imageUrl = "",
  filename = "",
  title = "",
  sourcePageUrl = "",
  altText = "",
} = {}) {
  const blob = [imageUrl, filename, title, sourcePageUrl, altText].map(nz).join(" ").toLowerCase();
  const evidence = [];

  // Accor typed asset codes (strongest signal) — include compound room/F&B tokens (roqec, bab001).
  // Type token is short (≤8). Do NOT run the loose fallback on Airtable CDN URLs —
  // attachment path hashes like `_wDc2BQ…_7` falsely match `wd*` → meeting_event.
  const ACCOR_TYPE =
    "(?:ho|ro|su|ba|re|bar|br|bab|rsr|sp|sw|fi|me|sm|wd|fu|xx|lobby)[a-z0-9]{0,8}";
  const isAirtableCdn = /airtableusercontent\.com/i.test(blob);
  const accor =
    blob.match(new RegExp(`/photos/([a-z0-9]+)_(${ACCOR_TYPE})_(\\d+)`, "i")) ||
    (!isAirtableCdn
      ? blob.match(new RegExp(`\\b([a-z0-9]{2,12})_(${ACCOR_TYPE})_(\\d{1,4})\\b`, "i"))
      : null);
  if (accor) {
    const type = accor[2].toLowerCase();
    const category = resolveAccorTypeCategory(type);
    evidence.push(`accor_type:${type}`);
    return {
      category,
      confidence: category === IMAGE_ROLES.unknown ? "low" : "high",
      evidence,
      accorHotelCode: accor[1].toLowerCase(),
      accorType: type,
      accorIndex: accor[3],
    };
  }

  // SLH / LucidCM filename cues (medium — useful warnings; Accor DAM codes remain high)
  if (/bedroom|guest[\s_-]?room|guestroom|suite_bedroom|in[\s_-]?room|bathroom/i.test(blob)) {
    evidence.push("filename_room_cue");
    return { category: IMAGE_ROLES.guest_room_suite, confidence: "medium", evidence };
  }
  if (/restaurant|dining|bar_|_bar|culinary|breakfast|buffet|terrace_dining/i.test(blob)) {
    evidence.push("filename_fb_cue");
    return { category: IMAGE_ROLES.food_beverage_experience, confidence: "medium", evidence };
  }
  if (/lobby|reception|lounge|atrium|staircase|stair|corridor|hallway|the-hub|the_hub/i.test(blob)) {
    evidence.push("filename_public_cue");
    return { category: IMAGE_ROLES.public_space_lobby, confidence: "medium", evidence };
  }
  if (/entrance|facade|façade|exterior|arrival|street|signage|hotel_sign|express-top|express_top/i.test(blob)) {
    evidence.push("filename_exterior_cue");
    return { category: IMAGE_ROLES.exterior_arrival, confidence: "medium", evidence };
  }
  if (/dji_|aerial|beach|ocean|pool|patio|garden|landscape|view_|skyline|harbor|harbour/i.test(blob)) {
    evidence.push("filename_setting_cue");
    const category = /pool|spa|wellness/i.test(blob)
      ? IMAGE_ROLES.wellness_pool_spa
      : IMAGE_ROLES.property_setting;
    return { category, confidence: "medium", evidence };
  }
  if (/meeting|ballroom|conference|boardroom/i.test(blob)) {
    evidence.push("filename_meeting_cue");
    return { category: IMAGE_ROLES.meeting_event, confidence: "medium", evidence };
  }

  // IHG Scene7 hotel-indigo cues (medium)
  if (/hotel-indigo/i.test(blob)) {
    if (/guest-room|bedroom|suite/i.test(blob)) {
      evidence.push("scene7_room");
      return { category: IMAGE_ROLES.guest_room_suite, confidence: "medium", evidence };
    }
    if (/restaurant|bar|dining/i.test(blob)) {
      evidence.push("scene7_fb");
      return { category: IMAGE_ROLES.food_beverage_experience, confidence: "medium", evidence };
    }
    if (/exterior|facade|arrival/i.test(blob)) {
      evidence.push("scene7_exterior");
      return { category: IMAGE_ROLES.exterior_arrival, confidence: "medium", evidence };
    }
    if (/lobby|lounge/i.test(blob)) {
      evidence.push("scene7_lobby");
      return { category: IMAGE_ROLES.public_space_lobby, confidence: "medium", evidence };
    }
  }

  evidence.push("insufficient_metadata");
  return { category: IMAGE_ROLES.unknown, confidence: "low", evidence };
}

/**
 * Roles that are compatible (caption role may accept detected category).
 */
const COMPATIBLE = Object.freeze({
  [IMAGE_ROLES.exterior_arrival]: new Set([
    IMAGE_ROLES.exterior_arrival,
    IMAGE_ROLES.property_setting, // street/destination approach sometimes overlaps
  ]),
  [IMAGE_ROLES.guest_room_suite]: new Set([IMAGE_ROLES.guest_room_suite]),
  [IMAGE_ROLES.public_space_lobby]: new Set([
    IMAGE_ROLES.public_space_lobby,
    IMAGE_ROLES.design_detail, // circulation / staircase detail sometimes dual
  ]),
  [IMAGE_ROLES.food_beverage_experience]: new Set([IMAGE_ROLES.food_beverage_experience]),
  [IMAGE_ROLES.design_detail]: new Set([
    IMAGE_ROLES.design_detail,
    IMAGE_ROLES.public_space_lobby, // architectural stair/lobby detail
    IMAGE_ROLES.wellness_pool_spa, // amenity design feature
  ]),
  [IMAGE_ROLES.property_setting]: new Set([
    IMAGE_ROLES.property_setting,
    IMAGE_ROLES.exterior_arrival,
    IMAGE_ROLES.neighborhood_context,
  ]),
  [IMAGE_ROLES.wellness_pool_spa]: new Set([IMAGE_ROLES.wellness_pool_spa]),
  [IMAGE_ROLES.meeting_event]: new Set([IMAGE_ROLES.meeting_event]),
  [IMAGE_ROLES.neighborhood_context]: new Set([
    IMAGE_ROLES.neighborhood_context,
    IMAGE_ROLES.property_setting,
  ]),
});

/** Hard conflicts — caption must never sit on these detected categories. */
const HARD_CONFLICTS = Object.freeze({
  [IMAGE_ROLES.guest_room_suite]: new Set([
    IMAGE_ROLES.food_beverage_experience,
    IMAGE_ROLES.exterior_arrival,
    IMAGE_ROLES.public_space_lobby,
    IMAGE_ROLES.property_setting,
    IMAGE_ROLES.wellness_pool_spa,
    IMAGE_ROLES.meeting_event,
  ]),
  [IMAGE_ROLES.food_beverage_experience]: new Set([
    IMAGE_ROLES.guest_room_suite,
    IMAGE_ROLES.exterior_arrival,
    IMAGE_ROLES.public_space_lobby,
    IMAGE_ROLES.property_setting,
    IMAGE_ROLES.meeting_event,
  ]),
  [IMAGE_ROLES.public_space_lobby]: new Set([
    IMAGE_ROLES.guest_room_suite,
    IMAGE_ROLES.exterior_arrival,
    IMAGE_ROLES.food_beverage_experience,
    IMAGE_ROLES.wellness_pool_spa,
  ]),
  [IMAGE_ROLES.exterior_arrival]: new Set([
    IMAGE_ROLES.guest_room_suite,
    IMAGE_ROLES.food_beverage_experience,
    IMAGE_ROLES.meeting_event,
    IMAGE_ROLES.wellness_pool_spa,
  ]),
  [IMAGE_ROLES.design_detail]: new Set([
    IMAGE_ROLES.guest_room_suite,
    IMAGE_ROLES.food_beverage_experience,
    IMAGE_ROLES.exterior_arrival, // generic facade/signage is not design detail by default
  ]),
  [IMAGE_ROLES.property_setting]: new Set([
    IMAGE_ROLES.guest_room_suite,
    IMAGE_ROLES.food_beverage_experience,
    IMAGE_ROLES.meeting_event,
  ]),
});

/**
 * Evaluate one image's caption/role vs detected visual category.
 */
export function evaluateImageRoleMatch(row = {}) {
  const imageUrl = nz(row.imageUrl);
  const filename = nz(row.imageFilename || row.filename);
  const title = nz(row.title);
  const assignedRole = parseAssignedRoleFromCaption(title);
  const detected = detectVisualCategory({
    imageUrl,
    filename,
    title,
    sourcePageUrl: row.sourcePageUrl,
    altText: row.altText,
  });
  const identity = buildImageIdentity(imageUrl, {
    slotKey: row.slotKey,
    title,
    filename,
    propertyName: row.propertyName,
    recordId: row.recordId,
  });

  let matchStatus = "pass";
  let issue = null;
  let recommendedCaption = null;
  let recommendedRole = null;

  if (!imageUrl) {
    matchStatus = "needs_replacement";
    issue = "missing_image_url";
  } else if (assignedRole === IMAGE_ROLES.unknown) {
    matchStatus = "needs_caption_patch";
    issue = "unrecognized_caption_role";
    if (detected.category !== IMAGE_ROLES.unknown) {
      recommendedRole = detected.category;
      recommendedCaption = GALLERY_ROLE_CAPTIONS[detected.category];
    }
  } else if (detected.category === IMAGE_ROLES.unknown) {
    matchStatus = "ambiguous";
    issue = "visual_category_unsupported_by_metadata";
  } else if (COMPATIBLE[assignedRole]?.has(detected.category)) {
    matchStatus = "pass";
  } else if (HARD_CONFLICTS[assignedRole]?.has(detected.category)) {
    // Proven Accor/DAM typed conflicts are hard fails; weaker filename cues are warnings.
    if (detected.confidence === "high") {
      matchStatus = "wrong_role";
      issue = `caption_${assignedRole}_on_${detected.category}`;
      recommendedRole = detected.category;
      recommendedCaption = GALLERY_ROLE_CAPTIONS[detected.category];
    } else {
      matchStatus = "caption_overclaim";
      issue = `possible_caption_${assignedRole}_on_${detected.category}`;
      recommendedRole = detected.category;
      recommendedCaption = GALLERY_ROLE_CAPTIONS[detected.category];
    }
  } else {
    matchStatus = "caption_overclaim";
    issue = `caption_${assignedRole}_weak_for_${detected.category}`;
    recommendedRole = detected.category;
    recommendedCaption = GALLERY_ROLE_CAPTIONS[detected.category];
  }

  const hardFail = matchStatus === "wrong_role" || matchStatus === "needs_replacement";

  return {
    slotKey: nz(row.slotKey),
    section: inferSection(row.slotKey),
    recordId: row.recordId || null,
    currentCaption: title,
    currentRole: assignedRole,
    imageUrl,
    filename,
    detectedVisualCategory: detected.category,
    detectionConfidence: detected.confidence,
    detectionEvidence: detected.evidence,
    matchStatus,
    issue,
    recommendedCaption,
    recommendedRole,
    hardFail,
    duplicateGroupId: identity.duplicateGroupId,
    propertyName: extractPropertyName(title),
  };
}

function inferSection(slotKey) {
  const sk = nz(slotKey);
  if (/^materials\.gallery\./.test(sk)) return "gallery";
  if (/^overview\.scenario\./.test(sk)) return "scenario";
  if (sk === "footprint.openings") return "property_example";
  return "other";
}

function extractPropertyName(title) {
  const t = nz(title);
  const idx = t.indexOf(" - ");
  if (idx < 0) return "";
  return t
    .slice(idx + 3)
    .replace(/\s*\(International Reference\)\s*$/i, "")
    .trim();
}

/**
 * Evaluate all gallery (+ optional scenario/property) rows.
 */
export function evaluateBrandImageRoleMatch({
  presentationRows = [],
  brandSlug = null,
  includeScenarios = true,
  includeProperties = true,
} = {}) {
  const rows = (presentationRows || []).filter((r) => {
    if (r?.visible === false) return false;
    const ext = nz(r?.externalDisplayStatus || r?.external_display_status);
    if (/^do not display$/i.test(ext) || /^internal only$/i.test(ext)) return false;
    if (r?.active === false) return false;
    const sk = nz(r.slotKey);
    if (/^materials\.gallery\.\d+$/.test(sk)) return true;
    if (includeScenarios && /^overview\.scenario\.\d+$/.test(sk)) return true;
    if (includeProperties && sk === "footprint.openings") return true;
    return false;
  });

  const evaluations = rows.map((r) => evaluateImageRoleMatch(r));
  const galleryEvals = evaluations.filter((e) => e.section === "gallery");
  // Gate failures: proven wrong_role / missing image only.
  // Low-confidence "ambiguous" is reported but does not block OS (no typed metadata yet).
  const unresolved = evaluations.filter(
    (e) => e.matchStatus === "wrong_role" || e.matchStatus === "needs_replacement"
  );
  const ambiguous = evaluations.filter((e) => e.matchStatus === "ambiguous");
  const warnings = evaluations.filter(
    (e) => e.matchStatus === "ambiguous" || e.matchStatus === "caption_overclaim" || e.matchStatus === "needs_caption_patch"
  );

  const pass = unresolved.length === 0 && galleryEvals.length >= 6;

  return {
    version: IMAGE_ROLE_MATCH_VERSION,
    brandSlug,
    pass,
    imageRoleMatchPass: pass,
    unresolvedRoleMismatchCount: unresolved.length,
    ambiguousCount: ambiguous.length,
    warningCount: warnings.length,
    galleryCount: galleryEvals.length,
    evaluations,
    unresolved,
    warnings,
    fieldRows: evaluations.map((e) => ({
      brand: brandSlug,
      section: e.section,
      slot: e.slotKey,
      currentCaption: e.currentCaption,
      currentRole: e.currentRole,
      imageUrl: e.imageUrl,
      detectedVisualCategory: e.detectedVisualCategory,
      matchStatus: e.matchStatus,
      issue: e.issue || "—",
      recommendedCaptionOrReplacement: e.recommendedCaption || "—",
    })),
  };
}

/**
 * Build Accor role-typed candidate URL for a hotel code.
 */
export function buildAccorRoleImageUrl(hotelCode, role, index = "00") {
  const code = nz(hotelCode).toLowerCase();
  if (!code) return null;
  const typeByRole = {
    [IMAGE_ROLES.exterior_arrival]: "ho",
    [IMAGE_ROLES.property_setting]: "ho",
    [IMAGE_ROLES.guest_room_suite]: "ro",
    [IMAGE_ROLES.food_beverage_experience]: "ba",
    [IMAGE_ROLES.wellness_pool_spa]: "sp",
    [IMAGE_ROLES.meeting_event]: "me",
    [IMAGE_ROLES.public_space_lobby]: "sp", // amenity/public circulation fallback when no lobby code
    [IMAGE_ROLES.design_detail]: "sp",
  };
  const type = typeByRole[role];
  if (!type) return null;
  // Prefer alternate index for property_setting so it differs from exterior ho_00
  const idx =
    role === IMAGE_ROLES.property_setting && index === "00" ? "01" : String(index).padStart(2, "0");
  return `https://www.ahstatic.com/photos/${code}_${type}_${idx}_p_1024x768.jpg`;
}

export function captionForRole(role, propertyName = "") {
  const base = GALLERY_ROLE_CAPTIONS[role] || "Gallery Image";
  const prop = nz(propertyName);
  return prop ? `${base} - ${prop}` : base;
}
