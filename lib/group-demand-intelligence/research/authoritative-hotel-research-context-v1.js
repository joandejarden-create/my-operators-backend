/**
 * Authoritative hotel research-context V1.
 *
 * Shared across Exa discovery/deep, ORS, and CIB drafting.
 * Detects HOTEL_CONTEXT_CONFLICT; does not invent geography.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const AUTHORITATIVE_HOTEL_RESEARCH_CONTEXT_V1 =
  "gdi-authoritative-hotel-research-context-v1";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function normalizeAddressKey(addr = "") {
  return text(addr)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Known official overrides when local shadow profiles omit address.
 * Provenance must be explicit; not silent prompt patches.
 */
export const OFFICIAL_HOTEL_ADDRESS_OVERRIDES_V1 = Object.freeze({
  recterKmNPdntCm0v: Object.freeze({
    officialStreetAddress:
      "756 Calzada General Mariano Escobedo, Col. Anzures, 11590 Mexico City, Mexico",
    neighborhood: "Anzures / Chapultepec",
    officialWebsite:
      "https://www.fiestamericanatravelty.com/en/grand-fiesta-americana/hotels/grand-fiesta-americana-chapultepec",
    contactPageUrl:
      "https://www.fiestamericanatravelty.com/en/grand-fiesta-americana/hotels/grand-fiesta-americana-chapultepec/contact",
    rooms: 203,
    meetingRooms: 7,
    sources: Object.freeze([
      {
        url: "https://www.fiestamericanatravelty.com/en/grand-fiesta-americana/hotels/grand-fiesta-americana-chapultepec/contact",
        title: "Contact and Location | Grand Fiesta Americana Chapultepec",
        establishes: "Official street address Mariano Escobedo 756, Col. Anzures",
      },
      {
        url: "https://www.posadas.com/en/grand-fiesta-americana",
        title: "Grand Fiesta Americana — Posadas",
        establishes: "Brand listing Mariano Escobedo 756 Col. Anzures CP 11590 · 203 rooms",
      },
    ]),
    rejectedAliases: Object.freeze([
      {
        value: "Campos Elíseos 218, Polanco, Mexico City",
        reason:
          "Not the official property street address on Fiesta Americana Travelty / Posadas pages; historically injected into research run context without profile provenance",
      },
    ]),
    verificationDate: "2026-10-28",
  }),
});

export function loadHotelConfigRecord(hotelId, { root = ROOT } = {}) {
  const id = text(hotelId);
  if (!id) return null;
  const p = path.join(
    root,
    "config/group-demand-intelligence/hotels",
    `${id}.json`,
  );
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

export function loadHotelProfileRecord(hotelId, { root = ROOT } = {}) {
  const id = text(hotelId);
  if (!id) return null;
  const p = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    id,
    "profile.json",
  );
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Merge runtime hotel input with optional data/profile.json and
 * config/group-demand-intelligence/hotels/<id>.json — hotel-agnostic.
 * Prefers explicit caller fields; never invents inventory.
 */
function resolveHotelProfileLayers(hotelId, hotelInput = {}, opts = {}) {
  const dataProfile = opts.profile || loadHotelProfileRecord(hotelId, opts) || {};
  const config = loadHotelConfigRecord(hotelId, opts) || {};
  const capability = config.capabilityProfile || {};
  const commercial = config.commercialPriorities || {};
  const territory = config.demandTerritory || {};
  return {
    dataProfile,
    config,
    displayName:
      text(hotelInput.displayName || hotelInput.name) ||
      text(dataProfile.displayName || dataProfile.name) ||
      text(config.displayName) ||
      null,
    address:
      text(dataProfile.officialStreetAddress || dataProfile.address) ||
      text(config.address) ||
      null,
    city: text(dataProfile.city || config.city || hotelInput.city) || null,
    country:
      text(dataProfile.country || config.country || hotelInput.country) || null,
    region: text(dataProfile.region || config.region || hotelInput.region) || null,
    officialWebsite:
      text(
        dataProfile.officialWebsite ||
          config.gdiOnboarding?.officialUrl ||
          hotelInput.officialWebsite,
      ) || null,
    rooms:
      dataProfile.rooms ??
      capability.totalGuestrooms ??
      hotelInput.rooms ??
      null,
    meetingRooms:
      dataProfile.meetingRooms ??
      capability.meetingRoomCount ??
      hotelInput.meetingRooms ??
      null,
    serviceLevel:
      text(
        hotelInput.serviceLevel ||
          dataProfile.hotelCharacteristics?.serviceLevel ||
          capability.serviceLevel,
      ) || null,
    classification:
      text(
        hotelInput.classification ||
          dataProfile.hotelCharacteristics?.classification ||
          capability.classification,
      ) || null,
    brand: text(hotelInput.brand || capability.brand || config.brand) || null,
    demandTerritoryLabel: text(territory.label || hotelInput.demandTerritoryLabel) || null,
    demandAnchorFocus: Array.isArray(commercial.demandAnchorFocus)
      ? commercial.demandAnchorFocus.map((x) => text(x)).filter(Boolean)
      : Array.isArray(hotelInput.demandAnchorFocus)
        ? hotelInput.demandAnchorFocus.map((x) => text(x)).filter(Boolean)
        : [],
    commercialNotes: text(commercial.notes || "") || null,
  };
}

/**
 * Build research context for a hotel. Flags conflicts; never silently picks
 * an unsupported address when conflict remains unresolved.
 */
export function buildAuthoritativeHotelResearchContext(
  hotelInput = {},
  opts = {},
) {
  const hotelId = text(hotelInput.hotelId || hotelInput.id);
  const layers = resolveHotelProfileLayers(hotelId, hotelInput, opts);
  const profile = layers.dataProfile;
  const override = OFFICIAL_HOTEL_ADDRESS_OVERRIDES_V1[hotelId] || null;

  const candidates = [];
  const pushCand = (value, source, authorityRank) => {
    const v = text(value);
    if (!v) return;
    candidates.push({
      value: v,
      key: normalizeAddressKey(v),
      source,
      authorityRank,
    });
  };

  pushCand(override?.officialStreetAddress, "official_override_verified", 100);
  pushCand(profile.officialStreetAddress || profile.address, "hotel_profile", 80);
  pushCand(layers.address, "hotel_config", 75);
  pushCand(hotelInput.officialStreetAddress, "caller_officialStreetAddress", 60);
  pushCand(hotelInput.address, "caller_address", 40);
  pushCand(opts.contextAddress, "run_context_address", 20);

  const byKey = new Map();
  for (const c of candidates) {
    if (!byKey.has(c.key)) byKey.set(c.key, []);
    byKey.get(c.key).push(c);
  }

  const distinctKeys = [...byKey.keys()];
  const conflict = distinctKeys.length > 1;

  let authoritative = null;
  if (override?.officialStreetAddress) {
    authoritative = {
      value: override.officialStreetAddress,
      source: "official_override_verified",
      authorityRank: 100,
    };
  } else if (!conflict && candidates[0]) {
    authoritative = candidates.sort((a, b) => b.authorityRank - a.authorityRank)[0];
  } else if (conflict) {
    // Prefer highest-authority candidate but keep conflict flag
    authoritative = [...candidates].sort((a, b) => b.authorityRank - a.authorityRank)[0];
  }

  const rejected = [];
  if (override?.rejectedAliases) {
    for (const r of override.rejectedAliases) {
      const rk = normalizeAddressKey(r.value);
      if (distinctKeys.includes(rk) || text(opts.contextAddress)) {
        rejected.push(r);
      }
    }
  }
  for (const [key, list] of byKey) {
    if (authoritative && key === authoritative.key) continue;
    rejected.push({
      value: list[0].value,
      reason: `Conflicts with authoritative address (${authoritative?.source || "unresolved"})`,
      sources: list.map((x) => x.source),
    });
  }

  const status = conflict
    ? authoritative && authoritative.authorityRank >= 80
      ? "HOTEL_CONTEXT_CONFLICT_RESOLVED"
      : "HOTEL_CONTEXT_CONFLICT"
    : authoritative
      ? "HOTEL_CONTEXT_OK"
      : "HOTEL_CONTEXT_INCOMPLETE";

  const ctx = {
    version: AUTHORITATIVE_HOTEL_RESEARCH_CONTEXT_V1,
    hotelId: hotelId || null,
    officialPropertyName:
      layers.displayName ||
      text(profile.displayName || profile.name || hotelInput.displayName || hotelInput.name) ||
      null,
    officialStreetAddress: authoritative?.value || null,
    addressSource: authoritative?.source || null,
    coordinates:
      profile.coordinates ||
      hotelInput.coordinates ||
      (profile.lat != null
        ? { lat: profile.lat, lng: profile.lng || profile.lon }
        : null),
    city: layers.city || text(profile.city || hotelInput.city) || null,
    country: layers.country || text(profile.country || hotelInput.country) || null,
    region: layers.region || null,
    neighborhood:
      text(override?.neighborhood || profile.neighborhood || hotelInput.neighborhood) ||
      null,
    officialWebsite:
      text(
        override?.officialWebsite ||
          layers.officialWebsite ||
          profile.officialWebsite ||
          hotelInput.officialWebsite,
      ) || null,
    contactPageUrl: text(override?.contactPageUrl || profile.contactPageUrl) || null,
    rooms: override?.rooms ?? layers.rooms ?? profile.rooms ?? hotelInput.rooms ?? null,
    meetingRooms:
      override?.meetingRooms ??
      layers.meetingRooms ??
      profile.meetingRooms ??
      hotelInput.meetingRooms ??
      null,
    serviceLevel: layers.serviceLevel || null,
    classification: layers.classification || null,
    brand: layers.brand || null,
    demandTerritoryLabel: layers.demandTerritoryLabel || null,
    demandAnchorFocus: layers.demandAnchorFocus || [],
    facilitiesNotes: text(profile.facilitiesNotes || layers.commercialNotes || "") || null,
    operatingStatus: text(profile.operatingStatus || "assumed_operating") || null,
    researchVerificationDate:
      override?.verificationDate ||
      profile.researchVerificationDate ||
      layers.config?.verifiedInventoryEvidence?.verifiedOn ||
      opts.verificationDate ||
      null,
    sourceProvenance: [
      ...(override?.sources || []),
      ...(Array.isArray(profile.addressSources) ? profile.addressSources : []),
      ...(Array.isArray(layers.config?.verifiedInventoryEvidence?.sourceUrls)
        ? layers.config.verifiedInventoryEvidence.sourceUrls.map((url) => ({
            url,
            title: "Hotel config verified inventory evidence",
            establishes: "Rooms / meetings inventory provenance from GDI hotel config",
          }))
        : []),
    ],
    status,
    conflict: conflict
      ? {
          code: "HOTEL_CONTEXT_CONFLICT",
          distinctAddresses: distinctKeys.map((k) => byKey.get(k)[0].value),
          candidates,
          rejected,
          propertySpecificGeographyAllowed:
            status === "HOTEL_CONTEXT_CONFLICT_RESOLVED",
        }
      : null,
    rejectedAliases: rejected,
  };

  return ctx;
}

/**
 * Compact block for Exa / ORS run context — always uses authoritative address.
 */
export function formatHotelResearchContextBlock(ctx = {}) {
  const anchors = Array.isArray(ctx.demandAnchorFocus)
    ? ctx.demandAnchorFocus.filter(Boolean).slice(0, 8).join("; ")
    : "";
  const lines = [
    `Hotel ID: ${ctx.hotelId || ""}`,
    `Official property name: ${ctx.officialPropertyName || ""}`,
    `Official street address: ${ctx.officialStreetAddress || "(unresolved — do not invent geography)"}`,
    `Neighborhood: ${ctx.neighborhood || ""}`,
    `City / country: ${ctx.city || ""} / ${ctx.country || ""}`,
    `Official website: ${ctx.officialWebsite || ""}`,
    `Brand: ${ctx.brand || ""}`,
    `Property classification: ${ctx.classification || ""}`,
    `Service level: ${ctx.serviceLevel || ""}`,
    `Demand territory: ${ctx.demandTerritoryLabel || ""}`,
    `Demand anchor focus: ${anchors}`,
    `Rooms: ${ctx.rooms ?? ""} · Meeting rooms: ${ctx.meetingRooms ?? ""}`,
    `Hotel context status: ${ctx.status || ""}`,
    `Address source: ${ctx.addressSource || ""}`,
    `Research verification date: ${ctx.researchVerificationDate || ""}`,
  ];
  if (ctx.status === "HOTEL_CONTEXT_CONFLICT") {
    lines.push(
      "WARNING: HOTEL_CONTEXT_CONFLICT — do not assert unsupported property-specific geographic conclusions until address is reconciled.",
    );
  }
  if (ctx.rejectedAliases?.length) {
    lines.push(
      `Do NOT use these non-authoritative addresses: ${ctx.rejectedAliases
        .map((r) => r.value)
        .join(" | ")}`,
    );
  }
  lines.push(
    "Use hotel classification/service level and demand anchors to judge hospitality-demand fit — do not assume city-center transient demand for resorts, or resort incentive demand for urban business hotels, without evidence.",
  );
  return lines.filter(Boolean).join("\n");
}

export function assertHotelContextSafeForPropertySpecificFit(ctx = {}) {
  if (ctx.status === "HOTEL_CONTEXT_CONFLICT") {
    return {
      ok: false,
      code: "HOTEL_CONTEXT_CONFLICT",
      allowPropertySpecificGeography: false,
      message:
        "Property-specific geographic win theses blocked until hotel address conflict is resolved",
    };
  }
  if (!ctx.officialStreetAddress) {
    return {
      ok: false,
      code: "HOTEL_CONTEXT_INCOMPLETE",
      allowPropertySpecificGeography: false,
      message: "No authoritative street address",
    };
  }
  return {
    ok: true,
    code: ctx.status,
    allowPropertySpecificGeography: true,
  };
}
