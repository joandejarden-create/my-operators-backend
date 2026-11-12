/**
 * GDI Hotel Commercial Profiles — Airtable + local shadow persistence.
 * Does not mutate ADP facility "Hotel Commercial Profiles" attribute records.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import Airtable from "airtable";
import {
  assertNotLegacyMvpCanonicalBase,
} from "../../decision-outcomes/airtable-base.js";
import {
  resolveNeedDateAirtableBaseId,
  isNeedDateAirtableLiveEnabled,
} from "./need-date-airtable-store-v3.js";
import {
  HOTEL_COMMERCIAL_PRIORITIES_V1,
  TABLE_GDI_HOTEL_COMMERCIAL_PROFILES,
  TABLE_GDI_COMMERCIAL_PROFILE_CHANGE_HISTORY,
  emptyCommercialProfile,
  DATA_PROVENANCE,
} from "./hotel-commercial-priorities-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const LOCAL_DIR = path.join(
  ROOT,
  "data/group-demand-intelligence/hotel-commercial-priorities",
);

export const F_GDI_COMMERCIAL_PROFILE = Object.freeze({
  profileId: "Profile ID",
  hotelId: "Hotel ID",
  hotelName: "Hotel Name",
  linkedFacilityProfile: "Linked Facility Profile",
  revenuePrioritiesJson: "Revenue Priorities JSON",
  preferredBusinessJson: "Preferred Business JSON",
  prioritySegmentsJson: "Priority Segments JSON",
  demandGeneratorsJson: "Demand Generators JSON",
  knownAccountsJson: "Known Accounts JSON",
  restrictionsJson: "Restrictions JSON",
  commercialTeamJson: "Commercial Team JSON",
  provenance: "Provenance",
  isSyntheticTest: "Is Synthetic Test",
  currentVersion: "Current Version",
  lastConfirmedAt: "Last Confirmed At",
  updatedBy: "Updated By",
  changeReason: "Change Reason",
  schemaVersion: "Schema Version",
  notes: "Notes",
});

export const F_PROFILE_HISTORY = Object.freeze({
  changeId: "Change ID",
  profileId: "Profile ID",
  hotelId: "Hotel ID",
  previousVersion: "Previous Version",
  newVersion: "New Version",
  changeType: "Change Type",
  previousValues: "Previous Values",
  newValues: "New Values",
  changedBy: "Changed By",
  changedAt: "Changed At",
  changeReason: "Change Reason",
  schemaVersion: "Schema Version",
});

function text(v) {
  return String(v || "").trim();
}

function token(env = process.env) {
  return env.AIRTABLE_API_KEY || env.AIRTABLE_PAT || "";
}

function getBase(env = process.env) {
  const apiKey = token(env);
  const baseId = resolveNeedDateAirtableBaseId(env);
  if (!apiKey || !baseId) {
    const err = new Error("commercial_profile_airtable_not_configured");
    err.code = "AIRTABLE_NOT_CONFIGURED";
    err.status = 503;
    throw err;
  }
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "commercial_priorities_v1" });
  Airtable.configure({ apiKey });
  return { base: new Airtable({ apiKey }).base(baseId), baseId };
}

function parseJsonField(raw, fallback) {
  if (raw == null || raw === "") return fallback;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return fallback;
  }
}

function localPath(hotelId) {
  return path.join(LOCAL_DIR, `${hotelId}.json`);
}

function historyPath(hotelId) {
  return path.join(LOCAL_DIR, `${hotelId}.history.json`);
}

export function loadLocalCommercialProfile(hotelId) {
  const p = localPath(hotelId);
  if (!fs.existsSync(p)) return emptyCommercialProfile(hotelId);
  return { ...emptyCommercialProfile(hotelId), ...JSON.parse(fs.readFileSync(p, "utf8")) };
}

export function saveLocalCommercialProfile(hotelId, profile, historyEntry = null) {
  fs.mkdirSync(LOCAL_DIR, { recursive: true });
  const next = {
    ...emptyCommercialProfile(hotelId),
    ...profile,
    hotelId,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(localPath(hotelId), `${JSON.stringify(next, null, 2)}\n`);
  if (historyEntry) {
    const hp = historyPath(hotelId);
    const hist = fs.existsSync(hp)
      ? JSON.parse(fs.readFileSync(hp, "utf8"))
      : { hotelId, changes: [] };
    hist.changes.push(historyEntry);
    fs.writeFileSync(hp, `${JSON.stringify(hist, null, 2)}\n`);
  }
  return next;
}

function recordToProfile(rec) {
  const f = rec.fields || {};
  const preferred = parseJsonField(f[F_GDI_COMMERCIAL_PROFILE.preferredBusinessJson], {});
  const segments = parseJsonField(f[F_GDI_COMMERCIAL_PROFILE.prioritySegmentsJson], {});
  const gens = parseJsonField(f[F_GDI_COMMERCIAL_PROFILE.demandGeneratorsJson], {});
  const accounts = parseJsonField(f[F_GDI_COMMERCIAL_PROFILE.knownAccountsJson], {});
  const restrictions = parseJsonField(f[F_GDI_COMMERCIAL_PROFILE.restrictionsJson], {});
  const team = parseJsonField(f[F_GDI_COMMERCIAL_PROFILE.commercialTeamJson], {});
  return {
    ...emptyCommercialProfile(text(f[F_GDI_COMMERCIAL_PROFILE.hotelId])),
    profileId: text(f[F_GDI_COMMERCIAL_PROFILE.profileId]),
    airtableRecordId: rec.id,
    hotelId: text(f[F_GDI_COMMERCIAL_PROFILE.hotelId]),
    hotelName: text(f[F_GDI_COMMERCIAL_PROFILE.hotelName]) || null,
    revenuePriorities: parseJsonField(
      f[F_GDI_COMMERCIAL_PROFILE.revenuePrioritiesJson],
      [],
    ),
    peakRoomsPreset: preferred.peakRoomsPreset || null,
    peakRoomsCustom: preferred.peakRoomsCustom || null,
    peakRoomsStrength: preferred.peakRoomsStrength || null,
    bookingWindow: preferred.bookingWindow || null,
    bookingWindowStrength: preferred.bookingWindowStrength || null,
    meetingSpaceNeed: preferred.meetingSpaceNeed || null,
    stayDuration: preferred.stayDuration || null,
    dayPreference: preferred.dayPreference || null,
    specificDays: preferred.specificDays || [],
    segmentMode: segments.segmentMode || null,
    prioritySegments: segments.prioritySegments || [],
    excludedSegments: segments.excludedSegments || [],
    demandGeneratorCategories: gens.categories || [],
    knownOrganizations: gens.organizations || [],
    knownAccounts: accounts.accounts || [],
    businessToAvoid: restrictions.businessToAvoid || [],
    hotelConsiderations: restrictions.hotelConsiderations || [],
    restrictionNotes: restrictions.notes || "",
    commercialTeam: team || {},
    provenance: text(f[F_GDI_COMMERCIAL_PROFILE.provenance]) || DATA_PROVENANCE.NOT_PROVIDED,
    isSyntheticTest: Boolean(f[F_GDI_COMMERCIAL_PROFILE.isSyntheticTest]),
    currentVersion: Number(f[F_GDI_COMMERCIAL_PROFILE.currentVersion] || 1),
    lastConfirmedAt: f[F_GDI_COMMERCIAL_PROFILE.lastConfirmedAt] || null,
    updatedBy: text(f[F_GDI_COMMERCIAL_PROFILE.updatedBy]) || null,
    schemaVersion: text(f[F_GDI_COMMERCIAL_PROFILE.schemaVersion]) || HOTEL_COMMERCIAL_PRIORITIES_V1,
  };
}

function profileToFields(profile) {
  const preferred = {
    peakRoomsPreset: profile.peakRoomsPreset,
    peakRoomsCustom: profile.peakRoomsCustom,
    peakRoomsStrength: profile.peakRoomsStrength,
    bookingWindow: profile.bookingWindow,
    bookingWindowStrength: profile.bookingWindowStrength,
    meetingSpaceNeed: profile.meetingSpaceNeed,
    stayDuration: profile.stayDuration,
    dayPreference: profile.dayPreference,
    specificDays: profile.specificDays || [],
  };
  return {
    [F_GDI_COMMERCIAL_PROFILE.profileId]: profile.profileId,
    [F_GDI_COMMERCIAL_PROFILE.hotelId]: profile.hotelId,
    [F_GDI_COMMERCIAL_PROFILE.hotelName]: profile.hotelName || undefined,
    [F_GDI_COMMERCIAL_PROFILE.revenuePrioritiesJson]: JSON.stringify(
      profile.revenuePriorities || [],
    ),
    [F_GDI_COMMERCIAL_PROFILE.preferredBusinessJson]: JSON.stringify(preferred),
    [F_GDI_COMMERCIAL_PROFILE.prioritySegmentsJson]: JSON.stringify({
      segmentMode: profile.segmentMode,
      prioritySegments: profile.prioritySegments || [],
      excludedSegments: profile.excludedSegments || [],
    }),
    [F_GDI_COMMERCIAL_PROFILE.demandGeneratorsJson]: JSON.stringify({
      categories: profile.demandGeneratorCategories || [],
      organizations: profile.knownOrganizations || [],
    }),
    [F_GDI_COMMERCIAL_PROFILE.knownAccountsJson]: JSON.stringify({
      accounts: profile.knownAccounts || [],
    }),
    [F_GDI_COMMERCIAL_PROFILE.restrictionsJson]: JSON.stringify({
      businessToAvoid: profile.businessToAvoid || [],
      hotelConsiderations: profile.hotelConsiderations || [],
      notes: profile.restrictionNotes || "",
    }),
    [F_GDI_COMMERCIAL_PROFILE.commercialTeamJson]: JSON.stringify(
      profile.commercialTeam || {},
    ),
    [F_GDI_COMMERCIAL_PROFILE.provenance]: profile.provenance,
    [F_GDI_COMMERCIAL_PROFILE.isSyntheticTest]: Boolean(profile.isSyntheticTest),
    [F_GDI_COMMERCIAL_PROFILE.currentVersion]: Number(profile.currentVersion || 1),
    [F_GDI_COMMERCIAL_PROFILE.lastConfirmedAt]:
      profile.lastConfirmedAt || new Date().toISOString(),
    [F_GDI_COMMERCIAL_PROFILE.updatedBy]: profile.updatedBy || undefined,
    [F_GDI_COMMERCIAL_PROFILE.changeReason]: profile.changeReason || undefined,
    [F_GDI_COMMERCIAL_PROFILE.schemaVersion]: HOTEL_COMMERCIAL_PRIORITIES_V1,
  };
}

export async function getCommercialProfileFromAirtable(hotelId, env = process.env) {
  const { base } = getBase(env);
  const rows = await base(TABLE_GDI_HOTEL_COMMERCIAL_PROFILES)
    .select({
      filterByFormula: `{${F_GDI_COMMERCIAL_PROFILE.hotelId}}='${String(hotelId).replace(/'/g, "\\'")}'`,
      maxRecords: 3,
    })
    .firstPage();
  if (!rows.length) return null;
  // Prefer non-synthetic hotel-confirmed when multiple exist
  const sorted = [...rows].sort((a, b) => {
    const as = a.fields?.[F_GDI_COMMERCIAL_PROFILE.isSyntheticTest] ? 1 : 0;
    const bs = b.fields?.[F_GDI_COMMERCIAL_PROFILE.isSyntheticTest] ? 1 : 0;
    return as - bs;
  });
  return recordToProfile(sorted[0]);
}

export async function loadCommercialProfile(hotelId, opts = {}) {
  const useAirtable =
    opts.useAirtable === true ||
    (opts.useAirtable !== false && isNeedDateAirtableLiveEnabled(opts.env || process.env));
  if (useAirtable) {
    try {
      const hit = await getCommercialProfileFromAirtable(hotelId, opts.env);
      if (hit) return { ...hit, dataMode: "AIRTABLE_AI_DEMAND_POSITIONING" };
    } catch (err) {
      if (opts.allowLocalFallback === false) throw err;
    }
  }
  const local = loadLocalCommercialProfile(hotelId);
  return { ...local, dataMode: "LOCAL_SHADOW" };
}

export async function saveCommercialProfile(hotelId, patch = {}, opts = {}) {
  const useAirtable =
    opts.useAirtable === true ||
    (opts.useAirtable !== false && isNeedDateAirtableLiveEnabled(opts.env || process.env));
  const actor = opts.actorUser || {};
  const reason = text(opts.reason) || "Hotel commercial priorities update";

  const current = await loadCommercialProfile(hotelId, {
    useAirtable,
    env: opts.env,
    allowLocalFallback: true,
  });

  if (
    opts.expectedVersion != null &&
    Number(current.currentVersion || 1) !== Number(opts.expectedVersion)
  ) {
    const err = new Error("commercial_profile_version_conflict");
    err.code = "COMMERCIAL_PROFILE_VERSION_CONFLICT";
    err.status = 409;
    err.currentVersion = current.currentVersion;
    err.expectedVersion = opts.expectedVersion;
    throw err;
  }

  const profileId =
    current.profileId ||
    `gcp_${createHash("sha256").update(`${hotelId}|gdi-commercial`).digest("hex").slice(0, 12)}`;

  const next = {
    ...current,
    ...patch,
    hotelId,
    profileId,
    currentVersion: Number(current.currentVersion || 1) + (current.profileId ? 1 : 0) || 1,
    lastConfirmedAt: new Date().toISOString(),
    updatedBy: actor.email || actor.id || opts.actorSource || "hotel_sales",
    changeReason: reason,
    provenance:
      patch.provenance ||
      current.provenance ||
      DATA_PROVENANCE.HOTEL_CONFIRMED,
  };
  if (!current.profileId) next.currentVersion = 1;

  const historyEntry = {
    changeId: `gcpchg_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 8)}`,
    profileId,
    hotelId,
    previousVersion: current.profileId ? Number(current.currentVersion || 1) : 0,
    newVersion: next.currentVersion,
    changeType: current.profileId ? "MODIFY" : "ADD",
    previousValues: current.profileId ? current : null,
    newValues: next,
    changedBy: next.updatedBy,
    changedAt: new Date().toISOString(),
    changeReason: reason,
    schemaVersion: HOTEL_COMMERCIAL_PRIORITIES_V1,
  };

  if (!useAirtable) {
    const saved = saveLocalCommercialProfile(hotelId, next, historyEntry);
    return { ok: true, profile: saved, change: historyEntry, dataMode: "LOCAL_SHADOW" };
  }

  const { base } = getBase(opts.env || process.env);
  const fields = profileToFields(next);
  // strip undefined
  for (const k of Object.keys(fields)) {
    if (fields[k] === undefined) delete fields[k];
  }

  let recordId = current.airtableRecordId;
  if (recordId) {
    await base(TABLE_GDI_HOTEL_COMMERCIAL_PROFILES).update(recordId, fields);
  } else {
    const created = await base(TABLE_GDI_HOTEL_COMMERCIAL_PROFILES).create(fields);
    recordId = created.id;
  }

  try {
    await base(TABLE_GDI_COMMERCIAL_PROFILE_CHANGE_HISTORY).create({
      [F_PROFILE_HISTORY.changeId]: historyEntry.changeId,
      [F_PROFILE_HISTORY.profileId]: profileId,
      [F_PROFILE_HISTORY.hotelId]: hotelId,
      [F_PROFILE_HISTORY.previousVersion]: historyEntry.previousVersion,
      [F_PROFILE_HISTORY.newVersion]: historyEntry.newVersion,
      [F_PROFILE_HISTORY.changeType]: historyEntry.changeType,
      [F_PROFILE_HISTORY.previousValues]: JSON.stringify(
        historyEntry.previousValues || {},
      ),
      [F_PROFILE_HISTORY.newValues]: JSON.stringify(historyEntry.newValues || {}),
      [F_PROFILE_HISTORY.changedBy]: historyEntry.changedBy,
      [F_PROFILE_HISTORY.changedAt]: historyEntry.changedAt,
      [F_PROFILE_HISTORY.changeReason]: reason,
      [F_PROFILE_HISTORY.schemaVersion]: HOTEL_COMMERCIAL_PRIORITIES_V1,
    });
  } catch {
    // History table may not exist yet during first ensure — local history still saved
    saveLocalCommercialProfile(hotelId, { ...next, airtableRecordId: recordId }, historyEntry);
  }

  const saved = {
    ...next,
    airtableRecordId: recordId,
    dataMode: "AIRTABLE_AI_DEMAND_POSITIONING",
  };
  saveLocalCommercialProfile(hotelId, saved, historyEntry);
  return { ok: true, profile: saved, change: historyEntry, dataMode: saved.dataMode };
}

/** Map legacy hotel-config targetSegments labels → 14-segment strategy ids. */
const CONFIG_SEGMENT_HINTS = Object.freeze([
  { pattern: /association/i, id: "association_events" },
  { pattern: /corporate/i, id: "scaling_companies" },
  { pattern: /government|contractor/i, id: "government_public_sector" },
  { pattern: /sport/i, id: "sports_entertainment" },
  { pattern: /weekend|social|wedding/i, id: "social_special_events" },
  { pattern: /medical|health|scientific|university|education/i, id: "education_scientific" },
]);

const CONFIG_AUDIENCE_HINTS = Object.freeze([
  { pattern: /nih|hospital|medical|health/i, id: "HOSPITALS_MEDICAL" },
  { pattern: /university|education/i, id: "UNIVERSITIES" },
  { pattern: /government|federal|military/i, id: "GOVERNMENT_AGENCIES" },
  { pattern: /corporate|employer|contractor/i, id: "CORPORATE_OFFICES" },
  { pattern: /sport/i, id: "SPORTS_FACILITIES" },
]);

/**
 * Build smart suggestions from verified property / inventory / hotel config —
 * NOT hotel-confirmed and never auto-written as Hotel Confirmed.
 */
export function buildCommercialProfileSuggestions(hotelId, hotelCtx = {}, inventory = []) {
  const suggestions = {
    provenance: DATA_PROVENANCE.SUGGESTED_FROM_RESEARCH,
    peakRoomsPreset: null,
    meetingSpaceNeed: null,
    dayPreference: null,
    prioritySegments: [],
    demandGeneratorCategories: [],
    notes: [],
  };
  const rooms = Number(
    hotelCtx.capabilityProfile?.totalGuestrooms ||
      hotelCtx.profile?.guestrooms?.totalGuestrooms ||
      0,
  );
  const meetingSqFt = Number(
    hotelCtx.capabilityProfile?.totalMeetingSpaceSqFt ||
      hotelCtx.profile?.meetingCapability?.totalMeetingSpaceSqFt ||
      0,
  );
  if (rooms > 0 && rooms <= 150) {
    suggestions.peakRoomsPreset = "26_50";
    suggestions.notes.push("Suggested peak rooms from hotel size (verified property data).");
  } else if (rooms > 150) {
    suggestions.peakRoomsPreset = "51_100";
  }
  if (meetingSqFt > 0) {
    suggestions.meetingSpaceNeed = "ROOMS_AND_MEETINGS";
  }

  let configCp = hotelCtx.commercialPriorities || null;
  if (!configCp) {
    try {
      const p = path.join(
        ROOT,
        "config/group-demand-intelligence/hotels",
        `${hotelId}.json`,
      );
      if (fs.existsSync(p)) {
        configCp = JSON.parse(fs.readFileSync(p, "utf8")).commercialPriorities || null;
      }
    } catch {
      configCp = null;
    }
  }

  if (configCp) {
    if (configCp.weekendFocus === true) {
      suggestions.dayPreference = "WEEKENDS";
      suggestions.notes.push("Suggested weekend focus from hotel commercial config.");
    } else if (configCp.weekendFocus === false) {
      suggestions.dayPreference = "WEEKDAYS";
    }
    const min = Number(configCp.coreTargetPeakRoomsMin || 0);
    const max = Number(configCp.coreTargetPeakRoomsMax || 0);
    if (min >= 101 || max >= 200) suggestions.peakRoomsPreset = "101_200";
    else if (min >= 51 || max >= 100) suggestions.peakRoomsPreset = "51_100";
    else if (min >= 26 || max >= 50) suggestions.peakRoomsPreset = "26_50";

    const segSet = new Set();
    for (const label of configCp.targetSegments || []) {
      for (const hint of CONFIG_SEGMENT_HINTS) {
        if (hint.pattern.test(String(label))) segSet.add(hint.id);
      }
    }
    suggestions.prioritySegments = [...segSet];

    const audSet = new Set();
    const audiencePool = [
      ...(configCp.demandAnchorFocus || []),
      ...(configCp.targetSegments || []),
    ];
    for (const label of audiencePool) {
      for (const hint of CONFIG_AUDIENCE_HINTS) {
        if (hint.pattern.test(String(label))) audSet.add(hint.id);
      }
    }
    if (audSet.size) suggestions.demandGeneratorCategories = [...audSet];
  }

  // Do not invent organizations — only categories from market context
  if (
    !suggestions.demandGeneratorCategories.length &&
    /boca|university|hospital/i.test(JSON.stringify(hotelCtx.demandAnchors || []))
  ) {
    suggestions.demandGeneratorCategories = ["CORPORATE_OFFICES"];
  }
  suggestions.inventoryAccountHints = (inventory || [])
    .slice(0, 12)
    .map((o) => ({
      organizationName: o.organizationName || o.companyName || o.title,
      opportunityId: o.id,
      provenance: DATA_PROVENANCE.SUGGESTED_FROM_RESEARCH,
    }))
    .filter((x) => x.organizationName);
  return suggestions;
}
