/**
 * Universal ADP → GDI hotel onboarding roster.
 *
 * ADP membership grants PROFILE onboarding eligibility only.
 * Research, publication, and weekly activation remain explicit permissions.
 *
 * Roster membership is derived from live ADP operational hygiene + existing
 * GDI hotels — never from manually typed hotel-name lists.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  listCanonicalAdp31PropertyIds,
  listFullAdpReportExtrasBeyond31,
  listOperationalAdpPropertyIds,
} from "../ai-demand-positioning/governance/adp-operational-registry-hygiene-v1.js";
import { getCensusRecordIdForAdpProperty } from "../ai-demand-positioning/census-link-registry.js";
import { loadPropertyProfile } from "../ai-demand-positioning/data-model.js";
import {
  REVIEWED_PROSPECT_HOTEL_IDS,
  evaluateReviewedProspectProductionListPreflight,
  isReviewedProspectHotelEnabled,
  parseReviewedProspectHotelAllowlist,
} from "./reviewed-prospect-publication-v1.js";
import { resolveHotelShareToken } from "./research/scaling-companies-full-e2e-publication-v1.js";
import {
  hotelConfigPathForId,
  resolveHotelGdiProfileForResearch,
} from "./hotel-gdi-profile-resolution-v1.js";

export const ADP_GDI_UNIVERSAL_HOTEL_ROSTER_V1 =
  "gdi-adp-universal-hotel-roster-v1";

/** Explicit commercial / spending permission states (independent). */
export const GDI_HOTEL_AUTH_STATE = Object.freeze({
  PROFILE_READY: "PROFILE_READY",
  RESEARCH_AUTHORIZED: "RESEARCH_AUTHORIZED",
  PUBLICATION_AUTHORIZED: "PUBLICATION_AUTHORIZED",
  WEEKLY_ACTIVE: "WEEKLY_ACTIVE",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const HOTEL_CONFIG_DIR = path.join(ROOT, "config/group-demand-intelligence/hotels");
const HOTEL_DATA_DIR = path.join(ROOT, "data/group-demand-intelligence/hotels");

function text(v) {
  return String(v || "").trim();
}

function truthy(raw) {
  const v = String(raw || "0").trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

function parseCsvIds(raw) {
  return String(raw || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function listConfigHotelIds() {
  if (!fs.existsSync(HOTEL_CONFIG_DIR)) return [];
  return fs
    .readdirSync(HOTEL_CONFIG_DIR)
    .filter((f) => f.endsWith(".json"))
    .map((f) => f.replace(/\.json$/i, ""))
    .filter((id) => /^rec[a-zA-Z0-9]{14}$/.test(id));
}

function listDataHotelIds() {
  if (!fs.existsSync(HOTEL_DATA_DIR)) return [];
  return fs
    .readdirSync(HOTEL_DATA_DIR, { withFileTypes: true })
    .filter((d) => d.isDirectory() && /^rec[a-zA-Z0-9]{14}$/.test(d.name))
    .map((d) => d.name);
}

/**
 * Live commissioned ADP portfolio with canonical HPC IDs.
 * Deduplicates by hotelId; skips synthetic/test properties via hygiene loader.
 */
export function loadCanonicalAdpCommissionedRoster() {
  const canonical31 = new Set(listCanonicalAdp31PropertyIds());
  const extras = new Set(listFullAdpReportExtrasBeyond31());
  const operational = listOperationalAdpPropertyIds();
  const byHotelId = new Map();

  for (const propertyId of operational) {
    const hotelId = getCensusRecordIdForAdpProperty(propertyId) || null;
    const profile = loadPropertyProfile(propertyId) || {};
    const hotelName =
      text(profile.name) ||
      text(profile.displayName) ||
      text(profile.hotelName) ||
      propertyId;
    const cohort = extras.has(propertyId)
      ? "FULL_ADP_E2E_EXTRA"
      : canonical31.has(propertyId)
        ? "ORIGINAL_31"
        : "OTHER_OPERATIONAL";
    const row = {
      propertyId,
      hotelId,
      hotelName,
      cohort,
      identityOk: Boolean(hotelId && /^rec[a-zA-Z0-9]{14}$/.test(hotelId)),
      source: "listOperationalAdpPropertyIds",
    };
    if (!row.hotelId) {
      byHotelId.set(`missing:${propertyId}`, row);
      continue;
    }
    const prev = byHotelId.get(row.hotelId);
    if (!prev) {
      byHotelId.set(row.hotelId, row);
    } else {
      prev.duplicatePropertyIds = [
        ...(prev.duplicatePropertyIds || []),
        propertyId,
      ];
    }
  }

  const rows = [...byHotelId.values()].sort((a, b) =>
    String(a.hotelName).localeCompare(String(b.hotelName))
  );
  return {
    version: ADP_GDI_UNIVERSAL_HOTEL_ROSTER_V1,
    commissionedCount: operational.length,
    resolvedHotelIdCount: rows.filter((r) => r.identityOk).length,
    missingHpcCount: rows.filter((r) => !r.identityOk).length,
    rows,
  };
}

/**
 * Hotels already present in GDI (config, data, or publication allowlist),
 * including non-ADP hotels that must remain authorized.
 */
export function loadExistingGdiHotelUniverse(env = process.env) {
  const configIds = listConfigHotelIds();
  const dataIds = listDataHotelIds();
  const builtIn = [...REVIEWED_PROSPECT_HOTEL_IDS];
  const envAllow = parseReviewedProspectHotelAllowlist(env);
  const all = new Set([...configIds, ...dataIds, ...builtIn, ...envAllow]);
  return {
    configIds,
    dataIds,
    builtInAllowlistIds: builtIn,
    envAllowlistIds: envAllow,
    hotelIds: [...all].sort(),
  };
}

/**
 * Union roster: every commissioned ADP hotel + every existing GDI hotel.
 * ADP membership ⇒ onboarding-eligible. Publication still separate.
 */
export function buildUniversalGdiOnboardingRoster(env = process.env) {
  const adp = loadCanonicalAdpCommissionedRoster();
  const gdi = loadExistingGdiHotelUniverse(env);
  const adpByHotel = new Map(
    adp.rows.filter((r) => r.identityOk).map((r) => [r.hotelId, r])
  );
  const hotelIds = new Set([
    ...adp.rows.filter((r) => r.identityOk).map((r) => r.hotelId),
    ...gdi.hotelIds,
  ]);

  const rows = [...hotelIds].sort().map((hotelId) => {
    const adpRow = adpByHotel.get(hotelId) || null;
    const hasConfig = fs.existsSync(hotelConfigPathForId(hotelId));
    const hasDataDir = fs.existsSync(path.join(HOTEL_DATA_DIR, hotelId));
    return {
      hotelId,
      hotelName: adpRow?.hotelName || null,
      adpPropertyId: adpRow?.propertyId || null,
      adpCohort: adpRow?.cohort || null,
      inAdpCommissioned: Boolean(adpRow),
      inGdiConfig: hasConfig,
      inGdiData: hasDataDir,
      inLocalBuiltInPublicationAllowlist: REVIEWED_PROSPECT_HOTEL_IDS.includes(hotelId),
      inEnvPublicationAllowlist: gdi.envAllowlistIds.includes(hotelId),
      gdiOnlyNotInAdp: !adpRow,
    };
  });

  return {
    version: ADP_GDI_UNIVERSAL_HOTEL_ROSTER_V1,
    adpCommissionedCount: adp.commissionedCount,
    rosterHotelCount: rows.length,
    gdiOnlyCount: rows.filter((r) => r.gdiOnlyNotInAdp).length,
    rows,
    adp,
    gdi,
  };
}

/**
 * Independent authorization assessment.
 * ADP membership never implies RESEARCH / PUBLICATION / WEEKLY.
 */
export function assessHotelAuthorizationStates(
  hotelId,
  {
    env = process.env,
    run = {},
    profileResolution = null,
  } = {}
) {
  const id = text(hotelId);
  const roster = buildUniversalGdiOnboardingRoster(env);
  const row = roster.rows.find((r) => r.hotelId === id) || null;
  const inRoster = Boolean(row);
  const inAdp = row?.inAdpCommissioned === true;

  const profileOk =
    profileResolution?.ok === true ||
    (profileResolution == null &&
      (row?.inGdiConfig === true || inAdp === true));

  const researchAuthorized =
    run.authorizePaidResearch === true &&
    Number(run.budgetCeilingUsd || 0) > 0;

  const publicationAuthorized = isReviewedProspectHotelEnabled(id, env) === true;

  const weeklyFlagOn = truthy(env.GDI_WEEKLY_DEEP_RESEARCH_ENABLED);
  const weeklyAllow = parseCsvIds(env.GDI_WEEKLY_ACTIVE_HOTEL_IDS);
  const weeklyActive =
    weeklyFlagOn &&
    weeklyAllow.length > 0 &&
    weeklyAllow.includes(id);

  const states = {
    [GDI_HOTEL_AUTH_STATE.PROFILE_READY]: inRoster && profileOk,
    [GDI_HOTEL_AUTH_STATE.RESEARCH_AUTHORIZED]: researchAuthorized,
    [GDI_HOTEL_AUTH_STATE.PUBLICATION_AUTHORIZED]: publicationAuthorized,
    [GDI_HOTEL_AUTH_STATE.WEEKLY_ACTIVE]: weeklyActive,
  };

  return {
    version: ADP_GDI_UNIVERSAL_HOTEL_ROSTER_V1,
    hotelId: id,
    inUniversalRoster: inRoster,
    inAdpCommissioned: inAdp,
    gdiOnlyNotInAdp: row?.gdiOnlyNotInAdp === true,
    states,
    notes: [
      inAdp
        ? "ADP commissioned membership enables profile onboarding only."
        : row?.gdiOnlyNotInAdp
          ? "Existing GDI hotel preserved though not in current ADP commissioned roster."
          : inRoster
            ? "On universal roster."
            : "Hotel not on ADP commissioned roster or existing GDI universe.",
      "PUBLICATION_AUTHORIZED requires reviewed-prospect allowlist — not ADP membership.",
      "RESEARCH_AUTHORIZED requires explicit run authorizePaidResearch + budget.",
      "WEEKLY_ACTIVE requires GDI_WEEKLY_DEEP_RESEARCH_ENABLED=1 and hotel on GDI_WEEKLY_ACTIVE_HOTEL_IDS.",
    ],
  };
}

/**
 * Preview / preflight readiness for PowerShell runs.
 * Surfaces avoidable blockers before paid Full E2E.
 */
export function assessGdiHotelPreviewReadiness(
  hotelId,
  {
    env = process.env,
    run = {},
    profileResolution = null,
    railwayEnv = null,
  } = {}
) {
  const id = text(hotelId);
  const auth = assessHotelAuthorizationStates(id, {
    env,
    run,
    profileResolution,
  });
  const share = resolveHotelShareToken(id);
  const localPub = evaluateReviewedProspectProductionListPreflight(id, {
    env,
    requireExplicitEnvAllowlist: false,
    targetEnvironmentLabel: "local",
  });
  const explicitPub = evaluateReviewedProspectProductionListPreflight(id, {
    env,
    requireExplicitEnvAllowlist: true,
    targetEnvironmentLabel: "local process env",
  });
  const railwayPub = railwayEnv
    ? evaluateReviewedProspectProductionListPreflight(id, {
        env: railwayEnv,
        requireExplicitEnvAllowlist: true,
        targetEnvironmentLabel: "Railway production",
      })
    : {
        ok: false,
        blockerCode: "RAILWAY_ENV_NOT_PROBED",
        requiredAction:
          "Probe Railway GDI_REVIEWED_PROSPECT_HOTEL_IDS (preserve existing IDs) before claiming production publication readiness.",
        note: "Local allowlist ≠ Railway production allowlist.",
      };

  const missing = [];
  if (!auth.inUniversalRoster) {
    missing.push({
      code: "HOTEL_NOT_ON_UNIVERSAL_ADP_GDI_ROSTER",
      action:
        "Confirm canonical HPC hotelId and ADP commissioned membership or existing GDI profile.",
    });
  }
  if (!auth.states.PROFILE_READY) {
    missing.push({
      code: "PROFILE_NOT_READY",
      action:
        "Resolve hotel via resolveHotelGdiProfileForResearch (HPC/ADP inventory). Persist synthesized config if needed.",
    });
  }
  if (run.authorizePaidResearch && !auth.states.RESEARCH_AUTHORIZED) {
    missing.push({
      code: "RESEARCH_NOT_AUTHORIZED",
      action: "Pass -AuthorizePaidResearch with -BudgetUSD > 0.",
    });
  }
  if (run.authorizePublication && !auth.states.PUBLICATION_AUTHORIZED) {
    missing.push({
      code: "PUBLICATION_NOT_AUTHORIZED",
      action:
        localPub.requiredAction ||
        "Append hotel to reviewed-prospect publication allowlist after separate founder GO.",
    });
  }
  if (run.authorizePublication && !explicitPub.ok) {
    missing.push({
      code: explicitPub.blockerCode || "EXPLICIT_PUBLICATION_ALLOWLIST_REQUIRED",
      action: explicitPub.requiredAction,
    });
  }
  if (run.probeLiveShare && !share?.token) {
    missing.push({
      code: "GDI_SHARE_TOKEN_UNAVAILABLE",
      action:
        "Issue a hotel-specific production GDI share via the established issuer (do not invent tokens).",
    });
  }
  if (run.authorizePublication && !railwayPub.ok) {
    missing.push({
      code: railwayPub.blockerCode || "RAILWAY_PUBLICATION_ALLOWLIST_MISSING_HOTEL",
      action: railwayPub.requiredAction,
    });
  }

  return {
    version: ADP_GDI_UNIVERSAL_HOTEL_ROSTER_V1,
    hotelId: id,
    auth,
    share: {
      ready: Boolean(share?.token),
      tokenId: share?.tokenId || null,
      source: share?.source || null,
    },
    publication: {
      local: localPub,
      explicitEnv: explicitPub,
      railway: railwayPub,
    },
    missingPrerequisites: missing,
    previewOkForPaidExecution: missing.length === 0,
    note: "Preview must report these blockers before any paid Full E2E spend.",
  };
}

/**
 * Assess profile readiness for one hotel (async; may hit Airtable HPC).
 */
export async function assessHotelProfileReadiness(hotelId, opts = {}) {
  const resolved = await resolveHotelGdiProfileForResearch(hotelId, {
    persistSynthesized: opts.persistSynthesized === true,
  });
  const hotel = resolved.hotel || null;
  const rooms =
    hotel?.capabilityProfile?.totalGuestrooms ??
    hotel?.totalGuestrooms ??
    null;
  const meetings =
    hotel?.capabilityProfile?.meetingRoomsIndoor ??
    hotel?.capabilityProfile?.meetingRoomCount ??
    hotel?.meetingRoomsIndoor ??
    null;
  const city = hotel?.city || hotel?.locale?.city || null;
  const country = hotel?.country || hotel?.locale?.country || null;
  const missingInventory = [];
  if (rooms == null || Number(rooms) <= 0) missingInventory.push("rooms");
  if (meetings == null || Number(meetings) <= 0) {
    missingInventory.push("meeting_rooms");
  }
  return {
    hotelId: text(hotelId),
    ok: resolved.ok === true,
    displayName: hotel?.displayName || null,
    city,
    country,
    rooms,
    meetings,
    missingInventory,
    synthesized: resolved.resolution?.synthesized === true,
    persistedConfigPath: resolved.resolution?.persistedConfigPath || null,
    blockers: resolved.resolution?.blockers || [],
    gdiOnboarding: hotel?.gdiOnboarding || null,
    resolution: resolved.resolution || null,
  };
}

/**
 * Propose publication-allowlist additions for ADP hotels not yet authorized.
 * Does NOT mutate env or Railway — founder GO required separately.
 */
export function proposePublicationAllowlistManifest({
  env = process.env,
  railwayAllowlistIds = null,
} = {}) {
  const roster = buildUniversalGdiOnboardingRoster(env);
  const localCurrent = parseReviewedProspectHotelAllowlist(env);
  const localEffective =
    localCurrent.length > 0 ? localCurrent : [...REVIEWED_PROSPECT_HOTEL_IDS];
  const railwayCurrent = Array.isArray(railwayAllowlistIds)
    ? railwayAllowlistIds
    : null;

  const adpRows = roster.rows.filter((r) => r.inAdpCommissioned);
  const adpHotelIds = adpRows.map((r) => r.hotelId);
  const nameById = new Map(
    adpRows.map((r) => [r.hotelId, r.hotelName || r.hotelId])
  );
  const named = (ids) =>
    (ids || []).map((id) => ({
      hotelId: id,
      hotelName: nameById.get(id) || id,
    }));

  const missingFromLocalBuiltIn = adpHotelIds.filter(
    (id) => !REVIEWED_PROSPECT_HOTEL_IDS.includes(id)
  );
  const missingFromLocalEnv = adpHotelIds.filter(
    (id) => !localEffective.includes(id)
  );
  const missingFromRailway = railwayCurrent
    ? adpHotelIds.filter((id) => !railwayCurrent.includes(id))
    : null;
  const railwayMissingVsLocal =
    railwayCurrent != null
      ? localEffective.filter((id) => !railwayCurrent.includes(id))
      : null;

  return {
    version: ADP_GDI_UNIVERSAL_HOTEL_ROSTER_V1,
    preserveExisting: true,
    autoApplyForbidden: true,
    note:
      "Founder must separately approve publication allowlist expansion. ADP membership alone does not authorize publication.",
    localBuiltIn: {
      current: [...REVIEWED_PROSPECT_HOTEL_IDS],
      currentNamed: named(REVIEWED_PROSPECT_HOTEL_IDS),
      proposedAppend: missingFromLocalBuiltIn,
      proposedAppendNamed: named(missingFromLocalBuiltIn),
      afterIfApproved: [
        ...REVIEWED_PROSPECT_HOTEL_IDS,
        ...missingFromLocalBuiltIn,
      ],
    },
    localEnv: {
      current: localEffective,
      proposedAppend: missingFromLocalEnv,
      proposedAppendNamed: named(missingFromLocalEnv),
    },
    railway: railwayCurrent
      ? {
          current: railwayCurrent,
          proposedAppend: missingFromRailway,
          proposedAppendNamed: named(missingFromRailway),
          afterIfApproved: [...railwayCurrent, ...missingFromRailway],
          keepFlag: "GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1",
          parityGapVsLocalAllowlist: railwayMissingVsLocal,
          parityGapVsLocalAllowlistNamed: named(railwayMissingVsLocal),
          proposedRailwayCsvIfApproved: [
            ...railwayCurrent,
            ...missingFromRailway,
          ].join(","),
        }
      : {
          current: null,
          proposedAppend: null,
          note: "Railway allowlist not probed in this pass.",
        },
  };
}
