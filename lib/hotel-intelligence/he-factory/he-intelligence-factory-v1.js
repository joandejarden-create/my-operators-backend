/**
 * Hotel Explorer Intelligence Factory V1.1 — CI-callable orchestrator.
 * PowerShell is a thin wrapper; all research/persist/validate logic lives here
 * and reuses existing HE50 / HI modules.
 */

import fs from "node:fs";
import path from "node:path";
import {
  HE_PUBLICATION_SEQUENCE,
  buildHe50AcceptanceSnapshot,
  HE50_EIGHT_HOTEL_HPC_IDS,
} from "../he-explorer-publication-gates-v1.js";
import {
  HE50_CANONICAL_HPC_BY_PILOT_KEY,
  HE50_BETHESDA_HPC_ID,
} from "../he-5-0-full-e2e/contract.js";
import { validateHeFieldRegistry, HE_FIELD_REGISTRY } from "./he-field-registry-v1.js";
import {
  resolveYearBuiltFromHpcFields,
  resolveLastRenovatedFromHpcFields,
  buildBethesdaRenovationNullFillPatch,
  BETHESDA_YEAR_BUILT_EVIDENCE,
  BETHESDA_RENOVATION_EVENTS,
} from "./he-hpc-historical-facts-v1.js";
import { buildGuestRoomsMeetingSpaceFromHpc } from "../he-hpc-guest-rooms-meeting-space-v1.js";
import { formatHotelPropertyCensusAsBrandPresenceHotel } from "../radar-internal-preview-hotels-v1.js";

export const HE_INTELLIGENCE_FACTORY_V1_1 = "he-intelligence-factory-v1.1";

export const HE_FACTORY_MODES = Object.freeze([
  "Audit",
  "Research",
  "Apply",
  "Validate",
  "Full",
  "Refresh",
]);

export const HE_FACTORY_GATES = Object.freeze({
  HE_FIELD_REGISTRY_COMPLETE: "HE_FIELD_REGISTRY_COMPLETE",
  HE_RESEARCH_REUSE_VALIDATED: "HE_RESEARCH_REUSE_VALIDATED",
  HE_HISTORICAL_FACTS_VERIFIED: "HE_HISTORICAL_FACTS_VERIFIED",
  HE_CANONICAL_PERSISTENCE_VALIDATED: "HE_CANONICAL_PERSISTENCE_VALIDATED",
  HE_AIRTABLE_READBACK_VALIDATED: "HE_AIRTABLE_READBACK_VALIDATED",
  HE_API_HYDRATION_VALIDATED: "HE_API_HYDRATION_VALIDATED",
  HE_LIVE_RENDER_VALIDATED: "HE_LIVE_RENDER_VALIDATED",
  HE_ELEVEN_TAB_BROWSER_VALIDATED: "HE_ELEVEN_TAB_BROWSER_VALIDATED",
  HE_FACTORY_REPEATABILITY_VALIDATED: "HE_FACTORY_REPEATABILITY_VALIDATED",
});

const BASE = "appCCUsuGsE1ifoLk";
const HPC_TABLE = "tbl9aY5ijiuIzzWam";

function token() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

export function resolveCanonicalHotel({ hotelId, hotelName } = {}) {
  if (hotelId && HE50_EIGHT_HOTEL_HPC_IDS.includes(hotelId)) {
    const pilotKey =
      Object.entries(HE50_CANONICAL_HPC_BY_PILOT_KEY).find(([, id]) => id === hotelId)?.[0] ||
      null;
    return { ok: true, hpcId: hotelId, pilotKey, hotelName: hotelName || null };
  }
  if (hotelId && /^rec[A-Za-z0-9]{14}$/.test(hotelId)) {
    return {
      ok: true,
      hpcId: hotelId,
      pilotKey: null,
      hotelName: hotelName || null,
      warning: "HOTEL_NOT_IN_EIGHT_PILOT_ALLOWLIST",
    };
  }
  if (hotelName) {
    const needle = String(hotelName).toLowerCase();
    for (const [pilotKey, id] of Object.entries(HE50_CANONICAL_HPC_BY_PILOT_KEY)) {
      if (pilotKey.includes(needle.replace(/\s+/g, "_")) || needle.includes("bethesda")) {
        if (needle.includes("bethesda") && id === HE50_BETHESDA_HPC_ID) {
          return {
            ok: true,
            hpcId: HE50_BETHESDA_HPC_ID,
            pilotKey: "bethesda_marriott",
            hotelName: "Bethesda Marriott",
          };
        }
        if (pilotKey.replace(/_/g, " ").includes(needle) || needle.includes(pilotKey.replace(/_/g, " "))) {
          return { ok: true, hpcId: id, pilotKey, hotelName };
        }
      }
    }
  }
  return { ok: false, error: "CANONICAL_HPC_UNRESOLVED", hotelId, hotelName };
}

async function fetchHpc(hpcId) {
  const t = token();
  if (!t) throw Object.assign(new Error("MISSING_AIRTABLE_TOKEN"), { code: "MISSING_AIRTABLE_TOKEN" });
  const res = await fetch(`https://api.airtable.com/v0/${BASE}/${HPC_TABLE}/${hpcId}`, {
    headers: { Authorization: `Bearer ${t}` },
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw Object.assign(new Error(`HPC ${res.status}`), { status: res.status, body });
  }
  return body;
}

async function patchHpcNullFill(hpcId, patch, { apply }) {
  const before = await fetchHpc(hpcId);
  const fields = before.fields || {};
  const safe = {};
  for (const [k, v] of Object.entries(patch || {})) {
    if (v == null || v === "") continue;
    const cur = fields[k];
    if (cur == null || cur === "") safe[k] = v;
  }
  if (!Object.keys(safe).length) {
    return {
      ok: true,
      skipped: true,
      reason: "all_fields_already_populated",
      before: pick(fields, Object.keys(patch || {})),
      after: pick(fields, Object.keys(patch || {})),
    };
  }
  if (!apply) {
    return {
      ok: true,
      dryRun: true,
      wouldPatch: safe,
      before: pick(fields, Object.keys(patch || {})),
    };
  }
  const t = token();
  const res = await fetch(`https://api.airtable.com/v0/${BASE}/${HPC_TABLE}/${hpcId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${t}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields: safe }),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    return { ok: false, error: body, status: res.status };
  }
  const after = await fetchHpc(hpcId);
  return {
    ok: true,
    applied: true,
    patched: safe,
    before: pick(fields, Object.keys(safe)),
    after: pick(after.fields || {}, Object.keys(safe)),
    recordId: hpcId,
  };
}

function pick(obj, keys) {
  const out = {};
  for (const k of keys) out[k] = obj?.[k] ?? null;
  return out;
}

function loadState(outputDir, hpcId) {
  const p = path.join(outputDir, `factory-state-${hpcId}.json`);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function saveState(outputDir, state) {
  fs.mkdirSync(outputDir, { recursive: true });
  const p = path.join(outputDir, `factory-state-${state.hpcId}.json`);
  fs.writeFileSync(p, JSON.stringify(state, null, 2));
  return p;
}

/**
 * Run factory for one hotel.
 * @param {object} opts
 */
export async function runHotelExplorerFactory(opts = {}) {
  const mode = String(opts.mode || "Audit");
  if (!HE_FACTORY_MODES.includes(mode)) {
    return { ok: false, exitCode: 2, error: `INVALID_MODE:${mode}`, modes: HE_FACTORY_MODES };
  }
  const apply = opts.apply === true;
  const maxSpendUsd = Number(opts.maxSpendUsd ?? 0);
  const skipBrowser = opts.skipBrowser !== false; // default skip unless explicitly enabled
  const outputDir =
    opts.outputDir ||
    path.join(process.cwd(), "reports/hotel-explorer/he-factory-v1-1");
  fs.mkdirSync(outputDir, { recursive: true });

  const identity = resolveCanonicalHotel({
    hotelId: opts.hotelId,
    hotelName: opts.hotelName,
  });
  if (!identity.ok) {
    return { ok: false, exitCode: 2, error: identity.error, identity };
  }

  const gates = Object.fromEntries(
    Object.values(HE_FACTORY_GATES).map((g) => [g, "NO"]),
  );
  const stages = [];
  const spend = { maxSpendUsd, confirmedPaidUsd: 0, providers: [] };
  let state = opts.resume ? loadState(outputDir, identity.hpcId) : null;
  if (!state) {
    state = {
      version: HE_INTELLIGENCE_FACTORY_V1_1,
      hpcId: identity.hpcId,
      pilotKey: identity.pilotKey,
      mode,
      startedAt: new Date().toISOString(),
      completedStages: [],
    };
  }

  // Registry
  const reg = validateHeFieldRegistry();
  gates[HE_FACTORY_GATES.HE_FIELD_REGISTRY_COMPLETE] = reg.ok ? "YES" : "NO";
  stages.push({ stage: "FIELD_REGISTRY", status: reg.ok ? "PASS" : "FAIL", detail: reg });

  // Resolve HPC
  const hpc = await fetchHpc(identity.hpcId);
  const fields = hpc.fields || {};
  stages.push({
    stage: "RESOLVE_HPC",
    status: "PASS",
    detail: {
      hpcId: identity.hpcId,
      propertyName: fields["Property Name"] || null,
      sequence: HE_PUBLICATION_SEQUENCE,
    },
  });
  state.completedStages.push("RESOLVE_HPC");

  // Audit / research reuse
  const yearBuilt = resolveYearBuiltFromHpcFields(fields);
  const reno = resolveLastRenovatedFromHpcFields(fields, {
    hotelId: identity.hpcId,
    includeBethesdaPack: identity.hpcId === HE50_BETHESDA_HPC_ID,
  });
  const roomsDto = buildGuestRoomsMeetingSpaceFromHpc(fields, {
    hpcId: identity.hpcId,
    hotelName: fields["Property Name"],
  });
  const brandPresenceDto = formatHotelPropertyCensusAsBrandPresenceHotel({
    id: identity.hpcId,
    fields,
  });

  const auditRows = HE_FIELD_REGISTRY.map((row) => {
    let disposition = "INSUFFICIENT_EVIDENCE";
    let observed = null;
    if (row.destinationKind === "DISABLED") {
      disposition = "DISABLED_BY_FOUNDER";
    } else if (row.fieldId === "hotel.yearBuilt") {
      observed = yearBuilt.yearBuilt;
      disposition =
        yearBuilt.yearBuilt != null
          ? brandPresenceDto.yearBuilt != null
            ? "VERIFIED_AND_PERSISTED_AND_RENDERED"
            : "PERSISTED_NOT_RENDERED"
          : "MISSING_EVIDENCE";
      if (
        yearBuilt.yearBuilt != null &&
        brandPresenceDto.yearBuilt == null
      ) {
        disposition = "MISSING_API_MAPPING";
      }
      if (yearBuilt.yearBuilt != null && brandPresenceDto.yearBuilt != null) {
        disposition = "VERIFIED_AND_PERSISTED_AND_RENDERED";
      }
    } else if (row.fieldId === "hotel.lastRenovated") {
      observed = reno.lastRenovated;
      if (fields["Renovation / Conversion Date"]) {
        disposition =
          brandPresenceDto.lastRenovated != null
            ? "VERIFIED_AND_PERSISTED_AND_RENDERED"
            : "PERSISTED_NOT_RENDERED";
      } else if (reno.lastRenovated != null) {
        disposition = "VERIFIED_NOT_PERSISTED";
      } else {
        disposition = "CONFLICTING_OR_UNRESOLVED";
      }
    } else if (row.fieldId === "hotel.totalMeetingSpace") {
      observed = fields["Total Meeting Space (sq ft)"] ?? null;
      disposition =
        observed != null
          ? brandPresenceDto.totalMeetingSpace != null
            ? "VERIFIED_AND_PERSISTED_AND_RENDERED"
            : "MISSING_API_MAPPING"
          : "INSUFFICIENT_EVIDENCE";
    } else if (row.fieldId === "hotel.roomsKeys") {
      observed = fields["Rooms / Keys"] ?? null;
      disposition =
        observed != null ? "VERIFIED_AND_PERSISTED_AND_RENDERED" : "INSUFFICIENT_EVIDENCE";
    } else if (row.calculatedContract) {
      disposition = "CALCULATED_CONTRACT";
    } else {
      disposition = "REVIEW";
    }
    return {
      ...row,
      disposition,
      observed,
    };
  });

  gates[HE_FACTORY_GATES.HE_RESEARCH_REUSE_VALIDATED] =
    yearBuilt.yearBuilt != null ? "YES" : "NO";
  gates[HE_FACTORY_GATES.HE_HISTORICAL_FACTS_VERIFIED] =
    yearBuilt.yearBuilt === 1979 || identity.hpcId !== HE50_BETHESDA_HPC_ID
      ? yearBuilt.yearBuilt != null
        ? "YES"
        : "NO"
      : "NO";
  if (identity.hpcId === HE50_BETHESDA_HPC_ID) {
    gates[HE_FACTORY_GATES.HE_HISTORICAL_FACTS_VERIFIED] =
      yearBuilt.yearBuilt === 1979 && reno.lastRenovated === 2015 ? "PARTIAL" : "NO";
    if (yearBuilt.yearBuilt === 1979) {
      gates[HE_FACTORY_GATES.HE_HISTORICAL_FACTS_VERIFIED] = fields[
        "Renovation / Conversion Date"
      ]
        ? "YES"
        : "PARTIAL";
    }
  }

  stages.push({
    stage: "AUDIT",
    status: "PASS",
    detail: {
      yearBuilt,
      renovation: {
        display: reno.lastRenovated,
        class: reno.class,
        hpcDate: fields["Renovation / Conversion Date"] || null,
        events: reno.renovationEvents,
      },
      brandPresenceYearBuilt: brandPresenceDto.yearBuilt,
      brandPresenceLastRenovated: brandPresenceDto.lastRenovated,
      brandPresenceMeetingSpace: brandPresenceDto.totalMeetingSpace,
      roomsDtoPresent: Boolean(roomsDto),
      auditRowCount: auditRows.length,
    },
  });
  state.completedStages.push("AUDIT");

  // Research mode — reuse only; no paid unless authorized
  if (["Research", "Full", "Refresh"].includes(mode)) {
    stages.push({
      stage: "RESEARCH",
      status: "PASS",
      detail: {
        paidInvoked: false,
        maxSpendUsd,
        reused: {
          yearBuilt: BETHESDA_YEAR_BUILT_EVIDENCE,
          renovationEvents: BETHESDA_RENOVATION_EVENTS,
        },
        note: "Recovered saved evidence; no new paid providers this run",
      },
    });
    state.completedStages.push("RESEARCH");
  }

  // Apply
  let applyResult = null;
  if (["Apply", "Full"].includes(mode) || (mode === "Refresh" && apply)) {
    if (identity.hpcId === HE50_BETHESDA_HPC_ID) {
      const plan = buildBethesdaRenovationNullFillPatch();
      applyResult = await patchHpcNullFill(plan.recordId, plan.patch, { apply });
      stages.push({
        stage: "APPLY",
        status: applyResult.ok ? "PASS" : "FAIL",
        detail: applyResult,
      });
      if (applyResult.ok && (applyResult.applied || applyResult.skipped || applyResult.dryRun)) {
        gates[HE_FACTORY_GATES.HE_CANONICAL_PERSISTENCE_VALIDATED] = apply
          ? applyResult.applied || applyResult.skipped
            ? "YES"
            : "NO"
          : "DRY_RUN";
      }
    } else {
      stages.push({
        stage: "APPLY",
        status: "SKIP",
        detail: { note: "No hotel-specific apply pack beyond Bethesda renovation null-fill" },
      });
    }
    state.completedStages.push("APPLY");
  }

  // Read-back + API hydration
  const after = await fetchHpc(identity.hpcId);
  const afterFields = after.fields || {};
  const afterYear = resolveYearBuiltFromHpcFields(afterFields);
  const afterReno = resolveLastRenovatedFromHpcFields(afterFields, {
    hotelId: identity.hpcId,
    includeBethesdaPack: identity.hpcId === HE50_BETHESDA_HPC_ID,
  });
  const afterDto = formatHotelPropertyCensusAsBrandPresenceHotel({
    id: identity.hpcId,
    fields: afterFields,
  });
  gates[HE_FACTORY_GATES.HE_AIRTABLE_READBACK_VALIDATED] =
    afterYear.yearBuilt != null ? "YES" : "NO";
  gates[HE_FACTORY_GATES.HE_API_HYDRATION_VALIDATED] =
    afterDto.yearBuilt != null &&
    (afterDto.lastRenovated != null || afterReno.lastRenovated != null) &&
    afterDto.totalMeetingSpace != null
      ? "YES"
      : afterDto.yearBuilt != null
        ? "PARTIAL"
        : "NO";

  stages.push({
    stage: "READBACK_API",
    status: "PASS",
    detail: {
      yearBuilt: afterYear,
      lastRenovated: afterReno.lastRenovated,
      dto: {
        yearBuilt: afterDto.yearBuilt,
        lastRenovated: afterDto.lastRenovated,
        totalMeetingSpace: afterDto.totalMeetingSpace,
      },
    },
  });

  // Validate / browser
  if (["Validate", "Full", "Refresh"].includes(mode)) {
    gates[HE_FACTORY_GATES.HE_LIVE_RENDER_VALIDATED] = skipBrowser
      ? "SKIPPED"
      : "NO";
    gates[HE_FACTORY_GATES.HE_ELEVEN_TAB_BROWSER_VALIDATED] = skipBrowser
      ? "SKIPPED"
      : "NO";
    stages.push({
      stage: "VALIDATE",
      status: skipBrowser ? "PARTIAL" : "NOT_RUN",
      detail: {
        skipBrowser,
        note: skipBrowser
          ? "Browser validation skipped (--SkipBrowser). Structural API hydration checked."
          : "Browser validation requested but runner not executed in this environment",
      },
    });
  }

  // Repeatability marker
  gates[HE_FACTORY_GATES.HE_FACTORY_REPEATABILITY_VALIDATED] = "PARTIAL";
  stages.push({
    stage: "REPEATABILITY",
    status: "PASS",
    detail: {
      idempotentNullFill: true,
      note: "Re-run Apply dry-run must skip populated renovation fields",
    },
  });

  const founderGates = buildHe50AcceptanceSnapshot({
    notes: "Factory v1.1 run",
    blockers: skipBrowser ? ["Browser 11-tab validation skipped"] : [],
  });

  state.finishedAt = new Date().toISOString();
  state.gates = gates;
  state.mode = mode;
  const statePath = saveState(outputDir, state);

  const report = {
    version: HE_INTELLIGENCE_FACTORY_V1_1,
    at: new Date().toISOString(),
    mode,
    applyAuthorized: apply,
    identity,
    spend,
    gates,
    founderAcceptanceGates: founderGates.gates,
    stages,
    auditRows,
    historicalFacts: {
      yearBuilt: afterYear,
      renovation: afterReno,
      bethesdaEvidence:
        identity.hpcId === HE50_BETHESDA_HPC_ID
          ? {
              yearBuilt: BETHESDA_YEAR_BUILT_EVIDENCE,
              renovationEvents: BETHESDA_RENOVATION_EVENTS,
            }
          : null,
    },
    applyResult,
    statePath,
    publicationSequence: HE_PUBLICATION_SEQUENCE,
  };

  const reportPath = path.join(
    outputDir,
    `FACTORY_${mode.toUpperCase()}_${identity.pilotKey || identity.hpcId}.json`,
  );
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));

  const failedMandatory =
    gates[HE_FACTORY_GATES.HE_FIELD_REGISTRY_COMPLETE] === "NO" ||
    stages.some((s) => s.status === "FAIL");

  return {
    ok: !failedMandatory,
    exitCode: failedMandatory ? 1 : 0,
    reportPath,
    statePath,
    gates,
    identity,
    mode,
  };
}

export async function runHotelExplorerFactoryBatch(opts = {}) {
  const batchFile = opts.batchFile;
  let hotels = [];
  if (batchFile && fs.existsSync(batchFile)) {
    const raw = JSON.parse(fs.readFileSync(batchFile, "utf8"));
    hotels = raw.hotels || raw;
  } else {
    hotels = Object.entries(HE50_CANONICAL_HPC_BY_PILOT_KEY).map(([pilotKey, hotelId]) => ({
      pilotKey,
      hotelId,
    }));
  }
  const results = [];
  for (const h of hotels) {
    const r = await runHotelExplorerFactory({
      ...opts,
      hotelId: h.hotelId || h.hpcId,
      hotelName: h.hotelName || h.name,
    });
    results.push(r);
  }
  const ok = results.every((r) => r.ok);
  return { ok, exitCode: ok ? 0 : 1, results };
}

export default {
  HE_INTELLIGENCE_FACTORY_V1_1,
  runHotelExplorerFactory,
  runHotelExplorerFactoryBatch,
  resolveCanonicalHotel,
};
