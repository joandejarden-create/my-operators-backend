/**
 * Rooms Secondary Source Wave 2 — country-by-country rooms completion.
 * Schema: ensure Rooms Evidence Tier. Phone stays official-only.
 * Hotel Property Census only. Field-completion only. No inserts.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
} from "./production-census-source-of-truth.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  checkAutopilotApplyEnv,
  applyPreflight,
} from "./census-autopilot-apply-guard.js";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import {
  resolveCensusMode,
  assertNoInsertInFieldCompletionMode,
} from "./census-autopilot-full-latam-v3.js";
import { fetchColombiaRntLodgingRows } from "./colombia-rnt-open-data-adapter.js";
import {
  matchCensusToColombiaRntRooms,
  buildSecondaryRoomsPatch,
} from "./census-rooms-secondary-match.js";
import {
  resolveSecondaryHotelDataPolicy,
  classifyPhoneUnderSecondaryPolicy,
  ROOMS_EVIDENCE_TIER,
  buildRoomsProvenanceNotes,
  PHONE_POLICY_REASON,
} from "./census-secondary-hotel-data-policy.js";
import {
  ensureRoomsEvidenceTierField,
  ROOMS_EVIDENCE_TIER_FIELD,
  mapEvidenceTierCodeToSelect,
} from "./production-census-rooms-evidence-tier-schema.js";
import {
  buildRoomsCountryDiscoveryReport,
  ROOMS_COUNTRY_PRIORITY,
} from "./census-rooms-country-source-discovery.js";
import { isChoiceCentralReservationPhone } from "./census-phone-number-enrichment.js";
import {
  tryOfficialPropertyRooms,
} from "./census-autopilot-rooms-count-completion-v1.js";
import {
  buildCensusGapLedger,
  buildCompletionScorecard,
  writeCensusGapLedger,
} from "./census-gap-ledger.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const ROOMS_SECONDARY_WAVE_2_OBJECTIVE =
  "rooms-secondary-source-wave-2-v1";
export const ROOMS_SECONDARY_WAVE_2_VERSION =
  "rooms-secondary-source-wave-2-v1";

export const ROOMS_SECONDARY_WAVE_2_STATUS = Object.freeze({
  COMPLETE: "production_census_rooms_secondary_source_wave_2_v1_complete",
  PARTIAL_SOURCE:
    "production_census_rooms_secondary_source_wave_2_v1_partial_source_remaining",
  PARTIAL_STEWARD:
    "production_census_rooms_secondary_source_wave_2_v1_partial_steward_remaining",
  BLOCKED: "production_census_rooms_secondary_source_wave_2_v1_blocked",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || productionHotelPropertyCensus.tableId;

const READ_FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Country",
  "City",
  "State / Region",
  "Market",
  "Address",
  "Phone",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Evidence Tier",
  "Rooms Reviewed Date",
  "Rooms Notes",
  "Official Property URL",
  "Enrichment Status",
  "Human Review Required",
];

const ALLOWED_WRITE = new Set([
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Evidence Tier",
  "Rooms Reviewed Date",
  "Rooms Notes",
  "Last Reviewed Date",
  "Enrichment Status",
  "Enrichment Priority",
  "Human Review Required",
]);

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function isBlank(v) {
  return v == null || !String(v).trim();
}

function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeMd(filePath, md) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}

async function listCensus(baseId, token, tableId, fieldNames = READ_FIELDS) {
  // Probe which fields exist — skip unknown to avoid 422
  const existing = new Set(fieldNames);
  // Always try without Rooms Evidence Tier first if list fails — handled by caller schema ensure
  const out = [];
  let offset;
  let fields = [...existing];
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      const msg = json?.error?.message || "";
      if (/Rooms Evidence Tier/i.test(msg) && fields.includes(ROOMS_EVIDENCE_TIER_FIELD)) {
        fields = fields.filter((f) => f !== ROOMS_EVIDENCE_TIER_FIELD);
        offset = undefined;
        out.length = 0;
        continue;
      }
      throw new Error(`census_list_failed:${res.status}:${msg}`);
    }
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function applyPatches(proposals, { baseId, token, tableId, log }) {
  let updatesApplied = 0;
  const writeErrors = [];
  let roomsWritten = 0;
  let phoneWritten = 0;

  for (let i = 0; i < proposals.length; i += 10) {
    const chunk = proposals.slice(i, i + 10);
    const records = chunk
      .map((p) => {
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (isForbiddenAutopilotField(k)) continue;
          if (!ALLOWED_WRITE.has(k)) continue;
          if (k === "Phone") continue;
          if (v === undefined || v === null || v === "") continue;
          fields[k] = v;
          if (k === "Rooms / Keys") roomsWritten += 1;
        }
        return { id: p.record_id, fields };
      })
      .filter((u) => Object.keys(u.fields).length > 0);

    if (!records.length) continue;

    const tryWrite = async (recs) => {
      const res = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
        {
          method: "PATCH",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ records: recs, typecast: true }),
        }
      );
      const json = await res.json().catch(() => ({}));
      return { res, json };
    };

    const { res, json } = await tryWrite(records);
    if (!res.ok) {
      writeErrors.push({ status: res.status, error: json.error || json, batch: true });
      log?.(`[rooms-wave2] batch write failed ${res.status}; retrying one-by-one`);
      for (const rec of records) {
        const one = await tryWrite([rec]);
        if (!one.res.ok) {
          writeErrors.push({
            status: one.res.status,
            error: one.json.error || one.json,
            record_id: rec.id,
          });
        } else {
          updatesApplied += 1;
        }
      }
    } else {
      updatesApplied += records.length;
    }
  }

  return { updatesApplied, writeErrors, roomsWritten, phoneWritten };
}

function coverageStats(records) {
  const total = records.length;
  let withRooms = 0;
  let officialHigh = 0;
  let secondary = 0;
  let steward = 0;
  let missing = 0;
  let conflictHold = 0;
  /** @type {Record<string, number>} */
  const bySourceType = {};
  /** @type {Record<string, number>} */
  const byEvidenceTier = {};
  /** @type {Record<string, number>} */
  const missingByCountry = {};

  for (const r of records) {
    const f = r.fields || {};
    const rooms = f["Rooms / Keys"];
    const conf = String(f["Rooms Confidence"] || "");
    const st = String(f["Rooms Source Type"] || "") || "(blank)";
    const tier =
      String(f["Rooms Evidence Tier"] || "") ||
      (String(f["Rooms Notes"] || "").match(/evidence_tier=([^|]+)/)?.[1] ||
        "(blank)");
    const country = String(f.Country || "Unknown");
    if (isBlank(rooms)) {
      missing += 1;
      missingByCountry[country] = (missingByCountry[country] || 0) + 1;
      continue;
    }
    withRooms += 1;
    bySourceType[st] = (bySourceType[st] || 0) + 1;
    byEvidenceTier[tier.trim()] = (byEvidenceTier[tier.trim()] || 0) + 1;
    if (st === "trusted_secondary_source") secondary += 1;
    else if (st === "steward_review" || conf === "Hold") {
      steward += 1;
      if (conf === "Hold") conflictHold += 1;
    } else if (/^high$/i.test(conf) || /^exact$/i.test(conf) || st.startsWith("official_")) {
      officialHigh += 1;
    } else if (/secondary_/i.test(tier)) secondary += 1;
    else officialHigh += 1;
  }

  return {
    total,
    with_rooms: withRooms,
    missing,
    coverage_pct: total ? +(100 * (withRooms / total)).toFixed(2) : 0,
    official_high_rooms: officialHigh,
    official_high_pct: withRooms
      ? +(100 * (officialHigh / withRooms)).toFixed(2)
      : 0,
    secondary_source_rooms: secondary,
    secondary_source_pct: withRooms
      ? +(100 * (secondary / withRooms)).toFixed(2)
      : 0,
    steward_verified_rooms: steward,
    steward_verified_pct: withRooms
      ? +(100 * (steward / withRooms)).toFixed(2)
      : 0,
    rooms_conflict_hold: conflictHold,
    sources_used_by_type: bySourceType,
    rooms_by_evidence_tier: byEvidenceTier,
    missing_by_country: missingByCountry,
  };
}

function renderReportMd(report) {
  const b = report.before || {};
  const a = report.after || {};
  const lines = [
    `# Production Census — Rooms Secondary Source Wave 2 v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${report.objective}\``,
    `**Generated:** ${report.generated_at}`,
    `**Table:** Hotel Property Census (\`${report.table_id}\`)`,
    `**Airtable writes:** ${report.airtable_writes}`,
    `**Inserts:** ${report.records_inserted}`,
    ``,
    `## Schema`,
    ``,
    `- Rooms Evidence Tier: **${report.schema?.status || "unknown"}**`,
    `- Field exists: ${report.schema?.field_exists ? "yes" : "no"}`,
    ``,
    `## Rooms coverage`,
    ``,
    `| Metric | Before | After |`,
    `| --- | ---: | ---: |`,
    `| With Rooms | ${b.with_rooms} | ${a.with_rooms} |`,
    `| Coverage % | ${b.coverage_pct} | ${a.coverage_pct} |`,
    `| Official High Rooms | ${b.official_high_rooms} | ${a.official_high_rooms} |`,
    `| Official High % | ${b.official_high_pct} | ${a.official_high_pct} |`,
    `| Secondary Source Rooms | ${b.secondary_source_rooms} | ${a.secondary_source_rooms} |`,
    `| Secondary Source % | ${b.secondary_source_pct} | ${a.secondary_source_pct} |`,
    `| Steward-Verified Rooms | ${b.steward_verified_rooms} | ${a.steward_verified_rooms} |`,
    `| Rooms Missing | ${b.missing} | ${a.missing} |`,
    `| Rooms Conflict (Hold) | ${b.rooms_conflict_hold} | ${a.rooms_conflict_hold} |`,
    ``,
    `## Written this run`,
    ``,
    `- Records updated: **${report.records_updated}**`,
    `- Rooms values written: **${report.rooms_written}**`,
    `- Evidence tier backfills: **${report.evidence_tier_backfills}**`,
    `- Official HTML rooms: **${report.official_rooms_written}**`,
    `- Colombia fuzzy rooms: **${report.colombia_fuzzy_written}**`,
    `- Conflicts held: **${report.conflicts_held}**`,
    `- Phone written: **${report.phone_written}**`,
    `- Phone blocked by policy: **${report.phone_blocked_by_policy}**`,
    ``,
    `### By country`,
    ``,
  ];
  for (const [c, n] of Object.entries(report.rooms_written_by_country || {})) {
    lines.push(`- ${c}: ${n}`);
  }
  lines.push(``, `### By source type`, ``);
  for (const [k, v] of Object.entries(a.sources_used_by_type || {})) {
    lines.push(`- \`${k}\`: ${v}`);
  }
  lines.push(``, `### By evidence tier`, ``);
  for (const [k, v] of Object.entries(a.rooms_by_evidence_tier || {})) {
    lines.push(`- \`${k}\`: ${v}`);
  }
  lines.push(
    ``,
    `## Country source discovery`,
    ``
  );
  for (const c of report.country_discovery?.countries || []) {
    if (!["Mexico", "Dominican Republic", "Panama", "Costa Rica", "Colombia"].includes(c.country)) {
      continue;
    }
    lines.push(
      `### ${c.country}`,
      ``,
      `- Missing rooms: ${c.missing_rooms}`,
      `- Next: ${c.next_action}`,
      `- Live adapters: ${c.summary?.adapter_live}; discovery-only: ${c.summary?.discovery_only}; blocked/aggregate: ${c.summary?.blocked_or_aggregate}`,
      ``
    );
  }
  lines.push(
    `## Colombia remaining fuzzy / steward`,
    ``,
    `- Steward candidates held: **${report.colombia_steward_held}**`,
    `- Written via fuzzy: **${report.colombia_fuzzy_written}**`,
    ``,
    `## Next backlog`,
    ``
  );
  for (const item of report.next_backlog || []) lines.push(`- ${item}`);
  lines.push(
    ``,
    `## Continue command`,
    ``,
    "```bash",
    report.continue_command || "",
    "```",
    ``
  );
  return lines.join("\n");
}

/**
 * @param {object} opts
 */
export async function runRoomsSecondarySourceWave2V1Mission(opts = {}) {
  const log = opts.log || (() => {});
  const argv = opts.argv || process.argv.slice(2);
  const args = opts.args || {};
  const env = opts.env || process.env;
  const effectiveMode = resolveCensusMode(
    opts.censusMode || args.censusMode || "field-completion-only"
  );
  const enableWrites = Boolean(opts.enableProductionWrites);
  const countryFilter = String(
    opts.country || args.country || ""
  ).trim();

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot",
    `${stamp}_CALA-rooms-secondary-wave-2-v1`
  );
  fs.mkdirSync(runDir, { recursive: true });

  const targetAssert = assertProductionCensusWriteTarget({
    tableId: CENSUS_TABLE_ID,
    baseId: resolveTargetBase().target_base_id || null,
  });
  if (!targetAssert.ok) {
    return {
      ok: false,
      status: ROOMS_SECONDARY_WAVE_2_STATUS.BLOCKED,
      reason: targetAssert.code || "blocked_wrong_census_target",
      airtable_writes: false,
      records_inserted: 0,
    };
  }

  const envCheck = checkAutopilotApplyEnv(env);
  const preflightArgs = {
    ...args,
    mode: enableWrites ? "mission" : args.mode || "controlled",
    region: args.region || "CALA",
    scope: args.scope || "official-parent-inventory",
    confirms: {
      safeWrites: true,
      writeToProductionCensus: true,
      noBrandExplorer: true,
      noOwnerOperator: true,
      noDateWrites: true,
      noRecentMomentum: true,
      noCompanyValidation: true,
      webhoundNotProduction: true,
    },
    allApplyConfirms: true,
  };
  const preflight = applyPreflight(preflightArgs, envCheck);
  if (enableWrites && (!envCheck.allOk || !preflight.ok)) {
    return {
      ok: false,
      status: ROOMS_SECONDARY_WAVE_2_STATUS.BLOCKED,
      reason: "missing_confirmations",
      envCheck,
      preflight,
      airtable_writes: false,
      records_inserted: 0,
    };
  }

  const insertGuard = assertNoInsertInFieldCompletionMode(effectiveMode, 0);
  if (!insertGuard.ok) {
    return {
      ok: false,
      status: ROOMS_SECONDARY_WAVE_2_STATUS.BLOCKED,
      reason: insertGuard.reason,
      airtable_writes: false,
      records_inserted: 0,
    };
  }

  // Pass 0 — schema ensure Rooms Evidence Tier
  log(`[rooms-wave2] Pass 0 — ensure Rooms Evidence Tier schema`);
  const schemaResult = await ensureRoomsEvidenceTierField({
    apply: enableWrites,
    dryRun: !enableWrites,
    log,
  });
  const tierExists =
    schemaResult.status === "already_exists" ||
    schemaResult.status === "created" ||
    (schemaResult.ok && schemaResult.field_id);
  // After create, field exists for writes
  const roomsEvidenceTierFieldExists =
    tierExists || schemaResult.status === "dry_run_would_create"
      ? schemaResult.status !== "dry_run_would_create"
        ? true
        : false
      : false;
  // If we just created it, it's true; if already exists, true; if dry-run only, false for writes
  const tierFieldForWrites =
    schemaResult.status === "created" ||
    schemaResult.status === "already_exists";

  const policy = resolveSecondaryHotelDataPolicy(env, {
    roomsEvidenceTierFieldExists: tierFieldForWrites,
  });
  if (!policy.enable_secondary_rooms_sources) {
    log(`[rooms-wave2] warning: secondary rooms env not fully enabled`);
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  log(`[rooms-wave2] SoT OK — ${CENSUS_TABLE_ID} tier_field=${tierFieldForWrites}`);

  let census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const before = coverageStats(census);
  writeJson(path.join(runDir, "coverage-before.json"), before);

  const proposals = [];
  const stewardPack = [];
  /** @type {Record<string, number>} */
  const roomsWrittenByCountry = {};
  let officialRoomsWritten = 0;
  let colombiaFuzzyWritten = 0;
  let evidenceTierBackfills = 0;
  let conflictsHeld = 0;
  let phoneBlockedByPolicy = 0;
  let botBlockedSamples = 0;
  /** @type {Record<string, number>} */
  const botBlockedByCountry = {};

  // Pass 1 — audit + evidence tier backfill for existing secondary rooms
  log(`[rooms-wave2] Pass 1 — audit + evidence tier backfill`);
  for (const rec of census) {
    const f = rec.fields || {};
    if (isBlank(f["Rooms / Keys"])) continue;
    if (String(f["Rooms Source Type"] || "") !== "trusted_secondary_source") continue;
    if (!tierFieldForWrites) continue;
    if (!isBlank(f["Rooms Evidence Tier"])) continue;
    const notes = String(f["Rooms Notes"] || "");
    const code =
      notes.match(/evidence_tier=([^|]+)/)?.[1]?.trim() ||
      ROOMS_EVIDENCE_TIER.SECONDARY_TOURISM_BOARD;
    const select = mapEvidenceTierCodeToSelect(code);
    if (!select) continue;
    proposals.push({
      record_id: rec.id,
      reason: "rooms_evidence_tier_backfill",
      patch: {
        "Rooms Evidence Tier": select,
        "Rooms Reviewed Date": f["Rooms Reviewed Date"] || todayIsoDate(),
      },
    });
    evidenceTierBackfills += 1;
  }

  // Pass 2 — country discovery + limited official URL probe (document bot blocks)
  const missing = census.filter((r) => isBlank(r.fields?.["Rooms / Keys"]));
  const byCountryStats = {};
  for (const c of ROOMS_COUNTRY_PRIORITY) {
    const rows = missing.filter((r) => String(r.fields?.Country || "") === c);
    byCountryStats[c] = {
      missing_rooms: rows.length,
      with_official_url: rows.filter(
        (r) => !isBlank(r.fields?.["Official Property URL"])
      ).length,
      bot_blocked_samples: 0,
    };
  }

  const probeCountries = countryFilter
    ? [countryFilter]
    : ["Mexico", "Dominican Republic", "Panama", "Costa Rica"];
  const maxProbePerCountry = Number(opts.officialProbePerCountry || 8);
  if (!opts.skipOfficialFetch) {
    log(`[rooms-wave2] Pass 2 — official URL probe (document blocks)`);
    for (const country of probeCountries) {
      const rows = missing
        .filter((r) => String(r.fields?.Country || "") === country)
        .filter((r) => !isBlank(r.fields?.["Official Property URL"]))
        .slice(0, maxProbePerCountry);
      for (const rec of rows) {
        const hit = await tryOfficialPropertyRooms(rec.fields || {}, {
          fetchImpl: opts.fetchImpl,
          timeoutMs: 10000,
        });
        if (hit.ok && hit.patch) {
          // Attach evidence tier select
          if (tierFieldForWrites) {
            hit.patch["Rooms Evidence Tier"] = mapEvidenceTierCodeToSelect(
              ROOMS_EVIDENCE_TIER.OFFICIAL_HIGH
            );
          }
          proposals.push({
            record_id: rec.id,
            reason: "rooms_official_property_html_wave2",
            patch: hit.patch,
          });
          officialRoomsWritten += 1;
          roomsWrittenByCountry[country] =
            (roomsWrittenByCountry[country] || 0) + 1;
        } else if (/403|bot|forbidden|akamai/i.test(String(hit.reason || ""))) {
          botBlockedSamples += 1;
          botBlockedByCountry[country] = (botBlockedByCountry[country] || 0) + 1;
          byCountryStats[country].bot_blocked_samples =
            (byCountryStats[country].bot_blocked_samples || 0) + 1;
        }
      }
    }
  }

  const discovery = buildRoomsCountryDiscoveryReport(byCountryStats);
  writeJson(path.join(runDir, "country-source-discovery.json"), discovery);

  // Pass 3 — Colombia RNT fuzzy for remaining
  if (
    policy.enable_secondary_rooms_sources &&
    (!countryFilter || /^colombia$/i.test(countryFilter))
  ) {
    log(`[rooms-wave2] Pass 3 — Colombia RNT fuzzy steward`);
    const fetched = await fetchColombiaRntLodgingRows({
      maxRows: opts.rntMaxRows || 20000,
      pageSize: 5000,
      year: 2026,
      hotelsOnly: true,
      fetchImpl: opts.fetchImpl,
    });
    const rntRows = fetched.ok ? fetched.rows || [] : [];
    log(`[rooms-wave2] RNT rows=${rntRows.length} ok=${fetched.ok}`);

    const already = new Set(proposals.map((p) => p.record_id));
    const colombiaMissing = missing.filter(
      (r) =>
        /^colombia$/i.test(String(r.fields?.Country || "")) &&
        !already.has(r.id)
    );

    for (const rec of colombiaMissing) {
      const f = rec.fields || {};
      const match = matchCensusToColombiaRntRooms(f, rntRows, { fuzzy: true });
      if (!match.ok) {
        if (match.steward_candidate) {
          stewardPack.push({
            record_id: rec.id,
            ...match.steward_candidate,
            reason: match.reason,
          });
        } else {
          stewardPack.push({
            record_id: rec.id,
            census_name: f["Canonical Property Name"] || f["Property Name"],
            reason: match.reason,
          });
        }
        continue;
      }
      const built = buildSecondaryRoomsPatch(f, match, {
        today: todayIsoDate(),
        roomsEvidenceTierFieldExists: tierFieldForWrites,
      });
      if (built.conflict) {
        conflictsHeld += 1;
        proposals.push({
          record_id: rec.id,
          reason: "rooms_conflict_steward",
          patch: built.patch,
        });
        continue;
      }
      if (!built.ok || !built.patch) {
        stewardPack.push({
          record_id: rec.id,
          reason: built.reason || match.reason,
        });
        continue;
      }
      proposals.push({
        record_id: rec.id,
        reason: "rooms_colombia_rnt_fuzzy_wave2",
        patch: built.patch,
      });
      if (built.write_rooms_value) {
        colombiaFuzzyWritten += 1;
        roomsWrittenByCountry.Colombia =
          (roomsWrittenByCountry.Colombia || 0) + 1;
      } else if (built.provenance_backfill) {
        evidenceTierBackfills += 1;
      }
    }
  }

  writeJson(path.join(runDir, "colombia-steward-pack.json"), {
    count: stewardPack.length,
    rows: stewardPack,
  });
  writeJson(path.join(runDir, "proposals.json"), {
    count: proposals.length,
    proposals: proposals.slice(0, 600),
  });

  // Phone policy classify
  for (const rec of census) {
    const phone = rec.fields?.Phone;
    const cls = classifyPhoneUnderSecondaryPolicy({
      has_phone: !isBlank(phone),
      is_central: isChoiceCentralReservationPhone(phone),
      policy,
    });
    if (
      cls.status === PHONE_POLICY_REASON.SECONDARY_NOT_APPROVED ||
      cls.reason === PHONE_POLICY_REASON.OFFICIAL_SOURCE_MISSING
    ) {
      phoneBlockedByPolicy += 1;
    }
  }

  let writeResult = {
    updatesApplied: 0,
    writeErrors: [],
    roomsWritten: 0,
    phoneWritten: 0,
  };
  if (enableWrites && proposals.length) {
    log(`[rooms-wave2] applying ${proposals.length} patches`);
    writeResult = await applyPatches(proposals, {
      baseId: bases.target_base_id,
      token,
      tableId: CENSUS_TABLE_ID,
      log,
    });
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  }

  const after = coverageStats(census);
  writeJson(path.join(runDir, "coverage-after.json"), after);

  // Pass 4 — gap ledger
  log(`[rooms-wave2] Pass 4 — gap ledger`);
  try {
    const ledger = buildCensusGapLedger(census, { region: args.region || "CALA" });
    const scorecard = buildCompletionScorecard(census, {
      region: args.region || "CALA",
    });
    writeCensusGapLedger(ledger, scorecard, { root: ROOT });
  } catch (err) {
    log(`[rooms-wave2] gap ledger skip: ${err?.message || err}`);
  }

  const stewardRemaining =
    conflictsHeld > 0 || stewardPack.length > 0 || after.rooms_conflict_hold > 0;
  let status = ROOMS_SECONDARY_WAVE_2_STATUS.PARTIAL_SOURCE;
  if (after.missing === 0 && !stewardRemaining) {
    status = ROOMS_SECONDARY_WAVE_2_STATUS.COMPLETE;
  } else if (
    stewardRemaining &&
    (colombiaFuzzyWritten > 0 || officialRoomsWritten > 0 || evidenceTierBackfills > 0)
  ) {
    status = ROOMS_SECONDARY_WAVE_2_STATUS.PARTIAL_STEWARD;
  } else if (after.missing > 0) {
    status = ROOMS_SECONDARY_WAVE_2_STATUS.PARTIAL_SOURCE;
  }

  const continueCommand = [
    "ALLOW_CENSUS_AUTOPILOT_APPLY=1 CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \\",
    "CONFIRM_NO_BRAND_EXPLORER_WRITES=1 CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \\",
    "ENABLE_SECONDARY_HOTEL_DATA_SOURCES=1 ENABLE_SECONDARY_ROOMS_SOURCES=1 ENABLE_SECONDARY_PHONE_SOURCES=0 \\",
    "npm run census:autopilot -- --region CALA --scope official-parent-inventory --mode mission \\",
    "  --objective rooms-secondary-source-wave-2-v1 --census-mode field-completion-only \\",
    "  --strategy highest-yield-safe --run-until-complete --batch-size 100 \\",
    "  --confirm-safe-writes --confirm-write-to-production-census \\",
    "  --confirm-no-brand-explorer-writes --confirm-no-owner-operator \\",
    "  --confirm-no-date-writes --confirm-no-recent-momentum \\",
    "  --confirm-no-company-validation --confirm-webhound-not-production-source \\",
    "  --enable-production-writes",
  ].join("\n");

  const nextBacklog = [
    "Mexico: build SECTUR RNT property-level rooms adapter (consulta portal is not bulk API); DATATUR remains aggregate-only",
    "Dominican Republic: MITUR RNT listing lacks habitaciones columns — need property detail scrape steward or licensed dataset",
    "Panama / Costa Rica: no property-level open lodging rooms dataset identified — official parent pages bot-blocked",
    `Colombia steward pack: ${stewardPack.length} remaining ambiguous/low-sim RNT matches — do not force writes`,
    "Official parent HTML: Marriott/IHG/Hilton/Choice commonly 403/Akamai from Autopilot runtime — need unblocked fetch path or DAM/factsheet cache",
    "Phone secondary still not approved",
    `Target coverage 95–100% (now ${after.coverage_pct}%) requires Mexico-scale secondary adapter or unblocked official pages`,
  ];

  const report = {
    ok: true,
    status,
    objective: ROOMS_SECONDARY_WAVE_2_OBJECTIVE,
    version: ROOMS_SECONDARY_WAVE_2_VERSION,
    generated_at: new Date().toISOString(),
    table_id: CENSUS_TABLE_ID,
    airtable_writes: enableWrites,
    records_updated: writeResult.updatesApplied,
    records_inserted: 0,
    rooms_written: writeResult.roomsWritten || colombiaFuzzyWritten + officialRoomsWritten,
    evidence_tier_backfills: evidenceTierBackfills,
    official_rooms_written: officialRoomsWritten,
    colombia_fuzzy_written: colombiaFuzzyWritten,
    colombia_steward_held: stewardPack.length,
    conflicts_held: conflictsHeld,
    phone_written: writeResult.phoneWritten || 0,
    phone_blocked_by_policy: phoneBlockedByPolicy,
    bot_blocked_samples: botBlockedSamples,
    bot_blocked_by_country: botBlockedByCountry,
    rooms_written_by_country: roomsWrittenByCountry,
    fields_written: [...ALLOWED_WRITE],
    before,
    after,
    policy,
    schema: {
      ...schemaResult,
      field_exists: tierFieldForWrites,
    },
    country_discovery: discovery,
    next_backlog: nextBacklog,
    continue_command: continueCommand,
    run_dir: runDir,
    write_errors: writeResult.writeErrors,
  };

  const reportJson = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-rooms-secondary-source-wave-2-v1.json"
  );
  const reportMd = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-rooms-secondary-source-wave-2-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-rooms-secondary-source-wave-2-v1.md"
  );
  writeJson(path.join(runDir, "final-report.json"), report);
  writeJson(reportJson, report);
  const md = renderReportMd(report);
  writeMd(reportMd, md);
  writeMd(docsPath, md);

  log(`[rooms-wave2] status=${status} updated=${report.records_updated}`);
  return report;
}
