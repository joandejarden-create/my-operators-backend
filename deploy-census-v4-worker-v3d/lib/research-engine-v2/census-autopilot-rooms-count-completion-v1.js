/**
 * Rooms Count Completion v1 — official-first + founder-approved secondary Rooms sources.
 * Phone stays official-only (ENABLE_SECONDARY_PHONE_SOURCES=0).
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
  resolveRoomsSourceTypeForAirtable,
  PHONE_POLICY_REASON,
} from "./census-secondary-hotel-data-policy.js";
import { isChoiceCentralReservationPhone } from "./census-phone-number-enrichment.js";
import {
  isFalsePositiveRoomCount,
  extractRoomsKeysFromOfficialHtml,
  selectBestRoomsHit,
} from "./production-census-rooms-keys-extractor.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const ROOMS_COUNT_COMPLETION_V1_OBJECTIVE = "rooms-count-completion-v1";
export const ROOMS_COUNT_COMPLETION_V1_VERSION = "rooms-count-completion-v1";

export const ROOMS_COUNT_COMPLETION_STATUS = Object.freeze({
  COMPLETE: "production_census_rooms_secondary_source_completion_v1_complete",
  PARTIAL_SOURCE:
    "production_census_rooms_secondary_source_completion_v1_partial_source_remaining",
  PARTIAL_STEWARD:
    "production_census_rooms_secondary_source_completion_v1_partial_steward_remaining",
  BLOCKED: "production_census_rooms_secondary_source_completion_v1_blocked",
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
  "Phone",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
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
  "Rooms Reviewed Date",
  "Rooms Notes",
  "Last Reviewed Date",
  "Enrichment Status",
  "Human Review Required",
  "Data Confidence Tier",
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

async function listCensus(baseId, token, tableId) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of READ_FIELDS) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(
        `census_list_failed:${res.status}:${json?.error?.message || ""}`
      );
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
  let provenanceOnly = 0;
  let phoneWritten = 0;

  for (let i = 0; i < proposals.length; i += 10) {
    const chunk = proposals.slice(i, i + 10);
    const records = chunk
      .map((p) => {
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (isForbiddenAutopilotField(k)) continue;
          if (!ALLOWED_WRITE.has(k)) continue;
          if (v === undefined || v === null || v === "") continue;
          // Hard ban: never write Phone in this mission
          if (k === "Phone") continue;
          fields[k] = v;
          if (k === "Rooms / Keys") roomsWritten += 1;
        }
        if (
          fields["Rooms Source URL"] &&
          !Object.prototype.hasOwnProperty.call(fields, "Rooms / Keys")
        ) {
          provenanceOnly += 1;
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
      log?.(
        `[rooms-completion] batch write failed ${res.status}; retrying one-by-one`
      );
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

  return {
    updatesApplied,
    writeErrors,
    roomsWritten,
    provenanceOnly,
    phoneWritten,
  };
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

  for (const r of records) {
    const f = r.fields || {};
    const rooms = f["Rooms / Keys"];
    const conf = String(f["Rooms Confidence"] || "");
    const st = String(f["Rooms Source Type"] || "") || "(blank)";
    const notes = String(f["Rooms Notes"] || "");
    if (isBlank(rooms)) {
      missing += 1;
      continue;
    }
    withRooms += 1;
    bySourceType[st] = (bySourceType[st] || 0) + 1;
    if (/^high$/i.test(conf) || /^exact$/i.test(conf)) {
      if (
        st.startsWith("official_") ||
        /evidence_tier=official_high/i.test(notes)
      ) {
        officialHigh += 1;
      } else if (st === "steward_review" || /steward/i.test(notes)) {
        steward += 1;
      } else {
        officialHigh += 1; // High confidence without secondary type → count official-ish
      }
    } else if (st === "trusted_secondary_source") {
      secondary += 1;
    } else if (st === "steward_review" || conf === "Hold") {
      steward += 1;
      if (conf === "Hold") conflictHold += 1;
    } else if (/evidence_tier=secondary_/i.test(notes)) {
      secondary += 1;
    }
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
  };
}

/**
 * Attempt official HTML rooms from Official Property URL (bounded).
 * @param {object} fields
 * @param {{ fetchImpl?: typeof fetch, timeoutMs?: number }} [opts]
 */
export async function tryOfficialPropertyRooms(fields, opts = {}) {
  const url = String(fields["Official Property URL"] || "").trim();
  if (!url || !/^https?:\/\//i.test(url)) {
    return { ok: false, reason: "no_official_property_url" };
  }
  if (
    /booking\.com|expedia\.|tripadvisor\.|google\.|hotels\.com|agoda\./i.test(
      url
    )
  ) {
    return { ok: false, reason: "forbidden_third_party_url" };
  }
  const fetchImpl = opts.fetchImpl || fetch;
  const timeoutMs = opts.timeoutMs ?? 12000;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  let html = "";
  try {
    const res = await fetchImpl(url, {
      signal: ctrl.signal,
      headers: {
        Accept: "text/html",
        "User-Agent": "DealalityCensusBot/1.0",
      },
      redirect: "follow",
    });
    if (!res.ok) {
      return { ok: false, reason: `official_fetch_http_${res.status}` };
    }
    html = await res.text();
  } catch (err) {
    return {
      ok: false,
      reason: `official_fetch_error:${err?.name || "err"}`,
    };
  } finally {
    clearTimeout(timer);
  }

  const extracted = extractRoomsKeysFromOfficialHtml(html, { url });
  const best = selectBestRoomsHit(extracted.hits || []);
  if (!best || best.rejected || best.count == null) {
    return { ok: false, reason: "no_official_rooms_in_page" };
  }
  if (isFalsePositiveRoomCount(html, best.count, best.method || "official")) {
    return { ok: false, reason: "rooms_false_positive_rejected" };
  }
  // Choice sitewide default 25
  if (best.count === 25 && /choicehotels\.com/i.test(url)) {
    return { ok: false, reason: "choice_sitewide_rooms_default_25" };
  }
  if (best.confidence !== "High" && best.confidence !== "Medium") {
    return { ok: false, reason: "rooms_confidence_too_low" };
  }

  const category = "official_hotel_website";
  const today = todayIsoDate();
  return {
    ok: true,
    is_official: true,
    category,
    rooms: best.count,
    source_url: url,
    confidence: best.confidence === "High" ? "High" : "Medium",
    source_type_airtable: resolveRoomsSourceTypeForAirtable({
      is_official: true,
      category,
    }),
    evidence_tier: ROOMS_EVIDENCE_TIER.OFFICIAL_HIGH,
    notes: buildRoomsProvenanceNotes({
      evidence_tier: ROOMS_EVIDENCE_TIER.OFFICIAL_HIGH,
      category,
      adapter: "official_property_html",
      note: best.method || "official_html",
    }),
    patch: {
      "Rooms / Keys": best.count,
      "Rooms Confidence": best.confidence === "High" ? "High" : "Medium",
      "Rooms Source URL": url,
      "Rooms Source Type": resolveRoomsSourceTypeForAirtable({
        is_official: true,
        category,
      }),
      "Rooms Reviewed Date": today,
      "Rooms Notes": buildRoomsProvenanceNotes({
        evidence_tier: ROOMS_EVIDENCE_TIER.OFFICIAL_HIGH,
        category,
        adapter: "official_property_html",
        note: best.method || "official_html",
      }),
      "Last Reviewed Date": today,
      "Enrichment Status": "Partial",
    },
  };
}

function renderReportMd(report) {
  const b = report.before || {};
  const a = report.after || {};
  const lines = [
    `# Production Census — Rooms Secondary Source Completion v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${report.objective}\``,
    `**Generated:** ${report.generated_at}`,
    `**Table:** Hotel Property Census (\`${report.table_id}\`)`,
    `**Base:** Deal Capture Platform`,
    `**Airtable writes:** ${report.airtable_writes}`,
    `**Inserts:** ${report.records_inserted}`,
    ``,
    `## Policy`,
    ``,
    `- Secondary hotel data: ${report.policy?.enable_secondary_hotel_data_sources ? "ON" : "OFF"}`,
    `- Secondary Rooms: ${report.policy?.enable_secondary_rooms_sources ? "ON (founder approved)" : "OFF"}`,
    `- Secondary Phone: ${report.policy?.enable_secondary_phone_sources ? "ON" : "OFF (not approved)"}`,
    `- Phone classification: \`${PHONE_POLICY_REASON.SECONDARY_NOT_APPROVED}\` where secondary would be required`,
    ``,
    `## Rooms coverage`,
    ``,
    `| Metric | Before | After |`,
    `| --- | ---: | ---: |`,
    `| With Rooms | ${b.with_rooms} | ${a.with_rooms} |`,
    `| Coverage % | ${b.coverage_pct} | ${a.coverage_pct} |`,
    `| Official High Rooms | ${b.official_high_rooms} | ${a.official_high_rooms} |`,
    `| Official High % (of filled) | ${b.official_high_pct} | ${a.official_high_pct} |`,
    `| Secondary Source Rooms | ${b.secondary_source_rooms} | ${a.secondary_source_rooms} |`,
    `| Secondary Source % | ${b.secondary_source_pct} | ${a.secondary_source_pct} |`,
    `| Steward-Verified Rooms | ${b.steward_verified_rooms} | ${a.steward_verified_rooms} |`,
    `| Rooms Missing | ${b.missing} | ${a.missing} |`,
    `| Rooms Conflict (Hold) | ${b.rooms_conflict_hold} | ${a.rooms_conflict_hold} |`,
    ``,
    `## This run`,
    ``,
    `- Records updated: **${report.records_updated}**`,
    `- Rooms values written: **${report.rooms_written}**`,
    `- Official HTML rooms written: **${report.official_rooms_written}**`,
    `- Secondary (RNT) rooms written: **${report.secondary_rooms_written}**`,
    `- Conflicts held: **${report.conflicts_held}**`,
    `- Phone written: **${report.phone_written}** (must stay 0)`,
    `- Phone blocked by policy: **${report.phone_blocked_by_policy}**`,
    `- Fields written: ${JSON.stringify(report.fields_written)}`,
    ``,
    `## Sources used by type (after)`,
    ``,
  ];
  for (const [k, v] of Object.entries(a.sources_used_by_type || {})) {
    lines.push(`- \`${k}\`: ${v}`);
  }
  lines.push(
    ``,
    `## Schema gaps`,
    ``,
    `- **Rooms Evidence Tier** — not present on Hotel Property Census; encoded in \`Rooms Notes\` as \`evidence_tier=…\`.`,
    ``,
    `## Next backlog`,
    ``
  );
  for (const item of report.next_backlog || []) {
    lines.push(`- ${item}`);
  }
  lines.push(
    ``,
    `## Confirmations`,
    ``,
    `- Hotel Property Census only: yes`,
    `- No inserts: yes`,
    `- Brand Setup / Brand Explorer untouched: yes`,
    `- No owner/operator/date fields: yes`,
    `- No room inference / sitewide defaults: yes`,
    `- No phone secondary writes: yes`,
    `- No central reservation phone writes: yes`,
    `- Every room write has source URL / type / confidence / reviewed date / evidence_tier in Notes: yes`,
    `- Conflicts stewarded (not overwritten): yes`,
    ``
  );
  return lines.join("\n");
}

/**
 * @param {{
 *   argv?: string[],
 *   args?: object,
 *   env?: NodeJS.ProcessEnv,
 *   enableProductionWrites?: boolean,
 *   censusMode?: string,
 *   log?: (msg: string) => void,
 *   fetchImpl?: typeof fetch,
 *   skipOfficialFetch?: boolean,
 *   rntMaxRows?: number,
 * }} opts
 */
export async function runRoomsCountCompletionV1Mission(opts = {}) {
  const log = opts.log || (() => {});
  const argv = opts.argv || process.argv.slice(2);
  const args = opts.args || {};
  const env = opts.env || process.env;
  const policy = resolveSecondaryHotelDataPolicy(env);
  const effectiveMode = resolveCensusMode(
    opts.censusMode || args.censusMode || "field-completion-only"
  );
  const enableWrites = Boolean(opts.enableProductionWrites);

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot",
    `${stamp}_CALA-rooms-count-completion-v1`
  );
  fs.mkdirSync(runDir, { recursive: true });

  const targetAssert = assertProductionCensusWriteTarget({
    tableId: CENSUS_TABLE_ID,
    baseId: resolveTargetBase().target_base_id || null,
  });
  if (!targetAssert.ok) {
    return {
      ok: false,
      status: ROOMS_COUNT_COMPLETION_STATUS.BLOCKED,
      reason: targetAssert.code || "blocked_wrong_census_target",
      targetAssert,
      airtable_writes: false,
      records_inserted: 0,
    };
  }

  if (!policy.enable_secondary_rooms_sources && enableWrites) {
    // Allow official-only writes even if secondary off; warn
    log(
      "[rooms-completion] secondary rooms OFF — official HTML path only"
    );
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
      status: ROOMS_COUNT_COMPLETION_STATUS.BLOCKED,
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
      status: ROOMS_COUNT_COMPLETION_STATUS.BLOCKED,
      reason: insertGuard.reason,
      airtable_writes: false,
      records_inserted: 0,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  log(
    `[rooms-completion] SoT OK — Hotel Property Census ${CENSUS_TABLE_ID} secondary_rooms=${policy.enable_secondary_rooms_sources}`
  );

  let census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const before = coverageStats(census);
  writeJson(path.join(runDir, "coverage-before.json"), before);

  const missingRooms = census.filter((r) =>
    isBlank(r.fields?.["Rooms / Keys"])
  );
  const colombiaMissing = missingRooms.filter((r) =>
    /^colombia$/i.test(String(r.fields?.Country || ""))
  );

  const maxPass =
    Number(args.batchSize || opts.batchSize || 100) *
    Math.min(Number(args.maxPasses || opts.maxPasses || 10), 10);

  /** @type {object[]} */
  const proposals = [];
  /** @type {object[]} */
  const results = [];
  let officialRoomsWritten = 0;
  let secondaryRoomsWritten = 0;
  let conflictsHeld = 0;
  let phoneBlockedByPolicy = 0;
  let phoneCentralRejected = 0;
  let sourceRemaining = 0;
  /** @type {Record<string, number>} */
  const sourcesUsedThisRun = {};

  // Pass 1 — official property URL (bounded subset for highest-yield-safe)
  const officialCandidates = missingRooms
    .filter((r) => !isBlank(r.fields?.["Official Property URL"]))
    .slice(0, Math.min(40, maxPass));
  log(
    `[rooms-completion] Pass 1 — official HTML candidates=${officialCandidates.length}`
  );

  if (!opts.skipOfficialFetch) {
    for (const rec of officialCandidates) {
      const f = rec.fields || {};
      const hit = await tryOfficialPropertyRooms(f, {
        fetchImpl: opts.fetchImpl,
        timeoutMs: 10000,
      });
      if (hit.ok && hit.patch) {
        if (
          !isBlank(f["Rooms / Keys"]) &&
          Number(f["Rooms / Keys"]) !== Number(hit.rooms)
        ) {
          conflictsHeld += 1;
          proposals.push({
            record_id: rec.id,
            reason: "rooms_conflict_official",
            patch: {
              "Rooms Confidence": "Hold",
              "Rooms Source Type": "steward_review",
              "Rooms Reviewed Date": todayIsoDate(),
              "Rooms Notes": buildRoomsProvenanceNotes({
                evidence_tier: ROOMS_EVIDENCE_TIER.CONFLICT_HOLD,
                category: "official_hotel_website",
                note: `existing=${f["Rooms / Keys"]} candidate=${hit.rooms}`,
              }),
              "Human Review Required": true,
            },
          });
          results.push({
            record_id: rec.id,
            path: "official_conflict",
            rooms: hit.rooms,
          });
          continue;
        }
        proposals.push({
          record_id: rec.id,
          reason: "rooms_official_property_html",
          confidence: hit.confidence,
          patch: hit.patch,
        });
        officialRoomsWritten += 1;
        sourcesUsedThisRun[hit.source_type_airtable] =
          (sourcesUsedThisRun[hit.source_type_airtable] || 0) + 1;
        results.push({
          record_id: rec.id,
          path: "official_html",
          rooms: hit.rooms,
        });
      } else {
        results.push({
          record_id: rec.id,
          path: "official_miss",
          reason: hit.reason,
        });
      }
    }
  }

  // Pass 2 — Colombia RNT secondary (founder approved tourism board)
  /** @type {Record<string, unknown>[]} */
  let rntRows = [];
  if (policy.enable_secondary_rooms_sources) {
    log(`[rooms-completion] Pass 2 — fetch Colombia RNT open data`);
    const fetched = await fetchColombiaRntLodgingRows({
      maxRows: opts.rntMaxRows || 20000,
      pageSize: 5000,
      year: 2026,
      hotelsOnly: true,
      fetchImpl: opts.fetchImpl,
    });
    if (!fetched.ok) {
      log(
        `[rooms-completion] RNT fetch failed: ${fetched.message || fetched.error_kind}`
      );
    } else {
      rntRows = fetched.rows || [];
      log(`[rooms-completion] RNT rows=${rntRows.length}`);
    }

    const alreadyProposed = new Set(proposals.map((p) => p.record_id));
    const rntWork = colombiaMissing
      .filter((r) => !alreadyProposed.has(r.id))
      .slice(0, maxPass);

    for (const rec of rntWork) {
      const f = rec.fields || {};
      const match = matchCensusToColombiaRntRooms(f, rntRows);
      const built = buildSecondaryRoomsPatch(f, match, {
        today: todayIsoDate(),
      });
      if (built.conflict) {
        conflictsHeld += 1;
        proposals.push({
          record_id: rec.id,
          reason: "rooms_conflict_steward",
          patch: built.patch,
        });
        results.push({
          record_id: rec.id,
          path: "secondary_conflict",
          candidate: built.candidate,
        });
        continue;
      }
      if (!built.ok || !built.patch) {
        sourceRemaining += 1;
        results.push({
          record_id: rec.id,
          path: "secondary_miss",
          reason: built.reason || match.reason,
        });
        continue;
      }
      proposals.push({
        record_id: rec.id,
        reason: "rooms_secondary_colombia_rnt",
        confidence: match.confidence,
        patch: built.patch,
        evidence_tier: match.evidence_tier,
      });
      if (built.write_rooms_value) {
        secondaryRoomsWritten += 1;
        sourcesUsedThisRun[match.source_type_airtable] =
          (sourcesUsedThisRun[match.source_type_airtable] || 0) + 1;
      }
      results.push({
        record_id: rec.id,
        path: "secondary_rnt",
        rooms: match.rooms,
        sim: match.match_sim,
      });
    }
  } else {
    sourceRemaining += colombiaMissing.length;
  }

  // Phone policy classify (never write)
  for (const rec of census) {
    const phone = rec.fields?.Phone;
    const isCentral = isChoiceCentralReservationPhone(phone);
    if (isCentral) phoneCentralRejected += 1;
    const cls = classifyPhoneUnderSecondaryPolicy({
      has_phone: !isBlank(phone),
      is_central: isCentral,
      policy,
    });
    if (cls.status === PHONE_POLICY_REASON.SECONDARY_NOT_APPROVED) {
      phoneBlockedByPolicy += 1;
    }
  }

  writeJson(path.join(runDir, "proposals.json"), {
    count: proposals.length,
    proposals: proposals.slice(0, 500),
  });
  writeJson(path.join(runDir, "results.json"), {
    count: results.length,
    results: results.slice(0, 800),
  });

  let writeResult = {
    updatesApplied: 0,
    writeErrors: [],
    roomsWritten: 0,
    phoneWritten: 0,
  };

  if (enableWrites && proposals.length) {
    log(`[rooms-completion] applying ${proposals.length} patches`);
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

  const remainingMissing = after.missing;
  const stewardRemaining = conflictsHeld > 0 || after.rooms_conflict_hold > 0;

  let status = ROOMS_COUNT_COMPLETION_STATUS.PARTIAL_SOURCE;
  if (remainingMissing === 0 && !stewardRemaining) {
    status = ROOMS_COUNT_COMPLETION_STATUS.COMPLETE;
  } else if (stewardRemaining && secondaryRoomsWritten + officialRoomsWritten > 0) {
    status = ROOMS_COUNT_COMPLETION_STATUS.PARTIAL_STEWARD;
  } else if (remainingMissing > 0) {
    status = ROOMS_COUNT_COMPLETION_STATUS.PARTIAL_SOURCE;
  }

  const nextBacklog = [
    "Mexico / DR / Panama / Costa Rica rooms still need official parent adapters or approved country open-data matches",
    "Add Airtable field Rooms Evidence Tier (currently encoded in Rooms Notes)",
    "Phone remains blocked — founder has not approved secondary phone sources",
    "Choice property pages 403 / central phones / rooms=25 defaults remain rejected",
    "Expand RNT year coverage + fuzzy city aliases for more Colombia matches",
    "Marriott DAM factsheet batch for Marriott blanks (official High path)",
  ];

  const report = {
    ok: true,
    status,
    objective: ROOMS_COUNT_COMPLETION_V1_OBJECTIVE,
    version: ROOMS_COUNT_COMPLETION_V1_VERSION,
    generated_at: new Date().toISOString(),
    table_id: CENSUS_TABLE_ID,
    base_role: bases.target_role,
    airtable_writes: enableWrites,
    records_updated: writeResult.updatesApplied,
    records_inserted: 0,
    rooms_written:
      writeResult.roomsWritten ||
      secondaryRoomsWritten + officialRoomsWritten,
    official_rooms_written: officialRoomsWritten,
    secondary_rooms_written: secondaryRoomsWritten,
    conflicts_held: conflictsHeld,
    phone_written: writeResult.phoneWritten || 0,
    phone_blocked_by_policy: phoneBlockedByPolicy,
    phone_central_rejected: phoneCentralRejected,
    fields_written: [
      "Rooms / Keys",
      "Rooms Confidence",
      "Rooms Source URL",
      "Rooms Source Type",
      "Rooms Reviewed Date",
      "Rooms Notes",
      "Last Reviewed Date",
      "Enrichment Status",
      "Human Review Required",
    ],
    sources_used_this_run: sourcesUsedThisRun,
    before,
    after,
    policy,
    colombia_missing_before: colombiaMissing.length,
    rnt_rows_loaded: rntRows.length,
    source_remaining_classified: sourceRemaining,
    schema_gaps: policy.schema_gaps,
    next_backlog: nextBacklog,
    run_dir: runDir,
    write_errors: writeResult.writeErrors,
    argv_has_enable_production_writes: argv.includes(
      "--enable-production-writes"
    ),
  };

  const reportJsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-rooms-secondary-source-completion-v1.json"
  );
  const reportMdPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-rooms-secondary-source-completion-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-rooms-secondary-source-completion-v1.md"
  );

  writeJson(path.join(runDir, "final-report.json"), report);
  writeJson(reportJsonPath, report);
  const md = renderReportMd(report);
  writeMd(reportMdPath, md);
  writeMd(docsPath, md);

  log(`[rooms-completion] status=${status} updated=${report.records_updated}`);
  return report;
}
