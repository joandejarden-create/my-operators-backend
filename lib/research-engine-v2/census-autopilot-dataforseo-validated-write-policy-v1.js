/**
 * DataForSEO Validated Write Policy v1 mission.
 * Promote v2 discovery candidates → Census writes only after underlying URL validation.
 * Hotel Property Census only. No Maps/address/phone/coord/Travel Weekly writes.
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
import {
  ensureRoomsEvidenceTierField,
  ROOMS_EVIDENCE_TIER_FIELD,
} from "./production-census-rooms-evidence-tier-schema.js";
import { buildSecondaryRoomsPatch } from "./census-rooms-secondary-match.js";
import {
  DATAFORSEO_VALIDATED_WRITE_POLICY_VERSION,
  resolveDataForSeoValidatedWriteGates,
  classifyCandidateForValidatedWrite,
  fetchUnderlyingCandidatePage,
  validateBrandOfficialUrl,
  validateBrandOfficialUrlBotBlocked,
  validateHotelOfficialStrict,
  extractValidatedRoomsFromHtml,
  hostFromUrl,
  SOURCE_TIER,
} from "./dataforseo-validated-write-policy.js";
import { isBrandOfficialHost } from "./census-discovery-host-policy.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const DATAFORSEO_VALIDATED_WRITE_OBJECTIVE =
  "dataforseo-validated-write-policy-v1";
export const DATAFORSEO_VALIDATED_WRITE_VERSION =
  DATAFORSEO_VALIDATED_WRITE_POLICY_VERSION;

export const DATAFORSEO_VALIDATED_WRITE_STATUS = Object.freeze({
  COMPLETE: "production_census_dataforseo_validated_write_policy_v1_complete",
  PARTIAL_POLICY:
    "production_census_dataforseo_validated_write_policy_v1_partial_policy_decision_needed",
  PARTIAL_SOURCE:
    "production_census_dataforseo_validated_write_policy_v1_partial_source_remaining",
  BLOCKED: "production_census_dataforseo_validated_write_policy_v1_blocked",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || productionHotelPropertyCensus.tableId;

const DEFAULT_CANDIDATES_PATH = path.join(
  ROOT,
  "reports/research-engine-v2/autopilot/2026-08-07T21-05-20_CALA-dataforseo-discovery-pilot-v2/per-record-candidates.json"
);

const READ_FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Country",
  "City",
  "Address",
  "Phone",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Evidence Tier",
  "Rooms Reviewed Date",
  "Rooms Review Status",
  "Rooms Notes",
  "Official Property URL",
  "Source URL",
  "Latitude",
  "Longitude",
  "Enrichment Status",
  "Human Review Required",
];

const ALLOWED_WRITE = new Set([
  "Official Property URL",
  "Source URL",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Evidence Tier",
  "Rooms Reviewed Date",
  "Rooms Notes",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Human Review Required",
]);

const FORBIDDEN_THIS_MISSION = new Set([
  "Address",
  "Phone",
  "Latitude",
  "Longitude",
  "Address Confidence",
  "Address Source URL",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Geocode Method",
  "Geocode Reviewed Date",
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

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function listCensusByIds(baseId, token, tableId, ids) {
  const out = new Map();
  const unique = [...new Set(ids.filter(Boolean))];
  for (let i = 0; i < unique.length; i += 1) {
    const id = unique[i];
    // Single-record GET does not accept fields[] — fetch full record.
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}/${encodeURIComponent(id)}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json().catch(() => ({}));
    if (res.ok && json.id) out.set(json.id, json);
    if (i % 20 === 19) await sleep(200);
  }
  return out;
}

async function applyPatches(proposals, { baseId, token, tableId, log }) {
  let updatesApplied = 0;
  const writeErrors = [];
  let urlWrites = 0;
  let roomsWrites = 0;

  for (let i = 0; i < proposals.length; i += 10) {
    const chunk = proposals.slice(i, i + 10);
    const records = chunk
      .map((p) => {
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (isForbiddenAutopilotField(k)) continue;
          if (FORBIDDEN_THIS_MISSION.has(k)) continue;
          if (!ALLOWED_WRITE.has(k)) continue;
          if (v === undefined || v === null || v === "") continue;
          fields[k] = v;
          if (k === "Official Property URL") urlWrites += 1;
          if (k === "Rooms / Keys") roomsWrites += 1;
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
        `[dfs-validated] batch write failed ${res.status}; retrying one-by-one`
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

  return { updatesApplied, writeErrors, urlWrites, roomsWrites };
}

function loadCandidateFile(env) {
  const p = String(
    env.DATAFORSEO_V2_CANDIDATES_PATH || DEFAULT_CANDIDATES_PATH
  ).trim();
  if (!fs.existsSync(p)) {
    return { ok: false, path: p, records: [], reason: "candidates_file_missing" };
  }
  const json = JSON.parse(fs.readFileSync(p, "utf8"));
  return {
    ok: true,
    path: p,
    records: json.records || [],
    count: json.count || (json.records || []).length,
  };
}

function pickUrlCandidates(useful = []) {
  return useful
    .filter(
      (c) =>
        c?.status === "useful" &&
        (c.categories || []).includes("official_hotel_url_candidate") &&
        (c.source_tier === SOURCE_TIER.brand_official ||
          c.source_tier === SOURCE_TIER.hotel_official) &&
        c.url
    )
    .sort((a, b) => {
      const tierRank = (t) =>
        t === SOURCE_TIER.brand_official
          ? 0
          : t === SOURCE_TIER.hotel_official
            ? 1
            : 9;
      return (
        tierRank(a.source_tier) - tierRank(b.source_tier) ||
        (b.match_confidence || 0) - (a.match_confidence || 0)
      );
    });
}

function pickRoomsCandidates(useful = []) {
  return useful
    .filter(
      (c) =>
        (c?.status === "useful" || c?.status === "secondary") &&
        (c.categories || []).includes("rooms_evidence_page_candidate") &&
        c.url
    )
    .filter((c) => c.status !== "secondary") // Travel Weekly held
    .sort((a, b) => {
      const tierRank = (t) =>
        t === SOURCE_TIER.brand_official
          ? 0
          : t === SOURCE_TIER.factsheet_pdf
            ? 1
            : t === SOURCE_TIER.tourism_registry ||
                t === SOURCE_TIER.tourism_board
              ? 2
              : t === SOURCE_TIER.hotel_official
                ? 3
                : 9;
      return (
        tierRank(a.source_tier) - tierRank(b.source_tier) ||
        (b.match_confidence || 0) - (a.match_confidence || 0)
      );
    });
}

function renderReportMd(report) {
  const lines = [
    `# DataForSEO Validated Write Policy v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${report.objective}\``,
    `**Generated:** ${report.generated_at}`,
    `**Mode:** field-completion-only · DataForSEO ≠ source of truth`,
    ``,
    `## Summary`,
    ``,
    `- Candidates reviewed: **${report.candidates_reviewed}**`,
    `- Candidates validated (URL or rooms): **${report.candidates_validated}**`,
    `- Official URL writes: **${report.official_url_writes}**`,
    `- hotel_official accepted: **${report.hotel_official_accepted}**`,
    `- hotel_official rejected: **${report.hotel_official_rejected}**`,
    `- Rooms writes: **${report.rooms_writes}**`,
    `- Records updated: **${report.records_updated}**`,
    `- Address/phone/maps held: **${report.address_phone_maps_held}**`,
    `- Live blank Official Property URL (workset): **${report.live_blank_official_url ?? "n/a"}**`,
    `- Live blank Rooms (workset): **${report.live_blank_rooms ?? "n/a"}**`,
    ``,
    `## Rooms split`,
    ``,
    `- By source type:`,
  ];
  for (const [k, n] of Object.entries(report.rooms_source_type_split || {})) {
    lines.push(`  - \`${k}\`: ${n}`);
  }
  lines.push(`- By confidence:`);
  for (const [k, n] of Object.entries(report.rooms_confidence_split || {})) {
    lines.push(`  - \`${k}\`: ${n}`);
  }
  lines.push(``, `## Rejected reasons`, ``);
  for (const [k, n] of Object.entries(report.rejected_reasons || {})) {
    lines.push(`- \`${k}\`: ${n}`);
  }
  lines.push(
    ``,
    `## Fields written`,
    ``,
    ...(report.fields_written || []).map((f) => `- ${f}`),
    ``,
    `## Safety`,
    ``,
    `- Census table: Hotel Property Census (\`${CENSUS_TABLE_ID}\`)`,
    `- Brand Setup / Brand Explorer writes: **0**`,
    `- Address / Phone / Coordinate writes: **0**`,
    `- Google Maps writes: **0**`,
    `- Travel Weekly direct writes: **0**`,
    `- SERP-snippet-only writes: **0**`,
    `- DataForSEO as SoT: **false**`,
    ``,
    `## Next policy decision`,
    ``,
    report.next_policy_decision || "",
    ``,
    `## Scale estimate`,
    ``,
    report.scale_estimate_notes || "",
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
 *   log?: Function,
 *   fetchImpl?: typeof fetch,
 *   maxUrlFetchesPerRecord?: number,
 *   maxRoomsFetchesPerRecord?: number,
 * }} opts
 */
export async function runDataForSeoValidatedWritePolicyV1Mission(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || console.log;
  const args = opts.args || {};
  const censusModeResolved = resolveCensusMode(opts.argv || [], {
    censusMode: opts.censusMode || args.censusMode || "field-completion-only",
  });
  const insertGuard = assertNoInsertInFieldCompletionMode(censusModeResolved, 0);
  if (!insertGuard.ok) {
    return {
      ok: false,
      status: DATAFORSEO_VALIDATED_WRITE_STATUS.BLOCKED,
      objective: DATAFORSEO_VALIDATED_WRITE_OBJECTIVE,
      reason: insertGuard.reason,
      census_writes: 0,
    };
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot",
    `${stamp}_CALA-dataforseo-validated-write-policy-v1`
  );
  fs.mkdirSync(runDir, { recursive: true });

  const gates = resolveDataForSeoValidatedWriteGates(env);
  const enableWrites = Boolean(opts.enableProductionWrites);

  if (!gates.ok) {
    const report = {
      ok: false,
      status: DATAFORSEO_VALIDATED_WRITE_STATUS.BLOCKED,
      objective: DATAFORSEO_VALIDATED_WRITE_OBJECTIVE,
      reason: "validated_write_gates_failed",
      blockers: gates.blockers,
      gates,
      census_writes: 0,
      run_dir: runDir,
    };
    writeJson(path.join(runDir, "blocked.json"), report);
    return report;
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
      ...(args.confirms || {}),
    },
    allApplyConfirms: true,
  };
  const preflight = applyPreflight(preflightArgs, envCheck);
  if (enableWrites && (!envCheck.allOk || !preflight.ok)) {
    return {
      ok: false,
      status: DATAFORSEO_VALIDATED_WRITE_STATUS.BLOCKED,
      objective: DATAFORSEO_VALIDATED_WRITE_OBJECTIVE,
      reason: "missing_confirmations",
      envCheck,
      preflight,
      census_writes: 0,
      run_dir: runDir,
    };
  }

  const targetAssert = assertProductionCensusWriteTarget({
    tableId: CENSUS_TABLE_ID,
    baseId: resolveTargetBase().target_base_id || null,
  });
  if (!targetAssert.ok) {
    return {
      ok: false,
      status: DATAFORSEO_VALIDATED_WRITE_STATUS.BLOCKED,
      objective: DATAFORSEO_VALIDATED_WRITE_OBJECTIVE,
      reason: targetAssert.code || "blocked_wrong_census_target",
      census_writes: 0,
      run_dir: runDir,
    };
  }

  const schemaResult = await ensureRoomsEvidenceTierField({
    apply: enableWrites,
    dryRun: !enableWrites,
    log,
  });
  const tierFieldForWrites =
    schemaResult.status === "created" ||
    schemaResult.status === "already_exists";

  const loaded = loadCandidateFile(env);
  if (!loaded.ok) {
    return {
      ok: false,
      status: DATAFORSEO_VALIDATED_WRITE_STATUS.BLOCKED,
      objective: DATAFORSEO_VALIDATED_WRITE_OBJECTIVE,
      reason: loaded.reason,
      candidates_path: loaded.path,
      census_writes: 0,
      run_dir: runDir,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases.target_base_id) {
    return {
      ok: false,
      status: DATAFORSEO_VALIDATED_WRITE_STATUS.BLOCKED,
      objective: DATAFORSEO_VALIDATED_WRITE_OBJECTIVE,
      reason: "missing_airtable_credentials",
      census_writes: 0,
      run_dir: runDir,
    };
  }

  log(
    `[dfs-validated] loading ${loaded.count} candidate records from ${loaded.path}`
  );
  const censusMap = await listCensusByIds(
    bases.target_base_id,
    token,
    CENSUS_TABLE_ID,
    loaded.records.map((r) => r.record_id)
  );
  log(`[dfs-validated] census rows loaded=${censusMap.size}`);

  let liveBlankUrl = 0;
  let liveBlankRooms = 0;
  for (const rec of censusMap.values()) {
    const f = rec.fields || {};
    if (isBlank(f["Official Property URL"])) liveBlankUrl += 1;
    if (isBlank(f["Rooms / Keys"])) liveBlankRooms += 1;
  }
  log(
    `[dfs-validated] live gaps blank_url=${liveBlankUrl} blank_rooms=${liveBlankRooms}`
  );

  const maxUrlFetches = Number(opts.maxUrlFetchesPerRecord || 5);
  const maxRoomsFetches = Number(opts.maxRoomsFetchesPerRecord || 3);
  const delayMs = Number(opts.delayMs ?? env.DATAFORSEO_FETCH_DELAY_MS ?? 250);

  /** @type {object[]} */
  const proposals = [];
  /** @type {Record<string, number>} */
  const rejectedReasons = {};
  /** @type {Record<string, number>} */
  const roomsSourceTypeSplit = {};
  /** @type {Record<string, number>} */
  const roomsConfidenceSplit = {};

  let candidatesReviewed = 0;
  let candidatesValidated = 0;
  let officialUrlWrites = 0;
  let hotelOfficialAccepted = 0;
  let hotelOfficialRejected = 0;
  let roomsWriteProposals = 0;
  let addressPhoneMapsHeld = 0;
  const fieldsWrittenSet = new Set();
  /** @type {object[]} */
  const decisionLog = [];

  const bumpReject = (reason) => {
    rejectedReasons[reason] = (rejectedReasons[reason] || 0) + 1;
  };

  for (let i = 0; i < loaded.records.length; i += 1) {
    const row = loaded.records[i];
    const rec = censusMap.get(row.record_id);
    if (!rec) {
      bumpReject("census_record_not_found");
      continue;
    }
    const f = rec.fields || {};
    const hotelName =
      f["Canonical Property Name"] ||
      f["Property Name"] ||
      row.hotel_name ||
      "";
    const city = f.City || row.city || "";
    const country = f.Country || row.country || "";
    const useful = row.useful_candidates || [];

    for (const c of useful) {
      candidatesReviewed += 1;
      const cats = c.categories || [];
      if (
        cats.includes("address_candidate") ||
        cats.includes("phone_candidate") ||
        cats.includes("google_maps_local_candidate") ||
        cats.includes("lat_long_candidate")
      ) {
        addressPhoneMapsHeld += 1;
      }
      const cls = classifyCandidateForValidatedWrite(c);
      if (cls.action === "hold") {
        bumpReject(cls.reason);
      }
    }

    /** @type {Record<string, unknown>} */
    let mergedPatch = {};
    let recordValidated = false;

    // --- Official URL ---
    if (gates.url_writes && isBlank(f["Official Property URL"])) {
      const urlCands = pickUrlCandidates(useful).slice(0, maxUrlFetches);
      for (const cand of urlCands) {
        const cls = classifyCandidateForValidatedWrite(cand);
        if (cls.action !== "validate_url") continue;

        const fetched = await fetchUnderlyingCandidatePage(cand.url, {
          fetchImpl: opts.fetchImpl,
        });
        const pageInput = {
          url: fetched.final_url || cand.url,
          html: fetched.html || "",
          hotelName,
          city,
          country,
          title: cand.title || "",
        };

        let validation;
        if (cand.source_tier === SOURCE_TIER.brand_official) {
          if (fetched.ok) {
            validation = validateBrandOfficialUrl(pageInput);
          } else if (/403|401|405|bot|forbidden|akamai/i.test(String(fetched.reason || ""))) {
            validation = validateBrandOfficialUrlBotBlocked(pageInput);
            bumpReject(`url_fetch_${fetched.reason || "blocked"}`);
          } else {
            bumpReject(fetched.reason || "fetch_failed");
            decisionLog.push({
              record_id: row.record_id,
              field: "url",
              url: cand.url,
              ok: false,
              reason: fetched.reason,
            });
            if (delayMs) await sleep(delayMs);
            continue;
          }
        } else {
          if (!fetched.ok) {
            bumpReject(fetched.reason || "fetch_failed");
            if (delayMs) await sleep(delayMs);
            continue;
          }
          validation = validateHotelOfficialStrict(pageInput);
          if (!validation.ok) {
            hotelOfficialRejected += 1;
          } else {
            hotelOfficialAccepted += 1;
          }
        }

        if (!validation.ok) {
          bumpReject(validation.reason || "url_validation_failed");
          decisionLog.push({
            record_id: row.record_id,
            field: "url",
            url: cand.url,
            ok: false,
            reason: validation.reason,
            source_tier: cand.source_tier,
          });
          if (delayMs) await sleep(delayMs);
          continue;
        }

        const writeUrl = fetched.final_url || cand.url;
        mergedPatch["Official Property URL"] = writeUrl;
        if (isBlank(f["Source URL"])) {
          mergedPatch["Source URL"] = writeUrl;
        }
        mergedPatch["Last Reviewed Date"] = todayIsoDate();
        mergedPatch["Enrichment Status"] = "Partial";
        if (validation.bot_blocked) {
          mergedPatch["Enrichment Priority"] = "Medium";
        }
        officialUrlWrites += 1;
        recordValidated = true;
        candidatesValidated += 1;
        decisionLog.push({
          record_id: row.record_id,
          field: "url",
          url: writeUrl,
          ok: true,
          source_tier: validation.source_tier || cand.source_tier,
          bot_blocked: Boolean(validation.bot_blocked),
        });
        if (delayMs) await sleep(delayMs);
        break;
      }
    }

    // --- Rooms ---
    if (gates.rooms_writes && isBlank(f["Rooms / Keys"])) {
      const roomsCands = pickRoomsCandidates(useful).slice(0, maxRoomsFetches);
      // Also try Official Property URL / just-validated URL
      const seedUrls = [];
      if (mergedPatch["Official Property URL"]) {
        seedUrls.push({
          url: mergedPatch["Official Property URL"],
          source_tier: SOURCE_TIER.brand_official,
          categories: ["rooms_evidence_page_candidate", "official_hotel_url_candidate"],
          status: "useful",
          match_confidence: 1,
        });
      } else if (!isBlank(f["Official Property URL"])) {
        seedUrls.push({
          url: f["Official Property URL"],
          source_tier: isBrandOfficialHost(hostFromUrl(f["Official Property URL"]))
            ? SOURCE_TIER.brand_official
            : SOURCE_TIER.hotel_official,
          categories: ["rooms_evidence_page_candidate", "official_hotel_url_candidate"],
          status: "useful",
          match_confidence: 1,
        });
      }
      const roomQueue = [...seedUrls, ...roomsCands];
      const seenRoomUrl = new Set();

      for (const cand of roomQueue) {
        const u = String(cand.url || "").trim();
        if (!u || seenRoomUrl.has(u)) continue;
        seenRoomUrl.add(u);
        if (seenRoomUrl.size > maxRoomsFetches + 1) break;

        const cls = classifyCandidateForValidatedWrite({
          ...cand,
          categories: [
            ...(cand.categories || []),
            "rooms_evidence_page_candidate",
          ],
        });
        // Allow seed official URLs even if classify would skip maps-only
        const allowSeed =
          cand.match_confidence === 1 &&
          (cand.source_tier === SOURCE_TIER.brand_official ||
            cand.source_tier === SOURCE_TIER.hotel_official);
        if (cls.action === "hold" || cls.action === "reject") {
          if (!allowSeed) {
            bumpReject(cls.reason || "rooms_candidate_blocked");
            continue;
          }
        }

        const fetched = await fetchUnderlyingCandidatePage(u, {
          fetchImpl: opts.fetchImpl,
        });
        if (!fetched.ok) {
          bumpReject(fetched.reason || "rooms_fetch_failed");
          if (delayMs) await sleep(delayMs);
          continue;
        }

        const pageVal =
          cand.source_tier === SOURCE_TIER.brand_official
            ? validateBrandOfficialUrl({
                url: fetched.final_url || u,
                html: fetched.html,
                hotelName,
                city,
                country,
              })
            : validateHotelOfficialStrict({
                url: fetched.final_url || u,
                html: fetched.html,
                hotelName,
                city,
                country,
              });

        // Tourism/factsheet may not pass brand property-specific checks — allow tourism hosts
        const host = hostFromUrl(fetched.final_url || u);
        const tourismHost =
          /gob\.|sectur|mincit|rnt\.|turismo|visit|ict\.go\.cr|embratur/i.test(
            host
          );
        if (!pageVal.ok && !tourismHost) {
          bumpReject(pageVal.reason || "rooms_page_validation_failed");
          if (cand.source_tier === SOURCE_TIER.hotel_official) {
            hotelOfficialRejected += 1;
          }
          if (delayMs) await sleep(delayMs);
          continue;
        }
        if (pageVal.ok && cand.source_tier === SOURCE_TIER.hotel_official) {
          hotelOfficialAccepted += 1;
        }

        const roomsHit = extractValidatedRoomsFromHtml(
          fetched.html,
          fetched.final_url || u,
          { hotelName, page_validated: pageVal.ok || tourismHost }
        );
        if (!roomsHit.ok) {
          bumpReject(roomsHit.reason || "rooms_extract_failed");
          if (delayMs) await sleep(delayMs);
          continue;
        }

        const match = {
          ok: true,
          rooms: roomsHit.rooms,
          confidence: roomsHit.confidence,
          source_url: roomsHit.source_url,
          source_type_airtable: roomsHit.source_type_airtable,
          evidence_tier: roomsHit.evidence_tier,
          evidence_tier_select: roomsHit.evidence_tier_select,
          category: roomsHit.category,
          adapter: "dataforseo_validated_write_policy_v1",
          notes: roomsHit.notes,
        };
        const built = buildSecondaryRoomsPatch(f, match, {
          today: todayIsoDate(),
          roomsEvidenceTierFieldExists: tierFieldForWrites,
        });
        if (built.conflict && built.patch) {
          Object.assign(mergedPatch, built.patch);
          proposals.push({
            record_id: row.record_id,
            reason: "rooms_conflict_steward",
            patch: { ...mergedPatch },
          });
          bumpReject("rooms_conflict_steward");
          decisionLog.push({
            record_id: row.record_id,
            field: "rooms",
            url: roomsHit.source_url,
            ok: false,
            reason: "rooms_conflict_steward",
          });
          mergedPatch = {};
          break;
        }
        if (!built.ok || !built.patch) {
          bumpReject(built.reason || "rooms_patch_failed");
          if (delayMs) await sleep(delayMs);
          continue;
        }

        Object.assign(mergedPatch, built.patch);
        if (tierFieldForWrites && roomsHit.evidence_tier_select) {
          mergedPatch[ROOMS_EVIDENCE_TIER_FIELD] = roomsHit.evidence_tier_select;
        }
        // Rooms Review Status is not present on Hotel Property Census — skip.
        roomsWriteProposals += 1;
        roomsSourceTypeSplit[roomsHit.source_type_airtable] =
          (roomsSourceTypeSplit[roomsHit.source_type_airtable] || 0) + 1;
        roomsConfidenceSplit[roomsHit.confidence] =
          (roomsConfidenceSplit[roomsHit.confidence] || 0) + 1;
        recordValidated = true;
        candidatesValidated += 1;
        decisionLog.push({
          record_id: row.record_id,
          field: "rooms",
          url: roomsHit.source_url,
          ok: true,
          rooms: roomsHit.rooms,
          confidence: roomsHit.confidence,
          source_type: roomsHit.source_type_airtable,
        });
        if (delayMs) await sleep(delayMs);
        break;
      }
    }

    if (Object.keys(mergedPatch).length) {
      // Final safety strip
      for (const k of Object.keys(mergedPatch)) {
        if (FORBIDDEN_THIS_MISSION.has(k) || isForbiddenAutopilotField(k)) {
          delete mergedPatch[k];
        } else if (!ALLOWED_WRITE.has(k)) {
          delete mergedPatch[k];
        } else {
          fieldsWrittenSet.add(k);
        }
      }
      if (Object.keys(mergedPatch).length) {
        proposals.push({
          record_id: row.record_id,
          reason: "dataforseo_validated_write_policy_v1",
          patch: mergedPatch,
          validated: recordValidated,
        });
      }
    }

    if ((i + 1) % 10 === 0 || i === loaded.records.length - 1) {
      log(
        `[dfs-validated] progress ${i + 1}/${loaded.records.length} proposals=${proposals.length} url=${officialUrlWrites} rooms=${roomsWriteProposals}`
      );
    }
  }

  writeJson(path.join(runDir, "proposals.json"), {
    count: proposals.length,
    proposals: proposals.slice(0, 800),
  });
  writeJson(path.join(runDir, "decision-log.json"), {
    count: decisionLog.length,
    decisions: decisionLog.slice(0, 2000),
  });

  let writeResult = {
    updatesApplied: 0,
    writeErrors: [],
    urlWrites: 0,
    roomsWrites: 0,
  };
  if (enableWrites && proposals.length) {
    log(`[dfs-validated] applying ${proposals.length} patches (production)`);
    writeResult = await applyPatches(proposals, {
      baseId: bases.target_base_id,
      token,
      tableId: CENSUS_TABLE_ID,
      log,
    });
  } else {
    log(
      `[dfs-validated] dry-run / no-apply — proposals=${proposals.length} census_writes=0`
    );
  }

  const urlHitRate =
    loaded.records.length > 0
      ? officialUrlWrites / loaded.records.length
      : 0;
  let status = DATAFORSEO_VALIDATED_WRITE_STATUS.PARTIAL_SOURCE;
  if (enableWrites && writeResult.updatesApplied > 0 && urlHitRate >= 0.15) {
    status = DATAFORSEO_VALIDATED_WRITE_STATUS.PARTIAL_POLICY;
  } else if (
    enableWrites &&
    writeResult.updatesApplied > 0 &&
    roomsWriteProposals + officialUrlWrites > 0
  ) {
    status = DATAFORSEO_VALIDATED_WRITE_STATUS.PARTIAL_POLICY;
  } else if (!enableWrites && proposals.length > 0) {
    status = DATAFORSEO_VALIDATED_WRITE_STATUS.PARTIAL_POLICY;
  } else if (proposals.length === 0 && candidatesReviewed > 0) {
    status = DATAFORSEO_VALIDATED_WRITE_STATUS.PARTIAL_SOURCE;
  }
  if (enableWrites && writeResult.writeErrors?.length && writeResult.updatesApplied === 0) {
    status = DATAFORSEO_VALIDATED_WRITE_STATUS.BLOCKED;
  }
  if (
    enableWrites &&
    writeResult.updatesApplied > 0 &&
    officialUrlWrites + roomsWriteProposals >= loaded.records.length * 0.5
  ) {
    status = DATAFORSEO_VALIDATED_WRITE_STATUS.COMPLETE;
  }

  const scaleFactor = loaded.records.length ? 1224 / loaded.records.length : 0;
  const report = {
    ok: status !== DATAFORSEO_VALIDATED_WRITE_STATUS.BLOCKED,
    status,
    objective: DATAFORSEO_VALIDATED_WRITE_OBJECTIVE,
    version: DATAFORSEO_VALIDATED_WRITE_VERSION,
    generated_at: new Date().toISOString(),
    gates,
    candidates_path: loaded.path,
    candidates_reviewed: candidatesReviewed,
    candidates_validated: candidatesValidated,
    official_url_writes: enableWrites
      ? writeResult.urlWrites || officialUrlWrites
      : officialUrlWrites,
    hotel_official_accepted: hotelOfficialAccepted,
    hotel_official_rejected: hotelOfficialRejected,
    rooms_writes: enableWrites
      ? writeResult.roomsWrites || roomsWriteProposals
      : roomsWriteProposals,
    rooms_source_type_split: roomsSourceTypeSplit,
    rooms_confidence_split: roomsConfidenceSplit,
    rejected_reasons: rejectedReasons,
    records_updated: enableWrites ? writeResult.updatesApplied : 0,
    records_proposed: proposals.length,
    fields_written: [...fieldsWrittenSet],
    address_phone_maps_held: addressPhoneMapsHeld,
    airtable_writes: enableWrites ? writeResult.updatesApplied : 0,
    census_writes: enableWrites ? writeResult.updatesApplied : 0,
    brand_setup_writes: 0,
    brand_explorer_writes: 0,
    write_errors: writeResult.writeErrors || [],
    next_policy_decision:
      liveBlankUrl === 0 && liveBlankRooms > 0
        ? "Official Property URL already populated for this v2 set. Rooms blocked mainly by brand-site bot 403s — next: unblocked official fetch path, tourism-registry adapters (e.g. Colombia RNT), or steward factsheet pack. Maps/address/phone/Travel Weekly still not approved."
        : "Decide whether to approve Google Maps contact/geo candidate validation, Travel Weekly secondary rooms, and scale validated URL/rooms promotion beyond the 200-record v2 set.",
    scale_estimate_notes: `This v2 set: blank Official Property URL=${liveBlankUrl}, blank Rooms=${liveBlankRooms}. Brand HTML mostly HTTP 403 from Autopilot runtime. Scale rooms only after unblocked official fetch or registry adapters; do not re-spend DataForSEO SERP for the same 200.`,
    live_blank_official_url: liveBlankUrl,
    live_blank_rooms: liveBlankRooms,
    production_target: {
      base: "Deal Capture Platform",
      table: "Hotel Property Census",
      table_id: CENSUS_TABLE_ID,
    },
    run_dir: runDir,
    enable_production_writes: enableWrites,
  };

  writeJson(path.join(runDir, "mission-report.json"), report);
  const reportJson = path.join(
    ROOT,
    "reports/research-engine-v2/dataforseo-validated-write-policy-v1.json"
  );
  const reportMd = path.join(
    ROOT,
    "reports/research-engine-v2/dataforseo-validated-write-policy-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/dataforseo-validated-write-policy-v1.md"
  );
  writeJson(reportJson, report);
  const md = renderReportMd(report);
  writeMd(reportMd, md);
  writeMd(docsPath, md);

  log(
    `[dfs-validated] done status=${status} proposed=${proposals.length} updated=${report.records_updated} url=${report.official_url_writes} rooms=${report.rooms_writes}`
  );
  return report;
}
