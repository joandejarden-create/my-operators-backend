/**
 * Level 2 Source Extraction Mission v1.
 *
 * Improves High official Address / Phone / Rooms extraction via parent adapters
 * (Hilton → Choice → Marriott → IHG → Accor → Wyndham → Preferred), applies
 * High patches to Hotel Property Census only, then chains cala-census-completion-v1.
 *
 * Brand Setup / Brand Explorer / VIC / old Census: read-only / blocked.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
} from "./production-census-source-of-truth.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  checkAutopilotApplyEnv,
  applyPreflight,
  parseAutopilotArgs,
  guardApplyBatch,
} from "./census-autopilot-apply-guard.js";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { classifyIdempotentProposals } from "./census-autopilot-idempotent-writer.js";
import {
  LEVEL_2_EXTRACTOR_VERSION,
  LEVEL_2_PARENT_ORDER,
  classifyLevel2Extraction,
  mergeLevel2ProposalsToPatch,
  warmFamilyDirectoryCaches,
} from "./census-level-2-parent-extractors.js";
import { evaluateCleanCorePass } from "./census-map-contact-size-readiness.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import { runCalaCensusCompletionV1Mission } from "./census-autopilot-cala-census-completion-v1.js";
import {
  snapshotMissionCensusMetrics,
  writeMissionPublicReports,
} from "./census-autopilot-mission.js";
import {
  classifyCensusReviewReasons,
  evaluateLevel2Eligibility,
} from "./census-brand-governance.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const LEVEL_2_SOURCE_EXTRACTION_V1_OBJECTIVE = "level-2-source-extraction-v1";
export const LEVEL_2_ADAPTER_WAVE_2_OBJECTIVE = "level-2-adapter-wave-2";
export const LEVEL_2_SOURCE_EXTRACTION_V1_VERSION = "level-2-adapter-wave-2";

export const LEVEL_2_SOURCE_EXTRACTION_STATUS = Object.freeze({
  COMPLETE: "production_census_level_2_adapter_wave_2_complete",
  PARTIAL: "production_census_level_2_adapter_wave_2_partial_source_remaining",
  BLOCKED: "production_census_level_2_adapter_wave_2_blocked_safety_stop",
});

/** @deprecated legacy v1 status aliases — prefer LEVEL_2_SOURCE_EXTRACTION_STATUS */
export const LEVEL_2_SOURCE_EXTRACTION_V1_STATUS_LEGACY = Object.freeze({
  COMPLETE: "production_census_level_2_source_extraction_v1_complete",
  PARTIAL: "production_census_level_2_source_extraction_v1_partial_source_remaining",
  BLOCKED: "production_census_level_2_source_extraction_v1_blocked_safety_stop",
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
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Source URL",
  "Official Property URL",
  "Family / Source Family",
  "Human Review Required",
  "Phone",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Latitude",
  "Longitude",
  "Continent",
  "Sub-Continent",
  "Data Confidence Tier",
  "Enrichment Status",
  "Last Reviewed Date",
];

const FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Owner Name",
  "Developer Name",
  "Developer",
  "Operator / Management Company",
  "Opening Date",
  "Renovation / Conversion Date",
  "Renovation Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
]);

function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeText(filePath, text) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, text, "utf8");
}

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !String(v).trim()) return true;
  return false;
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
    if (!res.ok) throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await new Promise((r) => setTimeout(r, 120));
  } while (offset);
  return out;
}

async function fetchOfficialPage(url, timeoutMs = 15000) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      headers: {
        "user-agent":
          "DealalityCensusBot/1.0 (+https://dealality.com; level-2-source-extraction)",
        accept: "text/html,application/xhtml+xml",
      },
      redirect: "follow",
      signal: controller.signal,
    });
    const text = await res.text();
    const blocked =
      res.status === 403 ||
      res.status === 429 ||
      /access denied|cf-challenge|attention required|akamai/i.test(text);
    return {
      ok: res.ok && !blocked && text.length > 400,
      status: res.status,
      url: res.url || url,
      text,
      blocked,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      text: "",
      blocked: false,
      error: err?.message || String(err),
    };
  } finally {
    clearTimeout(t);
  }
}

function needsLevel2Work(record) {
  const f = record.fields || {};
  const review = classifyCensusReviewReasons({ fields: f });
  // Data-quality review blocks Level 2; governance-only does not
  if (review.data_quality_review_required) return false;
  if (f["Human Review Required"] === true && !review.governance_only) return false;

  const clean = evaluateCleanCorePass(record);
  const elig = evaluateLevel2Eligibility(record, {
    cleanCoreResult: clean,
    allowAutofillableCleanCoreGaps: true,
  });
  if (!elig.eligible && !elig.clean_core) {
    const autofillOnly =
      (clean.missing || []).every((m) =>
        ["Canonical Property Name", "Source Family", "Data Confidence Tier"].includes(m)
      ) &&
      (clean.blockers || []).every(
        (b) =>
          ["canonical_blank_can_autofill", "canonical_dirty_can_clean"].includes(b) ||
          (review.governance_only &&
            (b === "human_review_required" || String(b).startsWith("canonical_steward")))
      );
    if (!autofillOnly) return false;
  }
  const addrMissing = !isStreetLevelAddress(f.Address || "");
  const phoneMissing = isBlank(f.Phone);
  const roomsMissing = isBlank(f["Rooms / Keys"]);
  return addrMissing || phoneMissing || roomsMissing;
}

function familyOf(record) {
  const f = record.fields || {};
  const fam = String(f["Brand Family"] || f["Family / Source Family"] || "").trim();
  if (["Marriott", "IHG", "Hilton", "Choice", "Accor", "Wyndham", "Preferred"].includes(fam)) {
    return fam;
  }
  if (/marriott/i.test(fam)) return "Marriott";
  if (/hilton/i.test(fam)) return "Hilton";
  if (/choice|radisson|ascend|comfort|quality|sleep|econo|cambria/i.test(fam)) return "Choice";
  if (/ihg|intercontinental|holiday inn|kimpton/i.test(fam)) return "IHG";
  if (/accor|ibis|novotel|mercure|sofitel|pullman/i.test(fam)) return "Accor";
  if (/wyndham|ramada|days inn|super 8|la quinta/i.test(fam)) return "Wyndham";
  if (/preferred/i.test(fam)) return "Preferred";
  const id = String(f["Property Identity Key"] || "");
  if (id.includes("_marriott_")) return "Marriott";
  if (id.includes("_hilton_")) return "Hilton";
  if (id.includes("_choice_")) return "Choice";
  if (id.includes("_ihg_")) return "IHG";
  if (id.includes("_accor_")) return "Accor";
  if (id.includes("_wyndham_")) return "Wyndham";
  return fam || "Other";
}

function isLikelyBotBlockedOfficialHost(url) {
  const u = String(url || "").toLowerCase();
  return (
    /marriott\.com/.test(u) ||
    /choicehotels\.com/.test(u) ||
    /ihg\.com/.test(u) ||
    /wyndhamhotels\.com/.test(u)
  );
}

/**
 * Build High Level 2 proposals for census records (directory + optional page fetch).
 */
export async function buildLevel2ExtractionProposals(opts = {}) {
  const records = opts.censusRecords || [];
  const parentFilter = opts.parentCompany || null;
  const fetchLimit = Math.max(
    0,
    Number(opts.fetchLimit ?? process.env.AUTOPILOT_LEVEL2_FETCH_LIMIT ?? 200) || 200
  );
  const log = opts.log || (() => {});
  const delayMs = Math.max(50, Number(opts.delayMs) || 120);

  log(`[level-2] warming CALA Hilton + Choice + Marriott directories…`);
  const warm = await warmFamilyDirectoryCaches({
    delayMs: 80,
    countries: opts.countries || null,
  });
  log(
    `[level-2] directories ready hilton=${warm.hilton_count} choice=${warm.choice_count} marriott=${warm.marriott_count || 0} errors=${warm.errors?.length || 0}`
  );

  const eligible = records.filter((r) => {
    if (!needsLevel2Work(r)) return false;
    if (!parentFilter) return true;
    const fam = familyOf(r).toLowerCase();
    const p = String(parentFilter).toLowerCase();
    return fam === p || fam.includes(p) || p.includes(fam);
  });

  const byParent = {};
  for (const p of LEVEL_2_PARENT_ORDER) byParent[p] = { scanned: 0, high: 0, blocked: 0, insufficient: 0 };
  byParent.Other = { scanned: 0, high: 0, blocked: 0, insufficient: 0 };

  const highProposals = [];
  const steward = [];
  const blocked = [];
  const examples = [];
  let fetchAttempted = 0;
  let fetchOk = 0;
  let fetchBlocked = 0;

  const ordered = [...eligible].sort((a, b) => {
    const rank = (f) => {
      const i = LEVEL_2_PARENT_ORDER.indexOf(familyOf(f));
      return i >= 0 ? i : 99;
    };
    return rank(a) - rank(b);
  });

  let processed = 0;
  for (const rec of ordered) {
    processed += 1;
    const fam = familyOf(rec);
    if (processed === 1 || processed % 50 === 0) {
      log(
        `[level-2] progress ${processed}/${ordered.length} family=${fam} high=${highProposals.length} fetch=${fetchAttempted}/${fetchLimit}`
      );
    }
    const bucket = byParent[fam] || byParent.Other;
    bucket.scanned += 1;

    const fields = rec.fields || {};
    const officialUrl = String(
      fields[MAP_FIRST_PASS.officialUrl] ||
        fields["Official Property URL"] ||
        fields[MAP_FIRST_PASS.sourceUrl] ||
        fields["Source URL"] ||
        ""
    ).trim();

    let classified = await classifyLevel2Extraction(rec, {
      parentCompany: parentFilter,
      directoryWarmed: true,
      allowAutofillableCleanCoreGaps: true,
    });

    const { patch: earlyPatch } = mergeLevel2ProposalsToPatch(classified);
    const addrStillMissing =
      !isStreetLevelAddress(fields.Address || "") && !earlyPatch.Address;
    const phoneStillMissing = isBlank(fields.Phone) && !earlyPatch.Phone;
    const roomsStillMissing =
      isBlank(fields["Rooms / Keys"]) && !earlyPatch["Rooms / Keys"];
    const stillNeedsPage = addrStillMissing || phoneStillMissing || roomsStillMissing;

    const needsPage =
      stillNeedsPage &&
      officialUrl &&
      /^https?:\/\//i.test(officialUrl) &&
      !isLikelyBotBlockedOfficialHost(officialUrl) &&
      fetchAttempted < fetchLimit;

    if (needsPage) {
      fetchAttempted += 1;
      const page = await fetchOfficialPage(officialUrl);
      await new Promise((r) => setTimeout(r, delayMs));
      if (page.blocked) {
        fetchBlocked += 1;
        // Re-classify with pageBlocked note but keep directory proposals
        classified = await classifyLevel2Extraction(rec, {
          parentCompany: parentFilter,
          pageBlocked: true,
          allowAutofillableCleanCoreGaps: true,
        });
        if (!(classified.proposals || []).length) {
          bucket.blocked += 1;
          blocked.push({
            record_id: rec.id,
            family: fam,
            reason: "source_blocked_level_2",
            url: officialUrl,
          });
        }
      } else if (page.ok) {
        fetchOk += 1;
        classified = await classifyLevel2Extraction(rec, {
          parentCompany: parentFilter,
          pageHtml: page.text,
          pageUrl: page.url,
          allowAutofillableCleanCoreGaps: true,
        });
      }
    }

    if (classified.steward_conflicts?.length) {
      steward.push(
        ...classified.steward_conflicts.map((s) => ({ ...s, record_id: rec.id, family: fam }))
      );
    }

    const { patch, sources } = mergeLevel2ProposalsToPatch(classified);
    if (Object.keys(patch).length) {
      bucket.high += 1;
      const proposal = {
        record_id: rec.id,
        identity_key: classified.identity_key,
        property_name: classified.property_name,
        family: fam,
        confidence: "High",
        action: "propose_update",
        queue: "level_2_source_extraction",
        patch,
        fields: patch,
        sources,
        method: "level_2_parent_extractors",
        current_fields: {
          Address: fields.Address ?? null,
          "Address Confidence": fields["Address Confidence"] ?? null,
          "Address Source URL": fields["Address Source URL"] ?? null,
          Phone: fields.Phone ?? null,
          "Rooms / Keys": fields["Rooms / Keys"] ?? null,
        },
      };
      highProposals.push(proposal);
      if (examples.length < 12) {
        examples.push({
          record_id: rec.id,
          property_name: classified.property_name,
          family: fam,
          before: {
            Address: fields.Address || null,
            Phone: fields.Phone || null,
            "Rooms / Keys": fields["Rooms / Keys"] || null,
          },
          after: patch,
          sources,
        });
      }
    } else if (classified.action === "source_insufficient" || classified.action === "blocked") {
      bucket.insufficient += 1;
    }
  }

  return {
    version: LEVEL_2_EXTRACTOR_VERSION,
    objective: LEVEL_2_SOURCE_EXTRACTION_V1_OBJECTIVE,
    parent_filter: parentFilter,
    counters: {
      records_scanned: records.length,
      eligible: eligible.length,
      high_proposals: highProposals.length,
      steward_conflicts: steward.length,
      blocked_pages: blocked.length,
      fetch_attempted: fetchAttempted,
      fetch_ok: fetchOk,
      fetch_blocked: fetchBlocked,
      fetch_limit: fetchLimit,
      by_parent: byParent,
      directory_warm: {
        hilton_count: warm.hilton_count,
        choice_count: warm.choice_count,
        marriott_count: warm.marriott_count,
      },
    },
    proposals: highProposals,
    steward_review: steward,
    blocked,
    examples_before_after: examples,
  };
}

async function applyHighPatches(proposals, opts = {}) {
  const { baseId, token, tableId, log } = opts;
  let updatesApplied = 0;
  const writeErrors = [];
  const fieldsWritten = new Set();

  const guarded = guardApplyBatch(proposals, { threshold: "High" });
  const classified = classifyIdempotentProposals(guarded.writable, { threshold: "High" });
  const writable = classified.writable || [];

  for (let i = 0; i < writable.length; i += 10) {
    const chunk = writable.slice(i, i + 10);
    const safeBody = {
      records: chunk
        .map((p) => {
          const fields = { ...(p.idempotent?.fields || p.patch || p.fields || {}) };
          for (const f of FORBIDDEN_WRITE_FIELDS) delete fields[f];
          for (const k of Object.keys(fields)) fieldsWritten.add(k);
          return { id: p.record_id, fields };
        })
        .filter((r) => Object.keys(r.fields).length > 0),
      typecast: true,
    };
    if (!safeBody.records.length) continue;
    try {
      const res = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
        {
          method: "PATCH",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(safeBody),
        }
      );
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        writeErrors.push({ status: res.status, error: json.error || json });
        log?.(`[level-2] PATCH error ${res.status}`);
      } else {
        updatesApplied += (json.records || []).length;
      }
    } catch (err) {
      writeErrors.push({ error: err?.message || String(err) });
    }
    await new Promise((r) => setTimeout(r, 200));
  }

  return {
    updates_applied: updatesApplied,
    writable_count: writable.length,
    skipped: classified.skipped?.length || 0,
    conflicts: classified.conflicts?.length || 0,
    steward: classified.steward?.length || 0,
    write_errors: writeErrors,
    fields_written: [...fieldsWritten],
  };
}

function level2Metrics(records) {
  let address = 0;
  let addressHigh = 0;
  let addressUrl = 0;
  let latLong = 0;
  let phone = 0;
  let rooms = 0;
  for (const r of records) {
    const f = r.fields || {};
    if (isStreetLevelAddress(f.Address || "")) address += 1;
    if (String(f["Address Confidence"] || "").toLowerCase() === "high") addressHigh += 1;
    if (!isBlank(f["Address Source URL"])) addressUrl += 1;
    if (f.Latitude != null && f.Longitude != null) latLong += 1;
    if (!isBlank(f.Phone)) phone += 1;
    if (!isBlank(f["Rooms / Keys"])) rooms += 1;
  }
  return {
    address_complete: address,
    address_confidence_high: addressHigh,
    address_source_url_complete: addressUrl,
    lat_long_complete: latLong,
    phone_complete: phone,
    rooms_complete: rooms,
    total_records: records.length,
  };
}

function renderLevel2Md(report) {
  const b = report.before || {};
  const a = report.after || {};
  const lines = [
    `# Level 2 Adapter Wave 2`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${report.objective || LEVEL_2_SOURCE_EXTRACTION_V1_OBJECTIVE}\``,
    `**Extractor:** \`${LEVEL_2_EXTRACTOR_VERSION}\``,
    `**Write target:** Hotel Property Census (\`${CENSUS_TABLE_ID}\`)`,
    `**Airtable writes:** ${report.airtable_writes ? "yes" : "no"}`,
    `**Brand Setup writes:** false`,
    `**Brand Explorer writes:** false`,
    `**Updates:** ${report.updates_applied ?? 0}`,
    `**Runtime ms:** ${report.runtime_ms ?? "—"}`,
    ``,
    `## Level 2 before → after`,
    ``,
    `| Metric | Before | After |`,
    `| --- | ---: | ---: |`,
    `| Address complete | ${b.address_complete ?? "—"} | ${a.address_complete ?? "—"} |`,
    `| Address Confidence High | ${b.address_confidence_high ?? "—"} | ${a.address_confidence_high ?? "—"} |`,
    `| Address Source URL complete | ${b.address_source_url_complete ?? "—"} | ${a.address_source_url_complete ?? "—"} |`,
    `| Lat/Long complete | ${b.lat_long_complete ?? "—"} | ${a.lat_long_complete ?? "—"} |`,
    `| Mapbox eligible | ${b.mapbox_eligible ?? "—"} | ${a.mapbox_eligible ?? "—"} |`,
    `| Coordinates written (chained) | — | ${report.chained_cala_completion?.coordinates_written ?? report.coordinates_written ?? "—"} |`,
    `| Phone complete | ${b.phone_complete ?? "—"} | ${a.phone_complete ?? "—"} |`,
    `| Rooms complete | ${b.rooms_complete ?? "—"} | ${a.rooms_complete ?? "—"} |`,
    `| Complete Census v1 | ${b.complete_census_v1 ?? "—"} | ${a.complete_census_v1 ?? "—"} |`,
    ``,
    `## Adapter yield`,
    ``,
    `| Adapter | High writes / notes |`,
    `| --- | --- |`,
    `| Choice High writes | ${report.choice_high_writes ?? report.extraction?.counters?.by_parent?.Choice?.high ?? 0} |`,
    `| Marriott High writes | ${report.marriott_high_writes ?? report.extraction?.counters?.by_parent?.Marriott?.high ?? 0} |`,
    `| Rooms High writes | ${report.rooms_high_writes ?? 0} |`,
    `| Bot-blocked sources | ${report.extraction?.counters?.fetch_blocked ?? 0} |`,
    `| Source-insufficient | ${Object.values(report.extraction?.counters?.by_parent || {}).reduce((n, c) => n + (c.insufficient || 0), 0)} |`,
    `| Steward conflicts | ${report.extraction?.counters?.steward_conflicts ?? 0} |`,
    `| Fields written | ${(report.fields_written || []).join(", ") || "(none)"} |`,
    `| Records updated | ${report.updates_applied ?? 0} |`,
    ``,
    `## By parent`,
    ``,
  ];
  const byParent = report.extraction?.counters?.by_parent || {};
  for (const [parent, c] of Object.entries(byParent)) {
    if (!c.scanned) continue;
    lines.push(
      `- **${parent}**: scanned=${c.scanned} high=${c.high} blocked=${c.blocked} insufficient=${c.insufficient}`
    );
  }
  lines.push(
    ``,
    `## Operations`,
    ``,
    `- High proposals: ${report.extraction?.counters?.high_proposals ?? 0}`,
    `- Records updated: ${report.updates_applied ?? 0}`,
    `- Fields written: ${(report.fields_written || []).join(", ") || "(none)"}`,
    `- Fetch attempted/ok/blocked: ${report.extraction?.counters?.fetch_attempted ?? 0}/${report.extraction?.counters?.fetch_ok ?? 0}/${report.extraction?.counters?.fetch_blocked ?? 0}`,
    `- Steward conflicts: ${report.extraction?.counters?.steward_conflicts ?? 0}`,
    `- Chained cala-census-completion: ${report.chained_cala_completion ? "yes" : "no"}`,
    `- Chained status: ${report.chained_cala_completion?.status || "—"}`,
    ``,
    `## Wave 2 adapter notes`,
    ``,
    `- Choice: property ID prefixes (all CALA), exact URL match, name+city+country match; property-level Address Source URL`,
    `- Marriott: non-Akamai sitemap/MARSHA metadata only; no invented address/phone/rooms when metadata insufficient`,
    `- Rooms: official HTML extractor + JSON-LD; High only; no OTA/inference`,
    `- Soft Clean Core autofill (Canonical / Source Family / Data Confidence Tier) when that is the only gate`,
    ``,
    `## Examples`,
    ``
  );
  for (const ex of (report.extraction?.examples_before_after || []).slice(0, 8)) {
    lines.push(
      `- **${ex.property_name}** (${ex.family}): \`${JSON.stringify(ex.before)}\` → \`${JSON.stringify(ex.after)}\``
    );
  }
  lines.push(
    ``,
    `## Safety`,
    ``,
    `- Hotel Property Census only`,
    `- Brand Setup / Brand Explorer untouched`,
    `- No owner/operator/date / Recent Momentum / Company Validated`,
    `- No Mapbox-as-address / Google / OTA phone`,
    `- No weak inference; steward conflicts held`,
    ``,
    `## Next recommended action`,
    ``,
    `${report.next_recommended_action || "—"}`,
    ``
  );
  return lines.join("\n");
}

export function writeLevel2ExtractionReports(report) {
  const jsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-level-2-adapter-wave-2.json"
  );
  const mdPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-level-2-adapter-wave-2.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-level-2-adapter-wave-2.md"
  );
  const md = renderLevel2Md(report);
  writeJson(jsonPath, report);
  writeMissionPublicReports(report, {
    objective: report.objective || LEVEL_2_ADAPTER_WAVE_2_OBJECTIVE,
  });
  writeText(mdPath, md);
  writeText(docsPath, md);
  // Also keep prior v1 filenames as pointers for continuity
  writeJson(
    path.join(ROOT, "reports/research-engine-v2/production-census-level-2-source-extraction-v1.json"),
    { ...report, note: "alias_of_level_2_adapter_wave_2" }
  );
  writeText(
    path.join(ROOT, "reports/research-engine-v2/production-census-level-2-source-extraction-v1.md"),
    md
  );
  return { jsonPath, mdPath, docsPath };
}

/**
 * Mission / controlled entrypoint.
 */
export async function runLevel2SourceExtractionV1Mission(opts = {}) {
  const argv = opts.argv || process.argv.slice(2);
  const args = opts.args || parseAutopilotArgs(argv);
  const env = opts.env || process.env;
  const log = opts.log || ((m) => console.log(m));
  const started = Date.now();

  args.objective = LEVEL_2_SOURCE_EXTRACTION_V1_OBJECTIVE;

  const envCheck = checkAutopilotApplyEnv(env);
  const preflight = applyPreflight(args, envCheck);
  const enableWrites = Boolean(
    opts.enableProductionWrites &&
      argv.includes("--enable-production-writes") &&
      args.allApplyConfirms &&
      envCheck.allOk &&
      preflight.ok &&
      args.mode === "mission"
  );

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    const blocked = {
      ok: false,
      status: LEVEL_2_SOURCE_EXTRACTION_STATUS.BLOCKED,
      objective: LEVEL_2_SOURCE_EXTRACTION_V1_OBJECTIVE,
      blocked_reason: writeTarget.reason || "wrong_census_target",
      airtable_writes: false,
      brand_setup_writes: false,
      brand_explorer_writes: false,
    };
    writeLevel2ExtractionReports(blocked);
    return blocked;
  }

  if (args.mode === "mission" && !preflight.ok) {
    const blocked = {
      ok: false,
      status: LEVEL_2_SOURCE_EXTRACTION_STATUS.BLOCKED,
      objective: LEVEL_2_SOURCE_EXTRACTION_V1_OBJECTIVE,
      blocked_reason: "confirmation_or_env",
      blockers: preflight.blockers,
      airtable_writes: false,
    };
    writeLevel2ExtractionReports(blocked);
    return blocked;
  }

  const token = opts.token ?? resolvePat();
  const bases = opts.bases ?? resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    const blocked = {
      ok: false,
      status: LEVEL_2_SOURCE_EXTRACTION_STATUS.BLOCKED,
      objective: LEVEL_2_SOURCE_EXTRACTION_V1_OBJECTIVE,
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
    };
    writeLevel2ExtractionReports(blocked);
    return blocked;
  }

  log(`[level-2] SoT guard OK — Hotel Property Census ${CENSUS_TABLE_ID}`);
  log(`[level-2] parent filter=${args.parentCompany || "all"} mode=${args.mode}`);

  const census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const beforeBase = snapshotMissionCensusMetrics(census, { env });
  const beforeL2 = level2Metrics(census);
  const before = { ...beforeBase, ...beforeL2 };

  const extraction = await buildLevel2ExtractionProposals({
    censusRecords: census,
    parentCompany: args.parentCompany || null,
    fetchLimit:
      args.mode === "controlled"
        ? 60
        : Number(process.env.AUTOPILOT_LEVEL2_FETCH_LIMIT || 250),
    log,
  });

  log(
    `[level-2] high proposals=${extraction.counters.high_proposals} steward=${extraction.counters.steward_conflicts} fetch=${extraction.counters.fetch_ok}/${extraction.counters.fetch_attempted}`
  );

  let applyResult = {
    updates_applied: 0,
    fields_written: [],
    writable_count: 0,
  };
  if (enableWrites && extraction.proposals.length) {
    applyResult = await applyHighPatches(extraction.proposals, {
      baseId: bases.target_base_id,
      token,
      tableId: CENSUS_TABLE_ID,
      log,
    });
    log(`[level-2] applied updates=${applyResult.updates_applied}`);
  } else if (args.mode === "controlled") {
    log(
      `[level-2] controlled mode — no Airtable writes (${extraction.proposals.length} High proposals)`
    );
  }

  let afterRecords = census;
  if (applyResult.updates_applied > 0) {
    afterRecords = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  }
  const afterBase = snapshotMissionCensusMetrics(afterRecords, { env });
  const afterL2 = level2Metrics(afterRecords);
  const after = { ...afterBase, ...afterL2 };

  let chained = null;
  const shouldChain =
    args.mode === "mission" &&
    enableWrites &&
    opts.skipCalaChain !== true &&
    !argv.includes("--skip-cala-chain");
  if (shouldChain) {
    log(`[level-2] chaining cala-census-completion-v1…`);
    chained = await runCalaCensusCompletionV1Mission({
      argv,
      args: { ...args, objective: "cala-census-completion-v1" },
      env,
      enableProductionWrites: true,
      token,
      bases,
      log,
    });
    // Final metrics include chained Mapbox / Clean Core / Level 2 follow-on writes
    afterRecords = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
    const afterBaseChained = snapshotMissionCensusMetrics(afterRecords, { env });
    const afterL2Chained = level2Metrics(afterRecords);
    Object.assign(after, afterBaseChained, afterL2Chained);
  }

  const addressGain = (after.address_complete || 0) - (before.address_complete || 0);
  const phoneGain = (after.phone_complete || 0) - (before.phone_complete || 0);
  const roomsGain = (after.rooms_complete || 0) - (before.rooms_complete || 0);

  let status = LEVEL_2_SOURCE_EXTRACTION_STATUS.PARTIAL;
  if (
    applyResult.updates_applied > 0 &&
    addressGain + phoneGain + roomsGain > 0 &&
    (after.address_complete || 0) >= 400 &&
    (after.phone_complete || 0) >= 100
  ) {
    status = LEVEL_2_SOURCE_EXTRACTION_STATUS.COMPLETE;
  }

  const report = {
    ok: status !== LEVEL_2_SOURCE_EXTRACTION_STATUS.BLOCKED,
    status,
    objective: LEVEL_2_ADAPTER_WAVE_2_OBJECTIVE,
    legacy_objective: LEVEL_2_SOURCE_EXTRACTION_V1_OBJECTIVE,
    version: LEVEL_2_SOURCE_EXTRACTION_V1_VERSION,
    extractor_version: LEVEL_2_EXTRACTOR_VERSION,
    mode: args.mode,
    parent_company: args.parentCompany || null,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: CENSUS_TABLE_ID,
    },
    airtable_writes: enableWrites && applyResult.updates_applied > 0,
    brand_setup_writes: false,
    brand_explorer_writes: false,
    updates_applied: applyResult.updates_applied + (chained?.updates_applied || 0),
    inserts_applied: 0,
    fields_written: [
      ...new Set([...(applyResult.fields_written || []), ...(chained?.fields_written || [])]),
    ],
    choice_high_writes: extraction?.counters?.by_parent?.Choice?.high || 0,
    marriott_high_writes: extraction?.counters?.by_parent?.Marriott?.high || 0,
    rooms_high_writes: (extraction?.proposals || []).filter(
      (p) => p.patch && p.patch["Rooms / Keys"] != null
    ).length,
    bot_blocked_source_counts: extraction?.counters?.fetch_blocked || 0,
    source_insufficient_counts: Object.values(extraction?.counters?.by_parent || {}).reduce(
      (n, c) => n + (c.insufficient || 0),
      0
    ),
    steward_conflicts: extraction?.counters?.steward_conflicts || 0,
    before,
    after,
    extraction,
    apply: applyResult,
    chained_cala_completion: chained
      ? {
          status: chained.status,
          updates_applied: chained.updates_applied,
          coordinates_written: chained.coordinates_written ?? chained.after?.lat_long_complete,
        }
      : null,
    runtime_ms: Date.now() - started,
    next_recommended_action:
      status === LEVEL_2_SOURCE_EXTRACTION_STATUS.COMPLETE
        ? "Level 2 Adapter Wave 2 complete for reachable official sources — continue Mapbox only on High Address packages."
        : "Official Level 2 sources remain partial (bot-blocked pages / missing street JSON-LD / directory gaps). Do not invent; expand parent adapters or steward blocked families.",
  };

  writeLevel2ExtractionReports(report);
  log(
    `[level-2] status=${report.status} updates=${report.updates_applied} address ${before.address_complete}→${after.address_complete} phone ${before.phone_complete}→${after.phone_complete} rooms ${before.rooms_complete}→${after.rooms_complete}`
  );
  return report;
}
