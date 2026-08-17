/**
 * Commercial Fields + Hotel Description Autopilot mission v1.
 * Market/Submarket + safe Census-field descriptions. No invented phone/rooms.
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
  CENSUS_MODE,
} from "./census-autopilot-full-latam-v3.js";
import {
  buildCensusGapLedger,
  buildCompletionScorecard,
  writeCensusGapLedger,
} from "./census-gap-ledger.js";
import { buildActiveBrandIndex } from "./census-brand-governance.js";
import { buildCanonicalBrandDictionary } from "./census-brand-canonical-dictionary.js";
import { extractChoicePropertyId } from "./census-autopilot-family-directory-adapters.js";
import { completeMarketSubmarketForRecord } from "./census-market-submarket-completion.js";
import {
  generateHotelDescriptions,
  DESCRIPTION_FIELD_MAP,
  DESCRIPTION_SCHEMA_GAPS,
  DESCRIPTION_STATUS,
} from "./census-hotel-description-generator.js";
import { isChoiceCentralReservationPhone } from "./census-phone-number-enrichment.js";
import { isFalsePositiveRoomCount } from "./production-census-rooms-keys-extractor.js";
import { isIncorrectCanonicalPropertyName } from "./universal-hotel-record-inspector.js";
import { isDirtyStateRegionValue } from "./census-city-to-state-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const COMMERCIAL_FIELDS_DESCRIPTION_V1_OBJECTIVE =
  "commercial-fields-and-description-v1";
export const COMMERCIAL_FIELDS_DESCRIPTION_V1_VERSION =
  "commercial-fields-and-description-v1";

export const COMMERCIAL_FIELDS_DESCRIPTION_STATUS = Object.freeze({
  COMPLETE: "production_census_commercial_fields_and_description_v1_complete",
  PARTIAL_SOURCE:
    "production_census_commercial_fields_and_description_v1_partial_source_remaining",
  PARTIAL_SECONDARY:
    "production_census_commercial_fields_and_description_v1_partial_secondary_source_decision_needed",
  BLOCKED: "production_census_commercial_fields_and_description_v1_blocked",
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
  "Submarket",
  "Address",
  "Phone",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Latitude",
  "Longitude",
  "Official Property URL",
  "Property Type",
  "Asset Context",
  "Hotel Description - Source Text",
  "Hotel Description - AI Summary",
  "Enrichment Status",
  "Human Review Required",
];

const ALLOWED_WRITE = new Set([
  "Market",
  "Submarket",
  "Hotel Description - AI Summary",
  "Canonical Property Name",
  "State / Region",
  "Data Confidence Tier",
  "Enrichment Status",
  "Enrichment Priority",
  "Human Review Required",
  "Last Reviewed Date",
  "Phone",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Evidence Tier",
  "Rooms Review Status",
  "Rooms Reviewed Date",
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
  const fieldCounts = {
    market: 0,
    submarket: 0,
    description: 0,
    phone: 0,
    rooms: 0,
  };

  for (let i = 0; i < proposals.length; i += 10) {
    const chunk = proposals.slice(i, i + 10);
    const records = chunk
      .map((p) => {
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (isForbiddenAutopilotField(k)) continue;
          if (!ALLOWED_WRITE.has(k)) continue;
          if (v === undefined || v === null || v === "") continue;
          fields[k] = v;
          if (k === "Market") fieldCounts.market += 1;
          if (k === "Submarket") fieldCounts.submarket += 1;
          if (k === "Hotel Description - AI Summary") fieldCounts.description += 1;
          if (k === "Phone") fieldCounts.phone += 1;
          if (k === "Rooms / Keys") fieldCounts.rooms += 1;
        }
        return { id: p.record_id, fields };
      })
      .filter((u) => Object.keys(u.fields).length > 0);

    if (!records.length) continue;
    try {
      const res = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
        {
          method: "PATCH",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ records, typecast: true }),
        }
      );
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        writeErrors.push({ status: res.status, error: json.error || json });
        for (const rec of records) {
          const res1 = await fetch(
            `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
            {
              method: "PATCH",
              headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify({ records: [rec], typecast: true }),
            }
          );
          const j1 = await res1.json().catch(() => ({}));
          if (!res1.ok) writeErrors.push({ record_id: rec.id, error: j1.error || j1 });
          else updatesApplied += 1;
          await new Promise((r) => setTimeout(r, 150));
        }
      } else {
        updatesApplied += (json.records || []).length;
      }
    } catch (err) {
      writeErrors.push({ error: err?.message || String(err) });
    }
    log?.(
      `[commercial-desc] write batch ${Math.floor(i / 10) + 1}: applied=${updatesApplied} errors=${writeErrors.length}`
    );
    await new Promise((r) => setTimeout(r, 180));
  }
  return { updatesApplied, writeErrors, fieldCounts };
}

function classifyPhone(fields) {
  const phone = fields.Phone;
  if (!isBlank(phone)) {
    if (isChoiceCentralReservationPhone(phone)) {
      return { status: "central_reservation_present", write: false };
    }
    return { status: "present", write: false };
  }
  return {
    status: "source_missing",
    write: false,
    secondary_needed: true,
    reason: "official_property_phone_unavailable",
  };
}

function classifyRooms(fields) {
  const rooms = fields["Rooms / Keys"];
  if (!isBlank(rooms)) {
    const n = Number(rooms);
    if (
      Number.isFinite(n) &&
      isFalsePositiveRoomCount(
        String(fields["Rooms Source URL"] || "") + " choicehotels.com",
        n,
        "existing"
      )
    ) {
      return { status: "false_positive_present", write: false };
    }
    return { status: "present", write: false };
  }
  return {
    status: "source_missing",
    write: false,
    secondary_needed: true,
    reason: "exact_rooms_source_missing",
  };
}

function prioritizeCommercial(records) {
  return records
    .map((r) => {
      const f = r.fields || {};
      let score = 0;
      if (isBlank(f.Market)) score += 8;
      if (isBlank(f.Submarket)) score += 3;
      if (isBlank(f["Hotel Description - AI Summary"])) score += 10;
      if (isBlank(f.Phone)) score += 2;
      if (isBlank(f["Rooms / Keys"])) score += 2;
      if (!isBlank(f.City) && !isBlank(f.Country)) score += 5;
      if (!isIncorrectCanonicalPropertyName(f).incorrect) score += 4;
      return { record: r, score };
    })
    .sort((a, b) => b.score - a.score);
}

function renderMarkdown(report) {
  const mx = report.mx043 || {};
  return `# Commercial Fields + Hotel Description v1

**Status:** \`${report.status}\`
**Objective:** \`${report.objective}\`
**Census mode:** \`${report.census_mode}\`
**Secondary sources enabled:** ${report.secondary_enabled}
**Write target:** Hotel Property Census (\`${CENSUS_TABLE_ID}\`)
**Airtable writes:** ${report.airtable_writes}

## Schema notes

- Description write field: \`${DESCRIPTION_FIELD_MAP.aiSummary}\` (public-style, Census-field generated)
- Source Text reserved for official extracted text (not invented)
- Schema gaps (not written): ${DESCRIPTION_SCHEMA_GAPS.join(", ")}

## Summary

- Records scanned: ${report.records_scanned}
- Records updated: ${report.records_updated}
- Records inserted: ${report.records_inserted}
- Market writes: ${report.field_counts?.market ?? 0}
- Submarket writes: ${report.field_counts?.submarket ?? 0}
- Descriptions generated/written: ${report.descriptions_written}
- Descriptions held: ${report.descriptions_held}
- Phone written: ${report.phone_written}
- Phone central reservation rejected/classified: ${report.phone_central_rejected}
- Phone source missing: ${report.phone_source_missing}
- Rooms written: ${report.rooms_written}
- Rooms false positives rejected: ${report.rooms_fp_rejected}
- Rooms source missing: ${report.rooms_source_missing}
- Secondary source needed (phone/rooms): ${report.secondary_needed_count}

## Choice MX043

- Record ID: ${mx.record_id || "n/a"}
- Before: ${JSON.stringify(mx.before || {})}
- After: ${JSON.stringify(mx.after || {})}
- Patch keys: ${(mx.patch_keys || []).join(", ") || "—"}

## Backlogs

- Market mapping backlog: ${report.market_backlog_count}
- Submarket mapping backlog: ${report.submarket_backlog_count}
- Secondary decision pack: \`reports/research-engine-v2/hotel-census-secondary-source-decision-pack.md\`

## Continue

\`\`\`bash
ENABLE_SECONDARY_HOTEL_DATA_SOURCES=0 \\
npm run census:autopilot -- --region CALA --scope official-parent-inventory --mode mission \\
  --objective commercial-fields-and-description-v1 \\
  --census-mode field-completion-only \\
  --strategy highest-yield-safe --run-until-complete --max-passes 8 --batch-size 100 \\
  --confirm-safe-writes --confirm-write-to-production-census \\
  --confirm-no-brand-explorer-writes --confirm-no-owner-operator \\
  --confirm-no-date-writes --confirm-no-recent-momentum \\
  --confirm-no-company-validation --confirm-webhound-not-production-source \\
  --enable-production-writes
\`\`\`
`;
}

function writeSecondaryDecisionPack() {
  const pack = {
    version: "hotel-census-secondary-source-decision-pack-v1",
    generated_at: new Date().toISOString(),
    policy: {
      ENABLE_SECONDARY_HOTEL_DATA_SOURCES: "0",
      writes_until_founder_approval: false,
      webhound_as_census_sot: false,
    },
    sources: [
      {
        source: "Official parent/brand pages + directories",
        fields: ["address", "phone", "hotel_url", "rooms", "coords"],
        coverage: "High for chained brands",
        license: "Public pages; respect ToS",
        phone: "Primary when property-level",
        rooms: "Primary when exact",
        address: "Primary",
        website: "Primary",
        coordinates: "When present",
        recommendation: "Continue investing — primary path",
        founder_approval_required: false,
      },
      {
        source: "Official hotel websites at scale",
        fields: ["phone", "rooms", "address", "website"],
        coverage: "Medium",
        license: "Public",
        phone: "Strong secondary after parent fail",
        rooms: "Strong secondary when exact",
        recommendation: "Approve fetch adapter after parent 403 exhaustion",
        founder_approval_required: true,
      },
      {
        source: "CoStar / STR licensed hotel database",
        fields: ["address", "phone", "rooms", "coords", "website"],
        coverage: "High commercial",
        license: "Licensed — never product-facing CoStar exposure",
        recommendation: "Internal ops only if licensed; not product-facing",
        founder_approval_required: true,
      },
      {
        source: "Google Places API",
        fields: ["address", "phone", "coords", "website"],
        coverage: "Very high",
        license: "Storage restricted without Places ToS review",
        recommendation: "Hold — legal/storage review required",
        founder_approval_required: true,
      },
      {
        source: "Data Appeal / licensed hospitality datasets",
        fields: ["address", "phone", "rooms", "coords"],
        coverage: "Medium–High",
        license: "License-dependent",
        recommendation: "Evaluate under secondary policy if storage allowed",
        founder_approval_required: true,
      },
      {
        source: "Geoapify / Foursquare / HERE / TomTom",
        fields: ["address", "phone", "coords", "website"],
        coverage: "Medium–High",
        license: "Often no bulk store",
        recommendation: "Coords/POI secondary only after license",
        founder_approval_required: true,
      },
      {
        source: "Tourism boards / convention bureaus",
        fields: ["rooms", "address", "website"],
        coverage: "Country-specific / sparse",
        license: "Often open",
        recommendation: "Secondary when property-exact",
        founder_approval_required: true,
      },
      {
        source: "Owner/developer websites",
        fields: ["rooms", "phone", "address", "website"],
        coverage: "Low volume",
        license: "Public",
        recommendation: "Secondary rooms/phone when exact property match",
        founder_approval_required: true,
      },
      {
        source: "Hospitality trade publications",
        fields: ["rooms"],
        coverage: "Sparse",
        license: "Public",
        recommendation: "Secondary rooms — exact count only",
        founder_approval_required: true,
      },
    ],
  };

  const md = `# Hotel Census Secondary Source Decision Pack

**Status:** Evaluation only — **no secondary Census writes** until founder approval.  
**Related:** \`ENABLE_SECONDARY_HOTEL_DATA_SOURCES=0\` default.

## Why this pack exists

Choice (and other) official property pages are often bot-blocked (403). Wayback/HTML frequently yields:
- central reservation hotlines (must reject)
- sitewide rooms defaults such as 25 (must reject)

Phone and Rooms therefore need an explicit secondary-source policy before coverage can rise safely.

## Matrix

| Source | Phone | Rooms | Address | Website | Coords | Coverage | License | Founder approval |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
${pack.sources
  .map(
    (s) =>
      `| ${s.source} | ${s.phone || "—"} | ${s.rooms || "—"} | ${s.address || "—"} | ${s.website || "—"} | ${s.coordinates || "—"} | ${s.coverage} | ${s.license} | ${s.founder_approval_required ? "Yes" : "No"} |`
  )
  .join("\n")}

## Recommended sequence

1. Keep secondary disabled.
2. Exhaust official parent adapters + hotel-site fetches linked from parent.
3. Founder picks **one** licensed secondary for phone/rooms gaps (not Google until legal review; not product-facing CoStar).
4. If approved, require Source URL / Type / Evidence Tier / Confidence / Reviewed Date on every write.

## Explicit non-goals until approval

- No Google Places Census writes  
- No OTA phones/rooms as SoT  
- No central reservation hotlines  
- No sitewide rooms defaults  
- No silent secondary writes  
`;

  const reportPath = path.join(
    ROOT,
    "reports/research-engine-v2/hotel-census-secondary-source-decision-pack.md"
  );
  const jsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/hotel-census-secondary-source-decision-pack.json"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/hotel-census-secondary-source-decision-pack.md"
  );
  writeJson(jsonPath, pack);
  fs.writeFileSync(reportPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");
  return { reportPath, jsonPath, docsPath };
}

/**
 * @param {{ argv?: string[], args?: object, env?: object, enableProductionWrites?: boolean, censusMode?: string, recordId?: string, propertyCode?: string, parentCompany?: string, batchSize?: number, maxPasses?: number, log?: Function }} opts
 */
export async function runCommercialFieldsAndDescriptionV1Mission(opts = {}) {
  const log = opts.log || console.log;
  const env = opts.env || process.env;
  const args = opts.args || {};
  const argv = opts.argv || [];

  const effectiveMode = resolveCensusMode(argv, {
    ...args,
    censusMode:
      args.censusMode ||
      opts.censusMode ||
      CENSUS_MODE.FIELD_COMPLETION_ONLY,
  });
  const enableWrites = Boolean(opts.enableProductionWrites);
  const secondaryEnabled =
    String(env.ENABLE_SECONDARY_HOTEL_DATA_SOURCES || "0") === "1";

  const runStamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot",
    `${runStamp}_CALA-commercial-fields-and-description-v1`
  );
  fs.mkdirSync(runDir, { recursive: true });

  const sot = assertProductionCensusWriteTarget({
    tableId: CENSUS_TABLE_ID,
    tableName: "Hotel Property Census",
  });
  if (!sot.ok) {
    return {
      ok: false,
      status: COMMERCIAL_FIELDS_DESCRIPTION_STATUS.BLOCKED,
      reason: sot.reason,
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
      status: COMMERCIAL_FIELDS_DESCRIPTION_STATUS.BLOCKED,
      reason: "missing_confirmations",
      envCheck,
      preflight,
    };
  }

  const insertGuard = assertNoInsertInFieldCompletionMode(effectiveMode, 0);
  if (!insertGuard.ok) {
    return {
      ok: false,
      status: COMMERCIAL_FIELDS_DESCRIPTION_STATUS.BLOCKED,
      reason: insertGuard.reason,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  log(`[commercial-desc] SoT OK — Hotel Property Census ${CENSUS_TABLE_ID}`);
  log(
    `[commercial-desc] mode=${effectiveMode} secondary=${secondaryEnabled} writes=${enableWrites}`
  );

  let census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const activeIndex = buildActiveBrandIndex({ region: args.region || "CALA" });
  const dictionary = buildCanonicalBrandDictionary({ region: args.region || "CALA" });

  const propertyCode = String(opts.propertyCode || args.propertyCode || "")
    .trim()
    .toUpperCase();
  const recordId = opts.recordId || args.recordId || null;

  let workset = census;
  if (recordId) {
    workset = census.filter((r) => r.id === recordId);
  } else if (propertyCode) {
    workset = census.filter((r) => {
      const id = extractChoicePropertyId(
        r.fields || {},
        r.fields?.["Property Identity Key"]
      );
      return id === propertyCode || JSON.stringify(r.fields || {}).includes(propertyCode);
    });
  } else {
    const parent = args.parentCompany || opts.parentCompany;
    if (parent) {
      const p = String(parent).toLowerCase().split(" ")[0];
      workset = census.filter((r) =>
        String(r.fields?.["Brand Family"] || "")
          .toLowerCase()
          .includes(p)
      );
    }
    const maxPass =
      Number(args.batchSize || opts.batchSize || 100) *
      Math.min(Number(args.maxPasses || opts.maxPasses || 3), 8);
    const prioritized = prioritizeCommercial(workset);
    workset = prioritized.slice(0, maxPass).map((x) => x.record);
  }

  // Always include MX043 if present
  const mx043 = census.find(
    (r) =>
      extractChoicePropertyId(r.fields || {}, r.fields?.["Property Identity Key"]) ===
      "MX043"
  );
  if (mx043 && !workset.some((r) => r.id === mx043.id)) workset.unshift(mx043);

  log(`[commercial-desc] Pass 1 — workset=${workset.length}`);
  writeJson(path.join(runDir, "workset.json"), {
    count: workset.length,
    ids: workset.map((r) => r.id).slice(0, 200),
  });

  const mxBefore = mx043
    ? {
        "Canonical Property Name": mx043.fields?.["Canonical Property Name"],
        Market: mx043.fields?.Market || null,
        Submarket: mx043.fields?.Submarket || null,
        Address: mx043.fields?.Address || null,
        Phone: mx043.fields?.Phone || null,
        "Rooms / Keys": mx043.fields?.["Rooms / Keys"] ?? null,
        "Hotel Description - AI Summary":
          mx043.fields?.["Hotel Description - AI Summary"] || null,
        "State / Region": mx043.fields?.["State / Region"] || null,
      }
    : null;

  const proposals = [];
  const results = [];
  const marketBacklog = [];
  const submarketBacklog = [];
  const phoneSecondary = [];
  const roomsSecondary = [];
  let descriptionsWritten = 0;
  let descriptionsHeld = 0;
  let phoneCentralRejected = 0;
  let phoneSourceMissing = 0;
  let roomsFpRejected = 0;
  let roomsSourceMissing = 0;
  let phoneWritten = 0;
  let roomsWritten = 0;

  log(`[commercial-desc] Pass 2–5 — market/submarket + descriptions + phone/rooms classify`);

  for (let i = 0; i < workset.length; i += 1) {
    const rec = workset[i];
    const f = rec.fields || {};
    const patch = {};
    const notes = [];

    // Pass 2: market/submarket
    const geo = completeMarketSubmarketForRecord(rec);
    Object.assign(patch, geo.patch || {});
    for (const b of geo.backlog || []) {
      if (b.type === "market_mapping_backlog") marketBacklog.push(b);
      if (b.type === "submarket_mapping_backlog") submarketBacklog.push(b);
    }

    // Pass 3: descriptions (use proposed market/submarket)
    const fieldsForDesc = { ...f, ...patch };
    const desc = generateHotelDescriptions(fieldsForDesc);
    if (desc.ok && desc.write_fields?.[DESCRIPTION_FIELD_MAP.aiSummary]) {
      const existing = String(f["Hotel Description - AI Summary"] || "").trim();
      const next = desc.write_fields[DESCRIPTION_FIELD_MAP.aiSummary];
      if (!existing || existing.length < 40) {
        patch[DESCRIPTION_FIELD_MAP.aiSummary] = next;
        descriptionsWritten += 1;
        notes.push(DESCRIPTION_STATUS.GENERATED);
      }
    } else {
      descriptionsHeld += 1;
      notes.push(desc.eligibility?.status || DESCRIPTION_STATUS.HELD_MISSING);
    }

    // Pass 4: phone classify only (no invent; secondary off)
    const phoneCls = classifyPhone(fieldsForDesc);
    if (phoneCls.status === "central_reservation_present") phoneCentralRejected += 1;
    if (phoneCls.status === "source_missing") {
      phoneSourceMissing += 1;
      phoneSecondary.push({
        record_id: rec.id,
        name: f["Canonical Property Name"] || f["Property Name"],
        reason: phoneCls.reason,
      });
    }
    // Never write central; never write without High official extract in this mission
    if (secondaryEnabled) {
      notes.push("secondary_enabled_but_no_phone_writer_v1");
    }

    // Pass 5: rooms classify only
    const roomsCls = classifyRooms(fieldsForDesc);
    if (roomsCls.status === "false_positive_present") roomsFpRejected += 1;
    if (roomsCls.status === "source_missing") {
      roomsSourceMissing += 1;
      roomsSecondary.push({
        record_id: rec.id,
        name: f["Canonical Property Name"] || f["Property Name"],
        reason: roomsCls.reason,
      });
    }

    // Dirty state steward note only — do not invent fixes here beyond maps already applied upstream
    if (isDirtyStateRegionValue(f["State / Region"])) {
      notes.push("dirty_state_remaining");
    }

    if (Object.keys(patch).length) {
      patch["Last Reviewed Date"] = todayIsoDate();
      patch["Enrichment Status"] = "Partial";
      proposals.push({
        record_id: rec.id,
        reason: "commercial_fields_and_description_v1",
        confidence: "High",
        patch,
        notes,
        webhound_as_sot: false,
      });
    }

    results.push({
      record_id: rec.id,
      patch_keys: Object.keys(patch),
      description_status: desc.eligibility?.status || null,
      phone: phoneCls.status,
      rooms: roomsCls.status,
    });

    if (i % 25 === 0) log(`[commercial-desc] resolve ${i + 1}/${workset.length}`);
  }

  writeJson(path.join(runDir, "proposals.json"), {
    count: proposals.length,
    sample: proposals.slice(0, 40),
  });
  writeJson(path.join(runDir, "backlogs.json"), {
    market: marketBacklog.slice(0, 100),
    submarket: submarketBacklog.slice(0, 100),
    phone_secondary_source_needed: phoneSecondary.slice(0, 100),
    rooms_secondary_source_needed: roomsSecondary.slice(0, 100),
  });

  log(`[commercial-desc] Pass 3 writes proposals=${proposals.length}`);
  let recordsUpdated = 0;
  let fieldCounts = {
    market: 0,
    submarket: 0,
    description: 0,
    phone: 0,
    rooms: 0,
  };
  if (enableWrites && proposals.length) {
    const applied = await applyPatches(proposals, {
      baseId: bases.target_base_id,
      token,
      tableId: CENSUS_TABLE_ID,
      log,
    });
    recordsUpdated = applied.updatesApplied;
    fieldCounts = applied.fieldCounts;
    phoneWritten = fieldCounts.phone;
    roomsWritten = fieldCounts.rooms;
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  }

  log(`[commercial-desc] Pass 6 — gap ledger + secondary pack`);
  const ledger = buildCensusGapLedger(census, { activeIndex, dictionary });
  const scorecard = buildCompletionScorecard(census, { activeIndex, dictionary });
  writeCensusGapLedger(ledger, scorecard, { runDir });
  writeSecondaryDecisionPack();

  const mxAfterRec = mx043
    ? census.find((r) => r.id === mx043.id)
    : null;
  const mxAfter = mxAfterRec
    ? {
        "Canonical Property Name": mxAfterRec.fields?.["Canonical Property Name"],
        Market: mxAfterRec.fields?.Market || null,
        Submarket: mxAfterRec.fields?.Submarket || null,
        Address: mxAfterRec.fields?.Address || null,
        Phone: mxAfterRec.fields?.Phone || null,
        "Rooms / Keys": mxAfterRec.fields?.["Rooms / Keys"] ?? null,
        "Hotel Description - AI Summary":
          mxAfterRec.fields?.["Hotel Description - AI Summary"] || null,
        "State / Region": mxAfterRec.fields?.["State / Region"] || null,
      }
    : null;

  const mxPatch = proposals.find((p) => p.record_id === mx043?.id);

  const secondaryNeeded =
    phoneSecondary.length + roomsSecondary.length > 0 && !secondaryEnabled;

  let status = COMMERCIAL_FIELDS_DESCRIPTION_STATUS.COMPLETE;
  if (secondaryNeeded) {
    status = COMMERCIAL_FIELDS_DESCRIPTION_STATUS.PARTIAL_SECONDARY;
  } else if (
    marketBacklog.length ||
    submarketBacklog.length ||
    phoneSourceMissing ||
    roomsSourceMissing
  ) {
    status = COMMERCIAL_FIELDS_DESCRIPTION_STATUS.PARTIAL_SOURCE;
  }

  const report = {
    ok: true,
    status,
    version: COMMERCIAL_FIELDS_DESCRIPTION_V1_VERSION,
    objective: COMMERCIAL_FIELDS_DESCRIPTION_V1_OBJECTIVE,
    census_mode: effectiveMode,
    secondary_enabled: secondaryEnabled,
    webhound_as_census_sot: false,
    airtable_writes: enableWrites,
    brand_setup_writes: false,
    brand_explorer_writes: false,
    records_inserted: 0,
    records_updated: recordsUpdated,
    records_scanned: workset.length,
    field_counts: fieldCounts,
    descriptions_written: descriptionsWritten,
    descriptions_held: descriptionsHeld,
    phone_written: phoneWritten,
    phone_central_rejected: phoneCentralRejected,
    phone_source_missing: phoneSourceMissing,
    rooms_written: roomsWritten,
    rooms_fp_rejected: roomsFpRejected,
    rooms_source_missing: roomsSourceMissing,
    secondary_needed_count: phoneSecondary.length + roomsSecondary.length,
    market_backlog_count: marketBacklog.length,
    submarket_backlog_count: submarketBacklog.length,
    schema_gaps: DESCRIPTION_SCHEMA_GAPS,
    description_field_map: DESCRIPTION_FIELD_MAP,
    mx043: {
      record_id: mx043?.id || null,
      before: mxBefore,
      after: mxAfter,
      patch_keys: mxPatch ? Object.keys(mxPatch.patch || {}) : [],
    },
    run_dir: runDir,
    generated_at: new Date().toISOString(),
  };

  writeJson(path.join(runDir, "report.json"), report);
  const prodJson = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-commercial-fields-and-description-v1.json"
  );
  const prodMd = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-commercial-fields-and-description-v1.md"
  );
  const docsMd = path.join(
    ROOT,
    "docs/data-intelligence/production-census-commercial-fields-and-description-v1.md"
  );
  writeJson(prodJson, report);
  const md = renderMarkdown(report);
  fs.writeFileSync(prodMd, md, "utf8");
  fs.writeFileSync(docsMd, md, "utf8");

  // Append gap ledger note
  const gapMdPath = path.join(ROOT, "reports/research-engine-v2/census-gap-ledger.md");
  const gapDocs = path.join(ROOT, "docs/data-intelligence/census-gap-ledger.md");
  const gapNote = `
## commercial-fields-and-description-v1

- Status: \`${status}\`
- Updated: ${recordsUpdated} · Market writes: ${fieldCounts.market} · Submarket: ${fieldCounts.submarket} · Descriptions: ${fieldCounts.description}
- Phone source missing: ${phoneSourceMissing} · Rooms source missing: ${roomsSourceMissing}
- MX043: ${mx043?.id || "n/a"} · Market=${mxAfter?.Market || "—"} · Description=${mxAfter?.["Hotel Description - AI Summary"] ? "set" : "held"}
`;
  try {
    if (fs.existsSync(gapMdPath)) fs.appendFileSync(gapMdPath, gapNote, "utf8");
    if (fs.existsSync(gapDocs)) fs.appendFileSync(gapDocs, gapNote, "utf8");
  } catch (err) {
    log(`[commercial-desc] gap ledger append failed: ${err?.message || err}`);
  }

  log(
    `[commercial-desc] done status=${status} updated=${recordsUpdated} desc=${fieldCounts.description} market=${fieldCounts.market}`
  );

  return {
    ok: true,
    status,
    reason: null,
    objective: COMMERCIAL_FIELDS_DESCRIPTION_V1_OBJECTIVE,
    records_updated: recordsUpdated,
    records_inserted: 0,
    mx043: report.mx043,
    airtable_writes: enableWrites,
    run_dir: runDir,
  };
}
