#!/usr/bin/env node
/**
 * Pilot Readiness — ensure Market Presence table + persist Wave 2 + migrate geography.
 *
 *   node scripts/operator-intelligence-pilot-readiness-apply.mjs --dry-run
 *   node scripts/operator-intelligence-pilot-readiness-apply.mjs --apply --approve-oi-pilot-writes
 */
import "../load-env.js";
import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  NEW_BASE_MASTER_TABLE,
  NEW_BASE_PLATFORM_TABLE,
  NEW_BASE_COMMERCIAL_TABLE,
  NEW_BASE_CASE_STUDIES_TABLE,
  fetchRecordsLinkedToMaster,
  airtableFetchJson,
  loadNewBaseOperatorBundle,
} from "../api/lib/operator-setup-new-base-read.js";
import {
  loadOperatorIntelligenceUniverse,
  loadCalibrationCohort,
} from "../lib/operator-intelligence/calibration-overlay.js";
import {
  MARKET_PRESENCE_TABLE,
  map_marketPresenceFields as MF,
  normalizePresenceType,
  establishesCurrentGeographicEligibility,
  countriesWithStrongPresence,
} from "../lib/operator-intelligence/market-presence.js";
import { resolvePublicationDecision, PUBLICATION_DECISION } from "../lib/operator-intelligence/publication-policy.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const APPROVED = process.argv.includes("--approve-oi-pilot-writes");
const DRY = !APPLY;

const APPROVED_COUNTRIES = new Set([
  "Mexico",
  "Dominican Republic",
  "Costa Rica",
  "Panama",
  "Colombia",
  "Peru",
  "Chile",
  "Argentina",
  "Brazil",
  "Jamaica",
  "Puerto Rico",
  "Curaçao",
]);

const APPROVED_STRUCTURES = new Set([
  "Full third-party management",
  "Brand-managed",
  "Franchise support",
  "Commercial-only support",
  "Pre-opening / transition support",
]);

const WAVE2_IDS = [
  "recLjxtxIIVJaGbXK",
  "recfwDdU5t9h4uFnZ",
  "recKVILWcRLqrQlWs",
  "reckyv9O0Y3auYpJJ",
];

const ALL_MIGRATE_IDS = [
  "recF5Z87OAqFgndoq",
  "recQ6Cf8O2z0tiqBz",
  "recWPKu5laVZxsvpn",
  "reciI2tYQBfMoMK9G",
  "rec3TUHT9Z4AnFp5P",
  "recGWxIJqnYHkJZFD",
  ...WAVE2_IDS,
];

const CLAIMS_TABLE = "Operator Intelligence - Claims";

function enc(s) {
  return encodeURIComponent(s);
}
function checksum(obj) {
  return createHash("sha256").update(JSON.stringify(obj)).digest("hex").slice(0, 16);
}

async function metaFetch(baseId, token, metaPath, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

async function patchRecord(table, recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(table)}/${enc(recordId)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!ok) throw new Error(`PATCH ${table} ${recordId}: ${status} ${JSON.stringify(json)}`);
  return json;
}

async function createRecord(table, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(table)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!ok) throw new Error(`CREATE ${table}: ${status} ${JSON.stringify(json)}`);
  return json;
}

async function ensureMarketPresenceTable(baseId, token, report) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`List tables failed: ${JSON.stringify(json)}`);
  const tables = json.tables || [];
  const master = tables.find((t) => t.name === NEW_BASE_MASTER_TABLE || t.name === "Operator Setup - Master");
  if (!master) throw new Error("Master table not found");
  let table = tables.find((t) => t.name === MARKET_PRESENCE_TABLE);

  const presenceChoices = [
    "Current Managed Property",
    "Current Operating Portfolio",
    "Regional Office or Team",
    "Active Development",
    "Historical Presence",
    "Strategic Interest",
    "Claimed Capability",
    "Unknown",
  ].map((name) => ({ name }));

  const fieldSpecs = [
    { name: "Presence Key", type: "singleLineText" },
    {
      name: MF.operator,
      type: "multipleRecordLinks",
      options: { linkedTableId: master.id },
    },
    { name: MF.country, type: "singleLineText" },
    { name: MF.region, type: "singleLineText" },
    {
      name: MF.presenceType,
      type: "singleSelect",
      options: { choices: presenceChoices },
    },
    {
      name: MF.currentOrHistorical,
      type: "singleSelect",
      options: {
        choices: [{ name: "Current" }, { name: "Historical" }, { name: "Non-current" }],
      },
    },
    { name: MF.effectiveDate, type: "date", options: { dateFormat: { name: "iso" } } },
    { name: MF.verificationDate, type: "date", options: { dateFormat: { name: "iso" } } },
    { name: MF.sourceUrls, type: "multilineText" },
    { name: MF.claimId, type: "singleLineText" },
    { name: MF.evidenceClass, type: "singleLineText" },
    { name: MF.publicationStatus, type: "singleLineText" },
    { name: MF.confidence, type: "singleLineText" },
    { name: MF.notes, type: "multilineText" },
    { name: MF.limitations, type: "multilineText" },
  ];

  if (!table) {
    report.schema.push({ operation: "create_table", table: MARKET_PRESENCE_TABLE, status: DRY ? "would_create" : "creating" });
    if (!DRY) {
      // Primary field must be text — create with Presence Key first, then remaining fields
      const [primary, ...rest] = fieldSpecs;
      const { res: cRes, json: cJson } = await metaFetch(baseId, token, "/tables", {
        method: "POST",
        body: JSON.stringify({
          name: MARKET_PRESENCE_TABLE,
          description: "Normalized operator country presence types for Fit eligibility",
          fields: [primary],
        }),
      });
      if (!cRes.ok) throw new Error(`Create Market Presence failed: ${JSON.stringify(cJson)}`);
      table = cJson;
      report.schema[report.schema.length - 1].status = "created";
      report.schema[report.schema.length - 1].tableId = cJson.id;
      for (const f of rest) {
        const { res: fRes, json: fJson } = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
          method: "POST",
          body: JSON.stringify(f),
        });
        if (!fRes.ok) throw new Error(`Create field ${f.name}: ${JSON.stringify(fJson)}`);
        report.schema.push({ operation: "create_field", table: MARKET_PRESENCE_TABLE, field: f.name, status: "created" });
      }
    }
  } else {
    report.schema.push({ operation: "table_exists", table: MARKET_PRESENCE_TABLE, tableId: table.id, status: "ok" });
    const existing = new Set((table.fields || []).map((f) => f.name));
    for (const f of fieldSpecs) {
      if (existing.has(f.name)) continue;
      report.schema.push({
        operation: "create_field",
        table: MARKET_PRESENCE_TABLE,
        field: f.name,
        status: DRY ? "would_create" : "creating",
      });
      if (!DRY) {
        const { res: fRes, json: fJson } = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
          method: "POST",
          body: JSON.stringify(f),
        });
        if (!fRes.ok) throw new Error(`Create field ${f.name}: ${JSON.stringify(fJson)}`);
        report.schema[report.schema.length - 1].status = "created";
      }
    }
  }
  return table;
}

function buildWritePlan(universe, backupsById) {
  const ops = [];
  const push = (partial) =>
    ops.push({
      operationId: `oi_pilot_${String(ops.length + 1).padStart(3, "0")}`,
      ...partial,
    });

  const sourcesById = Object.fromEntries((universe.sources || []).map((s) => [s.id, s]));
  const w2 = loadCalibrationCohort(join(ROOT, "data", "operator-intelligence", "wave-2-cohort"));

  // Wave 2 Platform Active Countries from strong presence only
  for (const id of WAVE2_IDS) {
    const geos = (universe.geography || []).filter((g) => g.operatorId === id);
    const strong = countriesWithStrongPresence(geos).filter((c) => APPROVED_COUNTRIES.has(c));
    const b = backupsById[id];
    if (!b?.platformId) {
      push({
        operatorId: id,
        operationType: "update_field",
        table: NEW_BASE_PLATFORM_TABLE,
        field: "Active Countries",
        applyOrSkip: "skip",
        skipReason: "No Platform row",
        proposedValue: strong,
        existingValue: [],
      });
      continue;
    }
    const existing = b.activeCountries || [];
    const same =
      [...existing].map(String).sort().join("|") === [...strong].map(String).sort().join("|");
    push({
      operatorId: id,
      operatorName: b.operatorName,
      operationType: "update_field",
      table: NEW_BASE_PLATFORM_TABLE,
      record: b.platformId,
      field: "Active Countries",
      existingValue: existing,
      proposedValue: strong,
      claimIds: (universe.claims || []).filter((c) => c.operatorId === id && c.claimCategory === "geography").map((c) => c.id),
      sourceIds: [...new Set(geos.flatMap((g) => g.sourceIds || []))],
      evidenceClass: "independently_referenced",
      publicationClass: 1,
      conflictStatus: "None",
      scoringRelevance: "High",
      rollbackValue: existing,
      applyOrSkip: same ? "skip" : "apply",
      skipReason: same ? "Already matches strong presence countries" : null,
      notes: "Only strong Market Presence types written to Active Countries",
    });

    const structs = (universe.managementStructures || [])
      .filter((s) => s.operatorId === id)
      .map((s) => {
        if (/third.?party/i.test(s.structure || "")) return "Full third-party management";
        if (/franchise/i.test(s.structure || "")) return "Franchise support";
        return null;
      })
      .filter((s) => s && APPROVED_STRUCTURES.has(s));
    const uniqStructs = [...new Set(structs)];
    const existingS = b.structuresBefore || [];
    if (b.commercialId && uniqStructs.length) {
      const merged = [...new Set([...existingS.filter((x) => APPROVED_STRUCTURES.has(x)), ...uniqStructs])];
      const sameS =
        [...existingS].map(String).sort().join("|") === [...merged].map(String).sort().join("|");
      push({
        operatorId: id,
        operatorName: b.operatorName,
        operationType: "update_field",
        table: NEW_BASE_COMMERCIAL_TABLE,
        record: b.commercialId,
        field: "Management Structures Supported",
        existingValue: existingS,
        proposedValue: merged,
        rollbackValue: existingS,
        applyOrSkip: sameS ? "skip" : "apply",
        skipReason: sameS ? "Already present" : null,
      });
    }
  }

  // Cenote remediation — keep Active Countries=[Mexico] as country of record;
  // eligibility comes from Market Presence Claimed Capability (not strong).
  {
    const id = "recQ6Cf8O2z0tiqBz";
    const b = backupsById[id];
    const existing = b?.activeCountries || [];
    const proposed = ["Mexico"];
    const unsupported = existing.filter((c) => c !== "Mexico");
    push({
      operatorId: id,
      operatorName: b?.operatorName || "Cenote Azul Operadores",
      operationType: "cenote_remediation",
      table: NEW_BASE_PLATFORM_TABLE,
      record: b?.platformId || null,
      field: "Active Countries",
      existingValue: existing,
      proposedValue: proposed,
      publicationClass: 2,
      conflictStatus: "resolved_via_market_presence",
      scoringRelevance: "High",
      rollbackValue: existing,
      applyOrSkip: !b?.platformId
        ? "skip"
        : existing.length === 1 && existing[0] === "Mexico" && unsupported.length === 0
          ? "skip"
          : "apply",
      skipReason: !b?.platformId
        ? "No Platform row"
        : existing.length === 1 && existing[0] === "Mexico"
          ? "Already Mexico-only; Market Presence Claimed Capability is SoT for eligibility"
          : null,
      notes:
        "Active Countries remains Mexico (country of record). Fit eligibility uses Market Presence Type=Claimed Capability — not Current Managed/Operating.",
    });
  }

  // Market Presence create ops (all migrate IDs)
  for (const id of ALL_MIGRATE_IDS) {
    const geos = (universe.geography || []).filter((g) => g.operatorId === id);
    const b = backupsById[id];
    for (const g of geos) {
      const country = g.country || (Array.isArray(g.countries) ? g.countries[0] : null);
      if (!country) continue;
      const pType = normalizePresenceType(g.presenceType || g.marketPresenceType);
      const urls = (g.sourceIds || [])
        .map((sid) => sourcesById[sid]?.url)
        .filter(Boolean)
        .join("\n");
      push({
        operatorId: id,
        operatorName: b?.operatorName || id,
        operationType: "create_market_presence",
        table: MARKET_PRESENCE_TABLE,
        field: MF.presenceType,
        country,
        existingValue: null,
        proposedValue: {
          "Presence Key": `${id}|${country}|${pType}`,
          [MF.operator]: [id],
          [MF.country]: country,
          [MF.presenceType]: pType,
          [MF.currentOrHistorical]: establishesCurrentGeographicEligibility(pType)
            ? "Current"
            : /Historical/i.test(pType)
              ? "Historical"
              : "Non-current",
          [MF.verificationDate]: "2026-08-04",
          [MF.sourceUrls]: urls,
          [MF.evidenceClass]: "independently_referenced",
          [MF.publicationStatus]: establishesCurrentGeographicEligibility(pType)
            ? PUBLICATION_DECISION.AUTO_PUBLISH
            : PUBLICATION_DECISION.PUBLISH_WITH_LABEL,
          [MF.notes]: g.evidence || "",
          [MF.limitations]: g.limitations || "",
          [MF.confidence]: "Moderate",
        },
        sourceIds: g.sourceIds || [],
        evidenceClass: "independently_referenced",
        publicationClass: establishesCurrentGeographicEligibility(pType) ? 1 : 2,
        conflictStatus: "None",
        scoringRelevance: establishesCurrentGeographicEligibility(pType) ? "High" : "Low",
        rollbackValue: null,
        applyOrSkip: "apply",
        skipReason: null,
        dedupeKey: `${id}|${country}|${pType}`,
      });
    }
  }

  // Wave 2 claims (auto-publish / labeled only)
  for (const c of w2.claims || []) {
    const d = resolvePublicationDecision(c, { sources: w2.sources });
    if (
      d.status === PUBLICATION_DECISION.INTERNAL_ONLY ||
      d.status === PUBLICATION_DECISION.HUMAN_REVIEW_REQUIRED ||
      d.status === PUBLICATION_DECISION.REJECTED ||
      d.status === PUBLICATION_DECISION.CONFLICTED ||
      d.status === PUBLICATION_DECISION.INSUFFICIENT_EVIDENCE
    ) {
      push({
        operatorId: c.operatorId,
        operationType: "create_claim",
        table: CLAIMS_TABLE,
        claimId: c.id,
        applyOrSkip: "skip",
        skipReason: d.status,
        proposedValue: c.normalizedValue || c.claimValue,
        existingValue: null,
        publicationClass: c.publicationClass,
        conflictStatus: c.conflictStatus || "None",
      });
      continue;
    }
    const urls = (c.sourceIds || [])
      .map((sid) => sourcesById[sid]?.url || (w2.sources || []).find((s) => s.id === sid)?.url)
      .filter(Boolean)
      .join("\n");
    push({
      operatorId: c.operatorId,
      operationType: "create_claim",
      table: CLAIMS_TABLE,
      claimId: c.id,
      existingValue: null,
      proposedValue: {
        "Claim ID": c.id,
        Operator: [c.operatorId],
        "Claim Category": c.claimCategory || "",
        Subject: c.claimSubject || "",
        Predicate: c.claimPredicate || "",
        "Raw Value": String(c.claimValue ?? ""),
        "Normalized Value": Array.isArray(c.normalizedValue)
          ? c.normalizedValue.join(", ")
          : String(c.normalizedValue ?? ""),
        "Evidence Class": c.evidenceClass || "",
        "Verification Status": c.verificationStatus || "",
        "Publication Status": d.status,
        "Conflict Status": c.conflictStatus || "None",
        "Scoring Relevance": c.scoringRelevance || "",
        Currentness: "Current",
        Notes: c.notes || "",
        Limitations: c.limitations || "",
        "Source URLs": urls,
      },
      sourceIds: c.sourceIds || [],
      evidenceClass: c.evidenceClass,
      publicationClass: c.publicationClass,
      conflictStatus: c.conflictStatus || "None",
      scoringRelevance: c.scoringRelevance,
      applyOrSkip: "apply",
      skipReason: null,
      dedupeKey: c.id,
    });
  }

  // Wave 2 comps
  for (const c of w2.comparables || []) {
    if (!/High|Moderate/i.test(c.comparabilityStrength || "")) {
      push({
        operatorId: c.operatorId,
        operationType: "create_comparable",
        table: NEW_BASE_CASE_STUDIES_TABLE,
        applyOrSkip: "skip",
        skipReason: "Weak comparability",
        proposedValue: c.propertyName,
      });
      continue;
    }
    push({
      operatorId: c.operatorId,
      operationType: "create_comparable",
      table: NEW_BASE_CASE_STUDIES_TABLE,
      existingValue: null,
      proposedValue: {
        Operator: [c.operatorId],
        property_name: c.propertyName,
        region: [c.city, c.country].filter(Boolean).join(", "),
        branded_independent: c.brand || "",
        owner_relevance: [c.whyComparable, c.assetType, c.developmentType].filter(Boolean).join(" · "),
        outcome: c.performanceEvidence || "Performance evidence unavailable or not independently verified.",
        "Why Comparable": c.whyComparable || "",
        "Comparability Strength": c.comparabilityStrength || "",
      },
      sourceIds: c.sourceIds || [],
      applyOrSkip: "apply",
      skipReason: null,
      dedupeKey: `${c.operatorId}|${String(c.propertyName || "").toLowerCase()}`,
    });
  }

  return {
    generatedAt: new Date().toISOString(),
    mode: DRY ? "dry-run" : "apply",
    operations: ops,
    applyCount: ops.filter((o) => o.applyOrSkip === "apply").length,
    skipCount: ops.filter((o) => o.applyOrSkip === "skip").length,
  };
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  if (!baseId || !token) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  if (APPLY && !APPROVED) throw new Error("Refusing --apply without --approve-oi-pilot-writes");

  const universe = loadOperatorIntelligenceUniverse();
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const backupDir = join(ROOT, "backups", "operator-intelligence", `pilot-${ts}`);
  mkdirSync(backupDir, { recursive: true });

  const backupsById = {};
  for (const id of ALL_MIGRATE_IDS) {
    const bundle = await loadNewBaseOperatorBundle(id);
    const caseStudies = await fetchRecordsLinkedToMaster(NEW_BASE_CASE_STUDIES_TABLE, id).catch(() => []);
    const snap = {
      operatorId: id,
      operatorName: bundle.master?.fields?.["Company Name"] || bundle.master?.fields?.Name || id,
      master: bundle.master,
      platform: bundle.platform,
      commercial: bundle.commercial,
      caseStudies,
    };
    writeFileSync(join(backupDir, `${id}.json`), JSON.stringify(snap, null, 2));
    backupsById[id] = {
      operatorId: id,
      operatorName: snap.operatorName,
      platformId: bundle.platform?.id || null,
      commercialId: bundle.commercial?.id || null,
      activeCountries: bundle.platform?.fields?.["Active Countries"] || [],
      structuresBefore: bundle.commercial?.fields?.["Management Structures Supported"] || [],
      checksum: checksum({
        platform: bundle.platform?.fields || {},
        commercial: bundle.commercial?.fields || {},
      }),
    };
  }
  writeFileSync(join(backupDir, "manifest.json"), JSON.stringify({ generatedAt: new Date().toISOString(), backupsById }, null, 2));

  const plan = buildWritePlan(universe, backupsById);
  writeFileSync(join(ROOT, "reports", "operator-intelligence-wave-2-approved-write-plan.json"), JSON.stringify(plan, null, 2));

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY ? "dry-run" : "apply",
    backupDir: backupDir.replace(/\\/g, "/"),
    schema: [],
    applied: [],
    skipped: [],
    errors: [],
    marketPresenceCreated: 0,
    claimsCreated: 0,
    compsCreated: 0,
  };

  await ensureMarketPresenceTable(baseId, token, report);

  // Existing claim IDs + presence dedupe keys
  const existingClaimIds = new Set();
  const existingPresenceKeys = new Set();
  try {
    const claimsUrl = `https://api.airtable.com/v0/${baseId}/${enc(CLAIMS_TABLE)}?pageSize=100&fields%5B%5D=${enc("Claim ID")}`;
    let offset;
    do {
      const u = offset ? `${claimsUrl}&offset=${enc(offset)}` : claimsUrl;
      const { ok, json } = await airtableFetchJson(u);
      if (!ok) break;
      for (const r of json.records || []) {
        if (r.fields?.["Claim ID"]) existingClaimIds.add(r.fields["Claim ID"]);
      }
      offset = json.offset;
    } while (offset);
  } catch {
    /* table may be empty */
  }

  try {
    const mpUrl = `https://api.airtable.com/v0/${baseId}/${enc(MARKET_PRESENCE_TABLE)}?pageSize=100`;
    let offset;
    do {
      const u = offset ? `${mpUrl}&offset=${enc(offset)}` : mpUrl;
      const { ok, json } = await airtableFetchJson(u);
      if (!ok) break;
      for (const r of json.records || []) {
        const op = (r.fields?.[MF.operator] || [])[0];
        const country = r.fields?.[MF.country];
        const pt = r.fields?.[MF.presenceType];
        if (op && country && pt) existingPresenceKeys.add(`${op}|${country}|${pt}`);
      }
      offset = json.offset;
    } while (offset);
  } catch {
    /* new table */
  }

  const existingCompNames = {};
  for (const id of WAVE2_IDS) {
    const rows = await fetchRecordsLinkedToMaster(NEW_BASE_CASE_STUDIES_TABLE, id).catch(() => []);
    existingCompNames[id] = new Set(
      rows.map((r) => String(r.fields?.property_name || "").toLowerCase()).filter(Boolean)
    );
  }

  for (const op of plan.operations) {
    if (op.applyOrSkip === "skip") {
      report.skipped.push(op);
      continue;
    }
    if (op.operationType === "create_market_presence" && existingPresenceKeys.has(op.dedupeKey)) {
      report.skipped.push({ ...op, skipReason: "duplicate presence key", applyOrSkip: "skip" });
      continue;
    }
    if (op.operationType === "create_claim" && existingClaimIds.has(op.claimId || op.dedupeKey)) {
      report.skipped.push({ ...op, skipReason: "duplicate claim id", applyOrSkip: "skip" });
      continue;
    }
    if (op.operationType === "create_comparable") {
      const key = String(op.proposedValue?.property_name || "").toLowerCase();
      if (key && existingCompNames[op.operatorId]?.has(key)) {
        report.skipped.push({ ...op, skipReason: "duplicate comparable", applyOrSkip: "skip" });
        continue;
      }
    }

    if (DRY) {
      report.applied.push({ ...op, status: "would_apply" });
      if (op.operationType === "create_market_presence") report.marketPresenceCreated += 1;
      if (op.operationType === "create_claim") report.claimsCreated += 1;
      if (op.operationType === "create_comparable") report.compsCreated += 1;
      continue;
    }

    try {
      if (op.operationType === "update_field" || op.operationType === "cenote_remediation") {
        await patchRecord(op.table, op.record, { [op.field]: op.proposedValue });
        report.applied.push({ ...op, status: "applied" });
      } else if (op.operationType === "create_market_presence") {
        await createRecord(MARKET_PRESENCE_TABLE, op.proposedValue);
        existingPresenceKeys.add(op.dedupeKey);
        report.marketPresenceCreated += 1;
        report.applied.push({ ...op, status: "applied" });
      } else if (op.operationType === "create_claim") {
        await createRecord(CLAIMS_TABLE, op.proposedValue);
        existingClaimIds.add(op.claimId);
        report.claimsCreated += 1;
        report.applied.push({ ...op, status: "applied" });
      } else if (op.operationType === "create_comparable") {
        const fields = { ...op.proposedValue };
        try {
          await createRecord(NEW_BASE_CASE_STUDIES_TABLE, fields);
        } catch {
          delete fields["Why Comparable"];
          delete fields["Comparability Strength"];
          await createRecord(NEW_BASE_CASE_STUDIES_TABLE, fields);
        }
        existingCompNames[op.operatorId]?.add(String(fields.property_name || "").toLowerCase());
        report.compsCreated += 1;
        report.applied.push({ ...op, status: "applied" });
      }
    } catch (err) {
      report.errors.push({ operationId: op.operationId, error: String(err.message || err) });
    }
  }

  writeFileSync(join(ROOT, "reports", "operator-intelligence-pilot-apply-result.json"), JSON.stringify(report, null, 2));

  // Migration markdown
  const migRows = plan.operations
    .filter((o) => o.operationType === "create_market_presence")
    .map((o) => {
      const strong = establishesCurrentGeographicEligibility(o.proposedValue?.[MF.presenceType]);
      return `| ${o.operatorName} | ${o.country} | Geography JSON / Active Countries | ${o.proposedValue?.[MF.presenceType]} | ${(o.sourceIds || []).join(", ") || "—"} | ${strong ? "Strong geo support" : "Does not establish current eligibility"} |`;
    });
  writeFileSync(
    join(ROOT, "reports", "operator-intelligence-market-presence-migration.md"),
    [
      "# Operator Intelligence — Market Presence Migration",
      "",
      `Mode: **${report.mode}** · Generated: ${report.generatedAt}`,
      "",
      "| Operator | Country | Previous Representation | Market Presence Type | Evidence | Eligibility Effect |",
      "| -------- | ------- | ----------------------- | -------------------- | -------- | ------------------ |",
      ...migRows,
      "",
      "## Schema",
      "",
      ...report.schema.map((s) => `- ${s.operation}: ${s.table}${s.field ? " / " + s.field : ""} → ${s.status}`),
      "",
    ].join("\n")
  );

  console.log(
    JSON.stringify(
      {
        mode: report.mode,
        backupDir: report.backupDir,
        applyOps: report.applied.length,
        skipOps: report.skipped.length,
        marketPresence: report.marketPresenceCreated,
        claims: report.claimsCreated,
        comps: report.compsCreated,
        errors: report.errors.length,
        planApply: plan.applyCount,
        planSkip: plan.skipCount,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error("[oi-pilot] FAILED", e);
  process.exit(1);
});
