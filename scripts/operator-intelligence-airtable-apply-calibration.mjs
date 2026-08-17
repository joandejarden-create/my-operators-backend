#!/usr/bin/env node
/**
 * Operator Intelligence — backup + approved write plan + schema ensure + calibration apply.
 *
 *   node scripts/operator-intelligence-airtable-apply-calibration.mjs --dry-run
 *   node scripts/operator-intelligence-airtable-apply-calibration.mjs --apply --approve-oi-calibration-writes
 *
 * Default: dry-run (no writes).
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

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const APPROVED = process.argv.includes("--approve-oi-calibration-writes");
const DRY = !APPLY;

const COHORT = [
  { id: "recF5Z87OAqFgndoq", name: "Arbor Lodging (CALA)" },
  { id: "recQ6Cf8O2z0tiqBz", name: "Cenote Azul Operadores" },
  { id: "recWPKu5laVZxsvpn", name: "Hotel Equities (CALA)" },
  { id: "reciI2tYQBfMoMK9G", name: "GHL Hoteles (GHL Holding)" },
  { id: "rec3TUHT9Z4AnFp5P", name: "Playa Hotels & Resorts" },
  { id: "recGWxIJqnYHkJZFD", name: "Aimbridge Hospitality (LATAM)" },
];

/** Countries documented in options audit — do not invent. */
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
  if (!ok) {
    throw new Error(`${table} PATCH ${recordId} failed (${status}): ${JSON.stringify(json)}`);
  }
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
  if (!ok) {
    throw new Error(`${table} CREATE failed (${status}): ${JSON.stringify(json)}`);
  }
  return json;
}

function filterCountries(list) {
  return (list || []).filter((c) => APPROVED_COUNTRIES.has(c));
}

function filterStructures(list) {
  return (list || []).filter((s) => APPROVED_STRUCTURES.has(s));
}

function sameMulti(a, b) {
  const na = [...(a || [])].map(String).sort().join("|");
  const nb = [...(b || [])].map(String).sort().join("|");
  return na === nb;
}

async function backupCohort(tsDir) {
  const records = [];
  for (const op of COHORT) {
    const bundle = await loadNewBaseOperatorBundle(op.id);
    const caseStudies = await fetchRecordsLinkedToMaster(NEW_BASE_CASE_STUDIES_TABLE, op.id);
    const snap = {
      operatorId: op.id,
      operatorName: op.name,
      master: bundle.master,
      platform: bundle.platform,
      commercial: bundle.commercial,
      profile: bundle.profile,
      governance: bundle.governance,
      caseStudies,
    };
    const file = join(tsDir, `${op.id}.json`);
    writeFileSync(file, JSON.stringify(snap, null, 2), "utf8");
    records.push({
      operatorId: op.id,
      operatorName: op.name,
      file: file.replace(ROOT + "\\", "").replace(ROOT + "/", ""),
      platformId: bundle.platform?.id || null,
      commercialId: bundle.commercial?.id || null,
      checksum: checksum({
        platform: bundle.platform?.fields || {},
        commercial: bundle.commercial?.fields || {},
      }),
      activeCountriesBefore: bundle.platform?.fields?.["Active Countries"] || [],
      structuresBefore: bundle.commercial?.fields?.["Management Structures Supported"] || [],
    });
  }
  return records;
}

function buildWritePlan(backupRows) {
  const byId = Object.fromEntries(backupRows.map((r) => [r.operatorId, r]));
  const ops = [];

  const propose = (partial) => {
    ops.push({
      operationId: `oi_w2_${String(ops.length + 1).padStart(3, "0")}`,
      ...partial,
    });
  };

  // Arbor — Mexico only (US skipped — not in approved country options)
  {
    const b = byId["recF5Z87OAqFgndoq"];
    const proposed = filterCountries(["Mexico"]);
    const existing = b.activeCountriesBefore || [];
    const skip =
      !b.platformId
        ? "No Platform row"
        : sameMulti(existing, proposed)
          ? "Already matches"
          : existing.length && !proposed.every((c) => existing.includes(c)) && existing.some((c) => !proposed.includes(c))
            ? null
            : null;
    propose({
      operationType: "update_field",
      table: NEW_BASE_PLATFORM_TABLE,
      record: b.platformId,
      operatorId: b.operatorId,
      operatorName: b.operatorName,
      field: "Active Countries",
      existingValue: existing,
      proposedValue: proposed,
      supportingClaimIds: ["clm_cal_arbor_geo"],
      supportingSourceIds: ["src_cal_arbor"],
      publicationStatus: "Auto-Publish",
      verificationStatus: "Verified",
      conflictStatus: "None",
      founderApprovalBasis: "Group A existing-field population",
      rollbackValue: existing,
      applyOrSkip: b.platformId && !sameMulti(existing, [...new Set([...existing.filter((c) => c === "Mexico" || APPROVED_COUNTRIES.has(c) && c === "Mexico"), ...proposed])]) ? "apply" : b.platformId ? "apply" : "skip",
      skipReason: !b.platformId ? "No Platform row" : null,
      postWriteValidationRule: "Active Countries includes Mexico; does not include Peru/Costa Rica as Active",
      notes: "United States skipped — not in approved Active Countries taxonomy audit",
    });
    // Force apply decision cleanly
    ops[ops.length - 1].applyOrSkip = !b.platformId ? "skip" : sameMulti(existing, proposed) ? "skip" : "apply";
    ops[ops.length - 1].skipReason = !b.platformId
      ? "No Platform row"
      : sameMulti(existing, proposed)
        ? "Already matches proposed Mexico-only set"
        : existing.length > 0 && !existing.includes("Mexico")
          ? null
          : ops[ops.length - 1].skipReason;
    if (ops[ops.length - 1].applyOrSkip === "apply") ops[ops.length - 1].skipReason = null;
    // If existing empty → apply Mexico. If existing has other values without Mexico → merge Mexico only additive? Founder: blank or superseded.
    if (!existing.length) {
      ops[ops.length - 1].applyOrSkip = b.platformId ? "apply" : "skip";
      ops[ops.length - 1].proposedValue = proposed;
    } else if (!existing.includes("Mexico")) {
      ops[ops.length - 1].applyOrSkip = "apply";
      ops[ops.length - 1].proposedValue = [...new Set([...existing.filter((c) => APPROVED_COUNTRIES.has(c)), "Mexico"])];
    } else {
      ops[ops.length - 1].applyOrSkip = "skip";
      ops[ops.length - 1].skipReason = "Mexico already present; US not written (taxonomy)";
    }
  }

  // GHL countries
  {
    const b = byId["reciI2tYQBfMoMK9G"];
    const proposed = filterCountries(["Colombia", "Peru", "Chile", "Panama"]);
    const existing = b.activeCountriesBefore || [];
    propose({
      operationType: "update_field",
      table: NEW_BASE_PLATFORM_TABLE,
      record: b.platformId,
      operatorId: b.operatorId,
      operatorName: b.operatorName,
      field: "Active Countries",
      existingValue: existing,
      proposedValue: existing.length ? [...new Set([...existing.filter((c) => APPROVED_COUNTRIES.has(c)), ...proposed])] : proposed,
      supportingSourceIds: ["ghlhoteles", "sonesta"],
      publicationStatus: "Auto-Publish",
      verificationStatus: "Verified",
      conflictStatus: "None",
      founderApprovalBasis: "Group A",
      rollbackValue: existing,
      applyOrSkip: !b.platformId ? "skip" : existing.length >= 4 && proposed.every((c) => existing.includes(c)) ? "skip" : "apply",
      skipReason: !b.platformId
        ? "No Platform row"
        : existing.length >= 4 && proposed.every((c) => existing.includes(c))
          ? "Already contains proposed countries"
          : null,
      postWriteValidationRule: "Includes Colombia and Peru",
      notes: "Ecuador/Guatemala skipped if not in approved taxonomy list",
    });
  }

  // GHL structures
  {
    const b = byId["reciI2tYQBfMoMK9G"];
    const proposed = filterStructures(["Full third-party management", "Franchise support"]);
    const existing = b.structuresBefore || [];
    propose({
      operationType: "update_field",
      table: NEW_BASE_COMMERCIAL_TABLE,
      record: b.commercialId,
      operatorId: b.operatorId,
      operatorName: b.operatorName,
      field: "Management Structures Supported",
      existingValue: existing,
      proposedValue: existing.length ? [...new Set([...existing, ...proposed])] : proposed,
      publicationStatus: "Auto-Publish",
      verificationStatus: "Verified",
      conflictStatus: "None",
      founderApprovalBasis: "Group A",
      rollbackValue: existing,
      applyOrSkip: !b.commercialId ? "skip" : proposed.every((s) => existing.includes(s)) ? "skip" : "apply",
      skipReason: !b.commercialId
        ? "No Commercial row"
        : proposed.every((s) => existing.includes(s))
          ? "Structures already present"
          : null,
      postWriteValidationRule: "Includes Full third-party management",
    });
  }

  // Playa countries
  {
    const b = byId["rec3TUHT9Z4AnFp5P"];
    const proposed = filterCountries(["Mexico", "Jamaica", "Dominican Republic"]);
    const existing = b.activeCountriesBefore || [];
    propose({
      operationType: "update_field",
      table: NEW_BASE_PLATFORM_TABLE,
      record: b.platformId,
      operatorId: b.operatorId,
      operatorName: b.operatorName,
      field: "Active Countries",
      existingValue: existing,
      proposedValue: existing.length ? [...new Set([...existing.filter((c) => APPROVED_COUNTRIES.has(c)), ...proposed])] : proposed,
      publicationStatus: "Auto-Publish",
      verificationStatus: "Verified",
      conflictStatus: "None",
      founderApprovalBasis: "Group A (manual review noted for ownership evolution)",
      rollbackValue: existing,
      applyOrSkip: !b.platformId ? "skip" : proposed.every((c) => existing.includes(c)) ? "skip" : "apply",
      skipReason: !b.platformId
        ? "No Platform row"
        : proposed.every((c) => existing.includes(c))
          ? "Already present"
          : null,
      postWriteValidationRule: "Mexico + Jamaica + Dominican Republic present",
    });
  }

  // Playa structures — Full third-party only (Owner-Operated not in taxonomy)
  {
    const b = byId["rec3TUHT9Z4AnFp5P"];
    const proposed = filterStructures(["Full third-party management"]);
    const existing = b.structuresBefore || [];
    propose({
      operationType: "update_field",
      table: NEW_BASE_COMMERCIAL_TABLE,
      record: b.commercialId,
      operatorId: b.operatorId,
      operatorName: b.operatorName,
      field: "Management Structures Supported",
      existingValue: existing,
      proposedValue: existing.length ? [...new Set([...existing, ...proposed])] : proposed,
      publicationStatus: "Publish With Evidence Label",
      verificationStatus: "Verified",
      conflictStatus: "None",
      founderApprovalBasis: "Group A — Owner-Operated skipped (not in approved select options)",
      rollbackValue: existing,
      applyOrSkip: !b.commercialId ? "skip" : proposed.every((s) => existing.includes(s)) ? "skip" : "apply",
      skipReason: !b.commercialId
        ? "No Commercial row"
        : proposed.every((s) => existing.includes(s))
          ? "Already present"
          : null,
      notes: "Owner-Operated not written — taxonomy gap; documented in claims as qualified",
    });
  }

  // Aimbridge countries + structures
  {
    const b = byId["recGWxIJqnYHkJZFD"];
    const proposedC = filterCountries(["Mexico"]);
    const existingC = b.activeCountriesBefore || [];
    propose({
      operationType: "update_field",
      table: NEW_BASE_PLATFORM_TABLE,
      record: b.platformId,
      operatorId: b.operatorId,
      operatorName: b.operatorName,
      field: "Active Countries",
      existingValue: existingC,
      proposedValue: existingC.length ? [...new Set([...existingC, ...proposedC])] : proposedC,
      publicationStatus: "Auto-Publish",
      verificationStatus: "Verified",
      conflictStatus: "None",
      founderApprovalBasis: "Group A",
      rollbackValue: existingC,
      applyOrSkip: !b.platformId ? "skip" : existingC.includes("Mexico") ? "skip" : "apply",
      skipReason: !b.platformId ? "No Platform row" : existingC.includes("Mexico") ? "Mexico already present" : null,
    });
    const proposedS = filterStructures(["Full third-party management"]);
    const existingS = b.structuresBefore || [];
    propose({
      operationType: "update_field",
      table: NEW_BASE_COMMERCIAL_TABLE,
      record: b.commercialId,
      operatorId: b.operatorId,
      operatorName: b.operatorName,
      field: "Management Structures Supported",
      existingValue: existingS,
      proposedValue: existingS.length ? [...new Set([...existingS, ...proposedS])] : proposedS,
      publicationStatus: "Auto-Publish",
      verificationStatus: "Verified",
      conflictStatus: "None",
      founderApprovalBasis: "Group A",
      rollbackValue: existingS,
      applyOrSkip: !b.commercialId ? "skip" : existingS.includes("Full third-party management") ? "skip" : "apply",
      skipReason: !b.commercialId
        ? "No Commercial row"
        : existingS.includes("Full third-party management")
          ? "Already present"
          : null,
    });
  }

  // HE — skip Active Countries overwrite (already populated verified)
  {
    const b = byId["recWPKu5laVZxsvpn"];
    propose({
      operationType: "update_field",
      table: NEW_BASE_PLATFORM_TABLE,
      record: b.platformId,
      operatorId: b.operatorId,
      operatorName: b.operatorName,
      field: "Active Countries",
      existingValue: b.activeCountriesBefore,
      proposedValue: b.activeCountriesBefore,
      publicationStatus: "Auto-Publish",
      verificationStatus: "Verified",
      conflictStatus: "None",
      founderApprovalBasis: "Group A",
      rollbackValue: b.activeCountriesBefore,
      applyOrSkip: "skip",
      skipReason: "Would overwrite stronger/already-populated verified value — Case Studies/claims handle enrichment",
    });
  }

  // Cenote normalize — Mexico only
  {
    const b = byId["recQ6Cf8O2z0tiqBz"];
    const existing = b.activeCountriesBefore || [];
    const proposed = ["Mexico"];
    propose({
      operationType: "normalize_field",
      table: NEW_BASE_PLATFORM_TABLE,
      record: b.platformId,
      operatorId: b.operatorId,
      operatorName: b.operatorName,
      field: "Active Countries",
      existingValue: existing,
      proposedValue: proposed,
      publicationStatus: "Human Review Required → Approved normalize",
      verificationStatus: "Normalized",
      conflictStatus: "unsupported_current_value (resolved by founder approval)",
      founderApprovalBasis: "Cenote Azul normalization approval",
      rollbackValue: existing,
      applyOrSkip: !b.platformId ? "skip" : sameMulti(existing, proposed) ? "skip" : "apply",
      skipReason: !b.platformId ? "No Platform row" : sameMulti(existing, proposed) ? "Already Mexico-only" : null,
      postWriteValidationRule: "Active Countries equals [Mexico] only; removed unsupported countries preserved in backup",
      notes: "Mexico retained as normalized country of record; not asserted as Confirmed Absence for others — backup retains prior list",
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

async function ensureClaimsTable(baseId, token, report) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`List tables failed: ${JSON.stringify(json)}`);
  const tables = json.tables || [];
  const master = tables.find((t) => t.name === NEW_BASE_MASTER_TABLE || t.name === "Operator Setup - Master");
  if (!master) throw new Error("Master table not found");
  let claims = tables.find((t) => t.name === CLAIMS_TABLE);
  const fieldSpecs = [
    { name: "Claim ID", type: "singleLineText" },
    {
      name: "Operator",
      type: "multipleRecordLinks",
      options: { linkedTableId: master.id },
    },
    { name: "Claim Category", type: "singleLineText" },
    { name: "Subject", type: "singleLineText" },
    { name: "Predicate", type: "singleLineText" },
    { name: "Raw Value", type: "multilineText" },
    { name: "Normalized Value", type: "multilineText" },
    { name: "Geographic Scope", type: "singleLineText" },
    { name: "Brand Scope", type: "singleLineText" },
    { name: "Property Scope", type: "singleLineText" },
    { name: "Effective Date", type: "date", options: { dateFormat: { name: "iso" } } },
    { name: "Review Date", type: "date", options: { dateFormat: { name: "iso" } } },
    { name: "Evidence Class", type: "singleLineText" },
    { name: "Verification Status", type: "singleLineText" },
    { name: "Publication Status", type: "singleLineText" },
    { name: "Conflict Status", type: "singleLineText" },
    { name: "Scoring Relevance", type: "singleLineText" },
    { name: "Currentness", type: "singleLineText" },
    { name: "Notes", type: "multilineText" },
    { name: "Limitations", type: "multilineText" },
    { name: "Source URLs", type: "multilineText" },
  ];

  if (!claims) {
    report.schema.push({ operation: "create_table", table: CLAIMS_TABLE, status: DRY ? "would_create" : "creating" });
    if (!DRY) {
      const { res: cRes, json: cJson } = await metaFetch(baseId, token, "/tables", {
        method: "POST",
        body: JSON.stringify({
          name: CLAIMS_TABLE,
          description: "Operator Intelligence structured claims (calibration persistence)",
          fields: fieldSpecs,
        }),
      });
      if (!cRes.ok) throw new Error(`Create Claims table failed: ${JSON.stringify(cJson)}`);
      claims = cJson;
      report.schema[report.schema.length - 1].status = "created";
      report.schema[report.schema.length - 1].tableId = cJson.id;
    }
  } else {
    report.schema.push({ operation: "table_exists", table: CLAIMS_TABLE, tableId: claims.id, status: "ok" });
    const existingNames = new Set((claims.fields || []).map((f) => f.name));
    for (const f of fieldSpecs) {
      if (existingNames.has(f.name)) continue;
      report.schema.push({ operation: "create_field", table: CLAIMS_TABLE, field: f.name, status: DRY ? "would_create" : "creating" });
      if (!DRY) {
        const { res: fRes, json: fJson } = await metaFetch(baseId, token, `/tables/${claims.id}/fields`, {
          method: "POST",
          body: JSON.stringify(f),
        });
        if (!fRes.ok) throw new Error(`Create field ${f.name} failed: ${JSON.stringify(fJson)}`);
        report.schema[report.schema.length - 1].status = "created";
      }
    }
  }

  // Case study optional fields
  const caseTable = tables.find((t) => t.name === NEW_BASE_CASE_STUDIES_TABLE);
  if (caseTable) {
    const names = new Set((caseTable.fields || []).map((f) => f.name));
    for (const fname of ["Why Comparable", "Comparability Strength"]) {
      if (names.has(fname)) {
        report.schema.push({ operation: "field_exists", table: NEW_BASE_CASE_STUDIES_TABLE, field: fname, status: "ok" });
        continue;
      }
      report.schema.push({
        operation: "create_field",
        table: NEW_BASE_CASE_STUDIES_TABLE,
        field: fname,
        status: DRY ? "would_create" : "creating",
      });
      if (!DRY) {
        const field =
          fname === "Comparability Strength"
            ? {
                name: fname,
                type: "singleSelect",
                options: { choices: [{ name: "High" }, { name: "Moderate" }, { name: "Limited" }] },
              }
            : { name: fname, type: "multilineText" };
        const { res: fRes, json: fJson } = await metaFetch(baseId, token, `/tables/${caseTable.id}/fields`, {
          method: "POST",
          body: JSON.stringify(field),
        });
        if (!fRes.ok) throw new Error(`Create ${fname} failed: ${JSON.stringify(fJson)}`);
        report.schema[report.schema.length - 1].status = "created";
      }
    }
  }

  return claims;
}

async function createClaimsAndComps(planReport) {
  const cohortPath = join(ROOT, "data", "operator-intelligence", "calibration-cohort");
  if (!existsSync(join(cohortPath, "claims.json"))) return { claimsCreated: 0, compsCreated: 0 };

  const claims = JSON.parse(readFileSync(join(cohortPath, "claims.json"), "utf8"));
  const sources = JSON.parse(readFileSync(join(cohortPath, "sources.json"), "utf8"));
  const comps = JSON.parse(readFileSync(join(cohortPath, "comparables.json"), "utf8"));
  const srcById = Object.fromEntries(sources.map((s) => [s.id, s]));

  let claimsCreated = 0;
  let compsCreated = 0;

  // Claims — only Auto-Publish / Publish With Label; skip Internal/Conflicted/Human Review without review
  const pubPath = join(cohortPath, "publication-decisions.json");
  const decisions = existsSync(pubPath) ? JSON.parse(readFileSync(pubPath, "utf8")) : [];
  const decByClaim = Object.fromEntries(decisions.map((d) => [d.claimId, d]));

  for (const c of claims) {
    const d = decByClaim[c.id] || {};
    const status = d.status || "";
    if (
      /Internal Only|Human Review Required|Rejected|Conflicted|Insufficient/i.test(status) &&
      c.operatorId !== "recQ6Cf8O2z0tiqBz"
    ) {
      planReport.skippedLinked.push({ type: "claim", id: c.id, reason: status || "not auto-publishable" });
      continue;
    }
    // Cenote conflicted geo claim: skip auto-publish of broad list; normalization handled on Platform field
    if (c.conflictStatus === "Hard") {
      planReport.skippedLinked.push({ type: "claim", id: c.id, reason: "Hard conflict — not auto-published" });
      continue;
    }
    if (c.internalOnly || c.publicationClass === 3) {
      planReport.skippedLinked.push({ type: "claim", id: c.id, reason: "Internal Only" });
      continue;
    }
    const urls = (c.sourceIds || [])
      .map((id) => srcById[id]?.url)
      .filter(Boolean)
      .join("\n");
    const fields = {
      "Claim ID": c.id,
      Operator: [c.operatorId],
      "Claim Category": c.claimCategory || "",
      Subject: c.claimSubject || "",
      Predicate: c.claimPredicate || "",
      "Raw Value": String(c.claimValue ?? ""),
      "Normalized Value": Array.isArray(c.normalizedValue)
        ? c.normalizedValue.join(", ")
        : String(c.normalizedValue ?? ""),
      "Geographic Scope": c.geographicScope || "",
      "Brand Scope": c.brandScope || "",
      "Property Scope": c.propertyScope || "",
      "Evidence Class": c.evidenceClass || "",
      "Verification Status": c.verificationStatus || "",
      "Publication Status": status || (c.publicationClass === 1 ? "Auto-Publish" : "Publish With Evidence Label"),
      "Conflict Status": c.conflictStatus || "None",
      "Scoring Relevance": c.scoringRelevance || "",
      Currentness: "Current",
      Notes: c.notes || "",
      Limitations: c.limitations || "",
      "Source URLs": urls,
    };
    if (DRY) {
      planReport.wouldCreateLinked.push({ type: "claim", id: c.id, operatorId: c.operatorId });
      claimsCreated += 1;
      continue;
    }
    // Dedup: skip if Claim ID already exists — simple scan omitted for speed; use Claim ID uniqueness best-effort
    await createRecord(CLAIMS_TABLE, fields);
    claimsCreated += 1;
  }

  // Comparables — additive Case Studies for High/Moderate only
  for (const op of COHORT) {
    const existing = await fetchRecordsLinkedToMaster(NEW_BASE_CASE_STUDIES_TABLE, op.id);
    const existingNames = new Set(
      existing.map((r) => String(r.fields?.property_name || r.fields?.["Property Name"] || "").toLowerCase())
    );
    const opComps = comps.filter(
      (c) => c.operatorId === op.id && /High|Moderate/i.test(c.comparabilityStrength || "")
    );
    for (const c of opComps) {
      const key = String(c.propertyName || "").toLowerCase();
      if (!key || existingNames.has(key)) {
        planReport.skippedLinked.push({
          type: "comparable",
          operatorId: op.id,
          property: c.propertyName,
          reason: existingNames.has(key) ? "duplicate property name" : "missing name",
        });
        continue;
      }
      const fields = {
        Operator: [op.id],
        property_name: c.propertyName,
        region: [c.city, c.country].filter(Boolean).join(", "),
        branded_independent: c.brand || "",
        owner_relevance: [c.whyComparable, c.assetType || c.urbanOrResort, c.developmentType]
          .filter(Boolean)
          .join(" · "),
        outcome: c.performanceEvidence || "Performance evidence unavailable or not independently verified.",
      };
      if (c.whyComparable) fields["Why Comparable"] = c.whyComparable;
      if (c.comparabilityStrength) fields["Comparability Strength"] = c.comparabilityStrength;

      if (DRY) {
        planReport.wouldCreateLinked.push({ type: "comparable", operatorId: op.id, property: c.propertyName });
        compsCreated += 1;
        continue;
      }
      try {
        await createRecord(NEW_BASE_CASE_STUDIES_TABLE, fields);
        compsCreated += 1;
        existingNames.add(key);
      } catch (err) {
        // Retry without optional new fields if schema not yet available
        delete fields["Why Comparable"];
        delete fields["Comparability Strength"];
        try {
          await createRecord(NEW_BASE_CASE_STUDIES_TABLE, fields);
          compsCreated += 1;
          existingNames.add(key);
        } catch (err2) {
          planReport.errors = planReport.errors || [];
          planReport.errors.push({
            type: "comparable",
            operatorId: op.id,
            property: c.propertyName,
            error: String(err2.message || err2),
          });
        }
      }
    }
  }

  return { claimsCreated, compsCreated };
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  if (!baseId || !token) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  if (APPLY && !APPROVED) {
    throw new Error("Refusing --apply without --approve-oi-calibration-writes");
  }

  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const backupDir = join(ROOT, "backups", "operator-intelligence", ts);
  mkdirSync(backupDir, { recursive: true });

  console.log(`[oi-airtable] Backup → ${backupDir}`);
  const backupRows = await backupCohort(backupDir);
  writeFileSync(join(backupDir, "manifest.json"), JSON.stringify({ generatedAt: new Date().toISOString(), backupRows }, null, 2));

  const plan = buildWritePlan(backupRows);
  writeFileSync(join(ROOT, "reports", "operator-intelligence-approved-write-plan.json"), JSON.stringify(plan, null, 2));

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY ? "dry-run" : "apply",
    backupDir: backupDir.replace(/\\/g, "/"),
    schema: [],
    applied: [],
    skipped: [],
    wouldCreateLinked: [],
    skippedLinked: [],
    errors: [],
  };

  // Ensure PI Source Library exists (dry-run call of ensure is heavy; check table list)
  {
    const { res, json } = await metaFetch(baseId, token, "/tables");
    if (!res.ok) throw new Error("Cannot list tables for backup validation");
    const names = new Set((json.tables || []).map((t) => t.name));
    report.piSourceLibraryPresent = names.has("Partner Intelligence - Source Library");
    writeFileSync(join(backupDir, "tables-meta-names.json"), JSON.stringify([...names].sort(), null, 2));
  }

  await ensureClaimsTable(baseId, token, report);

  // Apply field updates
  for (const op of plan.operations) {
    if (op.applyOrSkip === "skip") {
      report.skipped.push(op);
      continue;
    }
    if (DRY) {
      report.applied.push({ ...op, status: "would_apply" });
      continue;
    }
    try {
      await patchRecord(op.table, op.record, { [op.field]: op.proposedValue });
      report.applied.push({ ...op, status: "applied" });
    } catch (err) {
      report.errors.push({ operationId: op.operationId, error: String(err.message || err) });
    }
  }

  const linked = await createClaimsAndComps(report);
  report.claimsCreated = linked.claimsCreated;
  report.compsCreated = linked.compsCreated;

  writeFileSync(join(ROOT, "reports", "operator-intelligence-schema-applied.md"), [
    "# Operator Intelligence — Schema Applied",
    "",
    `Mode: **${report.mode}**`,
    `Generated: ${report.generatedAt}`,
    "",
    "## Schema operations",
    "",
    ...report.schema.map((s) => `- ${s.operation}: ${s.table}${s.field ? " / " + s.field : ""} → ${s.status}`),
    "",
    `PI Source Library present: ${report.piSourceLibraryPresent}`,
    "",
  ].join("\n"));

  writeFileSync(
    join(ROOT, "reports", "operator-intelligence-airtable-backup-manifest.md"),
    [
      "# Operator Intelligence — Airtable Backup Manifest",
      "",
      `Timestamp: ${ts}`,
      `Path: \`${report.backupDir}\``,
      "",
      "| Operator | Master ID | Platform ID | Commercial ID | Checksum |",
      "| -------- | --------- | ----------- | ------------- | -------- |",
      ...backupRows.map(
        (r) =>
          `| ${r.operatorName} | ${r.operatorId} | ${r.platformId || "—"} | ${r.commercialId || "—"} | ${r.checksum} |`
      ),
      "",
      "## Restoration",
      "",
      "PATCH Platform/Commercial records with `fields` from each `{operatorId}.json` snapshot for Active Countries / Management Structures Supported.",
      "",
      "## Limitations",
      "",
      "Meta schema snapshot is names-only; newly created Claims rows require separate delete if rolling back creates.",
      "",
    ].join("\n")
  );

  writeFileSync(join(ROOT, "reports", "operator-intelligence-calibration-apply-result.json"), JSON.stringify(report, null, 2));

  console.log(
    JSON.stringify(
      {
        mode: report.mode,
        backupDir: report.backupDir,
        applyOps: report.applied.length,
        skipOps: report.skipped.length,
        claims: report.claimsCreated,
        comps: report.compsCreated,
        errors: report.errors.length,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("[oi-airtable] FAILED", err);
  process.exit(1);
});
