#!/usr/bin/env node
/**
 * Apply Arbor Lodging (CALA) Best Fit & Project Fit JSON to Commercial Fit & Terms.
 *
 *   node scripts/apply-arbor-cala-best-fit.mjs
 *   node scripts/apply-arbor-cala-best-fit.mjs --dry-run
 *   node scripts/apply-arbor-cala-best-fit.mjs --fixture fixtures/operator-best-fit-arbor-cala.json
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  NEW_BASE_COMMERCIAL_TABLE,
  fetchAllRecordsRest,
  airtableFetchJson,
} from "../api/lib/operator-setup-new-base-read.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recF5Z87OAqFgndoq";
const DEFAULT_FIXTURE = path.join(ROOT, "fixtures", "operator-best-fit-arbor-cala.json");

const PLACEHOLDER_RE =
  /^Select deal types Arbor pursues in CALA|^Choose values that match the CALA deals you pursue|\[Internal fill guidance/i;

function isPlaceholderCopy(value) {
  const s = nz(value);
  return s ? PLACEHOLDER_RE.test(s) : false;
}

function scrubPlaceholderFromList(values) {
  return (values || []).filter((v) => !isPlaceholderCopy(v));
}

function enc(s) {
  return encodeURIComponent(s);
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function parseArgs(argv) {
  const args = argv.slice(2);
  const dryRun = args.includes("--dry-run");
  const fixtureArg = args.find((a) => a.startsWith("--fixture="));
  const masterArg = args.find((a) => a.startsWith("--master="));
  const pos = args.filter((a) => !a.startsWith("--"));
  return {
    dryRun,
    masterId: masterArg ? masterArg.split("=")[1] : pos[0] || DEFAULT_MASTER,
    fixturePath: fixtureArg ? fixtureArg.split("=")[1] : DEFAULT_FIXTURE,
  };
}

function rowsLinkedToMaster(rows, masterId) {
  return (rows || []).filter((r) => {
    const op = r.fields && r.fields.Operator;
    return Array.isArray(op) && op.includes(masterId);
  });
}

function validateJsonField(key, value) {
  if (value == null || value === "") return { ok: true, value: "" };
  const raw = typeof value === "string" ? value : JSON.stringify(value);
  if (PLACEHOLDER_RE.test(raw)) {
    return { ok: false, error: `${key}: placeholder text is not valid JSON content` };
  }
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return { ok: false, error: `${key}: expected JSON array` };
    }
    return { ok: true, value: raw };
  } catch (e) {
    return { ok: false, error: `${key}: invalid JSON (${e.message})` };
  }
}

function buildPatch(commercialFields) {
  const patch = {};
  const failures = [];
  for (const [key, value] of Object.entries(commercialFields || {})) {
    if (value == null) continue;
    if (key.endsWith("_json")) {
      if (value === "") continue;
      const check = validateJsonField(key, value);
      if (!check.ok) {
        failures.push(check.error);
        continue;
      }
      patch[key] = check.value;
      continue;
    }
    if (Array.isArray(value)) {
      const cleaned = scrubPlaceholderFromList(value);
      if (!cleaned.length && value.length) {
        failures.push(`${key}: all multi-select values were placeholder text`);
        continue;
      }
      patch[key] = cleaned;
      continue;
    }
    const text = nz(value);
    if (!text) {
      patch[key] = "";
      continue;
    }
    if (isPlaceholderCopy(text)) {
      failures.push(`${key}: refusing placeholder instruction text`);
      continue;
    }
    patch[key] = text;
  }
  return { patch, failures };
}

async function patchCommercial(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(NEW_BASE_COMMERCIAL_TABLE)}/${enc(recordId)}`;
  return airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
}

async function main() {
  const { dryRun, masterId, fixturePath } = parseArgs(process.argv);
  if (!fs.existsSync(fixturePath)) {
    throw new Error(`Missing fixture: ${fixturePath}`);
  }
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const commercialFields = fixture.commercialFields;
  if (!commercialFields || typeof commercialFields !== "object") {
    throw new Error("Fixture missing commercialFields object");
  }

  const { patch, failures } = buildPatch(commercialFields);
  if (failures.length) {
    console.error("[arbor-best-fit] Validation failures:");
    failures.forEach((f) => console.error("  -", f));
    process.exit(1);
  }
  if (!Object.keys(patch).length) {
    throw new Error("No fields to patch after validation");
  }

  const commRows = rowsLinkedToMaster(
    await fetchAllRecordsRest(NEW_BASE_COMMERCIAL_TABLE),
    masterId
  );
  if (!commRows.length) {
    throw new Error(`No Commercial Fit row linked to master ${masterId}`);
  }
  const commId = commRows[0].id;

  console.log(
    JSON.stringify(
      {
        dryRun,
        masterId,
        commercialRecordId: commId,
        fieldCount: Object.keys(patch).length,
        fields: Object.keys(patch),
      },
      null,
      2
    )
  );

  if (dryRun) {
    console.log("[arbor-best-fit] Dry run — no Airtable writes.");
    return;
  }

  const { ok, status, json } = await patchCommercial(commId, patch);
  if (!ok) {
    const msg = json?.error?.message || JSON.stringify(json);
    throw new Error(`Commercial PATCH failed (${status}): ${msg}`);
  }
  console.log("[arbor-best-fit] Applied Best Fit fields to", commId);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
