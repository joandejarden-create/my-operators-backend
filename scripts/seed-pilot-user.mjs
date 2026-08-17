/**
 * Seed pilot owner Users rows (no Memberstack — link MS later).
 *
 *   node scripts/seed-pilot-user.mjs --email dfernandez@northblueventures.com
 *   node scripts/seed-pilot-user.mjs --apply --email dfernandez@northblueventures.com
 */
import "../load-env.js";
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  PUF,
  PLATFORM_USERS_TABLE_ID,
  REGION_CODE_TO_CHECKBOX_FIELDS,
} from "../lib/airtable/platform-users-table.js";
import { getUsersStatusFieldCandidates } from "../lib/pilot-provisioning/pilot-field-registry.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TABLE = PLATFORM_USERS_TABLE_ID;
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const apply = process.argv.includes("--apply");

function parseEmailArg() {
  const idx = process.argv.indexOf("--email");
  return idx >= 0 ? String(process.argv[idx + 1] || "").trim().toLowerCase() : "";
}

function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

async function fetchByEmail(email) {
  const lit = email.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  const formula = `LOWER({Email}) = '${lit}'`;
  const url =
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}` +
    `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const data = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(data));
  return (data.records || [])[0] || null;
}

function extractUnknownFieldName(errBody) {
  const msg =
    typeof errBody === "string"
      ? errBody
      : errBody?.error?.message
        ? String(errBody.error.message)
        : JSON.stringify(errBody || "");
  const match = msg.match(/Unknown field name:\s*"([^"]+)"/i);
  return match ? match[1] : "";
}

function buildFieldsFromSeed(form, companyProfileId) {
  const fields = {};
  const em = normalizeEmail(form.email);
  if (form.firstName) fields[PUF.firstName] = String(form.firstName).trim();
  if (form.lastName) fields[PUF.lastName] = String(form.lastName).trim();
  if (em) {
    fields[PUF.email] = em;
    fields[PUF.companyEmail] = em;
  }
  if (companyProfileId?.startsWith("rec")) {
    fields[PUF.companyProfile] = [companyProfileId];
  }
  if (form.companyTitle) fields[PUF.companyTitle] = String(form.companyTitle).trim();
  if (form.platformRole) fields[PUF.platformRole] = String(form.platformRole).trim();
  if (form.contactVisibility) fields[PUF.contactVisibility] = String(form.contactVisibility).trim();
  if (form.country) fields[PUF.country] = String(form.country).trim();
  if (form.companyName) fields["Company Name"] = String(form.companyName).trim();
  if (form.userType) fields["User Type"] = String(form.userType).trim();

  const statusField = getUsersStatusFieldCandidates()[0] || "Account Status";
  if (form.accountStatus) fields[statusField] = String(form.accountStatus).trim();

  if (Array.isArray(form.regionFocus) && form.regionFocus.length) {
    const codes = new Set(form.regionFocus.map((c) => String(c).trim().toUpperCase()));
    for (const [code, columnNames] of Object.entries(REGION_CODE_TO_CHECKBOX_FIELDS)) {
      const checked = codes.has(code);
      for (const col of columnNames) fields[col] = checked;
    }
  }

  return fields;
}

async function writeRecord(id, fields) {
  const working = { ...fields };
  const removed = [];
  const maxRetries = Math.max(20, Object.keys(working).length + 5);
  let attempts = 0;

  while (attempts <= maxRetries) {
    const url = id
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}/${id}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}`;
    const res = await fetch(url, {
      method: id ? "PATCH" : "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: working, typecast: true }),
    });
    const json = await res.json();
    if (res.ok) return { record: json, removed };
    const unknown = extractUnknownFieldName(json);
    if (!unknown || !Object.prototype.hasOwnProperty.call(working, unknown)) {
      throw new Error(JSON.stringify(json));
    }
    delete working[unknown];
    if (!removed.includes(unknown)) removed.push(unknown);
    attempts += 1;
  }
  throw new Error(`No writable fields left for ${id || "new record"}`);
}

async function main() {
  if (!baseId || !apiKey) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const emailFilter = parseEmailArg();
  const seedPath = join(__dirname, "../data/pilot-users-seed.json");
  const seed = JSON.parse(readFileSync(seedPath, "utf8"));
  let users = seed.users || [];
  if (emailFilter) {
    users = users.filter((u) => normalizeEmail(u.matchEmail || u.form?.email) === emailFilter);
    if (!users.length) {
      console.error(`No seed user matched --email ${emailFilter}`);
      process.exit(1);
    }
  }

  const log = { mode: apply ? "apply" : "dry-run", results: [] };

  for (const entry of users) {
    const form = entry.form || {};
    const email = normalizeEmail(entry.matchEmail || form.email);
    const companyProfileId = entry.companyProfileAirtableId || form.companyProfileId;
    const existing = email ? await fetchByEmail(email) : null;
    const fields = buildFieldsFromSeed(form, companyProfileId);

    const result = {
      pilotId: entry.pilotId,
      email,
      action: existing ? "update" : "create",
      recordId: existing?.id || null,
      companyProfileId,
      accountStatus: form.accountStatus,
      memberstackLinked: false,
      fieldCount: Object.keys(fields).length,
      fieldsPreview: fields,
    };

    if (apply) {
      try {
        const { record, removed } = await writeRecord(existing?.id || "", fields);
        result.recordId = record.id;
        result.status = "ok";
        if (removed.length) result.removedUnknownFields = removed;
      } catch (err) {
        result.status = "error";
        result.error = String(err.message || err);
      }
    }

    log.results.push(result);
    console.log(
      `${result.action.toUpperCase()} ${email}`,
      result.recordId ? `(${result.recordId})` : "",
      result.status === "error" ? `[ERROR: ${result.error}]` : ""
    );
    console.log("  Company Profile:", companyProfileId);
    console.log("  Account Status:", form.accountStatus, "(Memberstack: not linked)");
  }

  const reportPath = join(__dirname, "../reports/pilot-users-seed-log.json");
  writeFileSync(reportPath, JSON.stringify(log, null, 2));
  console.log(`\nWrote ${reportPath}`);
  console.log(apply ? "Applied to Airtable." : "Dry run only — pass --apply to write.");

  const failed = log.results.filter((r) => r.status === "error");
  if (failed.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
