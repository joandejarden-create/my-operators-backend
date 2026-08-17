/**
 * Seed / update Company Profile records for CALA hospitality operators.
 * Uses verified public data in data/cala-company-profiles-seed.json (no logo).
 *
 *   node scripts/seed-cala-company-profiles.mjs
 *   node scripts/seed-cala-company-profiles.mjs --apply
 *   node scripts/seed-cala-company-profiles.mjs --apply --name "Grupo Posadas"
 */
import "../load-env.js";
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { formToAirtableFields } from "../api/company-profile.js";
import { finalizeCompanyProfileFieldsForAirtableWrite } from "../lib/company-profile-owner-operator-fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TABLE = process.env.COMPANY_PROFILE_TABLE_ID || "tblItyfH6MlOnMKZ9";
const BRAND_TABLE = "Brand Setup - Brand Basics";
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const apply = process.argv.includes("--apply");
const onlyName = (() => {
  const idx = process.argv.indexOf("--name");
  return idx >= 0 ? process.argv[idx + 1] : "";
})();

function normalizeNameKey(name) {
  return String(name || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

async function fetchAll(tableIdOrName) {
  const records = [];
  let offset;
  for (;;) {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableIdOrName)}?${qs}`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    records.push(...(json.records || []));
    offset = json.offset;
    if (!offset) break;
  }
  return records;
}

function buildBrandIndex(brandRecords) {
  const byExact = new Map();
  for (const rec of brandRecords) {
    const name = String(rec.fields["Brand Name"] || "").trim();
    if (!name) continue;
    byExact.set(normalizeNameKey(name), rec.id);
  }
  return byExact;
}

function resolveBrandIds(brandNames, brandIndex) {
  const ids = [];
  const missing = [];
  const seen = new Set();
  for (const raw of brandNames || []) {
    const key = normalizeNameKey(raw);
    const id = brandIndex.get(key);
    if (id && !seen.has(id)) {
      ids.push(id);
      seen.add(id);
      continue;
    }
    missing.push(raw);
  }
  return { ids, missing };
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

async function writeRecord(id, fields) {
  const working = { ...fields };
  const removed = [];
  const maxRetries = Math.max(20, Object.keys(working).length + 5);
  let attempts = 0;

  while (attempts <= maxRetries) {
    finalizeCompanyProfileFieldsForAirtableWrite(working, { loud: false });
    const url = id
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}/${id}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}`;
    const res = await fetch(url, {
      method: id ? "PATCH" : "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: working, typecast: false }),
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

function bodyFromSeed(form, brandIds) {
  const body = {
    ...form,
    companyCapabilitiesJson: JSON.stringify(form.companyCapabilities || []),
    potentialConflictFlagsJson: JSON.stringify(form.potentialConflictFlags || []),
    brandsOperateSupport: brandIds.join(","),
  };
  delete body.companyCapabilities;
  delete body.potentialConflictFlags;
  delete body.brandNames;
  return body;
}

async function main() {
  if (!baseId || !apiKey) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const seedPath = join(__dirname, "../data/cala-company-profiles-seed.json");
  const seed = JSON.parse(readFileSync(seedPath, "utf8"));
  let profiles = seed.profiles || [];
  if (onlyName) {
    const key = normalizeNameKey(onlyName);
    profiles = profiles.filter(
      (p) =>
        normalizeNameKey(p.matchName) === key ||
        normalizeNameKey(p.form?.companyName) === key
    );
    if (!profiles.length) {
      console.error(`No seed profile matched --name ${onlyName}`);
      process.exit(1);
    }
  }

  const [companyRecords, brandRecords] = await Promise.all([
    fetchAll(TABLE),
    fetchAll(BRAND_TABLE),
  ]);
  const brandIndex = buildBrandIndex(brandRecords);
  const byName = new Map(
    companyRecords.map((r) => [normalizeNameKey(r.fields["Company Name"]), r])
  );

  const log = { mode: apply ? "apply" : "dry-run", results: [] };

  for (const profile of profiles) {
    const form = profile.form || {};
    const matchKey = normalizeNameKey(profile.matchName || form.companyName);
    let record = profile.airtableId
      ? companyRecords.find((r) => r.id === profile.airtableId)
      : byName.get(matchKey);

    if (!record && profile.createIfMissing !== false) {
      record = byName.get(normalizeNameKey(form.companyName));
    }

    const { ids: brandIds, missing: missingBrands } = resolveBrandIds(
      form.brandNames,
      brandIndex
    );

    const body = bodyFromSeed(form, brandIds);
    const fields = formToAirtableFields(body);

  const entry = {
      matchName: profile.matchName,
      companyName: form.companyName,
      action: record ? "update" : "create",
      recordId: record?.id || null,
      fieldCount: Object.keys(fields).length,
      brandLinks: brandIds.length,
      missingBrands,
      fieldsPreview: Object.fromEntries(
        Object.entries(fields).filter(([k]) => !k.startsWith("Primary - ") && !k.startsWith("Addl - "))
      ),
    };

    if (apply) {
      try {
        const { record: saved, removed } = await writeRecord(record?.id || "", fields);
        entry.recordId = saved.id;
        entry.status = "ok";
        if (removed.length) entry.removedUnknownFields = removed;
      } catch (err) {
        entry.status = "error";
        entry.error = String(err.message || err);
      }
    }

    log.results.push(entry);
    console.log(
      `${entry.action.toUpperCase()} ${form.companyName}`,
      entry.recordId ? `(${entry.recordId})` : "",
      entry.missingBrands?.length ? `[missing brands: ${entry.missingBrands.join(", ")}]` : ""
    );
  }

  const reportPath = join(__dirname, "../reports/cala-company-profiles-seed-log.json");
  writeFileSync(reportPath, JSON.stringify(log, null, 2));
  console.log(`\nWrote ${reportPath}`);
  console.log(apply ? "Applied to Airtable." : "Dry run only — pass --apply to write.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
