/**
 * Ensure bidirectional Owner Targets ↔ Contacts link fields in GTM base.
 *
 *   node scripts/ensure-gtm-owner-contact-links.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { GTM_OWNER_TARGET_TABLES, MAP_GTM_OWNER_TARGET } from "../lib/gtm-owner-target/field-map.js";
import {
  GTM_CONTACT_TABLE,
  MAP_GTM_CONTACT,
  VAL_GTM_CONTACT_RELEVANCE,
} from "../lib/gtm-owner-target/contact-field-map.js";
import { assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");

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

async function ensureField(baseId, token, table, fieldSpec) {
  const existing = new Set((table.fields || []).map((f) => f.name));
  if (existing.has(fieldSpec.name)) return { status: "skipped", name: fieldSpec.name };
  if (!APPLY) return { status: "would_create", name: fieldSpec.name };
  const { res, json } = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
    method: "POST",
    body: JSON.stringify(fieldSpec),
  });
  if (!res.ok) return { status: "failed", name: fieldSpec.name, error: json };
  return { status: "created", name: fieldSpec.name };
}

async function main() {
  const { apiKey, baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  const { res, json } = await metaFetch(baseId, apiKey, "/tables");
  if (!res.ok) throw new Error(`List tables failed: ${JSON.stringify(json)}`);

  const tables = json.tables || [];
  const contactsTable = tables.find((t) => t.name === GTM_CONTACT_TABLE);
  const ownerTargetsTable = tables.find((t) => t.name === GTM_OWNER_TARGET_TABLES.ownerTargets);
  if (!contactsTable || !ownerTargetsTable) {
    throw new Error(`Missing Contacts or Owner Targets table in GTM base ${baseId}`);
  }

  const results = [];

  results.push(
    await ensureField(baseId, apiKey, contactsTable, {
      name: MAP_GTM_CONTACT.ownerTargets,
      type: "multipleRecordLinks",
      options: { linkedTableId: ownerTargetsTable.id },
      description: "Linked GTM owner target rollups for this contact.",
    })
  );

  const relevanceExists = (contactsTable.fields || []).some((f) => f.name === MAP_GTM_CONTACT.contactRelevance);
  if (!relevanceExists) {
    results.push(
      await ensureField(baseId, apiKey, contactsTable, {
        name: MAP_GTM_CONTACT.contactRelevance,
        type: "singleSelect",
        options: { choices: VAL_GTM_CONTACT_RELEVANCE.map((name) => ({ name })) },
        description: "Hospitality owner-operator vs CoStar broker noise.",
      })
    );
  } else {
    results.push({ status: "skipped", name: MAP_GTM_CONTACT.contactRelevance });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    contactsTableId: contactsTable.id,
    ownerTargetsTableId: ownerTargetsTable.id,
    results,
  };

  const outPath = path.join(ROOT, "reports", "ensure-gtm-owner-contact-links.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

  for (const r of results) console.log(r.status.toUpperCase(), r.name);
  console.log("Wrote", outPath);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
