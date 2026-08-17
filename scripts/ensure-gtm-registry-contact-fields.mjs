/**
 * Add registry verification fields to GTM Contacts table.
 *
 *   node scripts/ensure-gtm-registry-contact-fields.mjs
 *   node scripts/ensure-gtm-registry-contact-fields.mjs --apply
 *
 * Report: reports/ensure-gtm-registry-contact-fields.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  GTM_CONTACT_TABLE,
  MAP_GTM_CONTACT,
  VAL_GTM_CONTACT_VERIFICATION_TIER,
  VAL_GTM_CONTACT_VERIFICATION_SOURCE,
  VAL_GTM_PHONE_VERIFICATION_TIER,
} from "../lib/gtm-owner-target/contact-field-map.js";
import { assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}

function buildRegistryContactFields() {
  return [
    {
      name: MAP_GTM_CONTACT.verificationTier,
      type: "singleSelect",
      options: choices(VAL_GTM_CONTACT_VERIFICATION_TIER),
      description: "V1=CoStar contact; V1R=named person email on corp domain + proof; V2=LinkedIn + named exec; V3=entity/switchboard/role mailbox (info@, ir@) only.",
    },
    {
      name: MAP_GTM_CONTACT.verificationSource,
      type: "singleSelect",
      options: choices(VAL_GTM_CONTACT_VERIFICATION_SOURCE),
      description: "Provenance of verified contact data (internal GTM only).",
    },
    {
      name: MAP_GTM_CONTACT.registrySystem,
      type: "singleLineText",
      description: "Registry system id (e.g. MX_SIGER, CO_RUES, DO_REGISTRO_MERCANTIL).",
    },
    {
      name: MAP_GTM_CONTACT.registryCountry,
      type: "singleLineText",
      description: "Canonical CALA country for registry lookup.",
    },
    {
      name: MAP_GTM_CONTACT.registryEntityName,
      type: "singleLineText",
      description: "Razón social / legal entity name from public registry.",
    },
    {
      name: MAP_GTM_CONTACT.registryEntityId,
      type: "singleLineText",
      description: "RFC, NIT, RNC, CNPJ, or other public entity identifier.",
    },
    {
      name: MAP_GTM_CONTACT.legalRepresentativeName,
      type: "singleLineText",
      description: "Legal representative from commercial or tourism registry.",
    },
    {
      name: MAP_GTM_CONTACT.verificationUrl,
      type: "url",
      description: "Public proof URL (registry search result or cert reference). Required for V1R/V2.",
    },
    {
      name: MAP_GTM_CONTACT.verifiedAt,
      type: "date",
      options: { dateFormat: { name: "iso" } },
      description: "Date contact was verified against public source.",
    },
    {
      name: MAP_GTM_CONTACT.businessPhone,
      type: "phoneNumber",
      description: "Verified direct business line (VP1) or entity HQ switchboard (VP3).",
    },
    {
      name: MAP_GTM_CONTACT.mobilePhone,
      type: "phoneNumber",
      description: "Verified person mobile/cell (VP2) when published on proof URL.",
    },
    {
      name: MAP_GTM_CONTACT.phoneVerificationTier,
      type: "singleSelect",
      options: choices(VAL_GTM_PHONE_VERIFICATION_TIER),
      description: "VP2=verified mobile; VP1=verified direct office line; VP3=entity switchboard/toll-free only.",
    },
  ];
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

async function main() {
  const { apiKey, baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  const { res: listRes, json: listJson } = await metaFetch(baseId, apiKey, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed: ${JSON.stringify(listJson)}`);

  const table = (listJson.tables || []).find((t) => t.name === GTM_CONTACT_TABLE);
  if (!table) {
    throw new Error(`Table "${GTM_CONTACT_TABLE}" not found. Run ensure-gtm-costar-contacts-table.mjs first.`);
  }

  const existing = new Set((table.fields || []).map((f) => f.name));
  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    tableName: table.name,
    tableId: table.id,
    fieldsWouldCreate: [],
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
  };

  for (const fieldSpec of buildRegistryContactFields()) {
    if (existing.has(fieldSpec.name)) {
      report.fieldsSkipped.push(fieldSpec.name);
      console.log("SKIP", fieldSpec.name);
      continue;
    }
    if (!APPLY) {
      report.fieldsWouldCreate.push(fieldSpec.name);
      console.log("WOULD CREATE", fieldSpec.name);
      continue;
    }
    const { res, json } = await metaFetch(baseId, apiKey, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(fieldSpec),
    });
    if (!res.ok) {
      report.fieldsFailed.push({ name: fieldSpec.name, error: json });
      console.error("FIELD FAILED", fieldSpec.name, JSON.stringify(json));
    } else {
      report.fieldsCreated.push(fieldSpec.name);
      console.log("CREATED", fieldSpec.name);
    }
  }

  const outPath = path.join(ROOT, "reports", "ensure-gtm-registry-contact-fields.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("Wrote", outPath);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
