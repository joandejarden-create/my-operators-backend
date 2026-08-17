/**
 * Import registry enrichment JSON files into GTM Contacts + link Owner Targets.
 *
 * Input: data/internal/gtm-registry-enrichments/*.json
 *        fixtures/gtm-registry-enrichment-example.json (reference shape)
 *
 * Usage:
 *   node scripts/import-gtm-registry-contact-enrichments.mjs --dry-run
 *   node scripts/import-gtm-registry-contact-enrichments.mjs --apply
 *   node scripts/import-gtm-registry-contact-enrichments.mjs --apply --file=path/to/one.json
 *
 * Report: reports/gtm-registry-contact-import.json
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, readdirSync, existsSync } from "fs";
import { join, dirname, resolve } from "path";
import { fileURLToPath } from "url";
import {
  GTM_CONTACT_TABLE,
  MAP_GTM_CONTACT,
} from "../lib/gtm-owner-target/contact-field-map.js";
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_OWNER_TARGET,
} from "../lib/gtm-owner-target/field-map.js";
import {
  getGtmAirtableBase,
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import {
  buildOwnerTargetIndexes,
  resolveLabelToOwnerTargets,
} from "../lib/gtm-owner-target/owner-contact-sync.js";
import {
  buildContactCalaMatchContext,
} from "../lib/gtm-owner-target/contact-cala-match.js";
import {
  fetchAllGtmProperties,
  groupAirtablePropertiesByOwner,
} from "../lib/gtm-owner-target/properties-read.js";
import { COMPANY_PROFILE_ENRICHMENTS } from "../lib/gtm-owner-target/company-profile-enrichments.js";
import { GTM_COMPANY_TABLE, MAP_GTM_COMPANY } from "../lib/gtm-owner-target/company-field-map.js";
import {
  validateRegistryEnrichmentRecord,
  buildContactFieldsFromRegistryEnrichment,
  registryContactDedupeKey,
  isRegistryVerifiedOwnerContact,
} from "../lib/gtm-owner-target/registry-contact-verification.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const DEFAULT_DIR = join(ROOT, "data", "internal", "gtm-registry-enrichments");
const REPORT_JSON = join(ROOT, "reports", "gtm-registry-contact-import.json");
const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;

function parseArgs() {
  const fileArg = process.argv.find((a) => a.startsWith("--file="));
  if (fileArg) {
    return { files: [resolve(fileArg.slice("--file=".length).replace(/^"|"$/g, ""))] };
  }
  if (!existsSync(DEFAULT_DIR)) {
    return { files: [] };
  }
  const files = readdirSync(DEFAULT_DIR)
    .filter((f) => f.endsWith(".json") && !f.startsWith("."))
    .map((f) => join(DEFAULT_DIR, f));
  return { files };
}

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

async function loadExistingContacts(base) {
  const records = await base(GTM_CONTACT_TABLE)
    .select({ fields: [MAP_GTM_CONTACT.contactDedupeKey, MAP_GTM_CONTACT.email, MAP_GTM_CONTACT.name] })
    .all();
  const byKey = new Map();
  for (const rec of records) {
    const key = rec.fields[MAP_GTM_CONTACT.contactDedupeKey];
    if (key) byKey.set(String(key), rec);
  }
  return { records, byKey };
}

async function main() {
  const { files } = parseArgs();
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  /** @type {object[]} */
  const enrichments = [];
  const fileErrors = [];

  for (const file of files) {
    try {
      const raw = JSON.parse(readFileSync(file, "utf8"));
      const rows = Array.isArray(raw) ? raw : [raw];
      for (const row of rows) {
        enrichments.push({ ...row, _sourceFile: file });
      }
    } catch (err) {
      fileErrors.push({ file, error: err.message || String(err) });
    }
  }

  let base = null;
  let existing = { byKey: new Map() };
  let ownerIndexes = null;
  let matchContext = null;

  if (APPLY || enrichments.length) {
    try {
      base = getGtmAirtableBase();
      existing = await loadExistingContacts(base);

      const [{ records: propertyRecords }, companyRecords, ownerRecords] = await Promise.all([
        fetchAllGtmProperties(),
        base(GTM_COMPANY_TABLE).select({ fields: [MAP_GTM_COMPANY.company] }).all(),
        base(GTM_OWNER_TARGET_TABLES.ownerTargets)
          .select({ fields: [MAP_GTM_OWNER_TARGET.ownerName] })
          .all(),
      ]);

      const ownerGroups = groupAirtablePropertiesByOwner(propertyRecords);
      matchContext = buildContactCalaMatchContext({
        ownerGroups,
        companyNames: companyRecords.map((r) => String(r.fields[MAP_GTM_COMPANY.company] || "")),
        profileEnrichments: COMPANY_PROFILE_ENRICHMENTS,
      });
      ownerIndexes = buildOwnerTargetIndexes(
        ownerRecords.map((r) => ({
          id: r.id,
          ownerName: String(r.fields[MAP_GTM_OWNER_TARGET.ownerName] || ""),
        }))
      );
    } catch (err) {
      if (APPLY) throw err;
      console.warn("Airtable preload skipped (dry-run):", err.message || err);
    }
  }

  /** @type {object[]} */
  const plan = [];
  const toCreate = [];
  const toUpdate = [];

  for (const enrichment of enrichments) {
    const validation = validateRegistryEnrichmentRecord(enrichment);
    if (!validation.ok) {
      plan.push({
        action: "reject",
        sourceFile: enrichment._sourceFile,
        ownerName: enrichment.ownerName,
        failures: validation.failures,
      });
      continue;
    }

    const fields = buildContactFieldsFromRegistryEnrichment(enrichment);
    const dedupeKey = registryContactDedupeKey(fields);
    if (!dedupeKey) {
      plan.push({
        action: "reject",
        sourceFile: enrichment._sourceFile,
        ownerName: enrichment.ownerName,
        failures: ["could not compute contact dedupe key"],
      });
      continue;
    }
    fields[MAP_GTM_CONTACT.contactDedupeKey] = dedupeKey;

    let ownerTargetIds = [];
    if (enrichment.ownerTargetId) {
      ownerTargetIds = [enrichment.ownerTargetId];
    } else if (ownerIndexes && matchContext) {
      const hits = resolveLabelToOwnerTargets(
        enrichment.ownerName || enrichment.registry?.entityName,
        matchContext.nameAliasIndex,
        ownerIndexes
      );
      ownerTargetIds = hits.map((h) => h.id);
    }
    if (ownerTargetIds.length) {
      fields[MAP_GTM_CONTACT.ownerTargets] = ownerTargetIds;
    }

    const contactProbe = {
      email: fields[MAP_GTM_CONTACT.email],
      contactRelevance: fields[MAP_GTM_CONTACT.contactRelevance],
      calaHotelContact: fields[MAP_GTM_CONTACT.calaHotelContact],
      verificationTier: fields[MAP_GTM_CONTACT.verificationTier],
      verificationSource: fields[MAP_GTM_CONTACT.verificationSource],
      verificationUrl: fields[MAP_GTM_CONTACT.verificationUrl],
      legalRepresentativeName: fields[MAP_GTM_CONTACT.legalRepresentativeName],
      registryEntityName: fields[MAP_GTM_CONTACT.registryEntityName],
      website: fields[MAP_GTM_CONTACT.website],
      company: fields[MAP_GTM_CONTACT.company],
    };
    const registryVerified = isRegistryVerifiedOwnerContact(contactProbe, { calaHotelContact: "yes" });

    const existingRec = existing.byKey.get(dedupeKey);
    const entry = {
      action: existingRec ? "update" : "create",
      sourceFile: enrichment._sourceFile,
      ownerName: enrichment.ownerName,
      dedupeKey,
      verificationTier: fields[MAP_GTM_CONTACT.verificationTier],
      registryVerified,
      ownerTargetIds,
      fieldPreview: {
        name: fields[MAP_GTM_CONTACT.name],
        email: fields[MAP_GTM_CONTACT.email],
        registryEntityName: fields[MAP_GTM_CONTACT.registryEntityName],
        legalRepresentativeName: fields[MAP_GTM_CONTACT.legalRepresentativeName],
        verificationUrl: fields[MAP_GTM_CONTACT.verificationUrl],
      },
    };
    plan.push(entry);

    if (existingRec) {
      toUpdate.push({ id: existingRec.id, fields });
    } else {
      toCreate.push({ fields });
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "apply",
    filesRead: files.length,
    fileErrors,
    enrichmentCount: enrichments.length,
    planSummary: {
      create: plan.filter((p) => p.action === "create").length,
      update: plan.filter((p) => p.action === "update").length,
      reject: plan.filter((p) => p.action === "reject").length,
      registryVerified: plan.filter((p) => p.registryVerified).length,
    },
    plan,
  };

  if (APPLY && base) {
    for (const batch of chunk(toCreate, 10)) {
      await base(GTM_CONTACT_TABLE).create(batch);
    }
    for (const batch of chunk(toUpdate, 10)) {
      await base(GTM_CONTACT_TABLE).update(batch);
    }
    report.applied = { created: toCreate.length, updated: toUpdate.length };
  }

  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));

  console.log(`Mode: ${DRY_RUN ? "DRY-RUN" : "APPLY"}`);
  console.log(`Files: ${files.length} | Enrichments: ${enrichments.length}`);
  console.log(
    `Plan: +${report.planSummary.create} ~${report.planSummary.update} reject=${report.planSummary.reject} verified=${report.planSummary.registryVerified}`
  );
  console.log("Wrote", REPORT_JSON);

  if (!files.length) {
    console.warn(`No JSON files in ${DEFAULT_DIR}. See fixtures/gtm-registry-enrichment-example.json`);
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
