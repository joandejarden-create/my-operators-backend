#!/usr/bin/env node
/**
 * Seed Capital Provider Explorer tables from curated public-source data.
 *
 *   node scripts/seed-capital-providers-public-data.mjs --dry-run
 *   node scripts/seed-capital-providers-public-data.mjs --apply
 *
 * Uses AIRTABLE_BASE_ID (Deal Capture MVP base). Idempotent upsert by Capital Provider Name.
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  CAPITAL_PROVIDER_FIELDS as F,
  TABLE_CAPITAL_PROVIDERS,
  TABLE_CRITERIA,
  TABLE_REQUIRED_DOCUMENTS,
  TABLE_CONTACTS,
  TABLE_REPRESENTATIVE_FINANCINGS,
  TABLE_SOURCE_REFERENCES,
} from "../lib/capital-setup/airtable-capital-setup-fields.js";
import {
  PROVIDER_FIELD,
  SOURCE_FIELD,
  CRITERIA_FIELD,
  DOCUMENT_FIELD,
  CONTACT_FIELD,
  FINANCING_FIELD,
  LAST_VERIFIED,
  PUBLIC_SEED_EXPLORER_HERO_VERIFICATION,
  PUBLIC_SEED_EXPLORER_HERO_DATA_SOURCE,
} from "../lib/capital-setup/capital-provider-public-seed-constants.js";
import { PUBLIC_SEED_PROVIDERS } from "../lib/capital-setup/capital-provider-public-seed-data.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const AUDIT_PATH = path.join(ROOT, "data", "capital-provider-public-seed-audit.json");

const APPLY = process.argv.includes("--apply");
const DRY = process.argv.includes("--dry-run") || !APPLY;

const SLEEP_MS = 220;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function compact(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v === undefined || v === null) continue;
    if (typeof v === "string" && !v.trim()) continue;
    if (Array.isArray(v) && v.length === 0) continue;
    out[k] = v;
  }
  return out;
}

function mapContactPathway(text) {
  if (!text) return undefined;
  const t = String(text).toLowerCase();
  if (t.includes("@") || t.includes("email")) return "Direct Contact Available";
  if (t.includes("official") || t.includes("website")) return "Direct Contact Available";
  return "Unknown";
}

function buildProviderAirtableFields(pkg) {
  const p = pkg.provider;
  return compact({
    [F.name]: pkg.name,
    [F.institutionType]: p.institutionType,
    [F.profileStatus]: p.profileStatus,
    [F.visibilityLevel]: p.visibilityLevel,
    [F.shortDescription]: p.shortDescription,
    [F.institutionOverview]: p.institutionOverview,
    [F.hotelLendingFocus]: p.hotelLendingFocus,
    [F.headquarters]: p.headquarters,
    [F.website]: p.website,
    [F.logoUrl]: p.logoUrl,
    [F.primaryRegion]: p.primaryRegion,
    [F.geographicCoverage]: p.geographicCoverage,
    [F.preferredMarkets]: p.preferredMarkets,
    [F.marketsExcludedPaused]: p.marketsExcludedPaused,
    [F.typicalDealTypes]: p.typicalDealTypes,
    [F.loanProductsOffered]: p.loanProductsOffered,
    [F.preferredAssetTypes]: p.preferredAssetTypes,
    [F.projectStageAppetite]: p.projectStageAppetite,
    [F.minimumLoanSize]: p.minimumLoanSize,
    [F.maximumLoanSize]: p.maximumLoanSize,
    [F.typicalLoanSizeSummary]: p.typicalLoanSizeSummary,
    [F.brandPreference]: p.brandPreference,
    [F.operatorPreference]: p.operatorPreference,
    [F.sponsorPreference]: p.sponsorPreference,
    [F.currentLendingAppetite]: p.currentLendingAppetite,
    [F.contactPathway]: p.contactPathwaySelect || mapContactPathway(p.contactPathway),
    [F.requiredInformationSummary]: p.requiredInformationSummary,
    [F.processOverview]: p.processOverview || p.contactPathway,
    [F.ownerFacingNotes]: p.ownerFacingNotes,
    [F.sourceType]: p.sourceType,
    [F.sourceConfidence]: p.sourceConfidence,
    [F.lastVerifiedDate]: p.lastVerifiedDate || LAST_VERIFIED,
    [F.createdBySeedSource]: p.createdBySeedSource,
    [F.explorerHeroVerification]:
      p.explorerHeroVerification || PUBLIC_SEED_EXPLORER_HERO_VERIFICATION,
    [F.explorerHeroDataSource]:
      p.explorerHeroDataSource || PUBLIC_SEED_EXPLORER_HERO_DATA_SOURCE,
  });
}

function buildSourceFields(src, providerId) {
  return compact({
    [SOURCE_FIELD.name]: src.sourceName,
    [SOURCE_FIELD.provider]: [providerId],
    [SOURCE_FIELD.sourceType]: src.sourceType,
    [SOURCE_FIELD.sourceUrl]: src.sourceUrl,
    [SOURCE_FIELD.sourceDate]: src.sourceDate,
    [SOURCE_FIELD.retrievedDate]: LAST_VERIFIED,
    [SOURCE_FIELD.summary]: src.sourceSummary,
    [SOURCE_FIELD.relevantFields]: src.relevantFields,
    [SOURCE_FIELD.confidence]: src.confidence,
    [SOURCE_FIELD.internalNotes]: src.internalNotes,
  });
}

function buildCriteriaFields(row, providerId) {
  return compact({
    [CRITERIA_FIELD.name]: row.criteriaName,
    [CRITERIA_FIELD.provider]: [providerId],
    [CRITERIA_FIELD.loanProduct]: row.loanProduct,
    [CRITERIA_FIELD.dealTypes]: row.dealTypes,
    [CRITERIA_FIELD.minLoan]: row.minLoan,
    [CRITERIA_FIELD.maxLoan]: row.maxLoan,
    [CRITERIA_FIELD.termRange]: row.termRange,
    [CRITERIA_FIELD.recourse]: row.recourse,
    [CRITERIA_FIELD.rateType]: row.rateType,
    [CRITERIA_FIELD.currency]: row.currency ? [row.currency] : undefined,
    [CRITERIA_FIELD.sponsorReq]: row.sponsorReq,
    [CRITERIA_FIELD.marketReq]: row.marketReq,
    [CRITERIA_FIELD.appetite]: row.appetite,
    [CRITERIA_FIELD.ownerSummary]: row.ownerSummary,
    [CRITERIA_FIELD.sourceConfidence]: row.sourceConfidence,
    [CRITERIA_FIELD.lastVerified]: row.lastVerified || LAST_VERIFIED,
  });
}

function buildDocumentFields(row, providerId) {
  return compact({
    [DOCUMENT_FIELD.reqName]: row.documentRequirementName,
    [DOCUMENT_FIELD.provider]: [providerId],
    [DOCUMENT_FIELD.docName]: row.documentName,
    [DOCUMENT_FIELD.category]: row.category,
    [DOCUMENT_FIELD.requiredLevel]: row.requiredLevel,
    [DOCUMENT_FIELD.dealTypes]: row.dealTypes,
    [DOCUMENT_FIELD.description]: row.description,
    [DOCUMENT_FIELD.ownerInstructions]: row.ownerInstructions,
    [DOCUMENT_FIELD.visibility]: row.visibility,
    [DOCUMENT_FIELD.sortOrder]: row.sortOrder,
    [DOCUMENT_FIELD.internalNotes]: row.internalNotes,
  });
}

function buildContactFields(row, providerId) {
  return compact({
    [CONTACT_FIELD.name]: row.contactName,
    [CONTACT_FIELD.provider]: [providerId],
    [CONTACT_FIELD.title]: row.title,
    [CONTACT_FIELD.department]: row.department,
    [CONTACT_FIELD.email]: row.email,
    [CONTACT_FIELD.phone]: row.phone,
    [CONTACT_FIELD.regionCoverage]: row.regionCoverage ? [].concat(row.regionCoverage) : undefined,
    [CONTACT_FIELD.contactRole]: row.contactRole,
    [CONTACT_FIELD.preferredMethod]: row.preferredMethod,
    [CONTACT_FIELD.contactStatus]: row.contactStatus,
    [CONTACT_FIELD.contactNotes]: row.contactNotes,
    [CONTACT_FIELD.internalOnly]: row.internalOnly === true,
  });
}

function buildFinancingFields(row, providerId) {
  return compact({
    [FINANCING_FIELD.name]: row.financingName,
    [FINANCING_FIELD.provider]: [providerId],
    [FINANCING_FIELD.projectName]: row.projectName,
    [FINANCING_FIELD.location]: row.location,
    [FINANCING_FIELD.dealType]: row.dealType,
    [FINANCING_FIELD.loanAmountLabel]: row.loanAmountLabel,
    [FINANCING_FIELD.loanAmountUsd]: row.loanAmountUsd,
    [FINANCING_FIELD.transactionYear]: row.transactionYear,
    [FINANCING_FIELD.ownerSummary]: row.ownerSummary,
    [FINANCING_FIELD.sourceName]: row.sourceName,
    [FINANCING_FIELD.sourceUrl]: row.sourceUrl,
    [FINANCING_FIELD.imageUrl]: row.imageUrl,
    [FINANCING_FIELD.visibility]: row.visibility || "Owner Visible",
    [FINANCING_FIELD.sortOrder]: row.sortOrder,
  });
}

async function loadTableIndex(base, tableName, keyFn) {
  const records = await base(tableName).select().all();
  const map = new Map();
  for (const rec of records) {
    const key = keyFn(rec);
    if (key) map.set(key, rec);
  }
  return map;
}

async function upsertRecord(base, tableName, key, index, fields, stats, label) {
  const hit = index.get(key);
  if (hit) {
    if (DRY) {
      stats.updated += 1;
      return { id: hit.id, action: "update" };
    }
    await base(tableName).update(hit.id, fields, { typecast: true });
    await sleep(SLEEP_MS);
    stats.updated += 1;
    return { id: hit.id, action: "update" };
  }
  if (DRY) {
    stats.created += 1;
    return { id: `dry-run-${label}`, action: "create" };
  }
  const rec = await base(tableName).create(fields, { typecast: true });
  await sleep(SLEEP_MS);
  index.set(key, rec);
  stats.created += 1;
  return { id: rec.id, action: "create" };
}

function fieldsPopulatedFromPackage(pkg) {
  const labelMap = { contactPathwaySelect: F.contactPathway };
  const keys = Object.keys(pkg.provider).filter((k) => {
    const v = pkg.provider[k];
    if (v === undefined || v === null) return false;
    if (typeof v === "string" && !v.trim()) return false;
    if (Array.isArray(v) && !v.length) return false;
    return true;
  });
  return [
    "Capital Provider Name",
    ...keys.map((k) => labelMap[k] || PROVIDER_FIELD[k] || k),
  ];
}

async function main() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env");
  }

  const base = new Airtable({ apiKey }).base(baseId);

  const providerIndex = await loadTableIndex(
    base,
    TABLE_CAPITAL_PROVIDERS,
    (r) => String(r.fields[F.name] || "").trim()
  );

  const sourceIndex = await loadTableIndex(
    base,
    TABLE_SOURCE_REFERENCES,
    (r) => {
      const url = String(r.fields[SOURCE_FIELD.sourceUrl] || "").trim();
      const providers = r.fields[SOURCE_FIELD.provider] || [];
      return `${providers[0] || ""}::${url}`;
    }
  );

  const criteriaIndex = await loadTableIndex(base, TABLE_CRITERIA, (r) => {
    const providers = r.fields[CRITERIA_FIELD.provider] || [];
    const name = String(r.fields[CRITERIA_FIELD.name] || "").trim();
    return `${providers[0] || ""}::${name}`;
  });

  const docIndex = await loadTableIndex(base, TABLE_REQUIRED_DOCUMENTS, (r) => {
    const providers = r.fields[DOCUMENT_FIELD.provider] || [];
    const name = String(r.fields[DOCUMENT_FIELD.reqName] || "").trim();
    return `${providers[0] || ""}::${name}`;
  });

  const contactIndex = await loadTableIndex(base, TABLE_CONTACTS, (r) => {
    const providers = r.fields[CONTACT_FIELD.provider] || [];
    const name = String(r.fields[CONTACT_FIELD.name] || "").trim();
    return `${providers[0] || ""}::${name}`;
  });

  const financingIndex = await loadTableIndex(base, TABLE_REPRESENTATIVE_FINANCINGS, (r) => {
    const providers = r.fields[FINANCING_FIELD.provider] || [];
    const name = String(r.fields[FINANCING_FIELD.name] || "").trim();
    return `${providers[0] || ""}::${name}`;
  });

  const auditRows = [];
  const reportLines = [];

  console.log(DRY ? "DRY RUN — no Airtable writes\n" : "APPLY — writing to Airtable\n");

  for (const pkg of PUBLIC_SEED_PROVIDERS) {
    const providerFields = buildProviderAirtableFields(pkg);
    const providerStats = { created: 0, updated: 0 };
    const existing = providerIndex.get(pkg.name);
    const providerResult = await upsertRecord(
      base,
      TABLE_CAPITAL_PROVIDERS,
      pkg.name,
      providerIndex,
      providerFields,
      providerStats,
      pkg.name
    );
    const providerId = providerResult.id;
    const providerAction =
      providerStats.created > 0 ? "created" : providerStats.updated > 0 ? "updated" : existing ? "unchanged" : "created";

    const childStats = {
      sources: { created: 0, updated: 0 },
      criteria: { created: 0, updated: 0 },
      documents: { created: 0, updated: 0 },
      contacts: { created: 0, updated: 0 },
      financings: { created: 0, updated: 0 },
    };

    for (const src of pkg.sources || []) {
      const key = `${providerId}::${src.sourceUrl}`;
      await upsertRecord(
        base,
        TABLE_SOURCE_REFERENCES,
        key,
        sourceIndex,
        buildSourceFields(src, providerId),
        childStats.sources,
        src.sourceName
      );
    }

    for (const row of pkg.criteria || []) {
      const key = `${providerId}::${row.criteriaName}`;
      await upsertRecord(
        base,
        TABLE_CRITERIA,
        key,
        criteriaIndex,
        buildCriteriaFields(row, providerId),
        childStats.criteria,
        row.criteriaName
      );
    }

    for (const row of pkg.documents || []) {
      const key = `${providerId}::${row.documentRequirementName}`;
      await upsertRecord(
        base,
        TABLE_REQUIRED_DOCUMENTS,
        key,
        docIndex,
        buildDocumentFields(row, providerId),
        childStats.documents,
        row.documentRequirementName
      );
    }

    for (const row of pkg.contacts || []) {
      const key = `${providerId}::${row.contactName}`;
      await upsertRecord(
        base,
        TABLE_CONTACTS,
        key,
        contactIndex,
        buildContactFields(row, providerId),
        childStats.contacts,
        row.contactName
      );
    }

    for (const row of pkg.financings || []) {
      const key = `${providerId}::${row.financingName}`;
      await upsertRecord(
        base,
        TABLE_REPRESENTATIVE_FINANCINGS,
        key,
        financingIndex,
        buildFinancingFields(row, providerId),
        childStats.financings,
        row.financingName
      );
    }

    const sourcesCount =
      (childStats.sources.created || 0) +
      (childStats.sources.updated || 0) ||
      (pkg.sources || []).length;
    const criteriaCount =
      (childStats.criteria.created || 0) +
      (childStats.criteria.updated || 0) ||
      (pkg.criteria || []).length;
    const documentsCount =
      (childStats.documents.created || 0) +
      (childStats.documents.updated || 0) ||
      (pkg.documents || []).length;
    const contactsCount =
      (childStats.contacts.created || 0) +
      (childStats.contacts.updated || 0) ||
      (pkg.contacts || []).length;
    const financingsCount =
      (childStats.financings.created || 0) +
      (childStats.financings.updated || 0) ||
      (pkg.financings || []).length;

    const audit = {
      providerName: pkg.name,
      airtableRecordId: providerId,
      providerAction,
      fieldsPopulated: fieldsPopulatedFromPackage(pkg),
      fieldsSkipped: pkg.fieldsSkipped || [],
      sourcesUsed: (pkg.sources || []).map((s) => ({
        name: s.sourceName,
        url: s.sourceUrl,
        confidence: s.confidence,
      })),
      confidenceLevel: pkg.provider.sourceConfidence,
      warnings: pkg.warnings || [],
      lastVerifiedDate: pkg.provider.lastVerifiedDate || LAST_VERIFIED,
      counts: {
        sourceReferences: sourcesCount,
        criteria: criteriaCount,
        requiredDocuments: documentsCount,
        contacts: contactsCount,
        representativeFinancings: financingsCount,
      },
    };
    auditRows.push(audit);

    reportLines.push({
      name: pkg.name,
      provider: providerAction,
      sources: sourcesCount,
      criteria: criteriaCount,
      documents: documentsCount,
      contacts: contactsCount,
      financings: financingsCount,
      confidence: pkg.provider.sourceConfidence,
      skipped: pkg.fieldsSkipped || [],
      warnings: pkg.warnings || [],
    });
  }

  fs.mkdirSync(path.dirname(AUDIT_PATH), { recursive: true });
  fs.writeFileSync(
    AUDIT_PATH,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: DRY ? "dry-run" : "apply",
        providers: auditRows,
      },
      null,
      2
    )
  );

  console.log("=== Validation Report ===\n");
  for (const row of reportLines) {
    console.log(`Provider: ${row.name}`);
    console.log(`  Provider record: ${row.provider}`);
    console.log(`  Source references: ${row.sources}`);
    console.log(`  Criteria records: ${row.criteria}`);
    console.log(`  Required document records: ${row.documents}`);
    console.log(`  Contact records: ${row.contacts}`);
    console.log(`  Source confidence: ${row.confidence}`);
    if (row.skipped.length) console.log(`  Missing / unknown fields: ${row.skipped.join("; ")}`);
    if (row.warnings.length) console.log(`  Warnings: ${row.warnings.join(" | ")}`);
    console.log("");
  }

  console.log(`Audit written: ${AUDIT_PATH}`);
  if (DRY) console.log("\nDry run only — re-run with --apply to write to Airtable.");
}

main().catch((err) => {
  console.error("[seed-capital-providers-public-data]", err);
  process.exit(1);
});
