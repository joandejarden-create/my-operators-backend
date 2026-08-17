#!/usr/bin/env node
/**
 * Audit and backfill Capital Provider Explorer Airtable readiness (public-source only).
 *
 *   node scripts/audit-capital-provider-explorer-readiness.mjs --dry-run
 *   node scripts/audit-capital-provider-explorer-readiness.mjs --apply
 *
 * Uses curated data in lib/capital-setup/capital-provider-public-seed-data.js — does not invent claims.
 * Idempotent: fills empty provider fields, creates missing child rows, adds general readiness docs when needed.
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
  TABLE_SOURCE_REFERENCES,
} from "../lib/capital-setup/airtable-capital-setup-fields.js";
import {
  PROVIDER_FIELD,
  SOURCE_FIELD,
  CRITERIA_FIELD,
  DOCUMENT_FIELD,
  CONTACT_FIELD,
  LAST_VERIFIED,
  PUBLIC_SEED_EXPLORER_HERO_VERIFICATION,
  PUBLIC_SEED_EXPLORER_HERO_DATA_SOURCE,
} from "../lib/capital-setup/capital-provider-public-seed-constants.js";
import { PUBLIC_SEED_PROVIDERS } from "../lib/capital-setup/capital-provider-public-seed-data.js";
import {
  buildGeneralReadinessDocumentRow,
  buildGlobalTabRecommendation,
  GENERAL_FINANCING_READINESS_DOCS,
  listMissingBackfillFields,
  listPopulatedProviderFields,
  overallReadinessFromTabs,
  recommendedUiTreatment,
  scoreProviderTabs,
  isGeneralReadinessDocument,
  fieldPopulated,
} from "../lib/capital-setup/capital-provider-explorer-readiness.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT_PATH = path.join(ROOT, "data", "capital-provider-explorer-readiness-audit.json");

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

function mergeProviderPatch(existingFields, patchFields) {
  const out = {};
  for (const [key, value] of Object.entries(patchFields)) {
    if (!fieldPopulated(existingFields, key)) {
      out[key] = value;
    }
  }
  return out;
}

function buildInternalNotesPatch(existingFields, pkg) {
  const warnings = (pkg.warnings || []).filter(Boolean);
  if (!warnings.length) return {};
  const existing = String(existingFields[F.internalNotes] || "").trim();
  const block = warnings.map((w) => `Warning: ${w}`).join("\n");
  if (!existing) return { [F.internalNotes]: block };
  const missing = warnings.filter((w) => !existing.includes(w));
  if (!missing.length) return {};
  return { [F.internalNotes]: `${existing}\n${missing.map((w) => `Warning: ${w}`).join("\n")}` };
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

function recordsForProvider(allRecords, providerFieldName, providerId) {
  return allRecords.filter((rec) => {
    const links = rec.fields[providerFieldName];
    if (Array.isArray(links)) return links.includes(providerId);
    return links === providerId;
  });
}

async function loadAll(base, tableName) {
  return base(tableName).select().all();
}

async function createIfMissing(base, tableName, key, index, fields, stats) {
  if (index.has(key)) {
    stats.skipped += 1;
    return { action: "exists", id: index.get(key).id };
  }
  if (DRY) {
    stats.created += 1;
    return { action: "create", id: `dry-run-${key}` };
  }
  const rec = await base(tableName).create(fields, { typecast: true });
  await sleep(SLEEP_MS);
  index.set(key, rec);
  stats.created += 1;
  return { action: "create", id: rec.id };
}

async function updateProviderIfNeeded(base, recordId, patch, stats) {
  if (!Object.keys(patch).length) return { action: "unchanged" };
  if (DRY) {
    stats.updated += 1;
    return { action: "update" };
  }
  await base(TABLE_CAPITAL_PROVIDERS).update(recordId, patch, { typecast: true });
  await sleep(SLEEP_MS);
  stats.updated += 1;
  return { action: "update" };
}

function pad(str, len) {
  const s = String(str ?? "");
  return s.length >= len ? s.slice(0, len - 1) + "…" : s.padEnd(len);
}

function printSummaryTable(providerReports) {
  const header =
    pad("Provider", 28) +
    " | " +
    pad("Overall", 10) +
    " | " +
    pad("Overview", 10) +
    " | " +
    pad("Lending", 10) +
    " | " +
    pad("Criteria", 10) +
    " | " +
    pad("Req Info", 10) +
    " | " +
    pad("Process", 10) +
    " | " +
    pad("Contact", 10) +
    " | " +
    pad("Sources", 10) +
    " | " +
    pad("UI", 18);
  console.log("\n" + header);
  console.log("-".repeat(header.length));
  for (const p of providerReports) {
    const t = p.tabReadiness;
    console.log(
      pad(p.providerName, 28) +
        " | " +
        pad(p.overallReadiness, 10) +
        " | " +
        pad(t.overview, 10) +
        " | " +
        pad(t.lendingFocus, 10) +
        " | " +
        pad(t.dealCriteria, 10) +
        " | " +
        pad(t.requiredInfo, 10) +
        " | " +
        pad(t.process, 10) +
        " | " +
        pad(t.contactPathway, 10) +
        " | " +
        pad(t.sources, 10) +
        " | " +
        pad(p.recommendedUiTreatment, 18)
    );
  }
}

async function main() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env");
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const runDate = new Date().toISOString();

  console.log(DRY ? "DRY RUN — no Airtable writes\n" : "APPLY — writing to Airtable\n");

  const providerRecords = await loadAll(base, TABLE_CAPITAL_PROVIDERS);
  const providerByName = new Map();
  for (const rec of providerRecords) {
    const name = String(rec.fields[F.name] || "").trim();
    if (name) providerByName.set(name, rec);
  }

  const allSources = await loadAll(base, TABLE_SOURCE_REFERENCES);
  const allCriteria = await loadAll(base, TABLE_CRITERIA);
  const allDocuments = await loadAll(base, TABLE_REQUIRED_DOCUMENTS);
  const allContacts = await loadAll(base, TABLE_CONTACTS);

  const sourceIndex = new Map();
  for (const rec of allSources) {
    const url = String(rec.fields[SOURCE_FIELD.sourceUrl] || "").trim();
    const providers = rec.fields[SOURCE_FIELD.provider] || [];
    sourceIndex.set(`${providers[0] || ""}::${url}`, rec);
  }

  const criteriaIndex = new Map();
  for (const rec of allCriteria) {
    const providers = rec.fields[CRITERIA_FIELD.provider] || [];
    const name = String(rec.fields[CRITERIA_FIELD.name] || "").trim();
    criteriaIndex.set(`${providers[0] || ""}::${name}`, rec);
  }

  const docIndex = new Map();
  for (const rec of allDocuments) {
    const providers = rec.fields[DOCUMENT_FIELD.provider] || [];
    const name = String(rec.fields[DOCUMENT_FIELD.reqName] || "").trim();
    docIndex.set(`${providers[0] || ""}::${name}`, rec);
  }

  const contactIndex = new Map();
  for (const rec of allContacts) {
    const providers = rec.fields[CONTACT_FIELD.provider] || [];
    const name = String(rec.fields[CONTACT_FIELD.name] || "").trim();
    contactIndex.set(`${providers[0] || ""}::${name}`, rec);
  }

  const providerReports = [];

  for (const pkg of PUBLIC_SEED_PROVIDERS) {
    const existingRec = providerByName.get(pkg.name);
    if (!existingRec) {
      console.warn(`[skip] Provider not found in Airtable: ${pkg.name}`);
      providerReports.push({
        providerName: pkg.name,
        recordId: null,
        overallReadiness: "Not Enough Data",
        recommendedProfileStatus: pkg.provider.profileStatus || "Needs Review",
        sourceConfidence: pkg.provider.sourceConfidence || "Needs Verification",
        tabReadiness: {
          overview: "Not Enough Data",
          lendingFocus: "Not Enough Data",
          dealCriteria: "Not Enough Data",
          requiredInfo: "Not Enough Data",
          process: "Not Enough Data",
          contactPathway: "Not Enough Data",
          sources: "Not Enough Data",
          internalNotes: "Not Enough Data",
        },
        fieldsPopulatedBefore: [],
        fieldsPopulatedAfter: [],
        fieldsStillMissing: [],
        recordsCreated: { criteria: 0, requiredDocuments: 0, sourceReferences: 0, contacts: 0 },
        warnings: [`Provider record missing in Airtable: ${pkg.name}`],
        recommendedUiTreatment: "Summary Only",
      });
      continue;
    }

    const providerId = existingRec.id;
    const fieldsBefore = { ...existingRec.fields };

    const criteriaBefore = recordsForProvider(allCriteria, CRITERIA_FIELD.provider, providerId);
    const documentsBefore = recordsForProvider(allDocuments, DOCUMENT_FIELD.provider, providerId);
    const sourcesBefore = recordsForProvider(allSources, SOURCE_FIELD.provider, providerId);
    const contactsBefore = recordsForProvider(allContacts, CONTACT_FIELD.provider, providerId);

    const tabBefore = scoreProviderTabs({
      providerFields: fieldsBefore,
      criteriaRecords: criteriaBefore,
      documentRecords: documentsBefore,
      sourceRecords: sourcesBefore,
      contactRecords: contactsBefore,
      documentFieldMap: DOCUMENT_FIELD,
      criteriaFieldMap: CRITERIA_FIELD,
    });

    const populatedBefore = listPopulatedProviderFields(fieldsBefore);
    const recordsCreated = {
      criteria: { created: 0, skipped: 0 },
      requiredDocuments: { created: 0, skipped: 0 },
      sourceReferences: { created: 0, skipped: 0 },
      contacts: { created: 0, skipped: 0 },
    };

    const seedFields = buildProviderAirtableFields(pkg);
    let providerPatch = mergeProviderPatch(fieldsBefore, seedFields);
    providerPatch = {
      ...providerPatch,
      ...mergeProviderPatch(fieldsBefore, buildInternalNotesPatch(fieldsBefore, pkg)),
    };

    if (
      pkg.provider.profileStatus === "Needs Review" &&
      String(fieldsBefore[F.profileStatus] || "") !== "Active"
    ) {
      if (!fieldPopulated(fieldsBefore, F.profileStatus) || fieldsBefore[F.profileStatus] !== "Active") {
        providerPatch[F.profileStatus] = "Needs Review";
      }
    }

    const providerStats = { created: 0, updated: 0 };
    await updateProviderIfNeeded(base, providerId, providerPatch, providerStats);

    for (const src of pkg.sources || []) {
      const key = `${providerId}::${src.sourceUrl}`;
      await createIfMissing(
        base,
        TABLE_SOURCE_REFERENCES,
        key,
        sourceIndex,
        buildSourceFields(src, providerId),
        recordsCreated.sourceReferences
      );
    }

    for (const row of pkg.criteria || []) {
      const key = `${providerId}::${row.criteriaName}`;
      await createIfMissing(
        base,
        TABLE_CRITERIA,
        key,
        criteriaIndex,
        buildCriteriaFields(row, providerId),
        recordsCreated.criteria
      );
    }

    for (const row of pkg.documents || []) {
      const key = `${providerId}::${row.documentRequirementName}`;
      await createIfMissing(
        base,
        TABLE_REQUIRED_DOCUMENTS,
        key,
        docIndex,
        buildDocumentFields(row, providerId),
        recordsCreated.requiredDocuments
      );
    }

    for (const row of pkg.contacts || []) {
      const key = `${providerId}::${row.contactName}`;
      await createIfMissing(
        base,
        TABLE_CONTACTS,
        key,
        contactIndex,
        buildContactFields(row, providerId),
        recordsCreated.contacts
      );
    }

    const providerSpecificDocCount = documentsBefore.filter(
      (d) => !isGeneralReadinessDocument(d, DOCUMENT_FIELD)
    ).length;
    const seedDocCount = (pkg.documents || []).length;
    const needsGeneralReadiness =
      providerSpecificDocCount + seedDocCount < 4 &&
      tabBefore.requiredInfo !== "Strong";

    if (needsGeneralReadiness) {
      for (const template of GENERAL_FINANCING_READINESS_DOCS) {
        const row = buildGeneralReadinessDocumentRow(pkg.name, template, providerId);
        const key = `${providerId}::${row.documentRequirementName}`;
        await createIfMissing(
          base,
          TABLE_REQUIRED_DOCUMENTS,
          key,
          docIndex,
          buildDocumentFields(row, providerId),
          recordsCreated.requiredDocuments
        );
      }
    }

    const fieldsAfter = { ...fieldsBefore, ...providerPatch };

    let criteriaAfter = [...criteriaBefore];
    for (const row of pkg.criteria || []) {
      const key = `${providerId}::${row.criteriaName}`;
      const hit = criteriaIndex.get(key);
      if (hit && !criteriaAfter.some((r) => r.id === hit.id)) criteriaAfter.push(hit);
      else if (!hit && DRY) criteriaAfter.push({ id: `dry-${key}`, fields: buildCriteriaFields(row, providerId) });
    }

    let documentsAfter = [...documentsBefore];
    for (const row of pkg.documents || []) {
      const key = `${providerId}::${row.documentRequirementName}`;
      const hit = docIndex.get(key);
      if (hit && !documentsAfter.some((r) => r.id === hit.id)) documentsAfter.push(hit);
      else if (!hit && DRY) documentsAfter.push({ id: `dry-${key}`, fields: buildDocumentFields(row, providerId) });
    }
    if (needsGeneralReadiness) {
      for (const template of GENERAL_FINANCING_READINESS_DOCS) {
        const reqName = `General Financing Readiness — ${template.documentName}`;
        const key = `${providerId}::${reqName}`;
        const hit = docIndex.get(key);
        if (hit && !documentsAfter.some((r) => r.id === hit.id)) documentsAfter.push(hit);
        else if (!hit && DRY) {
          documentsAfter.push({
            id: `dry-${key}`,
            fields: buildDocumentFields(
              buildGeneralReadinessDocumentRow(pkg.name, template, providerId),
              providerId
            ),
          });
        }
      }
    }
    if (!DRY) {
      const freshDocs = await loadAll(base, TABLE_REQUIRED_DOCUMENTS);
      documentsAfter = recordsForProvider(freshDocs, DOCUMENT_FIELD.provider, providerId);
    }

    let sourcesAfter = [...sourcesBefore];
    for (const src of pkg.sources || []) {
      const key = `${providerId}::${src.sourceUrl}`;
      const hit = sourceIndex.get(key);
      if (hit && !sourcesAfter.some((r) => r.id === hit.id)) sourcesAfter.push(hit);
      else if (!hit && DRY) sourcesAfter.push({ id: `dry-${key}`, fields: buildSourceFields(src, providerId) });
    }
    if (!DRY) {
      const freshSources = await loadAll(base, TABLE_SOURCE_REFERENCES);
      sourcesAfter = recordsForProvider(freshSources, SOURCE_FIELD.provider, providerId);
    }

    let contactsAfter = [...contactsBefore];
    for (const row of pkg.contacts || []) {
      const key = `${providerId}::${row.contactName}`;
      const hit = contactIndex.get(key);
      if (hit && !contactsAfter.some((r) => r.id === hit.id)) contactsAfter.push(hit);
      else if (!hit && DRY) contactsAfter.push({ id: `dry-${key}`, fields: buildContactFields(row, providerId) });
    }
    if (!DRY) {
      const freshContacts = await loadAll(base, TABLE_CONTACTS);
      contactsAfter = recordsForProvider(freshContacts, CONTACT_FIELD.provider, providerId);
    }

    if (!DRY) {
      const freshCriteria = await loadAll(base, TABLE_CRITERIA);
      criteriaAfter = recordsForProvider(freshCriteria, CRITERIA_FIELD.provider, providerId);
    }

    const tabAfter = scoreProviderTabs({
      providerFields: fieldsAfter,
      criteriaRecords: criteriaAfter,
      documentRecords: documentsAfter,
      sourceRecords: sourcesAfter,
      contactRecords: contactsAfter,
      documentFieldMap: DOCUMENT_FIELD,
      criteriaFieldMap: CRITERIA_FIELD,
    });

    const populatedAfter = listPopulatedProviderFields(fieldsAfter);
    const stillMissing = listMissingBackfillFields(fieldsAfter);

    const report = {
      providerName: pkg.name,
      recordId: providerId,
      overallReadiness: overallReadinessFromTabs(tabAfter),
      overallReadinessBefore: overallReadinessFromTabs(tabBefore),
      recommendedProfileStatus: fieldsAfter[F.profileStatus] || pkg.provider.profileStatus,
      sourceConfidence: fieldsAfter[F.sourceConfidence] || pkg.provider.sourceConfidence,
      tabReadiness: tabAfter,
      tabReadinessBefore: tabBefore,
      fieldsPopulatedBefore: populatedBefore,
      fieldsPopulatedAfter: populatedAfter,
      fieldsStillMissing: stillMissing.map((name) => name),
      recordsCreated: {
        criteria: recordsCreated.criteria.created,
        requiredDocuments: recordsCreated.requiredDocuments.created,
        sourceReferences: recordsCreated.sourceReferences.created,
        contacts: recordsCreated.contacts.created,
      },
      providerFieldsUpdated: providerStats.updated,
      generalReadinessDocumentsAdded: needsGeneralReadiness,
      warnings: [...(pkg.warnings || [])],
      fieldsSkippedByDesign: pkg.fieldsSkipped || [],
      recommendedUiTreatment: recommendedUiTreatment(tabAfter),
    };

    providerReports.push(report);

    console.log(
      `${pkg.name}: ${report.overallReadinessBefore} → ${report.overallReadiness} | UI: ${report.recommendedUiTreatment}` +
        (needsGeneralReadiness ? " | +general readiness docs" : "")
    );
  }

  const globalRecommendation = buildGlobalTabRecommendation(providerReports);

  const audit = {
    runDate,
    mode: DRY ? "dry-run" : "apply",
    providersAudited: PUBLIC_SEED_PROVIDERS.length,
    providersFoundInAirtable: providerReports.filter((p) => p.recordId).length,
    providers: providerReports,
    globalRecommendation,
  };

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, JSON.stringify(audit, null, 2), "utf8");

  printSummaryTable(providerReports);

  console.log(`\nGlobal recommendation: Option ${globalRecommendation.option}`);
  console.log(`Tabs: ${globalRecommendation.recommendedMvpTabStructure.join(" | ")}`);
  console.log(`Reasoning: ${globalRecommendation.reasoning}`);
  console.log(`\nAudit report: ${REPORT_PATH}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
