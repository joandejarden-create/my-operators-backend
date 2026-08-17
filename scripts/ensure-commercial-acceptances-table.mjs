/**
 * Ensure "Commercial Acceptances" table + fields on Deal Capture MVP (AIRTABLE_BASE_ID).
 *
 * Spec: docs/commercial-acceptance-airtable-fields.md
 * Field map: lib/commercial-acceptance/field-map.js
 *
 * Prerequisites:
 *   AIRTABLE_API_KEY with schema.bases:read + schema.bases:write
 *   AIRTABLE_BASE_ID = Deal Capture MVP
 *
 * Usage:
 *   node scripts/ensure-commercial-acceptances-table.mjs
 *   node scripts/ensure-commercial-acceptances-table.mjs --apply
 *
 * Report: reports/ensure-commercial-acceptances-table.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  PLATFORM_USERS_COMPANY_TABLE_ID,
  PLATFORM_USERS_TABLE_ID,
} from "../lib/airtable/platform-users-table.js";
import {
  COMMERCIAL_ACCEPTANCES_TABLE_NAME,
  MAP_COMMERCIAL_ACCEPTANCE as F,
  RECORD_LABEL_FORMULA,
  VAL_ACCEPTANCE_METHOD,
  VAL_ACCEPTANCE_STATUS,
  VAL_ACCEPTANCE_TYPE,
  VAL_BILLING_CLASS,
  VAL_DISCOUNT_APPLIES_TO,
  VAL_DISCOUNT_DURATION,
  VAL_DISCOUNT_TYPE,
  VAL_MEMBER_TYPE,
  VAL_PARTICIPATION_LABEL,
  VAL_SCHEDULE_TEMPLATE,
} from "../lib/commercial-acceptance/field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}

function singleSelect(name, optionNames, description) {
  const field = { name, type: "singleSelect", options: choices(optionNames) };
  if (description) field.description = description;
  return field;
}

function multiSelect(name, optionNames, description) {
  const field = { name, type: "multipleSelects", options: choices(optionNames) };
  if (description) field.description = description;
  return field;
}

function dateField(name, description) {
  const field = {
    name,
    type: "date",
    options: { dateFormat: { name: "iso" } },
  };
  if (description) field.description = description;
  return field;
}

function dateTimeField(name, description) {
  const field = {
    name,
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  };
  if (description) field.description = description;
  return field;
}

function currencyField(name, description) {
  const field = {
    name,
    type: "currency",
    options: { precision: 2, symbol: "USD" },
  };
  if (description) field.description = description;
  return field;
}

function percentField(name, description) {
  const field = {
    name,
    type: "percent",
    options: { precision: 0 },
  };
  if (description) field.description = description;
  return field;
}

function numberField(name, precision = 0, description) {
  const field = { name, type: "number", options: { precision } };
  if (description) field.description = description;
  return field;
}

function checkboxField(name, description) {
  const field = {
    name,
    type: "checkbox",
    options: { icon: "check", color: "greenBright" },
  };
  if (description) field.description = description;
  return field;
}

function linkField(name, linkedTableId, description) {
  const field = {
    name,
    type: "multipleRecordLinks",
    options: { linkedTableId },
  };
  if (description) field.description = description;
  return field;
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

/** Fields created with the table (no self-links). Primary field first. */
function buildCoreFields(companyProfileTableId, usersTableId) {
  return [
    {
      name: F.acceptanceId,
      type: "singleLineText",
      description: "Human ID, e.g. ACC-2026-0001 or CTS-FOUND-001",
    },
    {
      name: F.memberLegalName,
      type: "singleLineText",
      description: "Exact legal entity name on Schedule",
    },
    linkField(F.companyProfile, companyProfileTableId, "Link when member has product account"),
    linkField(F.users, usersTableId, "Member Representative user record(s)"),
    { name: F.memberAccountId, type: "singleLineText" },
    singleSelect(F.acceptanceType, VAL_ACCEPTANCE_TYPE),
    singleSelect(F.memberType, VAL_MEMBER_TYPE),
    singleSelect(F.billingClass, VAL_BILLING_CLASS),
    singleSelect(F.participationLabel, VAL_PARTICIPATION_LABEL),
    { name: F.termsVersion, type: "singleLineText", description: "e.g. 2026-07-16" },
    { name: F.scheduleVersion, type: "singleLineText", description: "e.g. v1.0" },
    singleSelect(F.scheduleTemplate, VAL_SCHEDULE_TEMPLATE),
    { name: F.termsUrl, type: "url" },
    { name: F.acceptedByName, type: "singleLineText" },
    { name: F.acceptedByEmail, type: "email" },
    { name: F.acceptedByTitle, type: "singleLineText" },
    dateTimeField(F.acceptedAt, "UTC preferred"),
    singleSelect(F.acceptanceMethod, VAL_ACCEPTANCE_METHOD),
    { name: F.acceptanceEvidence, type: "multipleAttachments" },
    { name: F.acceptanceEvidenceNotes, type: "multilineText" },
    { name: F.ipAddress, type: "singleLineText" },
    { name: F.userAgent, type: "multilineText" },
    dateField(F.effectiveDate),
    dateField(F.initialTermEndDate),
    dateField(F.foundingEndDate),
    dateField(F.paidTransitionReviewDate, "Reminder: ~30 days before founding end"),
    checkboxField(F.autoRenewal),
    numberField(F.nonRenewalNoticeDays, 0),
    currencyField(F.listSubscriptionAnnualUsd, "Pre-discount / standard list price"),
    currencyField(F.subscriptionAnnualUsd, "Net after discount; 0 for founding"),
    checkboxField(F.successFeeWaived),
    currencyField(F.upfrontSubmissionFeeUsd),
    currencyField(F.listPerKeyRateUsd, "Pre-discount per-key rate"),
    currencyField(F.perKeyRateUsd, "Net after discount"),
    currencyField(F.listMinimumSuccessFeeUsd),
    currencyField(F.minimumSuccessFeeUsd),
    percentField(F.loiCommitmentFeePct, "e.g. 80%"),
    percentField(F.finalSuccessFeePct, "e.g. 20%"),
    numberField(F.tailPeriodMonths, 0),
    checkboxField(F.discountApplied),
    singleSelect(F.discountType, VAL_DISCOUNT_TYPE),
    percentField(F.discountPercent, "e.g. 25 for 25% off"),
    currencyField(F.discountAmountUsd, "Fixed dollar discount"),
    multiSelect(F.discountAppliesTo, VAL_DISCOUNT_APPLIES_TO),
    singleSelect(F.discountDuration, VAL_DISCOUNT_DURATION),
    dateField(F.discountValidThrough),
    { name: F.discountCodeLabel, type: "singleLineText" },
    { name: F.discountReason, type: "multilineText" },
    { name: F.discountApprovedBy, type: "singleLineText" },
    { name: F.feeNotes, type: "multilineText" },
    singleSelect(F.acceptanceStatus, VAL_ACCEPTANCE_STATUS),
    checkboxField(F.platformAccessGranted),
    dateTimeField(F.accessGrantedAt),
    { name: F.grantedBy, type: "singleLineText", description: "Internal ops owner name" },
    { name: F.internalNotes, type: "multilineText" },
    {
      name: F.dealalityContactEmail,
      type: "email",
      description: "Default hello@aohospitalityadvisors.com",
    },
    { name: F.memberRepresentativeEmail, type: "email" },
  ];
}

function buildFormulaField() {
  return {
    name: F.recordLabel,
    type: "formula",
    description: "Display label for grids and linked records",
    options: { formula: RECORD_LABEL_FORMULA },
  };
}

function buildSelfLinkFields(tableId) {
  return [
    linkField(F.supersededBy, tableId, "Points to newer schedule row"),
    linkField(F.previousAcceptance, tableId, "Prior schedule this replaces"),
  ];
}

function findTable(tables, nameOrId) {
  return (tables || []).find((t) => t.name === nameOrId || t.id === nameOrId);
}

function existingFieldNames(table) {
  return new Set((table.fields || []).map((f) => f.name));
}

async function createField(baseId, token, tableId, fieldSpec, report) {
  if (!APPLY) {
    report.wouldCreateFields.push(fieldSpec.name);
    return { ok: true, dryRun: true };
  }
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify(fieldSpec),
  });
  if (!res.ok) {
    report.errors.push({ field: fieldSpec.name, status: res.status, error: json });
    return { ok: false, json };
  }
  report.createdFields.push({ name: fieldSpec.name, id: json.id });
  return { ok: true, json };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID (Deal Capture MVP)");
  }

  const report = {
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    tableName: COMMERCIAL_ACCEPTANCES_TABLE_NAME,
    tableId: null,
    createdTable: false,
    wouldCreateTable: false,
    wouldCreateFields: [],
    createdFields: [],
    skippedExisting: [],
    errors: [],
  };

  console.log(`Mode: ${report.mode}`);
  console.log(`Base: ${baseId} (Deal Capture MVP / AIRTABLE_BASE_ID)`);
  console.log(`Table: ${COMMERCIAL_ACCEPTANCES_TABLE_NAME}`);

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) {
    throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);
  }

  const tables = listJson.tables || [];
  const companyProfile =
    findTable(tables, PLATFORM_USERS_COMPANY_TABLE_ID) ||
    findTable(tables, "Company Profile");
  const users =
    findTable(tables, PLATFORM_USERS_TABLE_ID) || findTable(tables, "Users");

  if (!companyProfile) {
    throw new Error(
      `Company Profile table not found (expected id ${PLATFORM_USERS_COMPANY_TABLE_ID} or name "Company Profile")`
    );
  }
  if (!users) {
    throw new Error(
      `Users table not found (expected id ${PLATFORM_USERS_TABLE_ID} or name "Users")`
    );
  }

  console.log(`Company Profile: ${companyProfile.name} (${companyProfile.id})`);
  console.log(`Users: ${users.name} (${users.id})`);

  let table = findTable(tables, COMMERCIAL_ACCEPTANCES_TABLE_NAME);

  if (!table) {
    const coreFields = buildCoreFields(companyProfile.id, users.id);
    if (!APPLY) {
      report.wouldCreateTable = true;
      report.wouldCreateFields = [
        ...coreFields.map((f) => f.name),
        F.recordLabel,
        F.supersededBy,
        F.previousAcceptance,
      ];
      console.log(
        `[dry-run] Would create table "${COMMERCIAL_ACCEPTANCES_TABLE_NAME}" with ${report.wouldCreateFields.length} fields`
      );
    } else {
      const body = {
        name: COMMERCIAL_ACCEPTANCES_TABLE_NAME,
        description:
          "Terms of Service and Commercial Terms Schedule acceptance records (confidential). Spec: docs/commercial-acceptance-airtable-fields.md",
        fields: coreFields,
      };
      const { res, json } = await metaFetch(baseId, token, "/tables", {
        method: "POST",
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        throw new Error(`Create table failed ${res.status}: ${JSON.stringify(json)}`);
      }
      table = json;
      report.createdTable = true;
      report.tableId = table.id;
      report.createdFields = (table.fields || []).map((f) => ({ name: f.name, id: f.id }));
      console.log(`Created table "${COMMERCIAL_ACCEPTANCES_TABLE_NAME}" (${table.id}).`);
    }
  } else {
    report.tableId = table.id;
    console.log(`Table already exists: ${table.id}`);
  }

  // Refresh table schema if we need to add missing fields
  if (table?.id || report.tableId) {
    const tableId = table?.id || report.tableId;
    const { res: refreshRes, json: refreshJson } = await metaFetch(baseId, token, "/tables");
    if (!refreshRes.ok) {
      throw new Error(`Refresh tables failed: ${JSON.stringify(refreshJson)}`);
    }
    table = findTable(refreshJson.tables, tableId) || table;
    report.tableId = table?.id || tableId;

    if (table) {
      const existing = existingFieldNames(table);
      const pending = [];

      // Core fields (if table pre-existed with fewer fields)
      for (const field of buildCoreFields(companyProfile.id, users.id)) {
        if (existing.has(field.name)) {
          report.skippedExisting.push(field.name);
        } else if (!report.createdTable || !report.createdFields.some((c) => c.name === field.name)) {
          // Only queue if not just created with the table
          if (!report.createdTable) pending.push(field);
        }
      }

      if (!existing.has(F.recordLabel)) {
        pending.push(buildFormulaField());
      } else {
        report.skippedExisting.push(F.recordLabel);
      }

      for (const field of buildSelfLinkFields(table.id)) {
        if (existing.has(field.name)) {
          report.skippedExisting.push(field.name);
        } else {
          pending.push(field);
        }
      }

      // Deduplicate pending by name
      const seen = new Set();
      const uniquePending = pending.filter((f) => {
        if (seen.has(f.name)) return false;
        seen.add(f.name);
        return true;
      });

      for (const field of uniquePending) {
        console.log(`${APPLY ? "Creating" : "[dry-run] Would create"} field: ${field.name}`);
        await createField(baseId, token, table.id, field, report);
      }
    }
  }

  // De-dupe skipped list
  report.skippedExisting = [...new Set(report.skippedExisting)];

  const outPath = path.join(ROOT, "reports", "ensure-commercial-acceptances-table.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

  console.log("\n--- Summary ---");
  console.log(`Created table: ${report.createdTable}`);
  console.log(`Would create table: ${report.wouldCreateTable}`);
  console.log(`Created fields: ${report.createdFields.length}`);
  console.log(`Would create fields: ${report.wouldCreateFields.length}`);
  console.log(`Skipped existing: ${report.skippedExisting.length}`);
  console.log(`Errors: ${report.errors.length}`);
  if (report.tableId) {
    console.log(`\nAdd to .env (optional): COMMERCIAL_ACCEPTANCES_TABLE_ID=${report.tableId}`);
  }
  console.log(`Report: ${outPath}`);

  if (!APPLY) {
    console.log("\nRe-run with --apply to write schema to Airtable.");
  }

  if (report.errors.length) {
    process.exitCode = 1;
    console.error("Errors:", JSON.stringify(report.errors, null, 2));
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
