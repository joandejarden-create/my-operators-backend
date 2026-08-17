/**
 * Ensure Decision Opportunities + Decision Opportunity Evidence tables on GTM base.
 *
 * Prerequisites:
 *   AIRTABLE_GTM_BASE_ID (must NOT be product base)
 *   Owner Targets, Properties, Contacts tables should already exist for links
 *   PAT with schema.bases:read (+ schema.bases:write for --apply)
 *
 * Usage:
 *   node scripts/ensure-gtm-decision-opportunities-schema.mjs
 *   node scripts/ensure-gtm-decision-opportunities-schema.mjs --dry-run
 *   node scripts/ensure-gtm-decision-opportunities-schema.mjs --apply
 *
 * Default is dry-run (safe). Explicit --apply required for writes.
 *
 * Report: reports/ensure-gtm-decision-opportunities-schema.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  GTM_DECISION_OPPORTUNITIES_TABLE,
  GTM_DECISION_OPPORTUNITY_EVIDENCE_TABLE,
  GTM_DECISION_RADAR_LINKED_TABLES,
  MAP_DECISION_OPPORTUNITY,
  MAP_DECISION_OPPORTUNITY_EVIDENCE,
} from "../lib/gtm-owner-target/decision-opportunity-field-map.js";
import {
  buildDecisionOpportunityCoreFields,
  buildDecisionOpportunityLinkFields,
  buildDecisionOpportunityEvidenceFields,
  classifyFieldEnsureAction,
  getDecisionRadarSchemaSummary,
} from "../lib/gtm-owner-target/decision-opportunity-schema-spec.js";
import {
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
// --dry-run is default; accept explicit flag for clarity
const DRY_RUN = !APPLY;

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

function findTable(tables, name) {
  return (tables || []).find((t) => t.name === name) || null;
}

function fieldByName(table, name) {
  return (table?.fields || []).find((f) => f.name === name) || null;
}

/**
 * @param {object} reportEntry
 * @param {object} table
 * @param {object[]} fieldSpecs
 * @param {{ baseId: string, apiKey: string }} ctx
 */
async function ensureFieldsOnTable(reportEntry, table, fieldSpecs, ctx) {
  for (const spec of fieldSpecs) {
    const existing = fieldByName(table, spec.name);
    const classification = classifyFieldEnsureAction(existing, spec);

    if (classification.action === "conflict") {
      reportEntry.fieldsConflict.push({
        name: spec.name,
        reason: classification.reason,
        existingType: existing?.type,
        desiredType: spec.type,
      });
      console.error("CONFLICT", table.name, spec.name, classification.reason);
      continue;
    }

    if (classification.action === "skip") {
      reportEntry.fieldsSkipped.push(spec.name);
      console.log("SKIP", table.name, spec.name);
      continue;
    }

    if (classification.action === "add_choices") {
      if (DRY_RUN) {
        reportEntry.choicesWouldAdd.push({
          field: spec.name,
          missing: classification.missingChoices,
        });
        console.log(
          "WOULD ADD CHOICES",
          table.name,
          spec.name,
          classification.missingChoices.join(", ")
        );
        continue;
      }
      const { res, json } = await metaFetch(
        ctx.baseId,
        ctx.apiKey,
        `/tables/${table.id}/fields/${existing.id}`,
        {
          method: "PATCH",
          body: JSON.stringify({
            options: {
              choices: classification.allChoices.map((name) => ({ name })),
            },
          }),
        }
      );
      if (!res.ok) {
        reportEntry.fieldsFailed.push({ name: spec.name, error: json, action: "add_choices" });
        console.error("CHOICES FAILED", table.name, spec.name, JSON.stringify(json));
      } else {
        reportEntry.choicesAdded.push({
          field: spec.name,
          missing: classification.missingChoices,
        });
        // refresh local field cache
        existing.options = json.options || existing.options;
        console.log("ADDED CHOICES", table.name, spec.name);
      }
      continue;
    }

    // create
    if (DRY_RUN) {
      reportEntry.fieldsWouldCreate.push(spec.name);
      console.log("WOULD CREATE FIELD", table.name, spec.name, `(${spec.type})`);
      continue;
    }
    const { res, json } = await metaFetch(
      ctx.baseId,
      ctx.apiKey,
      `/tables/${table.id}/fields`,
      {
        method: "POST",
        body: JSON.stringify(spec),
      }
    );
    if (!res.ok) {
      reportEntry.fieldsFailed.push({ name: spec.name, error: json });
      console.error("FIELD FAILED", table.name, spec.name, JSON.stringify(json));
    } else {
      reportEntry.fieldsCreated.push(spec.name);
      table.fields = table.fields || [];
      table.fields.push(json);
      console.log("CREATED FIELD", table.name, spec.name);
    }
  }
}

async function ensureTableCreated(tables, report, ctx, { tableName, description, primaryField }) {
  let table = findTable(tables, tableName);
  const entry = {
    tableName,
    tableId: table?.id || null,
    createdTable: false,
    wouldCreateTable: false,
    fieldsWouldCreate: [],
    fieldsCreated: [],
    fieldsSkipped: [],
    fieldsFailed: [],
    fieldsConflict: [],
    choicesWouldAdd: [],
    choicesAdded: [],
  };

  if (!table) {
    if (DRY_RUN) {
      entry.wouldCreateTable = true;
      console.log("WOULD CREATE TABLE", tableName);
      report.tables.push(entry);
      return { table: null, entry };
    }
    const { res, json } = await metaFetch(ctx.baseId, ctx.apiKey, "/tables", {
      method: "POST",
      body: JSON.stringify({
        name: tableName,
        description,
        fields: [primaryField],
      }),
    });
    if (!res.ok) {
      throw new Error(`Create table ${tableName} failed: ${JSON.stringify(json)}`);
    }
    table = json;
    entry.createdTable = true;
    entry.tableId = json.id;
    entry.fieldsCreated.push(primaryField.name);
    tables.push(json);
    console.log("CREATED TABLE", json.name, json.id);
  } else {
    console.log("TABLE EXISTS", table.name, table.id);
  }

  report.tables.push(entry);
  return { table, entry };
}

async function main() {
  const { apiKey, baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const ctx = { apiKey, baseId };

  const { res: listRes, json: listJson } = await metaFetch(baseId, apiKey, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed: ${JSON.stringify(listJson)}`);

  const tables = listJson.tables || [];
  const ownerTargets = findTable(tables, GTM_DECISION_RADAR_LINKED_TABLES.ownerTargets);
  const properties = findTable(tables, GTM_DECISION_RADAR_LINKED_TABLES.properties);
  const contacts = findTable(tables, GTM_DECISION_RADAR_LINKED_TABLES.contacts);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    schemaSummary: getDecisionRadarSchemaSummary(),
    linkedTables: {
      ownerTargets: ownerTargets
        ? { id: ownerTargets.id, name: ownerTargets.name }
        : { missing: true, expected: GTM_DECISION_RADAR_LINKED_TABLES.ownerTargets },
      properties: properties
        ? { id: properties.id, name: properties.name }
        : { missing: true, expected: GTM_DECISION_RADAR_LINKED_TABLES.properties },
      contacts: contacts
        ? { id: contacts.id, name: contacts.name }
        : { missing: true, expected: GTM_DECISION_RADAR_LINKED_TABLES.contacts },
    },
    warnings: [],
    tables: [],
    blocked: false,
    blockReasons: [],
    guardrails: {
      internalOnly: true,
      neverSyncToProductBases: true,
      separateFromTargetOs: true,
      noAutoOutreach: true,
      noWebhoundAutoIngest: true,
    },
  };

  if (!ownerTargets) {
    report.warnings.push(
      `Owner Targets table "${GTM_DECISION_RADAR_LINKED_TABLES.ownerTargets}" not found — Owner Target link will be skipped.`
    );
  }
  if (!properties) {
    report.warnings.push(
      `Properties table "${GTM_DECISION_RADAR_LINKED_TABLES.properties}" not found — Lead Property link will be skipped.`
    );
  }
  if (!contacts) {
    report.warnings.push(
      `Contacts table "${GTM_DECISION_RADAR_LINKED_TABLES.contacts}" not found — Decision Makers link will be skipped.`
    );
  }

  const M = MAP_DECISION_OPPORTUNITY;
  const coreOpportunityFields = buildDecisionOpportunityCoreFields();
  const primaryOpportunity =
    coreOpportunityFields.find((f) => f.name === M.opportunityName) ||
    coreOpportunityFields[0];

  const { table: opportunitiesTable, entry: oppEntry } = await ensureTableCreated(
    tables,
    report,
    ctx,
    {
      tableName: GTM_DECISION_OPPORTUNITIES_TABLE,
      description:
        "Dealality Decision Radar — one hotel/project + one strategic decision window. Internal GTM only. Not Target OS.",
      primaryField: {
        name: primaryOpportunity.name,
        type: primaryOpportunity.type,
        ...(primaryOpportunity.description
          ? { description: primaryOpportunity.description }
          : {}),
      },
    }
  );

  // Remaining core fields (exclude primary already created on new table)
  const opportunityFieldsToEnsure = opportunitiesTable
    ? coreOpportunityFields
    : coreOpportunityFields; // dry-run without table: list all as would-create via fieldsWouldCreate on entry

  if (!opportunitiesTable && DRY_RUN) {
    oppEntry.fieldsWouldCreate = coreOpportunityFields.map((f) => f.name);
    const linkPreview = buildDecisionOpportunityLinkFields({
      ownerTargetsTableId: ownerTargets?.id || "tbl_owner_targets_preview",
      propertiesTableId: properties?.id || "tbl_properties_preview",
      contactsTableId: contacts?.id || "tbl_contacts_preview",
      opportunitiesTableId: "tbl_opportunities_preview",
    });
    for (const f of linkPreview) {
      if (!oppEntry.fieldsWouldCreate.includes(f.name)) {
        oppEntry.fieldsWouldCreate.push(f.name);
      }
    }
    console.log(
      "WOULD CREATE FIELDS",
      GTM_DECISION_OPPORTUNITIES_TABLE,
      `(${oppEntry.fieldsWouldCreate.length})`
    );
  } else if (opportunitiesTable) {
    await ensureFieldsOnTable(oppEntry, opportunitiesTable, opportunityFieldsToEnsure, ctx);
  }

  // Evidence table
  const E = MAP_DECISION_OPPORTUNITY_EVIDENCE;
  const opportunitiesTableId =
    opportunitiesTable?.id ||
    findTable(tables, GTM_DECISION_OPPORTUNITIES_TABLE)?.id ||
    null;

  const evidenceCore = buildDecisionOpportunityEvidenceFields({});
  const primaryEvidence =
    evidenceCore.find((f) => f.name === E.evidenceId) || evidenceCore[0];

  const { table: evidenceTable, entry: evidenceEntry } = await ensureTableCreated(
    tables,
    report,
    ctx,
    {
      tableName: GTM_DECISION_OPPORTUNITY_EVIDENCE_TABLE,
      description:
        "Evidence/provenance for Decision Opportunities. Supports Open and Supports Closed / Too Late.",
      primaryField: {
        name: primaryEvidence.name,
        type: primaryEvidence.type,
        ...(primaryEvidence.description ? { description: primaryEvidence.description } : {}),
      },
    }
  );

  const evidenceFields = buildDecisionOpportunityEvidenceFields({
    opportunitiesTableId: opportunitiesTableId || undefined,
  });

  if (!evidenceTable && DRY_RUN) {
    evidenceEntry.fieldsWouldCreate = evidenceFields.map((f) => f.name);
    if (!opportunitiesTableId) {
      evidenceEntry.fieldsWouldCreate = buildDecisionOpportunityEvidenceFields({
        opportunitiesTableId: "tbl_opportunities_preview",
      }).map((f) => f.name);
    }
    console.log(
      "WOULD CREATE FIELDS",
      GTM_DECISION_OPPORTUNITY_EVIDENCE_TABLE,
      `(${evidenceEntry.fieldsWouldCreate.length})`
    );
  } else if (evidenceTable) {
    await ensureFieldsOnTable(evidenceEntry, evidenceTable, evidenceFields, ctx);
  }

  // Opportunity link fields (Owner Target, Lead Property, Decision Makers, Duplicate Of)
  if (opportunitiesTable) {
    const linkFields = buildDecisionOpportunityLinkFields({
      ownerTargetsTableId: ownerTargets?.id,
      propertiesTableId: properties?.id,
      contactsTableId: contacts?.id,
      opportunitiesTableId: opportunitiesTable.id,
    });
    await ensureFieldsOnTable(oppEntry, opportunitiesTable, linkFields, ctx);
  }

  // Note inverse evidence field expectation
  if (opportunitiesTable) {
    const inverse = fieldByName(opportunitiesTable, M.evidence);
    if (inverse) {
      oppEntry.fieldsSkipped.push(M.evidence);
      console.log("SKIP", opportunitiesTable.name, M.evidence, "(inverse link present)");
    } else if (evidenceTable || DRY_RUN) {
      report.warnings.push(
        `Inverse link "${M.evidence}" on Decision Opportunities is created by Airtable when Evidence."${E.decisionOpportunity}" is added. Re-run ensure after apply if missing.`
      );
    }
  }

  const hasConflicts = report.tables.some((t) => (t.fieldsConflict || []).length > 0);
  const hasFailures = report.tables.some((t) => (t.fieldsFailed || []).length > 0);
  if (hasConflicts) {
    report.blocked = true;
    report.blockReasons.push(
      "Incompatible existing field(s) — will not modify silently. Resolve conflicts before --apply."
    );
  }
  if (hasFailures) {
    report.blocked = true;
    report.blockReasons.push("One or more field create/patch operations failed.");
  }

  const outPath = path.join(ROOT, "reports", "ensure-gtm-decision-opportunities-schema.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("Wrote", outPath);
  console.log(
    APPLY
      ? report.blocked
        ? "Apply finished with BLOCKERS — see report."
        : "Apply complete."
      : "Dry-run complete (no Airtable writes). Pass --apply to create schema."
  );

  if (report.blocked && APPLY) process.exitCode = 1;
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
