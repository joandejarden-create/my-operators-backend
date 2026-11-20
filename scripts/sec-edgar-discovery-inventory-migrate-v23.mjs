#!/usr/bin/env node
/**
 * SEC EDGAR V2.3 — bounded migration of quality-classified inventory into Airtable.
 *
 * Cohort A: VERIFIED_PROPERTY (identity-verified lifecycle)
 * Cohort B: PROBABLE_PROPERTY (research-only)
 * Does NOT import ambiguous / multi / artifact / generic rows.
 *
 *   node scripts/sec-edgar-discovery-inventory-migrate-v23.mjs --dry-run
 *   node scripts/sec-edgar-discovery-inventory-migrate-v23.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import {
  DISCOVERY_INVENTORY_CONTRACT_V23,
  DISCOVERY_SCHEMA_VERSION,
  GRAPH_BASE_ID_CANONICAL,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  assertCanonicalDiscoveryBase,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import { normalizeHotelDisplayName } from "../lib/hotel-intelligence/sec-edgar/hotel-identity-quality-v2.js";
import { detectBrandFamily } from "../lib/hotel-intelligence/sec-edgar/brand-families-v2.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;

const IDS_PATH = path.join(
  ROOT,
  "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json",
);
const INVENTORY_PATH = path.join(
  ROOT,
  "reports/sec-edgar-identity-quality-v2/VERIFIED_HOTEL_INTELLIGENCE_INVENTORY.json",
);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function atFetch(baseId, tableId, token, { method = "GET", pathSuffix = "", body } = {}) {
  const url = `https://api.airtable.com/v0/${baseId}/${tableId}${pathSuffix}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

function brandFamilyOption(row) {
  const fam = String(row.brandFamily || detectBrandFamily(row.hotelName || "").family || "UNKNOWN").toUpperCase();
  const allowed = new Set([
    "MARRIOTT",
    "HILTON",
    "HYATT",
    "IHG",
    "WYNDHAM",
    "CHOICE",
    "ACCOR",
    "RADISSON",
    "INDEPENDENT",
    "OTHER",
    "UNKNOWN",
  ]);
  return allowed.has(fam) ? fam : fam ? "OTHER" : "UNKNOWN";
}

function sourceLedgerLine(row) {
  const parts = [
    row.sourceAccession || "",
    row.sourceFiling || "",
    row.sourceUrl || "",
    row.firstDiscoveredAt || row.discoveredAt || "",
  ];
  return parts.join(" | ");
}

function buildInventoryFields(row, cohort) {
  const M = MAP_DISCOVERY_INVENTORY;
  const propertyId = row.propertyId || row.discoveryId;
  const normalized = normalizeHotelDisplayName(row.hotelName || row.rawHotelName || "");
  const aliases = new Set();
  if (row.rawHotelName && row.rawHotelName !== normalized) aliases.add(row.rawHotelName);
  if (row.hotelName && row.hotelName !== normalized) aliases.add(row.hotelName);
  for (const a of row.registrantAliases || []) {
    /* registrant is not a hotel alias — skip */
  }
  const notes = [];
  if (row.locationSuspicion?.suspicious) {
    notes.push(`LOCATION_SUSPICION: ${(row.locationSuspicion.issues || []).join("; ")}`);
  }
  if (/ltd\.|l\.p\.|llc/i.test(String(row.city || ""))) {
    notes.push("CITY_MAY_INCLUDE_LEGAL_SUFFIX_BLEED — research address before HPC match");
  }
  if (cohort === "PROBABLE") {
    notes.push("RESEARCH_ONLY — PROBABLE_PROPERTY; not promoted to verified intelligence");
  }

  const lifecycle =
    cohort === "VERIFIED" ? "IDENTITY_VERIFIED" : "DISCOVERY_PERSISTED";
  const persistence = cohort === "VERIFIED" ? "DISCOVERY_PERSISTED" : "RESEARCH_ONLY";

  const fields = {
    [M.discoveryPropertyId]: propertyId,
    [M.hotelNameNormalized]: normalized,
    [M.hotelNameRaw]: row.rawHotelName || row.hotelName || normalized,
    [M.historicalAliases]: [...aliases].join("\n") || null,
    [M.city]: row.city || null,
    [M.stateRegion]: row.stateRegion || null,
    [M.country]: row.country || "United States",
    [M.address]: row.address || null,
    [M.brandFamily]: brandFamilyOption(row),
    [M.brandLabel]: row.historicalBrand || row.brandHint || null,
    [M.historicalBrand]: row.historicalBrand || null,
    [M.currentBrand]: null,
    [M.identityClass]: row.identityClass,
    [M.evidenceConfidence]: row.evidenceConfidence || "MEDIUM",
    [M.secRegistrant]: row.registrant || null,
    [M.registrantCik]: row.cik || null,
    [M.sourceFilingUrl]: row.sourceUrl || null,
    [M.sourceDocumentLedger]: sourceLedgerLine(row),
    [M.sourceAccessionPrimary]: row.sourceAccession || null,
    [M.discoveryMethod]: row.discoveryMethod || null,
    [M.hpcMatchStatus]:
      row.hpcMatchStatus === "DISCOVERED_MATCHED_HPC" || row.hpcRecordId
        ? "MATCHED"
        : "DISCOVERED_NOT_IN_HPC",
    [M.lifecycleStatus]: lifecycle,
    [M.persistenceStatus]: persistence,
    [M.propertyKey]: row.propertyKey || null,
    [M.notesQuarantine]: notes.length ? notes.join("\n") : null,
    [M.schemaVersion]: DISCOVERY_SCHEMA_VERSION,
    [M.publicationState]: "CUSTOMER_BLOCKED",
  };

  if (row.firstDiscoveredAt || row.discoveredAt) {
    fields[M.firstDiscoveredAt] = row.firstDiscoveredAt || row.discoveredAt;
  }
  if (row.lastResearchedAt) {
    fields[M.lastResearchedAt] = row.lastResearchedAt;
  }
  // Filing date only when ISO-like
  if (row.filingDate && /^\d{4}-\d{2}-\d{2}/.test(row.filingDate)) {
    fields[M.filingDate] = row.filingDate.slice(0, 10);
  }
  if (row.hpcRecordId) {
    fields[M.hpcHotel] = [row.hpcRecordId];
    fields[M.lifecycleStatus] = "HPC_MATCHED";
  }

  // Drop nulls
  for (const k of Object.keys(fields)) {
    if (fields[k] == null || fields[k] === "") delete fields[k];
  }
  return fields;
}

function findingId(propertyId, finding, idx) {
  const raw = `${propertyId}|${finding.evidenceFieldName || finding.agreementType || "f"}|${finding.primaryDate || ""}|${idx}`;
  return `find_${crypto.createHash("sha1").update(raw).digest("hex").slice(0, 16)}`;
}

function buildFindingFields(propertyId, discoveryRecordId, finding, row, idx) {
  const M = MAP_DISCOVERY_FINDINGS;
  const fields = {
    [M.findingId]: findingId(propertyId, finding, idx),
    [M.discoveryProperty]: discoveryRecordId ? [discoveryRecordId] : undefined,
    [M.findingType]: finding.agreementType ? "CONTRACT_MILESTONE" : "OTHER",
    [M.fieldName]: finding.evidenceFieldName || "Contract Disclosure",
    [M.valueObserved]: String(finding.primaryDate || finding.initialExpiration || finding.snippet || "").slice(0, 2000),
    [M.agreementType]: finding.agreementType || "UNKNOWN_AGREEMENT",
    [M.historicalApplicability]: finding.historicalApplicability || "REQUIRES_FURTHER_RESEARCH",
    [M.sourceUrl]: finding.source?.documentUrl || row.sourceUrl || null,
    [M.secAccession]: finding.source?.accessionNumber || row.sourceAccession || null,
    [M.registrantCik]: finding.source?.cik || row.cik || null,
    [M.evidenceSnippet]: String(finding.snippet || "").slice(0, 4000) || null,
    [M.confidence]: "MEDIUM",
    [M.verificationStatus]:
      String(finding.verificationClass || "").startsWith("VERIFIED")
        ? "HISTORICALLY_VERIFIED"
        : "EVIDENCE_COLLECTED",
    [M.hpcPromotionStatus]: row.hpcRecordId ? "ELIGIBLE" : "BLOCKED_NO_HPC_MATCH",
    [M.schemaVersion]: DISCOVERY_SCHEMA_VERSION,
    [M.notes]: "Migrated from filesystem quality inventory — not customer-published",
  };
  if (finding.primaryDate && /^\d{4}-\d{2}-\d{2}/.test(finding.primaryDate)) {
    fields[M.effectiveDate] = finding.primaryDate.slice(0, 10);
  } else if (finding.initialExpirationYear) {
    fields[M.effectiveDate] = `${finding.initialExpirationYear}-01-01`;
  }
  for (const k of Object.keys(fields)) {
    if (fields[k] == null || fields[k] === "") delete fields[k];
  }
  return fields;
}

function dedupeByPropertyId(rows) {
  const map = new Map();
  const duplicates = [];
  for (const r of rows) {
    const id = r.propertyId || r.discoveryId;
    if (!id) continue;
    if (map.has(id)) {
      duplicates.push({ propertyId: id, hotelName: r.hotelName });
      // Merge source ledger into kept row
      const kept = map.get(id);
      kept._extraSources = kept._extraSources || [];
      kept._extraSources.push(sourceLedgerLine(r));
      continue;
    }
    map.set(id, { ...r });
  }
  return { unique: [...map.values()], duplicatesPrevented: duplicates.length, duplicates };
}

async function listExistingByPropertyId(baseId, tableId, token) {
  const byId = new Map();
  let offset;
  do {
    const qs = new URLSearchParams({
      pageSize: "100",
      "fields[]": MAP_DISCOVERY_INVENTORY.discoveryPropertyId,
    });
    if (offset) qs.set("offset", offset);
    const { res, json } = await atFetch(baseId, tableId, token, {
      pathSuffix: `?${qs}`,
    });
    if (!res.ok) throw new Error(`List inventory failed: ${JSON.stringify(json)}`);
    for (const rec of json.records || []) {
      const pid = rec.fields?.[MAP_DISCOVERY_INVENTORY.discoveryPropertyId];
      if (pid) byId.set(pid, rec.id);
    }
    offset = json.offset;
    await sleep(220);
  } while (offset);
  return byId;
}

async function createBatch(baseId, tableId, token, records) {
  const out = [];
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10);
    const { res, json } = await atFetch(baseId, tableId, token, {
      method: "POST",
      body: { records: chunk.map((fields) => ({ fields })), typecast: true },
    });
    if (!res.ok) throw new Error(`Create batch failed: ${JSON.stringify(json)}`);
    out.push(...(json.records || []));
    await sleep(250);
  }
  return out;
}

async function readBack(baseId, tableId, token, recordId) {
  const { res, json } = await atFetch(baseId, tableId, token, {
    pathSuffix: `/${recordId}`,
  });
  if (!res.ok) return { ok: false, error: json };
  return { ok: true, id: json.id, fields: json.fields };
}

function reconcileHpcDryRun(rows) {
  // Non-destructive: report existing hpcRecordId links only — no HPC field writes
  const matched = rows.filter((r) => r.hpcRecordId);
  return {
    mode: "DRY_RUN_NO_HPC_MUTATION",
    matchedCount: matched.length,
    matched: matched.map((r) => ({
      propertyId: r.propertyId,
      hotelName: r.hotelName,
      hpcRecordId: r.hpcRecordId,
      note: "Link only when identity verified; do not convert historical ownership to current",
    })),
    enrichmentPlan: matched.map((r) => ({
      propertyId: r.propertyId,
      eligibleFindings: (r.contractFindings || []).length,
      hpcFieldsEnriched: [],
      blocked: "Customer publication blocked; Graph Evidence promotion requires separate apply",
    })),
  };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!token) throw new Error("Set AIRTABLE_API_KEY");

  const gate = assertCanonicalDiscoveryBase(GRAPH_BASE_ID_CANONICAL);
  if (!gate.ok) throw new Error(JSON.stringify(gate));

  if (!fs.existsSync(IDS_PATH)) {
    throw new Error(`Missing table ids — run ensure --apply first: ${IDS_PATH}`);
  }
  const ids = JSON.parse(fs.readFileSync(IDS_PATH, "utf8"));
  const invTableId = ids.tableIds?.[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
  const findTableId = ids.tableIds?.[TABLE_HI_DISCOVERY_FINDINGS];
  if (!invTableId || !findTableId) throw new Error("Table ids incomplete in generated registry");

  const inventory = JSON.parse(fs.readFileSync(INVENTORY_PATH, "utf8"));
  const verifiedRaw = inventory.rows || [];
  const probableRaw = inventory.probableRows || [];

  const verifiedDeduped = dedupeByPropertyId(verifiedRaw);
  const probableDeduped = dedupeByPropertyId(probableRaw);

  const existing = DRY
    ? new Map()
    : await listExistingByPropertyId(GRAPH_BASE_ID_CANONICAL, invTableId, token);

  const report = {
    contract: DISCOVERY_INVENTORY_CONTRACT_V23,
    schemaVersion: DISCOVERY_SCHEMA_VERSION,
    generatedAt: new Date().toISOString(),
    mode: DRY ? "DRY_RUN" : "APPLY",
    baseId: GRAPH_BASE_ID_CANONICAL,
    source: INVENTORY_PATH,
    cohorts: {
      A_verified: {
        input: verifiedRaw.length,
        uniqueAfterDedupe: verifiedDeduped.unique.length,
        duplicatesPrevented: verifiedDeduped.duplicatesPrevented,
      },
      B_probable: {
        input: probableRaw.length,
        uniqueAfterDedupe: probableDeduped.unique.length,
        duplicatesPrevented: probableDeduped.duplicatesPrevented,
      },
      retainedInFilesystemArchive: {
        ambiguous: 1124,
        multiHotel: 21,
        rejectedArtifacts: 378,
        note: "Not imported as properties; raw research archive retained",
      },
    },
    importedVerified: [],
    importedProbable: [],
    skippedExisting: [],
    findingsPersisted: [],
    readBacks: [],
    quarantinedNotes: [],
    hpcReconciliation: reconcileHpcDryRun([...verifiedDeduped.unique, ...probableDeduped.unique]),
    researchSpendUsd: 0,
  };

  async function migrateCohort(rows, cohort) {
    for (const row of rows) {
      const propertyId = row.propertyId || row.discoveryId;
      const fields = buildInventoryFields(row, cohort);
      if (row._extraSources?.length) {
        fields[MAP_DISCOVERY_INVENTORY.sourceDocumentLedger] = [
          fields[MAP_DISCOVERY_INVENTORY.sourceDocumentLedger],
          ...row._extraSources,
        ]
          .filter(Boolean)
          .join("\n");
      }
      if (fields[MAP_DISCOVERY_INVENTORY.notesQuarantine]) {
        report.quarantinedNotes.push({
          propertyId,
          notes: fields[MAP_DISCOVERY_INVENTORY.notesQuarantine],
        });
      }

      if (existing.has(propertyId)) {
        report.skippedExisting.push({ propertyId, cohort, airtableId: existing.get(propertyId) });
        continue;
      }

      if (DRY) {
        const entry = { propertyId, cohort, fieldsPreview: fields, wouldCreate: true };
        if (cohort === "VERIFIED") report.importedVerified.push(entry);
        else report.importedProbable.push(entry);
        for (let i = 0; i < (row.contractFindings || []).length; i++) {
          report.findingsPersisted.push({
            wouldCreate: true,
            propertyId,
            finding: buildFindingFields(propertyId, null, row.contractFindings[i], row, i),
          });
        }
        continue;
      }

      const created = await createBatch(GRAPH_BASE_ID_CANONICAL, invTableId, token, [fields]);
      const rec = created[0];
      if (!rec) throw new Error(`Create failed for ${propertyId}`);
      existing.set(propertyId, rec.id);

      const rb = await readBack(GRAPH_BASE_ID_CANONICAL, invTableId, token, rec.id);
      report.readBacks.push({
        kind: "inventory",
        propertyId,
        airtableId: rec.id,
        ok: rb.ok,
        identityClass: rb.fields?.[MAP_DISCOVERY_INVENTORY.identityClass],
        lifecycle: rb.fields?.[MAP_DISCOVERY_INVENTORY.lifecycleStatus],
      });
      if (!rb.ok) throw new Error(`Read-back failed for ${propertyId}`);

      const entry = {
        propertyId,
        cohort,
        airtableId: rec.id,
        hotelName: fields[MAP_DISCOVERY_INVENTORY.hotelNameNormalized],
        identityClass: fields[MAP_DISCOVERY_INVENTORY.identityClass],
        lifecycleStatus: fields[MAP_DISCOVERY_INVENTORY.lifecycleStatus],
      };
      if (cohort === "VERIFIED") report.importedVerified.push(entry);
      else report.importedProbable.push(entry);

      for (let i = 0; i < (row.contractFindings || []).length; i++) {
        const fFields = buildFindingFields(propertyId, rec.id, row.contractFindings[i], row, i);
        const fCreated = await createBatch(GRAPH_BASE_ID_CANONICAL, findTableId, token, [fFields]);
        const fRec = fCreated[0];
        const fRb = await readBack(GRAPH_BASE_ID_CANONICAL, findTableId, token, fRec.id);
        report.findingsPersisted.push({
          propertyId,
          findingId: fFields[MAP_DISCOVERY_FINDINGS.findingId],
          airtableId: fRec.id,
          readBackOk: fRb.ok,
        });
        report.readBacks.push({ kind: "finding", propertyId, airtableId: fRec.id, ok: fRb.ok });
      }
    }
  }

  await migrateCohort(verifiedDeduped.unique, "VERIFIED");
  await migrateCohort(probableDeduped.unique, "PROBABLE");

  report.summary = {
    verifiedImported: report.importedVerified.length,
    probableImported: report.importedProbable.length,
    skippedExisting: report.skippedExisting.length,
    findingsPersisted: report.findingsPersisted.length,
    readBacksOk: report.readBacks.filter((r) => r.ok).length,
    readBacksFailed: report.readBacks.filter((r) => r.ok === false).length,
    hpcMatchesLinked: report.hpcReconciliation.matchedCount,
    hpcFieldsEnriched: 0,
    duplicatesPrevented:
      verifiedDeduped.duplicatesPrevented + probableDeduped.duplicatesPrevented,
    researchSpendUsd: 0,
  };

  const outDir = path.join(ROOT, "reports", "sec-edgar-v23-discovery-inventory");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, DRY ? "MIGRATION_DRY_RUN.json" : "MIGRATION_APPLY.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, outPath, summary: report.summary }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
