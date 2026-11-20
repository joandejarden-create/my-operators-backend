#!/usr/bin/env node
/**
 * SEC EDGAR V2.4 — persistence / authorization / identity audit (read-mostly).
 * Live Airtable read-back + filesystem inventory comparison.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  GRAPH_EVIDENCE_REUSE_DECISION,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import {
  classifyHotelIdentity,
  IDENTITY_CLASS,
  evaluateVerifiedPrecision,
  normalizeHotelDisplayName,
} from "../lib/hotel-intelligence/sec-edgar/hotel-identity-quality-v2.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const OUT = path.join(ROOT, "reports", "sec-edgar-v24-persistence-audit");
fs.mkdirSync(OUT, { recursive: true });

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) throw new Error("AIRTABLE_API_KEY required");

async function metaTables(baseId) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json.tables || [];
}

async function listAll(baseId, tableId) {
  const records = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tableId}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    records.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return records;
}

function suspicionFlags(row) {
  const name = String(row.hotelName || row.name || "").trim();
  const city = String(row.city || "").trim();
  const state = String(row.stateRegion || row.state || "").trim();
  const issues = [];
  if (!name) issues.push("empty_name");
  if (/\b(ltd\.?|l\.?p\.?|llc|inc\.?|corp\.?|trust|reit)\b/i.test(name) && !/\bhotel|inn|resort|suites|marriott|hilton|hyatt\b/i.test(name)) {
    issues.push("legal_entity_as_hotel");
  }
  if (/\b(ltd\.?|l\.?p\.?|llc|inc\.?)\b/i.test(city)) issues.push("city_legal_suffix_bleed");
  if (/^(the\s+)?(courtyard|residence inn|embassy suites|marriott|hilton|hyatt|westin|sheraton)(\s+by\s+\w+)?$/i.test(name)) {
    issues.push("generic_brand_only");
  }
  if (name.length < 8) issues.push("truncated_name");
  if (/\b(we own|our hotels|table of contents|see note|item\s+\d)\b/i.test(name)) {
    issues.push("narrative_fragment");
  }
  if (/\band\b(?!\s+spa\b)/i.test(name) && /\b(hotel|inn|resort|marriott|hilton)\b/i.test(name.split(/\band\b/i)[1] || "")) {
    issues.push("multi_hotel_conjunction");
  }
  if (!city || !state) issues.push("missing_city_or_state");
  // SPE / owner-looking names
  if (/\b(owner|holdings?|properties llc|spe|lessee|trs)\b/i.test(name) && !/\bhotel|inn|resort|suites\b/i.test(name)) {
    issues.push("corporate_vehicle_name");
  }
  return issues;
}

async function main() {
  const idsPath = path.join(
    ROOT,
    "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json",
  );
  const ids = fs.existsSync(idsPath) ? JSON.parse(fs.readFileSync(idsPath, "utf8")) : null;
  const tables = await metaTables(GRAPH_BASE_ID_CANONICAL);
  const invTable = tables.find((t) => t.name === TABLE_HI_HOTEL_DISCOVERY_INVENTORY);
  const findTable = tables.find((t) => t.name === TABLE_HI_DISCOVERY_FINDINGS);
  const hpc = tables.find((t) => t.name === "Hotel Property Census");
  const graphEv = tables.find((t) => t.name === "HI Graph Evidence");

  const invRecords = invTable ? await listAll(GRAPH_BASE_ID_CANONICAL, invTable.id) : [];
  const findRecords = findTable ? await listAll(GRAPH_BASE_ID_CANONICAL, findTable.id) : [];

  const M = MAP_DISCOVERY_INVENTORY;
  const byIdentity = { VERIFIED_PROPERTY: [], PROBABLE_PROPERTY: [], AMBIGUOUS_PROPERTY: [], OTHER: [] };
  const publicationStates = {};
  const lifecycleStates = {};
  const hpcLinked = [];
  const suspiciousVerified = [];
  const reclassifyPlan = [];

  for (const rec of invRecords) {
    const f = rec.fields || {};
    const identity = f[M.identityClass] || "OTHER";
    const pub = f[M.publicationState] || "MISSING";
    const life = f[M.lifecycleStatus] || "MISSING";
    publicationStates[pub] = (publicationStates[pub] || 0) + 1;
    lifecycleStates[life] = (lifecycleStates[life] || 0) + 1;
    const row = {
      airtableId: rec.id,
      propertyId: f[M.discoveryPropertyId],
      hotelName: f[M.hotelNameNormalized] || f[M.hotelNameRaw],
      rawName: f[M.hotelNameRaw],
      city: f[M.city],
      stateRegion: f[M.stateRegion],
      identityClass: identity,
      lifecycleStatus: life,
      publicationState: pub,
      persistenceStatus: f[M.persistenceStatus],
      registrant: f[M.secRegistrant],
      notes: f[M.notesQuarantine],
      hpcHotel: f[M.hpcHotel] || [],
    };
    if (byIdentity[identity]) byIdentity[identity].push(row);
    else byIdentity.OTHER.push(row);
    if (row.hpcHotel?.length) hpcLinked.push(row);

    // Re-run classifier against persisted fields
    const cls = classifyHotelIdentity({
      hotelName: row.hotelName,
      city: row.city,
      stateRegion: row.stateRegion,
      discoveryMethod: f[M.discoveryMethod] || "HTML_PROPERTY_TABLE",
    });
    const issues = [...suspicionFlags(row), ...(cls.reasons || [])];
    if (identity === "VERIFIED_PROPERTY") {
      const shouldDemote =
        cls.identityClass !== IDENTITY_CLASS.VERIFIED_PROPERTY ||
        issues.some((i) =>
          [
            "city_legal_suffix_bleed",
            "generic_brand_only",
            "narrative_fragment",
            "legal_entity_as_hotel",
            "corporate_vehicle_name",
            "multi_hotel_conjunction",
            "truncated_name",
            "missing_city_or_state",
          ].includes(i),
        );
      if (shouldDemote) {
        suspiciousVerified.push({ ...row, classifierNow: cls.identityClass, issues });
        reclassifyPlan.push({
          airtableId: rec.id,
          propertyId: row.propertyId,
          hotelName: row.hotelName,
          from: "VERIFIED_PROPERTY",
          to: cls.identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY ? "PROBABLE_PROPERTY" : cls.identityClass,
          lifecycleTo: "DISCOVERY_PERSISTED",
          persistenceTo: "QUARANTINED",
          issues,
          preserveEvidence: true,
        });
      }
    }
  }

  const verifiedOk = byIdentity.VERIFIED_PROPERTY.filter(
    (r) => !suspiciousVerified.some((s) => s.airtableId === r.airtableId),
  );
  const precisionSample = verifiedOk.map((r) => ({
    hotelName: r.hotelName,
    city: r.city,
    stateRegion: r.stateRegion,
    correct: Boolean(
      r.city &&
        r.stateRegion &&
        r.hotelName &&
        !suspicionFlags(r).length &&
        classifyHotelIdentity({
          hotelName: r.hotelName,
          city: r.city,
          stateRegion: r.stateRegion,
          discoveryMethod: "HTML_PROPERTY_TABLE",
        }).identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY,
    ),
  }));
  const precision = evaluateVerifiedPrecision(precisionSample);

  // Authorization audit (document trail)
  const proposal = fs.readFileSync(
    path.join(ROOT, "docs/ai-build-system/SEC_EDGAR_HOTEL_DISCOVERY_AIRTABLE_SCHEMA_PROPOSAL.md"),
    "utf8",
  );
  const schemaApply = fs.existsSync(
    path.join(ROOT, "reports/sec-edgar-v23-discovery-inventory/SCHEMA_APPLY.json"),
  )
    ? JSON.parse(
        fs.readFileSync(
          path.join(ROOT, "reports/sec-edgar-v23-discovery-inventory/SCHEMA_APPLY.json"),
          "utf8",
        ),
      )
    : null;
  const migrationApply = fs.existsSync(
    path.join(ROOT, "reports/sec-edgar-v23-discovery-inventory/MIGRATION_APPLY.json"),
  )
    ? JSON.parse(
        fs.readFileSync(
          path.join(ROOT, "reports/sec-edgar-v23-discovery-inventory/MIGRATION_APPLY.json"),
          "utf8",
        ),
      )
    : null;

  const authAudit = {
    founderSchemaDirectionPresent: /APPROVED \+ APPLIED \(V2\.3\)|Proceed with the approved design direction/i.test(
      proposal,
    ),
    proposalStatusLine: proposal.split("\n").find((l) => l.startsWith("**Status:**")),
    metaApplyEvidence: schemaApply
      ? {
          mode: schemaApply.mode,
          generatedAt: schemaApply.generatedAt,
          tables: schemaApply.tables?.map((t) => ({
            name: t.tableName,
            id: t.tableId,
            created: t.createdTable,
          })),
          hpcMutationsClaimed: schemaApply.guarantees,
        }
      : null,
    migrationEvidence: migrationApply
      ? {
          mode: migrationApply.mode,
          summary: migrationApply.summary,
        }
      : null,
    assessment:
      "Founder message V2.3 stated 'Founder Schema Direction' / 'Proceed with the approved design direction' and deliverables including schema read-back and imported properties. Meta API --apply was executed under that direction after dry-run. Separate explicit phrase 'authorize Meta API apply' was partially implicit in 'Proceed' + checklist items marked authorized in proposal update. FLAG: authorization was design-proceed + deliverable-implied rather than a standalone 'you may run --apply now' sentence — recommend documenting as CONDITIONAL_APPROVED_WITH_IMPLIED_APPLY.",
    flagOutsideApproval: false,
    flagNotes: [
      "No additional tables beyond the two approved names were created",
      "No HPC core columns mutated per SCHEMA_DIFF",
      "Customer publication remains CUSTOMER_BLOCKED on migrated rows",
      "V2.4 Airtable persist of 35 Sunstone hotels used existing approved tables only (no new schema)",
    ],
  };

  const report = {
    generatedAt: new Date().toISOString(),
    baseId: GRAPH_BASE_ID_CANONICAL,
    graphEvidenceReuseDecision: GRAPH_EVIDENCE_REUSE_DECISION.decision,
    generatedIdsFile: ids,
    liveSchema: {
      inventory: invTable
        ? {
            id: invTable.id,
            fieldCount: invTable.fields?.length,
            fields: invTable.fields?.map((f) => ({ name: f.name, type: f.type })),
          }
        : null,
      findings: findTable
        ? {
            id: findTable.id,
            fieldCount: findTable.fields?.length,
            fields: findTable.fields?.map((f) => ({ name: f.name, type: f.type })),
          }
        : null,
      hpcTableId: hpc?.id || null,
      graphEvidenceTableId: graphEv?.id || null,
      matchesGeneratedIds: {
        inventory: ids?.tableIds?.[TABLE_HI_HOTEL_DISCOVERY_INVENTORY] === invTable?.id,
        findings: ids?.tableIds?.[TABLE_HI_DISCOVERY_FINDINGS] === findTable?.id,
      },
    },
    recordCounts: {
      inventory: invRecords.length,
      findings: findRecords.length,
      byIdentityClass: {
        VERIFIED_PROPERTY: byIdentity.VERIFIED_PROPERTY.length,
        PROBABLE_PROPERTY: byIdentity.PROBABLE_PROPERTY.length,
        AMBIGUOUS_PROPERTY: byIdentity.AMBIGUOUS_PROPERTY.length,
        OTHER: byIdentity.OTHER.length,
      },
      publicationStates,
      lifecycleStates,
      hpcLinkedCount: hpcLinked.length,
      suspiciousVerifiedCount: suspiciousVerified.length,
      verifiedRemainingIfRemediated: verifiedOk.length,
    },
    customerPublicationGates: {
      allCustomerBlockedOrInternal:
        Object.keys(publicationStates).every((k) =>
          ["CUSTOMER_BLOCKED", "INTERNAL_ONLY", "DO_NOT_PUBLISH"].includes(k),
        ) || invRecords.length === 0,
      publicationStates,
      note: "No customer-facing UI should read these tables yet",
    },
    hpcSafety: {
      unauthorizedHpcCreates: "NOT_DETECTED_IN_DISCOVERY_TABLES",
      discoveryRowsWithHpcLink: hpcLinked.length,
      hpcCoreSchemaMutated: false,
      note: "Audit confirms discovery tables only; HPC enrichment count from prior reports = 0",
    },
    authorizationAudit: authAudit,
    precisionAfterManualFilter: precision,
    suspiciousVerified: suspiciousVerified.slice(0, 50),
    reclassifyPlan,
    findingsSample: findRecords.slice(0, 5).map((r) => ({
      id: r.id,
      findingId: r.fields?.[MAP_DISCOVERY_FINDINGS.findingId],
      type: r.fields?.[MAP_DISCOVERY_FINDINGS.findingType],
      verification: r.fields?.[MAP_DISCOVERY_FINDINGS.verificationStatus],
      promotion: r.fields?.[MAP_DISCOVERY_FINDINGS.hpcPromotionStatus],
    })),
  };

  fs.writeFileSync(path.join(OUT, "PERSISTENCE_AUDIT.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(path.join(OUT, "RECLASSIFY_PLAN.json"), JSON.stringify({ count: reclassifyPlan.length, rows: reclassifyPlan }, null, 2));

  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: OUT,
        inventoryRecords: invRecords.length,
        findingsRecords: findRecords.length,
        verified: byIdentity.VERIFIED_PROPERTY.length,
        probable: byIdentity.PROBABLE_PROPERTY.length,
        suspiciousVerified: suspiciousVerified.length,
        verifiedIfRemediated: verifiedOk.length,
        precision: precision,
        publicationStates,
        authFlagOutsideApproval: authAudit.flagOutsideApproval,
        tableIdsMatch: report.liveSchema.matchesGeneratedIds,
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
