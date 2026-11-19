#!/usr/bin/env node
/**
 * Synthetic HEI × 3 pilot HPC hotels — native link demo (not verified research).
 *
 * Hotels: Wyndham San Diego Bayside, Renaissance NYTS, Westin Tampa Waterside
 * Org: synthetic HEI Hotel & Resorts (INTERNAL_ONLY)
 *
 *   node scripts/demo-he-intel-001-p2-hpc-link-synthetic-v1.mjs --dry-run
 *   node scripts/demo-he-intel-001-p2-hpc-link-synthetic-v1.mjs --apply
 *
 * Second --apply must not duplicate org/relationship rows (dedupe keys).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { generateEntityId, generateRelationshipId } from "../lib/hotel-intelligence/ownership/ids.js";
import {
  GRAPH_BASE_ID_CANONICAL,
  GRAPH_SCHEMA_VERSION,
  HI_GRAPH_TABLES,
  MAP_HI_ORGANIZATION,
  MAP_HI_HOTEL_ORG_RELATIONSHIP,
  HPC_TABLE_ID,
  HPC_RECIPROCAL_RELATIONSHIP_FIELD,
  assertCanonicalGraphBase,
  resolveGraphBaseId,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";
import { orgDedupeKey, relationshipDedupeKey } from "../lib/hotel-intelligence/research-engine-v1/entity-keys.js";
import {
  validateHotelOrgRelationshipRefs,
  buildRelationshipAirtableFields,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-referential-integrity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;

const PILOT = Object.freeze([
  { hpcId: "recnPB3Q02NO8Ma1o", name: "Wyndham San Diego Bayside" },
  { hpcId: "recG66DQJKP2c0UNh", name: "Renaissance New York Times Square Hotel" },
  { hpcId: "recdmo0CnhIZAwHGC", name: "The Westin Tampa Waterside" },
]);

const SYNTHETIC_ORG = Object.freeze({
  legal_name: "HEI Hotel & Resorts (Synthetic P2 Link Demo)",
  jurisdiction: "US-MD",
  organization_type: "Operator",
  note: "SYNTHETIC_P2_LINK_DEMO — not verified research",
});

function loadRegistry() {
  const p = path.join(
    ROOT,
    "lib/hotel-intelligence/research-engine-v1/airtable-graph-table-ids.generated.json"
  );
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

async function atFetch(baseId, token, tableIdOrPath, init = {}) {
  const url = tableIdOrPath.startsWith("http")
    ? tableIdOrPath
    : `https://api.airtable.com/v0/${baseId}/${tableIdOrPath}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

async function findByDedupe(baseId, token, tableId, fieldName, dedupeKey) {
  const formula = `{${fieldName}}='${String(dedupeKey).replace(/'/g, "\\'")}'`;
  const q = `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=3`;
  const { res, json } = await atFetch(baseId, token, `${tableId}${q}`);
  if (!res.ok) throw new Error(`findByDedupe failed: ${JSON.stringify(json)}`);
  return json.records || [];
}

async function getHpc(baseId, token, hpcId) {
  const { res, json } = await atFetch(baseId, token, `${HPC_TABLE_ID}/${hpcId}`);
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`HPC get failed: ${JSON.stringify(json)}`);
  return json;
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!token) {
    console.error("Missing AIRTABLE_API_KEY");
    process.exit(2);
  }
  const baseId = resolveGraphBaseId(process.env);
  const gate = assertCanonicalGraphBase(baseId);
  if (!gate.ok) {
    console.error("STOP base gate", gate);
    process.exit(3);
  }
  const registry = loadRegistry();
  if (registry.baseId !== GRAPH_BASE_ID_CANONICAL) {
    console.error("STOP registry base mismatch", registry.baseId);
    process.exit(3);
  }
  const orgTableId = registry.tableIds[HI_GRAPH_TABLES.organizations];
  const relTableId = registry.tableIds[HI_GRAPH_TABLES.hotelOrgRelationships];
  if (!orgTableId || !relTableId) {
    console.error("STOP missing table ids");
    process.exit(3);
  }

  const orgDedupe = orgDedupeKey({
    legalName: SYNTHETIC_ORG.legal_name,
    jurisdiction: SYNTHETIC_ORG.jurisdiction,
  });
  const report = {
    at: new Date().toISOString(),
    mode: DRY ? "DRY_RUN" : "APPLY",
    baseId,
    synthetic: true,
    research_claim: false,
    note: SYNTHETIC_ORG.note,
    orgDedupe,
    hotels: [],
    organization: null,
    relationships: [],
    secondPassSafe: null,
  };

  // Resolve HPC records
  for (const hotel of PILOT) {
    const rec = await getHpc(baseId, token, hotel.hpcId);
    report.hotels.push({
      ...hotel,
      exists: Boolean(rec),
      airtableId: rec?.id || null,
    });
    if (!rec) {
      console.error("STOP HPC missing", hotel);
      process.exit(3);
    }
  }

  // Org upsert (idempotent)
  const existingOrgs = await findByDedupe(
    baseId,
    token,
    orgTableId,
    MAP_HI_ORGANIZATION.dedupeKey,
    orgDedupe
  );
  let orgAirtableRecId = existingOrgs[0]?.id || null;
  let orgCanonicalId = existingOrgs[0]?.fields?.[MAP_HI_ORGANIZATION.orgId] || null;
  if (!orgCanonicalId) orgCanonicalId = generateEntityId();

  const orgFields = {
    [MAP_HI_ORGANIZATION.orgId]: orgCanonicalId,
    [MAP_HI_ORGANIZATION.legalName]: SYNTHETIC_ORG.legal_name,
    [MAP_HI_ORGANIZATION.organizationType]: SYNTHETIC_ORG.organization_type,
    [MAP_HI_ORGANIZATION.jurisdiction]: SYNTHETIC_ORG.jurisdiction,
    [MAP_HI_ORGANIZATION.dedupeKey]: orgDedupe,
    [MAP_HI_ORGANIZATION.verificationState]: "Unverified",
    [MAP_HI_ORGANIZATION.confidence]: "UNVERIFIED",
    [MAP_HI_ORGANIZATION.publicationState]: "INTERNAL_ONLY",
    [MAP_HI_ORGANIZATION.schemaVersion]: GRAPH_SCHEMA_VERSION,
    [MAP_HI_ORGANIZATION.notes]: SYNTHETIC_ORG.note,
  };

  if (DRY) {
    report.organization = {
      op: orgAirtableRecId ? "WOULD_UPDATE" : "WOULD_CREATE",
      orgCanonicalId,
      orgAirtableRecId,
      dedupe: orgDedupe,
    };
  } else if (orgAirtableRecId) {
    const { res, json } = await atFetch(baseId, token, `${orgTableId}/${orgAirtableRecId}`, {
      method: "PATCH",
      body: JSON.stringify({ fields: orgFields }),
    });
    if (!res.ok) throw new Error(`org update failed ${JSON.stringify(json)}`);
    report.organization = { op: "UPDATE", id: json.id, orgCanonicalId, dedupe: orgDedupe };
  } else {
    const { res, json } = await atFetch(baseId, token, orgTableId, {
      method: "POST",
      body: JSON.stringify({ fields: orgFields }),
    });
    if (!res.ok) throw new Error(`org create failed ${JSON.stringify(json)}`);
    orgAirtableRecId = json.id;
    report.organization = { op: "CREATE", id: json.id, orgCanonicalId, dedupe: orgDedupe };
  }

  // Relationships
  for (const hotel of PILOT) {
    const dedupe = relationshipDedupeKey({
      hpcId: hotel.hpcId,
      relationshipType: "Operator",
      orgDedupe,
    });
    const existingRels = await findByDedupe(
      baseId,
      token,
      relTableId,
      MAP_HI_HOTEL_ORG_RELATIONSHIP.dedupeKey,
      dedupe
    );
    const integrity = validateHotelOrgRelationshipRefs(
      {
        hpcHotelId: hotel.hpcId,
        hpcHotelLinkIds: [hotel.hpcId],
        organizationId: orgCanonicalId,
        organizationLinkIds: orgAirtableRecId ? [orgAirtableRecId] : [],
      },
      {
        hpcRecord: { id: hotel.hpcId },
        orgRecord: orgAirtableRecId ? { id: orgAirtableRecId } : null,
        orgAirtableRecId,
      }
    );
    if (!integrity.ok && !DRY) {
      report.relationships.push({ hotel, dedupe, integrity, status: "HELD" });
      continue;
    }
    const relationshipId =
      existingRels[0]?.fields?.[MAP_HI_HOTEL_ORG_RELATIONSHIP.relationshipId] ||
      generateRelationshipId();
    const built = buildRelationshipAirtableFields({
      relationshipId,
      hpcHotelId: hotel.hpcId,
      organizationCanonicalId: orgCanonicalId,
      organizationAirtableRecId: orgAirtableRecId || "recPLACEHOLDER0000",
      relationshipType: "Operator",
      dedupeKey: dedupe,
      confidence: "UNVERIFIED",
      notes: SYNTHETIC_ORG.note,
      schemaVersion: GRAPH_SCHEMA_VERSION,
    });
    if (DRY) {
      report.relationships.push({
        hotel,
        dedupe,
        integrity,
        op: existingRels[0] ? "WOULD_UPDATE" : "WOULD_CREATE",
        fieldsPreview: built.ok ? built.fields : null,
      });
      continue;
    }
    if (!built.ok) {
      report.relationships.push({ hotel, dedupe, built, status: "BUILD_FAILED" });
      continue;
    }
    // Fix placeholder if dry path used — org must exist for apply
    built.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.organizationLink] = [orgAirtableRecId];

    let record;
    if (existingRels[0]) {
      const { res, json } = await atFetch(baseId, token, `${relTableId}/${existingRels[0].id}`, {
        method: "PATCH",
        body: JSON.stringify({ fields: built.fields }),
      });
      if (!res.ok) throw new Error(`rel update failed ${JSON.stringify(json)}`);
      record = json;
    } else {
      const { res, json } = await atFetch(baseId, token, relTableId, {
        method: "POST",
        body: JSON.stringify({ fields: built.fields }),
      });
      if (!res.ok) throw new Error(`rel create failed ${JSON.stringify(json)}`);
      record = json;
    }

    // Read-after-write
    const hpcLink = record.fields?.[MAP_HI_HOTEL_ORG_RELATIONSHIP.hpcHotelLink] || [];
    const orgLink = record.fields?.[MAP_HI_HOTEL_ORG_RELATIONSHIP.organizationLink] || [];
    const storedId = record.fields?.[MAP_HI_HOTEL_ORG_RELATIONSHIP.hpcHotelId];
    const rawOk =
      hpcLink.length === 1 &&
      hpcLink[0] === hotel.hpcId &&
      storedId === hotel.hpcId &&
      orgLink.length === 1 &&
      orgLink[0] === orgAirtableRecId;

    report.relationships.push({
      hotel,
      dedupe,
      op: existingRels[0] ? "UPDATE" : "CREATE",
      recordId: record.id,
      hpcLink,
      orgLink,
      storedHpcHotelId: storedId,
      read_after_write_ok: rawOk,
    });
  }

  // Idempotency second pass check (counts)
  const orgRows = await findByDedupe(
    baseId,
    token,
    orgTableId,
    MAP_HI_ORGANIZATION.dedupeKey,
    orgDedupe
  );
  const relCounts = [];
  for (const hotel of PILOT) {
    const dedupe = relationshipDedupeKey({
      hpcId: hotel.hpcId,
      relationshipType: "Operator",
      orgDedupe,
    });
    const rows = await findByDedupe(
      baseId,
      token,
      relTableId,
      MAP_HI_HOTEL_ORG_RELATIONSHIP.dedupeKey,
      dedupe
    );
    relCounts.push({ hotel: hotel.hpcId, count: rows.length });
  }
  report.secondPassSafe = {
    orgCount: orgRows.length,
    relCounts,
    ok: orgRows.length <= 1 && relCounts.every((r) => r.count <= 1),
  };

  // Schema field verification
  const meta = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const tables = (await meta.json()).tables || [];
  const relTable = tables.find((t) => t.id === relTableId);
  const hpcTable = tables.find((t) => t.id === HPC_TABLE_ID);
  const hpcHotelField = (relTable?.fields || []).find(
    (f) => f.name === MAP_HI_HOTEL_ORG_RELATIONSHIP.hpcHotelLink
  );
  const recip = (hpcTable?.fields || []).find((f) => f.name === HPC_RECIPROCAL_RELATIONSHIP_FIELD);
  report.schema = {
    hpcHotelField: hpcHotelField
      ? {
          id: hpcHotelField.id,
          name: hpcHotelField.name,
          linkedTableId: hpcHotelField.options?.linkedTableId,
        }
      : null,
    reciprocal: recip
      ? { id: recip.id, name: recip.name, linkedTableId: recip.options?.linkedTableId }
      : null,
  };

  const out = path.join(
    ROOT,
    "reports/hotel-explorer/HE-INTEL-001-P2-hpc-link-synthetic-demo-v1.json"
  );
  fs.writeFileSync(out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify(report, null, 2));
  console.log("Wrote", out);
  if (!report.secondPassSafe.ok && !DRY) process.exit(4);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
