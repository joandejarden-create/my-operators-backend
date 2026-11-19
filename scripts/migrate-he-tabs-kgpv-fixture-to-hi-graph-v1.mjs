#!/usr/bin/env node
/**
 * Migrate verified KGPV golden-demo ownership orgs/relationships into HI Airtable graph.
 * Idempotent; INTERNAL_ONLY; does not overwrite HPC Owner/Operator flat fields.
 *
 *   node scripts/migrate-he-tabs-kgpv-fixture-to-hi-graph-v1.mjs --dry-run
 *   node scripts/migrate-he-tabs-kgpv-fixture-to-hi-graph-v1.mjs --apply
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
const HPC_ID = "recUNycnMwOVFX0hc";
const OUT = path.join(ROOT, "reports/hotel-explorer/HE-TABS-1-5-KGPV-HI-GRAPH-MIGRATE-V1.json");

const ORGS = [
  {
    legal_name: "Grupo Hotelero Santa Fe, S.A.B. de C.V.",
    trading_name: "Grupo Hotelero Santa Fe",
    jurisdiction: "MX",
    organization_type: "Economic Owner",
    official_website: "https://www.gsf.mx/",
    roles: ["Economic Owner", "Operator"],
  },
  {
    legal_name: "Inmobiliaria en Hoteleria Vallarta Santa Fe",
    trading_name: "Inmobiliaria en Hoteleria Vallarta Santa Fe",
    jurisdiction: "MX",
    organization_type: "Legal Owner",
    roles: ["Legal Owner"],
  },
];

function loadRegistry() {
  return JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "lib/hotel-intelligence/research-engine-v1/airtable-graph-table-ids.generated.json"),
      "utf8",
    ),
  );
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

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!token) throw new Error("Missing AIRTABLE_API_KEY");
  const baseId = resolveGraphBaseId(process.env);
  const gate = assertCanonicalGraphBase(baseId);
  if (!gate.ok) throw new Error(`base gate failed ${JSON.stringify(gate)}`);
  const registry = loadRegistry();
  if (registry.baseId !== GRAPH_BASE_ID_CANONICAL) {
    throw new Error(`registry base mismatch ${registry.baseId}`);
  }
  const orgTableId = registry.tableIds[HI_GRAPH_TABLES.organizations];
  const relTableId = registry.tableIds[HI_GRAPH_TABLES.hotelOrgRelationships];

  const { res: hpcRes, json: hpcJson } = await atFetch(baseId, token, `${HPC_TABLE_ID}/${HPC_ID}`);
  if (!hpcRes.ok) throw new Error(`HPC missing ${JSON.stringify(hpcJson)}`);

  const report = {
    at: new Date().toISOString(),
    mode: DRY ? "DRY_RUN" : "APPLY",
    hpcId: HPC_ID,
    hotelName: "Krystal Grand Puerto Vallarta",
    source: "golden-demo KGPV ownership fixture (verified cohort)",
    publication_state: "INTERNAL_ONLY",
    hpc_owner_operator_overwrite: false,
    organizations: [],
    relationships: [],
  };

  const orgRuntime = [];

  for (const org of ORGS) {
    const dedupe = orgDedupeKey({
      legalName: org.legal_name,
      jurisdiction: org.jurisdiction,
    });
    const existing = await findByDedupe(
      baseId,
      token,
      orgTableId,
      MAP_HI_ORGANIZATION.dedupeKey,
      dedupe,
    );
    let orgAirtableRecId = existing[0]?.id || null;
    let orgCanonicalId = existing[0]?.fields?.[MAP_HI_ORGANIZATION.orgId] || generateEntityId();
    const orgFields = {
      [MAP_HI_ORGANIZATION.orgId]: orgCanonicalId,
      [MAP_HI_ORGANIZATION.legalName]: org.legal_name,
      [MAP_HI_ORGANIZATION.tradingName]: org.trading_name,
      [MAP_HI_ORGANIZATION.organizationType]: org.organization_type,
      [MAP_HI_ORGANIZATION.jurisdiction]: org.jurisdiction,
      [MAP_HI_ORGANIZATION.officialWebsite]: org.official_website || undefined,
      [MAP_HI_ORGANIZATION.dedupeKey]: dedupe,
      [MAP_HI_ORGANIZATION.verificationState]: "Validated",
      [MAP_HI_ORGANIZATION.confidence]: "HIGH",
      [MAP_HI_ORGANIZATION.publicationState]: "INTERNAL_ONLY",
      [MAP_HI_ORGANIZATION.provenanceSummary]:
        "Migrated from HE golden-demo KGPV ownership fixture — INTERNAL_ONLY",
      [MAP_HI_ORGANIZATION.schemaVersion]: GRAPH_SCHEMA_VERSION,
      [MAP_HI_ORGANIZATION.notes]: "HE tabs 1–5 Airtable-first migrate (KGPV)",
    };
    Object.keys(orgFields).forEach((k) => {
      if (orgFields[k] === undefined) delete orgFields[k];
    });

    if (DRY) {
      report.organizations.push({
        op: orgAirtableRecId ? "WOULD_UPDATE" : "WOULD_CREATE",
        dedupe,
        orgCanonicalId,
        orgAirtableRecId,
        legal_name: org.legal_name,
      });
    } else if (orgAirtableRecId) {
      const { res, json } = await atFetch(baseId, token, `${orgTableId}/${orgAirtableRecId}`, {
        method: "PATCH",
        body: JSON.stringify({ fields: orgFields }),
      });
      if (!res.ok) throw new Error(`org update failed ${JSON.stringify(json)}`);
      report.organizations.push({
        op: "UPDATE",
        id: json.id,
        dedupe,
        orgCanonicalId,
        legal_name: org.legal_name,
      });
    } else {
      const { res, json } = await atFetch(baseId, token, orgTableId, {
        method: "POST",
        body: JSON.stringify({ fields: orgFields }),
      });
      if (!res.ok) throw new Error(`org create failed ${JSON.stringify(json)}`);
      orgAirtableRecId = json.id;
      report.organizations.push({
        op: "CREATE",
        id: json.id,
        dedupe,
        orgCanonicalId,
        legal_name: org.legal_name,
      });
    }

    orgRuntime.push({
      ...org,
      dedupe,
      orgCanonicalId,
      orgAirtableRecId: orgAirtableRecId || (DRY ? "recPLACEHOLDER0000" : null),
    });
  }

  for (const org of orgRuntime) {
    for (const relationshipType of org.roles) {
      const dedupe = relationshipDedupeKey({
        hpcId: HPC_ID,
        relationshipType,
        orgDedupe: org.dedupe,
      });
      const existingRels = await findByDedupe(
        baseId,
        token,
        relTableId,
        MAP_HI_HOTEL_ORG_RELATIONSHIP.dedupeKey,
        dedupe,
      );
      const integrity = validateHotelOrgRelationshipRefs(
        {
          hpcHotelId: HPC_ID,
          hpcHotelLinkIds: [HPC_ID],
          organizationId: org.orgCanonicalId,
          organizationLinkIds: org.orgAirtableRecId ? [org.orgAirtableRecId] : [],
        },
        {
          hpcRecord: { id: HPC_ID },
          orgRecord: org.orgAirtableRecId ? { id: org.orgAirtableRecId } : null,
          orgAirtableRecId: org.orgAirtableRecId,
          orgCreateApproved: DRY,
        },
      );
      const relationshipId =
        existingRels[0]?.fields?.[MAP_HI_HOTEL_ORG_RELATIONSHIP.relationshipId] ||
        generateRelationshipId();
      const built = buildRelationshipAirtableFields({
        relationshipId,
        hpcHotelId: HPC_ID,
        organizationCanonicalId: org.orgCanonicalId,
        organizationAirtableRecId: org.orgAirtableRecId,
        relationshipType,
        dedupeKey: dedupe,
        confidence: "HIGH",
        notes: "KGPV fixture→HI graph migrate (INTERNAL_ONLY)",
        schemaVersion: GRAPH_SCHEMA_VERSION,
      });

      if (DRY) {
        report.relationships.push({
          op: existingRels[0] ? "WOULD_UPDATE" : "WOULD_CREATE",
          relationshipType,
          dedupe,
          integrity,
          fieldsPreview: built.ok ? built.fields : built,
        });
        continue;
      }
      if (!integrity.ok || !built.ok) {
        report.relationships.push({
          status: "HELD",
          relationshipType,
          dedupe,
          integrity,
          built,
        });
        continue;
      }
      built.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.organizationLink] = [org.orgAirtableRecId];
      built.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.verificationState] = "Validated";
      built.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.publicationState] = "INTERNAL_ONLY";

      if (existingRels[0]) {
        const { res, json } = await atFetch(baseId, token, `${relTableId}/${existingRels[0].id}`, {
          method: "PATCH",
          body: JSON.stringify({ fields: built.fields }),
        });
        if (!res.ok) throw new Error(`rel update failed ${JSON.stringify(json)}`);
        report.relationships.push({
          op: "UPDATE",
          id: json.id,
          relationshipType,
          dedupe,
        });
      } else {
        const { res, json } = await atFetch(baseId, token, relTableId, {
          method: "POST",
          body: JSON.stringify({ fields: built.fields }),
        });
        if (!res.ok) throw new Error(`rel create failed ${JSON.stringify(json)}`);
        report.relationships.push({
          op: "CREATE",
          id: json.id,
          relationshipType,
          dedupe,
        });
      }
    }
  }

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, mode: report.mode, out: OUT, orgs: report.organizations.length, rels: report.relationships.length }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
