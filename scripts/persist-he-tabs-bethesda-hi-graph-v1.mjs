#!/usr/bin/env node
/**
 * Persist Bethesda Marriott operator (+ optional brand parent) into HI Airtable graph.
 * Reuses existing HPC facts — no paid research. INTERNAL_ONLY.
 *
 *   node scripts/persist-he-tabs-bethesda-hi-graph-v1.mjs --dry-run
 *   node scripts/persist-he-tabs-bethesda-hi-graph-v1.mjs --apply
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
const HPC_ID = "recLuxvwwxID7U2B8";
const OUT = path.join(ROOT, "reports/hotel-explorer/HE-TABS-1-5-BETHESDA-HI-GRAPH-PERSIST-V1.json");

const ORGS = [
  {
    legal_name: "Marriott International, Inc.",
    trading_name: "Marriott International",
    jurisdiction: "US-MD",
    organization_type: "Operator",
    official_website: "https://www.marriott.com/",
    roles: ["Operator", "Brand"],
    note: "From HPC Operator / Management Company + Current Brand — operator/brand entity; NOT asserted as economic owner",
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
  if (!gate.ok) throw new Error(JSON.stringify(gate));
  const registry = loadRegistry();
  const orgTableId = registry.tableIds[HI_GRAPH_TABLES.organizations];
  const relTableId = registry.tableIds[HI_GRAPH_TABLES.hotelOrgRelationships];

  const { res: hpcRes, json: hpcJson } = await atFetch(baseId, token, `${HPC_TABLE_ID}/${HPC_ID}`);
  if (!hpcRes.ok) throw new Error(`HPC missing ${JSON.stringify(hpcJson)}`);

  const report = {
    at: new Date().toISOString(),
    mode: DRY ? "DRY_RUN" : "APPLY",
    hpcId: HPC_ID,
    hotelName: "Bethesda Marriott",
    economic_owner: "UNRESOLVED — not invented; requires research",
    organizations: [],
    relationships: [],
    kgpv_blocker:
      "recUNycnMwOVFX0hc is Hotel Census–native (tblgj2qEwxjTcg6q0); HI HPC Hotel link targets tbl9aY5ijiuIzzWam — relationships held until identity unification",
  };

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
      [MAP_HI_ORGANIZATION.officialWebsite]: org.official_website,
      [MAP_HI_ORGANIZATION.dedupeKey]: dedupe,
      [MAP_HI_ORGANIZATION.verificationState]: "Validated",
      [MAP_HI_ORGANIZATION.confidence]: "HIGH",
      [MAP_HI_ORGANIZATION.publicationState]: "INTERNAL_ONLY",
      [MAP_HI_ORGANIZATION.provenanceSummary]:
        "Reuse from HPC Operator/Brand fields + official Marriott site — not economic ownership claim",
      [MAP_HI_ORGANIZATION.schemaVersion]: GRAPH_SCHEMA_VERSION,
      [MAP_HI_ORGANIZATION.notes]: org.note,
    };

    if (DRY) {
      report.organizations.push({
        op: orgAirtableRecId ? "WOULD_UPDATE" : "WOULD_CREATE",
        dedupe,
        orgCanonicalId,
        orgAirtableRecId,
      });
    } else if (orgAirtableRecId) {
      const { res, json } = await atFetch(baseId, token, `${orgTableId}/${orgAirtableRecId}`, {
        method: "PATCH",
        body: JSON.stringify({ fields: orgFields }),
      });
      if (!res.ok) throw new Error(`org update ${JSON.stringify(json)}`);
      report.organizations.push({ op: "UPDATE", id: json.id, dedupe, orgCanonicalId });
    } else {
      const { res, json } = await atFetch(baseId, token, orgTableId, {
        method: "POST",
        body: JSON.stringify({ fields: orgFields }),
      });
      if (!res.ok) throw new Error(`org create ${JSON.stringify(json)}`);
      orgAirtableRecId = json.id;
      report.organizations.push({ op: "CREATE", id: json.id, dedupe, orgCanonicalId });
    }

    const orgAt = orgAirtableRecId || (DRY ? "recPLACEHOLDER0000" : null);

    for (const relationshipType of org.roles) {
      const relDedupe = relationshipDedupeKey({
        hpcId: HPC_ID,
        relationshipType,
        orgDedupe: dedupe,
      });
      const existingRels = await findByDedupe(
        baseId,
        token,
        relTableId,
        MAP_HI_HOTEL_ORG_RELATIONSHIP.dedupeKey,
        relDedupe,
      );
      const integrity = validateHotelOrgRelationshipRefs(
        {
          hpcHotelId: HPC_ID,
          hpcHotelLinkIds: [HPC_ID],
          organizationId: orgCanonicalId,
          organizationLinkIds: orgAt ? [orgAt] : [],
        },
        {
          hpcRecord: { id: HPC_ID },
          orgRecord: orgAt ? { id: orgAt } : null,
          orgAirtableRecId: orgAt,
          orgCreateApproved: DRY,
        },
      );
      const relationshipId =
        existingRels[0]?.fields?.[MAP_HI_HOTEL_ORG_RELATIONSHIP.relationshipId] ||
        generateRelationshipId();
      const built = buildRelationshipAirtableFields({
        relationshipId,
        hpcHotelId: HPC_ID,
        organizationCanonicalId: orgCanonicalId,
        organizationAirtableRecId: orgAt,
        relationshipType,
        dedupeKey: relDedupe,
        confidence: "HIGH",
        notes: org.note,
        schemaVersion: GRAPH_SCHEMA_VERSION,
      });

      if (DRY) {
        report.relationships.push({
          op: existingRels[0] ? "WOULD_UPDATE" : "WOULD_CREATE",
          relationshipType,
          dedupe: relDedupe,
          integrityOk: integrity.ok,
        });
        continue;
      }
      if (!integrity.ok || !built.ok) {
        report.relationships.push({ status: "HELD", relationshipType, integrity, built });
        continue;
      }
      built.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.organizationLink] = [orgAirtableRecId];
      built.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.verificationState] = "Validated";
      built.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.publicationState] = "INTERNAL_ONLY";

      if (existingRels[0]) {
        const { res, json } = await atFetch(baseId, token, `${relTableId}/${existingRels[0].id}`, {
          method: "PATCH",
          body: JSON.stringify({ fields: built.fields }),
        });
        if (!res.ok) throw new Error(`rel update ${JSON.stringify(json)}`);
        report.relationships.push({ op: "UPDATE", id: json.id, relationshipType });
      } else {
        const { res, json } = await atFetch(baseId, token, relTableId, {
          method: "POST",
          body: JSON.stringify({ fields: built.fields }),
        });
        if (!res.ok) throw new Error(`rel create ${JSON.stringify(json)}`);
        report.relationships.push({ op: "CREATE", id: json.id, relationshipType });
      }
    }
  }

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, mode: report.mode, out: OUT, report }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
