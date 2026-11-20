#!/usr/bin/env node
/**
 * HE 5.0 — Bethesda Phase 3 HI graph apply under expanded standing authorization.
 *
 * Allowlisted tables only:
 *   HI Organizations, HI Hotel–Org Relationships, HI Org Contacts, HI Graph Evidence
 *
 * Quarantines:
 *   HI Contact Channels (not on standing allowlist) — LinkedIn / org routes in Notes + evidence
 *   Mullen portfolio holdings on non-Bethesda hotels (Proposed only)
 *   Surfe personal channels
 *
 *   node scripts/he50-beth-phase3-hi-graph-standing-apply-v1.mjs
 *   node scripts/he50-beth-phase3-hi-graph-standing-apply-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { generateEntityId, generateRelationshipId } from "../lib/hotel-intelligence/ownership/ids.js";
import {
  GRAPH_SCHEMA_VERSION,
  HI_GRAPH_TABLES,
  MAP_HI_ORGANIZATION,
  MAP_HI_HOTEL_ORG_RELATIONSHIP,
  MAP_HI_ORG_CONTACT,
  MAP_HI_GRAPH_EVIDENCE,
  HPC_TABLE_ID,
  assertCanonicalGraphBase,
  resolveGraphBaseId,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";
import {
  orgDedupeKey,
  relationshipDedupeKey,
  personDedupeKey,
  evidenceDedupeKey,
} from "../lib/hotel-intelligence/research-engine-v1/entity-keys.js";
import {
  validateHotelOrgRelationshipRefs,
  buildRelationshipAirtableFields,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-referential-integrity.js";
import { HE50_BETHESDA_HPC_ID } from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";
import {
  HE50_STANDING_AUTH_VERSION,
  HE50_ALLOWED_DESTINATION_TABLES,
  HE50_HI_ORGANIZATIONS_TABLE_ID,
  HE50_HI_HOTEL_ORG_RELATIONSHIPS_TABLE_ID,
  HE50_HI_ORG_CONTACTS_TABLE_ID,
  HE50_HI_GRAPH_EVIDENCE_TABLE_ID,
  HE50_PILOT_HOTEL_ALLOWLIST,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/standing-authorization-v1.js";

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
const HPC_ID = HE50_BETHESDA_HPC_ID;
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-explorer/he50-expanded-standing-auth-completion-v1",
);
const OUT = path.join(
  OUT_DIR,
  DRY ? "BETH_PHASE3_HI_GRAPH_DRY_RUN.json" : "BETH_PHASE3_HI_GRAPH_APPLY.json",
);

const ORGS = [
  {
    legal_name: "Marbeth Acquisition LLC",
    trading_name: "Marbeth Acquisition",
    jurisdiction: "US-PA",
    organization_type: "Legal Owner",
    confidence: "HIGH",
    note: "SDAT legal titleholder (PropCo). Standing auth VERIFIED_LEGAL_OWNER.",
  },
  {
    legal_name: "Mullen Real Estate Capital",
    trading_name: "Mullen Real Estate Capital",
    jurisdiction: "US-PA",
    organization_type: "Economic Owner",
    official_website: "https://mullencap.com/",
    confidence: "MEDIUM",
    note: "Economic sponsor via portfolio + HQ mailing match; UBO unresolved. Not Proposed promotion.",
  },
  {
    legal_name: "Mullen Hospitality Management",
    trading_name: "Mullen Hospitality Management",
    jurisdiction: "US-PA",
    organization_type: "Asset Manager",
    official_website: "https://mullenhospitality.com/",
    confidence: "MEDIUM",
    note: "Firm-level asset manager; property-assigned AM contact unresolved.",
  },
];

const RELS = [
  {
    org_legal_name: "Marbeth Acquisition LLC",
    relationship_type: "Legal Owner",
    confidence: "HIGH",
    verification_state: "Validated",
    note: "Maryland SDAT Real Property — Montgomery County acct 03547343",
  },
  {
    org_legal_name: "Mullen Real Estate Capital",
    relationship_type: "Economic Owner",
    confidence: "MEDIUM",
    verification_state: "Validated",
    note: "Sponsor attribution from MREC portfolio + mailing match; UBO unresolved",
  },
  {
    org_legal_name: "Mullen Hospitality Management",
    relationship_type: "Asset Manager",
    confidence: "MEDIUM",
    verification_state: "Validated",
    note: "Firm-level MHM; property-assigned AM unresolved",
  },
];

const CONTACTS = [
  {
    display_name: "Jeff Mullen",
    title: "President and CEO",
    org_legal_name: "Mullen Real Estate Capital",
    role_group: "Ownership / Decision Authority",
    confidence: "HIGH",
    linkedin: "https://www.linkedin.com/in/jeffmmullen/",
  },
  {
    display_name: "Katherine Button",
    title: "VP, Asset Management and Investments",
    org_legal_name: "Mullen Real Estate Capital",
    role_group: "Asset Management",
    confidence: "HIGH",
    linkedin: "https://www.linkedin.com/in/katherine-button/",
  },
  {
    display_name: "Patrick Reece",
    title: "Director, Asset Management",
    org_legal_name: "Mullen Real Estate Capital",
    role_group: "Asset Management",
    confidence: "MEDIUM",
    linkedin: "https://www.linkedin.com/in/patrick-reece-375b8937/",
  },
];

function loadRegistry() {
  return JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "lib/hotel-intelligence/research-engine-v1/airtable-graph-table-ids.generated.json",
      ),
      "utf8",
    ),
  );
}

function loadEvidenceFromPhase3() {
  const p = path.join(
    ROOT,
    "reports/hotel-explorer/phase3-research/HE-BETH-PHASE3-HI-GRAPH-DRY-RUN-V1.json",
  );
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  return j.staged?.evidence || [];
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

async function findEvidenceByUrl(baseId, token, tableId, hpcId, sourceUrl) {
  const formula = `AND({${MAP_HI_GRAPH_EVIDENCE.hpcHotelId}}='${hpcId}',{${MAP_HI_GRAPH_EVIDENCE.sourceUrl}}='${String(sourceUrl).replace(/'/g, "\\'")}')`;
  const q = `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=3`;
  const { res, json } = await atFetch(baseId, token, `${tableId}${q}`);
  if (!res.ok) throw new Error(`findEvidence failed: ${JSON.stringify(json)}`);
  return json.records || [];
}

function assertAllowlisted(tableId) {
  if (!HE50_ALLOWED_DESTINATION_TABLES[tableId]) {
    throw new Error(`Table ${tableId} not on HE50 standing allowlist`);
  }
}

async function main() {
  if (!HE50_PILOT_HOTEL_ALLOWLIST.includes(HPC_ID)) {
    throw new Error(`HPC ${HPC_ID} not on pilot allowlist`);
  }

  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!token) throw new Error("Missing AIRTABLE_API_KEY / AIRTABLE_PAT");
  const baseId = resolveGraphBaseId(process.env);
  const gate = assertCanonicalGraphBase(baseId);
  if (!gate.ok) throw new Error(JSON.stringify(gate));

  const registry = loadRegistry();
  const orgTableId = registry.tableIds[HI_GRAPH_TABLES.organizations];
  const relTableId = registry.tableIds[HI_GRAPH_TABLES.hotelOrgRelationships];
  const contactTableId = registry.tableIds[HI_GRAPH_TABLES.orgContacts];
  const evidenceTableId = registry.tableIds[HI_GRAPH_TABLES.graphEvidence];

  for (const [label, id] of [
    ["orgs", orgTableId],
    ["rels", relTableId],
    ["contacts", contactTableId],
    ["evidence", evidenceTableId],
  ]) {
    if (!id) throw new Error(`Missing registry table id for ${label}`);
  }
  assertAllowlisted(HE50_HI_ORGANIZATIONS_TABLE_ID);
  assertAllowlisted(HE50_HI_HOTEL_ORG_RELATIONSHIPS_TABLE_ID);
  assertAllowlisted(HE50_HI_ORG_CONTACTS_TABLE_ID);
  assertAllowlisted(HE50_HI_GRAPH_EVIDENCE_TABLE_ID);
  if (orgTableId !== HE50_HI_ORGANIZATIONS_TABLE_ID) {
    throw new Error(`Org table mismatch registry=${orgTableId}`);
  }
  if (relTableId !== HE50_HI_HOTEL_ORG_RELATIONSHIPS_TABLE_ID) {
    throw new Error(`Rel table mismatch registry=${relTableId}`);
  }
  if (contactTableId !== HE50_HI_ORG_CONTACTS_TABLE_ID) {
    throw new Error(`Contact table mismatch registry=${contactTableId}`);
  }
  if (evidenceTableId !== HE50_HI_GRAPH_EVIDENCE_TABLE_ID) {
    throw new Error(`Evidence table mismatch registry=${evidenceTableId}`);
  }

  const { res: hpcRes, json: hpcJson } = await atFetch(
    baseId,
    token,
    `${HPC_TABLE_ID}/${HPC_ID}`,
  );
  if (!hpcRes.ok) throw new Error(`HPC missing ${JSON.stringify(hpcJson)}`);

  const report = {
    at: new Date().toISOString(),
    mode: DRY ? "DRY_RUN" : "APPLY",
    standingAuthVersion: HE50_STANDING_AUTH_VERSION,
    hpcId: HPC_ID,
    hotelName: "Bethesda Marriott",
    quarantined: [
      {
        reason: "HI Contact Channels not on standing allowlist",
        disposition: "QUARANTINED — LinkedIn + org routes stored in contact Notes + Graph Evidence",
        items: [
          "info@mullencap.com",
          "7 Campus Blvd., Suite 300, Newtown Square, PA 19073",
          "LinkedIn public profile URLs on three contacts",
        ],
      },
      {
        reason: "Mullen 8-hotel portfolio candidates remain Proposed",
        disposition: "QUARANTINED_PROPOSED_OWNERSHIP — no cross-hotel OWNED_BY writes",
      },
      {
        reason: "Surfe personal channels",
        disposition: "QUARANTINED_SURFE_SESSION_ONLY",
      },
    ],
    organizations: [],
    relationships: [],
    contacts: [],
    evidence: [],
    readback: null,
  };

  /** @type {Map<string, {airtableId:string, canonicalId:string, dedupe:string}>} */
  const orgByLegal = new Map();

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
    let orgCanonicalId =
      existing[0]?.fields?.[MAP_HI_ORGANIZATION.orgId] || generateEntityId();
    const orgFields = {
      [MAP_HI_ORGANIZATION.orgId]: orgCanonicalId,
      [MAP_HI_ORGANIZATION.legalName]: org.legal_name,
      [MAP_HI_ORGANIZATION.tradingName]: org.trading_name,
      [MAP_HI_ORGANIZATION.organizationType]: org.organization_type,
      [MAP_HI_ORGANIZATION.jurisdiction]: org.jurisdiction,
      [MAP_HI_ORGANIZATION.dedupeKey]: dedupe,
      [MAP_HI_ORGANIZATION.verificationState]: "Validated",
      [MAP_HI_ORGANIZATION.confidence]: org.confidence,
      [MAP_HI_ORGANIZATION.publicationState]: "INTERNAL_ONLY",
      [MAP_HI_ORGANIZATION.provenanceSummary]: org.note,
      [MAP_HI_ORGANIZATION.schemaVersion]: GRAPH_SCHEMA_VERSION,
      [MAP_HI_ORGANIZATION.notes]: org.note,
    };
    if (org.official_website) {
      orgFields[MAP_HI_ORGANIZATION.officialWebsite] = org.official_website;
    }

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
      if (!res.ok) throw new Error(`org update ${JSON.stringify(json)}`);
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
      if (!res.ok) throw new Error(`org create ${JSON.stringify(json)}`);
      orgAirtableRecId = json.id;
      report.organizations.push({
        op: "CREATE",
        id: json.id,
        dedupe,
        orgCanonicalId,
        legal_name: org.legal_name,
      });
    }

    orgByLegal.set(org.legal_name, {
      airtableId: orgAirtableRecId || (DRY ? "recPLACEHOLDER0000" : null),
      canonicalId: orgCanonicalId,
      dedupe,
    });
  }

  for (const rel of RELS) {
    const org = orgByLegal.get(rel.org_legal_name);
    if (!org) throw new Error(`Missing org for rel ${rel.org_legal_name}`);
    const relDedupe = relationshipDedupeKey({
      hpcId: HPC_ID,
      relationshipType: rel.relationship_type,
      orgDedupe: org.dedupe,
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
        organizationId: org.canonicalId,
        organizationLinkIds: org.airtableId ? [org.airtableId] : [],
      },
      {
        hpcRecord: { id: HPC_ID },
        orgRecord: org.airtableId ? { id: org.airtableId } : null,
        orgAirtableRecId: org.airtableId,
        orgCreateApproved: DRY,
      },
    );
    const relationshipId =
      existingRels[0]?.fields?.[MAP_HI_HOTEL_ORG_RELATIONSHIP.relationshipId] ||
      generateRelationshipId();
    const built = buildRelationshipAirtableFields({
      relationshipId,
      hpcHotelId: HPC_ID,
      organizationCanonicalId: org.canonicalId,
      organizationAirtableRecId: org.airtableId,
      relationshipType: rel.relationship_type,
      dedupeKey: relDedupe,
      confidence: rel.confidence,
      notes: rel.note,
      schemaVersion: GRAPH_SCHEMA_VERSION,
    });

    if (DRY) {
      report.relationships.push({
        op: existingRels[0] ? "WOULD_UPDATE" : "WOULD_CREATE",
        relationshipType: rel.relationship_type,
        dedupe: relDedupe,
        integrityOk: integrity.ok,
        confidence: rel.confidence,
      });
      continue;
    }
    if (!integrity.ok || !built.ok) {
      report.relationships.push({
        status: "HELD",
        relationshipType: rel.relationship_type,
        integrity,
        built,
      });
      continue;
    }
    built.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.organizationLink] = [org.airtableId];
    built.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.verificationState] = rel.verification_state;
    built.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.publicationState] = "INTERNAL_ONLY";

    if (existingRels[0]) {
      const { res, json } = await atFetch(
        baseId,
        token,
        `${relTableId}/${existingRels[0].id}`,
        { method: "PATCH", body: JSON.stringify({ fields: built.fields }) },
      );
      if (!res.ok) throw new Error(`rel update ${JSON.stringify(json)}`);
      report.relationships.push({
        op: "UPDATE",
        id: json.id,
        relationshipType: rel.relationship_type,
      });
    } else {
      const { res, json } = await atFetch(baseId, token, relTableId, {
        method: "POST",
        body: JSON.stringify({ fields: built.fields }),
      });
      if (!res.ok) throw new Error(`rel create ${JSON.stringify(json)}`);
      report.relationships.push({
        op: "CREATE",
        id: json.id,
        relationshipType: rel.relationship_type,
      });
    }
  }

  for (const person of CONTACTS) {
    const org = orgByLegal.get(person.org_legal_name);
    if (!org) throw new Error(`Missing org for contact ${person.display_name}`);
    const pDedupe = personDedupeKey({
      displayName: person.display_name,
      orgDedupe: org.dedupe,
    });
    const affDedupe = `aff::${pDedupe}::${org.dedupe}`;
    const existing = await findByDedupe(
      baseId,
      token,
      contactTableId,
      MAP_HI_ORG_CONTACT.personDedupeKey,
      pDedupe,
    );
    let personId =
      existing[0]?.fields?.[MAP_HI_ORG_CONTACT.personId] || generateEntityId();
    const fields = {
      [MAP_HI_ORG_CONTACT.personId]: personId,
      [MAP_HI_ORG_CONTACT.displayName]: person.display_name,
      [MAP_HI_ORG_CONTACT.organizationId]: org.canonicalId,
      [MAP_HI_ORG_CONTACT.title]: person.title,
      [MAP_HI_ORG_CONTACT.functionRole]: person.role_group,
      [MAP_HI_ORG_CONTACT.affiliationVerified]: true,
      [MAP_HI_ORG_CONTACT.roleCurrency]: "Current",
      [MAP_HI_ORG_CONTACT.confidence]: person.confidence,
      [MAP_HI_ORG_CONTACT.verificationState]: "Validated",
      [MAP_HI_ORG_CONTACT.personDedupeKey]: pDedupe,
      [MAP_HI_ORG_CONTACT.affiliationDedupeKey]: affDedupe,
      [MAP_HI_ORG_CONTACT.publicationState]: "INTERNAL_ONLY",
      [MAP_HI_ORG_CONTACT.schemaVersion]: GRAPH_SCHEMA_VERSION,
      [MAP_HI_ORG_CONTACT.notes]: [
        `Public LinkedIn (not persisted as HI Contact Channel — allowlist quarantine): ${person.linkedin}`,
        "No Surfe personal email/phone persisted.",
        "Org fallback: info@mullencap.com (evidence only).",
      ].join(" | "),
    };
    if (!DRY && org.airtableId && !String(org.airtableId).startsWith("recPLACEHOLDER")) {
      fields[MAP_HI_ORG_CONTACT.organizationLink] = [org.airtableId];
    }

    if (DRY) {
      report.contacts.push({
        op: existing[0] ? "WOULD_UPDATE" : "WOULD_CREATE",
        display_name: person.display_name,
        dedupe: pDedupe,
        existingId: existing[0]?.id || null,
      });
      continue;
    }

    if (existing[0]) {
      const { res, json } = await atFetch(
        baseId,
        token,
        `${contactTableId}/${existing[0].id}`,
        { method: "PATCH", body: JSON.stringify({ fields }) },
      );
      if (!res.ok) throw new Error(`contact update ${JSON.stringify(json)}`);
      report.contacts.push({
        op: "UPDATE",
        id: json.id,
        display_name: person.display_name,
      });
    } else {
      const { res, json } = await atFetch(baseId, token, contactTableId, {
        method: "POST",
        body: JSON.stringify({ fields }),
      });
      if (!res.ok) throw new Error(`contact create ${JSON.stringify(json)}`);
      report.contacts.push({
        op: "CREATE",
        id: json.id,
        display_name: person.display_name,
      });
    }
  }

  const evidenceRows = loadEvidenceFromPhase3();
  for (const ev of evidenceRows) {
    const url = String(ev.source_url || "").trim();
    if (!url) continue;
    const dedupe = evidenceDedupeKey({ hpcId: HPC_ID, sourceUrl: url });
    const existing = await findEvidenceByUrl(baseId, token, evidenceTableId, HPC_ID, url);
    const evidenceId =
      existing[0]?.fields?.[MAP_HI_GRAPH_EVIDENCE.evidenceId] || generateEntityId();
    const fields = {
      [MAP_HI_GRAPH_EVIDENCE.evidenceId]: evidenceId,
      [MAP_HI_GRAPH_EVIDENCE.hpcHotelId]: HPC_ID,
      [MAP_HI_GRAPH_EVIDENCE.hpcHotelLink]: [HPC_ID],
      [MAP_HI_GRAPH_EVIDENCE.entityType]: ev.entity_type || "Hotel–Org Relationship",
      [MAP_HI_GRAPH_EVIDENCE.fieldName]: ev.field_name || null,
      [MAP_HI_GRAPH_EVIDENCE.valueObserved]: ev.value_observed || null,
      [MAP_HI_GRAPH_EVIDENCE.sourceName]: ev.source_name || null,
      [MAP_HI_GRAPH_EVIDENCE.sourceType]: ev.source_type || null,
      [MAP_HI_GRAPH_EVIDENCE.sourceUrl]: url,
      [MAP_HI_GRAPH_EVIDENCE.retrievedAt]: ev.retrieved_at || null,
      [MAP_HI_GRAPH_EVIDENCE.evidenceSnippet]: (ev.evidence_snippet || "").slice(0, 100000),
      [MAP_HI_GRAPH_EVIDENCE.confidence]: ev.confidence || "MEDIUM",
      [MAP_HI_GRAPH_EVIDENCE.evidenceStrength]: ev.evidence_strength || "MEDIUM",
      [MAP_HI_GRAPH_EVIDENCE.current]: ev.current !== false,
      [MAP_HI_GRAPH_EVIDENCE.notes]: [
        ev.notes || "",
        dedupe ? `dedupe:${dedupe}` : "",
        "he50-standing-auth-phase3-apply-v1",
      ]
        .filter(Boolean)
        .join(" | "),
      [MAP_HI_GRAPH_EVIDENCE.schemaVersion]: GRAPH_SCHEMA_VERSION,
    };

    if (DRY) {
      report.evidence.push({
        op: existing[0] ? "WOULD_SKIP_EXISTING" : "WOULD_CREATE",
        source_url: url,
        existingId: existing[0]?.id || null,
        dedupe,
      });
      continue;
    }
    if (existing[0]) {
      report.evidence.push({
        op: "SKIP_EXISTING",
        id: existing[0].id,
        source_url: url,
      });
      continue;
    }
    const { res, json } = await atFetch(baseId, token, evidenceTableId, {
      method: "POST",
      body: JSON.stringify({ fields }),
    });
    if (!res.ok) throw new Error(`evidence create ${JSON.stringify(json)}`);
    report.evidence.push({ op: "CREATE", id: json.id, source_url: url });
  }

  if (!DRY) {
    const orgIds = [...orgByLegal.values()].map((o) => o.airtableId).filter(Boolean);
    const contactFormula = `OR(${CONTACTS.map((c) => {
      const org = orgByLegal.get(c.org_legal_name);
      const d = personDedupeKey({ displayName: c.display_name, orgDedupe: org.dedupe });
      return `{${MAP_HI_ORG_CONTACT.personDedupeKey}}='${d}'`;
    }).join(",")})`;
    const { json: contactRead } = await atFetch(
      baseId,
      token,
      `${contactTableId}?filterByFormula=${encodeURIComponent(contactFormula)}&maxRecords=10`,
    );
    const { json: relRead } = await atFetch(
      baseId,
      token,
      `${relTableId}?filterByFormula=${encodeURIComponent(`{${MAP_HI_HOTEL_ORG_RELATIONSHIP.hpcHotelId}}='${HPC_ID}'`)}&maxRecords=25`,
    );
    report.readback = {
      orgIds,
      contactsFound: (contactRead.records || []).length,
      relationshipsForHotel: (relRead.records || []).length,
      relationshipTypes: (relRead.records || []).map(
        (r) => r.fields?.[MAP_HI_HOTEL_ORG_RELATIONSHIP.relationshipType],
      ),
    };
  }

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        mode: report.mode,
        out: OUT,
        orgs: report.organizations.length,
        rels: report.relationships.length,
        contacts: report.contacts.length,
        evidence: report.evidence.length,
        quarantined: report.quarantined.length,
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
