/**
 * GDI Inventory Membership Persistence V1
 *
 * Separates canonical opportunity identity from hotel-specific publication
 * membership (STAGED / CURRENT / research-only). Reuses existing GDO payload
 * concepts (inventoryPopulationPrimary) and immutable publication packages.
 *
 * No unauthorized Airtable schema changes or live writes from this module.
 * Dry-run / fixture default.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";

export const GDI_INVENTORY_MEMBERSHIP_PERSISTENCE_V1 =
  "gdi-inventory-membership-persistence-v1";

/** Existing GDI ops base — Group Demand Opportunities (no new base). */
export const GDI_INVENTORY_MEMBERSHIP_AIRTABLE = Object.freeze({
  baseId: "appa2cE7FTRmIbB32",
  tableName: "Group Demand Opportunities",
  tableId: "tblRuReslJMwsfRQj",
});

/**
 * Proposed additive GDO fields (founder schema authorization required before create).
 * Prefer reusing payload JSON already stored on GDO when fields already exist.
 */
export const PROPOSED_GDO_MEMBERSHIP_FIELDS = Object.freeze([
  {
    name: "Hotel Inventory Membership JSON",
    type: "multilineText",
    purpose:
      "Hotel-scoped membership envelope: staged/current versions, status, first/last verification, change class",
  },
  {
    name: "Staged Publication Version",
    type: "singleLineText",
    purpose: "Exact STAGED publicationVersion membership for this hotel opportunity row",
  },
  {
    name: "Current Publication Version",
    type: "singleLineText",
    purpose: "Exact CURRENT publicationVersion membership when founder-approved",
  },
  {
    name: "Inventory Population Primary",
    type: "singleSelect",
    purpose:
      "Hotel-specific commercial population (SALES_OPPORTUNITIES / CMI / WATCHLIST) — may already exist in payload",
    optionsHint: [
      "SALES_OPPORTUNITIES",
      "COMPETITIVE_MARKET_INTELLIGENCE",
      "WATCHLIST",
      "RESEARCH_ONLY",
      "EXCLUDED",
    ],
  },
]);

export const MEMBERSHIP_STATUS = Object.freeze({
  RESEARCH_ONLY: "RESEARCH_ONLY",
  STAGED: "STAGED",
  CURRENT: "CURRENT",
  RETAINED: "RETAINED",
  EXCLUDED: "EXCLUDED",
  SUPERSEDED: "SUPERSEDED",
});

export const CHANGE_CLASS = Object.freeze({
  NEW: "NEW",
  MATERIAL_CHANGE: "MATERIAL_CHANGE",
  REVALIDATED: "REVALIDATED",
  RETAINED: "RETAINED",
  EXCLUDED: "EXCLUDED",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function durableMembershipPath(hotelId, root = process.cwd()) {
  return path.join(
    root,
    "data",
    "group-demand-intelligence",
    "hotels",
    text(hotelId) || "_unknown",
    "inventory-membership-v1.json",
  );
}

/**
 * Build a hotel-scoped membership record for one canonical opportunity.
 */
export function buildInventoryMembershipRecord(input = {}) {
  const hotelId = text(input.hotelId);
  const opportunityId = text(input.opportunityId || input.id);
  const now = text(input.now) || new Date().toISOString();
  const status = text(input.membershipStatus) || MEMBERSHIP_STATUS.RESEARCH_ONLY;
  const stagedVersion = text(input.stagedPublicationVersion) || null;
  const currentVersion = text(input.currentPublicationVersion) || null;
  return {
    schema: GDI_INVENTORY_MEMBERSHIP_PERSISTENCE_V1,
    hotelId: hotelId || null,
    opportunityId: opportunityId || null,
    canonicalOpportunityId: text(input.canonicalOpportunityId) || opportunityId || null,
    sharedOpportunity: input.sharedOpportunity === true,
    membershipStatus: status,
    stagedPublicationVersion: stagedVersion,
    currentPublicationVersion: currentVersion,
    inventoryPopulationPrimary:
      text(input.inventoryPopulationPrimary) || null,
    hotelSpecificRelevance: text(input.hotelSpecificRelevance) || null,
    hotelSpecificQualification: text(input.hotelSpecificQualification) || null,
    hotelSpecificPursuit: text(input.hotelSpecificPursuit) || null,
    firstDiscoveredAt: text(input.firstDiscoveredAt) || now,
    lastVerifiedAt: text(input.lastVerifiedAt) || now,
    changeClass: text(input.changeClass) || CHANGE_CLASS.RETAINED,
    evidenceSupportedExclusion:
      input.evidenceSupportedExclusion === true
        ? {
            reason: text(input.exclusionReason) || null,
            at: now,
          }
        : null,
    contentFingerprintSha256: text(input.contentFingerprintSha256) || null,
  };
}

/**
 * Derive membership rows from a STAGED (and optional CURRENT) package vs research bag.
 */
export function deriveMembershipFromPublications({
  hotelId,
  stagedPackage = null,
  currentPackage = null,
  researchOpportunities = [],
  now = new Date().toISOString(),
} = {}) {
  const stagedIds = new Set(
    (stagedPackage?.inventoryIds ||
      stagedPackage?.opportunities?.map((o) => text(o.id || o.opportunityId)) ||
      []
    )
      .map(text)
      .filter(Boolean),
  );
  const currentIds = new Set(
    (currentPackage?.inventoryIds ||
      currentPackage?.opportunities?.map((o) => text(o.id || o.opportunityId)) ||
      []
    )
      .map(text)
      .filter(Boolean),
  );
  const stagedVersion = text(stagedPackage?.publicationVersion) || null;
  const currentVersion = text(currentPackage?.publicationVersion) || null;
  const byId = new Map();

  for (const opp of researchOpportunities || []) {
    const id = text(opp.id || opp.opportunityId);
    if (!id) continue;
    byId.set(id, opp);
  }
  for (const opp of stagedPackage?.opportunities || []) {
    const id = text(opp.id || opp.opportunityId);
    if (!id) continue;
    if (!byId.has(id)) byId.set(id, opp);
  }
  for (const opp of currentPackage?.opportunities || []) {
    const id = text(opp.id || opp.opportunityId);
    if (!id) continue;
    if (!byId.has(id)) byId.set(id, opp);
  }

  const rows = [];
  for (const [opportunityId, opp] of byId) {
    let membershipStatus = MEMBERSHIP_STATUS.RESEARCH_ONLY;
    if (currentIds.has(opportunityId)) membershipStatus = MEMBERSHIP_STATUS.CURRENT;
    else if (stagedIds.has(opportunityId)) membershipStatus = MEMBERSHIP_STATUS.STAGED;

    rows.push(
      buildInventoryMembershipRecord({
        hotelId,
        opportunityId,
        canonicalOpportunityId: text(opp.canonicalOpportunityId) || opportunityId,
        sharedOpportunity: opp.sharedOpportunity === true,
        membershipStatus,
        stagedPublicationVersion: stagedIds.has(opportunityId) ? stagedVersion : null,
        currentPublicationVersion: currentIds.has(opportunityId)
          ? currentVersion
          : null,
        inventoryPopulationPrimary: opp.inventoryPopulationPrimary || null,
        hotelSpecificRelevance: opp.whyThisHotel || opp.hotelFit || null,
        hotelSpecificQualification: opp.qualificationStatus || null,
        firstDiscoveredAt: opp.firstDiscoveredAt || opp.createdAt || now,
        lastVerifiedAt: now,
        changeClass: stagedIds.has(opportunityId)
          ? CHANGE_CLASS.REVALIDATED
          : CHANGE_CLASS.RETAINED,
        contentFingerprintSha256: opp.contentFingerprintSha256 || null,
        now,
      }),
    );
  }

  return {
    schema: GDI_INVENTORY_MEMBERSHIP_PERSISTENCE_V1,
    hotelId: text(hotelId) || null,
    generatedAt: now,
    stagedPublicationVersion: stagedVersion,
    currentPublicationVersion: currentVersion,
    counts: {
      total: rows.length,
      staged: rows.filter((r) => r.membershipStatus === MEMBERSHIP_STATUS.STAGED || r.membershipStatus === MEMBERSHIP_STATUS.CURRENT).length,
      current: rows.filter((r) => r.membershipStatus === MEMBERSHIP_STATUS.CURRENT).length,
      researchOnly: rows.filter((r) => r.membershipStatus === MEMBERSHIP_STATUS.RESEARCH_ONLY).length,
    },
    rows,
  };
}

export function loadDurableMembershipLedger(hotelId, root = process.cwd()) {
  const p = durableMembershipPath(hotelId, root);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function writeDurableMembershipLedger(ledger, { root = process.cwd(), dryRun = true } = {}) {
  const hotelId = text(ledger?.hotelId);
  const p = durableMembershipPath(hotelId, root);
  const body = `${JSON.stringify(ledger, null, 2)}\n`;
  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      path: p,
      bytes: Buffer.byteLength(body),
      sha256: createHash("sha256").update(body).digest("hex"),
    };
  }
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, body, "utf8");
  return {
    ok: true,
    dryRun: false,
    path: p,
    bytes: Buffer.byteLength(body),
    sha256: createHash("sha256").update(body).digest("hex"),
  };
}

/**
 * Dry-run Airtable write plan — never writes. Maps membership into proposed GDO fields
 * or embedded payload keys when schema fields are not yet authorized.
 */
export function planInventoryMembershipAirtableWrites(ledger = {}, opts = {}) {
  const schemaAuthorized = opts.schemaAuthorized === true;
  const patches = (ledger.rows || []).map((row) => {
    const envelope = {
      schema: GDI_INVENTORY_MEMBERSHIP_PERSISTENCE_V1,
      hotelId: row.hotelId,
      opportunityId: row.opportunityId,
      membershipStatus: row.membershipStatus,
      stagedPublicationVersion: row.stagedPublicationVersion,
      currentPublicationVersion: row.currentPublicationVersion,
      inventoryPopulationPrimary: row.inventoryPopulationPrimary,
      firstDiscoveredAt: row.firstDiscoveredAt,
      lastVerifiedAt: row.lastVerifiedAt,
      changeClass: row.changeClass,
    };
    const fields = schemaAuthorized
      ? {
          "Hotel Inventory Membership JSON": JSON.stringify(envelope),
          "Staged Publication Version": row.stagedPublicationVersion || "",
          "Current Publication Version": row.currentPublicationVersion || "",
          "Inventory Population Primary": row.inventoryPopulationPrimary || "",
        }
      : {
          // Payload-only until founder authorizes schema fields.
          inventoryMembershipV1: envelope,
          stagedPublicationVersion: row.stagedPublicationVersion,
          currentPublicationVersion: row.currentPublicationVersion,
          inventoryPopulationPrimary: row.inventoryPopulationPrimary,
        };
    return {
      opportunityId: row.opportunityId,
      hotelId: row.hotelId,
      mode: schemaAuthorized ? "PROPOSED_GDO_FIELDS" : "PAYLOAD_EMBED_UNTIL_SCHEMA_AUTH",
      fields,
    };
  });
  return {
    schema: GDI_INVENTORY_MEMBERSHIP_PERSISTENCE_V1,
    dryRun: true,
    airtableWrites: false,
    schemaAuthorized,
    baseId: GDI_INVENTORY_MEMBERSHIP_AIRTABLE.baseId,
    tableId: GDI_INVENTORY_MEMBERSHIP_AIRTABLE.tableId,
    proposedFields: PROPOSED_GDO_MEMBERSHIP_FIELDS,
    patchCount: patches.length,
    patches,
    note: schemaAuthorized
      ? "Schema authorized — still requires ExactWriteSetAuthorization before apply"
      : "SCHEMA_AUTHORIZATION_REQUIRED — payload embed plan only; no live Airtable schema/data writes",
  };
}

/**
 * Reconstruction helper: filter Airtable/research rows to STAGED membership using
 * durable membership ledger or embedded membership fields.
 */
export function filterRowsByStagedMembership(rows = [], membershipLedger = null, stagedVersion = null) {
  const wantVersion = text(stagedVersion);
  const byId = new Map(
    (membershipLedger?.rows || []).map((r) => [text(r.opportunityId), r]),
  );
  const out = [];
  for (const row of rows || []) {
    const id = text(row.id || row.opportunityId);
    const mem =
      byId.get(id) ||
      row.inventoryMembershipV1 ||
      row.fields?.inventoryMembershipV1 ||
      null;
    if (!mem) continue;
    const status = text(mem.membershipStatus);
    const stagedVer = text(mem.stagedPublicationVersion);
    const inStaged =
      status === MEMBERSHIP_STATUS.STAGED ||
      status === MEMBERSHIP_STATUS.CURRENT ||
      (wantVersion && stagedVer === wantVersion);
    if (inStaged) out.push(row);
  }
  return out;
}

export function evaluateMembershipPersistenceReadiness({
  membershipLedger = null,
  airtableRows = [],
  stagedPackage = null,
} = {}) {
  const stagedIds = new Set(
    (stagedPackage?.inventoryIds ||
      stagedPackage?.opportunities?.map((o) => text(o.id || o.opportunityId)) ||
      []
    )
      .map(text)
      .filter(Boolean),
  );
  const ledgerStaged = new Set(
    (membershipLedger?.rows || [])
      .filter(
        (r) =>
          r.membershipStatus === MEMBERSHIP_STATUS.STAGED ||
          r.membershipStatus === MEMBERSHIP_STATUS.CURRENT,
      )
      .map((r) => text(r.opportunityId))
      .filter(Boolean),
  );
  const airtableWithMembership = (airtableRows || []).filter((r) => {
    const mem =
      r.inventoryMembershipV1 ||
      r.fields?.inventoryMembershipV1 ||
      r.fields?.["Hotel Inventory Membership JSON"];
    return Boolean(mem);
  }).length;

  const missingFromLedger = [...stagedIds].filter((id) => !ledgerStaged.has(id));
  const corrections = [];
  if (!membershipLedger) {
    corrections.push({
      code: "DURABLE_MEMBERSHIP_LEDGER_MISSING",
      correction: "Generate inventory-membership-v1.json via deriveMembershipFromPublications (dry-run first).",
    });
  }
  if (missingFromLedger.length) {
    corrections.push({
      code: "STAGED_IDS_MISSING_FROM_MEMBERSHIP_LEDGER",
      count: missingFromLedger.length,
      sample: missingFromLedger.slice(0, 10),
    });
  }
  if (airtableWithMembership === 0 && stagedIds.size > 0) {
    corrections.push({
      code: "INVENTORY_MEMBERSHIP_NOT_PERSISTED",
      correction:
        "Airtable GDO rows lack membership envelope. Persist after founder ExactWriteSetAuthorization (schema fields if authorized).",
    });
  }

  return {
    schema: GDI_INVENTORY_MEMBERSHIP_PERSISTENCE_V1,
    ok: corrections.length === 0,
    status: corrections.length === 0 ? "READY" : "PARTIAL",
    stagedCount: stagedIds.size,
    ledgerStagedCount: ledgerStaged.size,
    airtableWithMembership,
    corrections,
  };
}
