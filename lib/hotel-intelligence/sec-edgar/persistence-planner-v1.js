/**
 * Plan non-destructive HI Graph Evidence (+ optional relationship/HPC) upserts.
 * Dry-run by default. No new Airtable fields.
 */

import crypto from "node:crypto";
import {
  MAP_HI_GRAPH_EVIDENCE,
  MAP_HI_HOTEL_ORG_RELATIONSHIP,
  MAP_HI_ORGANIZATION,
  GRAPH_SCHEMA_VERSION,
  HI_GRAPH_TABLES,
} from "../research-engine-v1/airtable-graph-contract-v1.js";
import { PERSISTENCE_CLASS, PUBLICATION_GATE, HPC_ALLOWLISTED_PROMOTION_FIELDS } from "./contract.js";

export const PERSISTENCE_PLANNER_VERSION = "sec-edgar-persistence-planner-v1";

function evidenceId(parts) {
  const raw = parts.filter(Boolean).join("|");
  return `evid_sec_${crypto.createHash("sha1").update(raw).digest("hex").slice(0, 24)}`;
}

function relationshipId(parts) {
  const raw = parts.filter(Boolean).join("|");
  return `rel_sec_${crypto.createHash("sha1").update(raw).digest("hex").slice(0, 24)}`;
}

/**
 * @param {object[]} attributedVerified — output rows from attributeFindings
 * @param {object} [opts]
 */
export function planContractPersistence(attributedVerified = [], opts = {}) {
  const evidenceRows = [];
  const relationshipRows = [];
  const orgRows = [];
  const hpcNullFills = [];
  const quarantined = [];
  const rejected = [];

  for (const row of attributedVerified) {
    if (row.persistenceClass === PERSISTENCE_CLASS.REJECTED_ESTIMATE) {
      rejected.push(row);
      continue;
    }
    if (!row.persistable || row.attribution?.quarantine) {
      quarantined.push(row);
      continue;
    }
    const f = row.finding || {};
    const hpcId = f.hpcHotelId || row.attribution?.hpcId;
    if (!hpcId) {
      quarantined.push(row);
      continue;
    }

    const eid = evidenceId([
      hpcId,
      f.evidenceFieldName,
      f.primaryDate,
      f.source?.accessionNumber,
      f.source?.documentUrl,
    ]);

    evidenceRows.push({
      action: "UPSERT",
      table: HI_GRAPH_TABLES.graphEvidence,
      dedupeKey: eid,
      fields: {
        [MAP_HI_GRAPH_EVIDENCE.evidenceId]: eid,
        [MAP_HI_GRAPH_EVIDENCE.hpcHotelId]: hpcId,
        [MAP_HI_GRAPH_EVIDENCE.entityType]: "Hotel–Org Relationship",
        [MAP_HI_GRAPH_EVIDENCE.fieldName]: f.evidenceFieldName,
        [MAP_HI_GRAPH_EVIDENCE.valueObserved]: f.primaryDate || f.verificationClass,
        [MAP_HI_GRAPH_EVIDENCE.sourceName]: `${f.source?.companyName || "SEC registrant"} ${f.source?.formType || ""}`.trim(),
        [MAP_HI_GRAPH_EVIDENCE.sourceType]: "SEC EDGAR Filing",
        [MAP_HI_GRAPH_EVIDENCE.sourceUrl]: f.source?.documentUrl || null,
        [MAP_HI_GRAPH_EVIDENCE.sourcePublishedDate]: f.source?.filingDate || null,
        [MAP_HI_GRAPH_EVIDENCE.retrievedAt]: f.extractedAt || new Date().toISOString(),
        [MAP_HI_GRAPH_EVIDENCE.evidenceSnippet]: String(f.snippet || "").slice(0, 1500),
        [MAP_HI_GRAPH_EVIDENCE.confidence]: "MEDIUM",
        [MAP_HI_GRAPH_EVIDENCE.evidenceStrength]: "Filing excerpt — hotel-attributed",
        [MAP_HI_GRAPH_EVIDENCE.current]:
          f.verificationClass === "HISTORICAL_AGREEMENT" ? false : true,
        [MAP_HI_GRAPH_EVIDENCE.notes]: JSON.stringify({
          verificationClass: f.verificationClass,
          agreementType: f.agreementType,
          accessionNumber: f.source?.accessionNumber,
          cik: f.source?.cik,
          publication: PUBLICATION_GATE,
        }),
        [MAP_HI_GRAPH_EVIDENCE.schemaVersion]: GRAPH_SCHEMA_VERSION,
      },
      findingRef: {
        hpcId,
        verificationClass: f.verificationClass,
        hotelName: f.attributedHotelName,
      },
    });

    if (opts.planRelationships !== false && f.agreementType) {
      const relType =
        f.agreementType === "FRANCHISE_AGREEMENT"
          ? "Brand"
          : f.agreementType === "MANAGEMENT_AGREEMENT"
            ? "Operator"
            : "Management Company";
      const rid = relationshipId([hpcId, relType, f.source?.cik, f.primaryDate]);
      relationshipRows.push({
        action: "UPSERT_IF_ORG_RESOLVED",
        table: HI_GRAPH_TABLES.hotelOrgRelationships,
        dedupeKey: rid,
        requiresOrganizationResolution: true,
        fields: {
          [MAP_HI_HOTEL_ORG_RELATIONSHIP.relationshipId]: rid,
          [MAP_HI_HOTEL_ORG_RELATIONSHIP.hpcHotelId]: hpcId,
          [MAP_HI_HOTEL_ORG_RELATIONSHIP.relationshipType]: relType,
          [MAP_HI_HOTEL_ORG_RELATIONSHIP.current]:
            f.verificationClass !== "HISTORICAL_AGREEMENT",
          [MAP_HI_HOTEL_ORG_RELATIONSHIP.effectiveFrom]:
            f.verificationClass === "VERIFIED_EFFECTIVE_DATE" ||
            f.verificationClass === "VERIFIED_SIGNING_DATE"
              ? f.primaryDate
              : null,
          [MAP_HI_HOTEL_ORG_RELATIONSHIP.effectiveTo]:
            f.verificationClass === "VERIFIED_EXPIRATION" ||
            f.verificationClass === "VERIFIED_TERMINATION"
              ? f.primaryDate
              : null,
          [MAP_HI_HOTEL_ORG_RELATIONSHIP.confidence]: "MEDIUM",
          [MAP_HI_HOTEL_ORG_RELATIONSHIP.verificationState]: "Evidence Collected",
          [MAP_HI_HOTEL_ORG_RELATIONSHIP.publicationState]: "INTERNAL_ONLY",
          [MAP_HI_HOTEL_ORG_RELATIONSHIP.schemaVersion]: GRAPH_SCHEMA_VERSION,
          [MAP_HI_HOTEL_ORG_RELATIONSHIP.notes]: `SEC ${f.source?.formType} ${f.source?.accessionNumber}`,
        },
      });
    }

    if (opts.planHpcNullFill === true && f.agreementType === "MANAGEMENT_AGREEMENT") {
      hpcNullFills.push({
        action: "NULL_FILL_ONLY",
        table: "Hotel Property Census",
        recordId: hpcId,
        allowedFields: [...HPC_ALLOWLISTED_PROMOTION_FIELDS],
        proposed: {
          "Management Model": "Management Agreement",
          "Operator Source URL": f.source?.documentUrl || null,
          "Operator Confidence": "MEDIUM",
        },
        note: "Only applied when existing HPC field is empty; never overwrites verified values.",
      });
    }
  }

  if (opts.companyOrg) {
    orgRows.push({
      action: "UPSERT",
      table: HI_GRAPH_TABLES.organizations,
      fields: {
        [MAP_HI_ORGANIZATION.legalName]: opts.companyOrg.legalName,
        [MAP_HI_ORGANIZATION.tradingName]: opts.companyOrg.tradingName || opts.companyOrg.legalName,
        [MAP_HI_ORGANIZATION.organizationType]: opts.companyOrg.organizationType || "REIT",
        [MAP_HI_ORGANIZATION.registryId]: `CIK:${opts.companyOrg.cik}`,
        [MAP_HI_ORGANIZATION.verificationState]: "Evidence Collected",
        [MAP_HI_ORGANIZATION.confidence]: "HIGH",
        [MAP_HI_ORGANIZATION.publicationState]: "INTERNAL_ONLY",
        [MAP_HI_ORGANIZATION.schemaVersion]: GRAPH_SCHEMA_VERSION,
        [MAP_HI_ORGANIZATION.notes]: "SEC EDGAR pilot registrant",
      },
    });
  }

  return {
    version: PERSISTENCE_PLANNER_VERSION,
    dryRun: true,
    publication: PUBLICATION_GATE,
    counts: {
      evidence: evidenceRows.length,
      relationships: relationshipRows.length,
      organizations: orgRows.length,
      hpcNullFills: hpcNullFills.length,
      quarantined: quarantined.length,
      rejectedEstimates: rejected.length,
    },
    evidenceRows,
    relationshipRows,
    orgRows,
    hpcNullFills,
    quarantined: quarantined.map((q) => ({
      class: q.verificationClass,
      reason: q.attribution?.reason || q.reasons,
      snippet: String(q.finding?.snippet || "").slice(0, 200),
    })),
    rejectedEstimates: rejected.length,
  };
}
