/**
 * Canonical persistence for orchestrator publication handoffs.
 * Distinguishes research-memory promotion from Airtable/FS source-of-truth writes.
 */

import fs from "node:fs";
import path from "node:path";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../opportunity-persistence.js";
import { ORCHESTRATOR_STOP_REASONS } from "./research-constants-v1.js";

/**
 * Persist one promoted opportunity through the existing canonical writer,
 * then read back. On failure, write a replayable package (fail closed).
 */
export async function persistPromotedOpportunityCanonical({
  hotelId,
  patched,
  outDir,
  authorizePublication = false,
} = {}) {
  if (!authorizePublication) {
    return {
      ok: false,
      status: "AUTHORIZE_REQUIRED",
      blockingGate: "authorizePublication_required",
      researchMemoryOnly: true,
    };
  }
  if (!patched?.id && !patched?.opportunityId) {
    return {
      ok: false,
      status: "MISSING_OPPORTUNITY_ID",
      blockingGate: "missing_id",
    };
  }

  const id = patched.id || patched.opportunityId;
  const packagePath = outDir
    ? path.join(outDir, "persist-packages", `${id}.json`)
    : null;

  const writePackage = {
    at: new Date().toISOString(),
    hotelId,
    opportunityId: id,
    airtableRecordId: patched._airtableRecordId || null,
    patched,
    status: "PENDING_CANONICAL_WRITE",
  };

  try {
    const doc = await loadOpportunitiesCanonical(hotelId, { bypassCache: true });
    const ops = Array.isArray(doc?.opportunities) ? [...doc.opportunities] : [];
    const idx = ops.findIndex((o) => (o.id || o.opportunityId) === id);
    if (idx >= 0) {
      // Preserve feedback / activity; overlay publication fields
      ops[idx] = {
        ...ops[idx],
        ...patched,
        activityHistory: [
          ...(ops[idx].activityHistory || []),
          ...(patched.activityHistory || []).slice(-3),
        ],
        publicationEvents: patched.publicationEvents || ops[idx].publicationEvents,
      };
    } else {
      ops.push(patched);
    }

    const saved = await saveOpportunitiesCanonical(hotelId, {
      ...doc,
      hotelId,
      opportunities: ops,
    });

    // Read-back
    const read = await loadOpportunitiesCanonical(hotelId, { bypassCache: true });
    const found = (read.opportunities || []).find(
      (o) => (o.id || o.opportunityId) === id
    );
    if (!found) {
      writePackage.status = ORCHESTRATOR_STOP_REASONS.PERSISTENCE_BLOCKED;
      writePackage.error = "read_back_missing";
      if (packagePath) {
        fs.mkdirSync(path.dirname(packagePath), { recursive: true });
        fs.writeFileSync(packagePath, JSON.stringify(writePackage, null, 2) + "\n");
      }
      return {
        ok: false,
        status: ORCHESTRATOR_STOP_REASONS.PERSISTENCE_BLOCKED,
        blockingGate: "read_back_missing",
        packagePath,
        persistence: saved?.persistence || null,
      };
    }

    const persistence = saved?.persistence || read?.persistence || null;
    const airtableRecordId = found._airtableRecordId || null;

    // Airtable mode that fell back to disk is not production publication — fail closed.
    if (persistence === "filesystem_fallback" || (persistence === "filesystem" && !airtableRecordId)) {
      if (packagePath) {
        writePackage.status = "FILESYSTEM_FALLBACK_NOT_AIRTABLE";
        writePackage.persistence = persistence;
        writePackage.note =
          "Replayable package preserved. Do not claim Airtable production publication or invent record ids.";
        fs.mkdirSync(path.dirname(packagePath), { recursive: true });
        fs.writeFileSync(packagePath, JSON.stringify(writePackage, null, 2) + "\n");
      }
      return {
        ok: false,
        status: "FILESYSTEM_FALLBACK_NOT_AIRTABLE",
        persistence,
        airtableRecordId: null,
        customerVisible: false,
        prospectHotelFacing: found.prospectHotelFacing === true,
        prospectPublicationBasis: found.prospectPublicationBasis || null,
        blockingGate: "airtable_unavailable_or_no_record_id",
        packagePath,
        productionPublished: false,
        readBack: {
          id: found.id,
          organizationName: found.organizationName,
          customerVisible: false,
          prospectHotelFacing: found.prospectHotelFacing,
          basis: found.prospectPublicationBasis,
          persistence,
        },
        note: "Filesystem write is not live Airtable publication",
      };
    }

    const visible = found.customerVisible === true;
    return {
      ok: true,
      status: visible ? "CANONICAL_CUSTOMER_VISIBLE" : "CANONICAL_SAVED_NOT_VISIBLE",
      persistence,
      airtableRecordId,
      customerVisible: found.customerVisible === true,
      prospectHotelFacing: found.prospectHotelFacing === true,
      prospectPublicationBasis: found.prospectPublicationBasis || null,
      productionPublished: Boolean(airtableRecordId),
      readBack: {
        id: found.id,
        organizationName: found.organizationName,
        customerVisible: found.customerVisible,
        prospectHotelFacing: found.prospectHotelFacing,
        basis: found.prospectPublicationBasis,
      },
    };
  } catch (err) {
    writePackage.status = ORCHESTRATOR_STOP_REASONS.PERSISTENCE_BLOCKED;
    writePackage.error = err?.message || String(err);
    if (packagePath) {
      fs.mkdirSync(path.dirname(packagePath), { recursive: true });
      fs.writeFileSync(packagePath, JSON.stringify(writePackage, null, 2) + "\n");
    }
    return {
      ok: false,
      status: ORCHESTRATOR_STOP_REASONS.PERSISTENCE_BLOCKED,
      blockingGate: "canonical_write_failed",
      error: err?.message || String(err),
      packagePath,
      researchMemoryOnly: true,
    };
  }
}

/**
 * Verify whether an opportunity is customer-visible in canonical store
 * (not merely research-memory).
 */
export async function verifyCanonicalCustomerVisibility(hotelId, opportunityId) {
  try {
    const doc = await loadOpportunitiesCanonical(hotelId, { bypassCache: true });
    const found = (doc.opportunities || []).find(
      (o) => (o.id || o.opportunityId) === opportunityId
    );
    if (!found) {
      return {
        found: false,
        customerVisible: false,
        persistence: doc.persistence || doc.source || null,
        note: "not_in_canonical_store",
      };
    }
    return {
      found: true,
      customerVisible: found.customerVisible === true,
      prospectHotelFacing: found.prospectHotelFacing === true,
      basis: found.prospectPublicationBasis || null,
      airtableRecordId: found._airtableRecordId || null,
      persistence: doc.persistence || doc.source || null,
      researchMemoryOnly: false,
    };
  } catch (err) {
    return {
      found: false,
      customerVisible: false,
      error: err?.message || String(err),
      note: "canonical_load_failed",
    };
  }
}
