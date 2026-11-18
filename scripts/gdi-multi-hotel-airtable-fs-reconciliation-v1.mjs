#!/usr/bin/env node
/**
 * YOTEL (D5-C) + Radisson/Guadalajara (D6) FS↔Airtable opportunity reconciliation.
 * Read-only. No deletes, overwrites, republish, or silent imports.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listOpportunitiesForHotel } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-full-e2e-airtable-persistence-adapter-v1",
);

const HOTELS = Object.freeze([
  {
    key: "yotel",
    name: "YOTEL Geneva Lake",
    hotelId: "recrPQcZg7SFARRb2",
    decision: "D5_OPTION_C",
  },
  {
    key: "radisson",
    name: "Radisson Blu Toronto Downtown",
    hotelId: "recH9h3McgMNFQEla",
    decision: "D6",
  },
  {
    key: "guadalajara",
    name: "Sheraton Guadalajara Expo",
    hotelId: "recCAGqkERSXgiku4",
    decision: "D6",
  },
]);

function loadFsOps(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) return { present: false, byId: new Map(), path: p };
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const arr = Array.isArray(j) ? j : j.opportunities || [];
  const byId = new Map();
  for (const o of arr) {
    const id = o.id || o.opportunityId;
    if (!id) continue;
    byId.set(String(id), {
      id: String(id),
      organizationName: o.organizationName || o.company || null,
      title: o.title || o.opportunityName || null,
      status: o.status || o.lifecycleStatus || null,
      demandThesis:
        o.demandThesis || o.demandThesisStandard || o.thesisStandard || null,
      priority: o.priority || null,
    });
  }
  return { present: true, byId, path: p, count: byId.size };
}

function publicationPointers(hotelId) {
  const dir = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
  );
  const out = {};
  for (const name of ["STAGED.json", "CURRENT.json"]) {
    const p = path.join(dir, name);
    if (!fs.existsSync(p)) {
      out[name.replace(".json", "").toLowerCase()] = null;
      continue;
    }
    try {
      const j = JSON.parse(fs.readFileSync(p, "utf8"));
      out[name.replace(".json", "").toLowerCase()] = {
        version: j.version || j.packageVersion || null,
        path: p,
      };
    } catch {
      out[name.replace(".json", "").toLowerCase()] = { parseError: true, path: p };
    }
  }
  return out;
}

function classifyPair(fsRow, atRow) {
  if (fsRow && atRow) {
    const conflicts = [];
    if (
      fsRow.organizationName &&
      atRow.organizationName &&
      String(fsRow.organizationName).toLowerCase() !==
        String(atRow.organizationName).toLowerCase()
    ) {
      conflicts.push("ORGANIZATION_NAME_MISMATCH");
    }
    return {
      class: conflicts.length ? "SHARED_IDENTITY_CONFLICT" : "SHARED",
      conflicts,
    };
  }
  if (atRow && !fsRow) {
    const active = /active|live|open|pursue|watch/i.test(
      String(atRow.status || ""),
    );
    return {
      class: active ? "AIRTABLE_ONLY_ACTIVE" : "AIRTABLE_ONLY_HISTORICAL_OR_UNSPECIFIED",
      conflicts: [],
    };
  }
  if (fsRow && !atRow) {
    return { class: "FILESYSTEM_ONLY", conflicts: [] };
  }
  return { class: "EMPTY", conflicts: [] };
}

async function reconcileHotel(hotel) {
  const fsOps = loadFsOps(hotel.hotelId);
  const atList = await listOpportunitiesForHotel(hotel.hotelId);
  const atById = new Map();
  for (const o of atList || []) {
    const id = o.id || o.opportunityId;
    if (!id) continue;
    atById.set(String(id), {
      id: String(id),
      airtableRecordId: o.airtableRecordId || o._airtableRecordId || null,
      organizationName: o.organizationName || o.company || null,
      title: o.title || o.opportunityName || null,
      status: o.status || o.lifecycleStatus || null,
      demandThesis:
        o.demandThesis || o.demandThesisStandard || o.thesisStandard || null,
    });
  }

  const allIds = new Set([...fsOps.byId.keys(), ...atById.keys()]);
  const rows = [];
  const counts = {
    SHARED: 0,
    SHARED_IDENTITY_CONFLICT: 0,
    AIRTABLE_ONLY_ACTIVE: 0,
    AIRTABLE_ONLY_HISTORICAL_OR_UNSPECIFIED: 0,
    FILESYSTEM_ONLY: 0,
  };
  for (const id of [...allIds].sort()) {
    const fsRow = fsOps.byId.get(id) || null;
    const atRow = atById.get(id) || null;
    const { class: cls, conflicts } = classifyPair(fsRow, atRow);
    counts[cls] = (counts[cls] || 0) + 1;
    rows.push({
      opportunityId: id,
      class: cls,
      conflicts,
      filesystem: fsRow,
      airtable: atRow,
      recommendedTreatment:
        cls === "SHARED"
          ? "KEEP_BOTH_IDEMPOTENT_UPSERT_BY_ID"
          : cls === "SHARED_IDENTITY_CONFLICT"
            ? "MANUAL_REVIEW_NO_AUTO_OVERWRITE"
            : cls === "FILESYSTEM_ONLY"
              ? "ELIGIBILITY_GATE_BEFORE_ANY_AIRTABLE_CREATE"
              : "DO_NOT_SILENTLY_IMPORT_OR_DELETE_AIRTABLE_ONLY",
    });
  }

  return {
    ...hotel,
    filesystemCount: fsOps.count || 0,
    airtableCount: atById.size,
    publication: publicationPointers(hotel.hotelId),
    counts,
    rows,
    policy: {
      filesystemResearchSoT: true,
      airtableHistoricalRegistry: true,
      publicationPackageCustomerSoT: true,
      noDelete: true,
      noSilentImport: true,
      noRepublish: true,
    },
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const hotels = [];
  for (const h of HOTELS) {
    hotels.push(await reconcileHotel(h));
  }
  const doc = {
    version: "gdi-multi-hotel-airtable-fs-reconciliation-v1",
    generatedAt: new Date().toISOString(),
    mutation: false,
    hotels: hotels.map((h) => ({
      key: h.key,
      name: h.name,
      hotelId: h.hotelId,
      decision: h.decision,
      filesystemCount: h.filesystemCount,
      airtableCount: h.airtableCount,
      counts: h.counts,
      publication: h.publication,
      policy: h.policy,
      sampleFilesystemOnly: h.rows
        .filter((r) => r.class === "FILESYSTEM_ONLY")
        .slice(0, 25)
        .map((r) => r.opportunityId),
      sampleAirtableOnly: h.rows
        .filter((r) => String(r.class).startsWith("AIRTABLE_ONLY"))
        .slice(0, 25)
        .map((r) => ({ id: r.opportunityId, class: r.class })),
      sampleShared: h.rows
        .filter((r) => r.class === "SHARED")
        .slice(0, 15)
        .map((r) => r.opportunityId),
      sampleConflicts: h.rows
        .filter((r) => r.class === "SHARED_IDENTITY_CONFLICT")
        .slice(0, 15),
    })),
  };
  // Full ID-level dumps per hotel (bounded privacy: no contact PII)
  for (const h of hotels) {
    fs.writeFileSync(
      path.join(OUT, `${h.key.toUpperCase()}_ID_RECONCILIATION.json`),
      JSON.stringify(
        {
          hotelId: h.hotelId,
          name: h.name,
          decision: h.decision,
          counts: h.counts,
          filesystemCount: h.filesystemCount,
          airtableCount: h.airtableCount,
          publication: h.publication,
          rows: h.rows,
        },
        null,
        2,
      ),
    );
  }
  fs.writeFileSync(
    path.join(OUT, "MULTI_HOTEL_RECONCILIATION_SUMMARY.json"),
    JSON.stringify(doc, null, 2),
  );
  console.log(JSON.stringify(doc.hotels.map((h) => ({
    key: h.key,
    fs: h.filesystemCount,
    at: h.airtableCount,
    counts: h.counts,
  })), null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
