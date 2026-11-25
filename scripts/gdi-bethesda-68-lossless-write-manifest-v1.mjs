#!/usr/bin/env node
/**
 * Rebuild Bethesda 68 write manifest under lossless SoT merge contract.
 * Dry-run only — no Airtable mutation. VALIDATED_FQO_ONLY gate preserved.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const ROOT_EARLY = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
dotenv.config({ path: path.join(ROOT_EARLY, ".env") });
if (!process.env.AIRTABLE_API_KEY && !process.env.AIRTABLE_PAT) {
  dotenv.config({
    path: path.join("C:/Dev/dealality/my-operators-backend", ".env"),
  });
}
import {
  buildResearchInventoryFields,
  mergeProposedResearchInventoryFields,
  listResearchInventoryForHotel,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import {
  MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F,
  buildHotelResearchCompositeKey,
} from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import {
  evaluateFinalizedQualifiedOpportunity,
} from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import {
  filterValidatedFqoPersistenceWriteSet,
  RESEARCH_PERSISTENCE_WRITE_SET_POLICY,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import { mergeResearchSotLossless } from "../lib/group-demand-intelligence/workflow/gdi-research-sot-lossless-merge-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-68-lossless-write-manifest-v1",
);
const BETHESDA = "recLuxvwwxID7U2B8";
const QUALITY =
  "gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1";

fs.mkdirSync(OUT, { recursive: true });

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function writeJson(name, data) {
  const p = path.join(OUT, name);
  fs.writeFileSync(p, `${JSON.stringify(data, null, 2)}\n`);
  return p;
}

function loadQualityOpps() {
  const p = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    BETHESDA,
    QUALITY,
    "package.json",
  );
  return (JSON.parse(fs.readFileSync(p, "utf8")).opportunities || []).map(
    (o) => ({
      ...o,
      opportunityId: text(o.opportunityId || o.id),
      hotelId: BETHESDA,
    }),
  );
}

const quality = loadQualityOpps();
const fqoGate = filterValidatedFqoPersistenceWriteSet(quality, {
  allowHistoricalSchemaCompat: true,
});
const keptIds = new Set(
  (fqoGate.kept || []).map((o) => text(o.opportunityId || o.id)).filter(Boolean),
);

const hasAirtable = Boolean(
  process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT,
);

let liveByOpp = new Map();
let airtableError = null;
if (hasAirtable) {
  try {
    const listed = await listResearchInventoryForHotel(BETHESDA);
    for (const r of listed || []) {
      const oid = text(r.fields?.[F.opportunityId] || r.opportunityId);
      if (oid) liveByOpp.set(oid, r);
    }
  } catch (err) {
    airtableError = String(err?.message || err);
  }
}

const buckets = {
  VERIFIED_ENRICHMENT: [],
  CERTIFICATION_METADATA_ONLY: [],
  UNCHANGED: [],
  CONFLICT_REQUIRES_REVIEW: [],
  BLOCKED_INFORMATION_LOSS: [],
  MISSING_LIVE_RECORD: [],
  NOT_IN_FQO_WRITE_SET: [],
};

const rows = [];
const historicallyCompatible = [];

for (const opp of quality) {
  const oid = opp.opportunityId;
  const fqo = evaluateFinalizedQualifiedOpportunity(opp, {
    historicalApprovedInventory: true,
  });
  const allowed = keptIds.has(oid);

  if (fqo.finalizedQualified === true) {
    historicallyCompatible.push({
      opportunityId: oid,
      fqoStatus: fqo.status || fqo.fqoStatus || null,
      evidencePreservedGate: true,
    });
  }

  if (!allowed) {
    buckets.NOT_IN_FQO_WRITE_SET.push(oid);
    rows.push({
      opportunityId: oid,
      classification: "NOT_IN_FQO_WRITE_SET",
      note: "Preserved as nonqualified research — no promotion/write",
    });
    continue;
  }

  const live = liveByOpp.get(oid);
  if (!live) {
    buckets.MISSING_LIVE_RECORD.push(oid);
    rows.push({
      opportunityId: oid,
      classification: "MISSING_LIVE_RECORD",
      note: hasAirtable
        ? "No live research inventory row — CREATE path out of canary scope"
        : "Airtable not queried",
    });
    continue;
  }

  const built = buildResearchInventoryFields(opp, { hotelId: BETHESDA });
  if (!built.ok) {
    buckets.BLOCKED_INFORMATION_LOSS.push(oid);
    rows.push({
      opportunityId: oid,
      classification: "BLOCKED_INFORMATION_LOSS",
      code: built.code,
    });
    continue;
  }

  const merged = mergeProposedResearchInventoryFields(
    built.fields,
    live.fields || live,
  );

  let classification = merged.classification || "UNCHANGED";
  if (!merged.ok) {
    classification =
      merged.code === "CONFLICT_REQUIRES_REVIEW"
        ? "CONFLICT_REQUIRES_REVIEW"
        : "BLOCKED_INFORMATION_LOSS";
  } else if (!classification || classification === "UNCHANGED") {
    // Confirm payload semantic class from sotMerge
    classification = merged.sotMerge?.classification || "UNCHANGED";
  }

  if (!buckets[classification]) {
    // Map unknown to VERIFIED_ENRICHMENT if ok
    classification = merged.ok ? "VERIFIED_ENRICHMENT" : "BLOCKED_INFORMATION_LOSS";
  }
  buckets[classification].push(oid);

  const livePayload = (() => {
    try {
      return JSON.parse(live.fields?.[F.researchPayloadJson] || "{}");
    } catch {
      return {};
    }
  })();
  const afterPayload = merged.ok
    ? JSON.parse(merged.fields[F.researchPayloadJson])
    : null;

  rows.push({
    opportunityId: oid,
    airtableRecordId: live.id || null,
    compositeKey: buildHotelResearchCompositeKey(BETHESDA, oid),
    classification,
    mergeOk: merged.ok === true,
    mergeCode: merged.code || null,
    livePayloadChars: String(live.fields?.[F.researchPayloadJson] || "").length,
    mergedPayloadChars: merged.ok
      ? String(merged.fields[F.researchPayloadJson] || "").length
      : 0,
    preservedMeetingHistory:
      afterPayload &&
      (afterPayload.meetingHistory || []).length >=
        (livePayload.meetingHistory || []).length,
    preservedCompetitors:
      afterPayload &&
      (afterPayload.competitors || []).length >=
        (livePayload.competitors || []).length,
    commercialDemandType: afterPayload?.commercialDemandType || livePayload.commercialDemandType || null,
    revenueStreamClass: afterPayload?.revenueStreamClass || livePayload.revenueStreamClass || null,
    autoResolvedConflicts: merged.sotMerge?.autoResolvedConflicts?.map((c) => c.code) || [],
    blockers: merged.blockers || merged.sotMerge?.blockers || [],
  });
}

const necessaryUpdates = [
  ...buckets.VERIFIED_ENRICHMENT,
  ...buckets.CERTIFICATION_METADATA_ONLY,
];

const summary = {
  hotelId: BETHESDA,
  qualityPackage: QUALITY,
  qualityOpportunityCount: quality.length,
  writeSetPolicy: RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
  airtableQueried: hasAirtable && !airtableError,
  airtableError,
  liveRecordCount: liveByOpp.size,
  bucketCounts: Object.fromEntries(
    Object.entries(buckets).map(([k, v]) => [k, v.length]),
  ),
  necessaryUpdateCount: necessaryUpdates.length,
  necessaryUpdateOpportunityIds: necessaryUpdates,
  historicallyCompatibleFqoCount: historicallyCompatible.length,
  mutationAuthorized: false,
  note: "Dry manifest only. Nonqualified research records are not modified or promoted. No canary/bulk write executed.",
};

writeJson("00-MANIFEST_SUMMARY.json", summary);
writeJson("01-PER_OPPORTUNITY_CLASSIFICATIONS.json", rows);
writeJson("02-BUCKETS.json", buckets);
writeJson("03-HISTORICALLY_COMPATIBLE_FQO.json", {
  count: historicallyCompatible.length,
  opportunities: historicallyCompatible,
});

console.log(JSON.stringify(summary, null, 2));
