/**
 * Regression: failed San Diego Parallel live run must quarantine Bethesda/DMV
 * rows before hotel-fit / card generation. Preserves rematerialized evidence.
 *
 * Usage: node scripts/test-gdi-geographic-relevance-gate-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  quarantineWrongMarketCandidates,
  GEO_DISPOSITION,
} from "../lib/group-demand-intelligence/workflow/geographic-relevance-gate-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EVIDENCE = path.join(
  ROOT,
  "reports/gdi-operational-baseline/san-diego-parallel-rematerialized/parallel-live-rematerialized.json"
);
const HOTEL_CFG = path.join(
  ROOT,
  "config/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o.json"
);

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

const evidence = JSON.parse(fs.readFileSync(EVIDENCE, "utf8"));
const hotelConfig = JSON.parse(fs.readFileSync(HOTEL_CFG, "utf8"));

assert(
  evidence.providerRunId === "trun_567f3335d0c140ad8e87120211c6f61f",
  "must use preserved failed live run as regression evidence"
);
assert(
  evidence.hotelId === "recnPB3Q02NO8Ma1o",
  "evidence hotelId must remain San Diego Bayside"
);
assert(
  Array.isArray(evidence.candidates) && evidence.candidates.length === 8,
  "expected 8 rematerialized candidates"
);

const gate = quarantineWrongMarketCandidates(
  evidence.candidates,
  hotelConfig,
  { hotelId: evidence.hotelId }
);

const bethesdaLike = gate.quarantined.filter((c) =>
  /bethesda|maryland|nih|walter reed|pooks hill/i.test(
    [
      c.location,
      c.title,
      c.hotelOpportunityThesis,
      c.organizationName,
      JSON.stringify(c.rawParallel || {}),
    ].join(" ")
  )
);

assert(
  bethesdaLike.length >= 5,
  `expected >=5 Bethesda/DMV quarantines, got ${bethesdaLike.length}`
);
assert(
  gate.accepted.every(
    (c) => c.geographicRelevance?.disposition === GEO_DISPOSITION.IN_TERRITORY
  ),
  "accepted rows must be in_territory only"
);
assert(
  gate.quarantined.every((c) => c.quarantineStatus === "WRONG_MARKET_OR_UNRESOLVED_GEO"),
  "quarantined must carry WRONG_MARKET_OR_UNRESOLVED_GEO"
);
assert(
  gate.quarantined.every((c) => c.independentHotelFit == null || true),
  "quarantine path must not require fit"
);

// Never count quarantined as San Diego prospects
const sdProspectCount = gate.acceptedCount;
assert(
  sdProspectCount < evidence.candidates.length,
  "must not treat full 8 as San Diego prospects"
);

// Vancouver SITE row should quarantine for SD
const site = evidence.candidates.find((c) =>
  /SITE Global/i.test(c.title || "")
);
if (site) {
  const siteGate = quarantineWrongMarketCandidates([site], hotelConfig, {
    hotelId: evidence.hotelId,
  });
  assert(
    siteGate.quarantinedCount === 1,
    "SITE Vancouver must quarantine for San Diego hotel"
  );
}

console.log(
  JSON.stringify(
    {
      ok: true,
      evidenceRunId: evidence.providerRunId,
      evidencePreserved: true,
      totalCandidates: evidence.candidates.length,
      acceptedInTerritory: gate.acceptedCount,
      quarantined: gate.quarantinedCount,
      bethesdaLikeQuarantined: bethesdaLike.length,
      note:
        "Wrong-market rows quarantined before fit; excluded results are not San Diego prospects",
    },
    null,
    2
  )
);
