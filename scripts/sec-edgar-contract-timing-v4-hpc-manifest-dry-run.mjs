#!/usr/bin/env node
/**
 * Materialize founder-reviewable HPC creation dry-run manifest.
 * Prefers enriched manifest when present. Never pads CREATE_READY. Never creates HPC.
 *
 *   node scripts/sec-edgar-contract-timing-v4-hpc-manifest-dry-run.mjs
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT_DIR = path.join(ROOT, "reports/sec-edgar-contract-timing-v4");
const ENRICHED = path.join(OUT_DIR, "US_PILOT_ENRICHED_MANIFEST.json");
const SRC = path.join(OUT_DIR, "US_PILOT_SELECTION_DRY_RUN.json");
const OUT = path.join(OUT_DIR, "HPC_CREATION_MANIFEST_DRY_RUN.json");

function loadFindingsIndex() {
  const byId = new Map();
  const paths = [
    ["reports/sec-edgar-contract-timing-v3/V3_FINDINGS_NEW.json", "findings"],
    ["reports/sec-edgar-contract-timing-v2/V2_FINDINGS_INCREMENTAL.json", "combined"],
    ["reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json", "verified"],
  ];
  for (const [rel, key] of paths) {
    const p = path.join(ROOT, rel);
    if (!fs.existsSync(p)) continue;
    const j = JSON.parse(fs.readFileSync(p, "utf8"));
    const rows =
      key === "verified" ? [...(j.verified || []), ...(j.probable || [])] : j[key] || [];
    for (const f of rows) {
      const id = f.findingId || f.id;
      if (id) byId.set(id, f);
    }
  }
  return byId;
}

function main() {
  if (fs.existsSync(ENRICHED)) {
    const enriched = JSON.parse(fs.readFileSync(ENRICHED, "utf8"));
    const body = {
      ...enriched,
      module: "VERIFIED_DISCOVERY_TO_HPC_CREATION_PILOT",
      version: "4.0.1-enriched-dry-run",
      status: "DRY_RUN_MANIFEST_STOP_FOR_FOUNDER_REVIEW",
      source: "US_PILOT_ENRICHED_MANIFEST.json",
      shortlistCounts: enriched.enrichedCounts,
      shortlistCount: enriched.enrichedCount,
      createApplyAuthorized: false,
    };
    fs.writeFileSync(OUT, JSON.stringify(body, null, 2));
    console.log(
      JSON.stringify(
        {
          out: OUT,
          source: "enriched",
          counts: body.shortlistCounts,
          actualCreateReady: body.actualCreateReady,
          manifestSha256: body.manifestSha256,
        },
        null,
        2,
      ),
    );
    return;
  }

  if (!fs.existsSync(SRC)) throw new Error(`Missing ${SRC}`);
  const selection = JSON.parse(fs.readFileSync(SRC, "utf8"));
  const findings = loadFindingsIndex();
  const shortlist = selection.shortlist || [];

  const candidates = shortlist.map((row, idx) => {
    const related = (row.findingIds || []).map((id) => findings.get(id)).filter(Boolean);
    const sourceUrls = [
      ...new Set(
        [
          ...(row.sourceUrls || []),
          ...related.flatMap((f) => [
            f.source?.documentUrl,
            f.source?.filingUrl,
            f.sourceUrl,
          ]),
        ].filter(Boolean),
      ),
    ].slice(0, 5);
    const accessions = [
      ...new Set(
        [
          ...(row.filingAccessions || []),
          ...related.map((f) => f.source?.accessionNumber),
        ].filter(Boolean),
      ),
    ].slice(0, 5);

    return {
      candidateId: `hpc_pilot_${String(idx + 1).padStart(3, "0")}`,
      discoveryRecordIds: row.findingIds || [],
      proposedCanonicalHotelName: row.hotelName,
      physicalAddressLocation: {
        address: row.addressHint || null,
        city: row.city,
        stateRegion: row.stateRegion,
        country: "United States",
      },
      currentOperatingStatusEvidence: {
        class: row.operatingStatus || "UNKNOWN_REQUIRES_CORROBORATION",
        note: "Without enrichment pass, CREATE_READY is forbidden",
      },
      identityVerificationEvidence: {
        score: row.score,
        scoreReasons: row.scoreReasons || [],
        brandFamily: row.brandFamily,
        brandLabel: row.brandLabel,
        waves: row.waves,
      },
      existingHpcCollisionSearch: {
        outcome: row.collision?.outcome || "NOT_RUN",
        hpcRecordId: row.collision?.hpcRecordId || null,
        candidatesReviewed:
          row.collision?.candidatesReviewed || row.collision?.candidates || [],
      },
      sourceUrls,
      filingAccessions: accessions,
      classification: row.disposition,
      publicationRestrictions: {
        publicationState: "CUSTOMER_BLOCKED",
        createApplyAuthorized: false,
      },
      founderCreateAuthorized: false,
    };
  });

  const counts = {
    LINK_EXISTING: candidates.filter((c) => c.classification === "LINK_EXISTING").length,
    CREATE_READY: candidates.filter((c) => c.classification === "CREATE_READY").length,
    CREATE_CANDIDATE_PENDING_VERIFICATION: candidates.filter(
      (c) => c.classification === "CREATE_CANDIDATE_PENDING_VERIFICATION",
    ).length,
    RESEARCH_REQUIRED: candidates.filter((c) => c.classification === "RESEARCH_REQUIRED")
      .length,
    REJECT_AMBIGUOUS: candidates.filter((c) => c.classification === "REJECT_AMBIGUOUS")
      .length,
    HISTORICAL_ONLY: candidates.filter((c) => c.classification === "HISTORICAL_ONLY").length,
  };

  const body = {
    module: "VERIFIED_DISCOVERY_TO_HPC_CREATION_PILOT",
    version: "4.0.1-dry-run",
    status: "DRY_RUN_MANIFEST_STOP_FOR_FOUNDER_REVIEW",
    generatedAt: new Date().toISOString(),
    baseId: "appCCUsuGsE1ifoLk",
    policy: {
      dryRunOnly: true,
      noHpcCreateApply: true,
      doNotPadCreateReady: true,
      createReadyRequiresIndependentCorroboration: true,
    },
    selectionUniverseSummary: selection.summary || {},
    shortlistCounts: counts,
    shortlistCount: candidates.length,
    actualCreateReady: counts.CREATE_READY,
    meetsCreateReadyBandWithoutPadding: false,
    note:
      counts.CREATE_READY < 25
        ? `Only ${counts.CREATE_READY} CREATE_READY — run enrich script; do not pad`
        : "CREATE_READY band met",
    candidates,
    stop: { hpcCreate: "REQUIRES_PER_ROW_FOUNDER_MANIFEST_APPROVAL" },
  };
  body.manifestSha256 = crypto
    .createHash("sha256")
    .update(JSON.stringify(body.candidates))
    .digest("hex");
  fs.writeFileSync(OUT, JSON.stringify(body, null, 2));
  console.log(
    JSON.stringify(
      { out: OUT, source: "selection", counts, actualCreateReady: counts.CREATE_READY },
      null,
      2,
    ),
  );
}

main();
