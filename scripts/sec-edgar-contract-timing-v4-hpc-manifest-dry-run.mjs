#!/usr/bin/env node
/**
 * Materialize founder-reviewable HPC creation dry-run manifest from US pilot selection.
 * Never creates HPC records. Never claims CREATE authorization.
 *
 *   node scripts/sec-edgar-contract-timing-v4-hpc-manifest-dry-run.mjs
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports/sec-edgar-contract-timing-v4");
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
      key === "verified"
        ? [...(j.verified || []), ...(j.probable || [])]
        : j[key] || [];
    for (const f of rows) {
      const id = f.findingId || f.id || f.evidenceId;
      if (id) byId.set(id, f);
    }
  }
  return byId;
}

function main() {
  if (!fs.existsSync(SRC)) {
    throw new Error(`Missing ${SRC}`);
  }
  const selection = JSON.parse(fs.readFileSync(SRC, "utf8"));
  const findings = loadFindingsIndex();
  const shortlist = selection.shortlist || [];

  const candidates = shortlist.map((row, idx) => {
    const relatedFindings = (row.findingIds || [])
      .map((id) => findings.get(id))
      .filter(Boolean);
    const sourceUrls = [
      ...new Set(
        relatedFindings
          .flatMap((f) => [f.sourceUrl, f.filingUrl, f.url, f.primarySourceUrl])
          .filter(Boolean),
      ),
    ].slice(0, 5);
    const accessions = [
      ...new Set(
        relatedFindings
          .flatMap((f) => [f.secAccession, f.accessionNumber, f.accession])
          .filter(Boolean),
      ),
    ].slice(0, 5);
    const milestones = relatedFindings
      .map((f) => ({
        date: f.contractDate || f.milestoneDate || f.date || null,
        meaning: f.dateMeaning || f.DATE_MEANING || null,
        agreementType: f.agreementType || f.contractType || null,
        statusClass: f.currentStatusClass || f.CONTRACT_CURRENT_STATUS || null,
      }))
      .filter((m) => m.date);

    const disposition = row.disposition;
    const approvalDisposition =
      disposition === "CREATE_READY"
        ? "PENDING_FOUNDER_CREATE_APPROVAL"
        : disposition === "LINK_EXISTING"
          ? "PENDING_FOUNDER_LINK_APPROVAL"
          : disposition === "RESEARCH_REQUIRED"
            ? "HOLD_RESEARCH"
            : disposition === "REJECT_AMBIGUOUS"
              ? "REJECT"
              : "HOLD";

    return {
      candidateId: `hpc_pilot_${String(idx + 1).padStart(3, "0")}`,
      discoveryRecordIds: row.findingIds || [],
      discoveryPropertyKey: row.propertyKey || null,
      proposedCanonicalHotelName: row.hotelName,
      physicalAddressLocation: {
        address: row.addressHint || null,
        city: row.city || null,
        stateRegion: row.stateRegion || null,
        country: "United States",
      },
      currentOperatingStatusEvidence: {
        class: "UNKNOWN_REQUIRES_CORROBORATION",
        note: "SEC/CMBS disclosure is historical evidence; live operating status not independently verified in this dry-run",
      },
      identityVerificationEvidence: {
        score: row.score,
        scoreReasons: row.scoreReasons || [],
        scorePenalties: row.scorePenalties || [],
        brandFamily: row.brandFamily || null,
        brandLabel: row.brandLabel || null,
        waves: row.waves || [],
      },
      existingHpcCollisionSearch: {
        outcome: row.collision?.outcome || "NOT_RUN",
        hpcRecordId: row.collision?.hpcRecordId || null,
        candidatesReviewed: row.collision?.candidates || [],
        propertyKey: row.collision?.propertyKey || null,
      },
      intendedHpcFieldPayload: {
        "Property Name": row.hotelName,
        Country: "United States",
        City: row.city || null,
        "State / Region": row.stateRegion || null,
        "Street Address": row.addressHint || null,
        "Current Brand": row.brandLabel || null,
        "Affiliation Status": row.brandLabel ? "Branded" : "Independent / Unknown",
        "Family / Source Family": row.brandFamily || "SEC_CMBS_CONTRACT_TIMING_V4",
        "Publication State": "CUSTOMER_BLOCKED",
      },
      relatedContractsAndMilestones: {
        milestoneCount: row.milestoneCount || milestones.length,
        futureMilestoneCount: row.futureMilestoneCount || 0,
        milestones: milestones.slice(0, 5),
      },
      sourceUrls,
      filingAccessions: accessions,
      approvalDisposition,
      classification: disposition,
      publicationRestrictions: {
        publicationState: "CUSTOMER_BLOCKED",
        customerFacing: false,
        createApplyAuthorized: false,
      },
      hotelExplorerEnrichmentQueueStatus: "QUEUED_PENDING_CREATE_OR_LINK",
      founderCreateAuthorized: false,
    };
  });

  const counts = {
    LINK_EXISTING: candidates.filter((c) => c.classification === "LINK_EXISTING").length,
    CREATE_READY: candidates.filter((c) => c.classification === "CREATE_READY").length,
    RESEARCH_REQUIRED: candidates.filter((c) => c.classification === "RESEARCH_REQUIRED")
      .length,
    REJECT_AMBIGUOUS: candidates.filter((c) => c.classification === "REJECT_AMBIGUOUS")
      .length,
    HISTORICAL_ONLY: candidates.filter((c) => c.classification === "HISTORICAL_ONLY").length,
  };

  const body = {
    module: "VERIFIED_DISCOVERY_TO_HPC_CREATION_PILOT",
    version: "4.0.0-dry-run",
    status: "DRY_RUN_MANIFEST_STOP_FOR_FOUNDER_REVIEW",
    generatedAt: new Date().toISOString(),
    baseId: "appCCUsuGsE1ifoLk",
    hpcTableId: "tbl9aY5ijiuIzzWam",
    discoveryInventoryTableId: "tblx77Nmh56h4xa9z",
    sourceSelection: "US_PILOT_SELECTION_DRY_RUN.json",
    policy: {
      dryRunOnly: true,
      noHpcCreateApply: true,
      founderPerRowApprovalRequired: true,
      maxPilotCreates: 50,
      geography: "US",
      publicationStateDefault: "CUSTOMER_BLOCKED",
      queueHotelExplorerEnrichment: true,
      reusePathway: "hpc-authorized-create-reuse-v1",
      collisionModule: "hpc-live-identity-collision-v1",
    },
    selectionUniverseSummary: selection.summary || {},
    shortlistCounts: counts,
    shortlistCount: candidates.length,
    meetsTargetBandWithoutPadding:
      candidates.length >= 25 &&
      candidates.length <= 50 &&
      counts.CREATE_READY >= 25,
    candidates,
    stop: {
      hpcCreate: "REQUIRES_PER_ROW_FOUNDER_MANIFEST_APPROVAL",
      note: "This file is not an authorization record. Canonical Promotion remains blocked.",
    },
  };

  const raw = JSON.stringify(body);
  body.manifestSha256 = crypto.createHash("sha256").update(raw).digest("hex");
  fs.writeFileSync(OUT, JSON.stringify(body, null, 2));
  console.log(
    JSON.stringify(
      {
        out: OUT,
        shortlistCount: candidates.length,
        counts,
        manifestSha256: body.manifestSha256,
        createApplyAuthorized: false,
      },
      null,
      2,
    ),
  );
}

main();
