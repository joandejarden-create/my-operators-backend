#!/usr/bin/env node
/**
 * Exact founder-reviewable HPC creation evidence packages for CREATE_READY hotels.
 * No HPC creates. founderApproval.approvedForCreate = false on every row.
 *
 *   node scripts/sec-edgar-contract-timing-v4-hpc-creation-evidence-packages.mjs
 *   node scripts/sec-edgar-contract-timing-v4-hpc-creation-evidence-packages.mjs --hpc-recheck
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  MAP_DISCOVERY_INVENTORY,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT_DIR = path.join(ROOT, "reports/sec-edgar-contract-timing-v4");
const HPC_RECHECK = process.argv.includes("--hpc-recheck");

function token() {
  return (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
async function at(tableId, method, body, qs = "") {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}${qs}`, {
    method,
    headers: {
      Authorization: `Bearer ${token()}`,
      ...(body ? { "Content-Type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  return { ok: res.ok, status: res.status, json: await res.json().catch(() => ({})) };
}
async function listAll(tableId, fields = []) {
  const out = [];
  let offset = null;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const r = await at(tableId, "GET", null, `?${params}`);
    if (!r.ok) throw new Error(JSON.stringify(r.json));
    out.push(...(r.json.records || []));
    offset = r.json.offset || null;
    await sleep(100);
  } while (offset);
  return out;
}

async function main() {
  const ids = JSON.parse(fs.readFileSync(path.join(OUT_DIR, "SCHEMA_TABLE_IDS.json"), "utf8"));
  const enriched = JSON.parse(fs.readFileSync(path.join(OUT_DIR, "US_PILOT_ENRICHED_MANIFEST.json"), "utf8"));
  const review = JSON.parse(fs.readFileSync(path.join(OUT_DIR, "CREATE_READY_FOUNDER_REVIEW.json"), "utf8"));

  const createReady = (enriched.candidates || []).filter((c) => c.classification === "CREATE_READY");
  const pending = (enriched.candidates || []).filter(
    (c) => c.classification === "CREATE_CANDIDATE_PENDING_VERIFICATION",
  );

  const findings = new Map();
  const v3p = path.join(ROOT, "reports/sec-edgar-contract-timing-v3/V3_FINDINGS_NEW.json");
  if (fs.existsSync(v3p)) {
    for (const f of JSON.parse(fs.readFileSync(v3p, "utf8")).findings || []) {
      findings.set(f.findingId, f);
    }
  }

  console.log("Loading live inventory/contracts/milestones…");
  const inv = await listAll(ids.inventoryTableId, [
    MAP_DISCOVERY_INVENTORY.discoveryPropertyId,
    MAP_DISCOVERY_INVENTORY.hotelNameNormalized,
    MAP_DISCOVERY_INVENTORY.city,
    MAP_DISCOVERY_INVENTORY.stateRegion,
    MAP_DISCOVERY_INVENTORY.address,
  ]);
  const contracts = await listAll(ids.contractsTableId, [
    "Contract ID",
    "Discovery Property",
    "Scheduled Expiration Date",
    "Primary Source URL",
    "Primary SEC Accession",
    "Agreement Type",
  ]);
  const milestones = await listAll(ids.milestonesTableId, [
    "Milestone ID",
    "Contract",
    "Milestone Date",
    "Milestone Type",
    "Source URL",
    "SEC Accession",
  ]);

  const contractsByInv = new Map();
  for (const c of contracts) {
    for (const invId of c.fields["Discovery Property"] || []) {
      if (!contractsByInv.has(invId)) contractsByInv.set(invId, []);
      contractsByInv.get(invId).push(c);
    }
  }
  // Prefer inventory rows that already have linked contracts (pilot/migration identity)
  const invByNameCity = new Map();
  for (const r of inv) {
    const key = [
      norm(r.fields[MAP_DISCOVERY_INVENTORY.hotelNameNormalized]),
      norm(r.fields[MAP_DISCOVERY_INVENTORY.city]),
      norm(r.fields[MAP_DISCOVERY_INVENTORY.stateRegion]),
    ].join("|");
    const existing = invByNameCity.get(key);
    const hasContracts = (contractsByInv.get(r.id) || []).length > 0;
    const existingHas = existing ? (contractsByInv.get(existing.id) || []).length > 0 : false;
    if (!existing || (hasContracts && !existingHas)) invByNameCity.set(key, r);
  }
  const milestonesByContractRec = new Map();
  for (const m of milestones) {
    for (const cid of m.fields.Contract || []) {
      if (!milestonesByContractRec.has(cid)) milestonesByContractRec.set(cid, []);
      milestonesByContractRec.get(cid).push(m);
    }
  }

  let collisionMod = null;
  if (HPC_RECHECK) {
    collisionMod = await import("../lib/hotel-census/hpc-live-identity-collision-v1.js");
  }

  const packages = [];
  for (const c of createReady) {
    const f = findings.get(c.discoveryRecordIds?.[0]);
    const key = [
      norm(c.proposedCanonicalHotelName),
      norm(c.physicalAddressLocation?.city),
      norm(c.physicalAddressLocation?.stateRegion),
    ].join("|");
    const invRec = invByNameCity.get(key) || null;
    const linkedContracts = invRec ? contractsByInv.get(invRec.id) || [] : [];
    const contractRec = linkedContracts[0] || null;
    const linkedMs = contractRec ? milestonesByContractRec.get(contractRec.id) || [] : [];
    const msRec = linkedMs[0] || null;

    let collision = c.existingHpcCollisionSearch || {};
    if (HPC_RECHECK && collisionMod) {
      try {
        const live = await collisionMod.performLiveHpcIdentityCollisionCheck({
          hotelName: c.proposedCanonicalHotelName,
          aliases: [],
          city: c.physicalAddressLocation?.city,
          country: "United States",
          brand: c.identityVerificationEvidence?.brandLabel || undefined,
        });
        collision = {
          outcome: live?.outcome,
          hpcRecordId: live?.hpcRecordId || null,
          candidatesReviewed: (live?.candidates || []).slice(0, 8).map((x) => ({
            id: x.id || x.recordId || null,
            name: x.name || x.canonicalName || null,
          })),
          recheckedAt: new Date().toISOString(),
        };
        await sleep(100);
      } catch (err) {
        collision = { ...collision, recheckError: String(err.message || err).slice(0, 160) };
      }
    }

    let collisionDecision = "CREATE_ELIGIBLE_VERIFIED_ABSENT";
    let creationEligibility = "CREATE_ELIGIBLE";
    let summaryRecommendation = "CREATE";
    if (collision.outcome === "HPC_EXISTING_RECORD_VERIFIED") {
      collisionDecision = "LINK_EXISTING";
      creationEligibility = "LINK_ONLY";
      summaryRecommendation = "LINK";
    } else if (collision.outcome === "HPC_IDENTITY_COLLISION_REQUIRES_REVIEW") {
      collisionDecision = "HOLD_COLLISION";
      creationEligibility = "HOLD";
      summaryRecommendation = "HOLD";
    } else if (collision.outcome !== "HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT") {
      collisionDecision = "HOLD_COLLISION_INCOMPLETE";
      creationEligibility = "HOLD";
      summaryRecommendation = "HOLD";
    }

    const brandOfficial = (c.sourceUrls || []).find((u) =>
      /marriott\.com|hilton\.com|hyatt\.com|ihg\.com|wyndhamhotels|choicehotels/i.test(u),
    );
    const secUrl =
      (c.sourceUrls || []).find((u) => /sec\.gov/i.test(u)) ||
      contractRec?.fields?.["Primary Source URL"] ||
      null;

    const intendedHpcFieldPayload = {
      "Property Name": c.proposedCanonicalHotelName,
      Country: "United States",
      City: c.physicalAddressLocation?.city || null,
      "State / Region": c.physicalAddressLocation?.stateRegion || null,
      "Street Address": c.physicalAddressLocation?.address || null,
      "Current Brand": c.identityVerificationEvidence?.brandLabel || null,
      "Affiliation Status": c.identityVerificationEvidence?.brandLabel ? "Branded" : "Independent / Unknown",
      "Family / Source Family":
        c.identityVerificationEvidence?.brandFamily || "SEC_CMBS_CONTRACT_TIMING_V4",
      "Publication State": "CUSTOMER_BLOCKED",
    };

    const pkg = {
      packageId: c.candidateId,
      discoveryInventoryRecordId: invRec?.id || null,
      discoveryPropertyId: invRec?.fields?.[MAP_DISCOVERY_INVENTORY.discoveryPropertyId] || null,
      researchFindingIds: c.discoveryRecordIds || [],
      canonicalProposedHotelName: c.proposedCanonicalHotelName,
      exactAddress: {
        street: c.physicalAddressLocation?.address || null,
        city: c.physicalAddressLocation?.city || null,
        stateRegion: c.physicalAddressLocation?.stateRegion || null,
        country: "United States",
        formatted: [
          c.physicalAddressLocation?.address,
          c.physicalAddressLocation?.city,
          c.physicalAddressLocation?.stateRegion,
          "United States",
        ]
          .filter(Boolean)
          .join(", "),
      },
      officialCurrentHotelBrandUrl: brandOfficial || null,
      independentOperatingStatusEvidence: {
        class: c.currentOperatingStatusEvidence?.class || null,
        independentCurrentEvidence: c.currentOperatingStatusEvidence?.independentCurrentEvidence || false,
        sources: c.currentOperatingStatusEvidence?.sources || [],
        note: c.currentOperatingStatusEvidence?.note || null,
      },
      currentBrandAndAffiliation: {
        brandLabel: c.identityVerificationEvidence?.brandLabel || null,
        brandFamily: c.identityVerificationEvidence?.brandFamily || null,
      },
      historicalNamesAndAliases: f?.notes ? [String(f.notes).slice(0, 200)] : [],
      historicalSecCmbs: {
        sourceUrl: secUrl,
        accession:
          (c.filingAccessions || [])[0] ||
          contractRec?.fields?.["Primary SEC Accession"] ||
          null,
      },
      linkedContractAndMilestoneIds: {
        contractAirtableRecordId: contractRec?.id || null,
        contractId: contractRec?.fields?.["Contract ID"] || null,
        milestoneAirtableRecordId: msRec?.id || null,
        milestoneId: msRec?.fields?.["Milestone ID"] || null,
        milestoneDate: msRec?.fields?.["Milestone Date"] || null,
        agreementType: contractRec?.fields?.["Agreement Type"] || null,
      },
      liveHpcMatchingAndAliasSearch: collision,
      exactIntendedHpcFieldPayload: intendedHpcFieldPayload,
      collisionDecision,
      creationEligibility,
      summaryRecommendation,
      founderApproval: {
        approvedForCreate: false,
        approvedForLink: false,
        approvedBy: null,
        approvedAt: null,
        note: "Must be set explicitly by founder before Canonical Promotion",
      },
      publicationRestrictions: {
        publicationState: "CUSTOMER_BLOCKED",
        hpcCreateAuthorized: false,
      },
    };
    packages.push(pkg);
  }

  const summary = {
    CREATE: packages.filter((p) => p.summaryRecommendation === "CREATE").length,
    LINK: packages.filter((p) => p.summaryRecommendation === "LINK").length,
    HOLD: packages.filter((p) => p.summaryRecommendation === "HOLD").length,
    REJECT_AMBIGUOUS: 0,
    HISTORICAL_ONLY: 0,
    PENDING_VERIFICATION_QUEUE: pending.length,
  };

  const body = {
    module: "V4_HPC_CREATION_EVIDENCE_PACKAGES",
    version: "4.0.2",
    status: "FOUNDER_REVIEW_ONLY_NO_HPC_CREATE",
    generatedAt: new Date().toISOString(),
    baseId: GRAPH_BASE_ID_CANONICAL,
    hpcTableId: ids.hpcTableId,
    hpcRecheck: HPC_RECHECK,
    packageCount: packages.length,
    summaryCounts: summary,
    pendingVerificationHotels: pending.map((p) => p.proposedCanonicalHotelName),
    packages,
    stop: {
      hpcCreate: "REQUIRES_PER_ROW_FOUNDER_APPROVAL_ON_THIS_MANIFEST",
      canonicalPromotion: "REQUIRES_HIF_APPLY_PROMOTION_AND_AUTH_BINDING",
    },
  };
  body.manifestSha256 = crypto
    .createHash("sha256")
    .update(JSON.stringify(body.packages))
    .digest("hex");

  // Approval manifest — same packages, explicit unapproved flags
  const approvalManifest = {
    module: "V4_HPC_FOUNDER_APPROVAL_MANIFEST",
    status: "AWAITING_FOUNDER_PER_ROW_APPROVAL",
    generatedAt: new Date().toISOString(),
    manifestSha256: body.manifestSha256,
    baseId: GRAPH_BASE_ID_CANONICAL,
    hpcTableId: ids.hpcTableId,
    maxBatchSize: packages.length,
    allowedActions: ["CREATE_HPC", "LINK_EXISTING"],
    summaryCounts: summary,
    rows: packages.map((p) => ({
      packageId: p.packageId,
      discoveryInventoryRecordId: p.discoveryInventoryRecordId,
      canonicalProposedHotelName: p.canonicalProposedHotelName,
      exactAddress: p.exactAddress.formatted,
      creationEligibility: p.creationEligibility,
      summaryRecommendation: p.summaryRecommendation,
      contractId: p.linkedContractAndMilestoneIds.contractId,
      milestoneId: p.linkedContractAndMilestoneIds.milestoneId,
      collisionOutcome: p.liveHpcMatchingAndAliasSearch?.outcome || null,
      intendedHpcFieldPayload: p.exactIntendedHpcFieldPayload,
      founderApproval: {
        approvedForCreate: false,
        approvedForLink: false,
      },
    })),
    instructions: [
      "Set founderApproval.approvedForCreate=true only for exact rows to create",
      "Bind Canonical Promotion to this manifestSha256 + HIF_APPLY_PROMOTION=1",
      "No row is auto-approved",
    ],
  };

  fs.writeFileSync(
    path.join(OUT_DIR, "HPC_CREATION_EVIDENCE_PACKAGES.json"),
    JSON.stringify(body, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "HPC_FOUNDER_APPROVAL_MANIFEST.json"),
    JSON.stringify(approvalManifest, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "HPC_FOUNDER_APPROVAL_MANIFEST.md"),
    `# HPC Founder Approval Manifest

**Status:** Awaiting per-row founder approval · **No HPC creates authorized**  
**Manifest SHA-256:** \`${body.manifestSha256}\`

## Summary

| Disposition | Count |
|---|---:|
| CREATE eligible | ${summary.CREATE} |
| LINK existing HPC | ${summary.LINK} |
| HOLD | ${summary.HOLD} |
| REJECT ambiguous | ${summary.REJECT_AMBIGUOUS} |
| Historical-only | ${summary.HISTORICAL_ONLY} |
| Pending verification (queue) | ${summary.PENDING_VERIFICATION_QUEUE} |

## Rows (all \`approvedForCreate: false\`)

| # | Hotel | Address | Eligibility | Contract ID | Milestone ID |
|---:|---|---|---|---|---|
${packages
  .map(
    (p, i) =>
      `| ${i + 1} | ${p.canonicalProposedHotelName} | ${p.exactAddress.formatted} | ${p.creationEligibility} | ${p.linkedContractAndMilestoneIds.contractId || "—"} | ${p.linkedContractAndMilestoneIds.milestoneId || "—"} |`,
  )
  .join("\n")}

Pending verification queue remains research-only (not in this CREATE manifest).
`,
  );

  console.log(
    JSON.stringify(
      {
        packages: packages.length,
        summary,
        manifestSha256: body.manifestSha256,
        withDiscoveryLink: packages.filter((p) => p.discoveryInventoryRecordId).length,
        withContractLink: packages.filter((p) => p.linkedContractAndMilestoneIds.contractId)
          .length,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
