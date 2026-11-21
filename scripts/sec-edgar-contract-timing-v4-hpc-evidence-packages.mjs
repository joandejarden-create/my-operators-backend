#!/usr/bin/env node
/**
 * Exact founder-reviewable HPC creation evidence packages for CREATE_READY hotels.
 * No HPC creates. founderApproval=false on every row.
 *
 *   node scripts/sec-edgar-contract-timing-v4-hpc-evidence-packages.mjs
 *   node scripts/sec-edgar-contract-timing-v4-hpc-evidence-packages.mjs --hpc-recheck
 */
import fs from "node:fs";
import path from "node:path";
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
const IDS = JSON.parse(fs.readFileSync(path.join(OUT_DIR, "SCHEMA_TABLE_IDS.json"), "utf8"));

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

async function at(tableId, method, qs = "") {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}${qs}`, {
    method,
    headers: { Authorization: `Bearer ${token()}` },
  });
  return { ok: res.ok, json: await res.json().catch(() => ({})) };
}

async function listAll(tableId, fields = []) {
  const out = [];
  let offset = null;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    for (const f of fields) params.append("fields[]", f);
    if (offset) params.set("offset", offset);
    const r = await at(tableId, "GET", `?${params}`);
    if (!r.ok) throw new Error(JSON.stringify(r.json));
    out.push(...(r.json.records || []));
    offset = r.json.offset || null;
    await sleep(100);
  } while (offset);
  return out;
}

async function main() {
  const enriched = JSON.parse(
    fs.readFileSync(path.join(OUT_DIR, "US_PILOT_ENRICHED_MANIFEST.json"), "utf8"),
  );
  const createReady = (enriched.candidates || []).filter((c) => c.classification === "CREATE_READY");
  const pending = (enriched.candidates || []).filter(
    (c) => c.classification === "CREATE_CANDIDATE_PENDING_VERIFICATION",
  );
  const pilot = JSON.parse(
    fs.readFileSync(path.join(OUT_DIR, "RESEARCH_PILOT_APPLY.json"), "utf8"),
  );
  const pilotByHotel = new Map(
    (pilot.results || []).map((r) => [norm(r.hotelName), r]),
  );

  const findings = new Map();
  const v3 = path.join(ROOT, "reports/sec-edgar-contract-timing-v3/V3_FINDINGS_NEW.json");
  if (fs.existsSync(v3)) {
    for (const f of JSON.parse(fs.readFileSync(v3, "utf8")).findings || []) {
      findings.set(f.findingId, f);
    }
  }

  const inv = await listAll(IDS.inventoryTableId, [
    MAP_DISCOVERY_INVENTORY.discoveryPropertyId,
    MAP_DISCOVERY_INVENTORY.hotelNameNormalized,
    MAP_DISCOVERY_INVENTORY.city,
    MAP_DISCOVERY_INVENTORY.stateRegion,
    MAP_DISCOVERY_INVENTORY.address,
  ]);
  const invByHotel = new Map();
  for (const rec of inv) {
    invByHotel.set(norm(rec.fields?.[MAP_DISCOVERY_INVENTORY.hotelNameNormalized]), rec);
  }

  const contracts = await listAll(IDS.contractsTableId, [
    "Contract ID",
    "Discovery Property",
    "Scheduled Expiration Date",
    "Primary Source URL",
    "Primary SEC Accession",
    "Publication State",
  ]);
  const contractsByInv = new Map();
  for (const rec of contracts) {
    for (const id of rec.fields?.["Discovery Property"] || []) {
      contractsByInv.set(id, rec);
    }
  }

  const milestones = await listAll(IDS.milestonesTableId, [
    "Milestone ID",
    "Contract",
    "Milestone Date",
    "Milestone Type",
    "Source URL",
    "SEC Accession",
    "Publication State",
  ]);
  const msByContractRec = new Map();
  for (const rec of milestones) {
    for (const id of rec.fields?.Contract || []) {
      if (!msByContractRec.has(id)) msByContractRec.set(id, []);
      msByContractRec.get(id).push(rec);
    }
  }

  let collisionMod = null;
  if (HPC_RECHECK) {
    collisionMod = await import("../lib/hotel-census/hpc-live-identity-collision-v1.js");
  }

  const packages = [];
  for (const c of createReady) {
    const hotelNorm = norm(c.proposedCanonicalHotelName);
    const invRec = invByHotel.get(hotelNorm) || null;
    const pilotRow = pilotByHotel.get(hotelNorm) || null;
    const contractRec = invRec ? contractsByInv.get(invRec.id) || null : null;
    const msRecs = contractRec ? msByContractRec.get(contractRec.id) || [] : [];
    const msRec = msRecs[0] || null;
    const f = findings.get(c.discoveryRecordIds?.[0]);

    let collision = {
      outcome: c.existingHpcCollisionSearch?.outcome || "NOT_RUN",
      hpcRecordId: c.existingHpcCollisionSearch?.hpcRecordId || null,
      candidatesReviewed: c.existingHpcCollisionSearch?.candidatesReviewed || [],
      aliasSearchNotes: "Live HPC identity collision module",
    };
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
          outcome: live?.outcome || collision.outcome,
          hpcRecordId: live?.hpcRecordId || null,
          candidatesReviewed: (live?.candidates || []).slice(0, 8).map((x) => ({
            id: x.id || x.recordId || null,
            name: x.name || x.canonicalName || null,
          })),
          recheckedAt: new Date().toISOString(),
        };
        await sleep(100);
      } catch (err) {
        collision.recheckError = String(err.message || err).slice(0, 160);
      }
    }

    let collisionDecision = "CREATE";
    if (collision.outcome === "HPC_EXISTING_RECORD_VERIFIED") collisionDecision = "LINK";
    else if (collision.outcome === "HPC_IDENTITY_COLLISION_REQUIRES_REVIEW")
      collisionDecision = "HOLD";
    else if (collision.outcome !== "HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT")
      collisionDecision = "HOLD";

    const brandOfficial = (c.sourceUrls || []).find((u) =>
      /marriott\.com|hilton\.com|hyatt\.com|ihg\.com|choicehotels\.com/i.test(u),
    );
    const secUrl =
      contractRec?.fields?.["Primary Source URL"] ||
      (c.sourceUrls || []).find((u) => /sec\.gov/i.test(u)) ||
      null;

    const discoveryInventoryRecordId = invRec?.id || pilotRow?.inventoryRecordId || null;
    const linkedContractId = contractRec?.id || pilotRow?.contractRecordId || null;
    const linkedMilestoneId = msRec?.id || pilotRow?.milestoneRecordId || null;
    const opsEvidence = c.currentOperatingStatusEvidence || null;
    const creationEligible =
      collisionDecision === "CREATE" &&
      Boolean(discoveryInventoryRecordId) &&
      Boolean(linkedContractId) &&
      Boolean(linkedMilestoneId) &&
      Boolean(brandOfficial) &&
      opsEvidence?.independentCurrentEvidence === true &&
      opsEvidence?.class === "OPERATING";

    packages.push({
      candidateId: c.candidateId,
      discoveryInventoryRecordId,
      discoveryPropertyId:
        invRec?.fields?.[MAP_DISCOVERY_INVENTORY.discoveryPropertyId] ||
        pilotRow?.discoveryPropertyId ||
        null,
      proposedCanonicalHotelName: c.proposedCanonicalHotelName,
      exactAddress: {
        street:
          c.physicalAddressLocation?.address ||
          invRec?.fields?.[MAP_DISCOVERY_INVENTORY.address] ||
          null,
        city: c.physicalAddressLocation?.city,
        stateRegion: c.physicalAddressLocation?.stateRegion,
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
      independentOperatingStatusEvidence: c.currentOperatingStatusEvidence || null,
      currentBrandAndAffiliation: {
        brandLabel: c.identityVerificationEvidence?.brandLabel || null,
        brandFamily: c.identityVerificationEvidence?.brandFamily || null,
        affiliationStatus: c.identityVerificationEvidence?.brandLabel
          ? "Branded"
          : "Independent / Unknown",
      },
      historicalNamesAndAliases: [],
      historicalSecCmbs: {
        sourceUrl: secUrl,
        accession:
          contractRec?.fields?.["Primary SEC Accession"] ||
          (c.filingAccessions || [])[0] ||
          null,
        findingIds: c.discoveryRecordIds || [],
      },
      linkedContractAndMilestone: {
        contractAirtableRecordId: contractRec?.id || pilotRow?.contractRecordId || null,
        contractId:
          contractRec?.fields?.["Contract ID"] || pilotRow?.contractId || null,
        milestoneAirtableRecordId: msRec?.id || pilotRow?.milestoneRecordId || null,
        milestoneId: msRec?.fields?.["Milestone ID"] || pilotRow?.milestoneId || null,
        milestoneDate: msRec?.fields?.["Milestone Date"] || f?.primaryDate || null,
        milestoneType: msRec?.fields?.["Milestone Type"] || f?.dateMeaning || null,
        publicationStateContract: contractRec?.fields?.["Publication State"] || null,
        publicationStateMilestone: msRec?.fields?.["Publication State"] || null,
      },
      liveHpcMatchingAndAliasSearch: collision,
      collisionDecision,
      intendedHpcFieldPayload: {
        "Property Name": c.proposedCanonicalHotelName,
        Country: "United States",
        City: c.physicalAddressLocation?.city || null,
        "State / Region": c.physicalAddressLocation?.stateRegion || null,
        "Street Address": c.physicalAddressLocation?.address || null,
        "Current Brand": c.identityVerificationEvidence?.brandLabel || null,
        "Affiliation Status": c.identityVerificationEvidence?.brandLabel
          ? "Branded"
          : "Independent / Unknown",
        "Family / Source Family":
          c.identityVerificationEvidence?.brandFamily || "SEC_CMBS_CONTRACT_TIMING_V4",
        "Publication State": "CUSTOMER_BLOCKED",
      },
      creationEligibility: creationEligible,
      founderApproval: false,
      recommendedAction: collisionDecision,
    });
  }

  const summary = {
    CREATE: packages.filter((p) => p.collisionDecision === "CREATE").length,
    LINK: packages.filter((p) => p.collisionDecision === "LINK").length,
    HOLD: packages.filter((p) => p.collisionDecision === "HOLD").length,
    REJECT: 0,
    HISTORICAL_ONLY: 0,
    creationEligibleTrue: packages.filter((p) => p.creationEligibility).length,
    founderApproved: packages.filter((p) => p.founderApproval).length,
  };

  const body = {
    module: "V4_HPC_CREATION_EVIDENCE_PACKAGES",
    status: "FOUNDER_REVIEW_ONLY_NO_HPC_CREATE",
    generatedAt: new Date().toISOString(),
    hpcRecheck: HPC_RECHECK,
    policy: {
      noHpcCreateApply: true,
      founderApprovalRequiredPerRow: true,
      noAutoApprove: true,
      publicationState: "CUSTOMER_BLOCKED",
    },
    summary,
    pendingVerificationQueue: pending.map((p) => p.proposedCanonicalHotelName),
    packages,
    founderApprovalManifest: {
      note: "Each CREATE action independently approvable; all founderApproval=false",
      eligibleForCreateApproval: packages
        .filter((p) => p.creationEligibility && p.collisionDecision === "CREATE")
        .map((p) => ({
          candidateId: p.candidateId,
          hotelName: p.proposedCanonicalHotelName,
          discoveryInventoryRecordId: p.discoveryInventoryRecordId,
          contractId: p.linkedContractAndMilestone.contractId,
          milestoneId: p.linkedContractAndMilestone.milestoneId,
          founderApproval: false,
        })),
    },
  };

  const outJson = path.join(OUT_DIR, "HPC_CREATION_EVIDENCE_PACKAGES.json");
  const outMd = path.join(OUT_DIR, "HPC_CREATION_APPROVAL_MANIFEST.md");
  fs.writeFileSync(outJson, JSON.stringify(body, null, 2));
  fs.writeFileSync(
    outMd,
    `# HPC Creation Approval Manifest

**Status:** Review only · **No HPC creates** · All \`founderApproval=false\`

## Summary

| Class | Count |
|---|---:|
| CREATE | ${summary.CREATE} |
| LINK | ${summary.LINK} |
| HOLD | ${summary.HOLD} |
| REJECT | ${summary.REJECT} |
| HISTORICAL_ONLY | ${summary.HISTORICAL_ONLY} |
| creationEligibility=true | ${summary.creationEligibleTrue} |
| founderApproved | ${summary.founderApproved} |

## Independently approvable CREATE rows

| Candidate | Hotel | Discovery Inventory | Contract ID | Milestone ID | Eligible | Approved |
|---|---|---|---|---|---|---|
${body.founderApprovalManifest.eligibleForCreateApproval
  .map(
    (r) =>
      `| ${r.candidateId} | ${r.hotelName} | ${r.discoveryInventoryRecordId || "—"} | ${r.contractId || "—"} | ${r.milestoneId || "—"} | yes | **false** |`,
  )
  .join("\n")}

## Pending verification (research queue)

${pending.map((p) => `- ${p.proposedCanonicalHotelName}`).join("\n")}

Full evidence packages: \`HPC_CREATION_EVIDENCE_PACKAGES.json\`
`,
  );

  console.log(
    JSON.stringify(
      {
        outJson,
        outMd,
        summary,
        packageCount: packages.length,
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
