#!/usr/bin/env node
/**
 * V4 verified historical contract migration (Research Mode only).
 *
 *   node scripts/sec-edgar-contract-timing-v4-verified-migration.mjs --dry-run
 *   HIF_APPLY_RESEARCH=1 node scripts/sec-edgar-contract-timing-v4-verified-migration.mjs --apply --batch-size=50
 *   HIF_APPLY_RESEARCH=1 node scripts/sec-edgar-contract-timing-v4-verified-migration.mjs --apply --idempotency-rerun
 *
 * Reconciles against live contracts by identity (name|city|state|type) AND Contract ID.
 * Max 50 agreements per batch. No HPC creates. CUSTOMER_BLOCKED only.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  DISCOVERY_SCHEMA_VERSION,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT_DIR = path.join(ROOT, "reports/sec-edgar-contract-timing-v4");
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY || process.argv.includes("--dry-run");
const IDEMPOTENCY = process.argv.includes("--idempotency-rerun");
const BATCH_SIZE = Math.min(
  50,
  Number((process.argv.find((a) => a.startsWith("--batch-size=")) || "").split("=")[1]) || 50,
);
const BATCH_INDEX = Math.max(
  0,
  Number((process.argv.find((a) => a.startsWith("--batch=")) || "").split("=")[1]) || 0,
);
const MAX_BATCHES = Number(
  (process.argv.find((a) => a.startsWith("--max-batches=")) || "").split("=")[1] || 0,
); // 0 = all remaining

function token() {
  return (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function sha(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex").slice(0, 16);
}
function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
function identityKey(a) {
  return [norm(a.hotelName), norm(a.city), norm(a.stateRegion), String(a.agreementType || "FRANCHISE").toUpperCase()].join("|");
}
function scrub(fields) {
  for (const k of Object.keys(fields)) {
    if (fields[k] == null || fields[k] === "") delete fields[k];
  }
  return fields;
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
    if (!r.ok) throw new Error(`LIST_FAIL ${tableId}: ${JSON.stringify(r.json)}`);
    out.push(...(r.json.records || []));
    offset = r.json.offset || null;
    await sleep(120);
  } while (offset);
  return out;
}

function loadFindings() {
  const byId = new Map();
  for (const [rel, key] of [
    ["reports/sec-edgar-contract-timing-v3/V3_FINDINGS_NEW.json", "findings"],
    ["reports/sec-edgar-contract-timing-v2/V2_FINDINGS_INCREMENTAL.json", "combined"],
    ["reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json", "verified"],
  ]) {
    const p = path.join(ROOT, rel);
    if (!fs.existsSync(p)) continue;
    const j = JSON.parse(fs.readFileSync(p, "utf8"));
    const rows =
      key === "verified" ? [...(j.verified || []), ...(j.probable || [])] : j[key] || [];
    for (const f of rows) if (f.findingId) byId.set(f.findingId, f);
  }
  return byId;
}

function milestoneTypeFromMeaning(meaning) {
  const m = String(meaning || "").toUpperCase();
  if (m.includes("MANAGEMENT")) return "MANAGEMENT_EXPIRATION";
  if (m.includes("FRANCHISE")) return "FRANCHISE_EXPIRATION";
  if (m.includes("COMMENCEMENT")) return "COMMENCEMENT";
  if (m.includes("RENEWAL")) return "RENEWAL";
  if (m.includes("EXTENSION")) return "EXTENSION";
  if (m.includes("TERMINATION")) return "TERMINATION";
  return "OTHER";
}

async function main() {
  if (APPLY && String(process.env.HIF_APPLY_RESEARCH || "") !== "1") {
    console.log(JSON.stringify({ ok: false, error: "HIF_APPLY_RESEARCH_ENV_REQUIRED" }));
    process.exitCode = 2;
    return;
  }
  if (!token()) {
    console.log(JSON.stringify({ ok: false, error: "NO_AIRTABLE_TOKEN" }));
    process.exitCode = 1;
    return;
  }

  const ids = JSON.parse(fs.readFileSync(path.join(OUT_DIR, "SCHEMA_TABLE_IDS.json"), "utf8"));
  const audit = JSON.parse(fs.readFileSync(path.join(OUT_DIR, "MIGRATION_QUALITY_AUDIT.json"), "utf8"));
  const findings = loadFindings();

  const verifiedAgreements = (audit.agreements || []).filter((a) => a.quality === "VERIFIED");
  const milestonesByContract = new Map();
  for (const m of audit.milestones || []) {
    if (m.quality !== "VERIFIED") continue;
    if (!milestonesByContract.has(m.contractId)) milestonesByContract.set(m.contractId, []);
    milestonesByContract.get(m.contractId).push(m);
  }

  // Quarantine ledger (no verified contract writes)
  const quarantine = {
    RESEARCH_ONLY: (audit.agreements || []).filter((a) => a.quality === "RESEARCH_ONLY"),
    CONFLICTED: (audit.agreements || []).filter((a) => a.quality === "CONFLICTED"),
    REJECTED_FINDINGS: audit.excluded || [],
  };
  fs.writeFileSync(
    path.join(OUT_DIR, "MIGRATION_QUARANTINE_LEDGER.json"),
    JSON.stringify(
      {
        at: new Date().toISOString(),
        note: "Not persisted as VERIFIED contracts. Retained for research/quarantine only.",
        counts: {
          RESEARCH_ONLY: quarantine.RESEARCH_ONLY.length,
          CONFLICTED: quarantine.CONFLICTED.length,
          REJECTED_FINDINGS: quarantine.REJECTED_FINDINGS.length,
        },
        RESEARCH_ONLY: quarantine.RESEARCH_ONLY.slice(0, 50),
        CONFLICTED: quarantine.CONFLICTED,
        REJECTED_FINDINGS: quarantine.REJECTED_FINDINGS,
      },
      null,
      2,
    ),
  );

  console.log("Listing live contracts/milestones for reconciliation…");
  const liveContracts = await listAll(ids.contractsTableId, [
    "Contract ID",
    "Hotel City",
    "Hotel State / Region",
    "Agreement Type",
    "Notes / Quarantine",
    "Publication State",
    "Discovery Property",
  ]);
  // Need hotel name — not a field on contracts; use Notes or Discovery Property. Fetch inventory names.
  const liveInv = await listAll(ids.inventoryTableId, [
    MAP_DISCOVERY_INVENTORY.discoveryPropertyId,
    MAP_DISCOVERY_INVENTORY.hotelNameNormalized,
    MAP_DISCOVERY_INVENTORY.city,
    MAP_DISCOVERY_INVENTORY.stateRegion,
  ]);
  const invByRecId = new Map(liveInv.map((r) => [r.id, r]));
  const liveMilestones = await listAll(ids.milestonesTableId, [
    "Milestone ID",
    "Contract",
    "Publication State",
    "Milestone Date",
  ]);

  const byContractId = new Map();
  const byIdentity = new Map();
  for (const rec of liveContracts) {
    const cid = rec.fields["Contract ID"];
    if (cid) byContractId.set(cid, rec);
    const discIds = rec.fields["Discovery Property"] || [];
    const inv = discIds[0] ? invByRecId.get(discIds[0]) : null;
    const hotelName = inv?.fields?.[MAP_DISCOVERY_INVENTORY.hotelNameNormalized] || null;
    const city =
      rec.fields["Hotel City"] || inv?.fields?.[MAP_DISCOVERY_INVENTORY.city] || null;
    const state =
      rec.fields["Hotel State / Region"] ||
      inv?.fields?.[MAP_DISCOVERY_INVENTORY.stateRegion] ||
      null;
    const type = rec.fields["Agreement Type"] || "FRANCHISE";
    if (hotelName) {
      const ik = identityKey({ hotelName, city, stateRegion: state, agreementType: type });
      if (!byIdentity.has(ik)) byIdentity.set(ik, rec);
    }
  }
  const byMilestoneId = new Map(
    liveMilestones.filter((r) => r.fields["Milestone ID"]).map((r) => [r.fields["Milestone ID"], r]),
  );

  // Determine remaining VERIFIED not yet persisted (by identity or audit contractId)
  const remaining = [];
  const alreadyMatched = [];
  for (const a of verifiedAgreements) {
    const ik = identityKey(a);
    const live = byContractId.get(a.contractId) || byIdentity.get(ik) || null;
    if (live) {
      alreadyMatched.push({
        auditContractId: a.contractId,
        liveContractId: live.fields["Contract ID"],
        liveRecordId: live.id,
        hotelName: a.hotelName,
        match: byContractId.has(a.contractId) ? "CONTRACT_ID" : "IDENTITY",
      });
    } else {
      remaining.push(a);
    }
  }

  const start = BATCH_INDEX * BATCH_SIZE;
  let batch = remaining.slice(start, start + BATCH_SIZE);
  if (MAX_BATCHES > 0) {
    // when running multi-batch loop externally; here single batch slice
  }
  // Process ALL remaining in sequential batches within one run when --apply --all
  const processAll = process.argv.includes("--all") || (!process.argv.some((a) => a.startsWith("--batch=")) && APPLY);
  const batches = [];
  if (processAll || DRY) {
    for (let i = 0; i < remaining.length; i += BATCH_SIZE) {
      batches.push(remaining.slice(i, i + BATCH_SIZE));
      if (MAX_BATCHES > 0 && batches.length >= MAX_BATCHES) break;
    }
  } else {
    batches.push(batch);
  }

  const checkpointPath = path.join(OUT_DIR, "VERIFIED_MIGRATION_CHECKPOINT.json");
  const checkpoint = fs.existsSync(checkpointPath)
    ? JSON.parse(fs.readFileSync(checkpointPath, "utf8"))
    : { completedContractIds: [], batches: [] };

  const allResults = [];
  let createdContracts = 0;
  let reusedContracts = 0;
  let createdMilestones = 0;
  let reusedMilestones = 0;
  let createdFindings = 0;
  let readBackOk = 0;
  let readBackFail = 0;
  let conflicted = 0;
  let rejected = 0;

  async function findInv(propertyId) {
    const formula = `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}='${propertyId}'`;
    const r = await at(
      ids.inventoryTableId,
      "GET",
      null,
      `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`,
    );
    return r.json.records?.[0] || null;
  }

  for (let bi = 0; bi < batches.length; bi++) {
    const current = batches[bi];
    console.log(
      JSON.stringify({
        batch: bi + (processAll ? 0 : BATCH_INDEX),
        size: current.length,
        remainingTotal: remaining.length,
        alreadyMatched: alreadyMatched.length,
        dryRun: DRY,
      }),
    );

    for (const a of current) {
      const ik = identityKey(a);
      // Re-check live (idempotency / race)
      let live = byContractId.get(a.contractId) || byIdentity.get(ik) || null;
      const msList = milestonesByContract.get(a.contractId) || [];
      const primaryMs = msList[0] || null;
      const findingId = (a.findingIds || [])[0];
      const f = findingId ? findings.get(findingId) : null;
      const sourceUrl = (a.sourceUrls || [])[0] || f?.source?.documentUrl || null;
      const accession = (a.sourceAccessions || [])[0] || f?.source?.accessionNumber || null;
      const primaryDate = primaryMs?.primaryDate || f?.primaryDate || null;
      const dateMeaning = primaryMs?.dateMeaning || f?.dateMeaning || "FRANCHISE_EXPIRATION";

      const row = {
        hotelName: a.hotelName,
        auditContractId: a.contractId,
        identityKey: ik,
        actions: [],
        changeClass: "NEW",
      };

      if (!sourceUrl || !accession || !primaryDate) {
        row.actions.push("REJECTED_INSUFFICIENT_EVIDENCE");
        rejected += 1;
        allResults.push(row);
        continue;
      }

      if (DRY) {
        row.actions.push(live ? "WOULD_REUSE" : "WOULD_CREATE");
        row.changeClass = live ? "UNCHANGED" : "NEW";
        if (live) reusedContracts += 1;
        else createdContracts += 1;
        allResults.push(row);
        continue;
      }

      const propertyKey = `${norm(a.hotelName)}|${norm(a.city)}|${norm(a.stateRegion)}`;
      const discoveryPropertyId = `disc_ctim_v4_${sha(propertyKey)}`;

      let invRec = await findInv(discoveryPropertyId);
      if (!invRec) {
        const fields = scrub({
          [MAP_DISCOVERY_INVENTORY.discoveryPropertyId]: discoveryPropertyId,
          [MAP_DISCOVERY_INVENTORY.hotelNameNormalized]: a.hotelName,
          [MAP_DISCOVERY_INVENTORY.hotelNameRaw]: a.hotelName,
          [MAP_DISCOVERY_INVENTORY.city]: a.city,
          [MAP_DISCOVERY_INVENTORY.stateRegion]: a.stateRegion,
          [MAP_DISCOVERY_INVENTORY.country]: "United States",
          [MAP_DISCOVERY_INVENTORY.brandFamily]: a.brandFamily || null,
          [MAP_DISCOVERY_INVENTORY.identityClass]: "PROBABLE_PROPERTY",
          [MAP_DISCOVERY_INVENTORY.lifecycleStatus]: "DISCOVERY_PERSISTED",
          [MAP_DISCOVERY_INVENTORY.persistenceStatus]: "RESEARCH_ONLY",
          [MAP_DISCOVERY_INVENTORY.publicationState]: "CUSTOMER_BLOCKED",
          [MAP_DISCOVERY_INVENTORY.sourceFilingUrl]: sourceUrl,
          [MAP_DISCOVERY_INVENTORY.sourceAccessionPrimary]: accession,
          [MAP_DISCOVERY_INVENTORY.schemaVersion]: "4.0.0",
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "DISCOVERED_NOT_IN_HPC",
          [MAP_DISCOVERY_INVENTORY.evidenceConfidence]: "MEDIUM",
          [MAP_DISCOVERY_INVENTORY.discoveryMethod]: "SEC_CMBS_VERIFIED_MIGRATION_V4",
          [MAP_DISCOVERY_INVENTORY.propertyKey]: propertyKey,
          [MAP_DISCOVERY_INVENTORY.notesQuarantine]: `V4 verified migration · ${a.contractId}`,
          "Geographic Processing Class": "PRIORITY_HPC_ENRICHMENT",
          "Continent / Macro Region": "NORTH_AMERICA",
        });
        const created = await at(ids.inventoryTableId, "POST", { fields, typecast: true });
        if (!created.ok) {
          row.actions.push("INVENTORY_FAIL");
          row.error = created.json;
          allResults.push(row);
          process.exitCode = 3;
          break;
        }
        invRec = created.json;
        row.actions.push("INVENTORY_CREATED");
        liveInv.push(invRec);
        invByRecId.set(invRec.id, invRec);
      } else {
        row.actions.push("INVENTORY_REUSED");
      }
      await sleep(140);

      // Re-fetch live match after inventory (identity)
      live = byContractId.get(a.contractId) || byIdentity.get(ik) || null;

      let contractRec = live;
      if (!contractRec) {
        // Also check by audit id in case created mid-run
        const formula = `{Contract ID}='${a.contractId}'`;
        const existing = await at(
          ids.contractsTableId,
          "GET",
          null,
          `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`,
        );
        contractRec = existing.json.records?.[0] || null;
      }

      if (!contractRec) {
        const fields = scrub({
          "Contract ID": a.contractId,
          "Discovery Property": invRec?.id ? [invRec.id] : undefined,
          "Agreement Type": a.agreementType || "FRANCHISE",
          "Agreement Role": "ORIGINAL",
          "Brand Family": a.brandFamily || null,
          "Hotel Country": "United States",
          "Hotel State / Region": a.stateRegion,
          "Hotel City": a.city,
          "Geographic Processing Class": "PRIORITY_HPC_ENRICHMENT",
          "Contract Status": primaryMs?.currentStatus || "EXPIRED_STATUS_UNCONFIRMED",
          "Scheduled Expiration Date":
            primaryDate && /^\d{4}-\d{2}-\d{2}/.test(primaryDate)
              ? primaryDate.slice(0, 10)
              : undefined,
          "Primary Source URL": sourceUrl,
          "Primary SEC Accession": accession,
          "Evidence Confidence": "MEDIUM",
          "Publication State": "CUSTOMER_BLOCKED",
          "Schema Version": "4.0.0",
          "Notes / Quarantine": `V4 verified migration · identity=${ik}`,
        });
        const created = await at(ids.contractsTableId, "POST", { fields, typecast: true });
        if (!created.ok) {
          row.actions.push("CONTRACT_CREATE_FAIL");
          row.error = created.json;
          allResults.push(row);
          process.exitCode = 3;
          break;
        }
        contractRec = created.json;
        byContractId.set(a.contractId, contractRec);
        byIdentity.set(ik, contractRec);
        createdContracts += 1;
        row.actions.push("CONTRACT_CREATED");
        row.changeClass = "NEW";
      } else {
        reusedContracts += 1;
        row.actions.push("CONTRACT_REUSED");
        row.changeClass = "UNCHANGED";
        row.liveContractId = contractRec.fields?.["Contract ID"] || contractRec.fields?.["Contract ID"];
        // If pilot used different Contract ID string, record alias — do not create second
        if (contractRec.fields?.["Contract ID"] && contractRec.fields["Contract ID"] !== a.contractId) {
          row.actions.push("IDENTITY_ALIAS_REUSED_PILOT_OR_PRIOR");
          row.auditContractId = a.contractId;
          row.liveContractId = contractRec.fields["Contract ID"];
        }
      }
      row.contractRecordId = contractRec.id;
      await sleep(140);

      // Milestones for this agreement
      for (const m of msList) {
        let msRec = byMilestoneId.get(m.milestoneId) || null;
        if (!msRec) {
          const formula = `{Milestone ID}='${m.milestoneId}'`;
          const existing = await at(
            ids.milestonesTableId,
            "GET",
            null,
            `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`,
          );
          msRec = existing.json.records?.[0] || null;
        }
        if (!msRec) {
          const fields = scrub({
            "Milestone ID": m.milestoneId,
            Contract: contractRec?.id ? [contractRec.id] : undefined,
            "Discovery Property": invRec?.id ? [invRec.id] : undefined,
            "Milestone Type": milestoneTypeFromMeaning(m.dateMeaning),
            "Milestone Date":
              m.primaryDate && /^\d{4}-\d{2}-\d{2}/.test(m.primaryDate)
                ? m.primaryDate.slice(0, 10)
                : undefined,
            "Date Meaning Precision": "YEAR_ONLY",
            "Current Status Class": m.currentStatus || "EXPIRED_STATUS_UNCONFIRMED",
            "Source URL": (m.sourceUrls || [])[0] || sourceUrl,
            "SEC Accession": (m.sourceAccessions || [])[0] || accession,
            "Evidence Snippet": String(f?.snippet || "").slice(0, 4000),
            "Publication State": "CUSTOMER_BLOCKED",
            "Schema Version": "4.0.0",
            Notes: `V4 verified migration · ${m.milestoneId}`,
          });
          const created = await at(ids.milestonesTableId, "POST", { fields, typecast: true });
          if (!created.ok) {
            row.actions.push("MILESTONE_CREATE_FAIL");
            row.error = created.json;
            process.exitCode = 3;
            break;
          }
          msRec = created.json;
          byMilestoneId.set(m.milestoneId, msRec);
          createdMilestones += 1;
          row.actions.push("MILESTONE_CREATED");
        } else {
          reusedMilestones += 1;
          row.actions.push("MILESTONE_REUSED");
        }
        row.milestoneRecordId = msRec.id;
        await sleep(120);

        // Finding link (one primary)
        if (findingId && f) {
          const airtableFindingId = `find_ctim_v4_${sha([discoveryPropertyId, m.dateMeaning, m.primaryDate, accession].join("|"))}`;
          const formula = `{${MAP_DISCOVERY_FINDINGS.findingId}}='${airtableFindingId}'`;
          let findingRec = (
            await at(
              ids.findingsTableId,
              "GET",
              null,
              `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`,
            )
          ).json.records?.[0];
          if (!findingRec) {
            const fields = scrub({
              [MAP_DISCOVERY_FINDINGS.findingId]: airtableFindingId,
              [MAP_DISCOVERY_FINDINGS.discoveryProperty]: invRec?.id ? [invRec.id] : undefined,
              [MAP_DISCOVERY_FINDINGS.findingType]: "CONTRACT_MILESTONE",
              [MAP_DISCOVERY_FINDINGS.fieldName]: m.dateMeaning,
              [MAP_DISCOVERY_FINDINGS.valueObserved]: String(m.primaryDate).slice(0, 200),
              [MAP_DISCOVERY_FINDINGS.agreementType]: a.agreementType || "FRANCHISE",
              [MAP_DISCOVERY_FINDINGS.historicalApplicability]: "CURRENT_STATUS_UNCONFIRMED",
              [MAP_DISCOVERY_FINDINGS.effectiveDate]:
                m.primaryDate && /^\d{4}-\d{2}-\d{2}/.test(m.primaryDate)
                  ? m.primaryDate.slice(0, 10)
                  : undefined,
              [MAP_DISCOVERY_FINDINGS.sourceUrl]: sourceUrl,
              [MAP_DISCOVERY_FINDINGS.secAccession]: accession,
              [MAP_DISCOVERY_FINDINGS.evidenceSnippet]: String(f.snippet || "").slice(0, 4000),
              [MAP_DISCOVERY_FINDINGS.confidence]: "MEDIUM",
              [MAP_DISCOVERY_FINDINGS.verificationStatus]: "HISTORICALLY_VERIFIED",
              [MAP_DISCOVERY_FINDINGS.hpcPromotionStatus]: "BLOCKED_CUSTOMER_PUBLICATION",
              [MAP_DISCOVERY_FINDINGS.schemaVersion]: DISCOVERY_SCHEMA_VERSION,
              [MAP_DISCOVERY_FINDINGS.notes]: `V4_VERIFIED_MIGRATION|${a.contractId}`,
              "Linked Contract": contractRec?.id ? [contractRec.id] : undefined,
              "Linked Milestone": msRec?.id ? [msRec.id] : undefined,
            });
            const created = await at(ids.findingsTableId, "POST", { fields, typecast: true });
            if (created.ok) {
              findingRec = created.json;
              createdFindings += 1;
              row.actions.push("FINDING_CREATED");
              await at(ids.milestonesTableId, "PATCH", {
                records: [{ id: msRec.id, fields: { "Related Finding": [findingRec.id] } }],
                typecast: true,
              });
            } else {
              row.actions.push("FINDING_CREATE_FAIL");
            }
          } else {
            row.actions.push("FINDING_REUSED");
          }
        }
      }

      // Read-back
      const rb = await at(ids.contractsTableId, "GET", null, `/${contractRec.id}`);
      const pub = rb.json?.fields?.["Publication State"];
      const rbOk = rb.ok && pub === "CUSTOMER_BLOCKED";
      row.readBack = {
        ok: rbOk,
        contractId: rb.json?.fields?.["Contract ID"],
        publicationState: pub,
        hpcLinked: (rb.json?.fields?.["HPC Hotel"] || []).length,
      };
      if (rbOk) readBackOk += 1;
      else {
        readBackFail += 1;
        process.exitCode = 3;
        allResults.push(row);
        break;
      }

      checkpoint.completedContractIds = [
        ...new Set([...(checkpoint.completedContractIds || []), a.contractId]),
      ];
      allResults.push(row);
      await sleep(100);
    }

    checkpoint.batches.push({
      at: new Date().toISOString(),
      batchIndex: bi,
      size: current.length,
      createdContracts,
      reusedContracts,
    });
    fs.writeFileSync(
      checkpointPath,
      JSON.stringify({ ...checkpoint, updatedAt: new Date().toISOString() }, null, 2),
    );

    if (process.exitCode === 3) break;
  }

  // Live totals after run
  let liveContractCount = liveContracts.length;
  let liveMilestoneCount = liveMilestones.length;
  if (!DRY) {
    const c2 = await listAll(ids.contractsTableId, ["Contract ID"]);
    const m2 = await listAll(ids.milestonesTableId, ["Milestone ID"]);
    liveContractCount = c2.length;
    liveMilestoneCount = m2.length;
  }

  const summary = {
    module: "V4_VERIFIED_HISTORICAL_MIGRATION",
    at: new Date().toISOString(),
    dryRun: DRY,
    apply: APPLY && !DRY,
    idempotencyRerun: IDEMPOTENCY,
    verifiedAuditAgreements: verifiedAgreements.length,
    alreadyMatchedBeforeRun: alreadyMatched.length,
    remainingBeforeRun: remaining.length,
    batchesProcessed: batches.length,
    batchSize: BATCH_SIZE,
    createdContracts,
    reusedContracts,
    createdMilestones,
    reusedMilestones,
    createdFindings,
    readBackOk,
    readBackFail,
    conflicted,
    rejected,
    liveContractCountAfter: liveContractCount,
    liveMilestoneCountAfter: liveMilestoneCount,
    hpcRecordsCreated: 0,
    hpcFieldsModified: 0,
    publicationState: "CUSTOMER_BLOCKED",
    quarantineCounts: {
      RESEARCH_ONLY: quarantine.RESEARCH_ONLY.length,
      CONFLICTED: quarantine.CONFLICTED.length,
      REJECTED_FINDINGS: quarantine.REJECTED_FINDINGS.length,
    },
    alreadyMatchedSample: alreadyMatched.slice(0, 10),
    results: allResults,
  };

  const outName = DRY
    ? "VERIFIED_MIGRATION_DRY_RUN.json"
    : IDEMPOTENCY
      ? "VERIFIED_MIGRATION_IDEMPOTENCY.json"
      : "VERIFIED_MIGRATION_APPLY.json";
  fs.writeFileSync(path.join(OUT_DIR, outName), JSON.stringify(summary, null, 2));
  console.log(
    JSON.stringify(
      {
        out: path.join(OUT_DIR, outName),
        verifiedAudit: verifiedAgreements.length,
        alreadyMatched: alreadyMatched.length,
        remaining: remaining.length,
        createdContracts,
        reusedContracts,
        createdMilestones,
        reusedMilestones,
        readBackOk,
        liveContractCountAfter: liveContractCount,
        liveMilestoneCountAfter: liveMilestoneCount,
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
