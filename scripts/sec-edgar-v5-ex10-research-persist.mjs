#!/usr/bin/env node
/**
 * V5 EX-10 Research Mode persistence — controlled, CUSTOMER_BLOCKED.
 *
 *   node scripts/sec-edgar-v5-ex10-research-persist.mjs --dry-run
 *   HIF_APPLY_RESEARCH=1 node scripts/sec-edgar-v5-ex10-research-persist.mjs --apply
 *
 * Forms/drafts/portfolio → Findings only (no invented hotel contracts).
 * Named-hotel amendments/extensions → Inventory + Contract + Finding when hotel extracted.
 * No HPC. No Meta schema apply. Independent read-back required on apply.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import {
  GRAPH_BASE_ID_CANONICAL,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import {
  classifyDocumentExecution,
  persistencePolicyForExecutionClass,
  mapAgreementRole,
  buildDocumentExecutionSchemaProposal,
  DOCUMENT_EXECUTION_CLASS,
} from "../lib/hotel-intelligence/sec-edgar/ex10-document-execution-class-v1.js";
import {
  extractEndDateAndNotice,
  isQualityHotelName,
} from "../lib/hotel-intelligence/sec-edgar/ex10-end-date-notice-extract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-factory-v5");
const READY = path.join(OUT, "EX10_RESEARCH_PERSIST_READY.json");
const SCHEMA_IDS = path.join(ROOT, "reports/sec-edgar-contract-timing-v4/SCHEMA_TABLE_IDS.json");
const APPLY = process.argv.includes("--apply") && process.env.HIF_APPLY_RESEARCH === "1";
const DRY = !APPLY;

function sha(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex").slice(0, 16);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function token() {
  return (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
}
function scrub(fields) {
  for (const k of Object.keys(fields)) {
    if (fields[k] == null || fields[k] === "") delete fields[k];
  }
  return fields;
}
function writeJson(fp, obj) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
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

async function findByFormula(tableId, formula) {
  const qs = `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`;
  const r = await at(tableId, "GET", null, qs);
  return r.json.records?.[0] || null;
}

async function main() {
  if (!fs.existsSync(READY)) throw new Error(`missing ${READY}`);
  if (!fs.existsSync(SCHEMA_IDS)) throw new Error(`missing ${SCHEMA_IDS}`);
  const ready = JSON.parse(fs.readFileSync(READY, "utf8"));
  const schemaIds = JSON.parse(fs.readFileSync(SCHEMA_IDS, "utf8"));
  const schemaProposal = buildDocumentExecutionSchemaProposal();
  writeJson(path.join(OUT, "EX10_DOCUMENT_EXECUTION_SCHEMA_PROPOSAL.json"), schemaProposal);

  if (!token() && APPLY) throw new Error("AIRTABLE token required for --apply");

  const edgar = createSecEdgarClient();
  const results = [];
  const classified = [];
  let failClosed = false;

  for (const rec of ready.records || []) {
    const executionClass = classifyDocumentExecution(rec);
    const policy = persistencePolicyForExecutionClass(executionClass);
    let text = "";
    let retrievalError = null;
    try {
      const res = await edgar.request(rec.sourceUrl, {
        accept: "text/html,*/*",
        maxAgeMs: 30 * 24 * 60 * 60 * 1000,
      });
      if (res.ok) text = res.text || "";
      else retrievalError = res.error || `HTTP_${res.status}`;
    } catch (err) {
      retrievalError = String(err?.message || err);
    }

    const timing = text
      ? extractEndDateAndNotice(
          { html: text },
          { sourceUrl: rec.sourceUrl, accession: rec.accession, currentStatusVerified: false },
        )
      : null;

    const hotelName = isQualityHotelName(timing?.primaryHotelName)
      ? timing.primaryHotelName
      : (timing?.namedHotelCandidates || []).find((n) => isQualityHotelName(n)) || null;
    const documentId = `doc_${sha([rec.accession, rec.sourceUrl, rec.exhibitId].join("|"))}`;
    const agreementId = `agr_${sha([rec.cik, rec.accession, rec.agreementType, hotelName || "NO_HOTEL", executionClass].join("|"))}`;
    const findingId = `find_ex10_${sha(documentId)}`;

    const row = {
      exhibitId: rec.exhibitId,
      documentId,
      agreementId,
      findingId,
      cik: rec.cik,
      accession: rec.accession,
      sourceUrl: rec.sourceUrl,
      agreementType: rec.agreementType === "OTHER" ? "UNKNOWN_AGREEMENT" : rec.agreementType,
      instrumentClass: rec.instrumentClass,
      scopeClass: rec.scopeClass,
      executionClass,
      policy,
      hotelName,
      namedHotelCandidates: timing?.namedHotelCandidates || [],
      scheduledEndDate: timing?.scheduledEndDate || null,
      endDateStatus: timing?.endDateStatus || null,
      noticePeriodDays: timing?.noticePeriodDays ?? rec.extracted?.renewalNoticeDays ?? null,
      noticeDeadline: timing?.noticeDeadline || null,
      commencementOrEffectiveDate: timing?.commencementOrEffectiveDate || null,
      originalTermYears: timing?.originalTermYears ?? null,
      assumptions: timing?.assumptions || [],
      retrievalError,
      publicationState: "CUSTOMER_BLOCKED",
      actions: [],
      readBack: null,
      airtable: {},
    };
    classified.push(row);

    const canContract =
      policy.createContractIdentity &&
      hotelName &&
      rec.scopeClass === "NAMED_HOTEL" &&
      rec.hotelAttributionAllowed === true &&
      !policy.inventHotelAttribution &&
      (executionClass === DOCUMENT_EXECUTION_CLASS.EXECUTED_NAMED_HOTEL_AGREEMENT ||
        executionClass === DOCUMENT_EXECUTION_CLASS.AMENDMENT_OR_EXTENSION_INSTRUMENT);

    if (DRY) {
      row.actions.push("DRY_RUN");
      if (policy.createFinding) row.actions.push("WOULD_WRITE_FINDING");
      if (canContract) row.actions.push("WOULD_WRITE_CONTRACT_INVENTORY");
      else if (policy.createContractIdentity && !hotelName) {
        row.actions.push("CONTRACT_SKIPPED_NO_NAMED_HOTEL");
      }
      if (
        executionClass === DOCUMENT_EXECUTION_CLASS.FORM_AGREEMENT ||
        executionClass === DOCUMENT_EXECUTION_CLASS.DRAFT_OR_PROPOSED_AGREEMENT
      ) {
        row.actions.push("FORM_OR_DRAFT_NOT_EXECUTED_CONTRACT");
      }
      results.push(row);
      continue;
    }

    // --- APPLY path ---
    try {
      // Always write document finding when allowed
      if (policy.createFinding) {
        let findingRec = await findByFormula(
          schemaIds.findingsTableId,
          `{${MAP_DISCOVERY_FINDINGS.findingId}}='${findingId}'`,
        );
        if (!findingRec) {
          const fields = scrub({
            [MAP_DISCOVERY_FINDINGS.findingId]: findingId,
            [MAP_DISCOVERY_FINDINGS.findingType]: "CONTRACT_MILESTONE",
            [MAP_DISCOVERY_FINDINGS.fieldName]: `EX10_${executionClass}`,
            [MAP_DISCOVERY_FINDINGS.valueObserved]: String(
              row.scheduledEndDate || row.noticePeriodDays || rec.instrumentClass || "",
            ).slice(0, 200),
            [MAP_DISCOVERY_FINDINGS.agreementType]:
              rec.agreementType === "FRANCHISE"
                ? "FRANCHISE"
                : rec.agreementType === "MANAGEMENT"
                  ? "MANAGEMENT"
                  : "UNKNOWN_AGREEMENT",
            [MAP_DISCOVERY_FINDINGS.historicalApplicability]: "CURRENT_STATUS_UNCONFIRMED",
            [MAP_DISCOVERY_FINDINGS.effectiveDate]: row.scheduledEndDate || undefined,
            [MAP_DISCOVERY_FINDINGS.sourceUrl]: rec.sourceUrl,
            [MAP_DISCOVERY_FINDINGS.secAccession]: rec.accession,
            [MAP_DISCOVERY_FINDINGS.registrantCik]: rec.cik,
            [MAP_DISCOVERY_FINDINGS.evidenceSnippet]: String(
              timing?.explicitEndExcerpt ||
                timing?.calculationProvenance ||
                `EX-10 ${rec.instrumentClass} ${rec.scopeClass}`,
            ).slice(0, 4000),
            [MAP_DISCOVERY_FINDINGS.confidence]: "MEDIUM",
            [MAP_DISCOVERY_FINDINGS.verificationStatus]: "EVIDENCE_COLLECTED",
            [MAP_DISCOVERY_FINDINGS.hpcPromotionStatus]: "NOT_PROMOTED",
            [MAP_DISCOVERY_FINDINGS.schemaVersion]: "5.0.0-ex10",
            [MAP_DISCOVERY_FINDINGS.notes]: [
              `DocumentExecutionClass=${executionClass}`,
              `DocumentId=${documentId}`,
              `AgreementId=${agreementId}`,
              `Scope=${rec.scopeClass}`,
              `EndDateStatus=${row.endDateStatus || "n/a"}`,
              `NoticeDays=${row.noticePeriodDays ?? "NOTICE_NOT_DISCLOSED"}`,
              `CUSTOMER_BLOCKED`,
              ...(timing?.assumptions || []),
            ].join(" · "),
          });
          const created = await at(schemaIds.findingsTableId, "POST", { fields, typecast: true });
          if (!created.ok) {
            row.actions.push("FINDING_FAIL");
            row.error = created.json;
            results.push(row);
            failClosed = true;
            break;
          }
          findingRec = created.json;
          row.actions.push("FINDING_CREATED");
        } else {
          row.actions.push("FINDING_REUSED");
        }
        row.airtable.findingRecordId = findingRec.id;
        await sleep(140);
      }

      if (canContract) {
        const discoveryPropertyId = `disc_ex10_${sha([norm(hotelName), rec.cik].join("|"))}`;
        let invRec = await findByFormula(
          schemaIds.inventoryTableId,
          `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}='${discoveryPropertyId}'`,
        );
        if (!invRec) {
          const fields = scrub({
            [MAP_DISCOVERY_INVENTORY.discoveryPropertyId]: discoveryPropertyId,
            [MAP_DISCOVERY_INVENTORY.hotelNameNormalized]: hotelName,
            [MAP_DISCOVERY_INVENTORY.hotelNameRaw]: hotelName,
            [MAP_DISCOVERY_INVENTORY.country]: "United States",
            [MAP_DISCOVERY_INVENTORY.identityClass]: "PROBABLE_PROPERTY",
            [MAP_DISCOVERY_INVENTORY.lifecycleStatus]: "DISCOVERY_PERSISTED",
            [MAP_DISCOVERY_INVENTORY.persistenceStatus]: "RESEARCH_ONLY",
            [MAP_DISCOVERY_INVENTORY.publicationState]: "CUSTOMER_BLOCKED",
            [MAP_DISCOVERY_INVENTORY.sourceFilingUrl]: rec.sourceUrl,
            [MAP_DISCOVERY_INVENTORY.sourceAccessionPrimary]: rec.accession,
            [MAP_DISCOVERY_INVENTORY.registrantCik]: rec.cik,
            [MAP_DISCOVERY_INVENTORY.schemaVersion]: "5.0.0-ex10",
            [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "DISCOVERED_NOT_IN_HPC",
            [MAP_DISCOVERY_INVENTORY.evidenceConfidence]: "MEDIUM",
            [MAP_DISCOVERY_INVENTORY.discoveryMethod]: "SEC_EX10_V5_RESEARCH_PERSIST",
            [MAP_DISCOVERY_INVENTORY.notesQuarantine]: `EX10 ${executionClass} · ${documentId}`,
          });
          const created = await at(schemaIds.inventoryTableId, "POST", { fields, typecast: true });
          if (!created.ok) {
            row.actions.push("INVENTORY_FAIL");
            row.error = created.json;
            results.push(row);
            failClosed = true;
            break;
          }
          invRec = created.json;
          row.actions.push("INVENTORY_CREATED");
        } else {
          row.actions.push("INVENTORY_REUSED");
        }
        row.airtable.inventoryRecordId = invRec.id;
        await sleep(140);

        let contractRec = await findByFormula(
          schemaIds.contractsTableId,
          `{Contract ID}='${agreementId}'`,
        );
        if (!contractRec) {
          const role = mapAgreementRole(rec.instrumentClass, executionClass);
          const fields = scrub({
            "Contract ID": agreementId,
            "Discovery Property": invRec?.id ? [invRec.id] : undefined,
            "Agreement Type":
              rec.agreementType === "FRANCHISE"
                ? "FRANCHISE"
                : rec.agreementType === "MANAGEMENT"
                  ? "MANAGEMENT"
                  : "OTHER",
            "Agreement Role": role,
            "Hotel Country": "United States",
            "Geographic Processing Class": "PRIORITY_HPC_ENRICHMENT",
            "Contract Status": "EXPIRED_STATUS_UNCONFIRMED",
            "Commencement Date": row.commencementOrEffectiveDate || undefined,
            "Scheduled Expiration Date": row.scheduledEndDate || undefined,
            "Term Years": row.originalTermYears || undefined,
            "Renewal Options Summary":
              row.noticePeriodDays != null
                ? `Renewal notice days: ${row.noticePeriodDays}`
                : "NOTICE_NOT_DISCLOSED",
            "Primary Source URL": rec.sourceUrl,
            "Primary SEC Accession": rec.accession,
            "Evidence Confidence": "MEDIUM",
            "Publication State": "CUSTOMER_BLOCKED",
            "Schema Version": "5.0.0-ex10",
            "Notes / Quarantine": [
              `DocumentExecutionClass=${executionClass}`,
              `DocumentId=${documentId}`,
              `EndDateStatus=${row.endDateStatus || "NOT_DISCLOSED"}`,
              `CURRENT_STATUS_NOT_VERIFIED`,
              ...(row.assumptions || []),
            ].join(" · "),
          });
          const created = await at(schemaIds.contractsTableId, "POST", { fields, typecast: true });
          if (!created.ok) {
            row.actions.push("CONTRACT_FAIL");
            row.error = created.json;
            results.push(row);
            failClosed = true;
            break;
          }
          contractRec = created.json;
          row.actions.push("CONTRACT_CREATED");
        } else {
          row.actions.push("CONTRACT_REUSED");
        }
        row.airtable.contractRecordId = contractRec.id;
        await sleep(140);
      } else if (policy.createContractIdentity && !hotelName) {
        row.actions.push("CONTRACT_SKIPPED_NO_NAMED_HOTEL");
      } else {
        row.actions.push("CONTRACT_SKIPPED_BY_POLICY");
      }

      // Independent read-back
      const rb = {};
      if (row.airtable.findingRecordId) {
        const r = await at(schemaIds.findingsTableId, "GET", null, `/${row.airtable.findingRecordId}`);
        rb.findingOk = r.ok;
        rb.findingId = r.json?.fields?.[MAP_DISCOVERY_FINDINGS.findingId] || null;
        rb.findingAccession = r.json?.fields?.[MAP_DISCOVERY_FINDINGS.secAccession] || null;
      }
      if (row.airtable.contractRecordId) {
        const r = await at(schemaIds.contractsTableId, "GET", null, `/${row.airtable.contractRecordId}`);
        rb.contractOk = r.ok;
        rb.contractId = r.json?.fields?.["Contract ID"] || null;
        rb.contractPublication = r.json?.fields?.["Publication State"] || null;
        rb.contractStatus = r.json?.fields?.["Contract Status"] || null;
        if (rb.contractPublication !== "CUSTOMER_BLOCKED") {
          row.actions.push("READBACK_PUBLICATION_FAIL");
          failClosed = true;
        }
      }
      if (row.airtable.inventoryRecordId) {
        const r = await at(schemaIds.inventoryTableId, "GET", null, `/${row.airtable.inventoryRecordId}`);
        rb.inventoryOk = r.ok;
        rb.inventoryPublication = r.json?.fields?.[MAP_DISCOVERY_INVENTORY.publicationState] || null;
        if (rb.inventoryPublication && rb.inventoryPublication !== "CUSTOMER_BLOCKED") {
          row.actions.push("READBACK_INVENTORY_PUBLICATION_FAIL");
          failClosed = true;
        }
      }
      row.readBack = rb;
      row.actions.push("READBACK_DONE");
      results.push(row);
      if (failClosed) break;
      await sleep(100);
    } catch (err) {
      row.actions.push("EXCEPTION");
      row.error = String(err?.message || err);
      results.push(row);
      failClosed = true;
      break;
    }
  }

  const summary = {
    dryRun: DRY,
    apply: APPLY,
    failClosed,
    total: results.length,
    byExecutionClass: {},
    findingsWouldOrDid: results.filter((r) =>
      r.actions.some((a) => /FINDING|WOULD_WRITE_FINDING/.test(a)),
    ).length,
    contractsWouldOrDid: results.filter((r) =>
      r.actions.some((a) => /CONTRACT_CREATED|CONTRACT_REUSED|WOULD_WRITE_CONTRACT/.test(a)),
    ).length,
    formOrDraftSeparated: results.filter((r) =>
      /FORM_AGREEMENT|DRAFT_OR_PROPOSED/.test(r.executionClass),
    ).length,
    portfolioSeparated: results.filter((r) =>
      r.executionClass === DOCUMENT_EXECUTION_CLASS.EXECUTED_PORTFOLIO_AGREEMENT,
    ).length,
    namedHotelsExtracted: results.filter((r) => r.hotelName).length,
    withScheduledEnd: results.filter((r) => r.scheduledEndDate).length,
    withNotice: results.filter((r) => r.noticePeriodDays != null).length,
    readBackOk: results.filter((r) => r.readBack && (r.readBack.findingOk || r.readBack.contractOk)).length,
    schemaProposalStatus: schemaProposal.status,
    publicationState: "CUSTOMER_BLOCKED",
    hpcCreates: 0,
  };
  for (const r of results) {
    summary.byExecutionClass[r.executionClass] = (summary.byExecutionClass[r.executionClass] || 0) + 1;
  }

  writeJson(path.join(OUT, "EX10_PERSIST_CLASSIFIED.json"), { classified });
  writeJson(path.join(OUT, "EX10_PERSIST_RESULTS.json"), { summary, results });
  console.log(JSON.stringify({ ok: !failClosed, summary }, null, 2));
  if (failClosed) process.exitCode = 2;
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
