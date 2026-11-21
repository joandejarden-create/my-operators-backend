#!/usr/bin/env node
/**
 * Contract Terms, Renewals & Decision Windows — E2E research pilot (PR #499).
 *
 *   node scripts/sec-edgar-contract-terms-renewals-pilot.mjs --select-only
 *   node scripts/sec-edgar-contract-terms-renewals-pilot.mjs --run
 *   node scripts/sec-edgar-contract-terms-renewals-pilot.mjs --run --limit=40
 *
 * Cache-first free SEC EDGAR. Paid Exa/SEC-API capped at $25 incremental.
 * No Meta schema apply. No HPC writes. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  selectContractTermsCohort,
  managementAgreementDiscoveryPlan,
} from "../lib/hotel-intelligence/sec-edgar/contract-terms-cohort-v1.js";
import { runEnrichContractTermsStage } from "../lib/hotel-intelligence/sec-edgar/contract-terms-enrich-stage-v1.js";
import { enrichOneAgreement } from "../lib/hotel-intelligence/sec-edgar/contract-terms-enrich-stage-v1.js";
import { buildAdditiveSchemaDiffPacket } from "../lib/hotel-intelligence/sec-edgar/contract-terms-schema-proposal-v4.js";
import { createContractTimingV2Budget } from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-budget-v1.js";
import { createExaContractTimingClient } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";
import { createSecApiIoClient } from "../lib/hotel-intelligence/sec-edgar/sec-api-io-client-v1.js";
import { classifyRenewalOutcome } from "../lib/hotel-intelligence/sec-edgar/contract-terms-renewal-classifier-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-contract-timing-v4");
const SELECT_ONLY = process.argv.includes("--select-only");
const RUN = process.argv.includes("--run") || !SELECT_ONLY;
const LIMIT = Math.min(
  50,
  Math.max(
    30,
    Number((process.argv.find((a) => a.startsWith("--limit=")) || "").split("=")[1]) || 40,
  ),
);
const MAX_PAID = 25;
const MANUAL_REVIEW_N = 8;

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(fp, obj) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
}

function loadFindingsIndex() {
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
      key === "verified" ? [...(j.verified || []), ...(j.probable || [])] : j[key] || j.findings || [];
    for (const f of rows) {
      if (f.findingId) byId.set(f.findingId, f);
    }
  }
  return byId;
}

function loadVerifiedAgreements() {
  const reconcilePath = path.join(OUT, "VERIFIED_MIGRATION_RECONCILE.json");
  const auditPath = path.join(OUT, "MIGRATION_QUALITY_AUDIT.json");
  const findings = loadFindingsIndex();
  let rows = [];
  if (fs.existsSync(reconcilePath)) {
    const j = JSON.parse(fs.readFileSync(reconcilePath, "utf8"));
    rows = [...(j.alreadyPersisted || []), ...(j.pending || [])];
  }
  if (!rows.length && fs.existsSync(auditPath)) {
    const j = JSON.parse(fs.readFileSync(auditPath, "utf8"));
    rows = (j.agreements || []).filter((a) => a.quality === "VERIFIED");
  }
  return rows.map((a) => {
    const snips = [...(a.evidenceSnippets || [])];
    let bestDate = (a.milestones || [])[0]?.primaryDate || null;
    for (const fid of a.findingIds || []) {
      const f = findings.get(fid);
      if (!f) continue;
      const snip = f.evidenceSnippet || f.snippet || f.evidence || "";
      if (snip) snips.push(String(snip));
      const d = f.primaryDate || f.date || f.isoDate;
      if (d && (!bestDate || String(d).slice(0, 4) > String(bestDate).slice(0, 4))) {
        bestDate = d;
      }
    }
    const milestones = [...(a.milestones || [])];
    if (milestones[0] && bestDate) {
      milestones[0] = { ...milestones[0], primaryDate: bestDate };
    } else if (!milestones.length && bestDate) {
      milestones.push({ primaryDate: bestDate, dateMeaning: "FRANCHISE_EXPIRATION" });
    }
    return {
      ...a,
      quality: a.quality || "VERIFIED",
      agreementType: a.agreementType || "FRANCHISE",
      evidenceSnippets: [...new Set(snips)],
      milestones,
    };
  });
}

function pct(n, d) {
  if (!d) return 0;
  return Math.round((1000 * n) / d) / 10;
}

function fieldFound(results, pred) {
  return results.filter(pred).length;
}

async function discoverManagementSubset(budget, maxResults = 8) {
  const plan = managementAgreementDiscoveryPlan();
  const leads = [];
  const exa = createExaContractTimingClient({ budget });
  const secApi = createSecApiIoClient({ budget });

  // Prefer free / subscription SEC-API full-text if available ($0 against incremental)
  if (typeof secApi?.fullTextSearch === "function") {
    try {
      const q = plan.queries.find((x) => x.provider === "SEC_API_IO");
      const res = await secApi.fullTextSearch({
        query: q.query,
        formTypes: ['"EX-10"', '"EX-10.1"'],
        page: "1",
      });
      const hits = res?.filings || res?.body?.filings || res?.results || [];
      for (const h of (hits || []).slice(0, maxResults)) {
        leads.push({
          source: "SEC_API_IO",
          title: h.companyName || h.title || null,
          form: h.formType || h.form || null,
          filedAt: h.filedAt || h.filingDate || null,
          url: h.linkToFilingDetails || h.url || h.linkToHtml || null,
          accession: h.accessionNo || h.accession || null,
          agreementTypeHint: "MANAGEMENT",
          verificationStatus: "DISCOVERY_LEAD_NOT_VERIFIED",
        });
      }
    } catch (err) {
      leads.push({ source: "SEC_API_IO", error: String(err?.message || err) });
    }
  }

  if (leads.filter((l) => l.url).length < 3 && exa) {
    const gate = budget.canSpend(0.025);
    if (gate.ok) {
      try {
        const q = plan.queries.find((x) => x.provider === "EXA");
        const res = await exa.search({
          query: q.query,
          numResults: 5,
          type: "lite",
        });
        const results = res?.results || res?.result?.results || [];
        for (const r of results) {
          leads.push({
            source: "EXA",
            title: r.title || null,
            url: r.url || null,
            publishedDate: r.publishedDate || null,
            agreementTypeHint: "MANAGEMENT",
            verificationStatus: "DISCOVERY_LEAD_NOT_VERIFIED",
            note: "Lead only — not a verified hotel management contract",
          });
        }
      } catch (err) {
        leads.push({ source: "EXA", error: String(err?.message || err) });
      }
    }
  }

  return {
    plan,
    leads,
    verifiedManagementInFranchiseCohort: 0,
    note: "Management leads are a separate discovery subset; franchises were not relabeled.",
  };
}

function buildQualityMetrics(enrich, management) {
  const results = enrich.results || [];
  const n = results.length || 1;
  const has = (fn) => fieldFound(results, fn);

  return {
    agreementsSampled: enrich.agreementsSampled,
    agreementsWithSourceDocumentsRetrieved: enrich.documentsRetrieved,
    agreementsWithSourceDocumentsRetrievedPct: pct(enrich.documentsRetrieved, enrich.agreementsSampled),
    originalTermFound: has((r) => r.profile?.extracted?.originalTermYears != null),
    originalTermFoundPct: pct(
      has((r) => r.profile?.extracted?.originalTermYears != null),
      enrich.agreementsSampled,
    ),
    commencementDateFound: has((r) => r.profile?.extracted?.commencementRaw != null),
    commencementDateFoundPct: pct(
      has((r) => r.profile?.extracted?.commencementRaw != null),
      enrich.agreementsSampled,
    ),
    originalExpirationFound: has((r) => Boolean(r.profile?.baseline?.scheduledExpiration)),
    originalExpirationFoundPct: pct(
      has((r) => Boolean(r.profile?.baseline?.scheduledExpiration)),
      enrich.agreementsSampled,
    ),
    renewalOptionsFound: has((r) => r.profile?.extracted?.renewalOptionsCount != null),
    renewalOptionsFoundPct: pct(
      has((r) => r.profile?.extracted?.renewalOptionsCount != null),
      enrich.agreementsSampled,
    ),
    noticeDeadlinesFound: has((r) => r.profile?.extracted?.renewalNoticeDays != null),
    noticeDeadlinesFoundPct: pct(
      has((r) => r.profile?.extracted?.renewalNoticeDays != null),
      enrich.agreementsSampled,
    ),
    automaticRenewalProvisionsFound: has(
      (r) => String(r.profile?.extracted?.renewalMode || "").includes("AUTOMATIC"),
    ),
    executedRenewalsVerified: has(
      (r) => r.renewal?.renewalStatusClass === "RENEWAL_EXECUTED_VERIFIED",
    ),
    amendmentsVerified: has(
      (r) => r.renewal?.renewalStatusClass === "EXTENSION_EXECUTED_VERIFIED",
    ),
    terminationRightsFound: has(
      (r) =>
        r.profile?.extracted?.terminationForCause ||
        r.profile?.extracted?.terminationWithoutCause ||
        r.profile?.extracted?.performanceTest,
    ),
    financialTermsFound: has(
      (r) => r.profile?.extracted?.royaltyMention || r.profile?.extracted?.managementFeeMention,
    ),
    decisionWindowsCalculated: has((r) => Boolean(r.decisionWindows?.nextDecisionWindow?.date)),
    decisionWindowsCalculatedPct: pct(
      has((r) => Boolean(r.decisionWindows?.nextDecisionWindow?.date)),
      enrich.agreementsSampled,
    ),
    contractualStatusUnresolved: has(
      (r) => r.renewal?.renewalStatusClass === "STATUS_UNCONFIRMED",
    ),
    extractionErrors: (enrich.errors || []).length,
    conflictedEvidence: 0,
    incrementalResearchCostUsd: enrich.paidUsd,
    managementDiscoveryLeads: (management.leads || []).filter((l) => l.url).length,
    managementLeadsWithTermsExtracted: Number(management._enrichedWithTerms || 0),
    incrementalBeyondExpiration: has((r) => r.profile?.incrementalBeyondExpiration),
    incrementalBeyondExpirationPct: pct(
      has((r) => r.profile?.incrementalBeyondExpiration),
      enrich.agreementsSampled,
    ),
    denominators: { agreementsSampled: enrich.agreementsSampled, enriched: results.length },
  };
}

function manualReviewSample(results, n = MANUAL_REVIEW_N) {
  const scored = [...results].sort(
    (a, b) => (b.profile?.disclosedFieldCount || 0) - (a.profile?.disclosedFieldCount || 0),
  );
  const top = scored.slice(0, Math.ceil(n / 2));
  const mid = scored.slice(Math.floor(scored.length / 3), Math.floor(scored.length / 3) + Math.floor(n / 4));
  const low = scored.slice(-Math.floor(n / 4));
  const picked = [...top, ...mid, ...low].slice(0, n);
  return picked.map((r) => ({
    hotelName: r.hotelName,
    contractId: r.contractId,
    brandFamily: r.brandFamily,
    sourceUrl: r.document?.url,
    accession: r.document?.accession,
    disclosedFieldCount: r.profile?.disclosedFieldCount,
    renewalStatusClass: r.renewal?.renewalStatusClass,
    nextWindow: r.decisionWindows?.nextDecisionWindow,
    evidenceExcerpts: (r.profile?.evidence || [])
      .filter((e) => e.value != null && e.excerpt)
      .slice(0, 4)
      .map((e) => ({ field: e.field, value: e.value, excerpt: e.excerpt })),
    reviewNotes:
      "Manual review: confirm excerpt matches disclosure; do not treat option language as executed renewal.",
  }));
}

async function main() {
  ensureDir(OUT);
  const verified = loadVerifiedAgreements();
  const franchiseTypes = new Set(verified.map((v) => v.agreementType));
  const selection = selectContractTermsCohort(verified, { targetSize: LIMIT });

  const capabilityAudit = {
    module: "V4_CONTRACT_TERMS_CAPABILITY_AUDIT",
    at: new Date().toISOString(),
    tables: {
      hiHotelContracts: "tblMjUCBtXF2o3r9u",
      hiContractMilestones: "tbljzAYjYs3dyHWZk",
      hiHotelDiscoveryInventory: "tblx77Nmh56h4xa9z",
      hiDiscoveryFindings: "tblGjTWNm8G4UOF1e",
      hiOrganizations: "tbliXEfUOWk32AN9y",
      baseId: "appCCUsuGsE1ifoLk",
    },
    existingContractFieldsSupportingTerms: [
      "Commencement Date",
      "Scheduled Expiration Date",
      "Term Years",
      "Renewal Options Summary",
      "Termination Rights Summary",
      "Contract Status",
      "Agreement Role",
    ],
    verifiedInventoryAgreementTypes: [...franchiseTypes],
    verifiedCount: verified.length,
    franchiseOnlyConfirmed: [...franchiseTypes].every((t) => t === "FRANCHISE"),
    additiveSchema: buildAdditiveSchemaDiffPacket(),
    researchModeEngine: "hotel-intelligence-factory RESEARCH + PERSIST_RESEARCH (reuse)",
    publicationState: "CUSTOMER_BLOCKED",
    preserveExistingPilotContracts: true,
  };
  writeJson(path.join(OUT, "CONTRACT_TERMS_CAPABILITY_AUDIT.json"), capabilityAudit);
  writeJson(path.join(OUT, "CONTRACT_TERMS_COHORT_SELECTION.json"), selection);
  writeJson(path.join(OUT, "CONTRACT_TERMS_ADDITIVE_SCHEMA_PROPOSAL.json"), buildAdditiveSchemaDiffPacket());

  console.log(
    JSON.stringify(
      {
        ok: true,
        phase: "SELECT",
        selected: selection.selectedCount,
        brandDistribution: selection.brandDistribution,
        expirationBucketDistribution: selection.expirationBucketDistribution,
        franchiseOnly: selection.franchiseOnly,
      },
      null,
      2,
    ),
  );

  if (SELECT_ONLY || !RUN) return;

  const budget = createContractTimingV2Budget({
    ceilingUsd: MAX_PAID,
    ledgerPath: path.join(ROOT, "data/sec-edgar/contract-terms-renewals-budget-ledger.json"),
    cwd: ROOT,
  });

  const management = await discoverManagementSubset(budget, 8);
  writeJson(path.join(OUT, "CONTRACT_TERMS_MANAGEMENT_DISCOVERY_SUBSET.json"), management);

  console.log(
    JSON.stringify(
      {
        phase: "MANAGEMENT_DISCOVERY",
        leads: (management.leads || []).filter((l) => l.url).length,
        spentUsd: typeof budget.snapshot === "function" ? budget.snapshot().spentUsd : 0,
      },
      null,
      2,
    ),
  );

  // Later-document renewal probes for a subset (Exa, budget-capped)
  const exa = createExaContractTimingClient({ budget });
  const laterByContract = {};
  const renewalProbeTargets = selection.cohort
    .filter((c) => {
      const y = Number(String(c.scheduledExpiration || "").slice(0, 4));
      return y && y <= new Date().getUTCFullYear();
    })
    .slice(0, 12);
  for (const c of renewalProbeTargets) {
    const gate = budget.canSpend(0.01);
    if (!gate.ok) {
      laterByContract[c.contractId] = { skipped: "BUDGET" };
      break;
    }
    try {
      const q = `"${c.hotelName}" (franchise OR brand) (renewed OR renewal OR reflag OR terminated OR "management agreement")`;
      const res = await exa.search({ query: q, numResults: 3, type: "lite" });
      const results = res?.results || res?.result?.results || [];
      const laterText = results
        .map((r) => `${r.title || ""} ${r.text || r.summary || ""} ${r.url || ""}`)
        .join("\n");
      laterByContract[c.contractId] = {
        query: q,
        hits: results.map((r) => ({ title: r.title, url: r.url })),
        laterText: laterText.slice(0, 4000),
      };
    } catch (err) {
      laterByContract[c.contractId] = { error: String(err?.message || err) };
    }
  }
  writeJson(path.join(OUT, "CONTRACT_TERMS_LATER_DOCUMENT_PROBES.json"), laterByContract);

  const enrich = await runEnrichContractTermsStage({
    cohort: selection.cohort,
    budget,
    fetchDocuments: true,
  });
  // Reclassify renewals with later-document text where available
  for (const row of enrich.results || []) {
    const later = laterByContract[row.contractId];
    if (!later?.laterText) continue;
    row.renewal = classifyRenewalOutcome({
      originalText: (row.profile?.evidence || []).map((e) => e.excerpt || "").join(" "),
      laterText: later.laterText,
      sameBrandObserved: /marriott|hilton|ihg|hyatt|choice|holiday inn|hampton|courtyard/i.test(
        later.laterText,
      ),
      expirationStillCited: Boolean(row.profile?.baseline?.scheduledExpiration),
      scheduledExpiration: row.profile?.baseline?.scheduledExpiration,
    });
    row.laterDocumentProbe = { hits: later.hits || [], query: later.query };
  }

  // Enrich separate management discovery leads (not franchise relabels)
  const managementEnrich = [];
  for (const lead of (management.leads || []).filter((l) => l.url).slice(0, 5)) {
    try {
      const fakeAgreement = {
        contractId: `mgmt_lead_${crypto.createHash("sha1").update(lead.url).digest("hex").slice(0, 12)}`,
        hotelName: lead.title || "Management agreement exhibit",
        agreementType: "MANAGEMENT",
        brandFamily: "UNKNOWN",
        sourceUrls: [lead.url],
        sourceAccessions: lead.accession ? [lead.accession] : [],
        evidenceSnippets: [],
        milestones: [],
      };
      const row = await enrichOneAgreement(fakeAgreement, { fetchDocuments: true });
      row.verificationStatus = "DISCOVERY_LEAD_NOT_VERIFIED";
      row.discoverySource = lead.source;
      managementEnrich.push(row);
    } catch (err) {
      managementEnrich.push({ url: lead.url, error: String(err?.message || err) });
    }
  }
  writeJson(path.join(OUT, "CONTRACT_TERMS_MANAGEMENT_ENRICH.json"), {
    count: managementEnrich.length,
    results: managementEnrich,
  });
  management._enrichedWithTerms = managementEnrich.filter(
    (r) => (r.profile?.disclosedFieldCount || 0) > 0,
  ).length;

  writeJson(path.join(OUT, "CONTRACT_TERMS_ENRICH_RESULTS.json"), enrich);

  const metrics = buildQualityMetrics(enrich, management);
  metrics.managementAgreementCoverage = {
    franchiseCohort: enrich.agreementsSampled,
    managementLeadsDiscovered: (management.leads || []).filter((l) => l.url).length,
    managementLeadsEnriched: managementEnrich.length,
    managementLeadsWithDisclosedTerms: management._enrichedWithTerms,
    franchiseRelabeledAsManagement: 0,
  };
  writeJson(path.join(OUT, "CONTRACT_TERMS_QUALITY_METRICS.json"), metrics);

  const manual = manualReviewSample(enrich.results || []);
  writeJson(path.join(OUT, "CONTRACT_TERMS_MANUAL_EVIDENCE_REVIEW.json"), {
    at: new Date().toISOString(),
    sampleSize: manual.length,
    samples: manual,
  });

  // Sample hotel profiles for founder report
  const samples = (enrich.results || [])
    .slice()
    .sort((a, b) => (b.profile?.disclosedFieldCount || 0) - (a.profile?.disclosedFieldCount || 0))
    .slice(0, 10)
    .map((r) => ({
      hotelName: r.hotelName,
      city: selection.cohort.find((c) => c.contractId === r.contractId)?.city,
      stateRegion: selection.cohort.find((c) => c.contractId === r.contractId)?.stateRegion,
      brandFamily: r.brandFamily,
      agreementType: r.agreementType,
      sourceUrl: r.document?.url,
      accession: r.document?.accession,
      baselineExpiration: r.profile?.baseline?.scheduledExpiration,
      extracted: r.profile?.extracted,
      renewalStatusClass: r.renewal?.renewalStatusClass,
      renewalReasons: r.renewal?.reasons,
      nextDecisionWindow: r.decisionWindows?.nextDecisionWindow,
      disclosedFieldCount: r.profile?.disclosedFieldCount,
      incrementalBeyondExpiration: r.profile?.incrementalBeyondExpiration,
    }));
  writeJson(path.join(OUT, "CONTRACT_TERMS_SAMPLE_PROFILES.json"), { samples });

  const spend = typeof budget.snapshot === "function" ? budget.snapshot() : { spentUsd: enrich.paidUsd };
  writeJson(path.join(OUT, "CONTRACT_TERMS_PILOT_SPEND.json"), {
    ceilingUsd: MAX_PAID,
    spentUsd: spend.spentUsd ?? enrich.paidUsd,
    byProvider: spend.byProvider || { FREE_SEC: enrich.paidUsd },
    note: "Free EDGAR retrieval is $0 against the incremental ceiling",
  });

  console.log(
    JSON.stringify(
      {
        ok: true,
        phase: "ENRICH_COMPLETE",
        metrics,
        sampleHotels: samples.map((s) => s.hotelName),
        schemaProposal: "PROPOSED_PENDING_FOUNDER_META_APPLY",
        publicationState: "CUSTOMER_BLOCKED",
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
