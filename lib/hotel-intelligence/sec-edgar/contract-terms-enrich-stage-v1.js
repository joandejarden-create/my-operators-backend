/**
 * ENRICH_CONTRACT_TERMS — reusable Hotel Intelligence Factory stage body.
 * Cache-first document retrieval; structured extraction; renewal classification;
 * decision windows; proposed research updates (no unauthorized Meta schema apply).
 */

import { extractContractTermsFromText, enrichAgreementProfile } from "./contract-terms-extractor-v1.js";
import { classifyRenewalOutcome } from "./contract-terms-renewal-classifier-v1.js";
import { calculateDecisionWindows } from "./contract-terms-decision-windows-v1.js";
import { buildAdditiveSchemaDiffPacket } from "./contract-terms-schema-proposal-v4.js";
import { createSecEdgarClient } from "./edgar-client-v1.js";

export const ENRICH_CONTRACT_TERMS_STAGE = "ENRICH_CONTRACT_TERMS";
export const CONTRACT_TERMS_ENRICH_STAGE_V1 = "contract-terms-enrich-stage-v1";

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<\/(p|div|br|li|h\d|tr)>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

function hotelWindow(text, hotelName, radius = 2500) {
  const t = String(text || "");
  const name = String(hotelName || "").trim();
  if (!name || name.length < 4) return t.slice(0, 12000);
  const idx = t.toLowerCase().indexOf(name.toLowerCase().slice(0, Math.min(40, name.length)));
  if (idx < 0) {
    // try first significant token
    const token = name.split(/\s+/).filter((w) => w.length > 4)[1] || name.split(/\s+/)[0];
    const i2 = token ? t.toLowerCase().indexOf(token.toLowerCase()) : -1;
    if (i2 < 0) return t.slice(0, 12000);
    return t.slice(Math.max(0, i2 - radius), i2 + radius);
  }
  return t.slice(Math.max(0, idx - radius), idx + radius);
}

/**
 * Enrich one agreement row.
 */
export async function enrichOneAgreement(agreement, opts = {}) {
  const edgar = opts.edgarClient || createSecEdgarClient({ env: opts.env || process.env });
  const url = (agreement.sourceUrls || [])[0] || null;
  const accession = (agreement.sourceAccessions || [])[0] || null;
  const snippets = (agreement.evidenceSnippets || []).join("\n");
  const baselineMs = (agreement.milestones || [])[0] || {};

  let docText = "";
  let retrieved = false;
  let fromCache = false;
  let retrievalError = null;
  let paidCostUsd = 0;

  if (url && opts.fetchDocuments !== false) {
    try {
      const res = await edgar.request(url, {
        accept: "text/html,application/xhtml+xml,*/*",
        maxAgeMs: 30 * 24 * 60 * 60 * 1000,
      });
      if (res.ok && (res.text || res.body)) {
        docText = stripHtml(typeof res.text === "string" ? res.text : String(res.body || ""));
        retrieved = true;
        fromCache = Boolean(res.fromCache);
      } else {
        retrievalError = res.error || `HTTP_${res.status}`;
      }
    } catch (err) {
      retrievalError = String(err?.message || err);
    }
  }

  // Property window + global franchise/management terms sections (clearly separated)
  const propertyText = hotelWindow(docText, agreement.hotelName);
  const globalTermsMatch = docText.match(
    /(?:franchise\s+agreement|renewal\s+options?|renewal\s+terms?|termination\s+(?:for|without)\s+cause|management\s+agreement|operating\s+term|initial\s+term|base\s+management\s+fee|incentive\s+fee)[\s\S]{0,3500}/gi,
  );
  const globalTermsText = (globalTermsMatch || []).slice(0, 8).join("\n\n");
  // Management / EX-10 exhibits: prefer large front section when no hotel identity
  const isManagementLead =
    String(agreement.agreementType || "").toUpperCase() === "MANAGEMENT" ||
    /management agreement|operating term|base management fee/i.test(docText.slice(0, 5000));
  const leadFront = isManagementLead ? docText.slice(0, 60000) : "";
  const localText = [snippets, propertyText, leadFront].filter(Boolean).join("\n\n");
  const extraction = extractContractTermsFromText(localText, {
    sourceUrl: url,
    accession,
    filingDate: null,
    confidence: retrieved ? "MEDIUM" : "LOW",
  });
  const portfolioExtraction = extractContractTermsFromText(globalTermsText, {
    sourceUrl: url,
    accession,
    confidence: "LOW",
  });
  // Merge portfolio disclosures only where property-level is NOT_DISCLOSED
  for (const [k, v] of Object.entries(portfolioExtraction.terms || {})) {
    if ((extraction.terms[k] == null || extraction.terms[k] === false) && v != null && v !== false) {
      extraction.terms[k] = v;
      const pe = (portfolioExtraction.evidence || []).find((e) => e.field === k && e.value != null);
      if (pe) {
        extraction.evidence.push({
          ...pe,
          field: k,
          confidence: "LOW",
          verificationStatus: "PORTFOLIO_CONTEXT_NOT_PROPERTY_SPECIFIC",
          note: "Extracted from filing-level franchise/management language; not hotel-attributed",
        });
      }
    }
  }
  extraction.disclosedFieldCount = extraction.evidence.filter(
    (e) => e.value != null && e.value !== false,
  ).length;
  const profile = enrichAgreementProfile(agreement, extraction, baselineMs);
  profile.portfolioContextUsed = Boolean(
    (extraction.evidence || []).some((e) => e.verificationStatus === "PORTFOLIO_CONTEXT_NOT_PROPERTY_SPECIFIC"),
  );

  const laterText = String(opts.laterDocumentText || "");
  const renewal = classifyRenewalOutcome({
    originalText: localText,
    laterText,
    sameBrandObserved: Boolean(opts.sameBrandObserved),
    expirationStillCited: Boolean(baselineMs.primaryDate),
    scheduledExpiration: agreement.scheduledExpiration || baselineMs.primaryDate,
    asOf: opts.asOf || new Date().toISOString(),
  });

  const dateObservation = {
    contractId: agreement.contractId,
    hotelName: agreement.hotelName,
    agreementType: agreement.agreementType,
    primaryDate: agreement.scheduledExpiration || baselineMs.primaryDate,
    dateMeaning: baselineMs.dateMeaning || null,
    evidenceSnippet: (agreement.evidenceSnippets || []).join("\n"),
    evidenceSnippets: agreement.evidenceSnippets || [],
    sourceUrls: agreement.sourceUrls || [],
  };
  const windows = calculateDecisionWindows({
    scheduledExpiration: agreement.scheduledExpiration || baselineMs.primaryDate,
    renewalNoticeDays: extraction.terms.renewalNoticeDays,
    renewalOptionsCount: extraction.terms.renewalOptionsCount,
    terminationWithoutCause: Boolean(extraction.terms.terminationWithoutCause),
    performanceTest: Boolean(extraction.terms.performanceTest),
    changeOfControl: Boolean(extraction.terms.changeOfControl),
    dateObservation,
    hotelAttributed: true,
    // Portfolio-derived term language must not invent hotel-level decision windows by itself;
    // scheduledExpiration still requires hotel-attributed date semantic pass.
    portfolioLevelOnly: false,
  });
  profile.dateSemanticGate = windows.dateSemanticGate || null;
  profile.decisionWindowsBlocked = Boolean(windows.blocked);

  const renewalSummaryParts = [];
  if (extraction.terms.renewalOptionsCount != null) {
    renewalSummaryParts.push(`${extraction.terms.renewalOptionsCount} option(s)`);
  }
  if (extraction.terms.renewalOptionYearsEach != null) {
    renewalSummaryParts.push(`${extraction.terms.renewalOptionYearsEach} years each`);
  }
  if (extraction.terms.renewalMode) renewalSummaryParts.push(`mode=${extraction.terms.renewalMode}`);
  if (extraction.terms.renewalNoticeDays != null) {
    renewalSummaryParts.push(`notice=${extraction.terms.renewalNoticeDays}d`);
  }

  const terminationParts = [];
  if (extraction.terms.terminationForCause) terminationParts.push("for-cause");
  if (extraction.terms.terminationWithoutCause) terminationParts.push("without-cause");
  if (extraction.terms.performanceTest) terminationParts.push("performance-test");
  if (extraction.terms.changeOfControl) terminationParts.push("change-of-control");

  const proposedExistingFieldUpdates = {
    // Only reuse fields that already exist — additive fields stay in research JSON
    "Renewal Options Summary": renewalSummaryParts.length
      ? renewalSummaryParts.join("; ")
      : "NOT_DISCLOSED",
    "Termination Rights Summary": terminationParts.length
      ? terminationParts.join("; ")
      : "NOT_DISCLOSED",
    "Term Years": extraction.terms.originalTermYears,
    "Notes / Quarantine": [
      `TERMS_ENRICH_V1`,
      `renewalStatus=${renewal.renewalStatusClass}`,
      windows.nextDecisionWindow
        ? `nextWindow=${windows.nextDecisionWindow.kind}@${windows.nextDecisionWindow.date || "UNDETERMINED"}`
        : null,
      `publication=CUSTOMER_BLOCKED`,
    ]
      .filter(Boolean)
      .join(" | "),
  };

  return {
    version: CONTRACT_TERMS_ENRICH_STAGE_V1,
    stage: ENRICH_CONTRACT_TERMS_STAGE,
    contractId: agreement.contractId,
    hotelName: agreement.hotelName,
    agreementType: agreement.agreementType,
    brandFamily: agreement.brandFamily,
    document: {
      url,
      accession,
      retrieved,
      fromCache,
      retrievalError,
      textChars: docText.length,
      paidCostUsd,
    },
    profile,
    renewal,
    decisionWindows: windows,
    proposedExistingFieldUpdates,
    proposedAdditiveFields: {
      note: "NOT_APPLIED — awaiting founder Meta API authorization",
      schema: buildAdditiveSchemaDiffPacket().addFields,
      values: {
        RenewalOptionsCount: extraction.terms.renewalOptionsCount,
        RenewalOptionYearsEach: extraction.terms.renewalOptionYearsEach,
        RenewalMode: extraction.terms.renewalMode,
        RenewalNoticeDays: extraction.terms.renewalNoticeDays,
        RenewalStatusClass: renewal.renewalStatusClass,
        DecisionWindowNextDate: windows.nextDecisionWindow?.date || null,
        DecisionWindowClass: windows.nextDecisionWindow?.disclosureClass || null,
        FinancialTermsSummary: [
          extraction.terms.royaltyMention ? `royalty ${extraction.terms.royaltyMention}` : null,
          extraction.terms.managementFeeMention
            ? `mgmt fee ${extraction.terms.managementFeeMention}`
            : null,
        ]
          .filter(Boolean)
          .join("; ") || "NOT_DISCLOSED",
      },
    },
    publicationState: "CUSTOMER_BLOCKED",
    hpcWrites: false,
  };
}

/**
 * Run ENRICH_CONTRACT_TERMS over a cohort.
 */
export async function runEnrichContractTermsStage(opts = {}) {
  const cohort = opts.cohort || [];
  const results = [];
  const errors = [];
  let retrieved = 0;
  let cacheHits = 0;
  let paidUsd = 0;

  for (const agreement of cohort) {
    if (opts.budget && typeof opts.budget.canSpend === "function") {
      const gate = opts.budget.canSpend(0);
      if (!gate.ok && gate.error === "BUDGET_STOPPED") {
        errors.push({ contractId: agreement.contractId, error: "BUDGET_STOPPED" });
        break;
      }
    }
    try {
      const row = await enrichOneAgreement(agreement, opts);
      if (row.document.retrieved) retrieved += 1;
      if (row.document.fromCache) cacheHits += 1;
      paidUsd += Number(row.document.paidCostUsd) || 0;
      results.push(row);
      if (opts.onProgress) opts.onProgress(row);
    } catch (err) {
      errors.push({ contractId: agreement.contractId, error: String(err?.message || err) });
    }
  }

  return {
    version: CONTRACT_TERMS_ENRICH_STAGE_V1,
    stage: ENRICH_CONTRACT_TERMS_STAGE,
    status: "ENRICH_CONTRACT_TERMS_COMPLETE",
    agreementsSampled: cohort.length,
    enriched: results.length,
    documentsRetrieved: retrieved,
    cacheHits,
    paidUsd,
    errors,
    results,
    schemaProposal: buildAdditiveSchemaDiffPacket(),
    publicationState: "CUSTOMER_BLOCKED",
    validationClaim: "RESEARCH_PILOT_LOCAL",
    note: "Additive Airtable fields not applied; HPC not written; CUSTOMER_BLOCKED maintained",
  };
}

export default {
  ENRICH_CONTRACT_TERMS_STAGE,
  enrichOneAgreement,
  runEnrichContractTermsStage,
};
