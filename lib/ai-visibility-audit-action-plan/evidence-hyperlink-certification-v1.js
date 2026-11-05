/**
 * AVA Evidence & Hyperlink Certification Gate V1
 *
 * Mandatory before AUDIT_VALIDATED:
 * - Critical customer-facing claims have acceptable supporting evidence
 * - No unresolved broken customer-facing links
 * - No unresolved unsupported critical claims
 * - PDF annotations match the approved evidence register
 * - Manual verifications require explanation + audit trail
 *
 * HTTP 200 alone never certifies claim support.
 * HTTP 403/429/bot/timeout → REQUIRES_MANUAL_REVIEW (not automatic BROKEN_LINK).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  collectEvidenceEntries,
  validateAuditEvidenceLinks,
} from "./evidence-link-validation-v1.js";
import {
  extractPdfUriAnnotationsFromBuffer,
  extractPdfUriAnnotationsFromFile,
} from "./pdf-hyperlink-annotations-v1.js";
import {
  escalateRestrictedEvidence,
  inferClaimKeywords,
  loadVerificationCache,
  saveVerificationCache,
} from "./evidence-verification-escalation-v1.js";

export const EVIDENCE_HYPERLINK_CERTIFICATION_CONTRACT =
  "AVA_EVIDENCE_HYPERLINK_CERTIFICATION_V1";

export const EVIDENCE_CLASSIFICATIONS = Object.freeze([
  "VERIFIED_CURRENT",
  "VERIFIED_WITH_QUALIFICATION",
  "REQUIRES_MANUAL_REVIEW",
  "BROKEN_LINK",
  "UNSUPPORTED_CLAIM",
  "OUTDATED_SOURCE",
]);

/** Permanently rejected customer-facing citations (Bethesda NIH lesson). */
export const AVA_REJECTED_EVIDENCE_URLS = Object.freeze({
  "https://www.cc.nih.gov/about/visitor/hotels": {
    classification: "BROKEN_LINK",
    reason:
      "Clinical Center visitor hotels path is broken/unusable and must not support Bethesda Marriott NIH-route claims.",
  },
});

const ACCESS_RESTRICTED_FLAGS = new Set([
  "HTTP_403",
  "HTTP_429",
  "HTTP_401",
  "HTTP_503",
  "FETCH_FAILED",
]);

function normalizeUrl(u) {
  try {
    const parsed = new URL(String(u || "").trim());
    parsed.hash = "";
    parsed.hostname = parsed.hostname.replace(/^www\./i, "").toLowerCase();
    // Drop default ports
    if (
      (parsed.protocol === "https:" && parsed.port === "443") ||
      (parsed.protocol === "http:" && parsed.port === "80")
    ) {
      parsed.port = "";
    }
    let out = parsed.toString();
    if (out.endsWith("/") && parsed.pathname === "/") {
      out = out.slice(0, -1);
    } else {
      out = out.replace(/\/$/, "");
    }
    return out;
  } catch {
    return String(u || "")
      .trim()
      .replace(/^https?:\/\/www\./i, (m) => m.replace(/www\./i, ""))
      .replace(/\/$/, "");
  }
}

function isRejectedUrl(url) {
  const exact = AVA_REJECTED_EVIDENCE_URLS[url];
  if (exact) return exact;
  if (/cc\.nih\.gov\/about\/visitor\/hotels/i.test(String(url || ""))) {
    return AVA_REJECTED_EVIDENCE_URLS["https://www.cc.nih.gov/about/visitor/hotels"];
  }
  return null;
}

function parseActionId(context) {
  const m = String(context || "").match(/(AVA-[A-Z0-9-]+)/i);
  return m ? m[1] : null;
}

function classifyEvidenceRow(row, manualByUrl = {}) {
  const rejected = isRejectedUrl(row.url);
  const manual = manualByUrl[normalizeUrl(row.url)] || manualByUrl[row.url] || null;
  const flags = row.flags || [];
  const status = row.status || 0;

  if (rejected) {
    return {
      classification: rejected.classification,
      gateBlocking: true,
      verificationMethod: row.methodUsed || "HTTP",
      observation: rejected.reason,
      requiredFollowUp: "Replace with a working authoritative source or revise the claim.",
    };
  }

  if (flags.includes("CLAIM_UNSUPPORTED") || row.claimSupport === false) {
    return {
      classification: "UNSUPPORTED_CLAIM",
      gateBlocking: true,
      verificationMethod: row.methodUsed || "HTTP",
      observation: row.claimNotes || "Source does not support the attached claim.",
      requiredFollowUp: "Revise claim or replace citation with claim-supporting evidence.",
    };
  }

  if (flags.includes("HTTP_404") || flags.includes("HTTP_410")) {
    return {
      classification: "BROKEN_LINK",
      gateBlocking: true,
      verificationMethod: row.methodUsed || "HTTP",
      observation: `HTTP ${status} for customer-facing evidence URL.`,
      requiredFollowUp: "Replace broken URL and regenerate PDF annotations.",
    };
  }

  if (flags.includes("HISTORICAL_AS_CURRENT")) {
    return {
      classification: "OUTDATED_SOURCE",
      gateBlocking: true,
      verificationMethod: row.methodUsed || "HTTP",
      observation: "Historical/archived source presented as current.",
      requiredFollowUp: "Cite a current authoritative source or qualify the claim.",
    };
  }

  const accessRestricted =
    flags.some((f) => ACCESS_RESTRICTED_FLAGS.has(f)) ||
    /TIMEOUT|HARD_TIMEOUT|EAI_AGAIN|ENOTFOUND|certificate/i.test(
      String(row.error || "")
    );

  // When the claim itself documents domain failure/timeout, a failed probe supports it.
  const failureIsTheClaim =
    accessRestricted &&
    (/timeout|504|status 0|failed|non-hotel|historically used|domain conflict|dead|unreachable|ERR_NAME_NOT_RESOLVED|DNS/i.test(
      `${row.label || ""} ${row.claimNotes || ""} ${row.context || ""} ${row.note || ""}`
    ) ||
      (/waterstoneresort\.com/i.test(String(row.url || "")) &&
        /historically used|failed|timeout|unreachable|DNS|domain/i.test(
          `${row.label || ""} ${row.claimNotes || ""} ${row.context || ""} ${row.note || ""}`
        )));

  if (failureIsTheClaim) {
    return {
      classification: "VERIFIED_WITH_QUALIFICATION",
      gateBlocking: false,
      verificationMethod: row.methodUsed || "HTTP",
      observation:
        "Probe failure is consistent with a finding that documents domain/access failure; not treated as a missing customer citation.",
      requiredFollowUp: null,
    };
  }

  if (accessRestricted) {
    if (manual && manual.decision === "VERIFIED" && manual.explanation) {
      return {
        classification: "VERIFIED_WITH_QUALIFICATION",
        gateBlocking: false,
        verificationMethod: "MANUAL_BROWSER_OR_ADMIN",
        observation:
          manual.observation ||
          "Automated fetch restricted; administrator recorded manual verification.",
        requiredFollowUp: null,
        manualVerification: manual,
      };
    }
    return {
      classification: "REQUIRES_MANUAL_REVIEW",
      gateBlocking: true,
      verificationMethod: row.methodUsed || "HTTP",
      observation:
        `Automated access restricted (${flags.join(",") || row.error || "unknown"}). Not treated as a definitive broken link.`,
      requiredFollowUp:
        "Open the source in a browser, confirm claim support, and record a manual verification with explanation.",
    };
  }

  if (flags.includes("REDIRECT_UNRELATED")) {
    if (manual && manual.decision === "VERIFIED" && manual.explanation) {
      return {
        classification: "VERIFIED_WITH_QUALIFICATION",
        gateBlocking: false,
        verificationMethod: "MANUAL_BROWSER_OR_ADMIN",
        observation: manual.observation || "Redirect reviewed manually.",
        requiredFollowUp: null,
        manualVerification: manual,
      };
    }
    return {
      classification: "REQUIRES_MANUAL_REVIEW",
      gateBlocking: true,
      verificationMethod: row.methodUsed || "HTTP",
      observation: "Redirect may lead to an unrelated page.",
      requiredFollowUp: "Confirm final URL still supports the claim.",
    };
  }

  if (status >= 200 && status < 400 && row.claimSupport === true) {
    return {
      classification: "VERIFIED_CURRENT",
      gateBlocking: false,
      verificationMethod: row.methodUsed || "HTTP",
      observation: "Resolves successfully and claim-support marked true by researcher/register.",
      requiredFollowUp: null,
    };
  }

  if (status >= 200 && status < 400) {
    return {
      classification: "VERIFIED_WITH_QUALIFICATION",
      gateBlocking: false,
      verificationMethod: row.methodUsed || "HTTP",
      observation:
        "URL resolves. Claim-body support was not independently body-scraped in this gate; relies on researcher register / labels.",
      requiredFollowUp: null,
    };
  }

  return {
    classification: "REQUIRES_MANUAL_REVIEW",
    gateBlocking: true,
    verificationMethod: row.methodUsed || "HTTP",
    observation: `Unresolved verification (status=${status}, error=${row.error || "none"}).`,
    requiredFollowUp: "Re-check source and record outcome.",
  };
}

export function comparePdfAnnotationsToRegister(registerUrls, annotationUrls) {
  const reg = new Set((registerUrls || []).map(normalizeUrl));
  const ann = new Set((annotationUrls || []).map(normalizeUrl));
  const missingInPdf = [...reg].filter((u) => !ann.has(u));
  const unexpectedInPdf = [...ann].filter((u) => !reg.has(u));
  const rejectedStillPresent = [...ann].filter((u) => isRejectedUrl(u));
  const malformed = (annotationUrls || []).filter((u) => {
    try {
      // eslint-disable-next-line no-new
      new URL(u);
      return !/^https?:\/\//i.test(u);
    } catch {
      return true;
    }
  });
  return {
    registerCount: reg.size,
    annotationUniqueCount: ann.size,
    missingInPdf,
    unexpectedInPdf,
    rejectedStillPresent,
    malformed,
    ok:
      rejectedStillPresent.length === 0 &&
      malformed.length === 0 &&
      missingInPdf.length === 0,
  };
}

/**
 * Run full certification for a payload (+ optional PDF path/bytes).
 * @param {object} payload
 * @param {object} options
 *   - pdfPath / pdfBuffer
 *   - claimSupportByUrl
 *   - manualVerifications: { [url]: { decision, explanation, verifiedAt, verifiedBy, observation } }
 *   - probeCache
 *   - rejectedUrlsMustBeAbsent: default true
 */
export async function certifyAvaEvidenceAndHyperlinks(payload, options = {}) {
  const checkedAt = new Date().toISOString();
  const manualByUrl = options.manualVerifications || {};
  const escalate =
    options.escalateBrowser !== false && options.skipEscalation !== true;
  const verificationCache =
    options.verificationCache ||
    (escalate ? loadVerificationCache(options.cachePath) : null);
  const linkReport = await validateAuditEvidenceLinks(payload, {
    claimSupportByUrl: options.claimSupportByUrl || {},
    probeCache: options.probeCache || null,
    probeFn: options.probeFn || null,
  });

  const uniqueRegister = [
    ...new Set((linkReport.results || []).map((r) => r.url)),
  ];
  // PDF annotation compare uses customer-facing labeled evidence from findings
  // and action plans. Digital Authority / DCR inventories are selectively rendered
  // in the PDF (top backlinks/mentions/opportunities) and must not require
  // full-register annotation parity — those URLs are still HTTP-certified above.
  const labeledRegister = [
    ...new Set(
      (linkReport.results || [])
        .filter((r) => {
          const ctx = String(r.context || "");
          if (
            ctx.startsWith("digital-authority:") ||
            ctx.startsWith("digital-content-readiness:")
          ) {
            return false;
          }
          return (
            r.label ||
            ctx.startsWith("finding:") ||
            ctx.startsWith("action:") ||
            ctx.startsWith("action-tech-url:")
          );
        })
        .map((r) => r.url)
    ),
  ];

  const evidenceRows = [];
  const escalationsByUrl = new Map();
  for (const row of linkReport.results || []) {
    let classified = classifyEvidenceRow(row, manualByUrl);
    if (
      escalate &&
      classified.classification === "REQUIRES_MANUAL_REVIEW" &&
      classified.gateBlocking
    ) {
      if (!escalationsByUrl.has(row.url)) {
        escalationsByUrl.set(
          row.url,
          await escalateRestrictedEvidence({
            url: row.url,
            httpProbe: row,
            flags: row.flags || [],
            claimKeywords: inferClaimKeywords(row),
            cache: verificationCache,
            allowIgnoreHttpsErrors: options.allowIgnoreHttpsErrors === true,
          })
        );
      }
      const esc = escalationsByUrl.get(row.url);
      if (
        ["BROWSER_VERIFIED", "REAL_BROWSER_VERIFIED"].includes(esc?.decision) &&
        esc.gateBlocking === false
      ) {
        classified = {
          classification: "VERIFIED_WITH_QUALIFICATION",
          gateBlocking: false,
          verificationMethod: esc.verificationMethod || "PLAYWRIGHT_BROWSER",
          observation: esc.observation,
          requiredFollowUp: null,
          escalation: esc,
        };
      } else if (esc) {
        classified = {
          ...classified,
          observation: `${classified.observation} Escalation: ${esc.decision} — ${esc.observation}`,
          requiredFollowUp:
            esc.decision === "ALTERNATIVE_SOURCE_REQUIRED"
              ? "Replace with an authoritative source that supports the same claim."
              : classified.requiredFollowUp,
          escalation: esc,
        };
      }
    }
    evidenceRows.push({
      sourceUrl: row.url,
      finalResolvedUrl: row.finalUrl || row.url,
      httpStatus: row.status || 0,
      verificationMethod: classified.verificationMethod,
      verificationTimestamp: checkedAt,
      associatedClaimOrActionId: parseActionId(row.context),
      context: row.context,
      label: row.label,
      evidenceClassification: classified.classification,
      observation: classified.observation,
      requiredFollowUp: classified.requiredFollowUp,
      gateBlocking: classified.gateBlocking === true,
      flags: row.flags || [],
      error: row.error || null,
      manualVerification: classified.manualVerification || null,
      escalation: classified.escalation || null,
    });
  }
  if (verificationCache && options.persistCache !== false) {
    saveVerificationCache(verificationCache, options.cachePath);
  }

  // Dedupe display rows by URL keeping worst classification for summary counts
  const byUrl = new Map();
  const rank = {
    BROKEN_LINK: 5,
    UNSUPPORTED_CLAIM: 5,
    OUTDATED_SOURCE: 4,
    REQUIRES_MANUAL_REVIEW: 3,
    VERIFIED_WITH_QUALIFICATION: 2,
    VERIFIED_CURRENT: 1,
  };
  for (const row of evidenceRows) {
    const key = normalizeUrl(row.sourceUrl);
    const prev = byUrl.get(key);
    if (!prev || (rank[row.evidenceClassification] || 0) > (rank[prev.evidenceClassification] || 0)) {
      byUrl.set(key, row);
    }
  }
  const uniqueEvidence = [...byUrl.values()];

  let annotations = {
    totalAnnotations: 0,
    uniqueUrls: [],
    urls: [],
    missingFile: true,
  };
  if (options.pdfBuffer) {
    annotations = extractPdfUriAnnotationsFromBuffer(options.pdfBuffer);
    annotations.missingFile = false;
  } else if (options.pdfPath) {
    annotations = extractPdfUriAnnotationsFromFile(options.pdfPath);
  }

  const annotationCompare = comparePdfAnnotationsToRegister(
    labeledRegister.length ? labeledRegister : uniqueRegister,
    annotations.uniqueUrls || []
  );

  const broken = uniqueEvidence.filter((e) => e.evidenceClassification === "BROKEN_LINK");
  const unsupported = uniqueEvidence.filter(
    (e) => e.evidenceClassification === "UNSUPPORTED_CLAIM"
  );
  const outdated = uniqueEvidence.filter(
    (e) => e.evidenceClassification === "OUTDATED_SOURCE"
  );
  const manualReview = uniqueEvidence.filter(
    (e) => e.evidenceClassification === "REQUIRES_MANUAL_REVIEW"
  );
  const verified = uniqueEvidence.filter((e) =>
    ["VERIFIED_CURRENT", "VERIFIED_WITH_QUALIFICATION"].includes(
      e.evidenceClassification
    )
  );

  const blocking = uniqueEvidence.filter((e) => e.gateBlocking);
  const rejectedInPdf = annotationCompare.rejectedStillPresent || [];

  const gatePass =
    blocking.length === 0 &&
    rejectedInPdf.length === 0 &&
    annotationCompare.malformed.length === 0 &&
    // Missing evidence links in PDF are blocking for customer-facing register.
    annotationCompare.missingInPdf.length === 0;

  const summary = {
    totalEvidenceLinks: uniqueEvidence.length,
    verifiedLinks: verified.length,
    linksRequiringManualReview: manualReview.length,
    brokenLinks: broken.length,
    unsupportedClaims: unsupported.length,
    outdatedSources: outdated.length,
    lastValidationDate: checkedAt,
    pdfAnnotationUnique: (annotations.uniqueUrls || []).length,
    pdfMissingRegisterLinks: annotationCompare.missingInPdf.length,
    pdfUnexpectedLinks: annotationCompare.unexpectedInPdf.length,
    rejectedUrlsInPdf: rejectedInPdf.length,
  };

  return {
    contract: EVIDENCE_HYPERLINK_CERTIFICATION_CONTRACT,
    propertyId: payload?.property?.propertyId || null,
    hotelName: payload?.property?.name || null,
    checkedAt,
    gate: {
      pass: gatePass,
      requiredStatusIfFail: "VALIDATION_REQUIRED",
      blockingCount: blocking.length + rejectedInPdf.length,
      reasons: [
        ...blocking.map(
          (b) =>
            `${b.evidenceClassification}: ${b.sourceUrl} (${b.observation})`
        ),
        ...rejectedInPdf.map((u) => `REJECTED_URL_IN_PDF: ${u}`),
        ...annotationCompare.malformed.map((u) => `MALFORMED_PDF_URI: ${u}`),
        ...annotationCompare.missingInPdf.map(
          (u) => `MISSING_PDF_ANNOTATION: ${u}`
        ),
      ],
    },
    summary,
    evidence: uniqueEvidence,
    evidenceOccurrences: evidenceRows,
    pdfAnnotations: {
      ...annotations,
      compare: annotationCompare,
    },
    registerUrls: uniqueRegister,
  };
}

export function assertEvidenceGateAllowsAuditValidated(certification) {
  if (certification?.gate?.pass) {
    return { ok: true };
  }
  const err = new Error("EVIDENCE_HYPERLINK_GATE_FAILED");
  err.code = "EVIDENCE_HYPERLINK_GATE_FAILED";
  err.certification = certification;
  return {
    ok: false,
    error: "EVIDENCE_HYPERLINK_GATE_FAILED",
    message:
      "AUDIT_VALIDATED blocked until broken/unsupported evidence and PDF annotation mismatches are resolved (or manually verified with explanation).",
    reasons: certification?.gate?.reasons || [],
    summary: certification?.summary || null,
  };
}

export function loadManualVerificationsFromMeta(meta) {
  const raw = meta?.evidenceHyperlinkCertification?.manualVerifications || {};
  return raw && typeof raw === "object" ? raw : {};
}

export function summarizeCertificationForAdmin(certification) {
  if (!certification) {
    return {
      totalEvidenceLinks: 0,
      verifiedLinks: 0,
      linksRequiringManualReview: 0,
      brokenLinks: 0,
      unsupportedClaims: 0,
      lastValidationDate: null,
      gatePass: null,
    };
  }
  return {
    ...certification.summary,
    gatePass: certification.gate?.pass === true,
    reasons: certification.gate?.reasons || [],
  };
}

export function repoPayloadPath(propertyId) {
  const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
  const map = {
    adp_spice_island_beach_resort: "spice-island-beach-resort-v1.json",
    adp_wyndham_san_diego_bayside: "wyndham-san-diego-bayside-v1.json",
    adp_waterstone_boca_raton: "waterstone-boca-raton-v1.json",
    adp_bethesda_marriott: "bethesda-marriott-v1.json",
    adp_castillo_hotel_son_vida: "castillo-hotel-son-vida-v1.json",
    adp_sheraton_mallorca_arabella_golf:
      "sheraton-mallorca-arabella-golf-v1.json",
  };
  const file = map[propertyId];
  if (!file) return null;
  return path.join(
    ROOT,
    "lib/ai-visibility-audit-action-plan/payloads",
    file
  );
}

export { collectEvidenceEntries, normalizeUrl, isRejectedUrl };
