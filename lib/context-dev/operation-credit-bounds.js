/**
 * Context.dev operation credit reservation bounds.
 *
 * Reservation uses a documented upper bound for the exact operation + options.
 * Settlement uses provider key_metadata.credits_consumed (per-request) when present.
 *
 * Official scrape markdown docs (https://docs.context.dev/api-reference/web-scraping/markdown):
 * - Header: "1 Credit With actions: 2 Credits"
 * - Billing table: 200 / 404 billed at 1 (or 2 with actions); partial return-partial at base 1
 * - Image enrichment guide: enriched image features → 5 credits total
 * - 400 / 401 / 403 / 408 / 413 / 415 / 429 / 500 → not billed (HTTP status only)
 *
 * Zero-charge exceptions require an attributable numeric provider HTTP status matching
 * the saved billing-table evidence. Generic TIMEOUT / ETIMEDOUT / abort / socket-timeout
 * client strings must NOT be treated as HTTP 408.
 *
 * Per-request key_metadata.credits_consumed always takes precedence over inferred rules.
 *
 * Pilot scrape contradiction (UNRESOLVED): two failed scrapes reported credits_consumed=10
 * while reserved at documented 1 for url+useMainContentOnly (no actions/OCR). That does
 * NOT authorize inventing a 10-credit scrape maximum. hard_bound_proven remains false.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const CONTEXT_DEV_CREDIT_BOUNDS_VERSION = "context-dev-credit-bounds-v3";

/** Documented schedule costs (reservation upper bounds + estimates). */
export const CONTEXT_DEV_DOCUMENTED_COSTS = Object.freeze({
  search_per_10_results: 1,
  scrape_markdown: 1,
  scrape_markdown_with_actions: 2,
  scrape_markdown_enriched_images: 5,
  extract: 10,
  brand_retrieve: 10,
});

const BILLING_EVIDENCE_REL =
  "docs/data-intelligence/context-dev-billing-evidence/scrape-markdown-http-billing-table-2026-09-19.json";

function loadScrapeBillingEvidence() {
  try {
    const here = path.dirname(fileURLToPath(import.meta.url));
    const candidates = [
      path.resolve(here, "../../", BILLING_EVIDENCE_REL),
      path.resolve(process.cwd(), BILLING_EVIDENCE_REL),
    ];
    for (const p of candidates) {
      if (fs.existsSync(p)) {
        return JSON.parse(fs.readFileSync(p, "utf8"));
      }
    }
  } catch {
    /* evidence optional at runtime; constants below remain authoritative */
  }
  return null;
}

const _billingEvidence = loadScrapeBillingEvidence();

export const SCRAPE_MARKDOWN_BOUND_SOURCE = Object.freeze({
  docs_url: "https://docs.context.dev/api-reference/web-scraping/markdown",
  enrichment_docs_url: "https://docs.context.dev/guides/scrape-websites-to-markdown",
  billing_evidence_path: BILLING_EVIDENCE_REL,
  billing_evidence_retrieval_date:
    _billingEvidence?.retrieval_date || "2026-09-19",
  conditions: {
    base_no_actions_no_enrichment: 1,
    with_actions: 2,
    with_enriched_images: 5,
    http_not_billed: [400, 401, 403, 408, 413, 415, 429, 500],
    http_billed: [200, 404],
  },
  note:
    "Bound schedule applies to GET /web/scrape/markdown with the stated options. Pilot wrapper contextDevScrapeMarkdown sends url + useMainContentOnly + optional maxAgeMs — no actions, no enriched images, no OCR → documented schedule 1. Provider-reported credits_consumed=10 on two failed pilot scrapes leaves hard_bound_proven=false.",
});

/**
 * Unresolved pilot contradiction: documented schedule vs provider-confirmed spend.
 * Does not invent a larger reservation maximum.
 */
export const SCRAPE_MARKDOWN_BOUND_CONTRADICTION = Object.freeze({
  status: "UNRESOLVED",
  hard_bound_proven: false,
  pilot_id: "pilot_763f42999f6bf0bd",
  observed_ops: [
    {
      work_key: "context_dev:scrape:c1e2d7d8efc5339463ccf83d",
      hotel: "Caesar Business Sao Paulo Paulista",
      op: "scrape",
      reserved_credits: 1,
      key_metadata_credits_consumed: 10,
      ok: false,
      reason: "[object Object]",
      request_id: null,
      wrapper: "contextDevScrapeMarkdown",
      request_options: ["url", "useMainContentOnly", "maxAgeMs"],
      sdk_maxRetries: 0,
      events: ["pre_dispatch/RESERVED", "call_started/IN_FLIGHT", "provider_failed/FAILED"],
      case_id: "rc_rec09k8RdHgiAAKsG_03dddc14",
      url: "https://all.accor.com/hotel/8939/index.en.shtml",
      utc_failed: "2026-09-19T08:41:59.830Z",
    },
    {
      work_key: "context_dev:scrape:eceb066564a97c817c5b4553",
      hotel: "Alles Blau",
      op: "scrape",
      reserved_credits: 1,
      key_metadata_credits_consumed: 10,
      ok: false,
      reason: "[object Object]",
      request_id: null,
      wrapper: "contextDevScrapeMarkdown",
      request_options: ["url", "useMainContentOnly", "maxAgeMs"],
      sdk_maxRetries: 0,
      events: ["pre_dispatch/RESERVED", "call_started/IN_FLIGHT", "provider_failed/FAILED"],
      case_id: "rc_rec00eC8DN3Ow0hFo_99c62e75",
      url: "http://hotelallesblau.com.br/index.html",
      utc_failed: "2026-09-19T08:43:50.774Z",
      remaining_anomaly:
        "sibling scrape remaining=6787 then this failure remaining=6778 then search remaining=6786 — inconsistent with a true −10 org debit",
    },
  ],
  documented_schedule_for_dispatched_shape: 1,
  explanation:
    "Saved ops show one journaled scrape each (maxRetries=0), no OCR/actions in the wrapper, and provider key_metadata.credits_consumed=10 on failure. Official docs list base 1 / actions 2 / enriched images 5, plus OCR +1/page only when OCR is enabled (not requested); docs also state non-success HTTP is unbilled. No request_id; reason previously stringified as [object Object]. Cannot prove the documented 1 is a hard ceiling for this dispatch shape; cannot invent a 10-credit scrape max. Historical reconciled total 54 preserved (includes these confirmed 10+10).",
  pilot_dispatch_posture: "BOUND_UNPROVEN — do not claim hard ceiling; overrun blocks further Context.dev after the fact; five-hotel pilot not ready until resolved or scrape explicitly refused by operator policy.",
});

/**
 * Resolve reservation upper bound for a Context.dev operation.
 * @returns {{ ok: boolean, credits?: number, reason?: string, bound_source?: string, documented_estimate?: number, block_if_unbounded?: boolean, docs_url?: string, hard_bound_proven?: boolean }}
 */
export function resolveContextDevCreditReservation(meta = {}) {
  const provider = String(meta.provider || "").toLowerCase();
  if (provider === "serpapi") {
    return { ok: true, credits: 0, bound_source: "serpapi_not_context_dev_credits" };
  }

  const op = String(meta.op || meta.operation || "").toLowerCase();
  const actions = meta.actions || meta.scrape_actions || null;
  const hasActions = Array.isArray(actions) && actions.length > 0;
  const enrichedImages =
    meta.enriched_images === true ||
    meta.include_enriched_images === true ||
    String(meta.image_mode || "").toLowerCase() === "enriched";
  const ocrEnabled =
    meta.ocr === true ||
    meta.enable_ocr === true ||
    meta.pdfOpts?.ocr === true ||
    meta.pdf_opts?.ocr === true;

  if (op === "search" || op === "domain_search") {
    const numResults = Number(meta.num_results ?? meta.numResults ?? 10) || 10;
    const credits = Math.max(
      1,
      Math.ceil(numResults / 10) * CONTEXT_DEV_DOCUMENTED_COSTS.search_per_10_results
    );
    const explicit = Number(meta.estimated_credits);
    const reserved = Number.isFinite(explicit) && explicit > credits ? explicit : credits;
    return {
      ok: true,
      credits: reserved,
      documented_estimate: credits,
      bound_source: "documented_search_per_10_results",
      docs_url: "https://docs.context.dev/api-reference/web-scraping/search",
      hard_bound_proven: true,
    };
  }

  if (op === "scrape" || op === "scrape_markdown") {
    // OCR-enabled scrapes have per-page variable cost — not a fixed documented ceiling.
    if (ocrEnabled) {
      return {
        ok: false,
        reason: "CONTEXT_DEV_SCRAPE_OCR_UNBOUNDED",
        block_if_unbounded: true,
        message:
          "OCR is billed per page recovered on top of base scrape cost; no fixed upper bound without an explicit page cap.",
        docs_url: SCRAPE_MARKDOWN_BOUND_SOURCE.docs_url,
        hard_bound_proven: false,
        contradiction_status: SCRAPE_MARKDOWN_BOUND_CONTRADICTION.status,
      };
    }
    let documented = CONTEXT_DEV_DOCUMENTED_COSTS.scrape_markdown;
    let boundSource = "documented_scrape_markdown_1_credit";
    if (enrichedImages) {
      documented = CONTEXT_DEV_DOCUMENTED_COSTS.scrape_markdown_enriched_images;
      boundSource = "documented_scrape_markdown_enriched_images_5";
    } else if (hasActions) {
      documented = CONTEXT_DEV_DOCUMENTED_COSTS.scrape_markdown_with_actions;
      boundSource = "documented_scrape_markdown_with_actions_2";
    }
    // Reject unsupported "observed ceiling" — observation is not a documented max.
    if (
      meta.use_scrape_reservation_ceiling === true ||
      meta.scrape_reservation_ceiling === true ||
      String(meta.reservation_mode || "").toLowerCase() === "pilot_observed_ceiling"
    ) {
      return {
        ok: false,
        reason: "CONTEXT_DEV_SCRAPE_OBSERVED_CEILING_UNSUPPORTED",
        block_if_unbounded: true,
        message:
          "Observed scrape credits_consumed=10 is not a documented upper bound. Use documented 1/2/5 for scrape_markdown options; overrun blocks further dispatch if provider reports above reservation.",
        docs_url: SCRAPE_MARKDOWN_BOUND_SOURCE.docs_url,
        documented_estimate: documented,
        hard_bound_proven: false,
        contradiction_status: SCRAPE_MARKDOWN_BOUND_CONTRADICTION.status,
      };
    }
    // Live pilot / operator may require a proven hard ceiling — refuse rather than claim one.
    if (
      meta.block_unproven_scrape_bound === true ||
      meta.require_proven_hard_bound === true
    ) {
      return {
        ok: false,
        reason: "CONTEXT_DEV_SCRAPE_HARD_BOUND_UNPROVEN",
        block_if_unbounded: true,
        message: SCRAPE_MARKDOWN_BOUND_CONTRADICTION.explanation,
        docs_url: SCRAPE_MARKDOWN_BOUND_SOURCE.docs_url,
        documented_estimate: documented,
        hard_bound_proven: false,
        contradiction_status: SCRAPE_MARKDOWN_BOUND_CONTRADICTION.status,
        pilot_dispatch_posture: SCRAPE_MARKDOWN_BOUND_CONTRADICTION.pilot_dispatch_posture,
      };
    }
    return {
      ok: true,
      credits: documented,
      documented_estimate: documented,
      bound_source: boundSource,
      docs_url: SCRAPE_MARKDOWN_BOUND_SOURCE.docs_url,
      note: SCRAPE_MARKDOWN_BOUND_SOURCE.note,
      hard_bound_proven: false,
      contradiction_status: SCRAPE_MARKDOWN_BOUND_CONTRADICTION.status,
    };
  }

  if (op === "extract") {
    return {
      ok: true,
      credits: CONTEXT_DEV_DOCUMENTED_COSTS.extract,
      documented_estimate: CONTEXT_DEV_DOCUMENTED_COSTS.extract,
      bound_source: "documented_extract",
      docs_url: "https://docs.context.dev/api-reference/web-extraction/extract",
      hard_bound_proven: true,
    };
  }

  if (op === "brand_retrieve" || op === "brand") {
    return {
      ok: true,
      credits: CONTEXT_DEV_DOCUMENTED_COSTS.brand_retrieve,
      documented_estimate: CONTEXT_DEV_DOCUMENTED_COSTS.brand_retrieve,
      bound_source: "documented_brand_retrieve",
      docs_url: "https://docs.context.dev/guides/get-brand-data",
      hard_bound_proven: true,
    };
  }

  if (!op || op === "unknown") {
    return {
      ok: false,
      reason: "CONTEXT_DEV_OPERATION_UNBOUNDED",
      block_if_unbounded: true,
      message: `No supported credit upper bound for Context.dev op=${op || "(empty)"}`,
    };
  }

  const explicit = Number(meta.estimated_credits);
  if (Number.isFinite(explicit) && explicit > 0) {
    return {
      ok: true,
      credits: explicit,
      documented_estimate: explicit,
      bound_source: "explicit_estimated_credits_caller",
      hard_bound_proven: false,
    };
  }

  return {
    ok: false,
    reason: "CONTEXT_DEV_OPERATION_UNBOUNDED",
    block_if_unbounded: true,
    message: `No supported credit upper bound for Context.dev op=${op}`,
  };
}

/**
 * Documented non-billable outcomes for GET /web/scrape/markdown (official billing table).
 * Requires an attributable numeric HTTP status from the provider response.
 * Never infers HTTP 408 from TIMEOUT / ETIMEDOUT / abort / socket-timeout client text.
 */
export function resolveDocumentedScrapeBillingGuarantee(result = {}, meta = {}) {
  const op = String(meta.op || meta.operation || result?.kind || "").toLowerCase();
  if (op && op !== "scrape" && op !== "scrape_markdown") {
    return { ok: false, reason: "NOT_SCRAPE_MARKDOWN" };
  }

  // Explicit per-request credits always win — never zero-charge over confirmed spend.
  const km =
    result?.key_metadata ||
    result?.data?.key_metadata ||
    result?.error?.key_metadata ||
    null;
  if (km && km.credits_consumed != null && Number.isFinite(Number(km.credits_consumed))) {
    return {
      ok: false,
      reason: "KEY_METADATA_CREDITS_CONSUMED_PRECEDENCE",
      credits_consumed: Number(km.credits_consumed),
    };
  }

  const rawStatus =
    result?.status ??
    result?.http_status ??
    result?.error?.status ??
    result?.error?.httpStatus ??
    result?.error?.statusCode ??
    null;
  // Only numeric (or numeric-string) provider HTTP statuses establish a guarantee.
  // Reject booleans / non-numeric garbage.
  const status =
    rawStatus === null || rawStatus === undefined || rawStatus === ""
      ? NaN
      : Number(rawStatus);
  if (!Number.isFinite(status) || status < 100 || status > 599) {
    return {
      ok: false,
      reason: "NO_ATTRIBUTABLE_PROVIDER_HTTP_STATUS",
      note: "Generic TIMEOUT/ETIMEDOUT/abort/socket-timeout client text does not establish HTTP 408.",
      docs_url: SCRAPE_MARKDOWN_BOUND_SOURCE.docs_url,
      billing_evidence_path: SCRAPE_MARKDOWN_BOUND_SOURCE.billing_evidence_path,
    };
  }

  const notBilled = new Set(
    SCRAPE_MARKDOWN_BOUND_SOURCE.conditions.http_not_billed.map((n) => Number(n))
  );
  if (notBilled.has(status)) {
    return {
      ok: true,
      billed: false,
      credits: 0,
      evidence: `documented_scrape_markdown_http_${status}_not_billed`,
      docs_url: SCRAPE_MARKDOWN_BOUND_SOURCE.docs_url,
      billing_evidence_path: SCRAPE_MARKDOWN_BOUND_SOURCE.billing_evidence_path,
      billing_evidence_retrieval_date: SCRAPE_MARKDOWN_BOUND_SOURCE.billing_evidence_retrieval_date,
      attributable_http_status: status,
    };
  }
  return { ok: false, reason: "NO_DOCUMENTED_BILLING_GUARANTEE", attributable_http_status: status };
}

/**
 * Extract per-request provider-confirmed credits from a sanitized provider result.
 * Meaning (OpenAPI KeyMetadata): "credits consumed by this request".
 */
export function readProviderConfirmedCredits(result) {
  const km =
    result?.key_metadata ||
    result?.data?.key_metadata ||
    result?.durable_result?.key_metadata ||
    null;
  if (!km || typeof km !== "object") {
    return { ok: false, credits: null, reason: "KEY_METADATA_ABSENT" };
  }
  if (km.credits_consumed == null || !Number.isFinite(Number(km.credits_consumed))) {
    return { ok: false, credits: null, reason: "CREDITS_CONSUMED_ABSENT" };
  }
  return {
    ok: true,
    credits: Number(km.credits_consumed),
    credits_remaining:
      km.credits_remaining != null && Number.isFinite(Number(km.credits_remaining))
        ? Number(km.credits_remaining)
        : null,
    meaning: "per_request_credits_consumed",
    evidence: "openapi_key_metadata.credits_consumed_by_this_request",
  };
}
