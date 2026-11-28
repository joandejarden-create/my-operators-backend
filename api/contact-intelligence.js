/**
 * Contact Intelligence V1 HTTP API.
 * Read-mostly. Paid enrichment disabled unless explicitly enabled + requested.
 */

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getDefaultContactIntelligenceService,
  createContactIntelligenceService,
  createContactStore,
  runContactBenchmark100,
  createDefaultBenchmarkResolver,
} from "../lib/hotel-intelligence/contact-intelligence/index.js";
import { LIVE_SLICE_TEN_HOTELS, LIVE_SLICE_LIMITATIONS } from "../lib/hotel-intelligence/contact-intelligence/live-slice-ten.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const LIVE_SLICE_REPORT = path.join(ROOT, "reports", "contact-intelligence-live-slice-ten-v1.1.json");

function sendError(res, status, error, details = {}) {
  return res.status(status).json({ success: false, error, ...details });
}

function serviceForRequest(req) {
  // Tests / ephemeral: allow CONTACT_INTELLIGENCE_DATA_ROOT override via env only.
  return getDefaultContactIntelligenceService();
}

export async function getContactIntelligenceMeta(req, res) {
  try {
    const svc = serviceForRequest(req);
    return res.json({ success: true, ...svc.meta() });
  } catch (err) {
    console.error("[contact-intelligence] meta", err);
    return sendError(res, 500, "contact_meta_failed");
  }
}

function contactEnrichmentFlags() {
  const enrichmentEnabled =
    String(process.env.CONTACT_ENRICHMENT_ENABLED || "").toLowerCase() === "true";
  const hasApiKey = Boolean(process.env.SURFE_API_KEY && String(process.env.SURFE_API_KEY).trim());
  const dryRunProvider =
    String(process.env.CONTACT_ENRICHMENT_DRY_RUN_PROVIDER || "").toLowerCase() === "true";
  const mockEnv = String(process.env.CONTACT_ENRICHMENT_MOCK || "").toLowerCase();
  /**
   * No-cost mock for Founder Preview / local QA when live Surfe is not configured.
   * Opt out with CONTACT_ENRICHMENT_MOCK=false. Live Surfe still requires
   * CONTACT_ENRICHMENT_ENABLED=true + SURFE_API_KEY (and MOCK not forcing dry-run).
   */
  const mockEnabled =
    dryRunProvider ||
    mockEnv === "true" ||
    (mockEnv !== "false" && !hasApiKey && !enrichmentEnabled);
  /** Live Surfe OR intentional no-cost mock for Founder Preview / local QA. */
  const surfeEnabled = (enrichmentEnabled && hasApiKey) || mockEnabled;
  let reason = null;
  let userReason = null;
  if (!surfeEnabled) {
    if (!enrichmentEnabled && !mockEnabled) {
      reason = "CONTACT_ENRICHMENT_ENABLED is not true";
      userReason = "Contact enrichment is not configured in this environment.";
    } else if (!hasApiKey) {
      reason = "SURFE_API_KEY is not configured";
      userReason = "Contact enrichment is not configured in this environment.";
    }
  }
  return {
    enrichmentEnabled,
    hasApiKey,
    dryRunProvider,
    mockEnabled,
    surfeEnabled,
    reason,
    userReason,
  };
}

function buildMockSurfePerson({ personName, organization, jobTitle, linkedinUrl, companyDomain }) {
  const slug = String(personName || "contact")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ".")
    .replace(/^\.+|\.+$/g, "");
  const domain =
    String(companyDomain || "")
      .trim()
      .replace(/^https?:\/\//i, "")
      .replace(/\/$/, "") || "example.com";
  return {
    ok: true,
    people: [
      {
        fullName: personName,
        personName,
        companyName: organization,
        jobTitle: jobTitle || null,
        linkedinUrl: linkedinUrl || null,
        email: `${slug}@${domain}`,
        workEmail: `${slug}@${domain}`,
        phone: null,
        mobile: null,
        emails: [{ email: `${slug}@${domain}`, type: "work" }],
        phones: [],
        confidence: "PROVIDER_MOCK",
        verificationStatus: "PROVIDER_RETURNED_UNREVIEWED",
      },
    ],
    pending: false,
    estimatedCostUsd: 0,
    mock: true,
  };
}

/** Read-only Surfe capability for HE Contact Intelligence UI (no credits). */
export async function getContactIntelligenceEnrichmentCapability(req, res) {
  try {
    const flags = contactEnrichmentFlags();
    return res.json({
      success: true,
      surfeEnabled: flags.surfeEnabled,
      hasApiKey: flags.hasApiKey,
      enrichmentEnabled: flags.enrichmentEnabled,
      dryRunProvider: flags.dryRunProvider,
      mockEnabled: flags.mockEnabled,
      autoEnrichOnLoad: false,
      persistOnReveal: false,
      reason: flags.reason,
      userReason: flags.userReason,
    });
  } catch (err) {
    console.error("[contact-intelligence] enrichment-capability", err);
    return sendError(res, 500, "enrichment_capability_failed");
  }
}

export async function getContactIntelligenceHotel(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return sendError(res, 400, "hotel_id_required");
    const audience = String(req.query.audience || "public").trim();
    let result = serviceForRequest(req).hotelGet({ hotel_id: hotelId, audience });
    if (!result.ok) return sendError(res, 404, result.error || "hotel_contact_unavailable", result);

    // Shared Canonical Contact Intelligence V1: prefer HI Org Contacts + Channels
    // whenever the graph has relationships/contacts — not only when local package is sparse.
    const hiGraphReadDisabled =
      String(process.env.CONTACT_INTELLIGENCE_HI_GRAPH_READ || "").trim() === "0";
    if (!hiGraphReadDisabled) {
      try {
        const { readHiGraphForHotel } = await import(
          "../lib/hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js"
        );
        const { overlayHiGraphOnContactHotelGet } = await import(
          "../lib/hotel-intelligence/contact-intelligence/he-hi-graph-contact-overlay-v1.js"
        );
        const graph = await readHiGraphForHotel(hotelId, {
          includeContacts: true,
          includePortfolio: false,
        });
        if (
          graph?.ok &&
          ((graph.orgContactCount || 0) > 0 || (graph.relationshipCount || 0) > 0)
        ) {
          result = overlayHiGraphOnContactHotelGet(
            result,
            graph,
            graph.orgContacts || [],
          );
          result.shared_canonical_contact_intelligence = {
            version: "shared-canonical-contact-intelligence-v1",
            sourceOfTruth: "HI_ORG_CONTACTS_PLUS_HI_CONTACT_CHANNELS",
            applied: true,
          };
        }
      } catch (graphErr) {
        console.warn(
          "[contact-intelligence] hi-graph overlay skipped",
          graphErr?.message || graphErr,
        );
      }
    }

    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[contact-intelligence] hotel", err);
    return sendError(res, 500, "contact_hotel_failed");
  }
}

export async function getContactIntelligenceOwner(req, res) {
  try {
    const ownerId = String(req.params.ownerId || "").trim();
    if (!ownerId) return sendError(res, 400, "owner_id_required");
    const audience = String(req.query.audience || "public").trim();
    const result = serviceForRequest(req).ownerGet({ owner_id: ownerId, audience });
    if (!result.ok) return sendError(res, 404, result.error || "owner_contact_unavailable", result);
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[contact-intelligence] owner", err);
    return sendError(res, 500, "contact_owner_failed");
  }
}

export async function getContactIntelligenceSliceTen(req, res) {
  try {
    const audience = String(req.query.audience || "public").trim();
    const result = serviceForRequest(req).sliceTen({ audience });
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[contact-intelligence] slice-ten", err);
    return sendError(res, 500, "contact_slice_ten_failed");
  }
}

export async function getContactIntelligenceBenchmark100(req, res) {
  try {
    const svc = serviceForRequest(req);
    const report = runContactBenchmark100({
      service: svc,
      resolveHotel: createDefaultBenchmarkResolver(svc),
      includeHeldOutDetail: String(req.query.detail || "") === "1",
    });
    return res.json({
      success: true,
      ...report,
      population_wide_enrichment_allowed: false,
    });
  } catch (err) {
    console.error("[contact-intelligence] benchmark-100", err);
    return sendError(res, 500, "contact_benchmark_failed");
  }
}

export async function getContactIntelligenceLiveSliceTen(req, res) {
  try {
    if (fs.existsSync(LIVE_SLICE_REPORT)) {
      const report = JSON.parse(fs.readFileSync(LIVE_SLICE_REPORT, "utf8"));
      return res.json({
        success: true,
        source: "reports/contact-intelligence-live-slice-ten-v1.1.json",
        ...report,
      });
    }
    return res.json({
      success: true,
      status: "NOT_RUN",
      hotels: LIVE_SLICE_TEN_HOTELS,
      limitations: LIVE_SLICE_LIMITATIONS,
      message:
        "Live slice not run yet. Execute: node scripts/contact-intelligence-live-slice-ten.mjs",
    });
  } catch (err) {
    console.error("[contact-intelligence] live-slice", err);
    return sendError(res, 500, "contact_live_slice_failed");
  }
}

export async function postContactIntelligenceHotelRefresh(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return sendError(res, 400, "hotel_id_required");
    const body = req.body || {};
    const result = serviceForRequest(req).refreshHotel({
      hotel_id: hotelId,
      paid: body.paid === true,
      provider_strategy: body.provider_strategy || "NATIVE",
      idempotency_token: body.idempotency_token || null,
      audience: body.audience || "public",
    });
    if (!result.ok && result.error === "paid_enrichment_disabled") {
      return sendError(res, 403, result.error, result);
    }
    if (!result.ok) return sendError(res, 400, result.error || "refresh_failed", result);
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[contact-intelligence] refresh", err);
    return sendError(res, 500, "contact_refresh_failed");
  }
}

/**
 * Staging research workflow entry — hotel → owner → person → contact (opt-in).
 * Admin-authenticated. Discovery-only by default (no paid enrichment).
 * Persists a resumable research case. Never promotes canonical ownership/contact.
 * Client budgets/options cannot enable paid enrichment or override trusted deny.
 */
export async function postContactIntelligenceResearchWorkflow(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return sendError(res, 400, "hotel_id_required");
    const body = req.body || {};
    if (body.enable_contact_enrichment === true || body.paid === true) {
      return sendError(res, 403, "paid_enrichment_disabled_on_http", {
        message:
          "HTTP research-workflow is discovery/staging only. Paid enrichment requires explicit offline evaluation scripts.",
      });
    }

    const { runFullResearchWorkflow } = await import(
      "../lib/hotel-intelligence/contact-intelligence/full-research-workflow.js"
    );
    const { createResearchCaseStore } = await import(
      "../lib/hotel-intelligence/contact-intelligence/research-case-store.js"
    );
    const { buildAdminResearchWorkbenchView } = await import(
      "../lib/hotel-intelligence/contact-intelligence/admin-research-workbench.js"
    );

    // Body fields are hints only — HPC loader resolves authoritative facts by hotelId
    const hints = {
      hotel_name: body.hotel_name || body.hotel?.hotel_name || body.name,
      city: body.city || body.hotel?.city || null,
      country: body.country || body.hotel?.country || null,
      address: body.address || body.hotel?.address || null,
      aliases: body.aliases || body.hotel?.aliases || [],
      language: body.language || body.hotel?.language || null,
      requested_hotel_id: body.hotel_id || body.hotel?.hotel_id || null,
    };

    // Trusted execution: client cannot force network/paid. Offline/default deny.
    const allowNetwork = body.allow_network === true && process.env.CONTACT_INTELLIGENCE_ALLOW_HTTP_NETWORK === "1";
    const fixtureDeps =
      process.env.NODE_ENV !== "production" && typeof req._researchDeps === "object";
    const storeRoot =
      process.env.NODE_ENV !== "production" && req.headers["x-research-case-root"]
        ? String(req.headers["x-research-case-root"])
        : undefined;
    const store = createResearchCaseStore(storeRoot ? { root: storeRoot } : {});

    // Offline fixture path: allow small context_dev budget for injected search/scrape only.
    // Never raises serpapi/enrichment; never enables paid contact enrichment.
    const contextCap = allowNetwork ? 40 : fixtureDeps ? 40 : 0;
    const serpCap = allowNetwork ? 5 : 0;
    const fixtureBudgets = fixtureDeps
      ? {
          model_reader_usd_max: Number(body.budgets?.model_reader_usd_max ?? 0) || 0,
          apply_structured_reader: body.apply_structured_reader === true,
          apply_model_follow_up_planner: body.apply_model_follow_up_planner === true,
          iterative_ownership_loop: body.iterative_ownership_loop !== false,
          iterative_max_queries: Number(body.budgets?.iterative_max_queries ?? body.iterative_max_queries ?? 6) || 6,
          iterative_max_documents: Number(body.budgets?.iterative_max_documents ?? body.iterative_max_documents ?? 6) || 6,
        }
      : {};

    const result = await runFullResearchWorkflow(
      {
        hotel: { ...hints, hotel_id: hotelId },
        hotel_id: hotelId,
        // Admin resume of a known case must fail closed when missing.
        // New runs omit case_id (workflow allocates). Explicit resume uses resume_case_id.
        ...(body.resume_case_id || body.resume === true
          ? { resume_case_id: body.resume_case_id || body.case_id || null }
          : body.case_id
            ? { resume_case_id: body.case_id }
            : {}),
        objective: body.objective || "HOTEL_OWNERSHIP_CONTACT",
        force_refresh: body.force_refresh === true,
        ...(fixtureDeps ? { iterative_ownership_loop: body.iterative_ownership_loop !== false } : {}),
      },
      {
        serpapi_max: Math.min(Number(body.budgets?.serpapi_max ?? 0), serpCap),
        context_dev_max: Math.min(Number(body.budgets?.context_dev_max ?? 0), contextCap),
        enrichment_max: 0,
        enable_contact_enrichment: false,
        disable_network: !allowNetwork,
        ...fixtureBudgets,
      },
      {
        store,
        networkBlocked: !allowNetwork,
        enableContactEnrichment: false,
        // Test/fixture injection only when non-production
        ...(fixtureDeps ? req._researchDeps : {}),
      }
    );

    const workbench = buildAdminResearchWorkbenchView(result.case || result);
    return res.json({
      success: true,
      staging: true,
      canonical_writes: false,
      customer_publication: "BLOCKED",
      workbench,
      ...result,
    });
  } catch (err) {
    console.error("[contact-intelligence] research-workflow", err);
    return sendError(res, 500, "research_workflow_failed", {
      message: err.message || "research_workflow_failed",
    });
  }
}

export async function getContactIntelligenceResearchCase(req, res) {
  try {
    const caseId = String(req.params.caseId || "").trim();
    if (!caseId) return sendError(res, 400, "case_id_required");
    const { loadResearchCase } = await import(
      "../lib/hotel-intelligence/contact-intelligence/full-research-workflow.js"
    );
    const { createResearchCaseStore } = await import(
      "../lib/hotel-intelligence/contact-intelligence/research-case-store.js"
    );
    const { buildAdminResearchWorkbenchView } = await import(
      "../lib/hotel-intelligence/contact-intelligence/admin-research-workbench.js"
    );
    const storeRoot =
      process.env.NODE_ENV !== "production" && req.headers["x-research-case-root"]
        ? String(req.headers["x-research-case-root"])
        : undefined;
    const store = createResearchCaseStore(storeRoot ? { root: storeRoot } : {});
    const result = loadResearchCase(caseId, { store });
    if (!result.ok) return sendError(res, 404, result.error || "case_not_found");
    const workbench = buildAdminResearchWorkbenchView(result.case);
    return res.json({ success: true, staging: true, workbench, ...result });
  } catch (err) {
    console.error("[contact-intelligence] research-case", err);
    return sendError(res, 500, "research_case_failed");
  }
}

/**
 * Human review notes/decisions on a staged research case.
 * Never promotes to canonical ownership/contact fields.
 * Lock first, then load history under the lock; reviewer from auth context.
 */
export async function postContactIntelligenceResearchCaseReview(req, res) {
  try {
    const caseId = String(req.params.caseId || "").trim();
    if (!caseId) return sendError(res, 400, "case_id_required");
    const body = req.body || {};
    if (body.canonical_promotion === true || body.promote === true) {
      return sendError(res, 403, "canonical_promotion_forbidden", {
        message: "Workbench review cannot promote findings to canonical ownership/contact data.",
      });
    }
    const { loadResearchCase } = await import(
      "../lib/hotel-intelligence/contact-intelligence/full-research-workflow.js"
    );
    const { createResearchCaseStore } = await import(
      "../lib/hotel-intelligence/contact-intelligence/research-case-store.js"
    );
    const {
      buildHumanReviewUpdate,
      buildAdminResearchWorkbenchView,
      resolveTrustedReviewer,
    } = await import(
      "../lib/hotel-intelligence/contact-intelligence/admin-research-workbench.js"
    );
    const storeRoot =
      process.env.NODE_ENV !== "production" && req.headers["x-research-case-root"]
        ? String(req.headers["x-research-case-root"])
        : undefined;
    const store = createResearchCaseStore(storeRoot ? { root: storeRoot } : {});

    const lock = store.tryAcquireLock(caseId, 15000);
    if (!lock.ok) return sendError(res, 409, "case_locked", { lock: lock.lock });
    try {
      const loaded = loadResearchCase(caseId, { store });
      if (!loaded.ok) return sendError(res, 404, loaded.error || "case_not_found");

      const trustedReviewer = resolveTrustedReviewer(req.dealalityUser);
      const updated = buildHumanReviewUpdate(body, loaded.case.human_review || null, {
        trustedReviewer,
      });
      if (!updated.ok) return sendError(res, 400, updated.error, { allowed: updated.allowed });

      const put = store.putCase(
        {
          case_id: caseId,
          human_review: updated.human_review,
          journal_entry: {
            kind: "human_review",
            event: "review_recorded",
            decision: updated.human_review.latest.decision,
            reviewer: trustedReviewer,
            canonical_promotion: false,
          },
        },
        { lock_token: lock.lock.lock_token }
      );
      if (!put.ok) return sendError(res, 409, put.reason || "review_write_failed");
      const workbench = buildAdminResearchWorkbenchView(put.case);
      return res.json({
        success: true,
        staging: true,
        canonical_writes: false,
        customer_publication: "BLOCKED",
        human_review: updated.human_review,
        workbench,
        case: put.case,
      });
    } finally {
      store.releaseLock(caseId, lock.lock?.lock_token);
    }
  } catch (err) {
    console.error("[contact-intelligence] research-case-review", err);
    return sendError(res, 500, "research_case_review_failed", {
      message: err.message || "research_case_review_failed",
    });
  }
}

const _heRevealInflight = new Map();

/**
 * User-initiated Surfe contact reveal for Hotel Explorer Contact Intelligence.
 * Never runs on GET. Never persists to Airtable. Checks existing channels first.
 */
export async function postContactIntelligencePersonReveal(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    const personId = String(req.params.personId || "").trim();
    if (!hotelId || !personId) {
      return sendError(res, 400, "hotel_and_person_required");
    }
    if (req.body && req.body.persist === true) {
      return sendError(res, 403, "persist_forbidden", {
        message:
          "Reveal returns ephemeral Surfe results only. Canonical Airtable persistence requires a separate reviewed apply path.",
      });
    }

    const body = req.body || {};
    const personName = String(body.personName || body.person_name || "").trim();
    const organization = String(body.organization || body.company || "").trim();
    const inflightKey = `${hotelId}::${personId}`;
    if (_heRevealInflight.has(inflightKey)) {
      return sendError(res, 409, "reveal_in_flight", {
        message: "A Surfe reveal is already in progress for this person.",
        surfeCalls: 0,
      });
    }

    // Prefer existing Contact Intelligence package channels (no credit spend).
    try {
      const existing = serviceForRequest(req).hotelGet({
        hotel_id: hotelId,
        audience: "internal",
      });
      if (existing?.ok && existing.package) {
        const people = existing.package.people || existing.package.persons || [];
        const match =
          people.find((p) => String(p.personId || p.person_id || "") === personId) ||
          people.find(
            (p) =>
              String(p.name || p.display_name || "")
                .trim()
                .toLowerCase() === personName.toLowerCase()
          );
        const emails = (match && (match.emailContacts || match.emails)) || [];
        const phones = (match && (match.phoneContacts || match.phones)) || [];
        const has =
          emails.some((c) => c && (c.value || c.email)) ||
          phones.some((c) => c && (c.value || c.phone));
        if (has) {
          return res.json({
            ok: true,
            cached: true,
            surfeCalls: 0,
            estimatedCostUsd: 0,
            provider: null,
            channels: { emails, phones },
            existing: { personId, personName: match?.name || personName },
            persistence: "NOT_WRITTEN",
            review_required: false,
          });
        }
      }
    } catch (_err) {
      // Fall through to Surfe path / capability report.
    }

    const flags = contactEnrichmentFlags();
    if (!flags.surfeEnabled) {
      return res.status(503).json({
        ok: false,
        error: "surfe_unavailable_for_hotel_explorer",
        message: flags.reason || "enrichment_not_configured",
        userMessage:
          flags.userReason || "Contact enrichment is not available in this environment.",
        surfeCalls: 0,
        persistence: "NOT_WRITTEN",
      });
    }
    if (!personName || personName.split(/\s+/).length < 2 || !organization) {
      return sendError(res, 400, "named_person_and_organization_required", {
        message: "Find Email & Phone requires a full person name and organization.",
        userMessage: "A full person name and organization are required.",
        surfeCalls: 0,
      });
    }

    const jobTitle = body.jobTitle || body.title || null;
    const linkedinUrl = body.linkedinUrl || null;
    const companyDomain = body.companyDomain || body.company_domain || null;

    _heRevealInflight.set(inflightKey, Date.now());
    try {
      // No-cost mock path for Founder Preview / local QA (never calls Surfe; never persists).
      if (flags.mockEnabled && !flags.hasApiKey) {
        const enrichRes = buildMockSurfePerson({
          personName,
          organization,
          jobTitle,
          linkedinUrl,
          companyDomain,
        });
        const person = (enrichRes.people && enrichRes.people[0]) || null;
        return res.json({
          ok: true,
          cached: false,
          pending: false,
          mock: true,
          surfeCalls: 0,
          estimatedCostUsd: 0,
          provider: "surfe_mock",
          providerAttribution: "Mock Surfe provider (no-cost test)",
          verificationStatus: "PROVIDER_RETURNED_UNREVIEWED",
          person,
          persistence: "NOT_WRITTEN",
          review_required: true,
          note: "Mock result only. Review required before any canonical Airtable write.",
        });
      }

      const { createSurfeContactProvider } = await import(
        "../lib/market-alerts-contact/surfe-adapter.js"
      );
      const { getContactEnrichmentLimits } = await import(
        "../lib/market-alerts-contact/eligibility.js"
      );
      const limits = getContactEnrichmentLimits();

      // Mock with key present still allows intentional dry-run without spend.
      if (flags.mockEnabled && limits.dryRunProvider) {
        const enrichRes = buildMockSurfePerson({
          personName,
          organization,
          jobTitle,
          linkedinUrl,
          companyDomain,
        });
        const person = (enrichRes.people && enrichRes.people[0]) || null;
        return res.json({
          ok: true,
          cached: false,
          pending: false,
          mock: true,
          surfeCalls: 0,
          estimatedCostUsd: 0,
          provider: "surfe_mock",
          providerAttribution: "Mock Surfe provider (dry-run)",
          verificationStatus: "PROVIDER_RETURNED_UNREVIEWED",
          person,
          persistence: "NOT_WRITTEN",
          review_required: true,
          note: "Dry-run mock result. Review required before any canonical Airtable write.",
        });
      }

      const provider = createSurfeContactProvider({ pollOnEnrich: true });
      const enrichRes = await provider.enrichPeople({
        people: [
          {
            personName,
            companyName: organization,
            jobTitle,
            linkedinUrl,
            companyDomain,
          },
        ],
        includeEmail: true,
        includeMobile: limits.includeMobile === true,
        includeLinkedIn: true,
      });
      if (!enrichRes?.ok) {
        return res.status(502).json({
          ok: false,
          error: enrichRes?.error || "surfe_enrich_failed",
          userMessage: "Contact enrichment could not complete. Try again later.",
          surfeCalls: 1,
          persistence: "NOT_WRITTEN",
        });
      }
      const person = (enrichRes.people && enrichRes.people[0]) || null;
      const hasChannel =
        person &&
        (person.email ||
          person.workEmail ||
          person.phone ||
          person.mobile ||
          (Array.isArray(person.emails) && person.emails.length) ||
          (Array.isArray(person.phones) && person.phones.length));
      if (!hasChannel) {
        return res.json({
          ok: true,
          cached: false,
          pending: Boolean(enrichRes.pending),
          surfeCalls: 1,
          estimatedCostUsd: enrichRes.estimatedCostUsd ?? null,
          provider: "surfe",
          providerAttribution: "Surfe on-demand (ephemeral)",
          verificationStatus: "PROVIDER_RETURNED_UNREVIEWED",
          person: null,
          error: "surfe_no_result",
          userMessage: "No additional professional contact details found.",
          persistence: "NOT_WRITTEN",
          review_required: false,
        });
      }
      return res.json({
        ok: true,
        cached: false,
        pending: Boolean(enrichRes.pending),
        surfeCalls: 1,
        estimatedCostUsd: enrichRes.estimatedCostUsd ?? null,
        provider: "surfe",
        providerAttribution: "Surfe on-demand (ephemeral)",
        verificationStatus: "PROVIDER_RETURNED_UNREVIEWED",
        person,
        persistence: "NOT_WRITTEN",
        review_required: true,
        note: "Review required before any canonical Airtable write.",
      });
    } finally {
      _heRevealInflight.delete(inflightKey);
    }
  } catch (err) {
    console.error("[contact-intelligence] person-reveal", err);
    return sendError(res, 500, "contact_person_reveal_failed", {
      message: String(err?.message || err).slice(0, 160),
      surfeCalls: 0,
    });
  }
}

/**
 * Shared Canonical Contact Intelligence V1 — hotel-scoped SoT read.
 * Same resolver consumed by Hotel Explorer and GDI.
 */
export async function getCanonicalHotelContacts(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return sendError(res, 400, "hotel_id_required");
    const {
      resolveCanonicalHotelContacts,
      toHeContactDirectoryRow,
      toGdiContactPresentation,
      computeSharedContactability,
    } = await import("../lib/shared-contact-intelligence/index.js");
    const resolved = await resolveCanonicalHotelContacts(hotelId, {
      updateIdentityMap: String(req.query.updateMap || "") === "1",
    });
    if (!resolved.ok) {
      const status =
        resolved.status === "BLOCKED" ? 503 : resolved.status === "NOT_TESTED" ? 503 : 404;
      return res.status(status).json({
        success: false,
        error: resolved.reason || "canonical_contacts_unavailable",
        ...resolved,
      });
    }
    const contactability = computeSharedContactability(resolved.people, { hotelId });
    return res.json({
      success: true,
      version: "shared-canonical-contact-intelligence-v1",
      hotelId,
      sourceOfTruth: resolved.sourceOfTruth,
      airtableLive: resolved.airtableLive,
      people: resolved.people,
      heDirectory: resolved.people.map(toHeContactDirectoryRow),
      gdiPresentation: resolved.people.map(toGdiContactPresentation),
      contactability,
      duplicatePersonRowsCollapsed: resolved.duplicatePersonRowsCollapsed || 0,
    });
  } catch (err) {
    console.error("[contact-intelligence] canonical-hotel", err);
    return sendError(res, 500, "canonical_hotel_contacts_failed");
  }
}

/**
 * Shared Canonical Contact Intelligence V1 — person-scoped SoT read.
 */
export async function getCanonicalPersonChannels(req, res) {
  try {
    const personId = String(req.params.personId || "").trim();
    const hotelId = String(req.query.hotelId || req.params.hotelId || "").trim();
    if (!personId) return sendError(res, 400, "person_id_required");
    const {
      resolveCanonicalPersonChannels,
      toHeContactDirectoryRow,
      toGdiContactPresentation,
    } = await import("../lib/shared-contact-intelligence/index.js");
    const resolved = await resolveCanonicalPersonChannels(
      {
        canonicalPersonId: personId,
        hotelId: hotelId || undefined,
        displayName: req.query.name || undefined,
      },
      { hotelId: hotelId || undefined },
    );
    if (!resolved.ok) {
      const status = resolved.status === "BLOCKED" ? 503 : 404;
      return res.status(status).json({
        success: false,
        error: resolved.reason || resolved.status || "canonical_person_unavailable",
        ...resolved,
      });
    }
    return res.json({
      success: true,
      version: "shared-canonical-contact-intelligence-v1",
      person: resolved.person,
      heRow: toHeContactDirectoryRow(resolved.person),
      gdiPresentation: toGdiContactPresentation(resolved.person),
      identity: resolved.identity,
      airtableLive: resolved.airtableLive,
    });
  } catch (err) {
    console.error("[contact-intelligence] canonical-person", err);
    return sendError(res, 500, "canonical_person_channels_failed");
  }
}

/** Test helper — ephemeral service rooted in tmp (not used by production routes). */
export function createEphemeralContactIntelligenceService() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ci-v1-"));
  const store = createContactStore({ root });
  return createContactIntelligenceService({ store, forceNew: true });
}
