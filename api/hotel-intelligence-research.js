/**
 * Packet 2.6C-R2 — Deep Research Center API.
 * Live Webhound only when external flag ON + explicit Run Research confirm.
 * Never auto-starts paid research on GET/page load.
 */

import {
  buildResearchCenterPayloadAsync,
  createResearchOrchestrator,
  createResearchRepository,
  listTemplates,
  getTemplate,
  resolveReportExportLinks,
  isExternalResearchEnabled,
  isFounderInternalDebug,
  getPilotMaxProviderBudgetUsd,
  KGPV_HOTEL_ID,
  ensureKgpvRun1Backfill,
  discoverProfessionalProfiles,
  enrichProfessionalProfiles,
  customerProfileLink,
  PROFILE_MATCH_STATUS,
} from "../lib/hotel-intelligence/research/index.js";

function repoFromReq(req) {
  const override =
    process.env.NODE_ENV !== "production" && req.headers["x-hi-research-root"]
      ? String(req.headers["x-hi-research-root"])
      : null;
  return createResearchRepository(override ? { root: override } : {});
}

export async function getHotelResearchCenter(req, res) {
  try {
    const hotelId = String(req.params.hotelId || req.params.recordId || "").trim();
    if (!hotelId) {
      return res.status(400).json({ ok: false, error: "hotel_id_required" });
    }
    const shareQ = String(req.query.share || "").toLowerCase();
    const clientSafeShare = shareQ === "1" || shareQ === "true";
    const includeAdmin = clientSafeShare
      ? false
      : String(req.query.admin || "") === "1" || String(req.query.includeAdmin || "") === "1";
    const payload = await buildResearchCenterPayloadAsync({
      hotel_id: hotelId,
      hotel_name: req.query.hotelName || req.query.hotel_name || null,
      includeAdmin,
      clientSafeShare,
      repository: repoFromReq(req),
      env: process.env,
    });
    return res.json(payload);
  } catch (err) {
    console.error("[hotel-intelligence-research] center", err);
    return res.status(500).json({ ok: false, error: "research_center_failed" });
  }
}

export async function listHotelResearchRequests(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return res.status(400).json({ ok: false, error: "hotel_id_required" });
    const repo = repoFromReq(req);
    if (hotelId === KGPV_HOTEL_ID) ensureKgpvRun1Backfill(repo);
    const data = repo.listHotelResearch(hotelId);
    return res.json({
      ok: true,
      hotel_id: hotelId,
      requests: data.requests,
      runs: data.runs,
      count: data.requests.length,
    });
  } catch (err) {
    console.error("[hotel-intelligence-research] list", err);
    return res.status(500).json({ ok: false, error: "research_list_failed" });
  }
}

export async function getResearchRequest(req, res) {
  try {
    const requestId = String(req.params.requestId || "").trim();
    const repo = repoFromReq(req);
    const found = repo.getRequestById(requestId);
    if (!found) return res.status(404).json({ ok: false, error: "request_not_found" });
    const orch = createResearchOrchestrator({ repository: repo, env: process.env });
    if (!found.request.immutable_historical) {
      await orch.advanceRequest(found.hotel_id, requestId, { to_completion: false });
    }
    const request = repo.getRequest(found.hotel_id, requestId);
    const runs = repo.listHotelResearch(found.hotel_id).runs.filter((r) => r.request_id === requestId);
    return res.json({
      ok: true,
      request,
      runs,
      export: resolveReportExportLinks(request),
      external_research_enabled: isExternalResearchEnabled(),
    });
  } catch (err) {
    console.error("[hotel-intelligence-research] get", err);
    return res.status(500).json({ ok: false, error: "research_get_failed" });
  }
}

export async function createHotelResearchRequest(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return res.status(400).json({ ok: false, error: "hotel_id_required" });

    const body = req.body && typeof req.body === "object" ? req.body : {};
    const templateId = String(body.template_id || body.templateId || "").trim();
    if (!templateId || !getTemplate(templateId)) {
      return res.status(400).json({
        ok: false,
        error: "invalid_template",
        message: "Choose a standard investigation.",
      });
    }

    const liveRequested =
      body.run_live === true ||
      body.confirm_spend === true ||
      (body.authorized === true && String(body.provider_strategy || "").toUpperCase() === "WEBHOUND");

    const externalOn = isExternalResearchEnabled();
    if (liveRequested && !externalOn) {
      return res.status(403).json({
        ok: false,
        error: "external_research_disabled",
        message: "Live research is disabled on this server.",
      });
    }

    if (liveRequested && body.confirm_spend !== true) {
      return res.status(400).json({
        ok: false,
        error: "explicit_run_research_required",
        message: "Confirm Run Research to start live investigation.",
        requires_confirm_spend: true,
        max_provider_budget_usd: getPilotMaxProviderBudgetUsd(),
      });
    }

    const repo = repoFromReq(req);
    const orch = createResearchOrchestrator({ repository: repo, env: process.env });
    const idempotency =
      body.idempotency_token ||
      req.headers["idempotency-key"] ||
      req.headers["x-idempotency-key"] ||
      null;

    let result;
    try {
      result = await orch.createResearchRequest({
        hotel_id: hotelId,
        hotel_name: body.hotel_name || body.hotelName || null,
        template_id: templateId,
        provider_strategy: liveRequested ? "WEBHOUND" : "SIMULATION",
        provider_budget_usd: liveRequested
          ? getPilotMaxProviderBudgetUsd()
          : body.provider_budget_usd || undefined,
        requested_by: body.requested_by || "ui",
        idempotency_token: idempotency ? String(idempotency) : null,
        parent_report_id: body.parent_report_id || null,
        known_facts: body.known_facts || null,
        known_gaps: body.known_gaps || null,
        execute: true,
        to_completion: liveRequested ? false : body.to_completion === false ? false : true,
        force_simulation: !liveRequested,
        run_live: liveRequested,
        confirm_spend: liveRequested ? true : false,
        authorized: liveRequested ? true : false,
        explicit_user_action: liveRequested ? true : false,
      });
    } catch (err) {
      const code = err.code || "research_create_failed";
      const status =
        code === "active_request_exists" || code === "max_concurrent_webhound_runs"
          ? 409
          : code === "external_research_disabled" ||
              code === "kgpv_full_investigation_immutable" ||
              code === "daily_external_research_budget_exhausted" ||
              code === "provider_budget_exceeds_hard_cap"
            ? 403
            : code === "explicit_run_research_required" || code === "custom_questions_not_enabled"
              ? 400
              : 500;
      return res.status(status).json({
        ok: false,
        error: code,
        message: err.customer_safe || "Research could not be started.",
        existing_request_id: err.existing?.request_id,
      });
    }

    return res.status(201).json({
      ok: true,
      request: result.request,
      run: result.run,
      export: result.export_links,
      mode: result.mode || (liveRequested ? "LIVE" : "SIMULATION"),
      idempotent_replay: Boolean(result.idempotent_replay),
      note: liveRequested
        ? "Research started. You can close this panel and return at any time."
        : "Investigation started.",
      internal_debug: isFounderInternalDebug()
        ? {
            live: liveRequested,
            max_provider_budget_usd: getPilotMaxProviderBudgetUsd(),
          }
        : undefined,
    });
  } catch (err) {
    console.error("[hotel-intelligence-research] create", err);
    return res.status(500).json({
      ok: false,
      error: "research_create_failed",
      message: "Research could not be started.",
    });
  }
}

export function listResearchTemplates(req, res) {
  try {
    const includeInternal = String(req.query.admin || "") === "1";
    return res.json({
      ok: true,
      templates: listTemplates({ includeInternal }),
      external_research_enabled: isExternalResearchEnabled(),
      max_provider_budget_usd: getPilotMaxProviderBudgetUsd(),
    });
  } catch (err) {
    console.error("[hotel-intelligence-research] templates", err);
    return res.status(500).json({ ok: false, error: "templates_failed" });
  }
}

export function getResearchReportMeta(req, res) {
  try {
    const reportId = String(req.params.reportId || "").trim();
    if (!reportId) return res.status(400).json({ ok: false, error: "report_id_required" });
    return res.json({
      ok: true,
      report_id: reportId,
      view_api: `/api/hotel-intelligence/dossiers/${encodeURIComponent(reportId)}`,
      pdf_api: `/api/hotel-intelligence/dossiers/${encodeURIComponent(reportId)}/pdf`,
      contract: resolveReportExportLinks({ report_id: reportId }),
    });
  } catch (err) {
    console.error("[hotel-intelligence-research] report-meta", err);
    return res.status(500).json({ ok: false, error: "report_meta_failed" });
  }
}

/**
 * Optional founder/internal: enrich Key People professional profiles.
 * Evidence-first (preserve research LinkedIn /in/ URLs), then optional native search.
 * Does not start Webhound.
 */
export async function enrichHotelPeopleProfiles(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const people = Array.isArray(body.people) ? body.people : [];
    if (!people.length) {
      return res.status(400).json({ ok: false, error: "people_required" });
    }
    const evidenceTexts = Array.isArray(body.evidence_texts)
      ? body.evidence_texts
      : Array.isArray(body.evidenceTexts)
        ? body.evidenceTexts
        : [];
    const enableNativeSearch = body.enable_native_search === true || body.enableNativeSearch === true;
    const enriched = await enrichProfessionalProfiles(people.slice(0, 20), {
      env: process.env,
      evidenceTexts,
      enableNativeSearch,
      max_queries: 2,
    });
    const results = (enriched.people || []).map((person, i) => {
      const audit = (enriched.audit || [])[i] || {};
      const verified =
        person.professional_profile_verified === true
          ? {
              profile_status: PROFILE_MATCH_STATUS.VERIFIED,
              profile_url: person.professional_profile_url,
            }
          : null;
      return {
        person,
        audit,
        customer: customerProfileLink(verified || { profile_status: audit.status }),
      };
    });
    return res.json({
      ok: true,
      hotel_id: hotelId,
      results,
      audit: enriched.audit,
      note: "Only VERIFIED person LinkedIn /in/ URLs are customer-visible. No paid Webhound.",
      paid_webhound: false,
    });
  } catch (err) {
    console.error("[hotel-intelligence-research] enrich-profiles", err);
    return res.status(500).json({ ok: false, error: "profile_enrichment_failed" });
  }
}
