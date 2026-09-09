/**
 * Packet 2.6C-R1 — Research archive integrity + completion guards.
 * Live customer archive never includes simulation / test runs.
 */

import { getDossierById } from "../dossier/index.js";
import { isActiveStatus } from "./statuses.js";

export const LIVE_ARCHIVE_STATUSES = Object.freeze([
  "COMPLETED",
  "COMPLETED_WITH_OPEN_QUESTIONS",
  "PARTIAL",
]);

export function isSimulationRequest(request = {}) {
  return (
    request.is_simulation === true ||
    request.simulated === true ||
    String(request.provider_strategy || "").toUpperCase() === "SIMULATION" ||
    String(request.provider || "").toUpperCase() === "SIMULATION" ||
    String(request.status || "").toUpperCase() === "SIMULATED"
  );
}

/**
 * True when report_id resolves to a real dossier/report surface.
 * Simulation placeholder IDs (addendum_sim_*) never resolve.
 */
export function reportArtifactExists(reportId) {
  const id = String(reportId || "").trim();
  if (!id) return false;
  if (/^addendum_sim_/i.test(id)) return false;
  const dossier = getDossierById(id);
  return Boolean(dossier && dossier.dossier_id);
}

export function canExposeViewReport(request = {}) {
  return reportArtifactExists(request.report_id);
}

export function canExposeDownloadPdf(request = {}) {
  // R9 PDF is dossier-scoped; same resolvability gate as View Report.
  return reportArtifactExists(request.report_id || request.pdf_id);
}

/**
 * Live archive eligibility: production/historical only, with resolvable report.
 */
export function isLiveArchiveEligible(request = {}, { includeSimulations = false } = {}) {
  if (!request) return false;
  if (!includeSimulations && isSimulationRequest(request)) return false;
  const status = String(request.status || "").toUpperCase();
  if (!LIVE_ARCHIVE_STATUSES.includes(status)) return false;
  if (!reportArtifactExists(request.report_id)) return false;
  return true;
}

/**
 * Zero-evidence completion is forbidden for real research unless explicitly validated.
 */
export function assertCompletableResearchResult(input = {}) {
  const simulated = isSimulationRequest(input);
  if (simulated) {
    return { ok: true, simulated: true, archive_eligible: false };
  }

  const reportId = input.report_id;
  const normalizedId = input.normalized_research_id;
  if (!reportId || !reportArtifactExists(reportId)) {
    const err = new Error("completed_requires_report_artifact");
    err.code = "completed_requires_report_artifact";
    throw err;
  }
  if (!normalizedId && !input.immutable_historical) {
    const err = new Error("completed_requires_research_artifact");
    err.code = "completed_requires_research_artifact";
    throw err;
  }

  const sources = Number(input.source_count);
  const findings = Number(input.finding_count);
  const oq = Number(input.open_question_count);
  const zeroEvidence =
    (Number.isFinite(sources) ? sources : 0) === 0 &&
    (Number.isFinite(findings) ? findings : 0) === 0 &&
    (Number.isFinite(oq) ? oq : 0) === 0;

  if (zeroEvidence && input.explicit_no_evidence_validated !== true) {
    const err = new Error("zero_evidence_completion_forbidden");
    err.code = "zero_evidence_completion_forbidden";
    throw err;
  }

  return { ok: true, simulated: false, archive_eligible: true };
}

export function filterLiveArchiveRequests(requests = [], opts = {}) {
  return (requests || []).filter((r) => isLiveArchiveEligible(r, opts));
}

export function filterLiveActiveRequests(requests = [], opts = {}) {
  const includeSim = Boolean(opts.includeSimulations);
  return (requests || []).filter((r) => {
    if (!isActiveStatus(r.status)) return false;
    if (!includeSim && isSimulationRequest(r)) return false;
    return true;
  });
}

/**
 * Remove simulation requests/runs from a hotel index (live store cleanup).
 * Does not delete fixture test roots used by automated tests.
 */
export function purgeSimulationRequestsFromIndex(index = {}) {
  const requests = Array.isArray(index.requests) ? index.requests : [];
  const runs = Array.isArray(index.runs) ? index.runs : [];
  const keepRequests = requests.filter((r) => !isSimulationRequest(r));
  const keepIds = new Set(keepRequests.map((r) => r.request_id));
  const keepRuns = runs.filter(
    (r) => keepIds.has(r.request_id) && !(r.simulated === true || r.is_simulation === true)
  );
  return {
    ...index,
    requests: keepRequests,
    runs: keepRuns,
    purged_request_ids: requests.filter((r) => isSimulationRequest(r)).map((r) => r.request_id),
  };
}

/**
 * KGPV property identity guard for Change & Opportunity / review themes.
 */
export const KGPV_PROPERTY_IDENTITY = Object.freeze({
  hotel_id: "recUNycnMwOVFX0hc",
  canonical_name: "Krystal Grand Puerto Vallarta",
  excluded_adjacent: ["Krystal Resort Puerto Vallarta"],
  note: "Krystal Grand Puerto Vallarta ≠ Krystal Resort Puerto Vallarta",
});

export function assertKgpvPropertyIdentity(evidenceHotelName) {
  const name = String(evidenceHotelName || "").trim().toLowerCase();
  if (!name) return { ok: false, reason: "missing_property_name" };
  if (/krystal\s+resort\s+puerto\s+vallarta/i.test(name) && !/grand/i.test(name)) {
    return { ok: false, reason: "adjacent_krystal_resort_excluded" };
  }
  if (/krystal\s+grand\s+puerto\s+vallarta/i.test(name)) {
    return { ok: true };
  }
  return { ok: false, reason: "property_name_mismatch" };
}

/**
 * Review theme frequency — single anecdote is never a hotel-level finding.
 */
export const THEME_FREQUENCY = Object.freeze([
  "ISOLATED",
  "REPEATED",
  "PERSISTENT",
  "MULTI_SOURCE",
]);

export function classifyThemeFrequency(input = {}) {
  const independentSources = Number(input.independent_source_count || 0);
  const distinctDates = Number(input.distinct_date_count || 0);
  const platforms = Number(input.platform_count || 0);
  if (independentSources <= 1 && distinctDates <= 1) return "ISOLATED";
  if (platforms >= 2 || independentSources >= 3) return "MULTI_SOURCE";
  if (distinctDates >= 3 || independentSources >= 2) return "PERSISTENT";
  if (independentSources >= 2 || distinctDates >= 2) return "REPEATED";
  return "ISOLATED";
}

export function mayPromoteReviewThemeToFinding(frequency) {
  return frequency === "REPEATED" || frequency === "PERSISTENT" || frequency === "MULTI_SOURCE";
}
