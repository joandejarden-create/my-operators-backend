/**
 * Bethesda three-search research scope.
 * Official-page research does not use this. The greenfield canary does not authorize it.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { planDemandSearches } from "./gdi-demand-sourcing-v1.js";
import { executeGuardedSerpQuery } from "./gdi-serpapi-google-discovery-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const approvalLedgerPath = path.join(root, "data/group-demand-intelligence/gdi-founder-approval-v1/approval-ledger-v1.json");

export const BETHESDA_WORKSTREAM_RESEARCH_HOTEL = "recLuxvwwxID7U2B8";
export const SERPAPI_INCLUDED_PLAN_USD_PER_CREDIT = 0.025;

const SELECTED = [
  {
    pathway: "corporate_institutional_programs",
    workstreams: ["local_meetings_catering", "scaling_companies", "professional_planners_dmc"],
  },
  {
    pathway: "group_accommodation_travel",
    workstreams: ["entertainment_production", "tours_educational_specialist", "exhibitor_sponsor_teams", "professional_planners_dmc"],
  },
  {
    pathway: "emerging_group_demand",
    workstreams: ["scaling_companies", "entertainment_production"],
  },
];

export function bethesdaWorkstreamResearchScope() {
  const planned = planDemandSearches({ language: "en", territoryLabel: "Bethesda Maryland" });
  const byPathway = new Map(planned.map((row) => [row.pathway, row]));
  const queries = SELECTED.map((row) => {
    const plannedQuery = byPathway.get(row.pathway);
    return {
      hotelId: BETHESDA_WORKSTREAM_RESEARCH_HOTEL,
      pathway: row.pathway,
      workstreams: row.workstreams,
      query: plannedQuery.query,
      language: "en",
    };
  });
  return {
    operation: "BETHESDA_WORKSTREAM_RESEARCH",
    hotelId: BETHESDA_WORKSTREAM_RESEARCH_HOTEL,
    provider: "serpapi",
    maxRequests: 3,
    maxRequestsPerHotel: 3,
    includedPlanUsdPerCredit: SERPAPI_INCLUDED_PLAN_USD_PER_CREDIT,
    planAllocationUsd: 0.075,
    incrementalCashUsd: 0,
    cashPurchase: false,
    estimatedUsd: 0.075,
    unsupportedEstimateUsd: 0.125,
    unsupportedReason: "Five pathway rows times $0.025. Two rows repeat an earlier query and are not additional searches.",
    restrictions: ["no_publication", "no_canonical_write", "no_automatic_retry", "not_the_greenfield_canary", "no_airtable_write"],
    queries,
  };
}

export function loadLiveBethesdaWorkstreamApproval() {
  const scope = bethesdaWorkstreamResearchScope();
  if (!fs.existsSync(approvalLedgerPath)) return { ok: false, reason: "MEMBERSTACK_BETHESDA_RESEARCH_APPROVAL_REQUIRED", scope };
  const ledger = JSON.parse(fs.readFileSync(approvalLedgerPath, "utf8"));
  const record = (ledger.records || []).find((row) => row.operation === "BETHESDA_WORKSTREAM_RESEARCH" && row.status === "APPROVED");
  if (!record) return { ok: false, reason: "MEMBERSTACK_BETHESDA_RESEARCH_APPROVAL_REQUIRED", scope };
  if (record.verifiedVia !== "rest" && record.verifiedVia !== "jwks") return { ok: false, reason: "SESSION_NOT_LIVE", scope };
  if (record.expiresAt && Date.parse(record.expiresAt) < Date.now()) return { ok: false, reason: "APPROVAL_EXPIRED", scope };
  const approved = (record.queries || []).map((row) => (typeof row === "string" ? row : row.query));
  const expected = scope.queries.map((row) => row.query);
  const sameHotel = Array.isArray(record.hotelIds) && record.hotelIds.length === 1 && record.hotelIds[0] === scope.hotelId;
  if (!sameHotel || record.provider !== "serpapi" || Number(record.maxRequests) !== 3 || approved.join("|") !== expected.join("|")) {
    return { ok: false, reason: "BETHESDA_RESEARCH_SCOPE_MISMATCH", scope };
  }
  if (!["no_publication", "no_canonical_write", "no_automatic_retry"].every((item) => (record.restrictions || []).includes(item))) {
    return { ok: false, reason: "BETHESDA_RESEARCH_SCOPE_MISMATCH", scope };
  }
  return { ok: true, approval: record, scope };
}

export async function runApprovedBethesdaResearch() {
  const live = loadLiveBethesdaWorkstreamApproval();
  if (!live.ok) return { executed: false, searches: [], organizations: [], reason: live.reason };
  const searches = [];
  const organizations = [];
  let stopped = null;
  for (const query of live.scope.queries) {
    const result = await executeGuardedSerpQuery({
      hotelId: live.scope.hotelId,
      query: query.query,
      pathway: query.pathway,
      workstream: query.workstreams[0],
      approvalId: live.approval.approvalId,
      approvalQuerySet: live.scope.queries.map((row) => row.query),
      maxRequests: 3,
      allowanceUsd: live.scope.planAllocationUsd,
    });
    searches.push(result.search || { query: query.query, status: result.reason });
    if (result.organizations) organizations.push(...result.organizations);
    if (!result.ok) {
      stopped = result.reason;
      break;
    }
  }
  return { executed: searches.some((row) => row.searchId), searches, organizations, stopped, reason: stopped };
}
