/**
 * Context.dev pilot measurement — aggregates journal ops + optional account USD evidence.
 * Does not invent a second accounting framework; reads existing journals/ledgers.
 */

import fs from "node:fs";
import path from "node:path";
import { CONTEXT_DEV_CREDIT_COSTS } from "../../context-dev/credit-ledger.js";
import { summarizeContextDevJournalAccounting } from "./research-operation-journal.js";

export const CI_DEV_PILOT_MEASUREMENT_VERSION = "ci-dev-pilot-measurement-v1";

function percentile(sortedAsc, p) {
  if (!sortedAsc.length) return null;
  if (sortedAsc.length === 1) return sortedAsc[0];
  const idx = (sortedAsc.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sortedAsc[lo];
  const w = idx - lo;
  return sortedAsc[lo] * (1 - w) + sortedAsc[hi] * w;
}

function mean(nums) {
  if (!nums.length) return null;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

/**
 * Extract Context.dev operation rows from a case operation_journal.
 */
export function extractContextDevOperationsFromJournal(caseRecord, meta = {}) {
  const journal = Array.isArray(caseRecord?.operation_journal) ? caseRecord.operation_journal : [];
  const byKey = new Map();
  let seq = 0;
  for (const e of journal) {
    const wk = e?.work_key;
    if (!wk) continue;
    const provider = String(e.provider || e.reservation?.provider || "").toLowerCase();
    if (!(provider === "context_dev" || /^context_dev:/.test(String(wk)))) continue;
    seq += 1;
    const prev = byKey.get(wk) || {
      work_key: wk,
      request_sequence: seq,
      hotel_id: meta.hotel_id || caseRecord?.hotel_id || null,
      case_id: caseRecord?.case_id || meta.case_id || null,
      pilot_id: meta.pilot_id || null,
      dhl_id: meta.dhl_id || null,
      op: e.op || null,
      endpoint: e.op || null,
      objective: caseRecord?.objective || meta.objective || null,
      stage: e.stage || caseRecord?.current_stage || null,
      timestamps: [],
      credits_reserved: null,
      accounting_estimate: null,
      provider_confirmed_credits: null,
      measurement_source: null,
      measurement_confidence: null,
      execution_status: "unknown",
      replay: false,
      key_metadata: null,
      durable_result_kind: null,
      evidence_urls: [],
    };
    prev.timestamps.push(e.at || e.ts || null);
    if (e.reservation_credits != null) prev.credits_reserved = Number(e.reservation_credits);
    if (e.estimated_credits != null && prev.accounting_estimate == null) {
      prev.accounting_estimate = Number(e.estimated_credits);
    }
    if (e.event === "replay_stored_result" || e.kind === "skip") {
      prev.replay = true;
      if (prev.execution_status === "unknown") prev.execution_status = "replay_zero_dispatch";
    }
    if (e.event === "call_started" || e.status === "IN_FLIGHT") {
      if (!prev.replay) prev.execution_status = "dispatched";
    }
    if (e.event === "call_completed" || e.status === "COMPLETED") {
      prev.execution_status = prev.replay ? "replay_zero_dispatch" : "success";
    }
    if (e.event === "provider_failed" || e.status === "FAILED") {
      prev.execution_status = "failure";
    }
    if (e.event === "pending_reconcile_no_blind_retry" || e.status === "IN_FLIGHT_UNKNOWN") {
      prev.execution_status = "unknown_in_flight";
    }
    const km =
      e.durable_result?.key_metadata ||
      e.result?.key_metadata ||
      e.key_metadata ||
      null;
    if (km && typeof km === "object") {
      prev.key_metadata = {
        credits_consumed:
          km.credits_consumed != null ? Number(km.credits_consumed) : null,
        credits_remaining:
          km.credits_remaining != null ? Number(km.credits_remaining) : null,
      };
      if (Number.isFinite(prev.key_metadata.credits_consumed)) {
        prev.provider_confirmed_credits = prev.key_metadata.credits_consumed;
        prev.measurement_source = "provider_key_metadata.credits_consumed";
        prev.measurement_confidence = "provider_reported_per_response";
      }
    }
    if (e.durable_result?.kind) prev.durable_result_kind = e.durable_result.kind;
    const urls = [];
    if (e.url) urls.push(e.url);
    if (e.durable_result?.data?.results) {
      for (const r of e.durable_result.data.results.slice(0, 10)) {
        if (r.url || r.link) urls.push(r.url || r.link);
      }
    }
    prev.evidence_urls = [...new Set([...(prev.evidence_urls || []), ...urls])].slice(0, 20);
    // Scheduled estimate from known cost table when not otherwise set
    if (prev.accounting_estimate == null && prev.op) {
      const op = String(prev.op);
      if (/search/i.test(op)) prev.accounting_estimate = CONTEXT_DEV_CREDIT_COSTS.search_per_10_results;
      else if (/scrape/i.test(op)) prev.accounting_estimate = CONTEXT_DEV_CREDIT_COSTS.scrape_markdown;
      else if (/extract/i.test(op)) prev.accounting_estimate = CONTEXT_DEV_CREDIT_COSTS.extract;
    }
    if (!prev.measurement_source && prev.credits_reserved != null) {
      prev.measurement_source = "journal_reservation_settle_estimate";
      prev.measurement_confidence = "scheduled_cost_table";
    }
    byKey.set(wk, prev);
  }
  return [...byKey.values()];
}

/**
 * Link Context.dev ops to final hotel outcome / evidence contribution.
 * Never infer contribution from HTTP success alone.
 */
export function annotateOperationEvidenceContribution(ops, caseRecord, hotelRow = {}) {
  const cited = new Set();
  const contradiction = new Set();
  const sources = [
    ...(Array.isArray(caseRecord?.evidence) ? caseRecord.evidence : []),
    ...(Array.isArray(caseRecord?.supporting_evidence) ? caseRecord.supporting_evidence : []),
    ...(Array.isArray(caseRecord?.claims) ? caseRecord.claims : []),
    caseRecord?.final_claim,
    caseRecord?.adjudication,
    hotelRow?.evidence,
  ].filter(Boolean);

  function collectUrls(node, into) {
    if (!node) return;
    if (typeof node === "string") {
      if (/^https?:\/\//i.test(node)) into.add(node);
      return;
    }
    if (Array.isArray(node)) {
      for (const x of node) collectUrls(x, into);
      return;
    }
    if (typeof node === "object") {
      for (const [k, v] of Object.entries(node)) {
        if (/url|link|source|citation/i.test(k) && typeof v === "string") into.add(v);
        else if (/contradict|reject|conflict/i.test(k)) collectUrls(v, contradiction);
        else collectUrls(v, into);
      }
    }
  }
  collectUrls(sources, cited);
  collectUrls(caseRecord?.contradictions || caseRecord?.rejected_claims, contradiction);

  const finalOutcome = hotelRow?.status || caseRecord?.final_status || null;

  return (ops || []).map((op) => {
    const urls = op.evidence_urls || [];
    let contribution = "contribution_unknown";
    if (op.replay) {
      contribution = "replay_zero_new_evidence";
    } else if (urls.some((u) => contradiction.has(u))) {
      contribution = "used_to_reject_or_contradict";
    } else if (urls.some((u) => cited.has(u))) {
      contribution = "used_in_supported_final_claim";
    } else if (urls.length > 0 && (cited.size > 0 || contradiction.size > 0)) {
      contribution = "retrieved_but_unused";
    } else if (urls.length > 0) {
      contribution = "contribution_unknown";
    } else if (op.execution_status === "success" || op.execution_status === "failure") {
      contribution = "contribution_unknown";
    }
    return {
      ...op,
      final_hotel_outcome: finalOutcome,
      evidence_contribution: contribution,
    };
  });
}

/**
 * Attempt account-based USD/credit rate. Never invent internet pricing.
 */
export async function attemptAccountBasedUsdPerCredit({
  env = process.env,
  root = process.cwd(),
  keyMetadataSamples = [],
  deps = {},
} = {}) {
  const out = {
    usd_per_credit: "UNKNOWN",
    evidence_source: null,
    period: null,
    numerator: null,
    denominator: null,
    calculation: null,
    confidence: "none",
    rate_type: null,
    notes: [],
  };

  const envRate = Number(env.CONTEXT_DEV_USD_PER_CREDIT ?? env.CI_20HOTEL_CONTEXT_DEV_USD_PER_CREDIT);
  if (Number.isFinite(envRate) && envRate > 0) {
    out.usd_per_credit = envRate;
    out.evidence_source = "env:CONTEXT_DEV_USD_PER_CREDIT";
    out.confidence = "explicit_env_documented_rate";
    out.rate_type = "configured_account_rate";
    out.calculation = "env value as confirmed account rate";
    return out;
  }

  // Local billing artifacts (if founder placed them)
  const candidates = [
    path.join(root, "data", "context-dev-billing.json"),
    path.join(root, "data", "context-dev", "billing.json"),
    path.join(root, "tmp", "context-dev-billing.json"),
  ];
  for (const p of candidates) {
    if (!fs.existsSync(p)) continue;
    try {
      const raw = JSON.parse(fs.readFileSync(p, "utf8"));
      const usd = Number(raw.usd_charged ?? raw.usd ?? raw.amount_usd);
      const credits = Number(raw.credits_consumed ?? raw.credits);
      if (Number.isFinite(usd) && usd > 0 && Number.isFinite(credits) && credits > 0) {
        out.usd_per_credit = usd / credits;
        out.evidence_source = `local_file:${path.relative(root, p)}`;
        out.numerator = usd;
        out.denominator = credits;
        out.period = raw.period || raw.invoice_period || null;
        out.calculation = `${usd} USD / ${credits} credits`;
        out.confidence = raw.confidence || "local_billing_file";
        out.rate_type = raw.rate_type || "effective_allocated";
        out.notes.push("Loaded from local billing artifact; not internet pricing.");
        return out;
      }
      out.notes.push(`Local billing file present but insufficient fields: ${p}`);
    } catch (err) {
      out.notes.push(`Failed to parse ${p}: ${String(err?.message || err)}`);
    }
  }

  // Provider key_metadata gives credits only — not USD. Balance delta cannot yield USD alone.
  const remainingSamples = keyMetadataSamples
    .map((s) => Number(s?.credits_remaining))
    .filter((n) => Number.isFinite(n));
  if (remainingSamples.length >= 2) {
    out.notes.push(
      `Observed credits_remaining samples (${remainingSamples.length}) but no USD numerator — cannot derive USD/credit.`
    );
  }

  // Optional SDK credit-usage (monitors) — credits only
  if (typeof deps.fetchMonitorCreditUsage === "function") {
    try {
      const usage = await deps.fetchMonitorCreditUsage();
      out.notes.push(
        `Monitor credit usage fetched (credits-only): ${JSON.stringify(usage?.total_credits ?? usage)?.slice(0, 120)}`
      );
    } catch (err) {
      out.notes.push(`Monitor credit usage unavailable: ${String(err?.message || err)}`);
    }
  }

  out.usd_per_credit = "UNKNOWN";
  out.confidence = "unavailable";
  out.notes.push(
    "No explicit Context.dev pricing metadata, no attributable invoice/balance USD evidence. USD remains UNKNOWN."
  );
  return out;
}

/**
 * Build economics report from hotel rows + case journals.
 */
export function buildContextDevEconomicsReport({
  pilotId,
  hotels = [],
  caseStore,
  policy,
  usdEvidence,
  startedAt,
  completedAt,
} = {}) {
  const perHotel = [];
  const allOps = [];

  for (const h of hotels) {
    const caseId = h.case_id;
    const caseRec = caseId && caseStore?.getCase ? caseStore.getCase(caseId) : null;
    const journalAcct = summarizeContextDevJournalAccounting(caseRec?.operation_journal || []);
    const rawOps = extractContextDevOperationsFromJournal(caseRec, {
      hotel_id: h.hotel_id,
      case_id: caseId,
      pilot_id: pilotId,
      dhl_id: h.dhl_id || h.property_identity_key || null,
      objective: caseRec?.objective,
    });
    const ops = annotateOperationEvidenceContribution(rawOps, caseRec, h);
    allOps.push(...ops);

    const dispatched = ops.filter((o) => o.execution_status === "success" || o.execution_status === "failure");
    const providerConfirmed = ops
      .map((o) => o.provider_confirmed_credits)
      .filter((n) => Number.isFinite(n));
    const settled = journalAcct.settled_credits;
    const perCaseMax = Number(policy?.limits?.per_case?.context_dev_max) || 8;
    const budgetLimited =
      settled >= perCaseMax ||
      h.stop_reason === "BUDGET_LIMITED" ||
      h.reason === "CASE_CONTEXT_EXHAUSTED" ||
      h.reason === "CONTEXT_DEV_BUDGET_EXHAUSTED" ||
      /BUDGET/i.test(String(h.missing_link || h.status || ""));

    perHotel.push({
      hotel_id: h.hotel_id,
      hotel_name: h.hotel || h.hotel_name || null,
      case_id: caseId,
      final_status: h.status || caseRec?.final_status || null,
      settled_credits: settled,
      settled_calls: journalAcct.settled_calls,
      outstanding_credits: journalAcct.outstanding_credits,
      dispatched_ops: dispatched.length,
      replay_ops: ops.filter((o) => o.replay).length,
      provider_confirmed_credits_sum: providerConfirmed.length
        ? providerConfirmed.reduce((a, b) => a + b, 0)
        : null,
      accounting_estimate_sum: ops.reduce((s, o) => s + (Number(o.accounting_estimate) || 0), 0),
      budget_limited: budgetLimited,
      owner_ok: Boolean(h.flags?.ownerOk),
      person_ok: Boolean(h.flags?.personOk || h.flags?.personCorroborated),
      email_ok: Boolean(h.flags?.acceptedEmail),
      phone_ok: Boolean(h.flags?.phoneNamed),
      ops,
    });
  }

  const settledSeries = perHotel.map((h) => Number(h.settled_credits) || 0).sort((a, b) => a - b);
  const totalSettled = settledSeries.reduce((a, b) => a + b, 0);
  const attempted = perHotel.length;
  const completed = perHotel.filter((h) => h.final_status && h.final_status !== "INTERRUPTED").length;

  const avgAmong = (pred) => {
    const rows = perHotel.filter(pred);
    if (!rows.length) return "N/A";
    return mean(rows.map((r) => Number(r.settled_credits) || 0));
  };
  const totalPerSuccess = (pred) => {
    const n = perHotel.filter(pred).length;
    if (!n) return "N/A";
    return totalSettled / n;
  };

  const byOp = {};
  for (const op of allOps) {
    if (op.replay) continue;
    const k = op.op || op.endpoint || "unknown";
    byOp[k] = byOp[k] || { count: 0, estimated_credits: 0, provider_confirmed: 0 };
    byOp[k].count += 1;
    byOp[k].estimated_credits += Number(op.accounting_estimate) || 0;
    if (Number.isFinite(op.provider_confirmed_credits)) {
      byOp[k].provider_confirmed += op.provider_confirmed_credits;
    }
  }

  const perCaseMax = Number(policy?.limits?.per_case?.context_dev_max) || 8;
  const observedP90 = percentile(settledSeries, 0.9);
  const safetyBuffer = 2;
  const budgetLimitedCount = perHotel.filter((h) => h.budget_limited).length;
  const fullyCensored = budgetLimitedCount === attempted && attempted > 0;
  // When observations are censored at the per-case cap, do not treat p90+buffer as a measured completion cost.
  const recommendedPerHotel = fullyCensored
    ? null
    : observedP90 == null
      ? null
      : Math.ceil(observedP90 + safetyBuffer);

  const usdRate = usdEvidence?.usd_per_credit;
  const usdKnown = typeof usdRate === "number" && Number.isFinite(usdRate) && usdRate > 0;

  return {
    version: CI_DEV_PILOT_MEASUREMENT_VERSION,
    pilot_id: pilotId,
    started_at: startedAt,
    completed_at: completedAt,
    sample_size: attempted,
    percentile_method: "linear interpolation on sorted ascending series; preliminary for n=5",
    hotels_attempted: attempted,
    hotels_completed: completed,
    actual_context_dev_dispatches: allOps.filter(
      (o) => o.execution_status === "success" || o.execution_status === "failure"
    ).length,
    total_settled_credits: totalSettled,
    total_settled_label: "journal_settleSpend_scheduled_or_reserved",
    provider_confirmed_total: (() => {
      const vals = allOps.map((o) => o.provider_confirmed_credits).filter((n) => Number.isFinite(n));
      return vals.length ? vals.reduce((a, b) => a + b, 0) : null;
    })(),
    averages: {
      mean_credits_per_hotel: mean(settledSeries),
      median: percentile(settledSeries, 0.5),
      p75: percentile(settledSeries, 0.75),
      p90: observedP90,
      max: settledSeries.length ? settledSeries[settledSeries.length - 1] : null,
    },
    credits_by_operation: byOp,
    average_credits_among: {
      supported_owner: avgAmong((h) => h.owner_ok),
      supported_person: avgAmong((h) => h.person_ok),
      attributable_email: avgAmong((h) => h.email_ok),
      attributable_phone: avgAmong((h) => h.phone_ok),
    },
    total_pilot_credits_per_success: {
      owner: totalPerSuccess((h) => h.owner_ok),
      person: totalPerSuccess((h) => h.person_ok),
      email: totalPerSuccess((h) => h.email_ok),
      phone: totalPerSuccess((h) => h.phone_ok),
    },
    credits_on_unknown_status: perHotel
      .filter((h) => /UNKNOWN|UNRESOLVED|PARTIAL/i.test(String(h.final_status || "")))
      .reduce((s, h) => s + (Number(h.settled_credits) || 0), 0),
    credits_on_budget_limited: perHotel
      .filter((h) => h.budget_limited)
      .reduce((s, h) => s + (Number(h.settled_credits) || 0), 0),
    credits_unused_evidence: allOps
      .filter((o) => o.evidence_contribution === "retrieved_but_unused")
      .reduce((s, o) => s + (Number(o.provider_confirmed_credits ?? o.accounting_estimate) || 0), 0),
    credits_contribution_unknown: allOps
      .filter((o) => o.evidence_contribution === "contribution_unknown")
      .reduce((s, o) => s + (Number(o.provider_confirmed_credits ?? o.accounting_estimate) || 0), 0),
    budget_limited_count: budgetLimitedCount,
    budget_limited_censored_note:
      budgetLimitedCount > 0
        ? "Budget-limited hotels are censored observations; true completion cost above the 8-credit cap was not measured."
        : null,
    per_hotel: perHotel,
    usd: {
      usd_per_credit: usdKnown ? usdRate : "UNKNOWN",
      usd_status: usdKnown ? "KNOWN" : "UNKNOWN",
      evidence: usdEvidence || null,
      total_usd:
        usdKnown ? totalSettled * usdRate : "UNKNOWN",
      recommended_usd_batch_cap: usdKnown
        ? (recommendedPerHotel || perCaseMax) * 5 * usdRate
        : "NOT_YET_CALCULABLE",
    },
    recommended_caps: {
      note: "Recommendations only — not applied.",
      CONTEXT_DEV_CREDIT_MAX_PER_HOTEL: recommendedPerHotel,
      calculation: fullyCensored
        ? `all ${attempted} hotels censored at per-case cap ${perCaseMax}; true completion cost not measured — do not treat ceil(p90+buffer) as sufficient`
        : observedP90 == null
          ? "insufficient_data"
          : `ceil(observed_p90=${observedP90} + safety_buffer=${safetyBuffer})`,
      normal_target: fullyCensored ? `unknown_above_${perCaseMax}` : percentile(settledSeries, 0.5),
      warning_threshold: fullyCensored ? perCaseMax : percentile(settledSeries, 0.75),
      hard_per_hotel_cap: fullyCensored
        ? `PROPOSE_FOLLOW_UP_ABOVE_${perCaseMax}`
        : recommendedPerHotel,
      five_hotel_batch: recommendedPerHotel == null ? null : recommendedPerHotel * 5,
      twenty_hotel_batch_hypothetical:
        recommendedPerHotel == null ? null : recommendedPerHotel * 20,
      CONTEXT_DEV_USD_MAX_PER_RUN: usdKnown
        ? (recommendedPerHotel || perCaseMax) * 5 * usdRate
        : "NOT_YET_CALCULABLE",
      censored_follow_up:
        budgetLimitedCount > 0
          ? `Propose bounded follow-up under founder review with higher per-case credit cap (e.g. 16–24) to unmask completion cost for ${budgetLimitedCount} budget-limited hotel(s). Extract(10) remains unaffordable under cap ${perCaseMax}.`
          : null,
      current_pilot_per_case_cap: perCaseMax,
      provisional_p90_plus_buffer_if_uncensored:
        observedP90 == null ? null : Math.ceil(observedP90 + safetyBuffer),
    },
  };
}
