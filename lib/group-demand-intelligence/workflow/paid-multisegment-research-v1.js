/**
 * Paid multi-segment GDI research pass — shared coordinator integration.
 *
 * Authorization envelope: founder-approved total NEW spend (default $11).
 * Historical Parallel $2 ledger is preserved separately and not reset.
 *
 * Does not auto-publish. Does not invent dates/volumes/budgets.
 * Reuses Parallel job store + budget ledger; never resubmits known run_ids.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createParallelClient } from "../../hotel-intelligence/research/providers/parallel/client.js";
import {
  PARALLEL_PRICE_ESTIMATES_USD,
  reserveParallelBudget,
  commitParallelBudgetReservation,
  releaseParallelBudgetReservation,
  readParallelBudgetLedger,
  remainingParallelBudgetUsd,
} from "../parallel/budget-ledger-v1.js";
import {
  beginOrResumeParallelJob,
  markParallelJobSubmitted,
  markParallelJobCompleted,
  markParallelJobUncertain,
  createParallelJobKey,
} from "../parallel/job-store-v1.js";
import {
  isGdiParallelEnabled,
  isGdiParallelHotelAllowed,
  isGdiParallelKillSwitchOn,
  getGdiParallelFlagState,
} from "../parallel/feature-flag.js";
import { quarantineWrongMarketCandidates } from "./geographic-relevance-gate-v1.js";
import { qualifyScalingCompanyCandidate } from "./scaling-companies-research-v1.js";
import { qualifyLocalMeetingsCateringCandidate } from "./local-meetings-catering-research-v1.js";
import {
  scorecardFromPaidTaskResult,
  writeScorecardArtifacts,
  RESEARCH_SCORECARD_VERSION,
} from "./research-scorecard-v1.js";
import {
  buildHotelSegmentResearchBrief,
  briefToPromptFocus,
} from "./hotel-segment-research-brief-v1.js";

export const PAID_MULTISEGMENT_RESEARCH_VERSION =
  "gdi-paid-multisegment-research-v1";

/** Founder-authorized NEW spend for this validation pass (not additive to prior proposals). */
export const AUTHORIZED_NEW_SPEND_USD = 11;

/** Historical exhausted Parallel pilot — keep as audit trail; do not reset. */
export const HISTORICAL_PARALLEL_PILOT_USD = 2;

/** Surfe credits are prepaid; estimate USD for combined-cap accounting only. */
export const SURFE_EMAIL_CREDIT_USD_ESTIMATE = 0.25;
export const SURFE_MOBILE_CREDIT_USD_ESTIMATE = 0.35;

export const MAYFLOWER_HOTEL_ID = "rec0eTEnCqlsGJJt9";
export const SAN_DIEGO_HOTEL_ID = "recnPB3Q02NO8Ma1o";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadHotelConfig(hotelId) {
  const p = path.join(
    ROOT,
    "config/group-demand-intelligence/hotels",
    `${String(hotelId || "").trim()}.json`
  );
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function resolveParallelKey(env = process.env) {
  return Boolean(String(env.PARALLEL_KEY || env.PARALLEL_API_KEY || "").trim());
}

function authorizationLedgerPath(env = process.env) {
  if (env.GDI_PAID_MS_BUDGET_LEDGER_PATH) {
    return path.resolve(env.GDI_PAID_MS_BUDGET_LEDGER_PATH);
  }
  const root = env.GDI_DATA_ROOT
    ? path.resolve(env.GDI_DATA_ROOT)
    : path.join(ROOT, "data/group-demand-intelligence");
  return path.join(root, "paid-multisegment-budget", "authorization-ledger-v1.json");
}

/**
 * Task-level allocation + stop criteria BEFORE any paid submit.
 * Unequal by hotel/segment — profile-driven.
 */
export function buildPaidMultisegmentAllocation(opts = {}) {
  const capUsd = Number(opts.authorizedUsd ?? AUTHORIZED_NEW_SPEND_USD);
  const tasks = [
    {
      taskId: "contact_wisedocs_may",
      hotelId: MAYFLOWER_HOTEL_ID,
      segment: "scaling_companies",
      kind: "contact_recovery",
      priority: 1,
      processor: "core",
      estimatedUsd: PARALLEL_PRICE_ESTIMATES_USD.core ?? 0.025,
      reserveUsd: 0.15,
      stopCriteria:
        "Stop after 1 Parallel task; publish only if usable email/phone/form recovered. Do not force Surfe without named person.",
      promptFocus:
        "Recover a usable commercial contact route for Wisedocs (wisedocs.ai) DC office — functional email, phone, contact form, or named workplace/customer-success owner. Do not invent contacts.",
      knownOrg: {
        organizationName: "Wisedocs",
        officialDomain: "wisedocs.ai",
        opportunityId: "gdi_sc_0fe98055e8df9f75",
      },
    },
    {
      taskId: "contact_shipyard_sd",
      hotelId: SAN_DIEGO_HOTEL_ID,
      segment: "scaling_companies",
      kind: "contact_recovery",
      priority: 1,
      processor: "core",
      estimatedUsd: PARALLEL_PRICE_ESTIMATES_USD.core ?? 0.025,
      reserveUsd: 0.15,
      stopCriteria:
        "Stop after 1 Parallel task; keep INTERNAL if still no usable route. Surfe only if named person + org established.",
      promptFocus:
        "Recover a usable commercial contact route for The Shipyard (theshipyard.com) San Diego HQ at 185 West F Street — email, phone, form, or named office/executive-support contact. Do not invent.",
      knownOrg: {
        organizationName: "The Shipyard",
        officialDomain: "theshipyard.com",
        opportunityId: "gdi_sc_188e25b6b3b56637",
      },
    },
    {
      taskId: "scaling_discovery_may",
      hotelId: MAYFLOWER_HOTEL_ID,
      segment: "scaling_companies",
      kind: "discovery",
      priority: 2,
      processor: "pro",
      estimatedUsd: PARALLEL_PRICE_ESTIMATES_USD.pro ?? 0.1,
      reserveUsd: 0.25,
      stopCriteria:
        "Stop after 1 task or when ≥2 candidates pass local+growth+hotel-use gates. Skip funding-only and directory hits.",
      promptFocus:
        "Discover growing companies with verified DC/downtown local connection and dated growth evidence that could use Mayflower for meetings/lodging. Exclude Kellen and Wisedocs (already assessed). Funding/hiring alone insufficient.",
    },
    {
      taskId: "scaling_discovery_sd",
      hotelId: SAN_DIEGO_HOTEL_ID,
      segment: "scaling_companies",
      kind: "discovery",
      priority: 2,
      processor: "pro",
      estimatedUsd: PARALLEL_PRICE_ESTIMATES_USD.pro ?? 0.1,
      reserveUsd: 0.25,
      stopCriteria:
        "Stop after 1 task or when ≥2 candidates qualify. Exclude ESET and The Shipyard.",
      promptFocus:
        "Discover growing companies with verified downtown San Diego / waterfront local connection and dated growth evidence for Wyndham San Diego Bayside lodging/meetings. Exclude ESET and The Shipyard.",
    },
    {
      taskId: "lmc_discovery_may",
      hotelId: MAYFLOWER_HOTEL_ID,
      segment: "local_meetings_catering",
      kind: "discovery",
      priority: 3,
      processor: "pro",
      estimatedUsd: PARALLEL_PRICE_ESTIMATES_USD.pro ?? 0.1,
      reserveUsd: 0.25,
      stopCriteria:
        "Stop after 1 task. Guestrooms not required. Reject venue-as-buyer and directory-only. Exclude ACEC/MW, MBA/MW, ASID DC Metro.",
      promptFocus:
        "Find named local meeting/catering buyers (chapters, private dining clubs, downtown organizers) with evidenced functions near Connecticut Avenue / downtown DC suitable for Mayflower banquet/meeting inventory. No guestroom requirement.",
    },
    {
      taskId: "lmc_discovery_sd",
      hotelId: SAN_DIEGO_HOTEL_ID,
      segment: "local_meetings_catering",
      kind: "discovery",
      priority: 3,
      processor: "pro",
      estimatedUsd: PARALLEL_PRICE_ESTIMATES_USD.pro ?? 0.1,
      reserveUsd: 0.25,
      stopCriteria:
        "Stop after 1 task. Exclude Downtown SD Partnership and BOMA SD. Guestrooms not required.",
      promptFocus:
        "Find named local meeting/catering buyers near downtown San Diego / Gaslamp / Little Italy with evidenced awards dinners, fundraisers, or recurring chapter functions suitable for Wyndham Bayside meetings product.",
    },
    {
      taskId: "pe_may_clubs_embassy",
      hotelId: MAYFLOWER_HOTEL_ID,
      segment: "weddings_social_private_events",
      kind: "discovery",
      priority: 4,
      processor: "pro",
      estimatedUsd: PARALLEL_PRICE_ESTIMATES_USD.pro ?? 0.1,
      reserveUsd: 0.25,
      stopCriteria:
        "Stop after 1–2 tasks or when 4+ venues assessed. Exclude Anderson House, DAR, Whittemore, Page House, Decatur House (already persisted). Venue marketing ≠ confirmed rooms.",
      promptFocus:
        "Assess remaining private clubs and Embassy Row / Dupont mansion venues for complementary lodging/referral relationships with Mayflower. Distinct venues with shared domains stay distinct. On-site lodging is not automatic rejection — assess actual relationship.",
    },
    {
      taskId: "pe_sd_gaslamp_yacht",
      hotelId: SAN_DIEGO_HOTEL_ID,
      segment: "weddings_social_private_events",
      kind: "discovery",
      priority: 4,
      processor: "pro",
      estimatedUsd: PARALLEL_PRICE_ESTIMATES_USD.pro ?? 0.1,
      reserveUsd: 0.35,
      maxTasks: 2,
      stopCriteria:
        "Up to 2 Parallel tasks. Cover Gaslamp/Little Italy independents and yacht/club gaps. Exclude Midway, EMPS, Maritime, Port Pavilion, The Lane (persisted). Stop early if low-yield.",
      promptFocus:
        "Discover independent Gaslamp / Little Italy event venues and yacht/private clubs relevant to Wyndham San Diego Bayside for venue-partnership / overflow lodging. Distinct venues remain distinct even if domains overlap.",
    },
    {
      taskId: "surfe_named_contact_budget",
      hotelId: null,
      segment: "contact_enrichment",
      kind: "surfe_reserve",
      priority: 5,
      processor: null,
      estimatedUsd: 1.0,
      reserveUsd: 1.0,
      stopCriteria:
        "Only spend when Parallel established named person + org for Wisedocs/Shipyard or a new qualified prospect. Max 4 email enrichments (~$0.25 est each). Do not invent people to enrich.",
      promptFocus: null,
      paidExecution: "surfe",
    },
  ];

  const sumReserve = tasks.reduce((s, t) => s + Number(t.reserveUsd || 0), 0);
  return {
    version: PAID_MULTISEGMENT_RESEARCH_VERSION,
    authorizedNewSpendUsd: capUsd,
    historicalPilotUsdPreserved: HISTORICAL_PARALLEL_PILOT_USD,
    pricingAsOf: PARALLEL_PRICE_ESTIMATES_USD.asOf || "2026-10-15",
    pricingNote: PARALLEL_PRICE_ESTIMATES_USD.docsNote,
    surfeCreditUsdEstimate: {
      email: SURFE_EMAIL_CREDIT_USD_ESTIMATE,
      mobile: SURFE_MOBILE_CREDIT_USD_ESTIMATE,
      note: "Surfe is prepaid credits — USD figures are combined-cap accounting estimates, not Parallel invoices",
    },
    totalReservedUsd: Number(sumReserve.toFixed(4)),
    remainingHeadroomUsd: Number((capUsd - sumReserve).toFixed(4)),
    stopRules: [
      "Spending all $11 is not a success criterion — stop low-yield tasks early",
      "Reuse existing jobs/results before new submits",
      "Do not resubmit known provider run_ids",
      "Do not force Wisedocs/Shipyard to publish by burning the whole budget",
      "No automatic renewal or unattended paid scheduling",
    ],
    tasks: tasks.map((t, i) => ({ ...t, order: i + 1 })),
  };
}

function hotelAntiMarketRules(hotelCfg = {}) {
  const id = text(hotelCfg.hotelId);
  const name = hotelCfg.displayName || hotelCfg.name || "the target hotel";
  const territory =
    hotelCfg.demandTerritory?.label || hotelCfg.city || "configured territory";
  if (id === SAN_DIEGO_HOTEL_ID) {
    return `- Do NOT return Bethesda, Maryland, NIH, Pooks Hill, Mayflower/DC, Dupont Circle DC, or other non-San-Diego market events for ${name}.
- Stay inside ${territory} (downtown / Harbor Drive / Gaslamp / Little Italy catchment as configured).`;
  }
  if (id === MAYFLOWER_HOTEL_ID) {
    return `- Do NOT return San Diego, Harbor Drive, Gaslamp, Bethesda Marriott/Pooks Hill sports packages, or other non-DC market events for ${name}.
- Stay inside ${territory} (downtown DC / Connecticut Avenue catchment as configured).`;
  }
  return `- Research ONLY occasions relevant to ${name} in ${territory}. Never substitute another hotel or market.`;
}

function buildSegmentTaskPrompt(task, hotelCfg) {
  const id = text(task.hotelId);
  const hotelName = hotelCfg?.displayName || hotelCfg?.name || "the target hotel";
  const city = hotelCfg?.city || "";
  const territory =
    hotelCfg?.demandTerritory?.label ||
    hotelCfg?.demandTerritory?.includes?.[0] ||
    city ||
    "";
  const rooms = hotelCfg?.capabilityProfile?.totalGuestrooms ?? "unknown";
  const meetingRooms =
    hotelCfg?.capabilityProfile?.meetingRoomCount ||
    hotelCfg?.capabilityProfile?.meetingRoomsIndoor ||
    "unknown";

  const schemaByKind =
    task.kind === "contact_recovery"
      ? `{ "candidates": [ {
  "organizationName", "officialDomain", "contactEmail", "contactPhone", "contactUrl",
  "contactName", "contactRole", "contactSourceUrl", "evidenceUrls",
  "whatWasFound", "whatRemainsUnresolved", "materialLimitations"
} ] }`
      : task.segment === "weddings_social_private_events"
        ? `{ "candidates": [ {
  "venueName", "organizationName", "officialDomain", "location", "category",
  "eventActivityEvidence", "lodgingOrReferralRationale", "contactEmail", "contactPhone",
  "contactUrl", "contactName", "contactRole", "sourceUrls", "whyRelevantToHotel",
  "knownVsInferred", "uncertainties", "recommendedFirstAction", "materialLimitations"
} ] }`
        : task.segment === "local_meetings_catering"
          ? `{ "candidates": [ {
  "organizationName", "officialDomain", "localConnection", "eventActivityEvidence",
  "functionType", "recurrenceNote", "contactEmail", "contactPhone", "contactUrl",
  "contactName", "contactRole", "hotelFitRationale", "whyThisMatters",
  "guestroomStatus", "sourceUrls", "recommendedFirstAction", "materialLimitations"
} ] }`
          : `{ "candidates": [ {
  "organizationName", "officialDomain", "localConnection", "datedGrowthEvidence",
  "growthEvidenceDate", "growthSignals", "operatingExpansionEvidence",
  "proposedHotelUse", "hotelFitRationale", "contactEmail", "contactPhone", "contactUrl",
  "contactName", "contactRole", "sourceUrls", "recommendedFirstAction", "materialLimitations"
} ] }`;

  return `You are researching hotel group demand for ONE hotel only.

TARGET HOTEL (identity lock):
- hotelId: ${id}
- name: ${hotelName}
- city/market: ${city || "see territory"}
- demand territory: ${territory || "hotel-configured catchment only"}
- guestrooms: ${rooms}
- meeting rooms: ${meetingRooms}

HARD RULES:
${hotelAntiMarketRules({ ...hotelCfg, hotelId: id })}
- Do not invent contacts, emails, dates, volumes, budgets, urgency, or booking authority.
- Cite first-party or high-authority sources with URLs.
- Prefer quality over quantity. Max 8 candidates.
- Unknown sourcing does not automatically disqualify a credible prospect.
- A functional email, phone, form, or intermediary can be a usable contact route.

TASK (${task.taskId} / ${task.segment} / ${task.kind}):
${task.promptFocus}

STOP CRITERIA: ${task.stopCriteria}

Return JSON only:
${schemaByKind}`;
}

function extractBalancedJsonObject(text) {
  const start = text.indexOf("{");
  if (start < 0) return null;
  let depth = 0;
  let inString = false;
  let escape = false;
  for (let i = start; i < text.length; i += 1) {
    const ch = text[i];
    if (inString) {
      if (escape) escape = false;
      else if (ch === "\\") escape = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') {
      inString = true;
      continue;
    }
    if (ch === "{") depth += 1;
    else if (ch === "}") {
      depth -= 1;
      if (depth === 0) return text.slice(start, i + 1);
    }
  }
  return null;
}

function tryParseJsonString(value) {
  if (typeof value !== "string") return value;
  const trimmed = value.trim();
  if (!trimmed) return value;
  try {
    return JSON.parse(trimmed);
  } catch {
    // Some model answers wrap JSON in markdown fences.
    const fenced = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/i);
    if (fenced?.[1]) {
      try {
        return JSON.parse(fenced[1].trim());
      } catch {
        /* fall through */
      }
    }
    // Discovery answers often append "## References" after the JSON blob.
    const balanced = extractBalancedJsonObject(trimmed);
    if (balanced) {
      try {
        return JSON.parse(balanced);
      } catch {
        return value;
      }
    }
    return value;
  }
}

/**
 * Parallel Task result shapes observed in production:
 * - contact recovery: output.content.output = JSON string with { candidates: [...] }
 * - discovery (core/pro): output.content.answer = JSON string with { candidates: [...] }
 * Never invent candidates — return [] when parse fails.
 */
export function parseParallelCandidates(result) {
  let output =
    result?.structured?.output ||
    result?.structured?.result ||
    result?.output ||
    result?.structured ||
    {};

  const content = output?.content;
  if (content != null && typeof content === "object") {
    if (content.output != null) output = content.output;
    else if (content.answer != null) output = content.answer;
    else if (content.candidates != null) output = content;
  } else if (typeof content === "string") {
    output = content;
  }

  output = tryParseJsonString(output);

  if (output && typeof output === "object" && !Array.isArray(output)) {
    if (typeof output.output === "string" || typeof output.answer === "string") {
      output = tryParseJsonString(output.output ?? output.answer);
    }
  }

  const list = Array.isArray(output?.candidates)
    ? output.candidates
    : Array.isArray(output)
      ? output
      : [];
  return list;
}

function extractBilledUsd(result, fallback) {
  const structured = result?.structured || {};
  const n = Number(
    structured.cost?.total ||
      structured.usage?.cost_usd ||
      structured.run?.cost ||
      NaN
  );
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

/**
 * Execute one Parallel task with job-store resume + budget reserve.
 */
export async function executePaidParallelTask(task, opts = {}) {
  const env = opts.env || process.env;
  const hotelId = text(task.hotelId);
  const hotelCfg = opts.hotelConfig || loadHotelConfig(hotelId) || { hotelId };
  const ledgerPath = opts.ledgerPath || authorizationLedgerPath(env);
  const processor = task.processor || "pro";
  const reserveUsd = Number(task.reserveUsd ?? 0.25);
  const maxUsd = Number(opts.maxUsd ?? AUTHORIZED_NEW_SPEND_USD);
  const timeoutMs = Number(opts.timeoutMs || env.GDI_PARALLEL_TIMEOUT_MS || 300000);
  const jobSegment = `paid-ms-v1-${task.taskId}`;
  const jobKey = createParallelJobKey(hotelId, jobSegment);

  if (isGdiParallelKillSwitchOn(env)) {
    return {
      taskId: task.taskId,
      status: "blocked",
      reason: "kill_switch",
      paidCalls: 0,
      estimatedUsd: 0,
      actualBilledUsd: 0,
    };
  }
  if (!isGdiParallelEnabled(env) || !isGdiParallelHotelAllowed(hotelId, env)) {
    return {
      taskId: task.taskId,
      status: "blocked",
      reason: "parallel_flag_or_hotel_not_allowed",
      paidCalls: 0,
      flags: getGdiParallelFlagState(env),
      estimatedUsd: 0,
      actualBilledUsd: 0,
    };
  }
  if (!resolveParallelKey(env)) {
    return {
      taskId: task.taskId,
      status: "blocked",
      reason: "parallel_key_missing",
      paidCalls: 0,
      estimatedUsd: 0,
      actualBilledUsd: 0,
    };
  }

  const resume = beginOrResumeParallelJob({
    jobKey,
    hotelId,
    label: `paid_multisegment:${task.taskId}`,
    env,
  });

  if (resume.shouldSubmit === false && resume.providerRunId) {
    try {
      const client = createParallelClient({ env });
      const result = await client.pollUntilComplete(resume.providerRunId, {
        timeoutMs,
      });
      const candidates = parseParallelCandidates(result);
      const billed = extractBilledUsd(result, reserveUsd);
      markParallelJobCompleted(
        jobKey,
        {
          candidateCount: candidates.length,
          paidCalls: 1,
          resumed: true,
          providerRunId: resume.providerRunId,
          taskId: task.taskId,
        },
        env
      );
      return {
        taskId: task.taskId,
        hotelId,
        segment: task.segment,
        kind: task.kind,
        status: "completed_resume",
        paidCalls: 1,
        providerRunId: resume.providerRunId,
        estimatedUsd: reserveUsd,
        actualBilledUsd: billed,
        candidates,
        resumed: true,
        note: "Resumed existing run_id — no duplicate submit",
      };
    } catch (err) {
      return {
        taskId: task.taskId,
        status: "incomplete",
        reason: err?.message || "resume_poll_failed",
        providerRunId: resume.providerRunId,
        paidCalls: 0,
        estimatedUsd: 0,
        actualBilledUsd: 0,
      };
    }
  }

  let reservation;
  try {
    reservation = reserveParallelBudget({
      label: `paid_ms:${task.taskId}`,
      processor,
      estimatedUsd: reserveUsd,
      jobKey,
      hotelId,
      maxUsd,
      env,
      ledgerPath,
    });
  } catch (err) {
    return {
      taskId: task.taskId,
      status: "blocked",
      reason: err?.code || err?.message || "budget_reserve_failed",
      paidCalls: 0,
      estimatedUsd: 0,
      actualBilledUsd: 0,
    };
  }

  const prompt = buildSegmentTaskPrompt(task, hotelCfg);
  let runId = null;
  try {
    const client = createParallelClient({ env });
    const created = await client.createTaskRun({
      processor,
      input: prompt,
    });
    runId =
      created.structured?.run_id ||
      created.structured?.id ||
      created.structured?.run?.id ||
      null;
    if (!runId) {
      releaseParallelBudgetReservation(reservation.reservationId, {
        env,
        ledgerPath,
        reason: "parallel_run_id_missing",
      });
      return {
        taskId: task.taskId,
        status: "incomplete",
        reason: "parallel_run_id_missing",
        paidCalls: 0,
        estimatedUsd: 0,
        actualBilledUsd: 0,
      };
    }
    markParallelJobSubmitted(
      jobKey,
      { providerRunId: runId, reservationId: reservation.reservationId },
      env
    );
    const result = await client.pollUntilComplete(runId, { timeoutMs });
    const candidates = parseParallelCandidates(result);
    const billed = extractBilledUsd(result, reserveUsd);
    commitParallelBudgetReservation(reservation.reservationId, {
      env,
      ledgerPath,
      providerRunId: runId,
      actualBilledUsd: billed,
    });
    markParallelJobCompleted(
      jobKey,
      {
        candidateCount: candidates.length,
        paidCalls: 1,
        providerRunId: runId,
        taskId: task.taskId,
        estimatedUsd: reserveUsd,
        actualBilledUsd: billed,
      },
      env
    );
    return {
      taskId: task.taskId,
      hotelId,
      segment: task.segment,
      kind: task.kind,
      status: "completed",
      paidCalls: 1,
      providerRunId: runId,
      estimatedUsd: reserveUsd,
      actualBilledUsd: billed,
      candidates,
      reservationId: reservation.reservationId,
    };
  } catch (err) {
    if (runId) {
      markParallelJobUncertain(jobKey, err?.message || "live_error", env);
    } else if (reservation?.reservationId) {
      releaseParallelBudgetReservation(reservation.reservationId, {
        env,
        ledgerPath,
        reason: err?.message || "failed_before_run_id",
      });
    }
    return {
      taskId: task.taskId,
      status: "incomplete",
      reason: err?.code || err?.message || "parallel_failed",
      providerRunId: runId,
      paidCalls: runId ? 1 : 0,
      estimatedUsd: runId ? reserveUsd : 0,
      actualBilledUsd: 0,
      note: "Do not resubmit if run_id known — reconcile job store first",
    };
  }
}

/**
 * Qualify raw Parallel candidates into segment dispositions.
 */
export function dispositionPaidCandidates(taskResult, opts = {}) {
  const hotelId = taskResult.hotelId;
  const hotelConfig = opts.hotelConfig || loadHotelConfig(hotelId) || { hotelId };
  const segment = taskResult.segment;
  const existingNames = new Set(
    (opts.existingOrganizationNames || []).map((n) => text(n).toLowerCase())
  );
  const rows = [];
  const list = Array.isArray(taskResult.candidates) ? taskResult.candidates : [];

  for (const raw of list) {
    const org =
      text(raw.organizationName) ||
      text(raw.venueName) ||
      text(raw.organization) ||
      "";
    const key = org.toLowerCase();
    if (!org) {
      rows.push({
        disposition: "rejected",
        reason: "missing_organization",
        raw,
      });
      continue;
    }

    // Contact recovery targets known orgs already on the board — do not treat
    // them as duplicate_existing; score usable route recovery instead.
    if (taskResult.kind === "contact_recovery") {
      const hasRoute = Boolean(
        text(raw.contactEmail) || text(raw.contactPhone) || text(raw.contactUrl)
      );
      rows.push({
        disposition: hasRoute ? "contact_resolved" : "contact_unresolved",
        organizationName: org,
        segment,
        hotelId,
        opportunityId: taskResult.knownOrg?.opportunityId || null,
        contact: {
          email: raw.contactEmail || null,
          phone: raw.contactPhone || null,
          url: raw.contactUrl || null,
          name: raw.contactName || null,
          role: raw.contactRole || null,
          sourceUrl: raw.contactSourceUrl || (raw.evidenceUrls || [])[0] || null,
        },
        raw,
      });
      continue;
    }

    if (existingNames.has(key)) {
      rows.push({
        disposition: "duplicate_existing",
        reason: "already_assessed_or_published",
        organizationName: org,
        raw,
      });
      continue;
    }

    if (segment === "scaling_companies" && taskResult.kind === "discovery") {
      const q = qualifyScalingCompanyCandidate(
        {
          organizationName: org,
          officialDomain: raw.officialDomain,
          localConnection: raw.localConnection,
          datedGrowthEvidence: raw.datedGrowthEvidence,
          growthEvidenceDate: raw.growthEvidenceDate,
          growthSignals: raw.growthSignals,
          operatingExpansionEvidence: raw.operatingExpansionEvidence,
          proposedHotelUse: raw.proposedHotelUse,
          hotelFitRationale: raw.hotelFitRationale,
          contactEmail: raw.contactEmail,
          contactPhone: raw.contactPhone,
          contactUrl: raw.contactUrl,
          sourceUrls: raw.sourceUrls,
        },
        hotelConfig
      );
      rows.push({
        disposition: q.ok
          ? text(raw.contactEmail || raw.contactPhone || raw.contactUrl)
            ? "qualified_publishable"
            : "internal_enrichment"
          : "rejected",
        qualification: q,
        organizationName: org,
        segment,
        hotelId,
        package: {
          hotelId,
          organizationName: org,
          officialDomain: raw.officialDomain,
          localConnection: raw.localConnection,
          datedGrowthEvidence: raw.datedGrowthEvidence,
          growthEvidenceDate: raw.growthEvidenceDate,
          growthSignals: raw.growthSignals,
          operatingExpansionEvidence: Boolean(raw.operatingExpansionEvidence),
          proposedHotelUse: raw.proposedHotelUse,
          hotelFitRationale: raw.hotelFitRationale,
          contactEmail: raw.contactEmail || null,
          contactPhone: raw.contactPhone || null,
          contactUrl: raw.contactUrl || null,
          contactRole: raw.contactRole || null,
          sourceUrl: (raw.sourceUrls || [])[0] || null,
          sourceUrls: raw.sourceUrls || [],
          recommendedAction: raw.recommendedFirstAction || null,
          demandUseCases: ["ROOMS_PLUS_MEETINGS", "MEETINGS_AND_CATERING"],
          publish: Boolean(
            q.ok && text(raw.contactEmail || raw.contactPhone || raw.contactUrl)
          ),
        },
      });
      continue;
    }

    if (segment === "local_meetings_catering") {
      const q = qualifyLocalMeetingsCateringCandidate(
        {
          organizationName: org,
          officialDomain: raw.officialDomain,
          localConnection: raw.localConnection,
          eventActivityEvidence: raw.eventActivityEvidence,
          functionType: raw.functionType,
          hotelFitRationale: raw.hotelFitRationale,
          whyThisMatters: raw.whyThisMatters,
          recommendedAction: raw.recommendedFirstAction || raw.recommendedAction,
          contactEmail: raw.contactEmail,
          contactPhone: raw.contactPhone,
          contactUrl: raw.contactUrl,
          guestroomStatus: raw.guestroomStatus || "NOT_REQUIRED",
          sourceUrls: raw.sourceUrls,
        },
        hotelConfig
      );
      const contactOk = Boolean(
        text(raw.contactEmail || raw.contactPhone || raw.contactUrl)
      );
      const softFailOnlyContact =
        !q.ok &&
        Array.isArray(q.failReasons) &&
        q.failReasons.length === 1 &&
        /MISSING_CONTACT/i.test(String(q.failReasons[0]));
      rows.push({
        disposition: q.ok
          ? "qualified_publishable"
          : softFailOnlyContact || (!q.ok && contactOk === false && q.failReasons?.every?.((f) => /CONTACT/i.test(String(f))))
            ? "internal_enrichment"
            : "rejected",
        qualification: q,
        organizationName: org,
        segment,
        hotelId,
        package: {
          hotelId,
          organizationName: org,
          officialDomain: raw.officialDomain,
          localConnection: raw.localConnection,
          eventActivityEvidence: raw.eventActivityEvidence,
          functionType: raw.functionType,
          recurrenceNote: raw.recurrenceNote,
          hotelFitRationale: raw.hotelFitRationale,
          whyThisMatters: raw.whyThisMatters,
          guestroomStatus: raw.guestroomStatus || "NOT_REQUIRED",
          contactEmail: raw.contactEmail || null,
          contactPhone: raw.contactPhone || null,
          contactUrl: raw.contactUrl || null,
          contactRole: raw.contactRole || null,
          sourceUrl: (raw.sourceUrls || [])[0] || null,
          sourceUrls: raw.sourceUrls || [],
          recommendedAction: raw.recommendedFirstAction || null,
          demandUseCases: ["MEETINGS_AND_CATERING"],
          publish: Boolean(
            q.ok && text(raw.contactEmail || raw.contactPhone || raw.contactUrl)
          ),
        },
      });
      continue;
    }

    if (segment === "weddings_social_private_events") {
      const hasVenue = Boolean(text(raw.venueName) || org);
      const hasLoc = Boolean(text(raw.location) || text(raw.localConnection));
      const hasActivity = Boolean(text(raw.eventActivityEvidence));
      const hasRationale = Boolean(text(raw.lodgingOrReferralRationale));
      const ok = hasVenue && hasLoc && hasActivity && hasRationale;
      rows.push({
        disposition: ok
          ? text(raw.contactEmail || raw.contactPhone || raw.contactUrl)
            ? "qualified_publishable"
            : "internal_enrichment"
          : "rejected",
        reason: ok
          ? null
          : "missing_venue_location_activity_or_referral_rationale",
        organizationName: org,
        venueName: raw.venueName || org,
        segment,
        hotelId,
        package: {
          hotelId,
          venueName: raw.venueName || org,
          organizationName: org,
          officialDomain: raw.officialDomain,
          location: raw.location,
          category: raw.category,
          eventActivityEvidence: raw.eventActivityEvidence,
          lodgingOrReferralRationale: raw.lodgingOrReferralRationale,
          contactEmail: raw.contactEmail || null,
          contactPhone: raw.contactPhone || null,
          contactUrl: raw.contactUrl || null,
          sourceUrls: raw.sourceUrls || [],
          whyRelevantToHotel: raw.whyRelevantToHotel,
          recommendedFirstAction: raw.recommendedFirstAction,
          publish: Boolean(
            ok && text(raw.contactEmail || raw.contactPhone || raw.contactUrl)
          ),
        },
      });
      continue;
    }

    rows.push({
      disposition: "rejected",
      reason: "unhandled_segment_or_kind",
      organizationName: org,
      segment,
      hotelId,
      raw,
    });
  }

  // Geographic quarantine for discovery rows with location fields
  if (taskResult.kind === "discovery" && rows.length) {
    const probe = rows.map((r) => ({
      ...r.package,
      organizationName: r.organizationName,
      location: r.package?.location || r.raw?.location,
    }));
    const geo = quarantineWrongMarketCandidates(probe, hotelConfig, {
      hotelId,
    });
    if (geo?.quarantinedCount > 0) {
      for (const q of geo.quarantined || []) {
        const name = text(q.organizationName || q.venueName).toLowerCase();
        const hit = rows.find(
          (r) => text(r.organizationName).toLowerCase() === name
        );
        if (hit) {
          hit.disposition = "quarantined_wrong_market";
          hit.geoReason =
            q.publicationBlockReason || q.geographicRelevance?.reason || null;
        }
      }
    }
  }

  return rows;
}

/**
 * Run the authorized paid pass for one or both hotels (Parallel tasks only).
 * Surfe is reserved separately and invoked only when named people exist.
 */
export async function runPaidMultisegmentResearchPass(opts = {}) {
  const env = opts.env || process.env;
  const allocation = opts.allocation || buildPaidMultisegmentAllocation(opts);
  const hotelFilter = opts.hotelIds
    ? new Set(opts.hotelIds.map((h) => text(h)))
    : null;
  const ledgerPath = opts.ledgerPath || authorizationLedgerPath(env);
  const dryRun = opts.dryRun === true;
  const existingByHotel = opts.existingOrganizationNamesByHotel || {};

  // Ensure authorization ledger exists with $11 cap (does not touch historical $2)
  readParallelBudgetLedger({
    ledgerPath,
    budgetUsd: allocation.authorizedNewSpendUsd,
    env: { ...env, GDI_PARALLEL_BUDGET_USD: String(allocation.authorizedNewSpendUsd) },
  });

  const results = [];
  let estimatedSpend = 0;
  let actualSpend = 0;
  const allDispositions = [];

  for (const task of allocation.tasks) {
    if (task.kind === "surfe_reserve") {
      results.push({
        taskId: task.taskId,
        status: "reserved_for_surfe",
        estimatedUsd: task.reserveUsd,
        actualBilledUsd: 0,
        note: "Surfe spend tracked when named-person enrichment is invoked",
      });
      continue;
    }
    if (hotelFilter && !hotelFilter.has(task.hotelId)) continue;

    const ledger = readParallelBudgetLedger({ ledgerPath, env });
    const remaining = remainingParallelBudgetUsd(ledger);
    if (remaining < Number(task.reserveUsd || 0)) {
      results.push({
        taskId: task.taskId,
        status: "skipped",
        reason: "budget_exhausted",
        remainingUsd: remaining,
        paidCalls: 0,
      });
      continue;
    }

    if (dryRun) {
      results.push({
        taskId: task.taskId,
        status: "dry_run_planned",
        estimatedUsd: task.reserveUsd,
        actualBilledUsd: 0,
        promptPreview: text(task.promptFocus).slice(0, 160),
      });
      continue;
    }

    const taskResult = await executePaidParallelTask(task, {
      env,
      ledgerPath,
      maxUsd: allocation.authorizedNewSpendUsd,
      hotelConfig: loadHotelConfig(task.hotelId),
      timeoutMs: opts.timeoutMs,
    });
    // attach knownOrg for contact recovery disposition
    if (task.knownOrg) taskResult.knownOrg = task.knownOrg;

    estimatedSpend += Number(taskResult.estimatedUsd || 0);
    actualSpend += Number(taskResult.actualBilledUsd || 0);

    let dispositions = [];
    if (
      taskResult.status === "completed" ||
      taskResult.status === "completed_resume"
    ) {
      dispositions = dispositionPaidCandidates(taskResult, {
        hotelConfig: loadHotelConfig(task.hotelId),
        existingOrganizationNames: existingByHotel[task.hotelId] || [],
      });
      allDispositions.push(...dispositions.map((d) => ({ ...d, taskId: task.taskId })));
    }

    results.push({ ...taskResult, dispositionCount: dispositions.length, dispositions });

    // Early stop for low-yield PE/scaling if completed with zero usable candidates
    if (
      task.kind === "discovery" &&
      taskResult.status === "completed" &&
      dispositions.filter((d) =>
        ["qualified_publishable", "internal_enrichment", "contact_resolved"].includes(
          d.disposition
        )
      ).length === 0 &&
      opts.stopOnZeroYield === true
    ) {
      results.push({
        note: `Low-yield stop after ${task.taskId} — remaining similar tasks may still run unless budget exhausted`,
      });
    }
  }

  return {
    version: PAID_MULTISEGMENT_RESEARCH_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    allocation,
    flags: getGdiParallelFlagState(env),
    ledgerPath,
    spend: {
      authorizedUsd: allocation.authorizedNewSpendUsd,
      estimatedUsd: Number(estimatedSpend.toFixed(4)),
      actualBilledUsd: Number(actualSpend.toFixed(4)),
      unusedAuthorizationUsd: Number(
        (allocation.authorizedNewSpendUsd - actualSpend).toFixed(4)
      ),
      historicalPilotUsdPreserved: HISTORICAL_PARALLEL_PILOT_USD,
      note: "actualBilledUsd uses provider-reported cost when present, else reserved estimate",
    },
    taskResults: results,
    dispositions: allDispositions,
  };
}

export function writePaidMultisegmentArtifacts(passResult, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "paid-multisegment-pass.json"),
    JSON.stringify(passResult, null, 2) + "\n"
  );
  fs.writeFileSync(
    path.join(outDir, "task-allocation.json"),
    JSON.stringify(passResult.allocation, null, 2) + "\n"
  );
  fs.writeFileSync(
    path.join(outDir, "budget-ledger-summary.json"),
    JSON.stringify(passResult.spend, null, 2) + "\n"
  );
  fs.writeFileSync(
    path.join(outDir, "candidate-dispositions.json"),
    JSON.stringify(
      { generatedAt: passResult.generatedAt, rows: passResult.dispositions },
      null,
      2
    ) + "\n"
  );

  // Hotel × segment briefs + research scorecard (instrument every run)
  const briefs = [];
  const tasks = passResult.taskResults || [];
  for (const t of tasks) {
    if (!t.hotelId || !t.segment || t.kind === "surfe_reserve") continue;
    const allocTask = (passResult.allocation?.tasks || []).find(
      (a) => a.taskId === t.taskId
    );
    const brief = buildHotelSegmentResearchBrief({
      hotelId: t.hotelId,
      segmentId: t.segment,
      specificGaps: allocTask?.stopCriteria
        ? [allocTask.stopCriteria]
        : [],
      costAllocationUsd: t.estimatedUsd ?? allocTask?.reserveUsd ?? null,
      effortBound: {
        maxPaidTasks: allocTask?.maxTasks || 1,
        stopCriteria: allocTask?.stopCriteria || null,
      },
    });
    briefs.push({
      taskId: t.taskId,
      promptFocus:
        allocTask?.promptFocus || briefToPromptFocus(brief),
      brief,
    });
  }
  fs.writeFileSync(
    path.join(outDir, "hotel-segment-research-briefs.json"),
    JSON.stringify(
      {
        version: "gdi-hotel-segment-research-brief-v1",
        generatedAt: passResult.generatedAt,
        briefs,
      },
      null,
      2
    ) + "\n"
  );

  const scorecards = tasks
    .filter((t) => t.kind !== "surfe_reserve")
    .map((t) => {
      const briefRow = briefs.find((b) => b.taskId === t.taskId);
      return scorecardFromPaidTaskResult(t, {
        runId: passResult.runId || null,
        authorizedUsd: passResult.spend?.authorizedUsd,
        remainingUsd: passResult.spend?.unusedAuthorizationUsd,
        objective: briefRow?.brief?.objective,
        declaredGeography: briefRow?.brief?.geographicCatchment?.label,
        briefVersion: briefRow?.brief?.briefVersion,
        promptFocus: briefRow?.promptFocus || null,
        dispositions: t.dispositions || [],
      });
    });
  writeScorecardArtifacts(
    {
      version: RESEARCH_SCORECARD_VERSION,
      generatedAt: passResult.generatedAt || new Date().toISOString(),
      source: "paid_multisegment_research_v1",
      spend: passResult.spend || null,
      tasks: scorecards,
    },
    path.join(outDir, "scorecard")
  );

  return outDir;
}

/**
 * Rebuild dispositions from saved Parallel result JSON (no resubmit / no new spend).
 * rawDir files are named `{taskId}.json` with the provider result payload.
 */
export function redisposePaidMultisegmentFromRawResults(opts = {}) {
  const allocation = opts.allocation || buildPaidMultisegmentAllocation(opts);
  const rawDir = opts.rawDir;
  const existingByHotel = opts.existingOrganizationNamesByHotel || {};
  const priorSpend = opts.priorSpend || null;
  const priorTaskResults = opts.priorTaskResults || [];
  const allDispositions = [];
  const results = [];

  for (const task of allocation.tasks) {
    if (task.kind === "surfe_reserve") {
      const prior =
        priorTaskResults.find((t) => t.taskId === task.taskId) || {
          taskId: task.taskId,
          status: "reserved_for_surfe",
          paidCalls: 0,
          estimatedUsd: task.reserveUsd,
          actualBilledUsd: 0,
        };
      results.push(prior);
      continue;
    }

    const rawPath = path.join(rawDir, `${task.taskId}.json`);
    const prior = priorTaskResults.find((t) => t.taskId === task.taskId) || {};
    if (!fs.existsSync(rawPath)) {
      results.push({
        ...prior,
        taskId: task.taskId,
        hotelId: task.hotelId,
        segment: task.segment,
        kind: task.kind,
        status: prior.status || "missing_raw",
        candidates: [],
        dispositions: [],
        dispositionCount: 0,
        note: "raw Parallel result file missing — no redispose",
      });
      continue;
    }

    const structured = JSON.parse(fs.readFileSync(rawPath, "utf8"));
    const candidates = parseParallelCandidates({ structured });
    const taskResult = {
      ...prior,
      taskId: task.taskId,
      hotelId: task.hotelId,
      segment: task.segment,
      kind: task.kind,
      status: prior.status || "completed",
      paidCalls: prior.paidCalls ?? 1,
      providerRunId: prior.providerRunId || null,
      estimatedUsd: prior.estimatedUsd ?? task.reserveUsd,
      actualBilledUsd: prior.actualBilledUsd ?? task.reserveUsd,
      candidates,
      knownOrg: task.knownOrg || null,
      redisposedFromRaw: true,
    };
    const hotelCfg = loadHotelConfig(task.hotelId) || { hotelId: task.hotelId };
    const dispositions = dispositionPaidCandidates(taskResult, {
      hotelConfig: hotelCfg,
      existingOrganizationNames: existingByHotel[task.hotelId] || [],
    }).map((d) => ({ ...d, taskId: task.taskId }));
    allDispositions.push(...dispositions);
    results.push({
      ...taskResult,
      dispositionCount: dispositions.length,
      dispositions,
    });
  }

  const estimatedUsd =
    priorSpend?.estimatedUsd ??
    results.reduce((s, t) => s + Number(t.estimatedUsd || 0), 0);
  const actualBilledUsd =
    priorSpend?.actualBilledUsd ??
    results.reduce((s, t) => s + Number(t.actualBilledUsd || 0), 0);

  return {
    version: PAID_MULTISEGMENT_RESEARCH_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: false,
    redisposed: true,
    allocation,
    ledgerPath: opts.ledgerPath || authorizationLedgerPath(opts.env || process.env),
    spend: {
      authorizedUsd: allocation.authorizedNewSpendUsd,
      estimatedUsd: Number(Number(estimatedUsd).toFixed(4)),
      actualBilledUsd: Number(Number(actualBilledUsd).toFixed(4)),
      unusedAuthorizationUsd: Number(
        (allocation.authorizedNewSpendUsd - Number(actualBilledUsd)).toFixed(4)
      ),
      historicalPilotUsdPreserved: HISTORICAL_PARALLEL_PILOT_USD,
      note:
        priorSpend?.note ||
        "Redisposed from saved Parallel results — no new provider submit",
    },
    taskResults: results,
    dispositions: allDispositions,
  };
}
