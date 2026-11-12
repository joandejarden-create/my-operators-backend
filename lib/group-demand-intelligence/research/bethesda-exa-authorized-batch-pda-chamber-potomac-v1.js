/**
 * Bethesda authorized Exa batch — PDA + Chamber + Potomac Events.
 *
 * Hard ceilings: 3 paid Exa Agent calls, $12 USD aggregate.
 * Fail-closed spend guard. Research-stage merge only.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "./bethesda-historical-attestation-bag-v1.js";
import { applyHistoricalCommercialCategoryProjection } from "./bethesda-historical-commercial-category-map-v1.js";
import {
  persistResearchStageOpportunityBag,
  researchStageHotelDir,
  writeJsonAtomic,
} from "./gdi-research-stage-persistence-v1.js";
import {
  runExaAgentResearch,
  quoteExaAgentRun,
  EXA_AGENT_EFFORT,
} from "./exa-agent/exa-agent-provider-adapter-v1.js";

export const BETHESDA_EXA_AUTHORIZED_BATCH_V1 =
  "gdi-bethesda-exa-authorized-batch-pda-chamber-potomac-v1";

export const AUTHORIZATION_ID =
  "FOUNDER_APPROVE_BETHESDA_EXA_3CALL_12USD_PDA_CHAMBER_POTOMAC_20261112";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_DIR_REL =
  "reports/gdi-bethesda-exa-authorized-batch-pda-chamber-potomac-v1";
const FIXTURE_REL = "fixtures/gdi/bethesda-29-historical-reconstruction-v1.json";
const EXPECTED_STORE_SHA =
  "fd438549ff030ffc6f219469a526ec9b88ad8a704871fa01784d5650468226ad";

/** Exact allowlist — founder authorization 2026-11-12. */
export const AUTHORIZED_TARGETS = Object.freeze([
  {
    id: "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
    organizationName: "Parenteral Drug Association (PDA)",
    label: "PDA",
    objective:
      "Find who controls lodging/hotel sourcing for PDA TRI Bethesda training; whether Dec 2026+ courses create guestroom demand; Bethesda Marriott eligibility; named buyer/intermediary; listed hotels; practical next action.",
  },
  {
    id: "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
    organizationName: "Greater Bethesda Chamber of Commerce",
    label: "CHAMBER",
    objective:
      "Verify Clarissa Harris role; upcoming/recurring Chamber events that could use hotel catering/meeting space; venue selection patterns; Bethesda Marriott fit; practical outreach step. Do not treat calendar events as uncommitted Marriott demand.",
  },
  {
    id: "gdi_pep_live_c165c897d233",
    organizationName: "Potomac Meetings and Events (Potomac Events)",
    label: "POTOMAC",
    objective:
      "Verify Carrie Rice role; whether Potomac Events coordinates hotel meetings/guestrooms/banquets/referrals; defensible partnership mechanism for Bethesda Marriott; preferred-venue restrictions; practical partnership introduction. Do not invent an existing partnership.",
  },
]);

export const SPEND_CEILING = Object.freeze({
  maxPaidCalls: 3,
  maxUsd: 12,
  effort: EXA_AGENT_EFFORT.MEDIUM,
  reservedPerCallUsd: 0.1, // fixed medium quote
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function loadDotEnv({ root = ROOT } = {}) {
  const envPath = path.join(root, ".env");
  if (!fs.existsSync(envPath)) return { loaded: false };
  const raw = fs.readFileSync(envPath, "utf8");
  let n = 0;
  for (const line of raw.split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const eq = t.indexOf("=");
    if (eq < 1) continue;
    const key = t.slice(0, eq).trim();
    let val = t.slice(eq + 1).trim();
    if (
      (val.startsWith('"') && val.endsWith('"')) ||
      (val.startsWith("'") && val.endsWith("'"))
    ) {
      val = val.slice(1, -1);
    }
    if (process.env[key] == null || process.env[key] === "") {
      process.env[key] = val;
      n += 1;
    }
  }
  return { loaded: true, keysSet: n };
}

function loadStaged({ root = ROOT } = {}) {
  const p = path.join(
    researchStageHotelDir(BETHESDA_CANONICAL_HOTEL_ID, { root }),
    "reconstructed-opportunities-v1.json"
  );
  if (fs.existsSync(p)) return { path: p, doc: JSON.parse(fs.readFileSync(p, "utf8")) };
  const fixture = path.join(root, FIXTURE_REL);
  if (fs.existsSync(fixture)) {
    return { path: fixture, doc: JSON.parse(fs.readFileSync(fixture, "utf8")) };
  }
  return { path: null, doc: null };
}

export function validateAuthorizedTargets({ root = ROOT } = {}) {
  const { doc } = loadStaged({ root });
  const rows = doc?.opportunities || [];
  const byId = new Map(rows.map((r) => [r.id || r.opportunityId, r]));
  const issues = [];
  const resolved = [];

  for (const t of AUTHORIZED_TARGETS) {
    const row = byId.get(t.id);
    if (!row) {
      issues.push({ id: t.id, issue: "MISSING_FROM_RESEARCH_STAGE" });
      continue;
    }
    if (text(row.hotelId) !== BETHESDA_CANONICAL_HOTEL_ID) {
      issues.push({ id: t.id, issue: "HOTEL_MISMATCH", hotelId: row.hotelId });
      continue;
    }
    if (!BETHESDA_29_ATTESTED_IDS.includes(t.id)) {
      issues.push({ id: t.id, issue: "NOT_IN_29_ATTESTED_SET" });
      continue;
    }
    const org = text(row.organizationName);
    if (!org.toLowerCase().includes(t.organizationName.split(" ")[0].toLowerCase()) &&
        !org.toLowerCase().includes(t.label.toLowerCase() === "pda" ? "parenteral" : t.label.toLowerCase() === "chamber" ? "chamber" : "potomac")) {
      // soft org check
      if (t.label === "PDA" && !/parenteral|pda/i.test(org)) {
        issues.push({ id: t.id, issue: "ORG_MISMATCH", org });
        continue;
      }
      if (t.label === "CHAMBER" && !/chamber/i.test(org)) {
        issues.push({ id: t.id, issue: "ORG_MISMATCH", org });
        continue;
      }
      if (t.label === "POTOMAC" && !/potomac/i.test(org)) {
        issues.push({ id: t.id, issue: "ORG_MISMATCH", org });
        continue;
      }
    }
    resolved.push({
      ...t,
      row,
      organizationNameLive: org,
      maturityBefore: row.commercialMaturity || null,
      contactBefore:
        row.primaryContact?.name ||
        row.contactRecovery?.email ||
        row.contactRecovery?.raw ||
        null,
    });
  }

  return {
    ok: issues.length === 0 && resolved.length === 3,
    issues,
    resolved,
    allowlistIds: AUTHORIZED_TARGETS.map((t) => t.id),
  };
}

function buildAgentPayload({ target, row, effort, maxCostDollars }) {
  const query = [
    `You are researching ONE hotel sales opportunity for Bethesda Marriott (5151 Pooks Hill Rd, Bethesda MD; hotelId ${BETHESDA_CANONICAL_HOTEL_ID}).`,
    `Opportunity ID (immutable): ${target.id}`,
    `Organization: ${target.organizationName}`,
    `Research objective: ${target.objective}`,
    `Existing evidence summary: ${text(row.whatIsHappening).slice(0, 900)}`,
    `Existing hotel fit: ${text(row.hotelFitRationale || row.whyThisHotel).slice(0, 500)}`,
    `Existing contact: ${JSON.stringify(row.primaryContact || row.contactRecovery || null).slice(0, 400)}`,
    `Rules: Use only independently verifiable public sources. Do not fabricate people, emails, or room-night estimates.`,
    `Do not claim Bethesda Marriott has a booking or partnership unless evidenced.`,
    `Return structured JSON matching the schema.`,
  ].join("\n");

  return {
    query,
    effort,
    // Keep fixed medium effort — do not use auto/max (unbounded without ceiling).
    // Explicitly omit enrichment-style budgets beyond fixed effort.
    outputSchema: {
      type: "object",
      required: [
        "organizationName",
        "demandOrPartnershipFindings",
        "bethesdaMarriottFit",
        "namedContacts",
        "sourceUrls",
        "recommendedNextAction",
        "unresolvedGaps",
        "demandConfidence",
      ],
      properties: {
        organizationName: { type: "string" },
        demandOrPartnershipFindings: { type: "string" },
        bethesdaMarriottFit: { type: "string" },
        namedContacts: {
          type: "array",
          items: {
            type: "object",
            properties: {
              name: { type: "string" },
              title: { type: "string" },
              email: { type: "string" },
              relevance: { type: "string" },
              sourceUrl: { type: "string" },
            },
          },
        },
        listedCompetitorHotels: { type: "array", items: { type: "string" } },
        timingOrRecurringPattern: { type: "string" },
        sourceUrls: { type: "array", items: { type: "string" } },
        recommendedNextAction: { type: "string" },
        unresolvedGaps: { type: "array", items: { type: "string" } },
        demandConfidence: {
          type: "string",
          enum: [
            "CONFIRMED_DEMAND",
            "RELIABLE_INFERRED_DEMAND",
            "SPECULATIVE_DEMAND",
            "PARTNERSHIP_POTENTIAL_ONLY",
            "OVERFLOW_WATCH_ONLY",
          ],
        },
      },
      additionalProperties: false,
    },
  };
}

function createLedgerSkeleton() {
  return {
    version: BETHESDA_EXA_AUTHORIZED_BATCH_V1,
    authorizationId: AUTHORIZATION_ID,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    allowlistIds: AUTHORIZED_TARGETS.map((t) => t.id),
    ceilings: { ...SPEND_CEILING },
    spentUsd: 0,
    reservedUsd: 0,
    paidCallsIssued: 0,
    entries: [],
    createdAt: new Date().toISOString(),
    finishedAt: null,
    status: "OPEN",
  };
}

function canIssueNextCall(ledger, quoteMaxUsd) {
  if (ledger.paidCallsIssued >= SPEND_CEILING.maxPaidCalls) {
    return { ok: false, reason: "MAX_PAID_CALLS_REACHED" };
  }
  if (ledger.paidCallsIssued >= 3) {
    return { ok: false, reason: "HARD_THREE_CALL_CAP" };
  }
  const pending = Number(quoteMaxUsd) || 0;
  if (!Number.isFinite(pending) || pending <= 0) {
    return { ok: false, reason: "QUOTE_NOT_BOUNDED" };
  }
  if (ledger.spentUsd + pending > SPEND_CEILING.maxUsd) {
    return { ok: false, reason: "WOULD_EXCEED_12_USD_CEILING" };
  }
  return { ok: true };
}

function mergeExaIntoRow(row, target, run, structured) {
  const s = structured && typeof structured === "object" ? structured : {};
  const named = Array.isArray(s.namedContacts) ? s.namedContacts : [];
  const firstNamed = named.find((c) => text(c?.name) && text(c.name).split(" ").length >= 2);
  const findings = text(s.demandOrPartnershipFindings) || text(run.text).slice(0, 1200);
  const fit = text(s.bethesdaMarriottFit) || text(row.hotelFitRationale);
  const action =
    text(s.recommendedNextAction) ||
    text(row.recommendedAction) ||
    "Continue research — review Exa findings before outreach.";
  const confidence = text(s.demandConfidence) || "SPECULATIVE_DEMAND";

  let maturity = row.commercialMaturity || "RESEARCH_CONTINUATION";
  if (confidence === "OVERFLOW_WATCH_ONLY" || target.label === "POTOMAC" && confidence === "PARTNERSHIP_POTENTIAL_ONLY") {
    maturity =
      confidence === "OVERFLOW_WATCH_ONLY" ? "OVERFLOW_WATCH" : "PARTNERSHIP_WATCH";
  } else if (confidence === "PARTNERSHIP_POTENTIAL_ONLY" || target.label === "POTOMAC") {
    maturity = "PARTNERSHIP_WATCH";
  } else if (
    confidence === "CONFIRMED_DEMAND" ||
    confidence === "RELIABLE_INFERRED_DEMAND"
  ) {
    // Still not Sales Ready without named commercially relevant buyer + thesis.
    maturity = firstNamed ? "RESEARCH_CONTINUATION" : "RESEARCH_CONTINUATION";
  }

  if (target.label === "POTOMAC") maturity = "PARTNERSHIP_WATCH";

  const sources = [
    ...(Array.isArray(row.sources) ? row.sources : []),
    {
      label: "exa_authorized_batch_v1",
      runId: run.runId || null,
      authorizationId: AUTHORIZATION_ID,
      costUsd: Number(run.costDollars?.total) || null,
    },
    ...(Array.isArray(s.sourceUrls)
      ? s.sourceUrls.filter(Boolean).map((url) => ({ label: "exa_source", url }))
      : []),
  ];

  let next = {
    ...row,
    whatIsHappening: findings || row.whatIsHappening,
    summaryWhat: findings || row.summaryWhat,
    tileCustomerSummary: findings || row.tileCustomerSummary,
    hotelFitRationale: fit,
    whyThisHotel: fit,
    summaryWhyHotel: fit,
    hotelOpportunityThesis: fit,
    recommendedAction: action,
    nextBestAction: action,
    sources,
    demandConfidence: confidence,
    timingOrRecurringPattern: text(s.timingOrRecurringPattern) || null,
    listedCompetitorHotels: Array.isArray(s.listedCompetitorHotels)
      ? s.listedCompetitorHotels
      : null,
    exaAuthorizedBatch: {
      version: BETHESDA_EXA_AUTHORIZED_BATCH_V1,
      authorizationId: AUTHORIZATION_ID,
      runId: run.runId || null,
      executedAt: new Date().toISOString(),
      costUsd: Number(run.costDollars?.total) || null,
      effort: SPEND_CEILING.effort,
      structured: s,
    },
    paidResearch: true,
    salesReady: false,
    customerFacing: false,
    customerVisible: false,
    publicationApproved: false,
    publication: "NOT_AUTHORIZED",
  };

  if (firstNamed) {
    next.primaryContact = {
      name: text(firstNamed.name),
      title: text(firstNamed.title) || null,
      role: text(firstNamed.title) || null,
      email: text(firstNamed.email) || null,
      namedPerson: true,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      sourceUrl: text(firstNamed.sourceUrl) || null,
    };
    next.contactRecovery = {
      raw: text(firstNamed.email) || text(firstNamed.name),
      namedPerson: true,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      usableRoute: Boolean(text(firstNamed.email)),
      email: text(firstNamed.email) || null,
      note: "Named contact from authorized Exa batch — verify before outreach.",
    };
  }

  next = applyHistoricalCommercialCategoryProjection(next);
  next.commercialMaturity = maturity;
  next.commercialStagePill =
    maturity === "PARTNERSHIP_WATCH"
      ? "Partnership Watch"
      : maturity === "OVERFLOW_WATCH"
        ? "Overflow Watch"
        : "Research Continuation";
  next.priorityPillLabel = next.commercialStagePill;
  next.unresolvedResearchGaps = Array.isArray(s.unresolvedGaps)
    ? s.unresolvedGaps
    : row.unresolvedResearchGaps || [];
  next.missingResearchRequirements = [
    ...new Set([
      ...(next.unresolvedResearchGaps || []),
      "CUSTOMER_PUBLICATION_NOT_AUTHORIZED",
      ...(next.salesReady ? [] : ["REFUSED_AUTO_SALES_READY_FROM_EXA_BATCH"]),
    ]),
  ];

  return next;
}

/**
 * Execute the founder-authorized Exa batch (max 3 calls / $12).
 */
export async function executeBethesdaExaAuthorizedBatch({
  root = ROOT,
  dryRun = false,
  authorizeExaAgentLive = true,
} = {}) {
  loadDotEnv({ root });
  process.env.GDI_EXA_AGENT_LIVE = "1";

  const preflight = validateAuthorizedTargets({ root });
  const outDir = path.join(root, REPORT_DIR_REL);
  fs.mkdirSync(outDir, { recursive: true });

  const ledger = createLedgerSkeleton();
  ledger.preflight = {
    ok: preflight.ok,
    issues: preflight.issues,
    allowlistIds: preflight.allowlistIds,
    resolved: preflight.resolved.map((r) => ({
      id: r.id,
      organizationName: r.organizationNameLive,
      label: r.label,
      maturityBefore: r.maturityBefore,
      contactBefore: r.contactBefore,
    })),
  };

  writeJsonAtomic(path.join(outDir, "00-preflight-and-budget-manifest.json"), {
    authorizationId: AUTHORIZATION_ID,
    exactOpportunityIds: preflight.allowlistIds,
    ceilings: SPEND_CEILING,
    preflight: ledger.preflight,
    dryRun,
  });

  if (!preflight.ok) {
    ledger.status = "STOPPED_PREFLIGHT_FAILED";
    ledger.finishedAt = new Date().toISOString();
    writeJsonAtomic(path.join(outDir, "01-authorization-ledger.json"), ledger);
    return { ok: false, stopped: "PREFLIGHT_FAILED", ledger, preflight };
  }

  if (!process.env.EXA_API_KEY && !process.env.EXA_KEY) {
    ledger.status = "STOPPED_EXA_API_KEY_ABSENT";
    ledger.finishedAt = new Date().toISOString();
    writeJsonAtomic(path.join(outDir, "01-authorization-ledger.json"), ledger);
    return { ok: false, stopped: "EXA_API_KEY_ABSENT", ledger };
  }

  const quote = quoteExaAgentRun({ effort: SPEND_CEILING.effort });
  if (!quote.bounded || Number(quote.maxUsd) > 4) {
    ledger.status = "STOPPED_QUOTE_UNBOUNDED_OR_TOO_HIGH";
    ledger.finishedAt = new Date().toISOString();
    writeJsonAtomic(path.join(outDir, "01-authorization-ledger.json"), ledger);
    return { ok: false, stopped: "QUOTE_BLOCKER", quote, ledger };
  }

  const { doc: stagedDoc, path: stagedPath } = loadStaged({ root });
  const byId = new Map(
    (stagedDoc.opportunities || []).map((r) => [r.id || r.opportunityId, r])
  );
  const updated = [];
  const scorecards = [];

  for (const target of preflight.resolved) {
    const gate = canIssueNextCall(ledger, quote.maxUsd);
    if (!gate.ok) {
      ledger.entries.push({
        opportunityId: target.id,
        status: "SKIPPED",
        reason: gate.reason,
        at: new Date().toISOString(),
      });
      break;
    }

    if (text(target.id) !== AUTHORIZED_TARGETS.find((t) => t.id === target.id)?.id) {
      ledger.entries.push({
        opportunityId: target.id,
        status: "SKIPPED",
        reason: "NOT_ON_ALLOWLIST",
      });
      continue;
    }

    const row = byId.get(target.id);
    const payload = buildAgentPayload({
      target,
      row,
      effort: SPEND_CEILING.effort,
      maxCostDollars: null,
    });

    const entryBase = {
      opportunityId: target.id,
      organizationName: target.organizationNameLive,
      label: target.label,
      objective: target.objective,
      providerOperation: "exa_agent_run",
      effort: SPEND_CEILING.effort,
      quoteMaxUsd: quote.maxUsd,
      requestTimestamp: new Date().toISOString(),
    };

    if (dryRun) {
      ledger.entries.push({
        ...entryBase,
        status: "DRY_RUN_NOT_EXECUTED",
        billableCalls: 0,
        billedCostUsd: 0,
        estimatedCostUsd: quote.maxUsd,
      });
      continue;
    }

    // Persist ledger checkpoint BEFORE paid call.
    ledger.paidCallsIssued += 1;
    ledger.reservedUsd = Number(
      (ledger.reservedUsd + Number(quote.maxUsd)).toFixed(6)
    );
    writeJsonAtomic(path.join(outDir, "01-authorization-ledger.json"), {
      ...ledger,
      status: "IN_FLIGHT",
      lastCheckpoint: entryBase,
    });

    const run = await runExaAgentResearch({
      mode: "deep_research",
      effort: SPEND_CEILING.effort,
      authorizeExaAgentLive: authorizeExaAgentLive === true,
      createPayloadOverride: payload,
      contractIdOverride: BETHESDA_EXA_AUTHORIZED_BATCH_V1,
      pollUntilFinished: true,
      pollIntervalMs: 4000,
      maxPolls: 90,
    });

    const billed = Number(run.costDollars?.total);
    const costForLedger = Number.isFinite(billed)
      ? billed
      : run.ok
        ? Number(quote.maxUsd)
        : Number(run.costDollars?.total) || 0;

    ledger.spentUsd = Number((ledger.spentUsd + (Number.isFinite(billed) ? billed : costForLedger)).toFixed(6));

    const entry = {
      ...entryBase,
      status: run.ok ? "COMPLETED" : `FAILED_${run.status}`,
      runId: run.runId || null,
      billableCalls: 1,
      billedCostUsd: Number.isFinite(billed) ? billed : null,
      estimatedOrPendingCostUsd: Number.isFinite(billed) ? null : costForLedger,
      aggregateSpentUsd: ledger.spentUsd,
      remainingUnder12: Number((SPEND_CEILING.maxUsd - ledger.spentUsd).toFixed(6)),
      evidenceReturned: Boolean(run.structured || run.text),
      structured: run.structured || null,
      textPreview: text(run.text).slice(0, 500) || null,
      error: run.error || run.reason || null,
    };
    ledger.entries.push(entry);
    writeJsonAtomic(path.join(outDir, "01-authorization-ledger.json"), ledger);

    if (run.ok) {
      const next = mergeExaIntoRow(row, target, run, run.structured);
      byId.set(target.id, next);
      updated.push(next);
      scorecards.push({
        id: target.id,
        organizationName: target.organizationNameLive,
        beforeMaturity: target.maturityBefore,
        afterMaturity: next.commercialMaturity,
        beforeContact: target.contactBefore,
        afterContact: next.primaryContact?.name || next.contactRecovery?.email || null,
        demandConfidence: next.demandConfidence,
        salesReady: false,
        category: next.opportunityTypeLabel,
        runId: run.runId,
        billedCostUsd: entry.billedCostUsd,
      });
    } else {
      scorecards.push({
        id: target.id,
        organizationName: target.organizationNameLive,
        beforeMaturity: target.maturityBefore,
        afterMaturity: target.maturityBefore,
        failed: true,
        status: run.status,
        salesReady: false,
      });
    }

    // Hard stop if ceiling reached after call.
    if (ledger.spentUsd >= SPEND_CEILING.maxUsd) break;
    if (ledger.paidCallsIssued >= SPEND_CEILING.maxPaidCalls) break;
  }

  // Persist research-stage merge for successful updates.
  let persist = null;
  if (!dryRun && updated.length) {
    const allRows = (stagedDoc.opportunities || []).map((r) => {
      const id = r.id || r.opportunityId;
      return byId.get(id) || r;
    });
    persist = persistResearchStageOpportunityBag({
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      targetPath: path.join(
        researchStageHotelDir(BETHESDA_CANONICAL_HOTEL_ID, { root }),
        "reconstructed-opportunities-v1.json"
      ),
      incomingDoc: {
        version: BETHESDA_EXA_AUTHORIZED_BATCH_V1,
        hotelId: BETHESDA_CANONICAL_HOTEL_ID,
        hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
        authorizationId: AUTHORIZATION_ID,
        generatedAt: new Date().toISOString(),
        safety: {
          paidResearch: true,
          paidResearchAuthorized: true,
          authorizationId: AUTHORIZATION_ID,
          maxUsd: SPEND_CEILING.maxUsd,
          maxPaidCalls: SPEND_CEILING.maxPaidCalls,
          opportunitiesJsonWrites: false,
          customerFacing: false,
          salesReadyForced: false,
        },
        opportunities: allRows,
      },
      protectedIds: [...BETHESDA_29_ATTESTED_IDS],
      root,
      label: "bethesda-exa-authorized-batch",
      mode: "merge",
    });

    // Refresh fixture with updated three rows only (full bag merge).
    const fixturePath = path.join(root, FIXTURE_REL);
    if (fs.existsSync(fixturePath)) {
      const prev = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
      writeJsonAtomic(fixturePath, {
        ...prev,
        version: BETHESDA_EXA_AUTHORIZED_BATCH_V1,
        authorizationId: AUTHORIZATION_ID,
        generatedAt: new Date().toISOString(),
        opportunities: allRows,
      });
    }
  }

  const oppPath = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    BETHESDA_CANONICAL_HOTEL_ID,
    "opportunities.json"
  );
  const storeSha = fs.existsSync(oppPath)
    ? crypto.createHash("sha256").update(fs.readFileSync(oppPath)).digest("hex")
    : null;

  ledger.finishedAt = new Date().toISOString();
  ledger.status = dryRun
    ? "DRY_RUN_COMPLETE"
    : updated.length
      ? "COMPLETED"
      : "COMPLETED_WITH_FAILURES";
  ledger.scorecards = scorecards;
  ledger.opportunityStore = {
    sha256: storeSha,
    expectedSha256: EXPECTED_STORE_SHA,
    untouched: storeSha === EXPECTED_STORE_SHA,
  };

  writeJsonAtomic(path.join(outDir, "01-authorization-ledger.json"), ledger);
  writeJsonAtomic(path.join(outDir, "02-scorecards.json"), { scorecards });
  writeJsonAtomic(path.join(outDir, "03-execution-summary.json"), {
    authorizationId: AUTHORIZATION_ID,
    exactOpportunityIds: preflight.allowlistIds,
    paidCallsIssued: ledger.paidCallsIssued,
    spentUsd: ledger.spentUsd,
    remainingUsd: Number((SPEND_CEILING.maxUsd - ledger.spentUsd).toFixed(6)),
    updatedCount: updated.length,
    persistOk: persist?.ok === true,
    storeUntouched: storeSha === EXPECTED_STORE_SHA,
    salesReadyPromotions: 0,
  });

  return {
    ok: true,
    ledger,
    scorecards,
    updated,
    persist,
    preflight,
    outDir,
  };
}
