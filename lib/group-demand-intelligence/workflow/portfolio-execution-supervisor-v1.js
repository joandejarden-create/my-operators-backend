/**
 * Portfolio Full E2E execution supervisor — durable state, live status,
 * heartbeat, resume. Extends portfolio_us_marriott_five_hotel_e2e_v1;
 * does not create a competing pipeline.
 */

import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  PORTFOLIO_HOTELS,
  PORTFOLIO_AUTH_WAVE_ID,
  portfolioLedgerPath,
  resolvePortfolioAuthorization,
} from "./portfolio-us-marriott-five-hotel-auth-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
export const PORTFOLIO_REPORT_DIR = path.join(
  ROOT,
  "reports/gdi-adp-five-hotel-portfolio-e2e-v1"
);
export const SUPERVISOR_STATE_PATH = path.join(
  PORTFOLIO_REPORT_DIR,
  "PORTFOLIO_SUPERVISOR_STATE.json"
);
export const LIVE_STATUS_JSON = path.join(
  PORTFOLIO_REPORT_DIR,
  "LIVE_EXECUTION_STATUS.json"
);
export const LIVE_STATUS_MD = path.join(
  PORTFOLIO_REPORT_DIR,
  "LIVE_EXECUTION_STATUS.md"
);
export const EXECUTION_JOURNAL = path.join(
  PORTFOLIO_REPORT_DIR,
  "EXECUTION_JOURNAL.jsonl"
);

export const EXEC_STATUS = Object.freeze({
  QUEUED: "QUEUED",
  RUNNING_ACTIVE: "RUNNING_ACTIVE",
  WAITING_PROVIDER: "WAITING_PROVIDER",
  WAITING_RETRY: "WAITING_RETRY",
  BLOCKED_AUTHORIZATION: "BLOCKED_AUTHORIZATION",
  BLOCKED_EXTERNAL_DEPENDENCY: "BLOCKED_EXTERNAL_DEPENDENCY",
  BLOCKED_TECHNICAL: "BLOCKED_TECHNICAL",
  STALLED: "STALLED",
  RESUMABLE_INTERRUPTED: "RESUMABLE_INTERRUPTED",
  RESEARCH_COMPLETE_DELIVERY_BLOCKED: "RESEARCH_COMPLETE_DELIVERY_BLOCKED",
  COMPLETE: "COMPLETE",
  EXCLUDED: "EXCLUDED",
});

function now() {
  return new Date().toISOString();
}

function readJson(p, fallback = null) {
  if (!fs.existsSync(p)) return fallback;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return fallback;
  }
}

function writeJson(p, doc) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(doc, null, 2));
}

export function appendJournal(event) {
  fs.mkdirSync(PORTFOLIO_REPORT_DIR, { recursive: true });
  fs.appendFileSync(
    EXECUTION_JOURNAL,
    `${JSON.stringify({ at: now(), ...event })}\n`
  );
}

export function readLedgerSpend(hotel, product) {
  const p = portfolioLedgerPath(hotel, product);
  const doc = readJson(p, {});
  const estimated =
    Number(doc.estimatedSpentUsd ?? doc.spentUsd ?? doc.spent?.usd ?? 0) || 0;
  const actual =
    doc.actualBilledUsd != null ? Number(doc.actualBilledUsd) : null;
  const reserved = Number(doc.reservedUsd ?? 0) || 0;
  const budget =
    product === "ADP" ? hotel.adpBudgetUsd : hotel.gdiBudgetUsd;
  const spent = actual != null ? actual : estimated;
  return {
    path: p,
    estimatedSpentUsd: estimated,
    actualBilledUsd: actual,
    reservedUsd: reserved,
    spentUsd: spent,
    budgetUsd: budget,
    remainingUsd: Math.max(0, budget - spent - reserved),
    entries: doc.entries || [],
  };
}

function combinationKey(hotelId, product) {
  return `${hotelId}:${product}`;
}

function detectCharlotteAdpProgress() {
  const periodPath = path.join(
    ROOT,
    "data/ai-demand-positioning/runtime/adp_period_adp_charlotte_marriott_city_center_20261019071607_f9f0ee.json"
  );
  const manifestPath = path.join(
    ROOT,
    "data/ai-demand-positioning/published/adp_charlotte_marriott_city_center/manifest.json"
  );
  const period = readJson(periodPath);
  const manifest = readJson(manifestPath);
  if (!period) {
    return {
      stage: "adp_measurement",
      status: EXEC_STATUS.QUEUED,
      completedWork: "none",
      pendingWork: "Run ADP baseline measurement",
      observations: 0,
      certified: false,
      evidence: { periodPath, exists: false },
    };
  }
  const obs = Array.isArray(period.observations)
    ? period.observations.length
    : 0;
  const certified =
    period.certified === true ||
    String(period.certificationStatus || "").includes("CERTIF");
  const published = manifest?.publishStatus === "Live";
  if (certified && obs >= 252) {
    return {
      stage: published
        ? "adp_full_e2e_evaluate"
        : "adp_publication_prep",
      status: published
        ? EXEC_STATUS.RESEARCH_COMPLETE_DELIVERY_BLOCKED
        : EXEC_STATUS.RUNNING_ACTIVE,
      completedWork: `${obs}/252 observations; certified=${certified}; costEstimate=${JSON.stringify(period.costEstimate || null)}`,
      pendingWork: published
        ? "Production delivery verify + Full E2E COMPLETE gates; owner-facing promotion gated"
        : "Local PDF/share verify + Full E2E",
      observations: obs,
      certified,
      published,
      periodId: period.periodId,
      costEstimate: period.costEstimate || null,
      evidence: { periodPath, manifestPath },
    };
  }
  return {
    stage: "adp_measurement",
    status: EXEC_STATUS.WAITING_PROVIDER,
    completedWork: `${obs}/252 observations`,
    pendingWork: "Complete remaining observations + certify",
    observations: obs,
    certified,
    evidence: { periodPath },
  };
}

function detectGdiProgress(hotel) {
  const runsRoot = path.join(
    PORTFOLIO_REPORT_DIR,
    "gdi-runs",
    hotel.slug,
    "runs"
  );
  const spend = readLedgerSpend(hotel, "GDI");
  if (!fs.existsSync(runsRoot)) {
    return {
      stage: "gdi_paid_discovery",
      status: EXEC_STATUS.QUEUED,
      completedWork: "No workflow runs yet",
      pendingWork: "Launch GDI full-e2e with paid discovery under hotel ledger",
      latestRunId: null,
      spend,
      evidence: { runsRoot },
    };
  }
  const runs = fs
    .readdirSync(runsRoot)
    .filter((d) => d.startsWith("gdi_workflow_"))
    .sort();
  const latest = runs[runs.length - 1] || null;
  if (!latest) {
    return {
      stage: "gdi_paid_discovery",
      status: EXEC_STATUS.QUEUED,
      completedWork: "Empty runs dir",
      pendingWork: "Launch GDI paid workflow",
      latestRunId: null,
      spend,
    };
  }
  const runDir = path.join(runsRoot, latest);
  const ledger = readJson(path.join(runDir, "stage-completion-ledger.json"), {});
  const stages = ledger.stages || {};
  const paidDone = stages.paid_multisegment?.status === "completed";
  const e2e = readJson(path.join(runDir, "full_e2e/full-e2e-completion.json"));
  const overall = e2e?.overallStatus || ledger.overallStatus || ledger.status;
  const dispPath = path.join(
    runDir,
    "paid_multisegment/candidate-dispositions.json"
  );
  const disp = readJson(dispPath);
  const dispCount = Array.isArray(disp?.rows) ? disp.rows.length : 0;
  const qualified = Array.isArray(disp?.rows)
    ? disp.rows.filter((r) => r.disposition === "qualified_publishable").length
    : 0;

  if (overall === "COMPLETE") {
    return {
      stage: "full_e2e_complete",
      status: EXEC_STATUS.COMPLETE,
      completedWork: `Full E2E COMPLETE; run=${latest}`,
      pendingWork: "None",
      latestRunId: latest,
      spend,
      dispositions: dispCount,
      qualifiedPublishableResearchLayer: qualified,
      fullE2e: overall,
      evidence: { runDir, e2e: e2e?.overallStatus },
    };
  }

  if (paidDone) {
    return {
      stage: "gdi_maturation_eligibility_ready",
      status:
        overall === "FAILED" || overall === "PARTIAL" || overall === "BLOCKED"
          ? EXEC_STATUS.RESUMABLE_INTERRUPTED
          : EXEC_STATUS.RUNNING_ACTIVE,
      completedWork: `Paid MS completed; dispositions=${dispCount} (qualified_publishable research-layer=${qualified}); spend≈$${spend.spentUsd}`,
      pendingWork:
        "Segment supervision + opportunity maturation + sales packets + Eligibility/Ready; fix segment N/A rationales; do not re-spend Parallel discovery",
      latestRunId: latest,
      spend,
      dispositions: dispCount,
      qualifiedPublishableResearchLayer: qualified,
      fullE2e: overall || null,
      discoverGaps: stages.discover_gaps?.status || null,
      evidence: { runDir, e2eReasons: (e2e?.reasons || []).slice(0, 8) },
    };
  }

  if (spend.entries.some((e) => e.status === "spent" || e.status === "reserved")) {
    return {
      stage: "gdi_paid_discovery",
      status: EXEC_STATUS.WAITING_PROVIDER,
      completedWork: `Ledger entries=${spend.entries.length}; spent≈$${spend.spentUsd}`,
      pendingWork: "Finish/reconcile paid MS tasks; resume workflow",
      latestRunId: latest,
      spend,
      evidence: { runDir },
    };
  }

  return {
    stage: "gdi_paid_discovery",
    status: EXEC_STATUS.QUEUED,
    completedWork: `Latest run ${latest} without paid MS complete`,
    pendingWork: "Launch/resume paid GDI discovery",
    latestRunId: latest,
    spend,
    evidence: { runDir },
  };
}

function detectAdpProgress(hotel) {
  if (!hotel.adpAuthorized) {
    return {
      stage: "awaiting_paid_authorization",
      status: EXEC_STATUS.EXCLUDED,
      completedWork:
        "Hotel is ADP portfolio-eligible; paid remonitor not in this wave budget",
      pendingWork: "Authorize separate paid ADP measurement wave if needed",
      evidence: {
        reason: "adp_paid_budget_not_allocated_this_wave",
        portfolioEligible: true,
      },
    };
  }
  if (hotel.adpPropertyId === "adp_charlotte_marriott_city_center") {
    const c = detectCharlotteAdpProgress();
    return { ...c, spend: readLedgerSpend(hotel, "ADP") };
  }
  const resultPath = path.join(
    PORTFOLIO_REPORT_DIR,
    "adp-runs",
    hotel.slug,
    "baseline-result.json"
  );
  const result = readJson(resultPath);
  const spend = readLedgerSpend(hotel, "ADP");
  const periodGlob = path.join(
    ROOT,
    "data/ai-demand-positioning/runtime"
  );
  let periodHit = null;
  if (fs.existsSync(periodGlob)) {
    const hit = fs
      .readdirSync(periodGlob)
      .filter((f) => f.includes(hotel.adpPropertyId) && f.endsWith(".json"))
      .sort()
      .at(-1);
    if (hit) {
      const p = readJson(path.join(periodGlob, hit));
      periodHit = {
        file: hit,
        obs: Array.isArray(p?.observations) ? p.observations.length : 0,
        certified: p?.certified === true,
        status: p?.status,
        certificationStatus: p?.certificationStatus,
      };
    }
  }
  if (periodHit?.certified && periodHit.obs >= 252) {
    return {
      stage: "adp_full_e2e_evaluate",
      status: EXEC_STATUS.RESEARCH_COMPLETE_DELIVERY_BLOCKED,
      completedWork: `${periodHit.obs} obs certified (${periodHit.file})`,
      pendingWork: "Delivery verify + Full E2E COMPLETE (publication gated)",
      spend,
      evidence: { periodHit, resultPath },
    };
  }
  if (periodHit && periodHit.obs > 0 && periodHit.obs < 252) {
    return {
      stage: "adp_measurement",
      status: EXEC_STATUS.WAITING_PROVIDER,
      completedWork: `${periodHit.obs}/252 observations; status=${periodHit.status || "in_progress"}`,
      pendingWork: "Continue ADP measurement to 252 + certify (do not restart completed pairs)",
      spend,
      observations: periodHit.obs,
      certified: false,
      evidence: { periodHit },
    };
  }
  if (result?.ok || result?.status === "ok") {
    return {
      stage: "adp_certify_delivery",
      status: EXEC_STATUS.RUNNING_ACTIVE,
      completedWork: "baseline-result present",
      pendingWork: "Certify + Full E2E",
      spend,
      evidence: { resultPath },
    };
  }
  return {
    stage: "adp_measurement",
    status: EXEC_STATUS.QUEUED,
    completedWork: "No ADP period for this property in wave",
    pendingWork: "Execute ADP baseline under $15 ledger",
    spend,
    evidence: { resultPath, exists: fs.existsSync(resultPath) },
  };
}

/**
 * Build / refresh durable supervisor state from disk evidence.
 */
export function reconcilePortfolioSupervisorState({
  owner = `portfolio_supervisor_${process.pid}`,
  forceLease = false,
} = {}) {
  fs.mkdirSync(PORTFOLIO_REPORT_DIR, { recursive: true });
  const prior = readJson(SUPERVISOR_STATE_PATH, null);
  if (
    prior?.lease?.owner &&
    prior.lease.owner !== owner &&
    prior.lease.expiresAt &&
    new Date(prior.lease.expiresAt).getTime() > Date.now() &&
    !forceLease
  ) {
    return {
      ok: false,
      reason: "LEASE_HELD_BY_OTHER",
      lease: prior.lease,
      state: prior,
    };
  }

  const combinations = [];
  for (const hotel of PORTFOLIO_HOTELS) {
    for (const product of ["GDI", "ADP"]) {
      if (product === "ADP" && !hotel.adpAuthorized) {
        combinations.push({
          key: combinationKey(hotel.hotelId, product),
          hotelId: hotel.hotelId,
          displayName: hotel.displayName,
          slug: hotel.slug,
          product,
          executionStatus: EXEC_STATUS.EXCLUDED,
          currentStage: "awaiting_paid_authorization",
          stageStartedAt: null,
          lastMeaningfulProgressAt: prior?.combinations?.find(
            (c) => c.key === combinationKey(hotel.hotelId, product)
          )?.lastMeaningfulProgressAt || null,
          childProcessId: null,
          providerJobIds: [],
          completedWork:
            "ADP portfolio-eligible; paid remonitor not in this wave budget",
          pendingWork: "Authorize separate paid ADP measurement if needed",
          nextAction: "Skip paid ADP this wave",
          nextPollingAt: null,
          budget: { spentUsd: 0, reservedUsd: 0, remainingUsd: 0, budgetUsd: 0 },
          fullE2eResult: "N/A",
          blockingReason: "adp_paid_budget_not_allocated_this_wave",
          retryCount: 0,
          evidence: { portfolioEligible: true },
        });
        continue;
      }
      const authId =
        product === "ADP"
          ? hotel.adpAuthorizationId
          : hotel.gdiAuthorizationId;
      const resolved = resolvePortfolioAuthorization({
        hotelId: hotel.hotelId,
        product,
        authorizationRef: authId,
      });
      const detected =
        product === "GDI"
          ? detectGdiProgress(hotel)
          : detectAdpProgress(hotel);
      const prev = prior?.combinations?.find(
        (c) => c.key === combinationKey(hotel.hotelId, product)
      );
      const key = combinationKey(hotel.hotelId, product);
      const childMeta = prior?.activeChildPids?.[key];
      const childAlive = childMeta?.pid
        ? (() => {
            try {
              process.kill(childMeta.pid, 0);
              return true;
            } catch {
              return false;
            }
          })()
        : false;
      const progressChanged =
        prev?.completedWork !== detected.completedWork ||
        prev?.currentStage !== detected.stage;
      let executionStatus = !resolved.ok
        ? EXEC_STATUS.BLOCKED_AUTHORIZATION
        : detected.status;
      // Preserve live child ownership over stale disk "QUEUED" detection
      if (childAlive && executionStatus === EXEC_STATUS.QUEUED) {
        executionStatus = EXEC_STATUS.RUNNING_ACTIVE;
      }
      if (
        childAlive &&
        executionStatus === EXEC_STATUS.RESUMABLE_INTERRUPTED
      ) {
        executionStatus = EXEC_STATUS.RUNNING_ACTIVE;
      }
      combinations.push({
        key,
        hotelId: hotel.hotelId,
        displayName: hotel.displayName,
        slug: hotel.slug,
        product,
        authorizationId: authId,
        executionStatus,
        currentStage: detected.stage,
          stageStartedAt: prev?.stageStartedAt || now(),
        lastMeaningfulProgressAt: progressChanged
          ? now()
          : prev?.lastMeaningfulProgressAt || now(),
        childProcessId: childAlive
          ? childMeta.pid
          : prev?.childProcessId || null,
        providerJobIds: detected.providerJobIds || prev?.providerJobIds || [],
        completedWork: detected.completedWork,
        pendingWork: detected.pendingWork,
        nextAction: deriveNextAction(hotel, product, detected),
        nextPollingAt: null,
        budget: detected.spend
          ? {
              spentUsd: detected.spend.spentUsd,
              reservedUsd: detected.spend.reservedUsd,
              remainingUsd: detected.spend.remainingUsd,
              budgetUsd: detected.spend.budgetUsd,
              estimatedSpentUsd: detected.spend.estimatedSpentUsd,
              actualBilledUsd: detected.spend.actualBilledUsd,
            }
          : readLedgerSpend(hotel, product),
        fullE2eResult: detected.fullE2e || null,
        blockingReason: !resolved.ok
          ? resolved.reason
          : detected.status?.startsWith("BLOCKED")
            ? detected.pendingWork
            : null,
        retryCount: prev?.retryCount || 0,
        dispositions: detected.dispositions ?? null,
        qualifiedPublishableResearchLayer:
          detected.qualifiedPublishableResearchLayer ?? null,
        observations: detected.observations ?? null,
        certified: detected.certified ?? null,
        latestRunId: detected.latestRunId || null,
        evidence: detected.evidence || {},
      });
    }
  }

  const state = {
    version: "portfolio-execution-supervisor-v1",
    waveId: PORTFOLIO_AUTH_WAVE_ID,
    owner,
    lease: {
      owner,
      acquiredAt: now(),
      expiresAt: new Date(Date.now() + 30 * 60 * 1000).toISOString(),
    },
    heartbeatAt: now(),
    lastReconcileAt: now(),
    resumeCommand:
      "node scripts/run-portfolio-us-marriott-five-hotel-e2e-v1.mjs --supervise --resume",
    combinations,
    activeChildPids: prior?.activeChildPids || {},
  };
  writeJson(SUPERVISOR_STATE_PATH, state);
  writeLiveStatus(state);
  appendJournal({
    kind: "reconcile",
    owner,
    combinations: combinations.map((c) => ({
      key: c.key,
      status: c.executionStatus,
      stage: c.currentStage,
    })),
  });
  return { ok: true, state };
}

function deriveNextAction(hotel, product, detected) {
  if (detected.status === EXEC_STATUS.EXCLUDED) return "Skip (excluded)";
  if (detected.status === EXEC_STATUS.COMPLETE) return "Maintain verification";
  if (product === "GDI") {
    if (detected.stage === "gdi_paid_discovery" && detected.status === EXEC_STATUS.QUEUED) {
      return `Run GDI paid: --gdi-paid --hotel ${hotel.hotelId}`;
    }
    if (detected.stage === "gdi_maturation_eligibility_ready") {
      return `Resume GDI maturation/Ready for ${hotel.slug} (no re-paid MS); run segment supervisor + packet/eligibility`;
    }
    return detected.pendingWork;
  }
  if (detected.stage === "adp_measurement" && detected.status === EXEC_STATUS.QUEUED) {
    return `Run ADP baseline: --adp-apply --hotel ${hotel.hotelId}`;
  }
  if (detected.status === EXEC_STATUS.RESEARCH_COMPLETE_DELIVERY_BLOCKED) {
    return "Hold research spend; record delivery/publication blockers; run Full E2E evaluator honesty";
  }
  return detected.pendingWork;
}

export function writeLiveStatus(state) {
  const doc = {
    at: now(),
    waveId: state.waveId,
    owner: state.owner,
    heartbeatAt: state.heartbeatAt,
    lease: state.lease,
    resumeCommand: state.resumeCommand,
    matrix: state.combinations.map((c) => ({
      hotel: c.displayName,
      hotelId: c.hotelId,
      product: c.product,
      executionStatus: c.executionStatus,
      currentStage: c.currentStage,
      stageStart: c.stageStartedAt,
      lastMeaningfulProgress: c.lastMeaningfulProgressAt,
      childProcessId: c.childProcessId,
      providerJobIds: c.providerJobIds,
      completedWork: c.completedWork,
      pendingWork: c.pendingWork,
      nextAction: c.nextAction,
      nextPollingCheck: c.nextPollingAt,
      budget: c.budget,
      fullE2eResult: c.fullE2eResult,
      blockingReason: c.blockingReason,
    })),
    summary: {
      complete: state.combinations.filter(
        (c) => c.executionStatus === EXEC_STATUS.COMPLETE
      ).length,
      excluded: state.combinations.filter(
        (c) => c.executionStatus === EXEC_STATUS.EXCLUDED
      ).length,
      runnable: state.combinations.filter((c) =>
        [
          EXEC_STATUS.QUEUED,
          EXEC_STATUS.RESUMABLE_INTERRUPTED,
          EXEC_STATUS.RUNNING_ACTIVE,
        ].includes(c.executionStatus)
      ).length,
      waiting: state.combinations.filter((c) =>
        [
          EXEC_STATUS.WAITING_PROVIDER,
          EXEC_STATUS.WAITING_RETRY,
        ].includes(c.executionStatus)
      ).length,
      blocked: state.combinations.filter((c) =>
        String(c.executionStatus).startsWith("BLOCKED")
      ).length,
      deliveryBlocked: state.combinations.filter(
        (c) =>
          c.executionStatus ===
          EXEC_STATUS.RESEARCH_COMPLETE_DELIVERY_BLOCKED
      ).length,
    },
  };
  writeJson(LIVE_STATUS_JSON, doc);

  const lines = [
    `# LIVE EXECUTION STATUS`,
    ``,
    `**Wave:** \`${state.waveId}\`  `,
    `**Owner:** \`${state.owner}\`  `,
    `**Heartbeat:** ${state.heartbeatAt}  `,
    `**Resume:** \`${state.resumeCommand}\``,
    ``,
    `| Hotel | Product | Status | Stage | Last progress | Budget spent/remain | Full E2E | Next action |`,
    `|---|---|---|---|---|---|---|---|`,
    ...doc.matrix.map(
      (r) =>
        `| ${r.hotel} | ${r.product} | ${r.executionStatus} | ${r.currentStage} | ${r.lastMeaningfulProgress || "—"} | $${r.budget?.spentUsd ?? "?"}/$${r.budget?.remainingUsd ?? "?"} | ${r.fullE2eResult || "—"} | ${(r.nextAction || "").slice(0, 80)} |`
    ),
    ``,
    `## Summary`,
    ``,
    "```json",
    JSON.stringify(doc.summary, null, 2),
    "```",
    ``,
    `Heartbeat proves supervisor liveness — not research quality.`,
    ``,
  ];
  fs.writeFileSync(LIVE_STATUS_MD, lines.join("\n"));
  return doc;
}

export function selectNextRunnableCombination(state) {
  const activeKeys = new Set(Object.keys(state.activeChildPids || {}));
  const order = [
    EXEC_STATUS.QUEUED,
    EXEC_STATUS.RESUMABLE_INTERRUPTED,
    EXEC_STATUS.RUNNING_ACTIVE,
  ];
  // Hotel-by-hotel GDI focus: prove one hotel's research/qualification before
  // scheduling new broad GDI paid discovery for others. ADP may continue.
  const focusGdiHotelId = String(
    process.env.GDI_PORTFOLIO_FOCUS_HOTEL_ID ||
      state?.gdiFocusHotelId ||
      ""
  ).trim();
  const gdiPaidActive = Object.values(state.activeChildPids || {}).some(
    (c) => c?.kind === "gdi_paid"
  );

  const eligible = (c) =>
    !activeKeys.has(c.key) &&
    c.blockingReason !== "resume_already_spawned";

  // Prefer focused hotel GDI maturation/resume first.
  if (focusGdiHotelId) {
    for (const status of order) {
      const focused = state.combinations.find(
        (c) =>
          eligible(c) &&
          c.executionStatus === status &&
          c.product === "GDI" &&
          c.hotelId === focusGdiHotelId
      );
      if (focused) return focused;
    }
  }

  // Prefer work that does not already have a live child.
  // Do not select WAITING_PROVIDER here — that is polled, not re-dispatched.
  // While proving one hotel, do not start a second GDI paid discovery.
  for (const status of order) {
    const hit = state.combinations.find((c) => {
      if (!eligible(c) || c.executionStatus !== status) return false;
      if (
        c.product === "GDI" &&
        c.currentStage === "gdi_paid_discovery" &&
        (gdiPaidActive ||
          (focusGdiHotelId && c.hotelId !== focusGdiHotelId))
      ) {
        return false;
      }
      return true;
    });
    if (hit) return hit;
  }
  return null;
}

/**
 * Spawn a tracked child for GDI paid or ADP apply (non-blocking).
 */
export function spawnTrackedWork({
  state,
  combination,
  kind, // gdi_paid | adp_apply | gdi_resume
  args,
  env = {},
}) {
  const logDir = path.join(
    PORTFOLIO_REPORT_DIR,
    "supervisor-logs",
    combination.slug || combination.hotelId
  );
  fs.mkdirSync(logDir, { recursive: true });
  const logPath = path.join(logDir, `${kind}-${Date.now()}.log`);
  const out = fs.openSync(logPath, "a");
  const child = spawn(process.execPath, args, {
    cwd: ROOT,
    env: { ...process.env, ...env },
    stdio: ["ignore", out, out],
    detached: false,
  });
  combination.childProcessId = child.pid;
  combination.executionStatus = EXEC_STATUS.RUNNING_ACTIVE;
  combination.stageStartedAt = now();
  state.activeChildPids[combination.key] = {
    pid: child.pid,
    kind,
    logPath,
    startedAt: now(),
  };
  writeJson(SUPERVISOR_STATE_PATH, state);
  writeLiveStatus(state);
  appendJournal({
    kind: "spawn",
    combination: combination.key,
    work: kind,
    pid: child.pid,
    logPath,
    args,
  });
  child.on("exit", (code) => {
    appendJournal({
      kind: "child_exit",
      combination: combination.key,
      work: kind,
      pid: child.pid,
      exitCode: code,
    });
    delete state.activeChildPids[combination.key];
    combination.childProcessId = null;
    combination.lastMeaningfulProgressAt = now();
    writeJson(SUPERVISOR_STATE_PATH, state);
  });
  return { pid: child.pid, logPath, child };
}

export function processAlive(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export function detectStalls(state, { stallMs = 45 * 60 * 1000 } = {}) {
  const stalls = [];
  state.activeChildPids = state.activeChildPids || {};

  // Drop dead child PIDs so capacity is released for other hotels
  for (const [key, meta] of Object.entries(state.activeChildPids)) {
    if (meta?.pid && !processAlive(meta.pid)) {
      delete state.activeChildPids[key];
      const c = state.combinations.find((x) => x.key === key);
      if (c) {
        c.childProcessId = null;
        if (
          c.executionStatus === EXEC_STATUS.RUNNING_ACTIVE ||
          c.executionStatus === EXEC_STATUS.WAITING_PROVIDER
        ) {
          c.executionStatus = EXEC_STATUS.RESUMABLE_INTERRUPTED;
          c.blockingReason = "child_process_exited";
        }
      }
      stalls.push({ key, reason: "child_dead_cleared" });
    }
  }

  for (const c of state.combinations) {
    if (
      ![
        EXEC_STATUS.RUNNING_ACTIVE,
        EXEC_STATUS.WAITING_PROVIDER,
      ].includes(c.executionStatus)
    ) {
      continue;
    }
    const last = Date.parse(c.lastMeaningfulProgressAt || 0);
    const child = state.activeChildPids?.[c.key];
    const alive = child?.pid ? processAlive(child.pid) : false;
    if (Date.now() - last > stallMs && !alive) {
      c.executionStatus = EXEC_STATUS.STALLED;
      c.blockingReason = `no_progress_for_${Math.round(stallMs / 60000)}m`;
      stalls.push({ key: c.key, reason: "stale_progress" });
    }
  }
  if (stalls.length) {
    writeJson(SUPERVISOR_STATE_PATH, state);
    writeLiveStatus(state);
    appendJournal({ kind: "stall_detection", stalls });
  }
  return stalls;
}

export function heartbeat(state) {
  state.heartbeatAt = now();
  state.lease.expiresAt = new Date(Date.now() + 30 * 60 * 1000).toISOString();
  writeJson(SUPERVISOR_STATE_PATH, state);
  writeLiveStatus(state);
  appendJournal({
    kind: "heartbeat",
    owner: state.owner,
    activeChildren: Object.keys(state.activeChildPids || {}),
  });
  return state;
}

export function allTerminalOrBlocked(state) {
  return state.combinations.every((c) =>
    [
      EXEC_STATUS.COMPLETE,
      EXEC_STATUS.EXCLUDED,
      EXEC_STATUS.BLOCKED_AUTHORIZATION,
      EXEC_STATUS.BLOCKED_EXTERNAL_DEPENDENCY,
      EXEC_STATUS.BLOCKED_TECHNICAL,
      EXEC_STATUS.RESEARCH_COMPLETE_DELIVERY_BLOCKED,
    ].includes(c.executionStatus)
  );
}
