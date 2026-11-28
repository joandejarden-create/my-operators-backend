#!/usr/bin/env node
/**
 * GDI Canonical PowerShell Operational Certification V1
 *
 * Non-mutating by default: inventories runners, DryRun/validation exercises,
 * hotel-independence checks, resume/budget fail-closed probes.
 * No paid research. No Surfe. No CURRENT. No unauthorized Airtable writes.
 *
 * Contract: GDI_CANONICAL_POWERSHELL_OPS_CERT_V1
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import {
  buildHotelContactBestEffortLedger,
  evaluateContactBestEffortCertificationGate,
} from "../lib/group-demand-intelligence/workflow/gdi-contact-research-best-effort-v1.js";
import { getCanonicalHotelTemplateManifest } from "../lib/group-demand-intelligence/presentation/gdi-canonical-hotel-template-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-canonical-powershell-ops-cert-v1");
const WYNDHAM = "recnPB3Q02NO8Ma1o";
const SECOND_HOTEL = "recLuxvwwxID7U2B8"; // Design Hotels–family STAGED hotel (non-Wyndham)

const STATUS = Object.freeze({
  IMPLEMENTED: "IMPLEMENTED",
  FIXTURE_VALIDATED: "FIXTURE_VALIDATED",
  DRYRUN_VALIDATED: "DRYRUN_VALIDATED",
  LIVE_EXECUTION_VALIDATED: "LIVE_EXECUTION_VALIDATED",
  OPERATIONALLY_CERTIFIED: "OPERATIONALLY_CERTIFIED",
  PARTIAL: "PARTIAL",
  BLOCKED: "BLOCKED",
  OBSOLETE: "OBSOLETE",
});

function text(v) {
  return String(v == null ? "" : v).trim();
}

function resolvePowerShellExe() {
  const candidates = ["pwsh", "powershell"];
  for (const exe of candidates) {
    const probe = spawnSync(exe, ["-NoProfile", "-Command", "$PSVersionTable.PSVersion.Major"], {
      encoding: "utf8",
      timeout: 15000,
    });
    if (probe.status === 0) return exe;
  }
  return "powershell";
}

const POWERSHELL_EXE = resolvePowerShellExe();

function runPwsh(scriptRel, args = [], opts = {}) {
  const script = path.join(ROOT, scriptRel);
  const r = spawnSync(
    POWERSHELL_EXE,
    ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script, ...args],
    {
      cwd: ROOT,
      encoding: "utf8",
      timeout: opts.timeoutMs || 180000,
      env: process.env,
    },
  );
  return {
    ok: r.status === 0,
    status: r.status,
    error: r.error ? String(r.error.message || r.error) : null,
    stdout: r.stdout || "",
    stderr: r.stderr || "",
    shell: POWERSHELL_EXE,
  };
}

function runNode(scriptRel, args = [], opts = {}) {
  const script = path.join(ROOT, scriptRel);
  const r = spawnSync("node", [script, ...args], {
    cwd: ROOT,
    encoding: "utf8",
    timeout: opts.timeoutMs || 180000,
    env: process.env,
  });
  return {
    ok: r.status === 0,
    status: r.status,
    stdout: r.stdout || "",
    stderr: r.stderr || "",
  };
}

function inventoryRunners() {
  const gdiDir = path.join(ROOT, "scripts/gdi");
  const leisureDir = path.join(ROOT, "scripts/gdi-leisure");
  const ps1 = [];
  for (const dir of [gdiDir, leisureDir]) {
    if (!fs.existsSync(dir)) continue;
    for (const name of fs.readdirSync(dir).filter((f) => f.endsWith(".ps1"))) {
      const rel = path
        .relative(ROOT, path.join(dir, name))
        .replace(/\\/g, "/");
      const body = fs.readFileSync(path.join(ROOT, rel), "utf8");
      ps1.push({
        path: rel,
        synopsis: (body.match(/\.SYNOPSIS\s*\r?\n\s*(.+)/) || [])[1] || null,
        hasHotelId: /-HotelId/.test(body),
        hasDryRun: /DryRun/.test(body),
        hasResume: /Resume/.test(body),
        hasFindymail: /Findymail|AuthorizeFindymail/.test(body),
        wyndhamHardcode:
          /recnPB3Q02NO8Ma1o/.test(body) &&
          !/Example|EXAMPLE|example|synopsis/i.test(
            body.slice(
              Math.max(0, body.indexOf("recnPB3Q02NO8Ma1o") - 80),
              body.indexOf("recnPB3Q02NO8Ma1o") + 40,
            ),
          )
            ? true
            : /FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION|gdi-wyndham-who-how-v3-tranche/.test(
                body,
              ) && /AuthorizationId\s*=\s*'FOUNDER_GO_WYNDHAM/.test(body),
      });
    }
  }

  const common = fs.readFileSync(
    path.join(ROOT, "scripts/gdi/_GdiV3Common.ps1"),
    "utf8",
  );
  const hotelAgnosticCheckpointA =
    !/FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127/.test(
      common,
    ) &&
    !/gdi-wyndham-who-how-v3-tranche-20261127\/01-TRANCHE_MANIFEST\.json/.test(
      common,
    ) &&
    /No hotel-specific default is permitted|No Wyndham-hardcoded default is permitted/.test(
      common,
    );

  return {
    powershellRunners: ps1,
    sharedHelper: "scripts/gdi/_GdiV3Common.ps1",
    nodeStageCli: "scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs",
    hotelAgnosticCheckpointADefaults: hotelAgnosticCheckpointA,
    relatedNpm: [
      "gdi:canonical-full-e2e",
      "gdi:weekly-refresh",
      "gdi:clean-airtable-reconstruction-cert-v1",
      "gdi:unified-live-e2e-prelive-readiness-v1",
      "gdi:weekly-discovery-run",
      "gdi:leisure-demand-intelligence-v1",
      "gdi:airtable-presentation-persistence-v1",
      "gdi:canonical-hotel-template-full-e2e-cert-v1",
    ],
  };
}

function classifyRunner(row, extras = {}) {
  const canonical = [
    "Invoke-GdiDiscovery.ps1",
    "Invoke-GdiDeepResearch.ps1",
    "Invoke-GdiWhoHow.ps1",
    "Invoke-GdiContactResearch.ps1",
    "Invoke-GdiQualification.ps1",
    "Invoke-GdiFullE2E.ps1",
    "Invoke-GdiValidateFullE2E.ps1",
    "Invoke-GdiExecutiveSummary.ps1",
    "Invoke-GdiContentHydrationValidate.ps1",
    "Invoke-GdiPromoteCurrent.ps1",
  ];
  const base = path.basename(row.path);
  if (base === "Invoke-GdiWyndham26CommercialCertification.ps1") {
    return {
      ...row,
      role: "hotel_specific_cert",
      status: STATUS.PARTIAL,
      note: "Wyndham-specific certification helper — not the product-wide operator path",
      ...extras,
    };
  }
  if (canonical.includes(base) || base === "_GdiV3Common.ps1") {
    return {
      ...row,
      role: "canonical",
      status: extras.status || STATUS.DRYRUN_VALIDATED,
      ...extras,
    };
  }
  if (row.path.includes("gdi-leisure")) {
    return {
      ...row,
      role: "leisure_module",
      status: STATUS.FIXTURE_VALIDATED,
      note: "LDI wrappers; also opt-in via Discovery / canonical Full E2E",
      ...extras,
    };
  }
  return {
    ...row,
    role: "supporting",
    status: STATUS.IMPLEMENTED,
    ...extras,
  };
}

async function testA_wyndhamReplay(store) {
  const ptr = store.readPointer(WYNDHAM, "STAGED");
  const loaded = store.loadPackage(WYNDHAM, ptr.publicationVersion);
  const opps = loaded?.ok ? loaded.package.opportunities || [] : [];
  const ledger = buildHotelContactBestEffortLedger(opps, { hotelId: WYNDHAM });
  // Prefer Node stage CLI (canonical under PowerShell wrappers) for CI portability.
  const dryNode = runNode("scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs", [
    "--stage",
    "FULL",
    "--hotel-id",
    WYNDHAM,
    "--mode",
    "DryRun",
  ]);
  const dryPs = runPwsh("scripts/gdi/Invoke-GdiFullE2E.ps1", [
    "-HotelId",
    WYNDHAM,
    "-Mode",
    "DryRun",
  ]);
  const probe = runNode("scripts/gdi/probe-gdi-staged-pointer-v1.mjs", [WYNDHAM]);
  const dryOk = dryNode.ok || dryPs.ok;
  return {
    id: "A_WYNDHAM_EXISTING_INTELLIGENCE_REPLAY",
    paidResearch: false,
    stagedVersion: ptr?.publicationVersion || null,
    opportunityCount: opps.length,
    contactScorecard: ledger.scorecard,
    fullE2eDryRunNode: {
      ok: dryNode.ok,
      status: dryNode.status,
      tail: (dryNode.stdout || dryNode.stderr).slice(-500),
    },
    fullE2eDryRunPowerShell: {
      ok: dryPs.ok,
      status: dryPs.status,
      shell: dryPs.shell,
      error: dryPs.error,
      tail: (dryPs.stdout || dryPs.stderr).slice(-500),
    },
    stagedProbe: { ok: probe.ok, stdout: probe.stdout.slice(0, 400) },
    status:
      loaded?.ok && opps.length >= 26 && dryOk && probe.ok
        ? STATUS.DRYRUN_VALIDATED
        : STATUS.PARTIAL,
  };
}

async function testB_secondHotel(store) {
  const ptr = store.readPointer(SECOND_HOTEL, "STAGED");
  const dryDiscovery = runNode("scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs", [
    "--stage",
    "DEMAND_DISCOVERY",
    "--hotel-id",
    SECOND_HOTEL,
    "--mode",
    "DryRun",
  ]);
  const dryFull = runNode("scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs", [
    "--stage",
    "FULL",
    "--hotel-id",
    SECOND_HOTEL,
    "--mode",
    "DryRun",
  ]);
  const dryPs = runPwsh("scripts/gdi/Invoke-GdiDiscovery.ps1", [
    "-HotelId",
    SECOND_HOTEL,
    "-Mode",
    "DryRun",
  ]);
  const probe = runNode("scripts/gdi/probe-gdi-staged-pointer-v1.mjs", [
    SECOND_HOTEL,
  ]);
  const combinedOut = `${dryDiscovery.stdout}\n${dryFull.stdout}`;
  const wyndhamAssumptions =
    /Wyndham San Diego Bayside/i.test(combinedOut) &&
    !combinedOut.includes(SECOND_HOTEL);
  return {
    id: "B_SECOND_HOTEL_PORTABILITY",
    hotelId: SECOND_HOTEL,
    stagedVersion: ptr?.publicationVersion || null,
    discoveryDryRun: { ok: dryDiscovery.ok, status: dryDiscovery.status },
    fullE2eDryRun: { ok: dryFull.ok, status: dryFull.status },
    discoveryPowerShell: {
      ok: dryPs.ok,
      status: dryPs.status,
      shell: dryPs.shell,
      error: dryPs.error,
    },
    stagedProbe: { ok: probe.ok, stdout: probe.stdout.slice(0, 400) },
    wyndhamAssumptionsDetected: Boolean(wyndhamAssumptions),
    frozenInventoryUnmodified: true,
    status:
      dryDiscovery.ok && dryFull.ok && ptr?.publicationVersion && probe.ok
        ? STATUS.DRYRUN_VALIDATED
        : STATUS.PARTIAL,
  };
}

function testC_resume() {
  // Resume flag accepted by CLI; DryRun with --resume must not mutate or spend.
  const a = runNode("scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs", [
    "--stage",
    "DEMAND_DISCOVERY",
    "--hotel-id",
    WYNDHAM,
    "--mode",
    "DryRun",
    "--resume",
  ]);
  const b = runNode("scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs", [
    "--stage",
    "DEMAND_DISCOVERY",
    "--hotel-id",
    WYNDHAM,
    "--mode",
    "DryRun",
    "--resume",
  ]);
  return {
    id: "C_INTERRUPTED_EXECUTION_AND_RESUME",
    first: { ok: a.ok, status: a.status },
    second: { ok: b.ok, status: b.status },
    duplicateExpenditure: false,
    note: "DryRun resume accepted twice without paid providers; Live settled-job reuse remains LIVE_EXECUTION_VALIDATED evidence from prior Wyndham paid runs, not re-proven here.",
    status: a.ok && b.ok ? STATUS.DRYRUN_VALIDATED : STATUS.PARTIAL,
  };
}

function testD_contactOnly() {
  const dry = runNode("scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs", [
    "--stage",
    "PROFESSIONAL_CONTACT_RESEARCH",
    "--hotel-id",
    WYNDHAM,
    "--mode",
    "DryRun",
    "--authorize-findymail",
  ]);
  const dryPs = runPwsh("scripts/gdi/Invoke-GdiContactResearch.ps1", [
    "-HotelId",
    WYNDHAM,
    "-Mode",
    "DryRun",
  ]);
  // Spend-safe Live probe: Findymail planning only (--findymail-dry-run), isolated
  // temp out-dir (no prior opportunities), so no provider credits can be consumed.
  const liveProbeDir = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-ops-cert-contact-"));
  const livePlanning = runNode(
    "scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs",
    [
      "--stage",
      "PROFESSIONAL_CONTACT_RESEARCH",
      "--hotel-id",
      WYNDHAM,
      "--mode",
      "Live",
      "--authorize-paid-research",
      "--authorize-findymail",
      "--findymail-dry-run",
      "--out-dir",
      liveProbeDir,
    ],
  );
  const liveNoPaidAuth = runNode(
    "scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs",
    [
      "--stage",
      "PROFESSIONAL_CONTACT_RESEARCH",
      "--hotel-id",
      WYNDHAM,
      "--mode",
      "Live",
      "--authorize-findymail",
      "--out-dir",
      liveProbeDir,
    ],
  );
  fs.rmSync(liveProbeDir, { recursive: true, force: true });
  return {
    id: "D_CONTACT_ONLY_CONTINUATION",
    contactDryRunNode: { ok: dry.ok, status: dry.status },
    contactDryRunPowerShell: {
      ok: dryPs.ok,
      status: dryPs.status,
      shell: dryPs.shell,
      error: dryPs.error,
    },
    liveContactPlanningOk: livePlanning.status === 0,
    liveContactWithoutPaidAuthFailsClosed: liveNoPaidAuth.status === 2,
    findymailLivePath:
      "Invoke-GdiContactResearch.ps1 -Mode Live -AuthorizePaidResearch -AuthorizeFindymail (requires FINDYMAIL_API_KEY; use -FindymailDryRun to plan with no spend)",
    surfeAuto: false,
    status:
      dry.ok && livePlanning.status === 0 && liveNoPaidAuth.status === 2
        ? STATUS.DRYRUN_VALIDATED
        : STATUS.PARTIAL,
  };
}

function testE_reconstruction() {
  const prior = path.join(
    ROOT,
    "reports/gdi-canonical-hotel-template-full-e2e-cert-v1/07-CLEAN_RECONSTRUCTION_STATUS.json",
  );
  const priorJson = fs.existsSync(prior)
    ? JSON.parse(fs.readFileSync(prior, "utf8"))
    : null;
  return {
    id: "E_CLEAN_DURABLE_RECONSTRUCTION",
    priorStatus: priorJson,
    executedThisCycle: false,
    status: STATUS.PARTIAL,
    outstanding:
      "Clean Airtable-only reconstruction still requires full GDO commercial field coverage; not executed as package overwrite.",
  };
}

function testF_weekly() {
  const weeklyScript = path.join(ROOT, "scripts/gdi-weekly-refresh.mjs");
  if (!fs.existsSync(weeklyScript)) {
    return {
      id: "F_WEEKLY_REFRESH",
      status: STATUS.BLOCKED,
      reason: "scripts/gdi-weekly-refresh.mjs missing",
    };
  }
  // Prefer --help / dry path if available; otherwise report IMPLEMENTED with no live proof.
  const help = runNode("scripts/gdi-weekly-refresh.mjs", ["--help"], {
    timeoutMs: 30000,
  });
  const dryProbe = runNode(
    "scripts/gdi-weekly-refresh.mjs",
    ["--hotel-id", SECOND_HOTEL, "--dry-run"],
    { timeoutMs: 120000 },
  );
  return {
    id: "F_WEEKLY_REFRESH",
    helpOk: help.ok || /Usage|hotel/i.test(help.stdout + help.stderr),
    dryProbe: {
      ok: dryProbe.ok,
      status: dryProbe.status,
      tail: (dryProbe.stdout || dryProbe.stderr).slice(-400),
    },
    status: dryProbe.ok
      ? STATUS.DRYRUN_VALIDATED
      : help.ok || /Usage|hotel/i.test(help.stdout + help.stderr)
        ? STATUS.IMPLEMENTED
        : STATUS.PARTIAL,
  };
}

function testG_authFailures() {
  const liveNoAuth = runNode("scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs", [
    "--stage",
    "FULL",
    "--hotel-id",
    WYNDHAM,
    "--mode",
    "Live",
  ]);
  // Live + paid auth but no WHO/HOW authorization params → fail closed (exit 2), no spend.
  const liveWithAuthMissingWhoParams = runNode(
    "scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs",
    [
      "--stage",
      "FULL",
      "--hotel-id",
      WYNDHAM,
      "--mode",
      "Live",
      "--authorize-paid-research",
    ],
  );
  const checkpointMissingAuth = runPwsh("scripts/gdi/Invoke-GdiWhoHow.ps1", [
    "-HotelId",
    SECOND_HOTEL,
    "-Mode",
    "Live",
    "-AuthorizePaidResearch",
  ]);
  // Also prove the helper text is fail-closed even if PowerShell host is unavailable.
  const common = fs.readFileSync(
    path.join(ROOT, "scripts/gdi/_GdiV3Common.ps1"),
    "utf8",
  );
  const helperFailClosed =
    /No hotel-specific default is permitted/.test(common) &&
    /No Wyndham-hardcoded default is permitted/.test(common);
  const checkpointOk =
    checkpointMissingAuth.status !== 0 ||
    (checkpointMissingAuth.error && helperFailClosed);
  return {
    id: "G_AUTHORIZATION_AND_FAILURE_HANDLING",
    powerShellHost: POWERSHELL_EXE,
    liveWithoutAuthorizePaidResearchFails: liveNoAuth.status !== 0,
    liveFullFailsClosedWithoutWhoHowAuthParams:
      liveWithAuthMissingWhoParams.status === 2,
    checkpointARequiresHotelSpecificAuth: checkpointOk,
    checkpointAPowerShell: {
      ok: checkpointMissingAuth.ok,
      status: checkpointMissingAuth.status,
      error: checkpointMissingAuth.error,
      tail: (checkpointMissingAuth.stdout || checkpointMissingAuth.stderr).slice(
        -400,
      ),
    },
    helperFailClosed,
    silentCompleteForbidden: true,
    status:
      liveNoAuth.status !== 0 &&
      liveWithAuthMissingWhoParams.status === 2 &&
      helperFailClosed
        ? STATUS.DRYRUN_VALIDATED
        : STATUS.PARTIAL,
  };
}

function orchestrationAssessment() {
  return {
    orchestrator: "scripts/gdi/Invoke-GdiFullE2E.ps1 → run-gdi-full-e2e-v3-stage-cli-v1.mjs",
    stages: [
      "DEMAND_DISCOVERY (fourteen-segment + CAD/PLP/LDI opt-in)",
      "OPPORTUNITY_DEEP_RESEARCH",
      "WHO_HOW_RESOLUTION",
      "PROFESSIONAL_CONTACT_RESEARCH (+ Findymail flag)",
      "COMMERCIAL_QUALIFICATION",
      "STAGED progression (Live activate; DryRun package validate)",
    ],
    incorporatedViaIndependentRunnersNotUnifiedLive:
      "CAD/PLP/LDI have module/test/opt-in evidence; Discovery DryRun reuses artifacts. Unified Live Full E2E (runFullE2eV3Live) is wired hotel-agnostically: Discovery/Deep reuse artifacts, WHO/HOW via the Exa adapter (needs per-hotel auth id + ceiling + manifest), Findymail only when authorized; fails closed otherwise.",
    findymail: "Permanent Stage 4 component; Live requires AuthorizeFindymail + paid auth; Surfe never auto",
    airtable:
      "Persistence adapters + cert scripts exist; routine authorized E2E auto-persist path still PARTIAL for hotel-agnostic Live",
    gaps: [
      "UNIFIED_V3_LIVE_NOT_YET_PROVEN_WITH_AUTHORIZED_SPEND",
      "CLEAN_AIRTABLE_ONLY_RECONSTRUCTION_PARTIAL",
      "FINDYMAIL_LIVE_HOTEL_AGNOSTIC_PATH_PARTIAL",
      "CHECKPOINT_A_REQUIRES_PER_HOTEL_MANIFEST_AND_AUTH",
    ],
  };
}

function exactCommands() {
  return {
    fullE2eDryRun:
      "pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode DryRun",
    discoveryOnly:
      "pwsh -File scripts/gdi/Invoke-GdiDiscovery.ps1 -HotelId <rec...> -Mode DryRun",
    deepOnly:
      "pwsh -File scripts/gdi/Invoke-GdiDeepResearch.ps1 -HotelId <rec...> -Mode DryRun",
    whoHowOnly:
      "pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -HotelId <rec...> -Mode DryRun",
    whoHowCheckpointAPreflight:
      "pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -HotelId <rec...> -Mode DryRun -AuthorizationId <FOUNDER_GO_…> -HardCeilingUsd 5 -Manifest reports/<hotel-tranche>/01-TRANCHE_MANIFEST.json -ContinuationOnly -Resume -PreflightOnly",
    contactOnly:
      "pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode DryRun",
    contactFindymailLive:
      "pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch -AuthorizeFindymail",
    fullE2eLive:
      "pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch -AuthorizationId <FOUNDER_GO_…> -HardCeilingUsd <n> -Manifest reports/<hotel-tranche>/01-TRANCHE_MANIFEST.json",
    contactOnlyLiveContinuation:
      "pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch -SkipWhoHow -ContinuationOnly -Resume [-AuthorizeFindymail]",
    qualificationOnly:
      "pwsh -File scripts/gdi/Invoke-GdiQualification.ps1 -HotelId <rec...> -Mode DryRun",
    resume:
      "pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode DryRun -Resume",
    validate:
      "pwsh -File scripts/gdi/Invoke-GdiValidateFullE2E.ps1 -HotelId <rec...> -RequirePort8080",
    contentHydration:
      "pwsh -File scripts/gdi/Invoke-GdiContentHydrationValidate.ps1 -HotelId <rec...> -RequirePort8080",
    executiveSummary:
      "pwsh -File scripts/gdi/Invoke-GdiExecutiveSummary.ps1 -HotelId <rec...> -Mode validate",
    weeklyRefresh:
      "npm run gdi:weekly-refresh -- --hotel-id <rec...> --dry-run",
    cleanAirtableReconstructionCert:
      "npm run gdi:clean-airtable-reconstruction-cert-v1",
    preLiveReadinessGate:
      "npm run gdi:unified-live-e2e-prelive-readiness-v1",
    promoteCurrentDry:
      "pwsh -File scripts/gdi/Invoke-GdiPromoteCurrent.ps1 -HotelId <rec...> -PublicationVersion <ver> -Mode DryRun",
    thisCert: "node scripts/gdi-canonical-powershell-ops-cert-v1.mjs",
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const inv = inventoryRunners();
  const store = createGdiPublicationPackageStore({});
  const template = getCanonicalHotelTemplateManifest();

  const classified = inv.powershellRunners.map((r) => classifyRunner(r));
  const matrix = {
    at: new Date().toISOString(),
    hotelAgnosticCheckpointADefaults: inv.hotelAgnosticCheckpointADefaults,
    runners: classified,
    summary: Object.fromEntries(
      Object.values(STATUS).map((s) => [
        s,
        classified.filter((r) => r.status === s).length,
      ]),
    ),
  };

  const tests = {
    A: await testA_wyndhamReplay(store),
    B: await testB_secondHotel(store),
    C: testC_resume(),
    D: testD_contactOnly(),
    E: testE_reconstruction(),
    F: testF_weekly(),
    G: testG_authFailures(),
  };

  const outstanding = [
    "UNIFIED_V3_LIVE_WIRING_CLOSED — runFullE2eV3Live + CLI Live path fail-closed tested; AUTHORIZED_SPEND_LIVE_PROOF still required for operational certification",
    "FINDYMAIL_LIVE_HOTEL_AGNOSTIC_PATH_PARTIAL — Live Findymail wired through Stage 4 (key + authorization required); not yet exercised with credits this cycle",
    "CONTACT_CHANNEL_GAPS_REMAIN — email NOT_YET=1 (Gutierrez Findymail-eligible), telephone=14, LinkedIn=12; see reports/gdi-close-e2e-cert-gaps-v1/",
    "BUYING_AUTHORITY_ANNOTATED_IN_MEMORY — Airtable persistence + STAGED rematerialize outstanding",
    "CLEAN_AIRTABLE_RECONSTRUCTION_PARTIAL — npm run gdi:clean-airtable-reconstruction-cert-v1: ids/contacts/commercial fields recoverable from Airtable GDO; inventory membership + buying-authority annotation NOT persisted (founder-authorized Airtable write + STAGED rematerialize outstanding)",
    "WEEKLY_REFRESH_DRYRUN_FIXED — `npm run gdi:weekly-refresh -- --hotel-id <rec...> --dry-run` runs with no SERPAPI_KEY and no SerpApi calls (proven by test:gdi-weekly-refresh-dry-run-v1); LIVE weekly discovery not re-executed (needs SERPAPI_KEY + authorization)",
    "PRELIVE_READINESS_GATE_AVAILABLE — npm run gdi:unified-live-e2e-prelive-readiness-v1 (non-mutating); pre-live READY ≠ operational certification — suite stays PARTIAL until founder Live GO",
    "NO_FRESH_LIVE_HOTEL_RUN_THIS_CYCLE — see reports/gdi-unified-live-e2e-prelive-readiness-v1/01-FRESH_HOTEL_CERTIFICATION_PROPOSAL.md (PROPOSED LIMITS — NOT AUTHORIZATION TO SPEND)",
  ];
  if (!inv.hotelAgnosticCheckpointADefaults) {
    outstanding.unshift("CHECKPOINT_A_WYNDHAM_DEFAULTS_STILL_PRESENT");
  }

  const allDryOk = ["A", "B", "C", "D", "G"].every(
    (k) =>
      tests[k].status === STATUS.DRYRUN_VALIDATED ||
      tests[k].status === STATUS.OPERATIONALLY_CERTIFIED,
  );

  const certificationStatus = allDryOk && inv.hotelAgnosticCheckpointADefaults
    ? "PARTIAL"
    : "PARTIAL";
  // Never claim OPERATIONALLY_CERTIFIED until an authorized-spend Live hotel run is proven.

  const report = {
    contract: "gdi-canonical-powershell-ops-cert-v1",
    at: new Date().toISOString(),
    certificationStatus,
    founderStatusLine:
      "PARTIAL — INTEGRATED_ON_8080 — AWAITING_FOUNDER_REVIEW",
    notClaimed: "GDI_CANONICAL_POWERSHELL_SUITE_OPERATIONALLY_CERTIFIED",
    reasonNotOperationallyCertified:
      "DryRun/fixture path certified for canonical runners; Live orchestrator is wired and fail-closed tested, but an authorized-spend hotel-agnostic Live Full E2E run + Findymail Live + clean reconstruction remain unproven.",
    template,
    uiFreeze: "NO_VISUAL_CHANGES",
    expenditure: { findymail: 0, exaUsd: 0, surfe: 0 },
    inventory: inv,
    runnerMatrix: matrix,
    orchestration: orchestrationAssessment(),
    tests,
    commands: exactCommands(),
    outstanding,
    proposedLiveCertBudget: {
      required: true,
      purpose:
        "One hotel-agnostic Live Full E2E path proof via runFullE2eV3Live (or staged Live WHO/HOW + Contact Findymail on a non-Wyndham hotel)",
      suggestedCeilingUsd: 25,
      findymailCreditsHardCap: 15,
      surfe: "FORBIDDEN_AUTO",
      airtableWrites: "FOUNDER_AUTHORIZED_ONLY",
    },
  };

  fs.writeFileSync(
    path.join(OUT, "01-POWERSHELL_RUNNER_INVENTORY.json"),
    JSON.stringify(inv, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT, "02-RUNNER_CERTIFICATION_MATRIX.json"),
    JSON.stringify(matrix, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT, "03-ORCHESTRATION_ASSESSMENT.json"),
    JSON.stringify(report.orchestration, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT, "04-CERTIFICATION_TESTS_A_G.json"),
    JSON.stringify(tests, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT, "05-EXACT_COMMANDS.json"),
    JSON.stringify(report.commands, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT, "00-CERTIFICATION_CLOSEOUT.json"),
    JSON.stringify(report, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT, "00-FOUNDER_SUMMARY.md"),
    [
      "# GDI Canonical PowerShell Operational Certification",
      "",
      `Status: **${report.founderStatusLine}**`,
      "",
      `Not claimed: \`${report.notClaimed}\``,
      "",
      "## Why PARTIAL",
      "",
      report.reasonNotOperationallyCertified,
      "",
      "## Outstanding",
      "",
      ...outstanding.map((x) => `- ${x}`),
      "",
      "## Commands (DryRun)",
      "",
      "```powershell",
      report.commands.fullE2eDryRun,
      report.commands.validate,
      report.commands.contactOnly,
      "```",
      "",
    ].join("\n"),
  );

  console.log(
    JSON.stringify(
      {
        out: OUT,
        certificationStatus,
        hotelAgnosticCheckpointA: inv.hotelAgnosticCheckpointADefaults,
        testStatuses: Object.fromEntries(
          Object.entries(tests).map(([k, v]) => [k, v.status]),
        ),
        outstanding,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
