#!/usr/bin/env node
/**
 * Canonical Checkpoint A runner — Stage 3 WHO/HOW via live adapter.
 *
 * Intended authorized PowerShell path (no ad-hoc bypass):
 *   .\scripts\gdi\Invoke-GdiWhoHow.ps1 -Mode Live -AuthorizePaidResearch ...
 *
 * Default: DryRun / fail-closed. Live spends only when dual-auth + env + founder id.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  assertWhoHowLiveAuthorization,
  executeWhoHowTranche,
  reconcileWhoHowJobSpend,
} from "../../lib/group-demand-intelligence/research/exa-agent/gdi-exa-who-how-live-adapter-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "../..");

const EXPECTED_AUTH =
  "FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127";
const DEFAULT_MANIFEST = path.join(
  root,
  "reports/gdi-wyndham-who-how-v3-tranche-20261127/01-TRANCHE_MANIFEST.json",
);
const CHECKPOINT_A_CEILING = 5;

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i >= 0 && process.argv[i + 1]) return process.argv[i + 1];
  return fallback;
}
function flag(name) {
  return process.argv.includes(name);
}

function usage() {
  console.log(`Usage:
  node scripts/gdi/run-gdi-who-how-checkpoint-a-v1.mjs --hotel-id <rec...>
    [--mode DryRun|Live]
    [--authorization-id ${EXPECTED_AUTH}]
    [--hard-ceiling-usd 5]
    [--manifest <path>]
    [--authorize-paid-research]
    [--resume]
    [--preflight-only]

DryRun / --preflight-only never spend.
Live requires --authorize-paid-research, authorization-id, hard-ceiling-usd,
and GDI_EXA_AGENT_LIVE=1 for provider calls.
`);
}

function loadManifestItems(manifestPath, hotelId, opportunityId = null) {
  const raw = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  const opps = Array.isArray(raw.opportunities) ? raw.opportunities : [];
  let selected = opps;
  if (opportunityId) {
    selected = opps.filter((o) => o.opportunityId === opportunityId);
  }
  return selected.map((o) => ({
    opportunityId: o.opportunityId,
    dossier: {
      opportunity: {
        candidateId: o.opportunityId,
        opportunityId: o.opportunityId,
        hotelId,
        organizationName: o.organizationName,
        title: o.title,
        segmentId: o.segmentId,
        competitiveClass: o.competitiveClass,
        hotelFitRationale:
          o.hotelFitRationale ||
          "Existing Wyndham San Diego Bayside continuation — Stage 3 WHO/HOW only.",
      },
    },
  }));
}

async function main() {
  const hotelId = arg("--hotel-id");
  const mode = arg("--mode", "DryRun");
  const preflightOnly = flag("--preflight-only");
  if (!hotelId) {
    usage();
    process.exit(2);
  }

  const authorizationId = arg("--authorization-id", EXPECTED_AUTH);
  const hardCeilingUsd = Number(
    arg("--hard-ceiling-usd", String(CHECKPOINT_A_CEILING)),
  );
  const manifestPath = path.resolve(arg("--manifest", DEFAULT_MANIFEST));
  const outDir =
    arg("--out-dir") ||
    path.join(
      root,
      "reports/gdi-wyndham-final-commercial-continuation-prep-v1/checkpoint-a",
    );
  fs.mkdirSync(outDir, { recursive: true });

  if (!fs.existsSync(manifestPath)) {
    console.error(`Manifest missing: ${manifestPath}`);
    process.exit(2);
  }

  if (mode === "Live" && !flag("--authorize-paid-research") && !preflightOnly) {
    console.error("Live mode requires --authorize-paid-research");
    process.exit(2);
  }
  if (flag("--authorize-paid-research") && mode !== "Live" && !preflightOnly) {
    console.error("--authorize-paid-research requires --mode Live");
    process.exit(2);
  }
  if (!Number.isFinite(hardCeilingUsd) || hardCeilingUsd <= 0) {
    console.error("--hard-ceiling-usd must be a positive number");
    process.exit(2);
  }
  if (hardCeilingUsd - CHECKPOINT_A_CEILING > 1e-9) {
    console.error(
      `Checkpoint A hard ceiling cannot exceed USD ${CHECKPOINT_A_CEILING}`,
    );
    process.exit(2);
  }

  const items = loadManifestItems(
    manifestPath,
    hotelId,
    arg("--opportunity-id"),
  );
  if (!items.length) {
    console.error("No opportunities in manifest scope");
    process.exit(2);
  }

  const effectiveMode = preflightOnly ? "DryRun" : mode;

  // Path verification: authorization gate is always evaluated for Live shape.
  const authProbe = assertWhoHowLiveAuthorization({
    mode: mode === "Live" && !preflightOnly ? "Live" : "DryRun",
    authorizePaidResearch: flag("--authorize-paid-research"),
    authorizeExaAgentLive: flag("--authorize-paid-research"),
    authorizationId,
    hardCeilingUsd,
    spentUsd: 0,
  });

  if (preflightOnly || effectiveMode === "DryRun") {
    const dry = await executeWhoHowTranche(items, {
      mode: "DryRun",
      authorizationId,
      hardCeilingUsd,
      hotelId,
    });
    const report = {
      ok: true,
      checkpoint: "A_WHO_HOW",
      preflightOnly: preflightOnly || effectiveMode === "DryRun",
      paid: false,
      spentUsd: 0,
      hardCeilingUsd,
      authorizationId,
      authorizationStatus: "NOT_SPENDING",
      authProbe,
      hotelId,
      opportunityCount: items.length,
      opportunityIds: items.map((i) => i.opportunityId),
      resume: flag("--resume"),
      manifestPath,
      tranche: dry,
      pathWired: true,
      note:
        "Canonical adapter path exercised in DryRun. Live spend requires founder GO + GDI_EXA_AGENT_LIVE=1.",
    };
    const outFile = path.join(outDir, "CHECKPOINT_A_PREFLIGHT.json");
    fs.writeFileSync(outFile, `${JSON.stringify(report, null, 2)}\n`);
    console.log(JSON.stringify({ ok: true, outFile, ...report }, null, 2));
    process.exit(0);
  }

  // Live authorized path — adapter enforces dual-auth + ceiling + job reuse
  const reconBefore = reconcileWhoHowJobSpend({ authorizationId });
  const live = await executeWhoHowTranche(items, {
    mode: "Live",
    authorizePaidResearch: true,
    authorizeExaAgentLive: true,
    authorizationId,
    hardCeilingUsd,
    spentUsd: Number(reconBefore.totalUsd || 0),
    hotelId,
  });
  const reconAfter = reconcileWhoHowJobSpend({ authorizationId });
  const report = {
    ok: live.blocked == null || live.processed > 0,
    checkpoint: "A_WHO_HOW",
    paid: Number(live.spentUsd || 0) > 0,
    spentUsd: live.spentUsd,
    remainingUsd: live.remainingUsd,
    hardCeilingUsd,
    authorizationId,
    hotelId,
    opportunityCount: items.length,
    resume: flag("--resume"),
    authProbe,
    reconcileBefore: reconBefore,
    reconcileAfter: reconAfter,
    tranche: live,
    pathWired: true,
  };
  const outFile = path.join(outDir, "CHECKPOINT_A_LIVE_RESULT.json");
  fs.writeFileSync(outFile, `${JSON.stringify(report, null, 2)}\n`);
  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        outFile,
        spentUsd: report.spentUsd,
        remainingUsd: report.remainingUsd,
        blocked: live.blocked || null,
        processed: live.processed,
        pathWired: true,
      },
      null,
      2,
    ),
  );

  // Fail closed if Live was requested but adapter refused all spend without processing
  if (live.blocked && live.processed === 0 && Number(live.spentUsd || 0) === 0) {
    const reason = live.blocked.reason || "LIVE_BLOCKED";
    // Auth/env missing is a clean stop — not an ad-hoc bypass
    if (
      /GDI_EXA_AGENT_LIVE|MISSING_FOUNDER|AUTH_OR_BUDGET/i.test(
        String(reason),
      )
    ) {
      console.error(`Checkpoint A Live blocked: ${reason}`);
      process.exit(4);
    }
    process.exit(1);
  }
  process.exit(0);
}

main().catch((err) => {
  console.error(err?.stack || err);
  process.exit(1);
});
