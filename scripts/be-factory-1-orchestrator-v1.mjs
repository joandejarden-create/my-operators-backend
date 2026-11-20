#!/usr/bin/env node
/**
 * Brand Explorer Factory 1.0 — brand-agnostic orchestrator.
 *
 * Stages (restartable, per-brand isolation):
 *   select → audit → research-plan → candidate → dry-run-writeplan → product-qa →
 *   founder-review → (blocked) writeback → readback → publish-verify
 *
 * Usage:
 *   node scripts/be-factory-1-orchestrator-v1.mjs --brand-id recEJCTDj1zrsjPM6 --stage status
 *   node scripts/be-factory-1-orchestrator-v1.mjs --brand-id recEJCTDj1zrsjPM6 --stage audit
 *   node scripts/be-factory-1-orchestrator-v1.mjs --brand-id recEJCTDj1zrsjPM6 --stage candidate
 *   node scripts/be-factory-1-orchestrator-v1.mjs --brand-id recEJCTDj1zrsjPM6 --stage dry-run-writeplan
 *   node scripts/be-factory-1-orchestrator-v1.mjs --brand-id rec… --stage writeback --apply \
 *     --founder-token <BRAND_SPECIFIC_TOKEN>
 *
 * Never reuses Curio/Ascend approval tokens for a new brand.
 * Never mutates Curio/Ascend Presentation during another brand's run.
 */
import "../load-env.js";
import { spawnSync } from "child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";

const ROOT = process.cwd();
const REPORT_ROOT = join(ROOT, "reports/brand-explorer/factory-1-0");

const PROTECTED = new Set([
  "receQkxgjlezsc1xg", // Curio — do not mutate during next-brand runs
  "reclkgOzvAcBheUSo", // Ascend — do not mutate during next-brand runs
]);

const STAGES = [
  "status",
  "select",
  "audit",
  "candidate",
  "dry-run-writeplan",
  "product-qa",
  "founder-review",
  "writeback",
];

function parseArgs(argv) {
  const out = {
    brandId: "",
    stage: "status",
    apply: false,
    founderToken: "",
    allowProtectedMutation: false,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--apply") out.apply = true;
    else if (a === "--allow-protected-mutation") out.allowProtectedMutation = true;
    else if (a === "--brand-id" && argv[i + 1]) out.brandId = String(argv[++i]).trim();
    else if (a === "--stage" && argv[i + 1]) out.stage = String(argv[++i]).trim().toLowerCase();
    else if (a === "--founder-token" && argv[i + 1]) out.founderToken = String(argv[++i]);
  }
  return out;
}

function brandStatePath(brandId) {
  return join(REPORT_ROOT, "runs", brandId, "factory-state.json");
}

function loadState(brandId) {
  const p = brandStatePath(brandId);
  if (!existsSync(p)) {
    return {
      brandId,
      createdAt: new Date().toISOString(),
      stages: {},
      researchSpendUsd: 0,
      writebackStatus: "NOT_STARTED",
      candidateHash: null,
      assembledPreviewHash: null,
    };
  }
  return JSON.parse(readFileSync(p, "utf8"));
}

function saveState(state) {
  const dir = join(REPORT_ROOT, "runs", state.brandId);
  mkdirSync(dir, { recursive: true });
  state.updatedAt = new Date().toISOString();
  writeFileSync(brandStatePath(state.brandId), JSON.stringify(state, null, 2));
  return state;
}

function runNode(script, args = []) {
  const r = spawnSync(process.execPath, [script, ...args], {
    cwd: ROOT,
    encoding: "utf8",
    env: process.env,
    maxBuffer: 20 * 1024 * 1024,
  });
  if (r.stdout) process.stdout.write(r.stdout);
  if (r.stderr) process.stderr.write(r.stderr);
  return r.status || 0;
}

function assertNotProtected(brandId, stage, allow) {
  if (!PROTECTED.has(brandId)) return;
  if (stage === "writeback" && !allow) {
    throw new Error(
      `Refusing writeback for protected Curio/Ascend brand ${brandId} during Factory 1.0 next-brand runs. Pass --allow-protected-mutation only with explicit founder authorization.`
    );
  }
}

async function main() {
  const args = parseArgs(process.argv);
  mkdirSync(REPORT_ROOT, { recursive: true });

  if (!STAGES.includes(args.stage)) {
    console.error(`Unknown stage ${args.stage}. Allowed: ${STAGES.join(", ")}`);
    process.exit(2);
  }

  if (args.stage === "select") {
    const code = runNode("scripts/be-factory-1-next-brand-select-v1.mjs");
    process.exit(code);
  }

  if (!args.brandId) {
    console.error("Required: --brand-id <Airtable Brand Basics record id>");
    process.exit(2);
  }

  assertNotProtected(args.brandId, args.stage, args.allowProtectedMutation);
  const state = loadState(args.brandId);

  if (args.stage === "status") {
    console.log(JSON.stringify({ contract: "BE_FACTORY_1_0_ORCHESTRATOR_V1", state }, null, 2));
    return;
  }

  if (args.stage === "audit") {
    // Reuse live presentation fetch via dry-run writeback path without apply.
    const code = runNode("scripts/be-presentation-writeback-v1.mjs", [
      "--brand-id",
      args.brandId,
    ]);
    state.stages.audit = {
      at: new Date().toISOString(),
      ok: code === 0,
      note: "Live Presentation dry-run inventory via brand-agnostic writeback planner",
    };
    saveState(state);
    process.exit(code);
  }

  if (args.stage === "candidate") {
    if (args.brandId !== "recEJCTDj1zrsjPM6") {
      console.error(
        "Generic research→candidate pack for arbitrary brands is not yet registered. Autograph uses scripts/be-factory-1-autograph-candidate-v1.mjs. Add a brand pack or extend the gap-fill generator before running candidate for other IDs."
      );
      process.exit(3);
    }
    const code = runNode("scripts/be-factory-1-autograph-candidate-v1.mjs");
    if (code === 0) {
      const buildPath = join(REPORT_ROOT, "03-autograph-candidate-build.json");
      if (existsSync(buildPath)) {
        const build = JSON.parse(readFileSync(buildPath, "utf8"));
        state.candidateHash = build.candidateHash || null;
        state.assembledPreviewHash = build.assembledPreviewHash || null;
        state.researchSpendUsd = build.researchSpendUsd || 0;
        state.writebackStatus = build.writebackStatus || "AWAITING_FOUNDER_EXACT_CANDIDATE_APPROVAL";
      }
      state.stages.candidate = { at: new Date().toISOString(), ok: true };
      saveState(state);
    }
    process.exit(code);
  }

  if (args.stage === "dry-run-writeplan") {
    const code = runNode("scripts/be-presentation-writeback-v1.mjs", [
      "--brand-id",
      args.brandId,
    ]);
    state.stages.dryRunWriteplan = { at: new Date().toISOString(), ok: code === 0 };
    saveState(state);
    process.exit(code);
  }

  if (args.stage === "product-qa") {
    if (args.brandId !== "recEJCTDj1zrsjPM6") {
      console.error("product-qa verify script currently wired for Autograph; extend for next brands.");
      process.exit(3);
    }
    const code = runNode("scripts/be-factory-1-autograph-8080-verify-v1.mjs");
    state.stages.productQa = { at: new Date().toISOString(), ok: code === 0 };
    if (code === 0) {
      state.writebackStatus = "AWAITING_FOUNDER_EXACT_CANDIDATE_APPROVAL";
    }
    saveState(state);
    process.exit(code);
  }

  if (args.stage === "founder-review") {
    state.stages.founderReview = {
      at: new Date().toISOString(),
      ok: false,
      status: "AWAITING_FOUNDER_EXACT_CANDIDATE_APPROVAL",
      candidateHash: state.candidateHash,
      assembledPreviewHash: state.assembledPreviewHash,
      note: "Founder must approve this exact candidate hash before writeback. Do not reuse Curio/Ascend tokens.",
    };
    state.writebackStatus = "AWAITING_FOUNDER_EXACT_CANDIDATE_APPROVAL";
    saveState(state);
    console.log(JSON.stringify(state.stages.founderReview, null, 2));
    return;
  }

  if (args.stage === "writeback") {
    if (!args.apply) {
      console.error("writeback stage requires --apply and a brand-specific --founder-token");
      process.exit(2);
    }
    if (!args.founderToken) {
      console.error("Missing --founder-token (must be brand-specific; Curio/Ascend tokens are invalid here)");
      process.exit(2);
    }
    const code = runNode("scripts/be-presentation-writeback-v1.mjs", [
      "--brand-id",
      args.brandId,
      "--apply",
      "--founder-token",
      args.founderToken,
      "--authorize-activation",
    ]);
    state.stages.writeback = {
      at: new Date().toISOString(),
      ok: code === 0,
      applied: code === 0,
    };
    state.writebackStatus = code === 0 ? "APPLIED" : "FAILED";
    saveState(state);
    process.exit(code);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
