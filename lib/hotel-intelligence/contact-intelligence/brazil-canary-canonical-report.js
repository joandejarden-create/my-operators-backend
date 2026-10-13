/**
 * Canonical reconciliation for Brazil live canary review packages.
 * Preserves initial automated assessment + correction; emits one canonical final summary
 * that must agree with FINAL_REPORT machine fields.
 */
import fs from "node:fs";
import path from "node:path";

/**
 * @param {{
 *   initial_assessment?: object,
 *   correction?: object|null,
 *   phase2?: object|null,
 *   starting_spend?: number,
 *   ending_spend?: number,
 *   temporary_ceiling?: number,
 *   founder_hard_cap?: number,
 *   overall_verdict?: string,
 * }} args
 */
export function buildCanonicalCanaryAssessment(args = {}) {
  const initial = args.initial_assessment || {};
  const correction = args.correction || null;
  const phase2 = args.phase2 || null;

  const initialGate = initial.phase1_gate || initial;
  const correctedGate = correction
    ? {
        ...initialGate,
        ...correction,
        pass: correction.pass !== false,
        gate_reason: correction.gate_reason || initialGate.gate_reason,
      }
    : null;

  const phase1Pass = correctedGate
    ? Boolean(correctedGate.pass)
    : Boolean(initialGate.pass);
  const phase2Executed = Boolean(
    phase2?.executed ?? phase2?.phase2_executed ?? args.phase2_executed ?? false
  );
  const starting = Number(
    args.starting_spend ?? initial.starting_spend ?? initialGate.starting_spend ?? NaN
  );
  const ending = Number(
    args.ending_spend ??
      phase2?.ending_spend ??
      correction?.ending_spend ??
      initial.ending_spend ??
      NaN
  );
  const newCredits =
    Number.isFinite(starting) && Number.isFinite(ending) ? ending - starting : null;

  const overall =
    args.overall_verdict ||
    (phase1Pass && (!phase2Executed || phase2?.ok !== false)
      ? "LIVE_BRAZIL_LADDER_VALIDATED"
      : "PHASE1_OR_OPS_FAIL");

  return {
    version: "brazil-canary-canonical-assessment-v1",
    initial_assessment: {
      phase1_gate_pass: Boolean(initialGate.pass),
      phase1_gate_reason: initialGate.gate_reason || null,
      phase2_executed: Boolean(initial.phase2_executed),
      starting_spend: initial.starting_spend ?? null,
      ending_spend: initial.ending_spend ?? null,
      new_credits: initial.new_credits ?? null,
      note: "Preserved automated first-pass assessment (may disagree with correction).",
    },
    correction: correction
      ? {
          applied: true,
          phase1_gate_pass: Boolean(correctedGate.pass),
          phase1_gate_reason: correctedGate.gate_reason || null,
          correction_note: correction.correction_note || null,
          phase2_authorized: correction.phase2_authorized === true,
          spend_at_gate: correction.spend_at_gate ?? null,
        }
      : { applied: false },
    canonical_final_assessment: {
      phase1_gate_pass: phase1Pass,
      phase1_gate_reason:
        correctedGate?.gate_reason || initialGate.gate_reason || null,
      phase2_executed: phase2Executed,
      starting_spend: starting,
      ending_spend: ending,
      new_credits: newCredits,
      temporary_ceiling: args.temporary_ceiling ?? null,
      founder_hard_cap: args.founder_hard_cap ?? null,
      overall_verdict: overall,
    },
  };
}

/**
 * Fail packaging when canonical fields disagree with an expected FINAL_REPORT snapshot.
 * @param {object} canonicalPackage — output of buildCanonicalCanaryAssessment
 * @param {object} finalReportFields — authoritative expected fields
 */
export function assertCanonicalAgreesWithFinalReport(canonicalPackage, finalReportFields = {}) {
  const c = canonicalPackage?.canonical_final_assessment || {};
  const errors = [];
  const checks = [
    ["phase1_gate_pass", c.phase1_gate_pass, finalReportFields.phase1_gate_pass],
    ["phase2_executed", c.phase2_executed, finalReportFields.phase2_executed],
    ["starting_spend", c.starting_spend, finalReportFields.starting_spend],
    ["ending_spend", c.ending_spend, finalReportFields.ending_spend],
    ["new_credits", c.new_credits, finalReportFields.new_credits],
    ["overall_verdict", c.overall_verdict, finalReportFields.overall_verdict],
  ];
  for (const [key, actual, expected] of checks) {
    if (expected === undefined) continue;
    if (actual !== expected) {
      errors.push(`${key}: canonical=${JSON.stringify(actual)} final=${JSON.stringify(expected)}`);
    }
  }
  return { ok: errors.length === 0, errors };
}

/**
 * Read a canary OUT dir and write canonical-final-summary.json (does not delete history).
 * @param {string} outDir
 * @param {object} [overrides]
 */
export function reconcileCanaryPackageDir(outDir, overrides = {}) {
  const summaryPath = path.join(outDir, "canary-summary.json");
  const correctedPath = path.join(outDir, "phase1-gate-corrected.json");
  const phase2SummaryPath = path.join(outDir, "phase2", "phase2-summary.json");
  const accountingPath = path.join(outDir, "final-accounting.json");

  const initial = fs.existsSync(summaryPath)
    ? JSON.parse(fs.readFileSync(summaryPath, "utf8"))
    : {};
  const correction = fs.existsSync(correctedPath)
    ? JSON.parse(fs.readFileSync(correctedPath, "utf8"))
    : null;
  const phase2Summary = fs.existsSync(phase2SummaryPath)
    ? JSON.parse(fs.readFileSync(phase2SummaryPath, "utf8"))
    : null;
  const accounting = fs.existsSync(accountingPath)
    ? JSON.parse(fs.readFileSync(accountingPath, "utf8"))
    : {};

  const ending =
    overrides.ending_spend ??
    accounting.ledger_spent ??
    phase2Summary?.ending_spend ??
    initial.ending_spend;
  const starting = overrides.starting_spend ?? initial.starting_spend;
  const phase2Executed =
    overrides.phase2_executed ??
    (Boolean(phase2Summary) ||
      Boolean(correction?.phase2_authorized && ending > (correction.spend_at_gate ?? 0)));

  const pkg = buildCanonicalCanaryAssessment({
    initial_assessment: initial,
    correction,
    phase2: {
      executed: phase2Executed,
      ending_spend: ending,
      ok: true,
    },
    phase2_executed: phase2Executed,
    starting_spend: starting,
    ending_spend: ending,
    temporary_ceiling: overrides.temporary_ceiling ?? initial.temporary_ceiling,
    founder_hard_cap: overrides.founder_hard_cap ?? initial.founder_hard_cap,
    overall_verdict: overrides.overall_verdict,
  });

  const outPath = path.join(outDir, "canonical-final-summary.json");
  fs.writeFileSync(outPath, JSON.stringify(pkg, null, 2));
  return { path: outPath, package: pkg };
}
