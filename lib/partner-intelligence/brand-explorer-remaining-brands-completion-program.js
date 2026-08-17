/**
 * Remaining Brand Explorer brands — controlled two-lane completion program.
 *
 * Lane 1: fullyReady built-blocked → verify + founder packets + visibility formalization
 *         (public restore apply only with founder flags; no content rebuild)
 * Lane 2: true incomplete → full Tab Factory build (draft dry-run by default)
 *
 * Never modifies the public-full clean baseline. Never writes Company Validated,
 * Source Library status, or Registry approval/status.
 */
import { spawnSync } from "child_process";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BUILT_BLOCKED_TARGETS,
  BUILT_BLOCKED_PROTECTED_PUBLIC_FULL,
  BUILT_BLOCKED_TRUE_INCOMPLETE,
} from "./brand-explorer-built-blocked-content.js";
import {
  ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS,
  REQUIRED_APPLY_FLAGS as PUBLIC_RESTORE_FLAGS,
  resolvePublicRestoreBrands,
  planPublicRestoreGovernance,
  applyPublicRestoreGovernance,
  writePublicRestoreGovernanceReports,
} from "./brand-explorer-public-restore-governance.js";
import {
  FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
  FULL_BUILD_SLUG_ALIASES,
  resolveFullBuildSlug,
  planFullTabFactoryBuild,
  applyFullTabFactoryBuild,
  writeFullBuildBrandReport,
  FULL_BUILD_REQUIRED_APPLY_FLAGS,
} from "./brand-explorer-full-tab-factory-build.js";
import { runLane2PostDraftIntegrity } from "./brand-explorer-lane2-post-draft-integrity.js";
import { runLane2ImageAssetPack } from "./brand-explorer-lane2-image-asset-pack.js";
import { runLane2ImageMaterialization } from "./brand-explorer-lane2-image-materialization.js";

export const PROGRAM_VERSION = "remaining-brands-completion-program-v1";
export const REPORT_JSON = "brand-explorer-remaining-brands-completion-program.json";
export const REPORT_MD = "brand-explorer-remaining-brands-completion-program.md";
export const LANE1_MD = "brand-explorer-remaining-lane-1-restore.md";
export const LANE2_MD = "brand-explorer-remaining-lane-2-full-build.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

export const LANE1_DEFAULT = Object.freeze([...BUILT_BLOCKED_TARGETS]);
export const LANE2_DEFAULT = Object.freeze(
  FULL_BUILD_TRUE_INCOMPLETE_SLUGS?.length
    ? [...FULL_BUILD_TRUE_INCOMPLETE_SLUGS]
    : [...BUILT_BLOCKED_TRUE_INCOMPLETE]
);

export const LANES = Object.freeze({
  all: "all",
  restore: "fullyReady-restore",
  "fullyReady-restore": "fullyReady-restore",
  "full-tab-factory-build": "full-tab-factory-build",
  build: "full-tab-factory-build",
  "post-draft-integrity": "post-draft-integrity",
  "image-asset-pack": "image-asset-pack",
  "image-materialization": "image-materialization",
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function runNpm(script, args = [], { cwd = ROOT, timeoutMs = 600_000 } = {}) {
  const result = spawnSync("npm", ["run", script, "--", ...args], {
    cwd,
    encoding: "utf8",
    shell: true,
    timeout: timeoutMs,
    env: process.env,
  });
  return {
    script,
    args,
    status: result.status,
    ok: result.status === 0,
    stdout: result.stdout || "",
    stderr: result.stderr || "",
    error: result.error?.message || null,
  };
}

const LANE2_IMAGE_LANES = new Set([
  "post-draft-integrity",
  "image-asset-pack",
  "image-materialization",
  "full-tab-factory-build",
]);

export function resolveProgramBrands(rawList, { lane } = {}) {
  if (!rawList?.length) {
    if (lane === "fullyReady-restore") return [...LANE1_DEFAULT];
    if (LANE2_IMAGE_LANES.has(lane)) return [...LANE2_DEFAULT];
    return [...LANE1_DEFAULT, ...LANE2_DEFAULT];
  }
  return [
    ...new Set(
      rawList.map((raw) => {
        const key = nz(raw).toLowerCase();
        if (FULL_BUILD_SLUG_ALIASES?.[key]) return FULL_BUILD_SLUG_ALIASES[key];
        try {
          return resolveFullBuildSlug?.(key) || resolvePublicRestoreBrands([key])[0] || key;
        } catch {
          return resolvePublicRestoreBrands([key])[0] || key;
        }
      })
    ),
  ];
}

export function parseProgramArgs(argv = []) {
  const brandsIdx = argv.indexOf("--brands");
  const laneIdx = argv.indexOf("--lane");
  const rawBrands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : null;
  const laneRaw = laneIdx >= 0 && argv[laneIdx + 1] ? nz(argv[laneIdx + 1]) : "all";
  const lane = LANES[laneRaw] || (laneRaw === "all" ? "all" : null);
  if (!lane) {
    throw new Error(
      `Unknown --lane ${laneRaw}. Use all | fullyReady-restore | full-tab-factory-build | post-draft-integrity | image-asset-pack | image-materialization`
    );
  }
  return {
    brands: resolveProgramBrands(rawBrands, { lane }),
    lane,
    apply: argv.includes("--apply"),
    dryRun: argv.includes("--dry-run") || !argv.includes("--apply"),
    skipVerify: argv.includes("--skip-verify"),
    skipFounderPrep: argv.includes("--skip-founder-prep"),
    skipPublicLockCheck: argv.includes("--skip-public-lock-check"),
    argv,
  };
}

async function runLane1({ brands, apply, argv, reportsDir }) {
  const lane1Brands = brands.filter((b) => LANE1_DEFAULT.includes(b));
  const protectedHits = brands.filter((b) => BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(b));
  const steps = [];

  if (protectedHits.length) {
    steps.push({
      id: "refuse_protected_baseline",
      ok: true,
      note: `Protected public-full baseline untouched: ${protectedHits.join(", ")}`,
    });
  }

  if (!lane1Brands.length) {
    return {
      lane: "fullyReady-restore",
      brands: [],
      steps,
      publicRestore: null,
      visibilityFormalization: {
        accidentalHoldSlugs: [...ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS],
        posture: "n/a",
      },
      summary: { brandCount: 0, note: "no_lane1_brands_in_selection" },
    };
  }

  if (!argv.includes("--skip-verify")) {
    const verify = runNpm("brand-explorer-built-blocked-remediation", [
      "--brands",
      lane1Brands.join(","),
      "--verify-only",
    ]);
    steps.push({
      id: "verify_only",
      ok: verify.ok,
      status: verify.status,
      stderrTail: (verify.stderr || "").slice(-800),
    });
  }

  if (!argv.includes("--skip-founder-prep")) {
    const prep = runNpm("brand-explorer-built-blocked-founder-review-prep", [
      "--brands",
      lane1Brands.join(","),
      "--dry-run",
    ]);
    steps.push({
      id: "founder_review_prep",
      ok: prep.ok,
      status: prep.status,
      stderrTail: (prep.stderr || "").slice(-800),
    });
  }

  const publicRestore = await planPublicRestoreGovernance({
    brands: lane1Brands,
    reportsDir,
  });
  writePublicRestoreGovernanceReports(publicRestore, { reportsDir });
  steps.push({
    id: "public_restore_governance_plan",
    ok: true,
    eligible: publicRestore.summary?.eligibleRestoreCount,
    heldAccidental: publicRestore.summary?.heldAccidentalUnlockCount,
  });

  let applyResult = null;
  if (apply && argv.includes("--approve-public-restore-governance")) {
    applyResult = await applyPublicRestoreGovernance({
      plan: publicRestore,
      apply: true,
      argv,
      reportsDir,
    });
    steps.push({
      id: "public_restore_governance_apply",
      ok: applyResult.applied === true,
      reason: applyResult.reason,
      restoredSlugs: applyResult.restoredSlugs || [],
    });
  } else {
    steps.push({
      id: "public_restore_governance_apply",
      ok: true,
      skipped: true,
      reason: apply
        ? "missing_--approve-public-restore-governance_or_confirm_flags"
        : "dry_run_hold_founder_preview_only",
      note:
        "Country/Suburban/WoodSpring accidental legacy unlock held; remaining Lane 1 brands stay founder-preview-only until founder-approved restore.",
    });
  }

  const lane1Md = [
    `# Remaining Brands — Lane 1 (FullyReady Restore)`,
    ``,
    `- Generated: ${new Date().toISOString()}`,
    `- Brands: ${lane1Brands.join(", ")}`,
    `- Public restore applied: **${applyResult?.applied === true}**`,
    ``,
    `## Visibility formalization`,
    ``,
    `- Accidental legacy unlock hold: ${ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS.join(", ")}`,
    `- Posture without founder approve: **founder-preview-only** (no accidental public-full)`,
    `- Intentional restore registry: \`data/brand-explorer-public-restore-intentional.json\``,
    ``,
    `## Steps`,
    ``,
    ...steps.map(
      (s) =>
        `- \`${s.id}\`: ${s.ok ? "ok" : "FAIL"}${s.skipped ? " (skipped)" : ""}${s.reason ? ` — ${s.reason}` : ""}`
    ),
    ``,
    `## Brand actions`,
    ``,
    ...(publicRestore.brandResults || []).map(
      (b) =>
        `- **${b.slug}**: fullyReady=${b.fullyReady} action=\`${b.action}\` hold=${b.accidentalLegacyUnlockHold}`
    ),
    ``,
    `## Founder restore command`,
    ``,
    "```bash",
    `npm run brand-explorer-public-restore-governance -- --brands ${lane1Brands.join(",")} --apply \\`,
    ...PUBLIC_RESTORE_FLAGS.map((f, i) =>
      i === PUBLIC_RESTORE_FLAGS.length - 1 ? `  ${f}` : `  ${f} \\`
    ),
    "```",
    ``,
  ];
  fs.writeFileSync(path.join(reportsDir, LANE1_MD), `${lane1Md.join("\n")}\n`, "utf8");

  return {
    lane: "fullyReady-restore",
    brands: lane1Brands,
    steps,
    publicRestore,
    applyResult,
    visibilityFormalization: {
      accidentalHoldSlugs: [...ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS],
      postureWithoutFounderApprove: "founder_preview_only",
      publicRestoreApplied: applyResult?.applied === true,
    },
    summary: {
      brandCount: lane1Brands.length,
      fullyReadyCount: publicRestore.summary?.fullyReadyCount ?? null,
      eligibleRestoreCount: publicRestore.summary?.eligibleRestoreCount ?? null,
      heldAccidentalUnlockCount: publicRestore.summary?.heldAccidentalUnlockCount ?? null,
    },
  };
}

async function runLane2ImageSubLane({ lane, brands, apply, argv, reportsDir }) {
  const lane2Brands = brands
    .map((b) => resolveFullBuildSlug(b))
    .filter((b) => LANE2_DEFAULT.includes(b));

  if (!lane2Brands.length) {
    return {
      lane,
      brands: [],
      summary: { brandCount: 0, note: "no_lane2_brands_in_selection" },
    };
  }

  if (lane === "post-draft-integrity") {
    const integrity = await runLane2PostDraftIntegrity({
      brands: lane2Brands,
      reportsDir,
      skipPublicLock: argv.includes("--skip-public-lock-check"),
    });
    return {
      lane,
      brands: lane2Brands,
      integrity,
      summary: {
        brandCount: lane2Brands.length,
        passCount: integrity.summary?.passCount ?? 0,
        failCount: integrity.summary?.failCount ?? 0,
      },
    };
  }

  if (lane === "image-asset-pack") {
    const assetPack = runLane2ImageAssetPack({ brands: lane2Brands, reportsDir });
    return {
      lane,
      brands: lane2Brands,
      assetPack,
      summary: {
        brandCount: lane2Brands.length,
        readyCount: assetPack.summary?.readyCount ?? 0,
        blockedSlugs: assetPack.summary?.blockedSlugs || [],
      },
    };
  }

  if (lane === "image-materialization") {
    const materialization = await runLane2ImageMaterialization({
      brands: lane2Brands,
      dryRun: !apply,
      argv,
      reportsDir,
    });
    return {
      lane,
      brands: lane2Brands,
      materialization,
      summary: {
        brandCount: lane2Brands.length,
        patchCount: materialization.summary?.patchCount ?? 0,
        blocked: materialization.summary?.blocked ?? 0,
        applied: materialization.summary?.applied === true,
      },
    };
  }

  return null;
}

async function runLane2({ brands, apply, argv, reportsDir }) {
  const lane2Brands = brands
    .map((b) => resolveFullBuildSlug(b))
    .filter((b) => LANE2_DEFAULT.includes(b));

  if (!lane2Brands.length) {
    return {
      lane: "full-tab-factory-build",
      brands: [],
      brandResults: [],
      summary: { brandCount: 0, note: "no_lane2_brands_in_selection" },
    };
  }

  const plan = await planFullTabFactoryBuild({ brands: lane2Brands, reportsDir });
  let applyResult = null;
  if (apply) {
    applyResult = await applyFullTabFactoryBuild({ plan, apply: true, argv, reportsDir });
  }

  for (const brand of plan.brandResults || []) {
    writeFullBuildBrandReport(brand, { reportsDir, applyResult });
  }

  const lane2Md = [
    `# Remaining Brands — Lane 2 (Full Tab Factory Build)`,
    ``,
    `- Generated: ${new Date().toISOString()}`,
    `- Brands: ${lane2Brands.join(", ")}`,
    `- Mode: **${apply ? "APPLY" : "dry-run"}**`,
    `- Applied: **${applyResult?.applied === true}**`,
    ``,
    `## Summary`,
    ``,
    `- Planned presentation writes: ${plan.summary?.plannedWriteCount ?? 0}`,
    `- Blocked brands: ${(plan.summary?.blockedSlugs || []).join(", ") || "—"}`,
    `- Release fields written: **false** (confirm-no-release-field-writes)`,
    `- Active release: **not performed** (requires separate founder restore/release)`,
    ``,
    `## Per brand`,
    ``,
    ...(plan.brandResults || []).map(
      (b) =>
        `- **${b.brandSlug}**: rows=${b.presentationRowCount ?? 0} planned=${b.patches?.length ?? 0} blocked=${b.blocked} — see \`reports/brand-explorer-full-build-${b.reportSlug || b.brandSlug}.md\``
    ),
    ``,
    `## Validation commands (after apply)`,
    ``,
    "```bash",
    `npm run brand-explorer-tab-factory-audit -- --brands ${lane2Brands.join(",")} --dry-run`,
    `npm run test:brand-explorer-rendered-field-completeness -- --brands ${lane2Brands.join(",")}`,
    `npm run test:brand-explorer-no-empty-rendered-components -- --brands ${lane2Brands.join(",")}`,
    `npm run brand-explorer-source-provenance-by-tab -- --brands ${lane2Brands.join(",")} --dry-run`,
    `npm run brand-explorer-image-uniqueness-audit -- --brands ${lane2Brands.join(",")} --dry-run`,
    `npm run brand-explorer-image-role-match-audit -- --brands ${lane2Brands.join(",")} --dry-run`,
    `npm run test:brand-explorer-section-pattern-parity -- --brands ${lane2Brands.join(",")}`,
    `npm run brand-explorer-os -- --brands ${lane2Brands.join(",")} --stage release-readiness --dry-run --skip-regression`,
    "```",
    ``,
    `## Apply flags`,
    ``,
    ...(FULL_BUILD_REQUIRED_APPLY_FLAGS || []).map((f) => `- \`${f}\``),
    ``,
  ];
  fs.writeFileSync(path.join(reportsDir, LANE2_MD), `${lane2Md.join("\n")}\n`, "utf8");

  return {
    lane: "full-tab-factory-build",
    brands: lane2Brands,
    plan,
    applyResult,
    summary: {
      brandCount: lane2Brands.length,
      plannedWriteCount: plan.summary?.plannedWriteCount ?? 0,
      applied: applyResult?.applied === true,
      blockedSlugs: plan.summary?.blockedSlugs || [],
    },
  };
}

export async function runRemainingBrandsCompletionProgram(options = {}) {
  const reportsDir = options.reportsDir || path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const opts = {
    brands: options.brands || [...LANE1_DEFAULT, ...LANE2_DEFAULT],
    lane: options.lane || "all",
    apply: options.apply === true,
    argv: options.argv || ["--dry-run"],
    reportsDir,
  };

  const result = {
    version: PROGRAM_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !opts.apply,
    lane: opts.lane,
    brands: opts.brands,
    protectedPublicFullBaseline: [...BUILT_BLOCKED_PROTECTED_PUBLIC_FULL],
    lane1: null,
    lane2: null,
    lane2Integrity: null,
    lane2AssetPack: null,
    lane2Materialization: null,
    globalAcceptance: null,
  };

  if (opts.lane === "all" || opts.lane === "fullyReady-restore") {
    result.lane1 = await runLane1(opts);
  }
  if (opts.lane === "all" || opts.lane === "full-tab-factory-build") {
    result.lane2 = await runLane2(opts);
  }
  if (opts.lane === "post-draft-integrity") {
    result.lane2Integrity = await runLane2ImageSubLane({ ...opts, lane: "post-draft-integrity" });
  }
  if (opts.lane === "image-asset-pack") {
    result.lane2AssetPack = await runLane2ImageSubLane({ ...opts, lane: "image-asset-pack" });
  }
  if (opts.lane === "image-materialization") {
    result.lane2Materialization = await runLane2ImageSubLane({
      ...opts,
      lane: "image-materialization",
    });
  }

  result.globalAcceptance = {
    lane1HeldOrRestored:
      result.lane1 == null
        ? null
        : result.lane1.visibilityFormalization?.publicRestoreApplied === true ||
          result.lane1.visibilityFormalization?.postureWithoutFounderApprove ===
            "founder_preview_only",
    lane2Built: result.lane2 == null ? null : (result.lane2.summary?.plannedWriteCount ?? 0) > 0,
    noAccidentalLegacyUnlockFinalState: true,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    noActiveReleaseWithoutFounderRestore: true,
    notes: [
      "Lane 1 public restore requires separate founder-approved public-restore-governance --apply.",
      "Lane 2 apply writes Presentation draft content only; does not set active_profile_ready or public-full.",
      "PVQL re-check required after any intentional public restore.",
    ],
  };

  return result;
}

export function writeProgramReports(result, { reportsDir = path.join(ROOT, "reports") } = {}) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");

  const md = [
    `# Brand Explorer — Remaining Brands Completion Program`,
    ``,
    `- Version: \`${result.version}\``,
    `- Generated: ${result.generatedAt}`,
    `- Lane: \`${result.lane}\``,
    `- Mode: **${result.dryRun ? "dry-run" : "APPLY"}**`,
    ``,
    `## Protected public-full baseline (untouched)`,
    ``,
    ...(result.protectedPublicFullBaseline || []).map((s) => `- \`${s}\``),
    ``,
    `## Lane 1 — FullyReady restore`,
    ``,
    result.lane1
      ? [
          `- Brands: ${(result.lane1.brands || []).join(", ") || "—"}`,
          `- Fully ready: ${result.lane1.summary?.fullyReadyCount ?? "—"}`,
          `- Eligible restore: ${result.lane1.summary?.eligibleRestoreCount ?? "—"}`,
          `- Accidental unlock held: ${result.lane1.summary?.heldAccidentalUnlockCount ?? "—"}`,
          `- Public restore applied: **${result.lane1.visibilityFormalization?.publicRestoreApplied === true}**`,
          `- Detail: \`reports/${LANE1_MD}\``,
        ].join("\n")
      : "_Skipped (lane selection)_",
    ``,
    `## Lane 2 — Full Tab Factory build`,
    ``,
    result.lane2
      ? [
          `- Brands: ${(result.lane2.brands || []).join(", ") || "—"}`,
          `- Planned writes: ${result.lane2.summary?.plannedWriteCount ?? 0}`,
          `- Applied: **${result.lane2.summary?.applied === true}**`,
          `- Detail: \`reports/${LANE2_MD}\``,
        ].join("\n")
      : "_Skipped (lane selection)_",
    ``,
    `## Lane 2 — Post-draft integrity`,
    ``,
    result.lane2Integrity
      ? [
          `- Brands: ${(result.lane2Integrity.brands || []).join(", ") || "—"}`,
          `- Pass: ${result.lane2Integrity.summary?.passCount ?? "—"}/${result.lane2Integrity.summary?.brandCount ?? "—"}`,
          `- Detail: \`reports/brand-explorer-lane2-post-draft-integrity.md\``,
        ].join("\n")
      : "_Skipped (lane selection)_",
    ``,
    `## Lane 2 — Image asset pack`,
    ``,
    result.lane2AssetPack
      ? [
          `- Brands: ${(result.lane2AssetPack.brands || []).join(", ") || "—"}`,
          `- Ready: ${result.lane2AssetPack.summary?.readyCount ?? "—"}/${result.lane2AssetPack.summary?.brandCount ?? "—"}`,
          `- Blocked: ${(result.lane2AssetPack.summary?.blockedSlugs || []).join(", ") || "—"}`,
          `- Detail: \`reports/brand-explorer-lane2-image-asset-pack.md\``,
        ].join("\n")
      : "_Skipped (lane selection)_",
    ``,
    `## Lane 2 — Image materialization`,
    ``,
    result.lane2Materialization
      ? [
          `- Brands: ${(result.lane2Materialization.brands || []).join(", ") || "—"}`,
          `- Patches: ${result.lane2Materialization.summary?.patchCount ?? 0}`,
          `- Applied: **${result.lane2Materialization.summary?.applied === true}**`,
          `- Detail: \`reports/brand-explorer-lane2-image-materialization.md\``,
        ].join("\n")
      : "_Skipped (lane selection)_",
    ``,
    `## Global acceptance`,
    ``,
    "```json",
    JSON.stringify(result.globalAcceptance, null, 2),
    "```",
    ``,
  ];
  fs.writeFileSync(mdPath, `${md.join("\n")}\n`, "utf8");
  return { jsonPath, mdPath };
}
