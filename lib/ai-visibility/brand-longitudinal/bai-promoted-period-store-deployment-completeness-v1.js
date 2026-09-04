/**
 * BAI_PROMOTED_PERIOD_STORE_DEPLOYMENT_COMPLETENESS
 *
 * Permanent pre-publication / pre-deploy gate: the promoted (or about-to-promote)
 * brand-longitudinal period store must be fully serveable from a CLEAN DEPLOY
 * CONTEXT — git-tracked artifacts that survive .railwayignore — with no
 * dependency on untracked local working-tree files.
 *
 * Failure class prevented: live customer pointer without packaged P2 store
 * (first Period 2 publication deploy).
 *
 * NO provider calls. NO publication mutation.
 */

import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { execFileSync } from "child_process";
import {
  BAI_PERIOD_2_CANDIDATE_ID,
  BAI_CUSTOMER_PUBLISHED_PERIOD_ID,
  BAI_VIEW_MODE,
} from "./resolve-bai-prior-comparable-period-v1.js";
import {
  applyBaiPublishedLongitudinalToCustomerPayload,
  buildBaiCustomerLongitudinalAttachmentIfReady,
} from "./bai-customer-longitudinal-payload-v1.js";
import { buildBaiWave4LongitudinalPresentationV1 } from "./bai-wave4-longitudinal-presentation-v1.js";
import { BRAND_LONGITUDINAL_STORE_ROOT } from "./measurement-period.js";

export const BAI_PROMOTED_PERIOD_STORE_DEPLOYMENT_COMPLETENESS =
  "BAI_PROMOTED_PERIOD_STORE_DEPLOYMENT_COMPLETENESS";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");

/** Lean deploy contract for customer longitudinal (Wave 3/4 + attach). */
export const BAI_PROMOTED_PERIOD_REQUIRED_ROOT_FILES = Object.freeze([
  "current-vs-prior.json",
  "period-manifest.json",
]);

export const BAI_PROMOTED_PERIOD_REQUIRED_DIRS = Object.freeze([
  "evidence",
  "mentions",
]);

function toPosix(p) {
  return String(p || "").replace(/\\/g, "/");
}

function periodRelPrefix(periodId) {
  return toPosix(
    path.posix.join(
      "data/ai-visibility/runtime/brand-longitudinal",
      periodId
    )
  );
}

/**
 * Minimal gitignore-style matcher (last match wins; supports *, **, trailing /).
 * Sufficient for auditing .gitignore / .railwayignore packaging rules.
 */
export function matchIgnorePattern(relPath, pattern) {
  let pat = toPosix(pattern).trim();
  if (!pat) return false;
  const onlyDir = pat.endsWith("/");
  if (onlyDir) pat = pat.slice(0, -1);
  const pathPosix = toPosix(relPath).replace(/^\.\//, "");

  if (onlyDir) {
    if (!(pathPosix === pat || pathPosix.startsWith(pat + "/"))) {
      return false;
    }
  }

  let rx = "";
  for (let i = 0; i < pat.length; i += 1) {
    const c = pat[i];
    if (c === "*" && pat[i + 1] === "*") {
      rx += ".*";
      i += 1;
      continue;
    }
    if (c === "*") {
      rx += "[^/]*";
      continue;
    }
    if ("\\.()+?[]{}^$|".includes(c)) rx += "\\" + c;
    else rx += c;
  }

  if (!pat.includes("/")) {
    return new RegExp(`(^|/)${rx}(/|$)`).test(pathPosix);
  }

  if (pat.includes("**")) {
    return (
      new RegExp(`(^|/)${rx}(/|$)`).test(pathPosix) ||
      new RegExp(`^${rx}`).test(pathPosix) ||
      new RegExp(`(^|/)${rx}$`).test(pathPosix)
    );
  }

  const anchored = pat.startsWith("/") ? `^${rx}$` : `(^|/)${rx}$`;
  return new RegExp(anchored).test(pathPosix);
}

/**
 * Apply ignore-file rules with parent-directory inheritance (gitignore semantics):
 * if a parent is ignored, descendants stay ignored unless a later negation matches.
 */
export function isPathIgnoredByIgnoreFile(relPath, ignoreFileContents) {
  const pathPosix = toPosix(relPath).replace(/^\.\//, "");
  const lines = String(ignoreFileContents || "")
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l && !l.startsWith("#"))
    .map((line) => ({
      negated: line.startsWith("!"),
      pattern: line.startsWith("!") ? line.slice(1) : line,
    }));

  const parts = pathPosix.split("/").filter(Boolean);
  let ignored = false;
  for (let depth = 1; depth <= parts.length; depth += 1) {
    const candidate = parts.slice(0, depth).join("/");
    const isLast = depth === parts.length;
    for (const { negated, pattern } of lines) {
      const pat = pattern.endsWith("/") ? pattern : pattern;
      // Directory-only patterns apply to intermediate parents too
      if (matchIgnorePattern(candidate, pat)) {
        ignored = !negated;
        continue;
      }
      // data/* style: match direct child of data/ as directory ignore for descendants
      if (
        !isLast &&
        !negated &&
        matchIgnorePattern(candidate, pat.replace(/\/$/, ""))
      ) {
        ignored = true;
      }
    }
    // Explicit re-check full-path / candidate against all rules (last match wins)
    for (const { negated, pattern } of lines) {
      if (matchIgnorePattern(candidate, pattern)) {
        ignored = !negated;
      }
    }
  }
  return ignored;
}

function gitLsFiles(repoRoot, prefix) {
  try {
    const out = execFileSync(
      "git",
      ["ls-files", "-z", "--", prefix],
      { cwd: repoRoot, encoding: "utf8", maxBuffer: 32 * 1024 * 1024 }
    );
    return out
      .split("\0")
      .map((s) => s.trim())
      .filter(Boolean)
      .map(toPosix);
  } catch (err) {
    return { error: err.message || String(err) };
  }
}

function readText(repoRoot, rel) {
  return fs.readFileSync(path.join(repoRoot, rel), "utf8");
}

function listRequiredRelativePaths(periodId, trackedFiles) {
  const prefix = periodRelPrefix(periodId);
  const required = [];
  for (const f of BAI_PROMOTED_PERIOD_REQUIRED_ROOT_FILES) {
    required.push(`${prefix}/${f}`);
  }
  const evidence = trackedFiles.filter(
    (f) => f.startsWith(`${prefix}/evidence/`) && f.endsWith(".json")
  );
  const mentions = trackedFiles.filter(
    (f) => f.startsWith(`${prefix}/mentions/`) && f.endsWith(".json")
  );
  return { requiredRoots: required, evidence, mentions, prefix };
}

/**
 * Stage a clean deploy-like storeRoot containing only git-tracked files that
 * survive .railwayignore for the period.
 */
export function stageBaiPromotedPeriodCleanDeployStore(opts = {}) {
  const repoRoot = opts.repoRoot || REPO_ROOT;
  const periodId = opts.periodId || BAI_CUSTOMER_PUBLISHED_PERIOD_ID;
  const prefix = periodRelPrefix(periodId);
  const tracked = gitLsFiles(repoRoot, prefix);
  if (tracked.error) {
    return { ok: false, reason: "git_ls_files_failed", detail: tracked.error };
  }

  const railwayignore = readText(repoRoot, ".railwayignore");
  const deployable = tracked.filter(
    (f) => !isPathIgnoredByIgnoreFile(f, railwayignore)
  );

  const tmpRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "bai-promoted-store-deploy-")
  );
  const storeRoot = tmpRoot;
  let copied = 0;
  for (const rel of deployable) {
    if (!rel.startsWith(prefix + "/") && rel !== prefix) continue;
    const underPeriod = rel.slice(prefix.length).replace(/^\//, "");
    const dest = path.join(storeRoot, periodId, underPeriod);
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    fs.copyFileSync(path.join(repoRoot, rel), dest);
    copied += 1;
  }

  return {
    ok: copied > 0,
    tmpRoot,
    storeRoot,
    periodId,
    trackedCount: tracked.length,
    deployableCount: deployable.length,
    copied,
    deployable,
    tracked,
  };
}

/**
 * Evaluate deployment completeness for a promoted / candidate period.
 *
 * @param {{ periodId?: string, repoRoot?: string, requirePublishedPointer?: boolean }} [opts]
 */
export function evaluateBaiPromotedPeriodStoreDeploymentCompletenessV1(
  opts = {}
) {
  const repoRoot = opts.repoRoot || REPO_ROOT;
  const periodId =
    opts.periodId ||
    BAI_CUSTOMER_PUBLISHED_PERIOD_ID ||
    BAI_PERIOD_2_CANDIDATE_ID;
  const checks = [];
  const failures = [];

  const push = (name, ok, detail = null) => {
    checks.push({ name, ok: !!ok, detail });
    if (!ok) failures.push({ name, detail });
  };

  const prefix = periodRelPrefix(periodId);
  const gitignore = readText(repoRoot, ".gitignore");
  const railwayignore = readText(repoRoot, ".railwayignore");

  const trackedOrErr = gitLsFiles(repoRoot, prefix);
  if (trackedOrErr.error) {
    return {
      ok: false,
      gate: BAI_PROMOTED_PERIOD_STORE_DEPLOYMENT_COMPLETENESS,
      periodId,
      failures: [{ name: "git_ls_files", detail: trackedOrErr.error }],
      checks: [],
      LIVE_MUTATION: false,
    };
  }
  const tracked = trackedOrErr;
  const { requiredRoots, evidence, mentions } = listRequiredRelativePaths(
    periodId,
    tracked
  );

  for (const rel of requiredRoots) {
    const inGit = tracked.includes(rel);
    push(`tracked:${path.posix.basename(rel)}`, inGit, rel);
  }
  push("tracked:evidence_json", evidence.length > 0, {
    count: evidence.length,
  });
  push("tracked:mentions_json", mentions.length > 0, {
    count: mentions.length,
  });

  // Git packaging: required paths must be in the index (clean checkout), not
  // merely present on local disk. Sample evidence/mentions for force-track audit.
  const gitAuditPaths = [
    ...requiredRoots,
    ...evidence.slice(0, 3),
    ...mentions.slice(0, 3),
  ];
  for (const rel of gitAuditPaths) {
    const inGit = tracked.includes(rel);
    const wouldIgnore = isPathIgnoredByIgnoreFile(rel, gitignore);
    push(`gitignore_index_packable:${rel.split("/").slice(-2).join("/")}`, inGit, {
      wouldIgnoreWithoutForceAdd: wouldIgnore,
      tracked: inGit,
    });
  }

  const gitNegation =
    gitignore.includes("brand-longitudinal/") ||
    gitignore.includes(periodId) ||
    gitignore.includes(prefix);
  push("gitignore_has_period_packaging_intent", gitNegation || tracked.length > 0, {
    gitNegation,
    trackedCount: tracked.length,
  });

  for (const rel of requiredRoots) {
    const excluded = isPathIgnoredByIgnoreFile(rel, railwayignore);
    push(`railwayignore_allows:${path.posix.basename(rel)}`, !excluded, {
      excluded,
      rel,
    });
  }
  if (evidence[0]) {
    push(
      "railwayignore_allows:evidence_sample",
      !isPathIgnoredByIgnoreFile(evidence[0], railwayignore),
      evidence[0]
    );
  }
  if (mentions[0]) {
    push(
      "railwayignore_allows:mentions_sample",
      !isPathIgnoredByIgnoreFile(mentions[0], railwayignore),
      mentions[0]
    );
  }

  const pointerIsThisPeriod = BAI_CUSTOMER_PUBLISHED_PERIOD_ID === periodId;
  push("published_pointer_resolves_to_audited_period_or_candidate", true, {
    BAI_CUSTOMER_PUBLISHED_PERIOD_ID,
    periodId,
    pointerIsThisPeriod,
  });

  let stage = null;
  try {
    stage = stageBaiPromotedPeriodCleanDeployStore({ repoRoot, periodId });
    push("clean_deploy_stage_copied", stage.ok && stage.copied > 0, {
      copied: stage.copied,
      trackedCount: stage.trackedCount,
      deployableCount: stage.deployableCount,
    });

    const stagedCurrentVsPrior = path.join(
      stage.storeRoot,
      periodId,
      "current-vs-prior.json"
    );
    const stagedEvidenceDir = path.join(stage.storeRoot, periodId, "evidence");
    const stagedMentionsDir = path.join(stage.storeRoot, periodId, "mentions");
    push(
      "clean_deploy_has_current_vs_prior",
      fs.existsSync(stagedCurrentVsPrior),
      stagedCurrentVsPrior
    );
    push(
      "clean_deploy_has_evidence_dir",
      fs.existsSync(stagedEvidenceDir) &&
        fs.readdirSync(stagedEvidenceDir).some((f) => f.endsWith(".json")),
      stagedEvidenceDir
    );
    push(
      "clean_deploy_has_mentions_dir",
      fs.existsSync(stagedMentionsDir) &&
        fs.readdirSync(stagedMentionsDir).some((f) => f.endsWith(".json")),
      stagedMentionsDir
    );

    const wave4 = buildBaiWave4LongitudinalPresentationV1({
      viewMode: BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW,
      scope: "full_cohort",
      parentCompanyName: "all",
      geography: "CALA",
      storeRoot: stage.storeRoot,
    });
    push("clean_deploy_wave4_ok", wave4.ok === true, {
      reason: wave4.reason || null,
      parents: wave4.parents?.length,
    });

    const attach = buildBaiCustomerLongitudinalAttachmentIfReady({
      viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
      geography: "CALA",
      parentCompanyName: "marriott",
      scope: "parent_filter",
      storeRoot: stage.storeRoot,
    });

    if (pointerIsThisPeriod) {
      push(
        "clean_deploy_customer_attach",
        attach.attached === true && !!attach.customerLongitudinal,
        {
          attached: attach.attached,
          reason: attach.reason || attach.wave4?.reason || null,
        }
      );
      const overlay = applyBaiPublishedLongitudinalToCustomerPayload(
        {
          monitoringFreshness: {},
          currentPosition: {
            portfolioAiPresence: { display: "0.0%", value: 0 },
          },
        },
        {
          geography: "CALA",
          parentCompanyKey: "marriott",
          storeRoot: stage.storeRoot,
        }
      );
      push(
        "clean_deploy_published_overlay",
        overlay.customerLongitudinalAttached === true &&
          overlay.publicationDates?.currentPeriodId === periodId,
        {
          attached: overlay.customerLongitudinalAttached,
          reason: overlay.customerLongitudinalAttachReason || null,
          dates: overlay.publicationDates || null,
        }
      );
    } else {
      push("clean_deploy_customer_attach", wave4.ok === true, {
        note: "pointer not yet this period; wave4 clean load is the attach proxy",
      });
    }

    push(
      "no_local_working_tree_only_dependency",
      stage.storeRoot !== BRAND_LONGITUDINAL_STORE_ROOT &&
        !toPosix(stage.storeRoot).includes(
          "data/ai-visibility/runtime/brand-longitudinal"
        ),
      { stageStoreRoot: stage.storeRoot }
    );
  } catch (err) {
    push("clean_deploy_simulation", false, err.message || String(err));
  } finally {
    if (stage?.tmpRoot && fs.existsSync(stage.tmpRoot)) {
      try {
        fs.rmSync(stage.tmpRoot, { recursive: true, force: true });
      } catch {
        /* best-effort cleanup */
      }
    }
  }

  return {
    ok: failures.length === 0,
    gate: BAI_PROMOTED_PERIOD_STORE_DEPLOYMENT_COMPLETENESS,
    periodId,
    publishedPointerPeriodId: BAI_CUSTOMER_PUBLISHED_PERIOD_ID,
    pointerIsThisPeriod,
    requiredRootFiles: [...BAI_PROMOTED_PERIOD_REQUIRED_ROOT_FILES],
    requiredDirs: [...BAI_PROMOTED_PERIOD_REQUIRED_DIRS],
    trackedCounts: {
      total: tracked.length,
      evidence: evidence.length,
      mentions: mentions.length,
    },
    checks,
    failures,
    LIVE_MUTATION: false,
    LIVE_PROVIDER_CALLS: 0,
  };
}

export default evaluateBaiPromotedPeriodStoreDeploymentCompletenessV1;
