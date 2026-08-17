/**
 * Wave 14 — Post-release Recent Momentum dated-card remediation (eight public brands).
 *
 * Scope: footprint.momentum (+ label if needed) only.
 * Flex / House / Morgans / Radisson / protected 46 content: untouched.
 * No Brand Status, release fields, images, CV / Source / Registry writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE14_VERSION,
  WAVE14_PARTIAL_PROMOTION_SLUGS,
  WAVE14_HELD_PROMOTION_SLUG,
  WAVE14_NEVER_WRITE_FIELDS,
  WAVE14_PROTECTED_BASELINE_COUNT,
} from "./brand-explorer-wave14-factory-plan.js";
import {
  WAVE14_DATED_MOMENTUM_PACKAGES_VERSION,
  WAVE14_DATED_MOMENTUM_SLUGS,
  getWave14DatedMomentumPackage,
} from "./brand-explorer-wave14-dated-momentum-packages.js";
import {
  parseMomentumPresentationBody,
} from "./brand-explorer-momentum-link-label.js";
import {
  isStructuredMomentumDateLine,
  RECENT_MOMENTUM_DEFAULT_LABEL,
} from "./brand-explorer-recent-momentum-contract.js";

export const WAVE14_DATED_MOMENTUM_CLEANUP_VERSION =
  "wave14-dated-momentum-cleanup-v1";

export const WAVE14_DATED_MOMENTUM_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-dated-momentum-cleanup",
  "--confirm-eight-public-brand-scope",
  "--confirm-four-points-flex-held",
  "--confirm-momentum-only",
  "--confirm-no-status-writes",
  "--confirm-no-release-field-writes",
  "--confirm-no-image-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-geo-rewrites",
  "--confirm-no-protected-46-brand-changes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WRITE_THROTTLE_MS = 280;
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const HOUSE_SLUG = "the-house-of-originals";
const MORGANS_SLUG = "morgans-originals";
const RADISSON_COLLECTION_SLUG = "radisson-collection";

const FORBIDDEN_VISIBLE_RES = Object.freeze([
  /\bADR\b/,
  /\bRevPAR\b/,
  /fee-?stack/i,
  /\bFDD\b/,
  /Item\s*19/i,
  /\bLOI\b/,
  /source-supported/i,
  /\bsource pack\b/i,
  /\bfactory\b/i,
  /\bstage\s*\d/i,
  /\bCompany Validated\b/i,
  /\billustrative activity\b/i,
  /\bdirectional themes?\b/i,
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(text) {
  return nz(text)
    .split(/\s+/)
    .filter(Boolean).length;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function checkFlags(required, argv, apply) {
  const missing = required.filter((f) => !argv.includes(f));
  return {
    apply: apply === true,
    ok: apply === true && missing.length === 0,
    missing,
    required: [...required],
  };
}

function resolveIdentity(slug) {
  return FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug] || null;
}

function isHidden(row) {
  return (
    row?.active === false ||
    /do not display|internal only/i.test(nz(row?.externalDisplayStatus))
  );
}

function findSlot(rows, slotKey) {
  const matches = (rows || [])
    .filter((r) => nz(r.slotKey) === slotKey && !isHidden(r))
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
  return matches[0] || null;
}

function findAllSlots(rows, slotKey) {
  return (rows || [])
    .filter((r) => nz(r.slotKey) === slotKey && !isHidden(r))
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
}

function isDatedForSectionPattern(dateLine, body, summary) {
  return /\b(20\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/i.test(
    `${dateLine || ""} ${body || ""} ${summary || ""}`
  );
}

function assertPackageClean(pkg, slug) {
  const issues = [];
  let datedCount = 0;
  for (const c of pkg.momentumCards || []) {
    for (const re of FORBIDDEN_VISIBLE_RES) {
      if (re.test(`${c.title}\n${c.dateLine}\n${c.summary}`)) {
        issues.push(`${slug}:forbidden:${re.source}`);
      }
    }
    if (!/^https?:\/\//i.test(c.url || "")) {
      issues.push(`${slug}:momentum_missing_url:${c.title}`);
    }
    if (!isStructuredMomentumDateLine(c.dateLine)) {
      issues.push(`${slug}:dateLine_not_structured:${c.dateLine}`);
    }
    if (isDatedForSectionPattern(c.dateLine, c.body, c.summary)) {
      datedCount += 1;
    }
    // Property overview pages must not invent year-only dateLines
    if (/^\d{4}$/.test(nz(c.dateLine)) && /\/overview\/?$/i.test(nz(c.url))) {
      issues.push(`${slug}:invented_year_on_property_listing:${c.title}`);
    }
    if (words(c.summary) < 35) {
      issues.push(`${slug}:momentum_summary_thin:${c.title}:${words(c.summary)}`);
    }
  }
  if ((pkg.momentumCards || []).length < 2) {
    issues.push(`${slug}:momentum_cards_below_2`);
  }
  if (datedCount < 2) {
    issues.push(`${slug}:section_dated_cards_below_2:${datedCount}`);
  }
  return [...new Set(issues)];
}

function scrubForbiddenFields(fields) {
  const next = { ...fields };
  for (const forbidden of [
    ...WAVE14_NEVER_WRITE_FIELDS,
    "Brand Status",
    "Active Profile Approved",
    "Ready for Active Profile",
    "Active Profile Approved Date",
    "Founder Visual Review Pass",
    "Image",
  ]) {
    if (next[forbidden] != null) delete next[forbidden];
  }
  return next;
}

async function airtablePatch(baseId, apiKey, table, recordId, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields: scrubForbiddenFields(fields) }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH failed ${res.status}`);
  return json;
}

async function airtableCreate(baseId, apiKey, table, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields: scrubForbiddenFields(fields) }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `POST failed ${res.status}`);
  return json;
}

export async function planWave14DatedMomentumCleanupForBrand(slug) {
  if (slug === WAVE14_HELD_PROMOTION_SLUG) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["flex_held_and_untouched"],
      patches: [],
    };
  }
  if ([HOUSE_SLUG, MORGANS_SLUG, RADISSON_COLLECTION_SLUG].includes(slug)) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["forbidden_excluded_brand"],
      patches: [],
    };
  }
  if (!WAVE14_DATED_MOMENTUM_SLUGS.includes(slug)) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["out_of_eight_public_scope"],
      patches: [],
    };
  }

  const identity = resolveIdentity(slug);
  let pkg;
  try {
    pkg = getWave14DatedMomentumPackage(slug);
  } catch (err) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: [`missing_package:${err.message}`],
      patches: [],
    };
  }
  if (!identity?.recordId) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["missing_identity"],
      patches: [],
    };
  }

  const packageIssues = assertPackageClean(pkg, slug);
  if (packageIssues.length) {
    return {
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: packageIssues,
      patches: [],
      stopReason: "unsupported_or_thin_package",
    };
  }

  const { rows, skipped } = await listPresentationRowsLight(identity.recordId, identity.name);
  if (skipped) {
    return {
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: [skipped],
      patches: [],
    };
  }

  const liveMomentum = findAllSlots(rows, "footprint.momentum");
  const before = {
    momentum: liveMomentum.map((r) => {
      const parsed = parseMomentumPresentationBody(r.body, r.title);
      return {
        recordId: r.recordId,
        title: nz(r.title),
        dateLine: parsed.dateLine || null,
        sectionDated: isDatedForSectionPattern(parsed.dateLine, r.body, parsed.description),
        structuredDate: isStructuredMomentumDateLine(parsed.dateLine),
        url: parsed.sourceUrl || null,
        bodyPreview: nz(r.body).slice(0, 140),
      };
    }),
  };

  const patches = [];

  // Prefer in-place PATCH of existing visible momentum rows (minimal write).
  for (let i = 0; i < pkg.momentumCards.length; i += 1) {
    const card = pkg.momentumCards[i];
    const live = liveMomentum[i];
    const fields = {
      Title: card.title,
      Body: card.body,
      Active: true,
      "Sort Order": card.sort || i + 1,
      "Case Summary Tags": card.regionLabel || "",
      "Case Summary Overview": `${card.dateLine} · ${card.regionLabel || ""}`.trim(),
      "External Display Status": null,
    };
    if (live?.recordId) {
      const parsed = parseMomentumPresentationBody(live.body, live.title);
      const needs =
        nz(live.title) !== card.title ||
        nz(live.body) !== card.body ||
        !isDatedForSectionPattern(parsed.dateLine, live.body, parsed.description) ||
        !isStructuredMomentumDateLine(parsed.dateLine);
      if (needs) {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: live.recordId,
          slotKey: "footprint.momentum",
          fields,
          reason: "repair_dated_momentum_card",
          evidence: {
            title: card.title,
            dateLine: card.dateLine,
            url: card.url,
            regionLabel: card.regionLabel,
            dateRationale: pkg.dateRationale,
          },
        });
      }
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey: "footprint.momentum",
        fields: {
          "Slot Key": "footprint.momentum",
          "Brand Name": identity.name,
          Brand: [identity.recordId],
          ...fields,
        },
        reason: "create_dated_momentum_card",
        evidence: {
          title: card.title,
          dateLine: card.dateLine,
          url: card.url,
          regionLabel: card.regionLabel,
          dateRationale: pkg.dateRationale,
        },
      });
    }
  }

  // Hide surplus undated live cards beyond package length
  for (let i = pkg.momentumCards.length; i < liveMomentum.length; i += 1) {
    const row = liveMomentum[i];
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: row.recordId,
      slotKey: "footprint.momentum",
      fields: {
        Active: false,
        "External Display Status": "Do Not Display",
      },
      reason: "hide_surplus_undated_momentum_card",
    });
  }

  const labelLive = findSlot(rows, "footprint.momentum_label");
  const labelBody = pkg.momentumLabel || RECENT_MOMENTUM_DEFAULT_LABEL;
  if (labelLive?.recordId) {
    if (nz(labelLive.body) !== labelBody) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: labelLive.recordId,
        slotKey: "footprint.momentum_label",
        fields: { Body: labelBody, Active: true },
        reason: "momentum_label_contract",
      });
    }
  }

  return {
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    blocked: false,
    blockers: [],
    before,
    after: {
      momentum: pkg.momentumCards.map((c) => ({
        title: c.title,
        dateLine: c.dateLine,
        regionLabel: c.regionLabel,
        summaryWords: words(c.summary),
        url: c.url,
      })),
      dateRationale: pkg.dateRationale,
    },
    patches,
    plannedWrites: patches.length,
    diagnostics: {
      liveMomentumCount: liveMomentum.length,
      packageMomentumCount: pkg.momentumCards.length,
      datedBefore: before.momentum.filter((m) => m.sectionDated).length,
      datedAfter: pkg.momentumCards.length,
    },
  };
}

function renderBrandMd(plan) {
  const lines = [
    `# Wave 14 Dated Momentum Cleanup — ${plan.brandName || plan.brandSlug}`,
    "",
    `Slug: \`${plan.brandSlug}\` · Record: \`${plan.recordId || "—"}\``,
    "",
  ];
  if (plan.blocked) {
    lines.push(`**BLOCKED:** ${(plan.blockers || []).join("; ")}`, "");
    return lines.join("\n");
  }
  lines.push(
    "## Before",
    "",
    `| Title | DateLine | Section dated | URL |`,
    `| --- | --- | --- | --- |`
  );
  for (const m of plan.before?.momentum || []) {
    lines.push(
      `| ${m.title || "(untitled)"} | ${m.dateLine || "—"} | ${m.sectionDated} | ${m.url || "—"} |`
    );
  }
  lines.push(
    "",
    "## After (planned)",
    "",
    `| Title | DateLine | Region | Source |`,
    `| --- | --- | --- | --- |`
  );
  for (const m of plan.after?.momentum || []) {
    lines.push(
      `| ${m.title} | **${m.dateLine}** | ${m.regionLabel || "—"} | ${m.url} |`
    );
  }
  lines.push(
    "",
    `Date rationale: ${plan.after?.dateRationale || "—"}`,
    "",
    `## Patches (${plan.plannedWrites})`,
    ""
  );
  for (const p of plan.patches || []) {
    lines.push(`- \`${p.action}\` ${p.slotKey} — ${p.reason}${p.recordId ? ` (\`${p.recordId}\`)` : ""}`);
  }
  lines.push("");
  return lines.join("\n");
}

function renderSummaryMd(report) {
  const lines = [
    `# Brand Explorer Wave 14 — Dated Momentum Cleanup`,
    "",
    `Version: \`${report.version}\` · Packages: \`${report.packagesVersion}\``,
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${report.writePerformed}**`,
    "",
    `Ready: \`${report.readyStatement}\``,
    "",
    `## Scope`,
    "",
    `- In: ${WAVE14_DATED_MOMENTUM_SLUGS.map((s) => `\`${s}\``).join(", ")}`,
    `- Held / untouched: \`${WAVE14_HELD_PROMOTION_SLUG}\``,
    `- Excluded: House of Originals · Morgans Originals · Radisson Collection`,
    `- Protected baseline remains **${WAVE14_PROTECTED_BASELINE_COUNT}** (read-only)`,
    "",
    `## Brands`,
    "",
  ];
  for (const b of report.brands || []) {
    lines.push(
      `- **${b.brandName || b.brandSlug}**: blocked=${b.blocked} · patches=${b.plannedWrites || 0}` +
        (b.diagnostics
          ? ` · dated before/after=${b.diagnostics.datedBefore}/${b.diagnostics.datedAfter}`
          : "")
    );
  }
  lines.push(
    "",
    `## Guardrails`,
    ""
  );
  for (const [k, v] of Object.entries(report.guardrails || {})) {
    lines.push(`- ${k}: ${v}`);
  }
  lines.push("");
  return lines.join("\n");
}

/**
 * @param {{ apply?: boolean, argv?: string[] }} opts
 */
export async function runWave14DatedMomentumCleanup({ apply = false, argv = [] } = {}) {
  const stage = "dated-momentum-cleanup";
  const flagCheck = checkFlags(WAVE14_DATED_MOMENTUM_CLEANUP_APPLY_FLAGS, argv, apply);
  const brandResults = [];

  for (const slug of WAVE14_DATED_MOMENTUM_SLUGS) {
    const plan = await planWave14DatedMomentumCleanupForBrand(slug);
    brandResults.push(plan);
    await sleep(120);
  }

  // Explicit Flex hold check (never plan Flex)
  const flexPlan = await planWave14DatedMomentumCleanupForBrand(WAVE14_HELD_PROMOTION_SLUG);
  if (!flexPlan.blocked || !flexPlan.blockers.includes("flex_held_and_untouched")) {
    throw new Error("Refuse: Four Points Flex must remain blocked/untouched");
  }

  const anyBlocked = brandResults.some((b) => b.blocked);
  const preflightOk = !anyBlocked && brandResults.length === WAVE14_DATED_MOMENTUM_SLUGS.length;
  const applyPerformed = apply === true && flagCheck.ok === true && preflightOk;
  const applyResults = [];
  let writePerformed = false;

  if (applyPerformed) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

    for (const plan of brandResults) {
      for (const patch of plan.patches || []) {
        try {
          if (patch.action === "PATCH") {
            await airtablePatch(baseId, apiKey, patch.table, patch.recordId, patch.fields);
          } else if (patch.action === "POST") {
            const created = await airtableCreate(baseId, apiKey, patch.table, patch.fields);
            patch.createdId = created.id;
          } else {
            throw new Error(`Unknown patch action ${patch.action}`);
          }
          writePerformed = true;
          applyResults.push({
            brandSlug: plan.brandSlug,
            action: patch.action,
            slotKey: patch.slotKey,
            recordId: patch.recordId || patch.createdId || null,
            reason: patch.reason,
            applied: true,
            evidence: patch.evidence || null,
          });
        } catch (err) {
          applyResults.push({
            brandSlug: plan.brandSlug,
            action: patch.action,
            slotKey: patch.slotKey,
            recordId: patch.recordId,
            reason: patch.reason,
            applied: false,
            error: err.message,
          });
        }
        await sleep(WRITE_THROTTLE_MS);
      }
    }
  } else if (apply && !flagCheck.ok) {
    applyResults.push({ applied: false, reason: "missing_apply_flags", missing: flagCheck.missing });
  } else if (apply && !preflightOk) {
    applyResults.push({
      applied: false,
      reason: "preflight_failed",
      blocked: brandResults.filter((b) => b.blocked).map((b) => ({
        slug: b.brandSlug,
        blockers: b.blockers,
      })),
    });
  }

  const readyStatement = applyPerformed
    ? "wave14_dated_momentum_cleanup_applied_ready_for_pvql_recheck"
    : anyBlocked
      ? "wave14_dated_momentum_cleanup_blocked"
      : "wave14_dated_momentum_cleanup_dry_run";

  const report = {
    version: WAVE14_DATED_MOMENTUM_CLEANUP_VERSION,
    packagesVersion: WAVE14_DATED_MOMENTUM_PACKAGES_VERSION,
    waveVersion: WAVE14_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed,
    dryRun: !applyPerformed,
    flagCheck,
    requiredApplyFlags: [...WAVE14_DATED_MOMENTUM_CLEANUP_APPLY_FLAGS],
    scope: {
      brands: [...WAVE14_DATED_MOMENTUM_SLUGS],
      held: WAVE14_HELD_PROMOTION_SLUG,
      protectedBaseline: WAVE14_PROTECTED_BASELINE_COUNT,
    },
    brands: brandResults,
    applyResults,
    summary: {
      brandCount: brandResults.length,
      blockedCount: brandResults.filter((b) => b.blocked).length,
      plannedPatches: brandResults.reduce((n, b) => n + (b.plannedWrites || 0), 0),
      appliedCount: applyResults.filter((r) => r.applied === true).length,
    },
    guardrails: {
      eightPublicOnly: true,
      flexHeld: true,
      momentumOnly: true,
      noBrandStatus: true,
      noReleaseFields: true,
      noImages: true,
      noGeoRewrites: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryApprovalWrites: false,
      protected46Untouched: true,
      houseOfOriginalsUntouched: true,
      morgansOriginalsUntouched: true,
      radissonCollectionUntouched: true,
    },
    freezeNote:
      "Do not freeze 54 until quiet PVQL + 24-tab pass after this cleanup. Prefer waiting for Flex founder A/B/C/D → 55, or an explicit interim-54 decision.",
    readyStatement,
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave14-dated-momentum-cleanup.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave14-dated-momentum-cleanup.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, `${renderSummaryMd(report)}\n`, "utf8");

  for (const plan of brandResults) {
    const brandMd = path.join(
      REPORTS_DIR,
      `brand-explorer-wave14-dated-momentum-cleanup-${plan.brandSlug}.md`
    );
    fs.writeFileSync(brandMd, `${renderBrandMd(plan)}\n`, "utf8");
  }

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave14-dated-momentum-cleanup.md");
  fs.writeFileSync(
    docPath,
    [
      `# Wave 14 — Post-Release Dated Momentum Cleanup`,
      "",
      "Remediates Recent Momentum `dated_cards_below_min` for the eight public Wave 14 brands.",
      "",
      "## Root cause",
      "",
      "Stage 6 used `Directory` dateLines (valid for structured-date evidence, **not** for section-pattern dated regex requiring year/month).",
      "",
      "## Dating convention",
      "",
      '`dateLine: "2026"` = steward-verified live on official Marriott.com / hotel-development.marriott.com pages (Wave 13 directory pattern). URLs from Wave 14 source packs.',
      "",
      "## Scope",
      "",
      `- In: ${WAVE14_DATED_MOMENTUM_SLUGS.join(", ")}`,
      `- Held: ${WAVE14_HELD_PROMOTION_SLUG}`,
      "",
      "## Commands",
      "",
      "```bash",
      "npm run brand-explorer-wave14-factory -- --stage dated-momentum-cleanup --dry-run",
      "npm run brand-explorer-wave14-factory -- --stage dated-momentum-cleanup --apply \\",
      "  --approve-wave14-dated-momentum-cleanup \\",
      "  --confirm-eight-public-brand-scope \\",
      "  --confirm-four-points-flex-held \\",
      "  --confirm-momentum-only \\",
      "  ...",
      "```",
      "",
      `Ready: \`${readyStatement}\``,
      "",
      `Last generated: ${report.generatedAt}`,
      "",
    ].join("\n"),
    "utf8"
  );

  return {
    ...report,
    paths: { jsonPath, mdPath, docPath },
    pass: preflightOk,
    stopRecommended: anyBlocked,
    airtableWrites: writePerformed,
  };
}
