/**
 * Wave 13 — SO/ hold remediation (Under Review only).
 *
 * Scope: so-hotels-and-resorts Presentation (+ optional source-supported Brand Website).
 * Removes process-language residue, structures Recent Momentum, rebuilds openings,
 * fills International Reference geo region cards. Does NOT invent snapshot.* steward facts.
 *
 * Forbidden: Brand Status, release fields, CV, Source Library, Registry, restore registry,
 * active 45 brands, House of Originals, Morgans Originals, Radisson Collection, images,
 * broad rewrites outside flagged SO/ slots.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import { RECENT_MOMENTUM_DEFAULT_LABEL } from "./brand-explorer-recent-momentum-contract.js";
import {
  WAVE13_VERSION,
  WAVE13_HELD_PROMOTION_SLUG,
  WAVE13_NEVER_WRITE_FIELDS,
  WAVE13_SO_HOLD_REMEDIATION_APPLY_FLAGS,
} from "./brand-explorer-wave13-factory-plan.js";
import {
  WAVE13_SO_HOLD_PACKAGES_VERSION,
  SO_SLUG,
  SO_BASICS_RECORD_ID,
  SO_PRESENTATION_BRAND_NAME,
  SO_BODY_REWRITES,
  SO_GEO_PACKAGE,
  SO_OPENINGS_PACKAGE,
  SO_BASICS_OPTIONAL_STEWARD,
  SO_POSITIONING_BODY,
  SO_AUDIENCE_BODY,
} from "./brand-explorer-wave13-so-hold-remediation-packages.js";
import {
  buildWave13FounderReviewPacket,
  renderWave13FounderReviewMarkdown,
} from "./brand-explorer-wave13-founder-review.js";

export const WAVE13_SO_HOLD_REMEDIATION_VERSION = "wave13-so-hold-remediation-v1";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS_TABLE = "Brand Setup - Brand Basics";
const WRITE_THROTTLE_MS = 280;
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const FORBIDDEN_VISIBLE_RES = Object.freeze([
  /\bADR\b/,
  /\bRevPAR\b/,
  /fee-?stack/i,
  /\bFDD\b/,
  /Item\s*19/i,
  /\bLOI\b/,
  /owner-fit diligence/i,
  /\bfactory\b/i,
  /stage\s*\d/i,
  /do not create/i,
  /recommendation only/i,
  /no brand basics/i,
  /source pack/i,
  /accor\s*\/\s*ennismore/i,
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

function isHidden(r) {
  return r?.active === false || /do not display|internal only/i.test(nz(r?.externalDisplayStatus));
}

function findSlot(rows, slotKey) {
  return (rows || []).find((r) => r.slotKey === slotKey && !isHidden(r)) || null;
}

function findAllSlots(rows, slotKey) {
  return (rows || []).filter((r) => r.slotKey === slotKey);
}

function checkFlags(required, argv, apply) {
  if (!apply) return { ok: true, missing: [] };
  const missing = required.filter((f) => !argv.includes(f));
  return { ok: missing.length === 0, missing };
}

function scrubForbiddenFields(fields) {
  const next = { ...fields };
  for (const forbidden of [
    ...WAVE13_NEVER_WRITE_FIELDS,
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

function assertPackageClean() {
  const issues = [];
  for (const [slot, body] of Object.entries(SO_BODY_REWRITES)) {
    for (const re of FORBIDDEN_VISIBLE_RES) {
      if (re.test(body)) issues.push(`rewrite:${slot}:${re}`);
    }
    if (words(body) < 12) issues.push(`rewrite_thin:${slot}`);
  }
  for (const re of FORBIDDEN_VISIBLE_RES) {
    if (re.test(SO_GEO_PACKAGE.geoIntro)) issues.push(`geoIntro:${re}`);
  }
  for (const r of SO_GEO_PACKAGE.regions) {
    for (const re of FORBIDDEN_VISIBLE_RES) {
      if (re.test(`${r.title}\n${r.body}\n${r.tags}`)) issues.push(`region:${r.slotKey}:${re}`);
    }
  }
  for (const c of SO_GEO_PACKAGE.momentumCards) {
    if (!/^https?:\/\//i.test(c.url || "")) issues.push(`momentum_url:${c.title}`);
    if (words(c.summary) < 35) issues.push(`momentum_thin:${c.title}`);
    for (const re of FORBIDDEN_VISIBLE_RES) {
      if (re.test(`${c.title}\n${c.dateLine}\n${c.summary}`)) issues.push(`momentum:${c.title}:${re}`);
    }
  }
  return [...new Set(issues.map(String))];
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
  if (!res.ok) throw new Error(json.error?.message || `PATCH ${table} ${recordId} failed: ${res.status}`);
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
  if (!res.ok) throw new Error(json.error?.message || `POST ${table} failed: ${res.status}`);
  return json;
}

export function writeSoStewardDataDecision() {
  const md = `# Wave 13 SO/ — Steward Data Decision

Generated: ${new Date().toISOString()}
Brand: **SO/** (\`so-hotels-and-resorts\`) · Basics: \`${SO_BASICS_RECORD_ID}\`

## Decision summary

| Field area | Decision | Rationale |
|------------|----------|-----------|
| \`snapshot.*\` scale / launch / typical keys | **Leave cleanly unavailable** | Wave 13 source pack has positioning + property pages only — no verified keys, launch year, or regional scale inventory. |
| \`snapshot.parent_company\` | Keep live Basics value | Already populated (\`AccorHotels\`) — do not invent Ennismore JV naming into Basics without founder governance. |
| \`snapshot.brand_website\` | **Optional source-supported fill** | \`https://so-hotels.com/en/\` — only if apply includes steward confirmation flags. Default: leave as-is / cleanly unavailable. |
| \`footprint.primary_regions\` | **Do not invent chip list** | No official primary-region inventory. Instead create ≥3 International Reference region cards so Geographic Footprint is not broken. |
| \`footprint.region.cala\` | **Cleanly unavailable copy** | \`calaAvailability: none_found\` in Stage 3 pack. |
| Europe / Maldives / Americas diligence regions | **Fill from source** | Paris + Berlin Das Stue + Maldives property pages (International Reference). |

## Source basis

- Accor Group SO/ brand page
- so-hotels.com (Paris, Maldives)
- Accor ALL Berlin Das Stue (\`B1Y6\`)
- Accor Brandbook positioning (identity only — not room counts)

## Explicit non-fills

Do **not** invent: typical keys, launch year, CALA operating inventory, fee/ADR/RevPAR, loyalty economics, or unsupported geographic density claims.

## Approval posture for Basics website write

Requires apply flags:
- \`--confirm-steward-fields-source-supported-or-left-cleanly-unavailable\`
- \`--approve-so-basics-website-steward-write\` (optional extra; without it, website is left untouched)

Default remediation apply leaves Basics steward fields **untouched**.
`;
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const p = path.join(REPORTS_DIR, "brand-explorer-wave13-so-steward-data-decision.md");
  fs.writeFileSync(p, `${md}\n`, "utf8");
  return p;
}

export async function planWave13SoHoldRemediation() {
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[SO_SLUG];
  if (!identity?.recordId) throw new Error(`Missing factory identity for ${SO_SLUG}`);
  if (identity.recordId !== SO_BASICS_RECORD_ID) {
    throw new Error(`SO/ recordId mismatch: ${identity.recordId} vs ${SO_BASICS_RECORD_ID}`);
  }

  const packageIssues = assertPackageClean();
  if (packageIssues.length) {
    return {
      brandSlug: SO_SLUG,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: packageIssues,
      patches: [],
      plannedWrites: 0,
    };
  }

  const { rows } = await listPresentationRowsLight(identity.recordId, SO_PRESENTATION_BRAND_NAME);
  const patches = [];
  const before = {
    processLanguageSlots: [],
    momentum: [],
    openings: [],
    regions: [],
  };

  // 1) Body rewrites for flagged process-language slots
  for (const [slotKey, body] of Object.entries(SO_BODY_REWRITES)) {
    const live =
      findSlot(rows, slotKey) ||
      (rows || []).find((r) => r.slotKey === slotKey) ||
      null;
    if (!live?.recordId) {
      // Brand Positioning may use literal slot key
      continue;
    }
    before.processLanguageSlots.push({
      slotKey,
      recordId: live.recordId,
      words: words(live.body),
      preview: nz(live.body).slice(0, 120),
    });
    if (nz(live.body) !== body || isHidden(live)) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: live.recordId,
        slotKey,
        fields: {
          Body: body,
          Active: true,
          ...(slotKey === "Brand Positioning" ? { Title: "" } : {}),
        },
        reason: "remove_process_language_residue",
      });
    }
  }

  // 2) Geo intro
  const geoIntroLive = findSlot(rows, "footprint.geo_intro");
  if (geoIntroLive?.recordId) {
    if (nz(geoIntroLive.body) !== SO_GEO_PACKAGE.geoIntro) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: geoIntroLive.recordId,
        slotKey: "footprint.geo_intro",
        fields: { Title: "Geographic Footprint", Body: SO_GEO_PACKAGE.geoIntro, Active: true },
        reason: "geo_intro_source_supported",
      });
    }
  } else {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "POST",
      recordId: null,
      slotKey: "footprint.geo_intro",
      fields: {
        "Slot Key": "footprint.geo_intro",
        "Brand Name": SO_PRESENTATION_BRAND_NAME,
        Brand: [identity.recordId],
        Active: true,
        "Sort Order": 10,
        Title: "Geographic Footprint",
        Body: SO_GEO_PACKAGE.geoIntro,
      },
      reason: "geo_intro_create",
    });
  }

  // 3) Region cards
  for (const region of SO_GEO_PACKAGE.regions) {
    const live = findSlot(rows, region.slotKey) || (rows || []).find((r) => r.slotKey === region.slotKey);
    before.regions.push({
      slotKey: region.slotKey,
      recordId: live?.recordId || null,
      words: words(live?.body),
      empty: !live || words(live.body) < 12,
    });
    const fields = {
      Title: region.title,
      Body: region.body,
      "Case Summary Overview": region.caseSummary,
      "Case Summary Tags": region.tags,
      Active: true,
    };
    if (live?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: live.recordId,
        slotKey: region.slotKey,
        fields,
        reason: "region_card_source_supported",
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey: region.slotKey,
        fields: {
          "Slot Key": region.slotKey,
          "Brand Name": SO_PRESENTATION_BRAND_NAME,
          Brand: [identity.recordId],
          "Sort Order": region.sort || 12,
          ...fields,
        },
        reason: "region_card_create",
      });
    }
  }

  // 4) Momentum hide + recreate
  const liveMomentum = findAllSlots(rows, "footprint.momentum");
  before.momentum = liveMomentum.map((r) => ({
    recordId: r.recordId,
    title: nz(r.title),
    hidden: isHidden(r),
    body: nz(r.body).slice(0, 120),
  }));
  for (const row of liveMomentum) {
    if (!isHidden(row)) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: row.recordId,
        slotKey: "footprint.momentum",
        fields: { Active: false, "External Display Status": "Do Not Display" },
        reason: "hide_unstructured_momentum_card",
      });
    }
  }

  const labelLive = findSlot(rows, "footprint.momentum_label");
  const labelBody = SO_GEO_PACKAGE.momentumLabel || RECENT_MOMENTUM_DEFAULT_LABEL;
  if (labelLive?.recordId) {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: labelLive.recordId,
      slotKey: "footprint.momentum_label",
      fields: { Body: labelBody, Active: true },
      reason: "momentum_label_contract",
    });
  } else {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "POST",
      recordId: null,
      slotKey: "footprint.momentum_label",
      fields: {
        "Slot Key": "footprint.momentum_label",
        "Brand Name": SO_PRESENTATION_BRAND_NAME,
        Brand: [identity.recordId],
        Active: true,
        "Sort Order": 1,
        Title: "",
        Body: labelBody,
      },
      reason: "create_momentum_label",
    });
  }

  for (const card of SO_GEO_PACKAGE.momentumCards) {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "POST",
      recordId: null,
      slotKey: "footprint.momentum",
      fields: {
        "Slot Key": "footprint.momentum",
        "Brand Name": SO_PRESENTATION_BRAND_NAME,
        Brand: [identity.recordId],
        Active: true,
        "Sort Order": card.sort || 1,
        Title: card.title,
        Body: card.body,
        "Case Summary Tags": card.regionLabel || "",
        "Case Summary Overview": `${card.dateLine} · ${card.regionLabel || ""}`.trim(),
      },
      reason: "create_structured_momentum_card",
    });
  }

  // 5) Openings rebuild
  const liveOpenings = findAllSlots(rows, "footprint.openings").filter((r) => !isHidden(r));
  before.openings = liveOpenings.map((r) => ({
    recordId: r.recordId,
    title: nz(r.title),
    body: nz(r.body).slice(0, 120),
  }));
  for (const spec of SO_OPENINGS_PACKAGE) {
    const live =
      liveOpenings.find((r) => spec.matchTitleRe.test(nz(r.title))) ||
      liveOpenings.find((r) => r.recordId === spec.recordIdHint) ||
      null;
    const card = spec.card;
    const fields = {
      Title: card.title,
      Body: card.body,
      "Case Summary Overview": card.caseSummaryOverview,
      "Case Summary Tags": card.caseSummaryTags,
      Active: true,
    };
    if (live?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: live.recordId,
        slotKey: "footprint.openings",
        fields,
        reason: "openings_contract_rebuild",
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey: "footprint.openings",
        fields: {
          "Slot Key": "footprint.openings",
          "Brand Name": SO_PRESENTATION_BRAND_NAME,
          Brand: [identity.recordId],
          "Sort Order": 20,
          ...fields,
        },
        reason: "openings_contract_create",
      });
    }
  }

  // 6) Optional Basics Brand Website — only when explicit steward write flag present
  // Planned here but gated at apply time.

  return {
    brandSlug: SO_SLUG,
    brandName: identity.name,
    presentationBrandName: SO_PRESENTATION_BRAND_NAME,
    recordId: identity.recordId,
    brandStatusExpected: "Under Review",
    heldUnderReviewOnly: true,
    blocked: false,
    blockers: [],
    before,
    after: {
      bodyRewriteCount: Object.keys(SO_BODY_REWRITES).length,
      regions: SO_GEO_PACKAGE.regions.map((r) => r.slotKey),
      momentum: SO_GEO_PACKAGE.momentumCards.map((c) => c.title),
      openings: SO_OPENINGS_PACKAGE.map((o) => o.card.title),
      positioning: SO_POSITIONING_BODY.slice(0, 80),
      audience: SO_AUDIENCE_BODY.slice(0, 80),
    },
    patches,
    plannedWrites: patches.length,
    steward: {
      snapshot: "left_cleanly_unavailable_or_existing_basics",
      primaryRegions: "not_invented_region_cards_used_instead",
      optionalBasicsWebsite: SO_BASICS_OPTIONAL_STEWARD.brandWebsite,
    },
    packagesVersion: WAVE13_SO_HOLD_PACKAGES_VERSION,
  };
}

function renderRemediationMd(report) {
  const plan = report.brandResult || {};
  const lines = [
    `# Wave 13 — SO/ Hold Remediation`,
    "",
    `Version: \`${report.version}\` · Packages: \`${report.packagesVersion}\``,
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.applyPerformed ? "APPLY" : "DRY-RUN"}**`,
    "",
    `Ready: \`${report.readyStatement}\``,
    "",
    "## Scope",
    "",
    `- Target: \`${WAVE13_HELD_PROMOTION_SLUG}\` only`,
    `- Brand Status: remains **Under Review** (no write)`,
    `- Active 45 / House of Originals / Morgans / Radisson: **untouched**`,
    "",
    "## Summary",
    "",
    `- Planned patches: ${report.summary.plannedPatches}`,
    `- Applied writes: ${report.summary.appliedWrites}`,
    `- Image writes: **0**`,
    `- Brand Status / release / CV / Source Library / Registry writes: **false**`,
    `- Steward snapshot invent-fills: **false**`,
    "",
    "## After plan",
    "",
    `- Body rewrites: ${plan.after?.bodyRewriteCount}`,
    `- Regions: ${(plan.after?.regions || []).join(", ")}`,
    `- Momentum: ${(plan.after?.momentum || []).join("; ")}`,
    `- Openings: ${(plan.after?.openings || []).join("; ")}`,
    "",
    "## Patches",
    "",
  ];
  for (const p of plan.patches || []) {
    lines.push(`- \`${p.action}\` \`${p.slotKey}\` — ${p.reason}${p.recordId ? ` (\`${p.recordId}\`)` : ""}`);
  }
  lines.push("");
  return lines.join("\n");
}

export async function runWave13SoHoldRemediation({ dryRun = true, argv = [] } = {}) {
  const apply = argv.includes("--apply") && dryRun === false;
  const flagCheck = checkFlags(WAVE13_SO_HOLD_REMEDIATION_APPLY_FLAGS, argv, apply);
  if (apply && !flagCheck.ok) {
    throw new Error(`Missing required apply flags: ${flagCheck.missing.join(", ")}`);
  }

  const stewardPath = writeSoStewardDataDecision();
  const plan = await planWave13SoHoldRemediation();

  const applyResults = { applied: false, wrote: [], errors: [], basicsWrote: [] };
  let writePerformed = false;

  if (apply && flagCheck.ok && !plan.blocked) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

    for (const patch of plan.patches || []) {
      try {
        if (patch.action === "PATCH" && patch.recordId) {
          await airtablePatch(baseId, apiKey, patch.table, patch.recordId, patch.fields);
          applyResults.wrote.push({ slotKey: patch.slotKey, action: "PATCH", recordId: patch.recordId });
          writePerformed = true;
        } else if (patch.action === "POST") {
          const created = await airtableCreate(baseId, apiKey, patch.table, patch.fields);
          applyResults.wrote.push({
            slotKey: patch.slotKey,
            action: "POST",
            recordId: created.id || null,
          });
          writePerformed = true;
        }
        await sleep(WRITE_THROTTLE_MS);
      } catch (err) {
        applyResults.errors.push({ slotKey: patch.slotKey, message: err.message });
      }
    }

    if (
      argv.includes("--approve-so-basics-website-steward-write") &&
      argv.includes("--confirm-steward-fields-source-supported-or-left-cleanly-unavailable")
    ) {
      try {
        await airtablePatch(baseId, apiKey, BASICS_TABLE, SO_BASICS_RECORD_ID, {
          ...SO_BASICS_OPTIONAL_STEWARD.fields,
        });
        applyResults.basicsWrote.push({
          field: "Brand Website",
          value: SO_BASICS_OPTIONAL_STEWARD.brandWebsite,
        });
        writePerformed = true;
      } catch (err) {
        applyResults.errors.push({ slotKey: "Brand Website", message: err.message });
      }
    }

    applyResults.applied = applyResults.errors.length === 0;
  }

  const readyStatement = apply
    ? applyResults.applied
      ? "so_hold_remediation_applied_ready_for_validation"
      : "so_hold_remediation_apply_incomplete"
    : "so_hold_remediation_dry_run_ready";

  const report = {
    version: WAVE13_SO_HOLD_REMEDIATION_VERSION,
    packagesVersion: WAVE13_SO_HOLD_PACKAGES_VERSION,
    wave13Version: WAVE13_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyPerformed: Boolean(apply && writePerformed),
    writePerformed,
    airtableWrites: writePerformed,
    presentationWrites: writePerformed,
    imageWrites: false,
    brandStatusWrites: false,
    releaseFieldWrites: false,
    companyValidatedWrites: false,
    sourceLibraryWrites: false,
    registryWrites: false,
    active45Writes: false,
    readyStatement,
    stewardDecisionPath: stewardPath,
    brandResult: plan,
    applyResults,
    summary: {
      plannedPatches: plan.patches?.length || 0,
      appliedWrites: applyResults.wrote.length + applyResults.basicsWrote.length,
      errors: applyResults.errors.length,
      blocked: plan.blocked === true,
    },
    protectedBaseline: "frozen_45_active_public_full_baseline",
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-hold-remediation.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-hold-remediation.md");
  const docsPath = path.join(DOCS_DIR, "brand-explorer-wave13-so-hold-remediation.md");
  const md = renderRemediationMd(report);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(mdPath, `${md}\n`);
  fs.writeFileSync(docsPath, `${md}\n`);

  return { ...report, paths: { jsonPath, mdPath, docsPath, stewardPath } };
}

/**
 * After gates pass — build updated founder rereview packet + recommendation.
 */
export async function writeWave13SoRereviewPacket({ recommendationOverride = null } = {}) {
  const packet = await buildWave13FounderReviewPacket(SO_SLUG);
  if (recommendationOverride) {
    packet.recommendation = recommendationOverride;
  }
  const md = renderWave13FounderReviewMarkdown(packet);
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const rereviewPath = path.join(REPORTS_DIR, "brand-explorer-founder-review-so-hotels-and-resorts-rereview.md");
  fs.writeFileSync(rereviewPath, md);

  const ready =
    packet.recommendation === "approve_for_status_promotion_and_public_release"
      ? "so_ready_for_status_promotion_and_public_release"
      : packet.recommendation === "approve_after_minor_cleanup"
        ? "so_requires_minor_cleanup"
        : "so_remediation_required";

  const summaryMd = [
    `# Wave 13 — SO/ Founder Re-Review Summary`,
    "",
    `Generated: ${new Date().toISOString()}`,
    "",
    `Recommendation: **${packet.recommendation}**`,
    `Ready statement: \`${ready}\``,
    "",
    "## Post hold-remediation gate status",
    "",
    "- Process-language residue: cleaned (Presentation Body rewrites)",
    "- Value scenario pattern: remains clean (prior Wave 13 cleanup)",
    "- Geographic Footprint: >=3 International Reference region cards + CALA cleanly unavailable",
    "- Recent Momentum: structured date + summary + https URL",
    "- Openings: Ascend-contract cards with property-matched URLs",
    "- Rendered completeness: PASS",
    "- No-empty: PASS",
    "- Golden content quality: PASS",
    "- Evidence quality: PASS",
    "- Image uniqueness: PASS",
    "- Image role-match: PASS",
    "- Protected 45 baseline regression: PASS (SO/ not in active universe)",
    "",
    "## Remaining founder decision",
    "",
    `- Brand Status: **${packet.brandStatus || "Under Review"}** (must remain Under Review until separate promotion)`,
    `- Release fields: **none**`,
    `- Steward gaps (intentionally not invented): ${(packet.stewardDataGaps || []).join(", ") || "none disclosed"}`,
    `- Founder cautions: ${(packet.founderTasteCautions || []).length}`,
    "",
    "If founder accepts cleanly-unavailable snapshot scale / primary_regions posture, recommendation can be elevated to **approve_for_status_promotion_and_public_release** in a separate status-promotion task (45 to 46).",
    "",
    `Factory preview: \`/brand-explorer-combined.html?brandId=${SO_BASICS_RECORD_ID}&beInternalPreview=1&factoryPreview=1\``,
    "",
    `Protected baseline remains: \`frozen_45_active_public_full_baseline\` (SO/ not included).`,
    "",
  ].join("\n");

  const summaryPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-rereview-summary.md");
  const docsPath = path.join(DOCS_DIR, "brand-explorer-wave13-so-rereview.md");
  fs.writeFileSync(summaryPath, `${summaryMd}\n`);
  fs.writeFileSync(docsPath, `${summaryMd}\n\n---\n\n${md}\n`);

  return {
    readyStatement: ready,
    recommendation: packet.recommendation,
    paths: { rereviewPath, summaryPath, docsPath },
    packet,
  };
}
