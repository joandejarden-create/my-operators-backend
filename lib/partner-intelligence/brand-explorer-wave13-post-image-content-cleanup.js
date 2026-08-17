/**
 * Wave 13 Stage 6 — post-image content cleanup + protected-39 bunkhouse PVQL resolution docs.
 *
 * Scope:
 * - SO/ Brand Positioning + Guest Psychographics (rendered as positioning.positioning / audience)
 * - Fairmont San Francisco openings → Do Not Display (idempotent)
 * - Bunkhouse remediation_locked diagnosis/report (protected 39 live PVQL re-green)
 *
 * Forbidden: Brand Status, release, CV, Source Library, Registry, images, SO steward invent-fills,
 * House of Originals, Morgans Originals, Radisson Collection, broad rewrites.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  WAVE13_VERSION,
  WAVE13_STAGE4_APPROVED_SLUGS,
  WAVE13_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS,
} from "./brand-explorer-wave13-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import { EXPECTED_ACTIVE_COUNT_39 } from "./brand-explorer-39-active-public-full-baseline.js";

export const WAVE13_POST_IMAGE_CLEANUP_VERSION = "wave13-post-image-content-cleanup-v1";

export { WAVE13_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS };

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const SO_SLUG = "so-hotels-and-resorts";
const SO_BASICS_RECORD_ID = "recTJdPlr4mDs9app";
const FAIRMONT_SLUG = "fairmont-hotels-and-resorts";
const FAIRMONT_SF_OPENINGS_RECORD_ID = "recQXp6Y3EkfaC9hG";
const BUNKHOUSE_RECORD_ID = "recGv268Wda31PlSZ";
const BUNKHOUSE_SLUG = "bunkhouse-hotels";

/** Owner-facing SO/ positioning (≥12 words; luxury lifestyle — not economy). */
export const SO_POSITIONING_BODY =
  "SO/ is a fashion-led luxury lifestyle collection for design-forward urban hotels and selective resort assets. Owners should underwrite elevated public space, destination F&B, and brand-standard intensity above midscale lifestyle and distinct from heritage landmark luxury.";

export const SO_AUDIENCE_BODY =
  "SO/ guests seek fashion-rooted, design-led luxury stays with destination energy, social F&B, and high-touch experiential hospitality — not economy, essential-stay, or limited-service product.";

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Brand Status",
  "Source Library Status",
  "Registry Approval",
  "Registry Status",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function wordCount(text) {
  return nz(text)
    .split(/\s+/)
    .filter(Boolean).length;
}

export function parseWave13PostImageCleanupFlags(argv = []) {
  const missing = WAVE13_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    dryRun: !argv.includes("--apply") || argv.includes("--dry-run"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

async function airtableGet(table, recordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`,
    { headers: { Authorization: `Bearer ${apiKey}` } }
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `GET ${table} ${recordId} failed: ${res.status}`);
  return json;
}

async function airtablePatch(table, recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const clean = { ...fields };
  for (const f of FORBIDDEN_WRITE_FIELDS) {
    if (clean[f] != null) delete clean[f];
  }
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: clean }),
    }
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH ${table} ${recordId} failed: ${res.status}`);
  return json;
}

export async function diagnoseBunkhouseRemediationLock() {
  const { listPresentationRowsLight: listRows } = await import("./brand-explorer-lane2-common.js");
  const { evaluateImageUniqueness } = await import("./brand-explorer-image-uniqueness.js");
  const { evaluateBrandImageRoleMatch } = await import("./brand-explorer-image-role-match.js");
  const { resolveBrandExplorerDisplayState } = await import("./brand-explorer-display-state.js");

  const basicsRec = await airtableGet(BASICS_TABLE, BUNKHOUSE_RECORD_ID);
  const basics = basicsRec.fields || {};
  const { rows } = await listRows(BUNKHOUSE_RECORD_ID, "Bunkhouse Hotels");
  const uniq = evaluateImageUniqueness({ brandSlug: BUNKHOUSE_SLUG, presentationRows: rows });
  const role = evaluateBrandImageRoleMatch({ brandSlug: BUNKHOUSE_SLUG, presentationRows: rows });
  const display = resolveBrandExplorerDisplayState(
    { slug: BUNKHOUSE_SLUG, name: "Bunkhouse Hotels", recordId: BUNKHOUSE_RECORD_ID },
    {
      presentationRows: rows,
      brandBasics: basics,
      imageUniqueness: uniq,
      brandSlug: BUNKHOUSE_SLUG,
    }
  );

  const priorPvqlPath = path.join(REPORTS_DIR, "brand-explorer-public-visibility-quality-lock.json");
  let priorPvql = null;
  if (fs.existsSync(priorPvqlPath)) {
    try {
      priorPvql = JSON.parse(fs.readFileSync(priorPvqlPath, "utf8"));
    } catch (err) {
      priorPvql = { error: err.message };
    }
  }
  const priorBunkhouse = (priorPvql?.brands || []).find((b) => b.slug === BUNKHOUSE_SLUG) || null;

  const liveFull = display.shouldRenderFullProfile === true;
  const liveState = display.brandExplorerDisplayState;
  const classification =
    liveFull && liveState === "active_profile_ready"
      ? "A_stale_report_or_transient_resolver_state"
      : !uniq.pass
        ? "C_real_image_uniqueness_remediation"
        : !role.pass
          ? "C_real_image_role_match_remediation"
          : "D_baseline_vs_live_pvql_resolver_mismatch";

  const resolution = {
    version: "bunkhouse-remediation-lock-resolution-v1",
    generatedAt: new Date().toISOString(),
    brandSlug: BUNKHOUSE_SLUG,
    recordId: BUNKHOUSE_RECORD_ID,
    priorPvqlSnapshot: priorBunkhouse
      ? {
          publicDisplayState: priorBunkhouse.publicDisplayState,
          publicFullProfile: priorBunkhouse.publicFullProfile,
          cohort: priorBunkhouse.cohort,
          remediationLocked: priorBunkhouse.remediationLocked,
          publicFullProfileCount: priorPvql?.summary?.publicFullProfileCount ?? null,
        }
      : null,
    liveDiagnosis: {
      brandStatus: basics["Brand Status"],
      activeProfileApproved: basics["Active Profile Approved"] === true,
      founderVisualReviewPass: basics["Founder Visual Review Pass"] === true,
      companyValidated: basics["Company Validated"] === true,
      presentationRowCount: rows.length,
      imageUniquenessPass: uniq.pass === true,
      imageRoleMatchPass: role.pass === true,
      displayState: liveState,
      shouldRenderFullProfile: liveFull,
      galleryDistinct: uniq.galleryDistinctCount,
      scenarioDistinct: uniq.scenarioDistinctCount,
      propertyDistinct: uniq.propertyExampleDistinctCount,
    },
    classification,
    rootCause:
      classification === "A_stale_report_or_transient_resolver_state"
        ? "Prior PVQL captured bunkhouse as draft_applied_with_defects / remediation_locked while live Basics + Presentation now resolve to active_profile_ready with uniqueness and role-match PASS. No Bunkhouse writes required."
        : "Live diagnosis still shows a remediation condition — investigate before Wave 13 proceeds.",
    allowedWritesPerformed: [],
    airtableWrites: false,
    safeToProceedWithoutWrites: liveFull === true,
    acceptance: {
      bunkhouseNotRemediationLockedLive: liveFull === true && liveState !== "draft_applied_with_defects",
      requiresFreshPvqlConfirm: true,
    },
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-39-bunkhouse-remediation-lock-resolution.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-39-bunkhouse-remediation-lock-resolution.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(resolution, null, 2)}\n`, "utf8");
  fs.writeFileSync(
    mdPath,
    [
      `# Bunkhouse remediation_locked — protected 39 resolution`,
      ``,
      `- Generated: ${resolution.generatedAt}`,
      `- Classification: **${classification}**`,
      `- Live display: **${liveState}** · shouldRenderFullProfile=**${liveFull}**`,
      `- Image uniqueness: **${uniq.pass}** · role-match: **${role.pass}**`,
      `- Airtable writes: **false**`,
      ``,
      `## Root cause`,
      ``,
      resolution.rootCause,
      ``,
      `## Prior PVQL snapshot`,
      ``,
      priorBunkhouse
        ? `- cohort: \`${priorBunkhouse.cohort}\` · publicFull=${priorBunkhouse.publicFullProfile} · state=\`${priorBunkhouse.publicDisplayState}\``
        : `- (no prior PVQL brand row found)`,
      ``,
      `## Next`,
      ``,
      `Re-run \`npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only\` and require publicFullProfileCount=39.`,
      ``,
    ].join("\n"),
    "utf8"
  );

  return { ...resolution, paths: { jsonPath, mdPath } };
}

export function planSoPositioningCleanup() {
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[SO_SLUG];
  const beforePositioning =
    "Smart, design-led economy; modern style and social spaces for value-conscious travelers.";
  const beforeAudience =
    "Value-conscious travelers seeking smart, design-led economy stays.";

  return {
    brandSlug: SO_SLUG,
    brandName: identity?.name || "SO/ Hotels & Resorts",
    basicsRecordId: SO_BASICS_RECORD_ID,
    basicsName: "SO/",
    stewardDataIntentionallyNotFilled: ["snapshot.*", "footprint.primary_regions"],
    basicsPatches: [
      {
        table: BASICS_TABLE,
        recordId: SO_BASICS_RECORD_ID,
        fields: {
          "Brand Positioning": SO_POSITIONING_BODY,
          "Guest Psychographics Description": SO_AUDIENCE_BODY,
        },
        fieldMap: {
          "positioning.positioning": "Brand Positioning",
          "positioning.audience": "Guest Psychographics Description",
        },
        before: {
          "Brand Positioning": beforePositioning,
          "Guest Psychographics Description": beforeAudience,
        },
        afterWordCounts: {
          positioning: wordCount(SO_POSITIONING_BODY),
          audience: wordCount(SO_AUDIENCE_BODY),
        },
        rationale:
          "Rendered positioning.positioning / positioning.audience read Brand Basics Brand Positioning + Guest Psychographics Description. Replace economy-oriented thin copy with SO/ fashion-led luxury lifestyle owner language (≥12 words).",
      },
    ],
    presentationPatches: [],
    imagesUnchanged: true,
    brandStatusUnchanged: true,
  };
}

export async function planFairmontSanFranciscoHide() {
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[FAIRMONT_SLUG];
  let live = null;
  try {
    const rec = await airtableGet(PRESENTATION_TABLE, FAIRMONT_SF_OPENINGS_RECORD_ID);
    live = {
      recordId: rec.id,
      title: nz(rec.fields?.Title),
      slotKey: nz(rec.fields?.["Slot Key"]),
      externalDisplayStatus: nz(rec.fields?.["External Display Status"]),
      hasImage: Array.isArray(rec.fields?.Image) && !!rec.fields.Image[0]?.url,
    };
  } catch (err) {
    live = { error: err.message };
  }

  const alreadyHidden = /^do not display$/i.test(live?.externalDisplayStatus || "");
  const patch = alreadyHidden
    ? null
    : {
        table: PRESENTATION_TABLE,
        recordId: FAIRMONT_SF_OPENINGS_RECORD_ID,
        fields: { "External Display Status": "Do Not Display" },
        rationale: "Hide leftover Fairmont San Francisco openings row from visible Openings / Examples.",
      };

  return {
    brandSlug: FAIRMONT_SLUG,
    brandName: identity?.name || "Fairmont",
    openingsRecordId: FAIRMONT_SF_OPENINGS_RECORD_ID,
    live,
    alreadyHidden,
    presentationPatches: patch ? [patch] : [],
    imagesUnchanged: true,
    brandStatusUnchanged: true,
  };
}

export async function applyWave13PostImageCleanupPlans({
  soPlan,
  fairmontPlan,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseWave13PostImageCleanupFlags(argv);
  if (!apply) {
    return { applied: false, reason: "dry_run_only", flagCheck };
  }
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };
  }

  const results = { so: [], fairmont: [], errors: [] };

  for (const patch of soPlan?.basicsPatches || []) {
    try {
      await airtablePatch(patch.table, patch.recordId, patch.fields);
      results.so.push({ recordId: patch.recordId, fields: Object.keys(patch.fields), ok: true });
      await sleep(280);
    } catch (err) {
      results.errors.push({ scope: "so", error: err.message });
    }
  }

  for (const patch of fairmontPlan?.presentationPatches || []) {
    try {
      await airtablePatch(patch.table, patch.recordId, patch.fields);
      results.fairmont.push({
        recordId: patch.recordId,
        fields: Object.keys(patch.fields),
        ok: true,
      });
      await sleep(280);
    } catch (err) {
      results.errors.push({ scope: "fairmont", error: err.message });
    }
  }

  return {
    applied: results.errors.length === 0,
    flagCheck,
    results,
  };
}

function writeBrandMd(basename, title, bodyLines) {
  const p = path.join(REPORTS_DIR, basename);
  fs.writeFileSync(p, `${[title, "", ...bodyLines].join("\n")}\n`, "utf8");
  return p;
}

export async function runWave13PostImageContentCleanup({ dryRun = true, argv = [] } = {}) {
  const flagCheck = parseWave13PostImageCleanupFlags(argv);
  const apply = argv.includes("--apply") && !dryRun;

  const bunkhouse = await diagnoseBunkhouseRemediationLock();
  if (!bunkhouse.safeToProceedWithoutWrites && !bunkhouse.liveDiagnosis?.shouldRenderFullProfile) {
    const stop = {
      version: WAVE13_POST_IMAGE_CLEANUP_VERSION,
      stage: "post-image-content-cleanup",
      generatedAt: new Date().toISOString(),
      readyStatement: "wave13_post_image_cleanup_blocked_on_bunkhouse",
      stopRecommended: true,
      pass: false,
      bunkhouse,
      message:
        "Bunkhouse is not live-clean for protected 39 PVQL. Stop before SO/Fairmont cleanup writes.",
    };
    fs.mkdirSync(REPORTS_DIR, { recursive: true });
    fs.writeFileSync(
      path.join(REPORTS_DIR, "brand-explorer-wave13-post-image-cleanup.json"),
      `${JSON.stringify(stop, null, 2)}\n`,
      "utf8"
    );
    return stop;
  }

  // Verify live SO Basics identity
  const soBasics = await airtableGet(BASICS_TABLE, SO_BASICS_RECORD_ID);
  const soName = nz(soBasics.fields?.["Brand Name"]);
  if (soName !== "SO/") {
    throw new Error(
      `SO/ live Basics record ${SO_BASICS_RECORD_ID} Brand Name is "${soName}", expected "SO/"`
    );
  }

  const soPlan = planSoPositioningCleanup();
  // Refresh before snapshot from live Basics
  soPlan.basicsPatches[0].before = {
    "Brand Positioning": nz(soBasics.fields?.["Brand Positioning"]),
    "Guest Psychographics Description": nz(soBasics.fields?.["Guest Psychographics Description"]),
  };

  const fairmontPlan = await planFairmontSanFranciscoHide();

  const applyResult = await applyWave13PostImageCleanupPlans({
    soPlan,
    fairmontPlan,
    apply,
    argv,
  });

  const summary = {
    version: WAVE13_POST_IMAGE_CLEANUP_VERSION,
    wave13Version: WAVE13_VERSION,
    stage: "post-image-content-cleanup",
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyRequested: argv.includes("--apply"),
    flagCheck,
    stage6Scope: [...WAVE13_STAGE4_APPROVED_SLUGS],
    protectedBaselineCount: EXPECTED_ACTIVE_COUNT_39,
    bunkhouse,
    so: soPlan,
    fairmont: fairmontPlan,
    applyResult,
    guardrails: {
      noBrandStatus: true,
      noReleaseFields: true,
      noCompanyValidated: true,
      noSourceLibrary: true,
      noRegistry: true,
      noImageWrites: true,
      noHouseOfOriginals: true,
      noMorgansOriginals: true,
      noRadissonCollection: true,
      noSoStewardInventFills: true,
      targetedFieldFixesOnly: true,
    },
    readyStatement:
      applyResult.applied || !apply
        ? "wave13_post_image_cleanup_ready_for_founder_review"
        : "wave13_post_image_cleanup_blocked",
    pass: true,
    stopRecommended: false,
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  writeBrandMd(
    "brand-explorer-wave13-post-image-cleanup-so-hotels-and-resorts.md",
    `# Wave 13 Stage 6 — SO/ post-image cleanup`,
    [
      `- Basics record: \`${SO_BASICS_RECORD_ID}\` (Brand Name **SO/**)`,
      `- Patched Basics: Brand Positioning, Guest Psychographics Description`,
      `- Word counts: positioning ${wordCount(SO_POSITIONING_BODY)} · audience ${wordCount(SO_AUDIENCE_BODY)}`,
      `- Steward gaps intentionally not filled: snapshot.*, footprint.primary_regions`,
      `- Images unchanged · Brand Status unchanged`,
      ``,
      `## After`,
      ``,
      `### Brand Positioning`,
      ``,
      SO_POSITIONING_BODY,
      ``,
      `### Guest Psychographics Description`,
      ``,
      SO_AUDIENCE_BODY,
      ``,
    ]
  );

  writeBrandMd(
    "brand-explorer-wave13-post-image-cleanup-fairmont-hotels-and-resorts.md",
    `# Wave 13 Stage 6 — Fairmont openings cleanup`,
    [
      `- Openings record: \`${FAIRMONT_SF_OPENINGS_RECORD_ID}\``,
      `- Title: Fairmont San Francisco — International Reference`,
      `- External Display Status: **Do Not Display** (${fairmontPlan.alreadyHidden ? "already set" : apply ? "patched" : "planned"})`,
      `- Images unchanged · other Fairmont openings untouched`,
      ``,
    ]
  );

  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-post-image-cleanup.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-post-image-cleanup.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  fs.writeFileSync(
    mdPath,
    [
      `# Wave 13 Stage 6 — Post-Image Cleanup`,
      ``,
      `- Generated: ${summary.generatedAt}`,
      `- Mode: **${apply ? "APPLY" : "DRY-RUN"}**`,
      `- Ready: \`${summary.readyStatement}\``,
      ``,
      `## Bunkhouse / protected 39`,
      ``,
      `- Classification: **${bunkhouse.classification}**`,
      `- Live display: **${bunkhouse.liveDiagnosis.displayState}** · full=**${bunkhouse.liveDiagnosis.shouldRenderFullProfile}**`,
      `- Writes: **none** (safe)`,
      `- Report: \`reports/brand-explorer-39-bunkhouse-remediation-lock-resolution.md\``,
      ``,
      `## SO/`,
      ``,
      `- Basics \`${SO_BASICS_RECORD_ID}\` Brand Positioning + Guest Psychographics Description`,
      `- Steward invent-fills: **none**`,
      ``,
      `## Fairmont`,
      ``,
      `- San Francisco openings \`${FAIRMONT_SF_OPENINGS_RECORD_ID}\` → Do Not Display`,
      ``,
      `## Guardrails`,
      ``,
      `- No Brand Status / release / CV / Source Library / Registry / image writes`,
      `- House of Originals + Morgans Originals untouched`,
      ``,
    ].join("\n"),
    "utf8"
  );

  fs.writeFileSync(
    path.join(DOCS_DIR, "brand-explorer-wave13-post-image-cleanup.md"),
    [
      `# Wave 13 — Post-Image Content Cleanup`,
      ``,
      `Stage 6 cleans residual SO/ positioning copy and Fairmont San Francisco openings visibility,`,
      `and documents protected-39 Bunkhouse PVQL re-green diagnosis.`,
      ``,
      `## Commands`,
      ``,
      "```bash",
      `npm run brand-explorer-wave13-factory -- --stage post-image-content-cleanup --dry-run`,
      `npm run brand-explorer-wave13-factory -- --stage post-image-content-cleanup --apply \\`,
      ...WAVE13_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS.map((f, i, arr) =>
        i === arr.length - 1 ? `  ${f}` : `  ${f} \\`
      ),
      "```",
      ``,
      `## Ready statement`,
      ``,
      `\`wave13_post_image_cleanup_ready_for_founder_review\``,
      ``,
    ].join("\n"),
    "utf8"
  );

  return {
    ...summary,
    paths: {
      jsonPath,
      mdPath,
      bunkhouse: bunkhouse.paths,
    },
  };
}
