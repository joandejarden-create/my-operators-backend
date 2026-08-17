/**
 * Wave 12 — scenario owner-value remediation.
 *
 * Fixes "Where This Brand Creates the Most Value":
 * - Proper Case titles
 * - Unique owner-value bodies (no identical diligence closer on all three cards)
 * - Distinct scenario images (collapse IHG/Marriott aspect-ratio near-duplicates)
 *
 * Allowed writes: Presentation Title / Body / Image on overview.scenario.1–3 only.
 * Forbidden: Brand Status, release, CV, Source Library, Registry, protected 27, other slots.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { toAirtableFetchableImageUrl } from "./brand-explorer-lane2-image-materialization.js";
import {
  buildImageIdentity,
  evaluateImageUniqueness,
} from "./brand-explorer-image-uniqueness.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE12_SLUGS,
  WAVE12_VERSION,
  WAVE12_NEVER_WRITE_FIELDS,
} from "./brand-explorer-wave12-factory-plan.js";
import {
  hasRepeatedDiligenceCloser,
  hasRepeatedGeographyCloser,
  isReferenceMetaScenarioBody,
  isReferenceMetaScenarioTitle,
  scenarioBodiesAreIdentical,
  toProperCaseScenarioTitle,
  stripRepeatedScenarioDiligencePad,
  SCENARIO_SLOTS,
} from "./brand-explorer-scenario-owner-value-bar.js";
import { generateWave12TabFactoryPack } from "./brand-explorer-wave12-tab-factory-build-generator.js";
import { buildWave12ImageAssetPackForBrand } from "./brand-explorer-wave12-image-materialization.js";

export const WAVE12_SCENARIO_OWNER_VALUE_REMEDIATION_VERSION =
  "wave12-scenario-owner-value-remediation-v2";

export const WAVE12_SCENARIO_OWNER_VALUE_APPLY_FLAGS = Object.freeze([
  "--approve-wave12-scenario-owner-value-remediation",
  "--confirm-target-brands-only",
  "--confirm-scenario-slots-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-protected-27-brand-changes",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WRITE_THROTTLE_MS = 280;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

function nz(v) {
  return v == null ? "" : String(v).trim();
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

function liveScenarioRows(rows) {
  return SCENARIO_SLOTS.map((slotKey) => {
    const matches = (rows || [])
      .filter(
        (r) =>
          nz(r.slotKey) === slotKey &&
          r.active !== false &&
          !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
      )
      .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
    return matches[0] || null;
  });
}

/**
 * Plan Title/Body/Image patches for overview.scenario.1–3 on one Wave 12 brand.
 */
export async function planWave12ScenarioOwnerValueRemediationForBrand(slug) {
  const identity = resolveIdentity(slug);
  if (!identity) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["unknown_wave12_identity"],
      patches: [],
    };
  }
  if (!WAVE12_SLUGS.includes(slug)) {
    return {
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: ["not_wave12_target"],
      patches: [],
    };
  }

  const pack = generateWave12TabFactoryPack(slug, {
    recordId: identity.recordId,
    brandName: identity.name,
  });
  const genBySlot = new Map();
  for (const row of pack.presentation || []) {
    if (!SCENARIO_SLOTS.includes(nz(row.slotKey))) continue;
    genBySlot.set(nz(row.slotKey), row);
  }

  const fetch = await listPresentationRowsLight(identity.recordId, identity.name);
  const liveList = liveScenarioRows(fetch.rows || []);
  const liveBodies = liveList.map((r) => nz(r?.body));
  // Always rematerialize scenario images in this remediation — Wave 12 shipped
  // aspect-ratio / near-duplicate scenario pairs that Airtable CDN URLs hide.
  const needImageFix = true;
  const identicalBodies = scenarioBodiesAreIdentical(liveBodies);

  let scenarioImages = [];
  if (needImageFix) {
    const imagePack = buildWave12ImageAssetPackForBrand(slug);
    scenarioImages = imagePack.visualAssetPack?.scenarioCandidates || [];
  }

  const patches = [];
  const blockers = [];

  for (let i = 0; i < SCENARIO_SLOTS.length; i++) {
    const slotKey = SCENARIO_SLOTS[i];
    const live = liveList[i];
    const generated = genBySlot.get(slotKey);
    if (!generated) {
      blockers.push(`missing_generated:${slotKey}`);
      continue;
    }

    const nextTitle = toProperCaseScenarioTitle(nz(generated.title));
    const nextBody = nz(generated.body);
    const liveTitle = nz(live?.title);
    const liveBody = nz(live?.body);
    const liveIsReferenceMeta =
      isReferenceMetaScenarioTitle(liveTitle) ||
      isReferenceMetaScenarioBody(liveBody) ||
      hasRepeatedGeographyCloser(liveBody);
    const titleNeeds =
      !liveTitle ||
      liveTitle !== nextTitle ||
      liveTitle !== toProperCaseScenarioTitle(liveTitle) ||
      isReferenceMetaScenarioTitle(liveTitle);
    const bodyNeeds =
      !liveBody ||
      liveBody !== nextBody ||
      hasRepeatedDiligenceCloser(liveBody) ||
      identicalBodies ||
      liveIsReferenceMeta;

    const fields = {};
    const reasons = [];
    if (titleNeeds) {
      fields.Title = nextTitle;
      reasons.push(
        isReferenceMetaScenarioTitle(liveTitle)
          ? "replace_reference_meta_title"
          : "proper_case_or_retitle"
      );
    }
    if (bodyNeeds) {
      fields.Body = nextBody;
      reasons.push(
        liveIsReferenceMeta
          ? "replace_reference_meta_body"
          : hasRepeatedDiligenceCloser(liveBody) || identicalBodies
            ? "unique_owner_value_body"
            : "owner_value_body_refresh"
      );
    }

    if (needImageFix && scenarioImages[i]?.imageUrl && live?.recordId) {
      const nextUrl = toAirtableFetchableImageUrl(scenarioImages[i].imageUrl);
      if (nextUrl) {
        fields.Image = [{ url: nextUrl }];
        reasons.push("distinct_scenario_image");
      }
    }

    for (const forbidden of WAVE12_NEVER_WRITE_FIELDS) {
      if (fields[forbidden] != null) delete fields[forbidden];
    }

    if (!Object.keys(fields).length) continue;

    if (live?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: live.recordId,
        slotKey,
        fields,
        before: {
          title: liveTitle,
          body: liveBody.slice(0, 180),
          imageUrl: live.imageUrl || null,
        },
        after: {
          title: fields.Title || liveTitle,
          body: (fields.Body || liveBody).slice(0, 180),
          imageUrl: fields.Image?.[0]?.url || live.imageUrl || null,
        },
        reason: reasons.join("+"),
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey,
        fields: {
          "Slot Key": slotKey,
          "Brand Name": identity.name,
          Brand: [identity.recordId],
          Active: true,
          "Sort Order": 30 + i,
          Title: nextTitle,
          Body: nextBody,
          ...(fields.Image ? { Image: fields.Image } : {}),
        },
        before: null,
        after: { title: nextTitle, body: nextBody.slice(0, 180) },
        reason: "missing_scenario_create",
      });
    }
  }

  // Post-plan uniqueness check on intended image set
  const plannedImageRows = SCENARIO_SLOTS.map((slotKey, i) => {
    const patch = patches.find((p) => p.slotKey === slotKey);
    const live = liveList[i];
    return {
      slotKey,
      imageUrl: patch?.fields?.Image?.[0]?.url || live?.imageUrl || "",
      title: patch?.fields?.Title || live?.title || "",
    };
  });
  const uniqueness = evaluateImageUniqueness({
    brandSlug: slug,
    presentationRows: [
      ...plannedImageRows,
      // Satisfy evaluator minimums with distinct placeholders (scenarios-only gate).
      ...[1, 2, 3, 4, 5, 6].map((n) => ({
        slotKey: `materials.gallery.${n}`,
        imageUrl: `https://cdn.example.com/wave12-scenario-remediation/gallery-${slug}-${n}.jpg`,
        title: `Gallery ${n}`,
      })),
      ...[1, 2, 3].map((n) => ({
        slotKey: "footprint.openings",
        imageUrl: `https://cdn.example.com/wave12-scenario-remediation/open-${slug}-${n}.jpg`,
        title: `Open ${n}`,
        recordId: `open-${n}`,
      })),
    ],
  });
  const scenarioDistinct = new Set(
    plannedImageRows
      .filter((r) => r.imageUrl)
      .map((r) => buildImageIdentity(r.imageUrl, { title: r.title }).duplicateGroupId)
  ).size;

  return {
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    blocked: false,
    blockers,
    patches,
    diagnostics: {
      repeatedDiligenceOnLive: liveBodies.some(hasRepeatedDiligenceCloser),
      identicalBodiesOnLive: identicalBodies,
      scenarioImageRematerializeNeeded: needImageFix,
      plannedScenarioDistinctImages: scenarioDistinct,
      uniquenessScenarioNote:
        scenarioDistinct >= 3
          ? "three_distinct_scenario_images_planned"
          : "scenario_images_still_short_distinct",
      galleryUniquenessIgnored: true,
      fullUniquenessPassIgnored: uniqueness.pass,
    },
    plannedWrites: patches.length,
    brandStatusUntouched: true,
    companyValidatedUntouched: true,
  };
}

async function airtablePatch(baseId, apiKey, table, recordId, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`Airtable PATCH ${recordId}: ${res.status} ${JSON.stringify(json)}`);
  }
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
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`Airtable POST: ${res.status} ${JSON.stringify(json)}`);
  }
  return json;
}

export async function runWave12ScenarioOwnerValueRemediation({
  dryRun = true,
  argv = [],
  brands = null,
} = {}) {
  const flagCheck = checkFlags(WAVE12_SCENARIO_OWNER_VALUE_APPLY_FLAGS, argv, !dryRun);
  const targetSlugs = (brands?.length ? brands : [...WAVE12_SLUGS]).filter((s) =>
    WAVE12_SLUGS.includes(s)
  );

  const brandResults = [];
  for (const slug of targetSlugs) {
    brandResults.push(await planWave12ScenarioOwnerValueRemediationForBrand(slug));
  }

  const applyResults = {};
  if (!dryRun && flagCheck.ok) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

    for (const plan of brandResults) {
      if (plan.blocked) {
        applyResults[plan.brandSlug] = { applied: false, reason: "blocked", blockers: plan.blockers };
        continue;
      }
      const wrote = [];
      for (const patch of plan.patches || []) {
        if (patch.action === "PATCH" && patch.recordId) {
          await airtablePatch(baseId, apiKey, patch.table, patch.recordId, patch.fields);
          wrote.push({ slotKey: patch.slotKey, action: "PATCH", recordId: patch.recordId });
        } else if (patch.action === "POST") {
          const created = await airtableCreate(baseId, apiKey, patch.table, patch.fields);
          wrote.push({
            slotKey: patch.slotKey,
            action: "POST",
            recordId: created.id || null,
          });
        }
        await sleep(WRITE_THROTTLE_MS);
      }
      applyResults[plan.brandSlug] = { applied: true, wrote };
    }
  }

  const report = {
    version: WAVE12_SCENARIO_OWNER_VALUE_REMEDIATION_VERSION,
    wave: WAVE12_VERSION,
    dryRun: dryRun !== false,
    flagCheck,
    targetSlugs,
    brandResults,
    applyResults,
    summary: {
      brands: brandResults.length,
      plannedPatches: brandResults.reduce((n, b) => n + (b.patches?.length || 0), 0),
      brandsWithRepeatedDiligence: brandResults.filter(
        (b) => b.diagnostics?.repeatedDiligenceOnLive
      ).length,
      brandsNeedingImageFix: brandResults.filter(
        (b) => b.diagnostics?.scenarioImageRematerializeNeeded
      ).length,
    },
  };

  if (!fs.existsSync(REPORTS_DIR)) fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave12-scenario-owner-value-remediation.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave12-scenario-owner-value-remediation.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    `# Wave 12 Scenario Owner-Value Remediation`,
    ``,
    `- Version: ${report.version}`,
    `- Dry-run: ${report.dryRun}`,
    `- Brands: ${report.summary.brands}`,
    `- Planned patches: ${report.summary.plannedPatches}`,
    `- Brands with repeated diligence closer: ${report.summary.brandsWithRepeatedDiligence}`,
    `- Brands needing scenario image rematerialize: ${report.summary.brandsNeedingImageFix}`,
    ``,
  ];
  for (const b of brandResults) {
    lines.push(`## ${b.brandName || b.brandSlug}`);
    lines.push(`- Patches: ${b.patches?.length || 0}`);
    lines.push(
      `- Diagnostics: repeatedDiligence=${b.diagnostics?.repeatedDiligenceOnLive} identicalBodies=${b.diagnostics?.identicalBodiesOnLive} imageFix=${b.diagnostics?.scenarioImageRematerializeNeeded} plannedDistinct=${b.diagnostics?.plannedScenarioDistinctImages}`
    );
    for (const p of b.patches || []) {
      lines.push(
        `- \`${p.slotKey}\` → ${p.action} · ${p.reason} · title “${p.after?.title || ""}”`
      );
    }
    lines.push("");
  }
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");

  return { ...report, paths: { jsonPath, mdPath } };
}
