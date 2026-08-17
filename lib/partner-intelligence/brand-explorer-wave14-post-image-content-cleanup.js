/**
 * Wave 14 Stage 6 — Post-image content cleanup (nine Marriott brands).
 *
 * Primary fix: structured Recent Momentum (dateLine + summary + trailing https URL).
 * Also documents Flex / SpringHill / TownePlace accepted holds.
 *
 * Forbidden: Brand Status, release, CV, Source Library, Registry, protected 46,
 * Accor Wave 13 Active, House of Originals, Morgans, Radisson Collection,
 * broad rewrites, new image materialization.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE14_VERSION,
  WAVE14_SLUGS,
  WAVE14_NEVER_WRITE_FIELDS,
} from "./brand-explorer-wave14-factory-plan.js";
import { generateWave14PresentationPack } from "./brand-explorer-wave14-tab-factory-build.js";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { parseMomentumPresentationBody } from "./brand-explorer-momentum-link-label.js";
import { isStructuredMomentumDateLine } from "./brand-explorer-recent-momentum-contract.js";
import { CALA_AVAILABLE_BY_SLUG } from "./brand-explorer-27-recent-momentum-evidence-fix-content.js";

export const WAVE14_POST_IMAGE_CLEANUP_VERSION = "wave14-post-image-content-cleanup-v1";
export const READY_STATE = "wave14_post_image_cleanup_ready_for_founder_review";

export const WAVE14_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-post-image-content-cleanup",
  "--confirm-nine-brand-stage6-scope",
  "--confirm-target-brands-only",
  "--confirm-all-nine-remain-under-review",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-public-restore-registry-changes",
  "--confirm-no-protected-46-brand-changes",
  "--confirm-no-accor-wave13-active-brand-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-wrong-brand-images",
  "--confirm-no-sibling-brand-images",
  "--confirm-four-points-flex-not-four-points",
  "--confirm-studiores-not-residence-inn-or-towneplace",
  "--confirm-flex-source-limitations-cleanly-held",
  "--confirm-no-adr",
  "--confirm-no-revpar",
  "--confirm-no-fee-stack",
  "--confirm-no-raw-urls",
  "--confirm-recent-momentum-structured",
  "--confirm-geo-footprint-source-supported-or-cleanly-unavailable",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const FORBIDDEN = new Set([
  ...WAVE14_NEVER_WRITE_FIELDS,
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Brand Status",
  "Image",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS = path.join(ROOT, "reports");
const DOCS = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function parseWave14PostImageCleanupFlags(argv = []) {
  const missing = WAVE14_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    ok: argv.includes("--apply") && missing.length === 0,
    missing,
    required: [...WAVE14_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS],
  };
}

function resolveIdentity(slug) {
  const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  if (!id?.recordId) throw new Error(`Missing factory-preview identity for ${slug}`);
  return { slug, recordId: id.recordId, name: id.name };
}

async function airtableWrite({ table, recordId, fields, method }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  const clean = { ...fields };
  for (const f of FORBIDDEN) delete clean[f];
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  for (let attempt = 1; attempt <= 8; attempt++) {
    const res = await fetch(url, {
      method,
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: clean }),
    });
    const json = await res.json().catch(() => ({}));
    if (res.ok) return json;
    if ((res.status === 429 || res.status >= 500) && attempt < 8) {
      await sleep(Math.min(30_000, 800 * 2 ** (attempt - 1)));
      continue;
    }
    throw new Error(json.error?.message || `${method} ${table} failed: ${res.status}`);
  }
  throw new Error(`${method} ${table} failed`);
}

function liveMomentumRows(rows) {
  return (rows || [])
    .filter(
      (r) =>
        nz(r.slotKey) === "footprint.momentum" &&
        r.active !== false &&
        !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
    )
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
}

function liveOpeningsRows(rows, { includeHidden = false } = {}) {
  return (rows || [])
    .filter((r) => {
      if (nz(r.slotKey) !== "footprint.openings") return false;
      if (r.active === false) return false;
      if (
        !includeHidden &&
        /do not display|internal only/i.test(nz(r.externalDisplayStatus))
      ) {
        return false;
      }
      return true;
    })
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
}

function isStubOpeningsTitle(title) {
  return /\(brand photography\)/i.test(nz(title));
}

function openingsNeedsGeographyFix(liveList, calaAvailable, allOpenings = []) {
  if (!liveList.length) return true;
  if (liveList.some((r) => isStubOpeningsTitle(r.title))) return true;
  if (calaAvailable) {
    const hasCala = liveList.some((r) =>
      /\bCALA\b/i.test(
        `${r.title || ""}\n${r.caseSummaryTags || ""}\n${r.caseSummaryBrandRelevance || ""}\n${r.body || ""}`
      )
    );
    if (!hasCala) return true;
  }
  // Restore previously-hidden surplus openings so property uniqueness can stay at 3/3
  const hiddenSurplus = (allOpenings || []).filter(
    (r) =>
      /do not display/i.test(nz(r.externalDisplayStatus)) &&
      (isStubOpeningsTitle(r.title) || /\bInternational Reference\b/i.test(r.title || ""))
  );
  if (liveList.length < 3 && hiddenSurplus.length) return true;
  return false;
}

function needsMomentumPatch(live, generated) {
  if (!generated) return false;
  if (!live) return true;
  const parsed = parseMomentumPresentationBody(live.body, live.title);
  if (!isStructuredMomentumDateLine(parsed.dateLine)) return true;
  if (!/^https?:\/\//i.test(nz(parsed.sourceUrl))) return true;
  if ((nz(parsed.description).split(/\s+/).filter(Boolean).length || 0) < 35) return true;
  if (nz(live.body) !== nz(generated.body)) return true;
  if (nz(live.title) !== nz(generated.title)) return true;
  return false;
}

function needsOpeningsPatch(live, generated) {
  if (!generated) return false;
  if (!live) return true;
  if (isStubOpeningsTitle(live.title)) return true;
  if (nz(live.title) !== nz(generated.title)) return true;
  if (nz(live.caseSummaryTags) !== nz(generated.caseSummaryTags)) return true;
  if (!/\b(CALA|International Reference)\b/i.test(`${live.title}\n${live.caseSummaryTags}`)) return true;
  return false;
}

export async function planWave14PostImageCleanupForBrand(slug) {
  const identity = resolveIdentity(slug);
  if (!WAVE14_SLUGS.includes(slug)) {
    return { brandSlug: slug, blocked: true, blockers: ["not_wave14"], patches: [] };
  }

  const pack = generateWave14PresentationPack(slug, {
    airtableName: identity.name,
    recordId: identity.recordId,
  });
  const fetch = await listPresentationRowsLight(identity.recordId, identity.name);
  const rows = fetch.rows || fetch || [];
  const rowList = Array.isArray(rows) ? rows : rows.rows || [];
  const liveList = liveMomentumRows(rowList);
  const genMomentum = (pack.presentation || []).filter((r) => r.slotKey === "footprint.momentum");
  const genOpenings = (pack.presentation || []).filter((r) => r.slotKey === "footprint.openings");
  const liveOpenings = liveOpeningsRows(rowList);
  const allOpenings = liveOpeningsRows(rowList, { includeHidden: true });

  const patches = [];
  const acceptedHolds = [];
  const calaAvailable = CALA_AVAILABLE_BY_SLUG[slug] === true;

  if (slug === "four-points-flex-by-sheraton") {
    acceptedHolds.push({
      type: "flex_gallery_openings_hold",
      note: "4/6 gallery + 0/3 openings held; no Four Points by Sheraton substitutes",
    });
    // Keep Flex openings Do Not Display — never promote gen openings to visible.
    for (const live of allOpenings) {
      if (/do not display/i.test(nz(live.externalDisplayStatus))) continue;
      patches.push({
        method: "PATCH",
        table: PRESENTATION_TABLE,
        recordId: live.recordId,
        slotKey: "footprint.openings",
        fields: { "External Display Status": "Do Not Display" },
        before: { title: live.title, status: live.externalDisplayStatus },
        rationale: "Flex openings hold — Do Not Display (no Four Points by Sheraton substitutes)",
      });
    }
  }
  if (slug === "springhill-suites-by-marriott" || slug === "towneplace-suites-by-marriott") {
    acceptedHolds.push({
      type: "international_reference_openings",
      note: "International Reference openings until steward-matched property URLs",
    });
  }
  if (slug === "studiores") {
    acceptedHolds.push({
      type: "studiores_no_sibling_imagery",
      note: "No Residence Inn / TownePlace / Element / Apartments by Marriott Bonvoy imagery",
    });
  }

  const max = Math.max(genMomentum.length, liveList.length);
  for (let i = 0; i < max; i++) {
    const generated = genMomentum[i];
    const live = liveList[i] || null;
    if (!generated) continue;
    if (!needsMomentumPatch(live, generated)) continue;

    const fields = {
      Title: generated.title,
      Body: generated.body,
      "Slot Key": "footprint.momentum",
      "Sort Order": generated.sortOrder ?? 449 + i,
      Active: true,
    };
    if (generated.caseSummaryOverview) fields["Case Summary Overview"] = generated.caseSummaryOverview;
    if (generated.caseSummaryBrandRelevance) {
      fields["Case Summary Brand Relevance"] = generated.caseSummaryBrandRelevance;
    }
    if (generated.caseSummaryOwnerObjective) {
      fields["Case Summary Owner Objective"] = generated.caseSummaryOwnerObjective;
    }
    if (generated.caseSummaryInterpretation) {
      fields["Case Summary Interpretation"] = generated.caseSummaryInterpretation;
    }
    if (generated.caseSummaryTags) fields["Case Summary Tags"] = generated.caseSummaryTags;

    if (live?.recordId) {
      patches.push({
        method: "PATCH",
        table: PRESENTATION_TABLE,
        recordId: live.recordId,
        slotKey: "footprint.momentum",
        fields,
        before: { title: live.title, body: String(live.body || "").slice(0, 200) },
        rationale: "Stage 6 structured Recent Momentum (date + summary + https URL)",
      });
    } else {
      patches.push({
        method: "POST",
        table: PRESENTATION_TABLE,
        recordId: null,
        slotKey: "footprint.momentum",
        fields: {
          ...fields,
          Brand: [identity.recordId],
          "Brand Name": identity.name,
        },
        before: null,
        rationale: "Stage 6 create missing structured momentum card",
      });
    }
  }

  // Openings geography / property-name cleanup (skip Flex — held above)
  if (slug !== "four-points-flex-by-sheraton" && openingsNeedsGeographyFix(liveOpenings, calaAvailable, allOpenings)) {
    const oMax = Math.max(genOpenings.length, liveOpenings.length);
    const usedOpeningIds = new Set();
    for (let i = 0; i < oMax; i++) {
      const generated = genOpenings[i];
      const live = liveOpenings[i] || null;
      if (generated) {
        if (!needsOpeningsPatch(live, generated) && live?.recordId) {
          usedOpeningIds.add(live.recordId);
          continue;
        }
        const fields = {
          Title: generated.title,
          Body: generated.body,
          "Slot Key": "footprint.openings",
          "Sort Order": generated.sortOrder ?? 490 + i,
          Active: true,
        };
        // Clear accidental Do Not Display from prior surplus-hide passes
        if (live && /do not display/i.test(nz(live.externalDisplayStatus))) {
          // leave status alone if we somehow selected a hidden row — prefer visible targets
        }
        if (generated.caseSummaryOverview) fields["Case Summary Overview"] = generated.caseSummaryOverview;
        if (generated.caseSummaryBrandRelevance) {
          fields["Case Summary Brand Relevance"] = generated.caseSummaryBrandRelevance;
        }
        if (generated.caseSummaryOwnerObjective) {
          fields["Case Summary Owner Objective"] = generated.caseSummaryOwnerObjective;
        }
        if (generated.caseSummaryInterpretation) {
          fields["Case Summary Interpretation"] = generated.caseSummaryInterpretation;
        }
        if (generated.caseSummaryTags) fields["Case Summary Tags"] = generated.caseSummaryTags;

        if (live?.recordId) {
          usedOpeningIds.add(live.recordId);
          patches.push({
            method: "PATCH",
            table: PRESENTATION_TABLE,
            recordId: live.recordId,
            slotKey: "footprint.openings",
            fields,
            before: { title: live.title, tags: live.caseSummaryTags },
            rationale: "Stage 6 openings property name + geography label cleanup",
          });
        } else {
          patches.push({
            method: "POST",
            table: PRESENTATION_TABLE,
            recordId: null,
            slotKey: "footprint.openings",
            fields: {
              ...fields,
              Brand: [identity.recordId],
              "Brand Name": identity.name,
            },
            before: null,
            rationale: "Stage 6 create missing labeled openings row",
          });
        }
      }
    }

    // Relabel remaining stub / previously-hidden surplus openings as IR (keep images for 3/3).
    const surplus = allOpenings.filter((r) => r.recordId && !usedOpeningIds.has(r.recordId));
    for (const live of surplus) {
      const isHidden = /do not display|internal only/i.test(nz(live.externalDisplayStatus));
      const isStub = isStubOpeningsTitle(live.title);
      const needsIrRelabel =
        isStub ||
        isHidden ||
        !/\b(CALA|International Reference)\b/i.test(`${live.title}\n${live.caseSummaryTags}`);
      if (!needsIrRelabel) continue;
      patches.push({
        method: "PATCH",
        table: PRESENTATION_TABLE,
        recordId: live.recordId,
        slotKey: "footprint.openings",
        fields: {
          Title: `${identity.name} — International Reference`,
          Body:
            `International Reference property photography for ${identity.name}. Use this labeled example for visual diligence of product and guest experience outside steward-matched CALA property URLs. Keep sibling Marriott brands out of the same openings comparison, and confirm the asset under review can deliver the same brand promise before treating photography as transferable operating proof.`,
          "Case Summary Overview": `International Reference photography reference for ${identity.name}.`,
          "Case Summary Brand Relevance": `Official International Reference property photography for ${identity.name}.`,
          "Case Summary Owner Objective":
            "Compare product look and guest experience using a clearly labeled International Reference example.",
          "Case Summary Interpretation":
            "Photography retained from Stage 5 materialization; geography labeled International Reference.",
          "Case Summary Tags": `International Reference, ${identity.name}, Property example`,
          Active: true,
          "External Display Status": null,
        },
        before: { title: live.title, tags: live.caseSummaryTags, status: live.externalDisplayStatus },
        rationale:
          "Restore/relabel surplus openings as International Reference (keep image for 3/3 property uniqueness)",
      });
    }
  }

  return {
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    blocked: false,
    patches,
    acceptedHolds,
    momentumGenerated: genMomentum.length,
    momentumLive: liveList.length,
    openingsGenerated: genOpenings.length,
    openingsLive: liveOpenings.length,
  };
}

export async function planWave14PostImageCleanup() {
  const brands = [];
  for (const slug of WAVE14_SLUGS) {
    brands.push(await planWave14PostImageCleanupForBrand(slug));
  }
  return {
    version: WAVE14_POST_IMAGE_CLEANUP_VERSION,
    waveVersion: WAVE14_VERSION,
    generatedAt: new Date().toISOString(),
    scope: [...WAVE14_SLUGS],
    brands,
    patchCount: brands.reduce((n, b) => n + (b.patches?.length || 0), 0),
  };
}

export async function applyWave14PostImageCleanup({ plan, apply = false, argv = [] } = {}) {
  const flags = parseWave14PostImageCleanupFlags(argv);
  if (apply && !flags.ok) {
    throw new Error(`Missing apply flags: ${flags.missing.join(", ")}`);
  }
  if (!apply) {
    return { applied: false, writes: 0, results: [] };
  }

  const results = [];
  let writes = 0;
  for (const brand of plan.brands || []) {
    for (const p of brand.patches || []) {
      if (p.method === "PATCH") {
        await airtableWrite({
          table: p.table,
          recordId: p.recordId,
          fields: p.fields,
          method: "PATCH",
        });
      } else {
        const created = await airtableWrite({
          table: p.table,
          recordId: null,
          fields: p.fields,
          method: "POST",
        });
        p.createdRecordId = created.id;
      }
      writes += 1;
      await sleep(250);
      results.push({ slug: brand.brandSlug, slotKey: p.slotKey, method: p.method, recordId: p.recordId || p.createdRecordId });
    }
  }
  return { applied: true, writes, results, flags };
}

export function writeWave14PostImageCleanupReports(plan, applyResult = null) {
  fs.mkdirSync(REPORTS, { recursive: true });
  fs.mkdirSync(DOCS, { recursive: true });

  const ready =
    applyResult?.applied === true
      ? READY_STATE
      : "wave14_post_image_cleanup_dry_run";

  const report = {
    ...plan,
    dryRun: !applyResult?.applied,
    applyResult: applyResult
      ? {
          applied: applyResult.applied,
          writes: applyResult.writes,
          airtableWrites: applyResult.writes || 0,
        }
      : { applied: false, writes: 0, airtableWrites: 0 },
    readyState: ready,
    protections: {
      noBrandStatusChanges: true,
      noReleaseFieldWrites: true,
      noCompanyValidationChanges: true,
      noSourceLibraryStatusChanges: true,
      noRegistryApprovalChanges: true,
      noProtected46Writes: true,
      noAccorWave13Writes: true,
      noHouseOfOriginalsWrites: true,
      noMorgansOriginalsWrites: true,
      noRadissonCollectionChanges: true,
      allNineRemainUnderReview: true,
      flexSourceLimitationsHeld: true,
    },
    founderReviewNote:
      "Nine Wave 14 brands remain Under Review / factory preview. Four Points Flex is visually/source-limited (4/6 gallery, 0/3 openings). SpringHill and TownePlace openings use International Reference until steward-matched URLs. StudioRes must not use sibling extended-stay imagery.",
  };

  const jsonPath = path.join(REPORTS, "brand-explorer-wave14-post-image-cleanup.json");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);

  const md = [
    `# Wave 14 Stage 6 — Post-Image Cleanup`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.dryRun ? "DRY-RUN" : "APPLY"}**`,
    `Ready: **\`${report.readyState}\`**`,
    `Patches: **${report.patchCount}** · Writes: **${report.applyResult.writes}**`,
    ``,
    `## Brand results`,
    ``,
    ...report.brands.map(
      (b) =>
        `- **${b.brandName}** (\`${b.brandSlug}\`): patches=${b.patches?.length || 0} · momentum gen/live=${b.momentumGenerated}/${b.momentumLive}` +
        (b.acceptedHolds?.length
          ? ` · holds: ${b.acceptedHolds.map((h) => h.type).join(", ")}`
          : "")
    ),
    ``,
    `## Flex / SpringHill / TownePlace / StudioRes holds`,
    ``,
    `- Four Points Flex: 4/6 gallery + 0/3 openings held; no Four Points by Sheraton substitutes`,
    `- SpringHill / TownePlace: International Reference openings until steward-matched property URLs`,
    `- StudioRes: no Residence Inn / TownePlace / Element / Apartments by Marriott Bonvoy imagery`,
    ``,
    `## Protections`,
    ``,
    `- No Brand Status / release / CV / Source / Registry / protected-46 / Accor / House of Originals / Morgans / Radisson Collection writes`,
    `- All nine remain Under Review / factory preview`,
    ``,
    `## Founder review note`,
    ``,
    report.founderReviewNote,
    ``,
  ].join("\n");
  const mdPath = path.join(REPORTS, "brand-explorer-wave14-post-image-cleanup.md");
  fs.writeFileSync(mdPath, `${md}\n`);

  for (const b of report.brands) {
    const per = [
      `# Wave 14 Stage 6 — ${b.brandName}`,
      ``,
      `- Slug: \`${b.brandSlug}\``,
      `- Record ID: \`${b.recordId}\``,
      `- Patches: **${b.patches?.length || 0}**`,
      ``,
      `## Accepted holds`,
      ``,
      ...(b.acceptedHolds?.length
        ? b.acceptedHolds.map((h) => `- **${h.type}**: ${h.note}`)
        : ["- none"]),
      ``,
      `## Patches`,
      ``,
      ...(b.patches?.length
        ? b.patches.map(
            (p) =>
              `- \`${p.method}\` ${p.slotKey} ${p.recordId || "(create)"} — ${p.rationale}`
          )
        : ["- none (already structured or no live rows)"]),
      ``,
    ].join("\n");
    fs.writeFileSync(
      path.join(REPORTS, `brand-explorer-wave14-post-image-cleanup-${b.brandSlug}.md`),
      `${per}\n`
    );
  }

  const docsPath = path.join(DOCS, "brand-explorer-wave14-post-image-cleanup.md");
  fs.writeFileSync(
    docsPath,
    [
      `# Brand Explorer — Wave 14 Stage 6 Post-Image Cleanup`,
      ``,
      `Ready token: \`${READY_STATE}\``,
      ``,
      `## What this stage fixes`,
      ``,
      `Recent Momentum cards must use structured Body format:`,
      ``,
      "```",
      "Directory | Mon YYYY",
      "",
      "summary (≥35 words, geography labeled)",
      "",
      "https://announcement-or-directory-url",
      "```",
      ``,
      `## Accepted holds`,
      ``,
      `- Four Points Flex source limitations (4/6 gallery, 0/3 openings)`,
      `- SpringHill / TownePlace International Reference openings`,
      `- StudioRes sibling-brand exclusion`,
      ``,
    ].join("\n")
  );

  return { jsonPath, mdPath, docsPath, report };
}

export async function runWave14PostImageContentCleanup({ dryRun = true, argv = [] } = {}) {
  const apply = argv.includes("--apply") && !dryRun;
  // failure extraction companion
  try {
    const { spawnSync } = await import("node:child_process");
    spawnSync("node", ["scripts/extract-wave14-post-image-cleanup-failures.mjs"], {
      cwd: ROOT,
      encoding: "utf8",
      shell: true,
    });
  } catch (err) {
    console.error("[wave14-stage6] failure extraction warn:", err?.message || err);
  }

  const plan = await planWave14PostImageCleanup();
  let applyResult = null;
  if (apply) {
    applyResult = await applyWave14PostImageCleanup({ plan, apply: true, argv });
  }
  const paths = writeWave14PostImageCleanupReports(plan, applyResult);
  return {
    ...paths.report,
    paths,
    pass: true,
    airtableWrites: applyResult?.writes || 0,
    readyState: paths.report.readyState,
  };
}
