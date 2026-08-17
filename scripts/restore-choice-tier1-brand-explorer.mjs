/**
 * Restore Choice Tier 1 Brand Explorer — L2 enrichment (Blu-parity pattern for full-json brands).
 *
 * Applies optional split fixtures (materials, gallery, case studies, momentum),
 * CALA footprint openings, portfolio context, and Brand Basics patch-missing.
 *
 *   node scripts/restore-choice-tier1-brand-explorer.mjs --brand "Ascend Hotel Collection"
 *   node scripts/restore-choice-tier1-brand-explorer.mjs --brand "Ascend Hotel Collection" --with-images
 *   node scripts/restore-choice-tier1-brand-explorer.mjs --brand "Comfort Inn & Suites" --dry-run
 *
 * Prerequisite: fixtures/brand-explorer-presentation-{slug}-full.json (L1 slot-complete baseline).
 * L2 overlays: {slug}-case-studies.json, -footprint-momentum.json, -materials.json, -gallery.json
 */
import fs from "fs";
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";
import { resolveChoiceBrandManifest } from "./lib/choice-brand-explorer-manifest.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const APPLY = path.join(ROOT, "scripts/apply-brand-explorer-presentation-fixture.mjs");

function parseArgs(argv) {
  const args = argv.slice(2);
  const i = args.indexOf("--brand");
  return {
    brandRef: i >= 0 ? String(args[i + 1] || "").trim() : "",
    dryRun: args.includes("--dry-run"),
    withImages: args.includes("--with-images"),
    syncFull: args.includes("--sync-full"),
  };
}

function run(cmd, args, label) {
  if (label) console.log(`\n>> ${label}`);
  const r = spawnSync(cmd, args, { cwd: ROOT, stdio: "inherit", env: process.env });
  if (r.status !== 0) process.exit(r.status ?? 1);
}

function fixtureExists(rel) {
  return fs.existsSync(path.join(ROOT, rel));
}

function runApply(brand, fixture, prefix, dryRun) {
  const args = [
    APPLY,
    "--brand-record-id",
    brand.recordId,
    "--fixture",
    path.join(ROOT, fixture),
    "--replace-slot-prefix",
    prefix,
  ];
  if (dryRun) args.push("--dry-run");
  console.log(`\n>> ${brand.profileName} · ${prefix} · ${fixture}`);
  const r = spawnSync(process.execPath, args, { cwd: ROOT, stdio: "inherit", env: process.env });
  if (r.status !== 0) process.exit(r.status ?? 1);
}

/** @param {import('./lib/choice-brand-explorer-manifest.mjs').listChoiceBrandManifest extends Function ? ReturnType<import('./lib/choice-brand-explorer-manifest.mjs').listChoiceBrandManifest>[number] : never} brand */
function optionalSplitFixtures(brand) {
  const slug = brand.slug;
  return [
    [`fixtures/brand-explorer-presentation-${slug}-case-studies.json`, "materials.caseStudy"],
    [`fixtures/brand-explorer-presentation-${slug}-footprint-momentum.json`, "footprint.momentum"],
    [`fixtures/brand-explorer-presentation-${slug}-materials.json`, "materials.file"],
    [`fixtures/brand-explorer-presentation-${slug}-gallery.json`, "materials.gallery."],
  ];
}

function main() {
  const { brandRef, dryRun, withImages, syncFull } = parseArgs(process.argv);
  if (!brandRef) {
    console.error(
      'Usage: node scripts/restore-choice-tier1-brand-explorer.mjs --brand "Ascend Hotel Collection" [--dry-run] [--with-images] [--sync-full]'
    );
    process.exit(1);
  }

  const brand = resolveChoiceBrandManifest(brandRef);
  if (!brand) {
    console.error("Unknown Choice brand:", brandRef);
    console.error("Run: npm run choice-brand-explorer:manifest");
    process.exit(1);
  }
  if (brand.applyStrategy === "premium-split") {
    console.error(
      `${brand.profileName} uses premium-split fixtures — run:`,
      brand.premium?.applyScript || "(see manifest)"
    );
    process.exit(1);
  }
  if (!brand.fullFixture) {
    console.error("No full fixture — run generate phase first:");
    console.error(`  npm run choice-brand-explorer:pipeline -- --brand "${brand.profileName}" --phase generate`);
    process.exit(1);
  }

  console.log(
    `Restoring ${brand.profileName} (${brand.recordId}) — Tier 1 L2 enrichment${dryRun ? " [dry-run]" : ""}…`
  );

  if (syncFull && !dryRun) {
    run(
      process.execPath,
      [
        APPLY,
        "--brand-record-id",
        brand.recordId,
        "--fixture",
        path.join(ROOT, brand.fullFixture),
        "--only-missing",
      ],
      `${brand.profileName} · sync full fixture (only-missing)`
    );
  } else if (syncFull) {
    console.log("\n[dry-run] Would apply full fixture with --only-missing");
  }

  let appliedSplits = 0;
  const splitSteps = optionalSplitFixtures(brand);
  const caseStudySplit = splitSteps.find(([f]) => f.includes("-case-studies.json"));
  const otherSplits = splitSteps.filter(([f]) => !f.includes("-case-studies.json"));

  for (const [fixture, prefix] of otherSplits) {
    if (!fixtureExists(fixture)) continue;
    runApply(brand, fixture, prefix, dryRun);
    appliedSplits += 1;
  }
  if (!appliedSplits && !caseStudySplit) {
    console.log("\nNo L2 split fixtures found — add brand-explorer-presentation-{slug}-*.json overlays.");
  }

  if (!dryRun) {
    run(
      process.execPath,
      [
        path.join(ROOT, "scripts/apply-choice-cala-footprint-openings-batch.mjs"),
        "--brand",
        brand.airtableName.includes("Radisson RED")
          ? "Radisson RED by Choice"
          : brand.airtableName,
      ],
      `${brand.airtableName} · CALA footprint.openings`
    );

    run(
      process.execPath,
      [
        path.join(ROOT, "scripts/apply-choice-footprint-momentum-from-openings-batch.mjs"),
        "--brand",
        brand.airtableName.includes("Radisson RED")
          ? "Radisson RED by Choice"
          : brand.airtableName,
      ],
      `${brand.airtableName} · footprint.momentum (curated press)`
    );

    if (caseStudySplit && fixtureExists(caseStudySplit[0])) {
      runApply(brand, caseStudySplit[0], caseStudySplit[1], dryRun);
    } else {
      run(
        process.execPath,
        [
          path.join(ROOT, "scripts/patch-choice-case-study-from-openings.mjs"),
          "--brand",
          brand.airtableName.includes("Radisson RED")
            ? "Radisson RED by Choice"
            : brand.airtableName,
        ],
        `${brand.profileName} · patch case studies from openings (no split fixture)`
      );
    }

    run(
      process.execPath,
      [
        path.join(ROOT, "scripts/apply-choice-portfolio-context-batch.mjs"),
        "--brand",
        brand.airtableName.includes("Radisson RED")
          ? "Radisson RED by Choice"
          : brand.airtableName,
      ],
      `${brand.airtableName} · portfolio context`
    );

    run(
      process.execPath,
      [path.join(ROOT, "scripts/apply-choice-brand-basics-batch.mjs")],
      "Brand Basics patch-missing (all CHI brands with fixtures)"
    );

    if (withImages) {
      run(
        process.execPath,
        [
          path.join(ROOT, "scripts/attach-choice-footprint-opening-images.mjs"),
          "--brand",
          brand.airtableName.includes("Radisson RED")
          ? "Radisson RED by Choice"
          : brand.airtableName,
          "--force",
        ],
        "Footprint opening hero images"
      );
      if (brand.slug === "ascend-hotel-collection" && fixtureExists("scripts/restore-ascend-choice-gallery-images.mjs")) {
        run(
          process.execPath,
          [path.join(ROOT, "scripts/restore-ascend-choice-gallery-images.mjs")],
          "Ascend materials.gallery press-kit images"
        );
      }
      if (brand.slug === "radisson-red-choice" && fixtureExists("scripts/restore-radisson-red-choice-gallery-images.mjs")) {
        run(
          process.execPath,
          [path.join(ROOT, "scripts/restore-radisson-red-choice-gallery-images.mjs")],
          "Radisson RED materials.gallery press-kit images"
        );
      }
    } else {
      console.log("\nTip: re-run with --with-images to attach footprint opening hero images.");
    }
  } else {
    console.log("\n[dry-run] Would run CALA openings, momentum, portfolio context, and basics batch.");
  }

  if (!dryRun) {
    run(
      process.execPath,
      [
        path.join(ROOT, "scripts/audit-choice-explorer-presentation-gaps.mjs"),
        "--brand",
        brand.profileName,
      ],
      "Post-restore gap audit"
    );
  }

  console.log(`\nRestore finished. Review Brand Explorer UI for ${brand.airtableName}.`);
  console.log(`  /brand-explorer-combined.html?id=${encodeURIComponent(brand.airtableName)}`);
}

main();
