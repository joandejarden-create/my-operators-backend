/**
 * Push owner-voice–corrected presentation fixtures to Radisson family brands.
 *
 *   node scripts/apply-owner-voice-radisson-family.mjs --dry-run
 *   node scripts/apply-owner-voice-radisson-family.mjs
 */
import { spawnSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const APPLY = path.join(ROOT, "scripts/apply-brand-explorer-presentation-fixture.mjs");
const BASICS_APPLY = path.join(ROOT, "scripts/apply-radisson-brand-basics-brand-on-a-page.mjs");

const BRANDS = [
  { id: "recXYvwtNQGUzFZcn", name: "Radisson", fixture: "fixtures/brand-explorer-presentation-radisson.example.json" },
  { id: "recywbx1YQSTCPqW1", name: "Radisson (Choice)", fixture: "fixtures/brand-explorer-presentation-radisson.example.json" },
  { id: "recWPEvxBQxVVzSq3", name: "Radisson Blu (Choice)", fixture: "fixtures/brand-explorer-presentation-radisson-blu.example.json" },
];

/** Never use bare `footprint.` — it deletes momentum, portfolio_mix, and openings. */
const RADISSON_GEO_GROWTH_PREFIXES = [
  ["fixtures/brand-explorer-presentation-radisson-footprint-geo-growth.json", "footprint.geo"],
  ["fixtures/brand-explorer-presentation-radisson-footprint-geo-growth.json", "footprint.region."],
  ["fixtures/brand-explorer-presentation-radisson-footprint-geo-growth.json", "footprint.growth"],
  ["fixtures/brand-explorer-presentation-radisson-footprint-geo-growth.json", "footprint.editorial"],
];

const BLU_GEO_GROWTH_PREFIXES = [
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint-geo-growth.json", "footprint.geo"],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint-geo-growth.json", "footprint.region."],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint-geo-growth.json", "footprint.growth"],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint-geo-growth.json", "footprint.editorial"],
];

const SHARED_STEPS = [
  ...RADISSON_GEO_GROWTH_PREFIXES,
  ["fixtures/brand-explorer-presentation-radisson-case-studies.json", "materials.caseStudy"],
];

const BLU_ONLY = [
  ...BLU_GEO_GROWTH_PREFIXES,
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint.json", "footprint.geo.summary"],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint.json", "footprint.growth.narrative"],
  ["fixtures/brand-explorer-presentation-radisson-blu-case-studies.json", "materials.caseStudy"],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint-openings.json", "footprint.openings"],
  ["fixtures/brand-explorer-presentation-radisson-blu-footprint-momentum.json", "footprint.momentum"],
  ["fixtures/brand-explorer-presentation-radisson-blu-portfolio-compliance-similar.json", "footprint.portfolio_mix"],
  ["fixtures/brand-explorer-presentation-standards-radisson-blu.json", "standards."],
];

function run(cmd, args) {
  const r = spawnSync(cmd, args, { cwd: ROOT, stdio: "inherit", env: process.env });
  if (r.status !== 0) process.exit(r.status ?? 1);
}

function applyFixture(brandId, brandName, fixture, prefix, dryRun) {
  const args = [APPLY, "--brand-record-id", brandId, "--fixture", path.join(ROOT, fixture), "--replace-slot-prefix", prefix];
  if (dryRun) args.splice(1, 0, "--dry-run");
  console.log(`\n>> ${brandName} · ${prefix} · ${fixture}`);
  run(process.execPath, args);
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");

  for (const b of BRANDS.slice(0, 2)) {
    for (const [fixture, prefix] of SHARED_STEPS) {
      applyFixture(b.id, b.name, fixture, prefix, dryRun);
    }
    if (!dryRun) {
      applyFixture(b.id, b.name, b.fixture, "hero.", false);
      applyFixture(b.id, b.name, b.fixture, "operations.", false);
      applyFixture(b.id, b.name, b.fixture, "overview.", false);
      applyFixture(b.id, b.name, b.fixture, "valueOwners.", false);
      applyFixture(b.id, b.name, b.fixture, "loyalty.", false);
      applyFixture(b.id, b.name, b.fixture, "insight.summary", false);
      applyFixture(
        b.id,
        b.name,
        b.id === BRANDS[2].id
          ? "fixtures/brand-explorer-presentation-radisson-blu-portfolio-compliance-similar.json"
          : "fixtures/brand-explorer-presentation-radisson-portfolio-compliance-similar.json",
        "insight.similar",
        false
      );
      applyFixture(
        b.id,
        b.name,
        b.id === BRANDS[2].id
          ? "fixtures/brand-explorer-presentation-radisson-blu-portfolio-compliance-similar.json"
          : "fixtures/brand-explorer-presentation-radisson-portfolio-compliance-similar.json",
        "footprint.portfolio_mix",
        false
      );
      applyFixture(
        b.id,
        b.name,
        b.id === BRANDS[2].id
          ? "fixtures/brand-explorer-presentation-radisson-blu-portfolio-compliance-similar.json"
          : "fixtures/brand-explorer-presentation-radisson-portfolio-compliance-similar.json",
        "operations.compliance.",
        false
      );
      applyFixture(b.id, b.name, "fixtures/brand-explorer-presentation-radisson-footprint-momentum.json", "footprint.momentum", false);
      applyFixture(b.id, b.name, "fixtures/brand-explorer-presentation-radisson-footprint-openings.json", "footprint.openings", false);
      applyFixture(b.id, b.name, "fixtures/brand-explorer-presentation-radisson-paramaribo-opening.json", "footprint.openings", false);
      run(process.execPath, [
        BASICS_APPLY,
        "--brand-record-id",
        b.id,
        "--fixture",
        path.join(ROOT, "fixtures/brand-basics-radisson-brand-on-a-page.json"),
      ]);
    }
  }

  const blu = BRANDS[2];
  if (!dryRun) {
    applyFixture(blu.id, blu.name, blu.fixture, "hero.", false);
    applyFixture(blu.id, blu.name, blu.fixture, "operations.", false);
    applyFixture(blu.id, blu.name, blu.fixture, "overview.", false);
    applyFixture(blu.id, blu.name, blu.fixture, "valueOwners.", false);
    applyFixture(blu.id, blu.name, blu.fixture, "loyalty.", false);
    applyFixture(blu.id, blu.name, blu.fixture, "insight.summary", false);
    applyFixture(
      blu.id,
      blu.name,
      "fixtures/brand-explorer-presentation-radisson-blu-portfolio-compliance-similar.json",
      "insight.similar",
      false
    );
    applyFixture(
      blu.id,
      blu.name,
      "fixtures/brand-explorer-presentation-radisson-blu-portfolio-compliance-similar.json",
      "operations.compliance.",
      false
    );
    for (const [fixture, prefix] of BLU_ONLY) {
      applyFixture(blu.id, blu.name, fixture, prefix, false);
    }
    run(process.execPath, [
      BASICS_APPLY,
      "--brand-record-id",
      blu.id,
      "--fixture",
      path.join(ROOT, "fixtures/brand-basics-radisson-blu-choice.json"),
    ]);
  } else {
    console.log("\n[dry-run] Would apply Blu-only footprint, standards, case studies, openings, basics.");
  }

  console.log("\nDone.");
}

main();
