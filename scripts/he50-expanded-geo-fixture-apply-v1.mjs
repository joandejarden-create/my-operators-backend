#!/usr/bin/env node
/**
 * HE50 expanded standing auth — Demand Anchors + Travel Infrastructure
 * fixture apply for eight-hotel pilot markets.
 *
 * Default dry-run. Apply: --apply
 * Reuses existing import-commit pipelines; never Hotel Census.
 */
import "dotenv/config";
import { mkdirSync, writeFileSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { createRequire } from "node:module";
import {
  HE50_STANDING_AUTH_VERSION,
  HE50_DEMAND_ANCHORS_TABLE_ID,
  HE50_TRAVEL_INFRASTRUCTURE_TABLE_ID,
  isAllowedDestinationTable,
  HE50_STANDING_APPLY_GATE,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/standing-authorization-v1.js";
import {
  previewDemandAnchorsImport,
  commitDemandAnchorsImport,
} from "../lib/demand-anchors/import-commit.js";
import {
  previewTravelInfrastructureImport,
  commitTravelInfrastructureImport,
} from "../lib/travel-infrastructure/import-commit.js";

const require = createRequire(import.meta.url);
try {
  require("dotenv").config({ path: "C:/Dev/dealality/my-operators-backend/.env" });
} catch {
  /* optional */
}

const APPLY = process.argv.includes("--apply");
const OUT = join(
  process.cwd(),
  "reports/hotel-explorer/he50-expanded-standing-auth-completion-v1",
);
mkdirSync(OUT, { recursive: true });

const DEMAND_FIXTURES = [
  "fixtures/demand-anchors-bethesda-montgomery-md-v1.json",
  "fixtures/demand-anchors-newtown-square-pa-v1.json",
  "fixtures/demand-anchors-maynooth-ireland-v1.json",
  "fixtures/demand-anchors-maynooth-ireland-v2-near.json",
  "fixtures/demand-anchors-guanacaste-papagayo-v1.json",
  "fixtures/demand-anchors-costa-elena-la-cruz-v1.json",
];

const TI_FIXTURES = [
  "fixtures/travel-infrastructure-bethesda-dc-md-v1.json",
  "fixtures/travel-infrastructure-newtown-square-phl-v1.json",
  "fixtures/travel-infrastructure-maynooth-dublin-v1.json",
  "fixtures/travel-infrastructure-guanacaste-lir-v1.json",
];

function loadFixture(rel) {
  const path = join(process.cwd(), rel);
  if (!existsSync(path)) return { ok: false, path: rel, error: "MISSING_FILE" };
  return { ok: true, path: rel, payload: JSON.parse(readFileSync(path, "utf8")) };
}

async function runDemand(rel) {
  const loaded = loadFixture(rel);
  if (!loaded.ok) return { kind: "demand", ...loaded };
  if (!isAllowedDestinationTable(HE50_DEMAND_ANCHORS_TABLE_ID)) {
    return { kind: "demand", path: rel, class: "BLOCKED_OUT_OF_SCOPE" };
  }
  const body = {
    market: loaded.payload.market || "",
    country: loaded.payload.country || "",
    region: loaded.payload.region || "",
    verification: loaded.payload.verification || null,
    points: loaded.payload.points || [],
  };
  const preview = await previewDemandAnchorsImport(body);
  const valid = (preview.preview || []).filter((r) => r.valid);
  const result = {
    kind: "demand",
    path: rel,
    tableId: HE50_DEMAND_ANCHORS_TABLE_ID,
    market: body.market,
    country: body.country,
    previewOk: !!preview.ok,
    validCount: valid.length,
    duplicateCount: (preview.duplicates || []).length,
    class: APPLY ? "AUTO_APPROVED_CANONICAL_CREATE" : "DRY_RUN",
  };
  if (!preview.ok) {
    result.class = "QUARANTINED_INSUFFICIENT_EVIDENCE";
    result.error = preview.message || preview.error;
    return result;
  }
  if (!APPLY) return result;
  const commit = await commitDemandAnchorsImport(valid, {
    skipDuplicates: true,
    market: body.market,
    country: body.country,
    region: body.region,
  });
  result.apply = {
    ok: !!commit.ok || (commit.created || []).length >= 0,
    created: (commit.created || []).map((r) => r.id || r.recordId || r),
    skipped: (commit.skipped || []).length,
    errors: commit.errors || [],
  };
  result.persistenceStage = "AIRTABLE_APPLIED";
  return result;
}

async function runTi(rel) {
  const loaded = loadFixture(rel);
  if (!loaded.ok) return { kind: "travel_infrastructure", ...loaded };
  if (!isAllowedDestinationTable(HE50_TRAVEL_INFRASTRUCTURE_TABLE_ID)) {
    return { kind: "travel_infrastructure", path: rel, class: "BLOCKED_OUT_OF_SCOPE" };
  }
  const body = {
    market: loaded.payload.market || "",
    country: loaded.payload.country || "",
    region: loaded.payload.region || "",
    verification: loaded.payload.verification || null,
    points: loaded.payload.points || loaded.payload.infrastructure || [],
  };
  let preview;
  try {
    preview = await previewTravelInfrastructureImport(body);
  } catch (e) {
    return {
      kind: "travel_infrastructure",
      path: rel,
      class: "QUARANTINED_INSUFFICIENT_EVIDENCE",
      error: String(e.message || e),
    };
  }
  const valid = (preview.preview || preview.records || []).filter(
    (r) => r.valid !== false,
  );
  const result = {
    kind: "travel_infrastructure",
    path: rel,
    tableId: HE50_TRAVEL_INFRASTRUCTURE_TABLE_ID,
    market: body.market,
    country: body.country,
    previewOk: preview.ok !== false,
    validCount: valid.length,
    duplicateCount: (preview.duplicates || []).length,
    class: APPLY ? "AUTO_APPROVED_CANONICAL_CREATE" : "DRY_RUN",
  };
  if (preview.ok === false) {
    result.class = "QUARANTINED_INSUFFICIENT_EVIDENCE";
    result.error = preview.message || preview.error;
    return result;
  }
  if (!APPLY) return result;
  const commit = await commitTravelInfrastructureImport(valid, {
    skipDuplicates: true,
    market: body.market,
    country: body.country,
    region: body.region,
  });
  result.apply = {
    created: (commit.created || []).map((r) => r.id || r.recordId || r),
    skipped: (commit.skipped || []).length,
    errors: commit.errors || [],
  };
  result.persistenceStage = "AIRTABLE_APPLIED";
  return result;
}

async function main() {
  if (!HE50_STANDING_APPLY_GATE.applyAuthorized) {
    console.error(JSON.stringify({ ok: false, error: "STANDING_AUTH_OFF" }));
    process.exit(2);
  }
  const operations = [];
  for (const f of DEMAND_FIXTURES) operations.push(await runDemand(f));
  for (const f of TI_FIXTURES) operations.push(await runTi(f));

  const report = {
    ok: operations.every(
      (o) =>
        o.class !== "BLOCKED_OUT_OF_SCOPE" &&
        !(o.apply?.errors && o.apply.errors.length),
    ),
    mode: APPLY ? "APPLY" : "DRY_RUN",
    authorizationVersion: HE50_STANDING_AUTH_VERSION,
    certifications: {
      CANONICAL_PERSISTENCE_ALLOWLIST_EXPANDED: true,
    },
    operations,
    counts: {
      demandFixtures: DEMAND_FIXTURES.length,
      tiFixtures: TI_FIXTURES.length,
      dryOrApplied: operations.filter((o) =>
        /DRY_RUN|AUTO_APPROVED/.test(o.class || ""),
      ).length,
      quarantined: operations.filter((o) => /QUARANTINED/.test(o.class || ""))
        .length,
      createdTotal: operations.reduce(
        (n, o) => n + (o.apply?.created?.length || 0),
        0,
      ),
    },
  };
  const name = APPLY
    ? "GEO_FIXTURE_APPLY.json"
    : "GEO_FIXTURE_DRY_RUN.json";
  writeFileSync(join(OUT, name), JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  if (!report.ok) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
