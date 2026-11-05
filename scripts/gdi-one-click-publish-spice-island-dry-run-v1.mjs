#!/usr/bin/env node
/**
 * Spice Island one-click publish dry-run + isolated apply verification.
 *
 * Uses the seven enriched V2 snapshots as the approved local set.
 * Applies into an isolated temp registry (never production volume).
 * STOP before production — no Railway / Airtable / live share mutation.
 */
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";
import {
  publishHotelInventoryOneClick,
  GDI_ONE_CLICK_HOTEL_PUBLISH_V1,
} from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";
import { createGdiPublishedInventoryRegistry } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import { mergePublishedInventoryIntoOpportunities } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import { assertManifestParity } from "../lib/group-demand-intelligence/commercial-inventory/gdi-canonical-display-eligibility-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-one-click-publication-architecture-v1",
);
const ENRICHED = path.join(
  ROOT,
  "reports/gdi-production-publication-full-content-parity-v1/enriched-snapshots",
);
const SPICE = "recKRJjcPnb4tVDDS";
const SEVEN = [
  "gdi_s4912_ba7b1cb8a3ab",
  "gdi_s4912_b956aa67a4a2",
  "gdi_s4912_b6d9b1929c05",
  "gdi_s4912_cb2d71ab1b15",
  "gdi_s4912_cc2a8da6ca6e",
  "gdi_s4912_03deda917778",
  "gdi_s4912_3cafec77a21f",
];
const AVENTA = "gdi_sc_f255eb5e9202";

function loadApproved() {
  return SEVEN.map((id) => {
    const snap = JSON.parse(
      fs.readFileSync(path.join(ENRICHED, `${id}.json`), "utf8"),
    );
    // Stamp registry class onto opportunity for publish classification.
    const registry = JSON.parse(
      fs.readFileSync(
        path.join(
          ROOT,
          "data/group-demand-intelligence/published-inventory/registry/current-registry-v1.json",
        ),
        "utf8",
      ),
    );
    const row = registry.hotels?.[SPICE]?.opportunities?.[id];
    return {
      ...snap,
      hotelId: SPICE,
      commercialInventoryClass:
        snap.commercialInventoryClass ||
        row?.commercialClassification ||
        null,
      commercialClassification:
        snap.commercialClassification ||
        row?.commercialClassification ||
        null,
    };
  });
}

function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const approved = loadApproved();
  const approval = {
    authorized: true,
    authorizedBy: "founder_one_click_publish_dry_run",
    authorizedAt: new Date().toISOString(),
    authorizationPhrase:
      "FOUNDER_GO_ONE_CLICK_PUBLISH_DRY_RUN_ISOLATED_REGISTRY_ONLY",
    note: "Isolated temp registry — not production",
  };

  const dry = publishHotelInventoryOneClick({
    hotelId: SPICE,
    opportunities: approved,
    opportunityIds: SEVEN,
    approval,
    dryRun: true,
  });

  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-oneclick-"));
  const registry = createGdiPublishedInventoryRegistry({ root: tmpRoot });
  const applied = publishHotelInventoryOneClick({
    hotelId: SPICE,
    opportunities: approved,
    opportunityIds: SEVEN,
    approval,
    dryRun: false,
    registry,
    root: tmpRoot,
  });

  const merged = mergePublishedInventoryIntoOpportunities(SPICE, [], {
    registry,
    strictPublishedOnly: true,
  });
  const ids = merged.map((o) => String(o.id)).sort();
  const hangers = merged.find((o) => o.id === SEVEN[2]);
  const parity = assertManifestParity(dry.prepared.manifest.manifest, merged, {
    hotelId: SPICE,
  });

  const report = {
    ok:
      dry.ok === true &&
      applied.ok === true &&
      ids.length === 7 &&
      !ids.includes(AVENTA) &&
      hangers?.primaryContact?.name === "Khaleen Johnson" &&
      hangers?.secondaryContact?.name === "Aviann Murray" &&
      (hangers?.establishedFacts || []).length >= 7 &&
      (hangers?.qualificationQuestions || []).length >= 8 &&
      parity.ok === true &&
      applied.pointer?.previousPublicationId === null,
    contract: GDI_ONE_CLICK_HOTEL_PUBLISH_V1,
    dryRun: {
      ok: dry.ok,
      opportunityCount: dry.plan?.opportunityCount,
      classificationCounts: dry.plan?.classificationCounts,
      manifestHash: dry.plan?.manifestHash,
      failures: dry.prepared?.failures || [],
    },
    isolatedApply: {
      ok: applied.ok,
      publicationId: applied.pointer?.publicationId || null,
      writes: applied.writes,
      verifyOk: applied.verify?.ok === true,
      tmpRoot,
    },
    verifiedPayload: {
      ids,
      count: ids.length,
      aventaExcluded: !ids.includes(AVENTA),
      hangers: {
        primary: hangers?.primaryContact?.name || null,
        secondary: hangers?.secondaryContact?.name || null,
        facts: (hangers?.establishedFacts || []).length,
        qq: (hangers?.qualificationQuestions || []).length,
        locked: hangers?.publishedPresentationLocked === true,
      },
      classCounts: dry.plan?.classificationCounts,
      manifestParity: parity.ok,
    },
    production: {
      status: "NOT_EXECUTED",
      note: "Founder authorization required for live volume one-click publish",
      preserveShareToken: true,
      preserveOtherHotels: true,
    },
  };

  fs.writeFileSync(
    path.join(OUT, "03-spice-island-one-click-dry-run.json"),
    JSON.stringify(report, null, 2),
  );
  console.log(JSON.stringify(report, null, 2));
  process.exit(report.ok ? 0 : 1);
}

main();
