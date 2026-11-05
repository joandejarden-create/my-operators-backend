#!/usr/bin/env node
/**
 * Universal GDI presentation & publication parity — contract + pipeline tests.
 * No production writes. No tokens. No paid research.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_CANONICAL_OPPORTUNITY_CONTRACT_V1,
  assertCanonicalCustomerFacingOpportunity,
} from "../lib/group-demand-intelligence/contracts/gdi-canonical-opportunity-contract-v1.js";
import {
  GDI_PRESENTATION_CONTRACT_V1,
  GDI_PRESENTATION_SURFACES,
  GDI_SHARED_UI_ASSETS,
  certifyPresentationParity,
  buildPresentationListDto,
} from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import {
  GDI_PUBLICATION_CONTRACT_V1,
  detectPublicationContentLoss,
  preparePublicationSnapshot,
} from "../lib/group-demand-intelligence/contracts/gdi-publication-contract-v1.js";
import {
  resolveGdiPresentationCapabilities,
  GDI_UI_CAPABILITY,
} from "../lib/group-demand-intelligence/presentation/gdi-presentation-capabilities-v1.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-universal-presentation-publication-parity-v1",
);
const ENRICHED = path.join(
  ROOT,
  "reports/gdi-production-publication-full-content-parity-v1/enriched-snapshots",
);

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

function sampleEnriched() {
  const p = path.join(ENRICHED, "gdi_s4912_b6d9b1929c05.json");
  assert.ok(fs.existsSync(p), "hangers enriched snapshot required");
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function leanStub() {
  return {
    id: "gdi_lean_stub",
    title: "Acme Events",
    organizationName: "Acme Events",
    whatIsHappening: "Acme Events",
    whyThisHotel: "fit",
    recommendedNextStep: "call",
    primaryContact: { name: "Pat" },
    sources: [{ url: "https://example.com" }],
  };
}

check("contracts_export_versions", () => {
  assert.equal(
    GDI_CANONICAL_OPPORTUNITY_CONTRACT_V1,
    "gdi-canonical-opportunity-contract-v1",
  );
  assert.equal(GDI_PRESENTATION_CONTRACT_V1, "gdi-presentation-contract-v1");
  assert.equal(GDI_PUBLICATION_CONTRACT_V1, "gdi-publication-contract-v1");
  assert.equal(
    GDI_SHARED_UI_ASSETS.cacheBust,
    "gdi-single-inventory-parity-v1",
  );
});

check("html_asset_cache_bust_aligned", () => {
  const auth = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence.html"),
    "utf8",
  );
  const share = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence-share.html"),
    "utf8",
  );
  const bust = GDI_SHARED_UI_ASSETS.cacheBust;
  for (const [label, html] of [
    ["auth", auth],
    ["share", share],
  ]) {
    assert.ok(
      html.includes(`group-demand-intelligence.css?v=${bust}`),
      `${label} css cache-bust`,
    );
    assert.ok(
      html.includes(`ai-visibility-shared.css?v=${bust}`),
      `${label} shared visibility css`,
    );
    assert.ok(
      html.includes(`dealality-gdi-ui.js?v=${bust}`),
      `${label} renderer cache-bust`,
    );
  }
  assert.ok(auth.includes("app.js?v=" + bust));
  assert.ok(share.includes("share-app.js?v=" + bust));
  assert.ok(share.includes("gdi-share-mode"));
});

check("capabilities_auth_vs_share", () => {
  const auth = resolveGdiPresentationCapabilities("auth");
  assert.equal(auth.canRunResearch, true);
  assert.equal(auth.canViewAdminChrome, true);
  assert.equal(auth.canSwitchHotel, true);

  const share = resolveGdiPresentationCapabilities("share", {
    capabilities: ["CAN_READ_OPPORTUNITIES", "CAN_READ_OPPORTUNITY_DETAIL"],
  });
  assert.equal(share.canRunResearch, false);
  assert.equal(share.canEditOpportunity, false);
  assert.equal(share.canEnrichContact, false);
  assert.equal(share.canViewResearchHistory, false);
  assert.equal(share.canSwitchHotel, false);
  assert.equal(share.canViewAdminChrome, false);
  assert.equal(share.canBrowseOpportunities, true);
  assert.equal(share.canExportCSV, true);
  assert.equal(share.canValidate, false);

  const shareValidate = resolveGdiPresentationCapabilities("share", {
    capabilities: ["CAN_READ_OPPORTUNITIES", "CAN_VALIDATE"],
  });
  assert.equal(shareValidate[GDI_UI_CAPABILITY.canValidate], true);
});

check("lean_publication_blocked", () => {
  const prep = preparePublicationSnapshot(leanStub(), {
    hotelId: "recTEST",
    classification: "ACCOUNT_DEVELOPMENT",
  });
  assert.equal(prep.ok, false);
  assert.ok(prep.fails.length > 0);
});

check("enriched_publication_accepted", () => {
  const snap = sampleEnriched();
  const prep = preparePublicationSnapshot(snap, {
    hotelId: "recKRJjcPnb4tVDDS",
    classification: "ACCOUNT_DEVELOPMENT",
  });
  assert.equal(prep.ok, true, prep.fails.join(","));
  const canonical = assertCanonicalCustomerFacingOpportunity(snap);
  assert.equal(canonical.ok, true, canonical.fails.join(","));
});

check("silent_loss_detection", () => {
  const before = sampleEnriched();
  const after = {
    ...before,
    whatIsHappening: before.title || before.organizationName,
    potentialHotelBusiness: "",
    establishedFacts: [],
    secondaryContact: null,
  };
  const loss = detectPublicationContentLoss(before, after);
  assert.equal(loss.ok, false);
  assert.ok(loss.losses.some((l) => l.includes("whatIsHappening")));
  assert.ok(loss.losses.some((l) => l.includes("potentialHotelBusiness")));
  assert.ok(loss.losses.some((l) => l.includes("establishedFacts")));
});

check("auth_share_list_dto_parity_hangers", () => {
  const snap = sampleEnriched();
  const cert = certifyPresentationParity(snap, {
    hotelId: "recKRJjcPnb4tVDDS",
  });
  assert.equal(cert.ok, true, JSON.stringify(cert.fieldDiffs.slice(0, 5)));
  assert.equal(cert.share.mode, "read_only");
});

check("seven_spice_enriched_parity", () => {
  const ids = [
    "gdi_s4912_ba7b1cb8a3ab",
    "gdi_s4912_b956aa67a4a2",
    "gdi_s4912_b6d9b1929c05",
    "gdi_s4912_cb2d71ab1b15",
    "gdi_s4912_cc2a8da6ca6e",
    "gdi_s4912_03deda917778",
    "gdi_s4912_3cafec77a21f",
  ];
  for (const id of ids) {
    const p = path.join(ENRICHED, `${id}.json`);
    assert.ok(fs.existsSync(p), id);
    const snap = JSON.parse(fs.readFileSync(p, "utf8"));
    const cert = certifyPresentationParity(snap, {
      hotelId: "recKRJjcPnb4tVDDS",
    });
    assert.equal(cert.ok, true, `${id}: ${JSON.stringify(cert.fieldDiffs)}`);
    const pub = preparePublicationSnapshot(snap, {
      hotelId: "recKRJjcPnb4tVDDS",
    });
    assert.equal(pub.ok, true, `${id} pub: ${pub.fails.join(",")}`);
  }
});

check("universal_pipeline_surfaces", () => {
  const snap = sampleEnriched();
  const auth = projectOpportunityForPresentation(snap, {
    surface: GDI_PRESENTATION_SURFACES.LOCAL_AUTH,
    hotelId: "recKRJjcPnb4tVDDS",
  });
  const share = projectOpportunityForPresentation(snap, {
    surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
    hotelId: "recKRJjcPnb4tVDDS",
    shareResolve: { capabilities: ["CAN_READ_OPPORTUNITIES"] },
  });
  assert.equal(auth.capabilities.canRunResearch, true);
  assert.equal(share.capabilities.canRunResearch, false);
  assert.ok(auth.dto.title || auth.dto.displayTitle);
  assert.equal(share.dto.mode, "read_only");
  const authList = buildPresentationListDto(snap, {
    surface: GDI_PRESENTATION_SURFACES.PRODUCTION_AUTH,
    hotelId: "recKRJjcPnb4tVDDS",
  });
  assert.ok(authList.id || authList.title);
});

check("shared_renderer_single_module", () => {
  const authHtml = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence.html"),
    "utf8",
  );
  const shareHtml = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence-share.html"),
    "utf8",
  );
  assert.ok(authHtml.includes("dealality-gdi-ui.js"));
  assert.ok(shareHtml.includes("dealality-gdi-ui.js"));
  assert.ok(fs.existsSync(path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js")));
});

const failed = results.filter((r) => !r.ok);
const out = {
  ok: failed.length === 0,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  results,
  contracts: {
    canonical: GDI_CANONICAL_OPPORTUNITY_CONTRACT_V1,
    presentation: GDI_PRESENTATION_CONTRACT_V1,
    publication: GDI_PUBLICATION_CONTRACT_V1,
  },
};
fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "07b-automated-parity-test-results.json"),
  JSON.stringify(out, null, 2),
);
console.log(JSON.stringify(out, null, 2));
process.exit(failed.length ? 1 : 0);
