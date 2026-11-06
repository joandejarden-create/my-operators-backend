#!/usr/bin/env node
/**
 * Founder-authorized Spice presentation-parity apply.
 *
 * Builds complete locked publication candidates from the approved LOCAL GDI
 * (list + detail), then publishes into a working copy of the production
 * published-inventory root. Does not rotate share tokens or touch other hotels
 * beyond registry pointer update for Spice.
 *
 * Requires:
 *   --authorize-production-release
 *   --authorization-phrase=FOUNDER_GO_GDI_SPICE_PRESENTATION_PARITY_REPUBLISH_7246defe
 *   --root=<working copy of production GDI published-inventory>
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { publishHotelInventoryOneClick } from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";
import { createGdiPublishedInventoryRegistry } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const HOTEL = "recKRJjcPnb4tVDDS";
const BASE = process.env.GDI_LOCAL_BASE_URL || "http://localhost:8080";
const WANT_PHRASE =
  "FOUNDER_GO_GDI_SPICE_PRESENTATION_PARITY_REPUBLISH_7246defe";
const APPROVED_IDS = [
  "gdi_s4912_ba7b1cb8a3ab",
  "gdi_s4912_b956aa67a4a2",
  "gdi_s4912_b6d9b1929c05",
  "gdi_s4912_cb2d71ab1b15",
  "gdi_s4912_cc2a8da6ca6e",
  "gdi_s4912_03deda917778",
  "gdi_s4912_3cafec77a21f",
];

function arg(name, fallback = null) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  if (hit) return hit.slice(name.length + 3);
  return process.argv.includes(`--${name}`) ? true : fallback;
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${url}`);
  return res.json();
}

function mergeListOntoDetail(detail, listRow = {}) {
  return {
    ...detail,
    salesActionKey: listRow.salesActionKey || detail.salesActionKey,
    salesActionPillLabel:
      listRow.salesActionPillLabel || detail.salesActionPillLabel,
    priorityFilterKey: listRow.priorityFilterKey || detail.priorityFilterKey,
    priority: listRow.priority || detail.priority,
    tileContactName: listRow.tileContactName || detail.tileContactName,
    tileContactRoleLabel:
      listRow.tileContactRoleLabel || detail.tileContactRoleLabel,
    salesOpportunityQuickFilterKeys:
      listRow.salesOpportunityQuickFilterKeys ||
      detail.salesOpportunityQuickFilterKeys,
    hasVerifiedNamedRelevantContact:
      listRow.hasVerifiedNamedRelevantContact ??
      detail.hasVerifiedNamedRelevantContact,
    inventorySalesReady:
      listRow.inventorySalesReady ?? detail.inventorySalesReady,
    commercialInventoryClass:
      listRow.commercialInventoryClass ||
      detail.commercialInventoryClass ||
      (detail.id === "gdi_s4912_ba7b1cb8a3ab"
        ? "SALES_READY"
        : "ACCOUNT_DEVELOPMENT"),
    commercialClassification:
      listRow.commercialClassification ||
      detail.commercialClassification ||
      listRow.commercialInventoryClass ||
      detail.commercialInventoryClass,
  };
}

async function loadApprovedOpportunities() {
  const listDoc = await fetchJson(
    `${BASE}/api/group-demand-intelligence/hotels/${HOTEL}/opportunities`,
  );
  const list = listDoc.opportunities || [];
  const opps = [];
  for (const id of APPROVED_IDS) {
    const detailDoc = await fetchJson(
      `${BASE}/api/group-demand-intelligence/hotels/${HOTEL}/opportunities/${id}`,
    );
    const detail = detailDoc.opportunity || detailDoc;
    const listRow = list.find((x) => x.id === id) || {};
    opps.push(mergeListOntoDetail({ ...detail, id, hotelId: HOTEL }, listRow));
  }
  return { list, opps };
}

function peerCounts(registryRoot) {
  const reg = createGdiPublishedInventoryRegistry({ root: registryRoot });
  const doc = reg.loadRegistry();
  const hotels = doc?.hotels || {};
  const out = {};
  for (const [hotelId, h] of Object.entries(hotels)) {
    const ids = h?.currentPublication?.opportunityIds || [];
    out[hotelId] = {
      opportunityCount: Array.isArray(ids) ? ids.length : 0,
      publicationId: h?.currentPublication?.publicationId || null,
      manifestHash: h?.currentPublication?.manifestHash || null,
    };
  }
  return out;
}

async function main() {
  const authorize = arg("authorize-production-release") === true;
  const phrase = String(arg("authorization-phrase", "") || "");
  const dryRun = arg("dry-run") === true || arg("dryRun") === true;
  const registryRoot = path.resolve(String(arg("root", "") || ""));
  const outDir = path.join(
    ROOT,
    "reports/gdi-spice-presentation-parity-fix-v1",
  );
  fs.mkdirSync(outDir, { recursive: true });

  if (!authorize || phrase !== WANT_PHRASE) {
    const blocked = {
      ok: false,
      code: "AUTHORIZATION_REQUIRED",
      wantPhrase: WANT_PHRASE,
      gotPhrase: phrase || null,
    };
    fs.writeFileSync(
      path.join(outDir, "08-apply-blocked.json"),
      JSON.stringify(blocked, null, 2),
    );
    console.log(JSON.stringify(blocked, null, 2));
    process.exit(2);
  }
  if (!registryRoot || !fs.existsSync(registryRoot)) {
    throw new Error(`registry_root_missing:${registryRoot}`);
  }
  const registryFile = path.join(
    registryRoot,
    "registry",
    "current-registry-v1.json",
  );
  if (!fs.existsSync(registryFile)) {
    throw new Error(`registry_file_missing:${registryFile}`);
  }

  const peersBefore = peerCounts(registryRoot);
  const { opps } = await loadApprovedOpportunities();
  const approval = {
    authorized: true,
    authorizedBy: "founder",
    authorizedAt: new Date().toISOString(),
    authorizationPhrase: WANT_PHRASE,
    note: "Spice presentation parity republish — complete locked objects from approved local GDI",
  };

  const result = publishHotelInventoryOneClick({
    hotelId: HOTEL,
    opportunities: opps,
    opportunityIds: APPROVED_IDS,
    approval,
    dryRun,
    revokeMissingPublishedIds: true,
    root: registryRoot,
    sourceResearchReference: `spice-presentation-parity:${WANT_PHRASE}`,
  });

  const peersAfter = peerCounts(registryRoot);
  const report = {
    ok: result.ok === true,
    dryRun,
    authorizationPhrase: WANT_PHRASE,
    registryRoot,
    peersBefore,
    peersAfter,
    spicePublicationId: result.plan?.publicationId || result.publicationId || null,
    spiceManifestHash: result.plan?.manifestHash || null,
    classificationCounts: result.plan?.classificationCounts || null,
    candidatePreview: (result.prepared?.candidates || []).map((c) => ({
      opportunityId: c.opportunityId,
      classification: c.classification,
      salesActionPillLabel: c.snapshot?.salesActionPillLabel,
      priorityFilterKey: c.snapshot?.priorityFilterKey,
      tileContactName: c.snapshot?.tileContactName,
      locked: c.snapshot?.publishedPresentationLocked === true,
      verified: c.snapshot?.hasVerifiedNamedRelevantContact === true,
      facts: (c.snapshot?.establishedFacts || []).length,
      sources: (c.snapshot?.sources || []).length,
      quick: c.snapshot?.salesOpportunityQuickFilterKeys || [],
    })),
    result,
  };

  const outName = dryRun ? "08-apply-dry-run.json" : "08-apply-result.json";
  fs.writeFileSync(path.join(outDir, outName), JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  if (!report.ok) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
