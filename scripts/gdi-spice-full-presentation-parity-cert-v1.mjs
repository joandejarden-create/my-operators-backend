#!/usr/bin/env node
/**
 * Spice Island seven-opportunity full-field presentation parity certification.
 *
 * Compares local auth detail vs production share detail for every customer-
 * visible presentation field. Does NOT write production. Does NOT republish.
 *
 * Usage:
 *   node scripts/gdi-spice-full-presentation-parity-cert-v1.mjs
 *   LOCAL_BASE=http://localhost:8080 PROD_BASE=https://... node scripts/...
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  compareFullCustomerPresentations,
  buildAuthoritativeCustomerPresentation,
  PARITY_RESULT,
} from "../lib/group-demand-intelligence/presentation/gdi-full-presentation-parity-v1.js";
import {
  dedupeOpportunityContacts,
  validateCustomerPresentationIntegrity,
} from "../lib/group-demand-intelligence/presentation/gdi-customer-presentation-integrity-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "gdi-spice-presentation-parity-fix-v1",
);

const HOTEL_ID = "recKRJjcPnb4tVDDS";
const LOCAL_BASE = process.env.LOCAL_BASE || "http://localhost:8080";
const PROD_BASE =
  process.env.PROD_BASE ||
  "https://my-operators-backend-production.up.railway.app";
const SHARE =
  process.env.SPICE_SHARE_TOKEN ||
  "gdishare.v1.eyJ2IjoxLCJ0aWQiOiJnZGlzaHRfc3BpY2VfaXNsYW5kX213M29sYXE1IiwiaG90ZWxJZCI6InJlY0tSSmpjUG5iNHRWRERTIiwic3VyZmFjZXMiOlsiYnJpZWYiLCJvcHBvcnR1bml0aWVzIiwib3Bwb3J0dW5pdHlfZGV0YWlsIiwic3VtbWFyeSJdLCJjYXBhYmlsaXRpZXMiOlsiQ0FOX1JFQURfQlJJRUYiLCJDQU5fUkVBRF9PUFBPUlRVTklUSUVTIiwiQ0FOX1JFQURfT1BQT1JUVU5JVFlfREVUQUlMIiwiQ0FOX1JFQURfU1VNTUFSWSJdLCJtb2RlIjoicmVhZF9vbmx5IiwiaWF0IjoxNzkzODk4MzEyLCJleHAiOm51bGx9.opmlDXOVDHAIq8ActTKfQrNs2B9yitTX_o6Kqo8Hc4k";

const EXPECTED_IDS = [
  "gdi_s4912_03deda917778", // Savor
  "gdi_s4912_ba7b1cb8a3ab", // CARPHA
  "gdi_s4912_cb2d71ab1b15", // A'yesha
];

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} ${url}`);
  }
  return res.json();
}

function summarizeContacts(o = {}) {
  const d = dedupeOpportunityContacts(o);
  return {
    primary: d.primaryContact?.name || null,
    primaryRole: d.primaryContact?.role || d.primaryContact?.title || null,
    secondary: d.secondaryContact?.name || null,
    secondaryRole:
      d.secondaryContact?.role || d.secondaryContact?.title || null,
    backupCount: (d.backupContacts || []).length,
    twoPrimary:
      d.scalingTwoContactPresentation?.primary?.name ||
      o.scalingTwoContactPresentation?.primary?.name ||
      null,
    twoPrimaryNamed:
      d.scalingTwoContactPresentation?.primary?.customerFacingNamed ??
      o.scalingTwoContactPresentation?.primary?.customerFacingNamed ??
      null,
  };
}

function commercialSummary(o = {}) {
  return {
    inventoryClass:
      o.commercialInventoryClass || o.commercialClassification || null,
    priority: o.priorityFilterKey || o.priority || null,
    salesAction: o.salesActionPillLabel || o.salesActionKey || null,
    sourcing:
      o.hotelSourcingStatusLabel ||
      o.hotelSourcingStatusNormalized ||
      o.hotelSourcingStatus ||
      null,
    recommendedNextStep: o.recommendedNextStep || o.recommendedAction || null,
  };
}

async function loadLocalDetail(id) {
  const data = await fetchJson(
    `${LOCAL_BASE}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities/${id}`,
  );
  return data.opportunity || data;
}

async function loadProdShareDetail(id) {
  const data = await fetchJson(
    `${PROD_BASE}/api/group-demand-intelligence/share/hotels/${HOTEL_ID}/opportunities/${id}?share=${encodeURIComponent(SHARE)}`,
  );
  return data.opportunity || data;
}

async function loadLocalList() {
  const data = await fetchJson(
    `${LOCAL_BASE}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities`,
  );
  return data.opportunities || data.items || [];
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const list = await loadLocalList();
  const ids = list.map((o) => String(o.id));
  if (ids.length !== 7) {
    throw new Error(`Expected 7 local Spice opportunities, got ${ids.length}`);
  }

  const rows = [];
  for (const id of ids) {
    const local = await loadLocalDetail(id);
    const prod = await loadProdShareDetail(id);
    const localAuth = buildAuthoritativeCustomerPresentation(local, {
      hotelId: HOTEL_ID,
    });
    const prodAuth = buildAuthoritativeCustomerPresentation(prod, {
      hotelId: HOTEL_ID,
      lock: true,
    });
    const integrityLocal = validateCustomerPresentationIntegrity(
      dedupeOpportunityContacts(local),
    );
    const integrityProd = validateCustomerPresentationIntegrity(
      dedupeOpportunityContacts(prod),
    );
    const cmp = compareFullCustomerPresentations(local, prod, {
      hotelId: HOTEL_ID,
    });

    // Contact integrity: named primary must not be unidentified; no dupes.
    const contactIntegrity = {
      local: summarizeContacts(local),
      prod: summarizeContacts(prod),
      localOk: integrityLocal.ok,
      prodOk: integrityProd.ok,
      localFails: integrityLocal.fails,
      prodFails: integrityProd.fails,
    };

    rows.push({
      opportunityId: id,
      title: local.title || local.displayTitle || prod.title || null,
      focus: EXPECTED_IDS.includes(id),
      localHash: localAuth.contentHash,
      prodHash: prodAuth.contentHash,
      fullFieldParity: cmp.result,
      fullFieldOk: cmp.ok,
      diffCount: (cmp.diffs || []).length,
      diffs: (cmp.diffs || []).slice(0, 40),
      contactIntegrity,
      actionIntegrity: {
        local: commercialSummary(local),
        prod: commercialSummary(prod),
        match:
          JSON.stringify(commercialSummary(local)) ===
          JSON.stringify(commercialSummary(prod)),
      },
      evidenceIntegrity: {
        localSourceCount: (local.sources || []).length,
        prodSourceCount: (prod.sources || []).length,
        localFacts: (local.establishedFacts || []).length,
        prodFacts: (prod.establishedFacts || []).length,
      },
      knownDefectHints: {
        prodMissingTwoContact: !prod.scalingTwoContactPresentation?.primary,
        ayeshaAccentName:
          id === "gdi_s4912_cb2d71ab1b15"
            ? local.primaryContact?.name
            : undefined,
      },
    });
  }

  const allFullPass = rows.every(
    (r) => r.fullFieldOk && r.fullFieldParity === PARITY_RESULT.FULL_PARITY_PASS,
  );
  const allIntegrity = rows.every(
    (r) => r.contactIntegrity.localOk && r.contactIntegrity.prodOk,
  );
  const ayesha = rows.find((r) => r.opportunityId === "gdi_s4912_cb2d71ab1b15");
  const carpha = rows.find((r) => r.opportunityId === "gdi_s4912_ba7b1cb8a3ab");
  const savor = rows.find((r) => r.opportunityId === "gdi_s4912_03deda917778");

  const status = allFullPass && allIntegrity
    ? "GDI_UNIVERSAL_FULL_PRESENTATION_PARITY_READY_FOR_DEPLOY_AUTH"
    : "GDI_FULL_PRESENTATION_PARITY_BLOCKED_WITH_EXACT_REASONS";

  const report = {
    contract: "GDI_FULL_PRESENTATION_PARITY_V1",
    hotelId: HOTEL_ID,
    hotelName: "Spice Island Beach Resort",
    generatedAt: new Date().toISOString(),
    localBase: LOCAL_BASE,
    prodBase: PROD_BASE,
    opportunityCount: rows.length,
    status,
    summary: {
      allFullPass,
      allIntegrity,
      passCount: rows.filter((r) => r.fullFieldOk).length,
      failCount: rows.filter((r) => !r.fullFieldOk).length,
      priorSubsetPassNote:
        "Prior PRODUCTION_GDI_FULL_PARITY_PASS compared selected tile/drawer fields only and must be labeled TILE_DRAWER_FIELD_SUBSET_PASS going forward.",
    },
    focus: {
      ayesha: ayesha
        ? {
            fullFieldParity: ayesha.fullFieldParity,
            localHash: ayesha.localHash,
            prodHash: ayesha.prodHash,
            contacts: ayesha.contactIntegrity,
            commercial: ayesha.actionIntegrity,
            diffs: ayesha.diffs,
          }
        : null,
      carpha: carpha
        ? {
            fullFieldParity: carpha.fullFieldParity,
            localHash: carpha.localHash,
            prodHash: carpha.prodHash,
            commercial: carpha.actionIntegrity,
          }
        : null,
      savor: savor
        ? {
            fullFieldParity: savor.fullFieldParity,
            localHash: savor.localHash,
            prodHash: savor.prodHash,
            commercial: savor.actionIntegrity,
          }
        : null,
    },
    opportunities: rows,
    productionSafety: {
      republishAuthorized: false,
      shareTokenPreserved: true,
      snapshotOverwriteBlockedUntilFounderAuth: true,
    },
  };

  const outJson = path.join(OUT_DIR, "13-full-presentation-parity-cert.json");
  fs.writeFileSync(outJson, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  console.error(`Wrote ${outJson}`);
  process.exit(allFullPass && allIntegrity ? 0 : 2);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
