#!/usr/bin/env node
/**
 * Build frozen Spice publication candidates from the approved local GDI API
 * (list + detail), then simulate standalone share projection (no Airtable merge).
 *
 * Dry-run only — does not write production volumes.
 */
import fs from "node:fs";
import path from "node:path";
import { prepareHotelPublicationCandidates } from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";

const HOTEL_ID = "recKRJjcPnb4tVDDS";
const BASE = process.env.GDI_LOCAL_BASE_URL || "http://localhost:8080";
const OUT_DIR = path.join(
  "reports",
  "gdi-spice-presentation-parity-fix-v1",
);

const APPROVED_IDS = [
  "gdi_s4912_ba7b1cb8a3ab", // CARPHA
  "gdi_s4912_03deda917778", // Savor 10
  "gdi_s4912_cc2a8da6ca6e", // St. James
  "gdi_s4912_cb2d71ab1b15", // A'yesha
  "gdi_s4912_b6d9b1929c05", // Hangers
  "gdi_s4912_b956aa67a4a2", // Beautiful Weddings
  "gdi_s4912_3cafec77a21f", // Kenwood
];

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

async function main() {
  const listDoc = await fetchJson(
    `${BASE}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities`,
  );
  const list = listDoc.opportunities || [];
  const opps = [];
  for (const id of APPROVED_IDS) {
    const detailDoc = await fetchJson(
      `${BASE}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities/${id}`,
    );
    const detail = detailDoc.opportunity || detailDoc;
    const listRow = list.find((x) => x.id === id) || {};
    opps.push(mergeListOntoDetail({ ...detail, id, hotelId: HOTEL_ID }, listRow));
  }

  const prepared = prepareHotelPublicationCandidates(HOTEL_ID, opps, {
    opportunityIds: APPROVED_IDS,
  });

  const simParity = [];
  if (prepared.ok) {
    for (const c of prepared.candidates) {
      const alone = {
        ...c.snapshot,
        id: c.opportunityId,
        hotelId: HOTEL_ID,
        gdiPublishedInventoryRegistry: true,
        publishedPresentationLocked: true,
      };
      const dto = projectOpportunityForPresentation(alone, {
        hotelId: HOTEL_ID,
        surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
        view: "list",
      }).dto;
      const local = list.find((x) => x.id === c.opportunityId);
      const salesOk = local.salesActionPillLabel === dto.salesActionPillLabel;
      const priOk = local.priorityFilterKey === dto.priorityFilterKey;
      const contactLocal =
        local.tileContactName || local.primaryContact?.name || null;
      const contactSim = dto.tileContactName || dto.primaryContact?.name || null;
      simParity.push({
        id: c.opportunityId,
        title: local.title,
        salesOk,
        priOk,
        contactOk: String(contactLocal || "") === String(contactSim || ""),
        sales: { local: local.salesActionPillLabel, sim: dto.salesActionPillLabel },
        priority: { local: local.priorityFilterKey, sim: dto.priorityFilterKey },
        contact: { local: contactLocal, sim: contactSim },
        quick: {
          local: local.salesOpportunityQuickFilterKeys || [],
          sim: dto.salesOpportunityQuickFilterKeys || [],
        },
      });
    }
  }

  const report = {
    ok: prepared.ok === true && simParity.every((r) => r.salesOk && r.priOk && r.contactOk),
    hotelId: HOTEL_ID,
    base: BASE,
    preparedOk: prepared.ok,
    failureCount: (prepared.failures || []).length,
    failures: prepared.failures || [],
    candidateCount: (prepared.candidates || []).length,
    candidates: (prepared.candidates || []).map((c) => ({
      opportunityId: c.opportunityId,
      classification: c.classification,
      contentHash: c.contentHash,
      salesActionKey: c.snapshot.salesActionKey,
      salesActionPillLabel: c.snapshot.salesActionPillLabel,
      priorityFilterKey: c.snapshot.priorityFilterKey,
      tileContactName: c.snapshot.tileContactName,
      hasVerifiedNamedRelevantContact:
        c.snapshot.hasVerifiedNamedRelevantContact,
      salesOpportunityQuickFilterKeys:
        c.snapshot.salesOpportunityQuickFilterKeys,
      facts: (c.snapshot.establishedFacts || []).length,
      sources: (c.snapshot.sources || []).length,
    })),
    simParity,
    localOrder: list.map((o) => o.title),
    authorizationRequired:
      "FOUNDER_GO_GDI_SPICE_PRESENTATION_PARITY_REPUBLISH_<sha8> after code deploy",
    note: "Dry-run only. Production volume write requires explicit founder authorization.",
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const outPath = path.join(OUT_DIR, "01-dry-run-frozen-candidates.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  console.log(`\nWrote ${outPath}`);
  process.exit(report.ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
