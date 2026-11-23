#!/usr/bin/env node
/**
 * Track C V7 — READ-ONLY relationship integrity audit.
 * No Airtable writes. No HPC creates. CUSTOMER_BLOCKED.
 *
 * Audits the four verified Mexico hotels + parent/deferred entities
 * against Discovery Inventory, Discovery Findings, HPC, and Track A corpus.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  HPC_TABLE_ID,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-c-v7-relationship-integrity");
fs.mkdirSync(OUT, { recursive: true });
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) {
  console.error("NO_AIRTABLE_TOKEN");
  process.exit(2);
}
const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const inventoryTableId = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const findingsTableId = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];

/** Track A RESEARCH_CANDIDATES corpus (authoritative DealSignal V1 finding IDs) */
const TRACK_A_CORPUS = new Set([
  "find_ds_cala_v1_finn_internalization_franchise_2026",
  "find_ds_cala_v1_finn_courtyard_pdc_committed_2026",
  "find_ds_cala_v1_fiho_yucatan_resort_lease_2026",
  "find_ds_cala_v1_cabo_rojo_puntacana_committed_2026",
  "find_ds_cala_v1_santander_concession_watch_2026",
  "find_ds_cala_v1_br_fii_hotel_context_2026",
]);

const TRACK_A_DISC = new Set([
  "disc_ds_cala_v1_finn_internalization_parent_2026",
  "disc_ds_cala_v1_finn_courtyard_pdc_2026",
  "disc_ds_cala_v1_fiho_yucatan_resort_2026",
  "disc_ds_cala_v1_cabo_rojo_puntacana_2026",
  "disc_ds_cala_v1_santander_concession_watch_2026",
  "disc_ds_cala_v1_br_fii_hotel_context_2026",
]);

const AUDIT_ROWS = [
  {
    label: "Courtyard Playa del Carmen",
    discoveryPropertyId: "disc_ds_cala_v1_finn_courtyard_pdc_2026",
    findingIdClaimed: "find_ds_cala_v1_finn_courtyard_pdc_committed_2026",
    expectedHpc: "rechcqksn3VfFD3c6",
    siblingHpc: "recf65FZ1FuVVqIrz",
    kind: "HOTEL",
  },
  {
    label: "The Yucatán Resort Playa del Carmen",
    discoveryPropertyId: "disc_ds_cala_v1_fiho_yucatan_resort_2026",
    findingIdClaimed: "find_ds_cala_v1_fiho_yucatan_resort_lease_2026",
    expectedHpc: "recY9Cxxo1iAbJgOt",
    kind: "HOTEL",
  },
  {
    label: "Holiday Inn Monterrey Valle",
    discoveryPropertyId: "disc_ds_cala_v1_finn_hi_monterrey_valle_2026",
    findingIdClaimed: "find_ds_cala_v1_finn_hi_monterrey_valle_2026",
    expectedHpc: "recJRNuLr8fEiGImt",
    kind: "HOTEL",
    trackCInventedFinding: true,
  },
  {
    label: "Holiday Inn Puebla La Noria",
    discoveryPropertyId: "disc_ds_cala_v1_finn_hi_puebla_la_noria_2026",
    findingIdClaimed: "find_ds_cala_v1_finn_hi_puebla_la_noria_2026",
    expectedHpc: "recfpT5RJBZAORXs2",
    kind: "HOTEL",
    trackCInventedFinding: true,
  },
  {
    label: "Fibra Inn internalization parent",
    discoveryPropertyId: "disc_ds_cala_v1_finn_internalization_parent_2026",
    findingIdClaimed: "find_ds_cala_v1_finn_internalization_franchise_2026",
    expectedHpc: null,
    kind: "PARENT",
  },
  {
    label: "Cabo Rojo destination parent",
    discoveryPropertyId: "disc_ds_cala_v1_cabo_rojo_puntacana_2026",
    findingIdClaimed: "find_ds_cala_v1_cabo_rojo_puntacana_committed_2026",
    expectedHpc: null,
    kind: "PARENT",
  },
  {
    label: "Santander concession watch",
    discoveryPropertyId: "disc_ds_cala_v1_santander_concession_watch_2026",
    findingIdClaimed: "find_ds_cala_v1_santander_concession_watch_2026",
    expectedHpc: null,
    kind: "UNRESOLVED",
  },
];

function writeJson(name, obj) {
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}

async function airtableGet(tableId, recordId) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) return { ok: false, status: res.status, json: await res.json().catch(() => ({})) };
  return { ok: true, json: await res.json() };
}

async function findByFormula(tableId, formula, max = 5) {
  const url =
    `https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}` +
    `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=${max}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json();
  return { ok: res.ok, records: json.records || [], error: json.error || null };
}

async function findInventory(discId) {
  return findByFormula(
    inventoryTableId,
    `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${discId}"`,
    3,
  );
}

async function findFinding(findingId) {
  return findByFormula(findingsTableId, `{${MAP_DISCOVERY_FINDINGS.findingId}}="${findingId}"`, 3);
}

async function findFindingsLinkedToInventory(inventoryRecordId) {
  // Linked record filter
  const formula = `FIND("${inventoryRecordId}", ARRAYJOIN({${MAP_DISCOVERY_FINDINGS.discoveryProperty}}))`;
  return findByFormula(findingsTableId, formula, 10);
}

function summarizeInventory(rec) {
  if (!rec) return null;
  const f = rec.fields || {};
  return {
    airtableId: rec.id,
    discoveryPropertyId: f[MAP_DISCOVERY_INVENTORY.discoveryPropertyId],
    hotelNameRaw: f[MAP_DISCOVERY_INVENTORY.hotelNameRaw],
    hpcMatchStatus: f[MAP_DISCOVERY_INVENTORY.hpcMatchStatus],
    hpcHotel: f[MAP_DISCOVERY_INVENTORY.hpcHotel] || [],
    currentBrand: f[MAP_DISCOVERY_INVENTORY.currentBrand],
    historicalBrand: f[MAP_DISCOVERY_INVENTORY.historicalBrand],
    historicalAliases: f[MAP_DISCOVERY_INVENTORY.historicalAliases],
    identityClass: f[MAP_DISCOVERY_INVENTORY.identityClass],
    country: f[MAP_DISCOVERY_INVENTORY.country],
    city: f[MAP_DISCOVERY_INVENTORY.city],
    address: f[MAP_DISCOVERY_INVENTORY.address],
    notesSnippet: String(f[MAP_DISCOVERY_INVENTORY.notesQuarantine] || "").slice(0, 280),
    publicationState: f[MAP_DISCOVERY_INVENTORY.publicationState],
  };
}

function summarizeFinding(rec) {
  if (!rec) return null;
  const f = rec.fields || {};
  return {
    airtableId: rec.id,
    findingId: f[MAP_DISCOVERY_FINDINGS.findingId],
    discoveryProperty: f[MAP_DISCOVERY_FINDINGS.discoveryProperty] || [],
    findingType: f[MAP_DISCOVERY_FINDINGS.findingType],
    fieldName: f[MAP_DISCOVERY_FINDINGS.fieldName],
    valueObserved: String(f[MAP_DISCOVERY_FINDINGS.valueObserved] || "").slice(0, 200),
    sourceUrl: f[MAP_DISCOVERY_FINDINGS.sourceUrl],
    verificationStatus: f[MAP_DISCOVERY_FINDINGS.verificationStatus],
    hpcPromotionStatus: f[MAP_DISCOVERY_FINDINGS.hpcPromotionStatus],
    relatedOrgName: f[MAP_DISCOVERY_FINDINGS.relatedOrgName],
    notesSnippet: String(f[MAP_DISCOVERY_FINDINGS.notes] || "").slice(0, 200),
  };
}

function summarizeHpc(rec) {
  if (!rec?.ok) return { ok: false, status: rec?.status };
  const f = rec.json.fields || {};
  return {
    ok: true,
    id: rec.json.id,
    name: f["Property Name"],
    canonicalName: f["Canonical Property Name"],
    brand: f["Current Brand"],
    brandFamily: f["Brand Family"],
    family: f["Family / Source Family"],
    address: f.Address,
    city: f.City,
    country: f.Country,
    officialUrl: f["Official Property URL"],
    affiliationStatus: f["Affiliation Status"],
    productionUseStatus: f["Production Use Status"],
    radarDisplayStatus: f["Radar Display Status"],
  };
}

async function main() {
  const results = [];
  const findingMismatchCauses = [];

  for (const row of AUDIT_ROWS) {
    const inv = await findInventory(row.discoveryPropertyId);
    const invRec = inv.records[0] || null;
    const invSum = summarizeInventory(invRec);

    const findingLookup = await findFinding(row.findingIdClaimed);
    const findingRec = findingLookup.records[0] || null;
    const findingSum = summarizeFinding(findingRec);

    let linkedFindings = { records: [] };
    if (invRec) {
      linkedFindings = await findFindingsLinkedToInventory(invRec.id);
    }

    let hpcSum = null;
    let hpcLinkOk = null;
    if (row.expectedHpc) {
      hpcSum = summarizeHpc(await airtableGet(HPC_TABLE_ID, row.expectedHpc));
      const linked = invSum?.hpcHotel || [];
      hpcLinkOk = linked[0] === row.expectedHpc && invSum?.hpcMatchStatus === "MATCHED";
    }

    let siblingSum = null;
    if (row.siblingHpc) {
      siblingSum = summarizeHpc(await airtableGet(HPC_TABLE_ID, row.siblingHpc));
    }

    const inTrackAFindings = TRACK_A_CORPUS.has(row.findingIdClaimed);
    const inTrackADisc = TRACK_A_DISC.has(row.discoveryPropertyId);
    const findingExistsLive = Boolean(findingRec);

    let mismatchCause = null;
    if (row.trackCInventedFinding) {
      mismatchCause = {
        code: "TRACK_C_SYNTHETIC_FINDING_ID_NEVER_IN_TRACK_A_CORPUS",
        detail:
          "Track C invented findingId + discoveryPropertyId for identity linking. Track A CALA Discovery V1 RESEARCH_CANDIDATES never emitted hotel-level findings for Monterrey Valle or Puebla La Noria — only portfolio-parent FINN internalization + named Courtyard/Yucatán/Cabo/Santander. Identity inventory exists; DealSignal finding row does not.",
      };
      findingMismatchCauses.push({
        hotel: row.label,
        claimedFindingId: row.findingIdClaimed,
        claimedDiscoveryPropertyId: row.discoveryPropertyId,
        ...mismatchCause,
        inventoryExists: Boolean(invRec),
        findingExistsLive,
        recommendedRepair:
          "Map hotel cards to parent finding find_ds_cala_v1_finn_internalization_franchise_2026 (evidence of HI conversions/franchise renewals) OR create Track A–authorized hotel-level findings later — do not invent duplicate navigation findings. Preserve Track C discoveryPropertyId inventory as identity SoT with HPC link.",
      });
    } else if (inTrackAFindings && !findingExistsLive) {
      mismatchCause = {
        code: "TRACK_A_FINDING_ID_NOT_IN_LIVE_AIRTABLE",
        detail: "Finding ID present in Track A corpus JSON but missing from HI Discovery Findings table.",
      };
      findingMismatchCauses.push({
        hotel: row.label,
        claimedFindingId: row.findingIdClaimed,
        ...mismatchCause,
        inventoryExists: Boolean(invRec),
        findingExistsLive,
        recommendedRepair: "Re-link or re-persist Track A finding only with founder-approved apply — out of scope for V7 without approval.",
      });
    } else if (inTrackAFindings && findingExistsLive) {
      // check if finding links to inventory
      const linkedToInv =
        invRec &&
        (findingSum.discoveryProperty || []).includes(invRec.id);
      if (!linkedToInv && invRec) {
        mismatchCause = {
          code: "FINDING_EXISTS_BUT_NOT_LINKED_TO_INVENTORY",
          detail: "Live finding exists but Discovery Property link may not point at matched inventory row.",
        };
        findingMismatchCauses.push({
          hotel: row.label,
          claimedFindingId: row.findingIdClaimed,
          ...mismatchCause,
          findingAirtableId: findingSum.airtableId,
          inventoryAirtableId: invRec.id,
          findingLinkedInventoryIds: findingSum.discoveryProperty,
          recommendedRepair:
            "Non-destructive PATCH: set Finding.Discovery Property → inventory record (preserve Finding ID). Requires founder approval for live write.",
        });
      }
    }

    const heNav = row.expectedHpc
      ? {
          deepLinkTarget: `http://localhost:8080/app#/opportunity-radar (open HE by HPC ${row.expectedHpc})`,
          hpcRecordId: row.expectedHpc,
          status: hpcSum?.ok ? "HPC_RESOLVABLE" : "HPC_MISSING",
          note: "HE open-by-id is client-owned; Track C provides HPC ID only",
        }
      : { status: "NOT_APPLICABLE_NO_HOTEL_HPC" };

    results.push({
      label: row.label,
      kind: row.kind,
      discoveryPropertyId: row.discoveryPropertyId,
      findingIdClaimed: row.findingIdClaimed,
      trackACorpusHasFindingId: inTrackAFindings,
      trackACorpusHasDiscoveryPropertyId: inTrackADisc,
      trackCInventedFinding: Boolean(row.trackCInventedFinding),
      inventory: invSum,
      inventoryDuplicateCount: inv.records.length,
      findingByClaimedId: findingSum,
      findingsLinkedToInventory: (linkedFindings.records || []).map(summarizeFinding),
      hpc: hpcSum,
      hpcInventoryLinkOk: hpcLinkOk,
      siblingHpc: siblingSum,
      mismatchCause,
      hotelExplorer: heNav,
    });
  }

  // Parent finding content check — does internalization finding mention Monterrey/Puebla?
  const parentFinding = await findFinding("find_ds_cala_v1_finn_internalization_franchise_2026");
  const parentSum = summarizeFinding(parentFinding.records[0] || null);

  const report = {
    asOf: new Date().toISOString(),
    mode: "READ_ONLY_AUDIT",
    publicationState: "CUSTOMER_BLOCKED",
    airtableWrites: 0,
    pr557: {
      status: "MERGED",
      mergeCommit: "06efe1eb7c3e5b67296608d6bae9f269b304bc37",
      note: "Do not repeat identity-matching; this audit verifies relationships only",
    },
    summary: {
      hotelsAudited: results.filter((r) => r.kind === "HOTEL").length,
      hotelsWithHpcLinkOk: results.filter((r) => r.hpcInventoryLinkOk === true).length,
      claimedFindingsPresentLive: results.filter((r) => r.findingByClaimedId).length,
      trackCSyntheticFindingIds: results.filter((r) => r.trackCInventedFinding).length,
      mismatchCauses: findingMismatchCauses.length,
    },
    findingMismatchCauses,
    parentFindingInternalization: parentSum,
    rows: results,
    proposedRelationshipCorrections: [
      {
        id: "CORR_1_MONTERREY_PUEBLA_FINDING_ALIGNMENT",
        severity: "NAVIGATION",
        action: "MAPPING_ONLY",
        liveWriteRequired: false,
        description:
          "Treat find_ds_cala_v1_finn_hi_monterrey_valle_2026 and find_ds_cala_v1_finn_hi_puebla_la_noria_2026 as NON-CORPUS alias labels stored only in inventory notes — not DealSignal Finding IDs. DealSignal card navigation for these hotels should use parent finding find_ds_cala_v1_finn_internalization_franchise_2026 (historical franchise/conversion evidence) while hotel identity continues via disc_ds_cala_v1_finn_hi_* → HPC. Optionally later: Track A may authorize true hotel-level findings with new IDs — do not duplicate now.",
      },
      {
        id: "CORR_2_VERIFY_COURTYARD_YUCATAN_FINDING_LINKS",
        severity: "RELATIONSHIP",
        action: "IF_UNLINKED_PATCH_FINDING_DISCOVERY_PROPERTY",
        liveWriteRequired: true,
        requiresFounderApproval: true,
        description:
          "If Courtyard/Yucatán findings exist but Discovery Property link ≠ matched inventory, PATCH link only (preserve Finding ID, source URL, evidence). No new findings.",
      },
      {
        id: "CORR_3_COURTYARD_WYNDHAM_STEWARDSHIP",
        severity: "IDENTITY",
        action: "ALIAS_RELATIONSHIP_DOC_ONLY",
        liveWriteRequired: false,
        requiresFounderApprovalForMerge: true,
        description:
          "Canonical current brand record = rechcqksn3VfFD3c6 (Courtyard). Historical = recf65FZ1FuVVqIrz (Wyndham Garden). Same physical Playacar property. Recommend steward alias / DUPLICATE_ALIAS inventory note; do NOT merge/delete without separate founder approval.",
      },
    ],
    unresolvedEvidenceRequired: [
      {
        entity: "Santander Bucaramanga",
        need: "Official SECOP II pliego confirming hotel use of the departmental inmueble",
        doNot: "Create HPC from aggregator listing alone",
      },
      {
        entity: "Cabo Rojo",
        need: "Named individual hotel brands/operators within destination",
        doNot: "Create destination as hotel HPC",
      },
      {
        entity: "Fibra Inn internalization portfolio",
        need: "None for hotel identity — keep as PARENT; hotel-level facts live on Courtyard/HI records",
        doNot: "Treat parent as hotel",
      },
      {
        entity: "Yucatán Resort operator",
        need: "FIHO disclosure naming lease counterparty / brand under arrendamiento",
        doNot: "Infer Playa Hotels & Resorts as current operator after Apr 2026 suspension",
      },
    ],
    requestedAirtableMutationsForApproval: [],
  };

  // Populate mutation requests only when audit proves unlink
  for (const r of results) {
    if (r.mismatchCause?.code === "FINDING_EXISTS_BUT_NOT_LINKED_TO_INVENTORY") {
      report.requestedAirtableMutationsForApproval.push({
        type: "PATCH_FINDING_DISCOVERY_PROPERTY_LINK",
        findingId: r.findingIdClaimed,
        findingAirtableId: r.findingByClaimedId?.airtableId,
        targetInventoryAirtableId: r.inventory?.airtableId,
        preserve: ["Finding ID", "Source URL", "Evidence Snippet", "Value Observed"],
        destructive: false,
        status: "AWAITING_FOUNDER_APPROVAL",
      });
    }
  }

  writeJson("RELATIONSHIP_INTEGRITY_AUDIT.json", report);

  const md = `# Track C V7 — Relationship Integrity Audit (READ-ONLY)

**As of:** ${report.asOf}  
**Airtable writes:** 0 · **CUSTOMER_BLOCKED** · PR #557 MERGED (not re-run)

## Finding-ID mismatch causes

${findingMismatchCauses
  .map(
    (c) =>
      `### ${c.hotel}\n- **Cause:** \`${c.code}\`\n- ${c.detail}\n- Inventory exists: ${c.inventoryExists} · Finding live: ${c.findingExistsLive}\n- **Repair:** ${c.recommendedRepair}\n`,
  )
  .join("\n") || "_None beyond synthetic Track C IDs / verified links._"}

## Hotel link matrix

| Hotel | Disc ID in Track A? | Finding ID in Track A? | Finding live? | HPC link OK? |
|-------|---------------------|------------------------|---------------|--------------|
${results
  .filter((r) => r.kind === "HOTEL")
  .map(
    (r) =>
      `| ${r.label} | ${r.trackACorpusHasDiscoveryPropertyId} | ${r.trackACorpusHasFindingId} | ${Boolean(r.findingByClaimedId)} | ${r.hpcInventoryLinkOk} |`,
  )
  .join("\n")}

## STOP

No destructive identity changes. No expanded live writes. Founder approval required for any Airtable mutations listed in JSON.
`;
  fs.writeFileSync(path.join(OUT, "RELATIONSHIP_INTEGRITY_AUDIT.md"), md);

  console.log(
    JSON.stringify(
      {
        done: true,
        writes: 0,
        summary: report.summary,
        mismatchCauses: findingMismatchCauses.map((c) => ({ hotel: c.hotel, code: c.code })),
        mutationsAwaitingApproval: report.requestedAirtableMutationsForApproval.length,
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
