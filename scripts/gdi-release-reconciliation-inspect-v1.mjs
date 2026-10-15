/**
 * Read-only GDI release reconciliation inspector.
 * Fresh local (Airtable-backed :8080) + production list/summary + freeze/scratch diffs.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  isCustomerFacingOpportunity,
} from "../lib/group-demand-intelligence/customer-visibility.js";
import { isReviewedProspectCustomerVisible } from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";
import {
  buildHotelActivityHistory,
  loadHotelActivityLedger,
} from "../lib/group-demand-intelligence/hotel-activity-history-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "gdi-release-reconciliation");
const HOLD_ROOT = "C:\\Dev\\dealality\\gdi-release-main";

const HOTELS = [
  { id: "rec0eTEnCqlsGJJt9", label: "Mayflower" },
  { id: "recnPB3Q02NO8Ma1o", label: "SanDiego" },
];

const LOCAL = process.env.GDI_LOCAL_BASE || "http://127.0.0.1:8080";
const PROD =
  process.env.GDI_PROD_BASE ||
  "https://my-operators-backend-production.up.railway.app";

const PE_SEVEN = [
  "gdi_pe_295cfef5822e63c3",
  "gdi_pe_879047c3211d2e82",
  "gdi_pe_5ce20a54b903e243",
  "gdi_pe_ca47fef6aa622566",
  "gdi_pe_5f72b9d7665314b5",
  "gdi_pe_4b9b49bae4136428",
  "gdi_pe_a7a2fda2a474aa0b",
];
const SHIPYARD = "gdi_sc_188e25b6b3b56637";

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: { Accept: "application/json" },
  });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }
  return { ok: res.ok, status: res.status, json, url };
}

function oppId(o) {
  return String(o?.id || o?.opportunityId || "");
}

function broadStamp(o) {
  return o?.prospectHotelFacing === true || o?.customerVisible === true;
}

function summarizeOpp(o, hotelId) {
  const id = oppId(o);
  const listFacing = isCustomerFacingOpportunity(o, { hotelId });
  const reviewed = isReviewedProspectCustomerVisible(o, { hotelId });
  return {
    id,
    title: o?.title || o?.opportunityName || null,
    organizationName: o?.organizationName || null,
    opportunityType: o?.opportunityType || o?.type || null,
    customerVisible: o?.customerVisible ?? null,
    prospectHotelFacing: o?.prospectHotelFacing ?? null,
    prospectPublicationBasis: o?.prospectPublicationBasis ?? null,
    reviewedPackageStamp: o?.reviewedPackageStamp ?? o?.packageStamp ?? null,
    listFacing,
    reviewedProspectVisible: reviewed,
    broadStamp: broadStamp(o),
    updatedAt: o?.updatedAt || null,
  };
}

function loadFreeze() {
  const p = path.join(
    HOLD_ROOT,
    "reports",
    "gdi-research-history-opportunity-type",
    "facing-inventory-freeze.json"
  );
  if (!fs.existsSync(p)) return { path: p, exists: false, hotels: {} };
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  const hotels = {};
  for (const h of HOTELS) {
    const block =
      raw.hotels?.[h.id] ||
      raw[h.id] ||
      (Array.isArray(raw.hotels)
        ? raw.hotels.find((x) => x.hotelId === h.id)
        : null);
    const ids = new Set(
      (
        block?.facingIds ||
        block?.opportunityIds ||
        block?.ids ||
        block?.facing ||
        []
      ).map(String)
    );
    // Alternate shapes
    if (!ids.size && Array.isArray(block?.opportunities)) {
      for (const o of block.opportunities) ids.add(oppId(o));
    }
    hotels[h.id] = {
      frozenAt: raw.frozenAt || block?.frozenAt || null,
      ids: [...ids].sort(),
      count: ids.size,
      rawKeys: block ? Object.keys(block) : [],
    };
  }
  return { path: p, exists: true, frozenAt: raw.frozenAt || null, hotels, rawTopKeys: Object.keys(raw) };
}

function loadScratch() {
  const p = path.join(
    ROOT,
    "reports",
    "gdi-quality-review",
    "_inventory-recon-scratch.json"
  );
  if (!fs.existsSync(p)) return { path: p, exists: false };
  return { path: p, exists: true, ...JSON.parse(fs.readFileSync(p, "utf8")) };
}

function setDiff(a, b) {
  const A = new Set(a);
  const B = new Set(b);
  return {
    onlyA: [...A].filter((x) => !B.has(x)).sort(),
    onlyB: [...B].filter((x) => !A.has(x)).sort(),
  };
}

async function loadList(base, hotelId) {
  const r = await fetchJson(
    `${base}/api/group-demand-intelligence/hotels/${hotelId}/opportunities`
  );
  const opps =
    r.json?.opportunities ||
    r.json?.items ||
    r.json?.data ||
    (Array.isArray(r.json) ? r.json : []);
  return {
    ...r,
    count: Array.isArray(opps) ? opps.length : 0,
    opportunities: Array.isArray(opps) ? opps : [],
    ids: (Array.isArray(opps) ? opps : []).map(oppId).filter(Boolean).sort(),
  };
}

async function loadSummary(base, hotelId) {
  return fetchJson(
    `${base}/api/group-demand-intelligence/hotels/${hotelId}/summary`
  );
}

function historyProbe(hotelId, opportunities, opportunityDoc) {
  const history = buildHotelActivityHistory(hotelId, {
    opportunities,
    opportunityDoc,
  });
  const ledger = loadHotelActivityLedger(hotelId);
  const eligible = (ledger.events || []).filter(
    (e) => e.customerHistoryEligible === true
  );
  const ineligible = (ledger.events || []).filter(
    (e) => e.customerHistoryEligible === false
  );
  return {
    version: history.version,
    lastResearchAt: history.lastResearchAt,
    runStatus: history.runStatus,
    runStatusLabel: history.runStatusLabel,
    latestRunId: history.latestRunId,
    runHistoryProvenance: history.runHistoryProvenance,
    lastSourceVerificationAt: history.lastSourceVerificationAt,
    lastPublishedUpdateAt: history.lastPublishedUpdateAt,
    lastReviewedPackageImportAt: history.lastReviewedPackageImportAt,
    excludedFromCustomerHistory: history.excludedFromCustomerHistory,
    ledgerPath: path.join(
      ROOT,
      "data",
      "group-demand-intelligence",
      "hotels",
      hotelId,
      "activity",
      "ledger-v1.json"
    ),
    ledgerExists: fs.existsSync(
      path.join(
        ROOT,
        "data",
        "group-demand-intelligence",
        "hotels",
        hotelId,
        "activity",
        "ledger-v1.json"
      )
    ),
    ledgerEventCount: (ledger.events || []).length,
    eligibleEventCount: eligible.length,
    ineligibleEventCount: ineligible.length,
    eligibleSample: eligible.slice(0, 8).map((e) => ({
      kind: e.kind,
      at: e.at || e.completedAt,
      status: e.status,
      runId: e.runId,
      provenance: e.provenance,
    })),
    ineligibleSample: ineligible.slice(0, 12).map((e) => ({
      kind: e.kind,
      at: e.at || e.completedAt,
      status: e.status,
      runId: e.runId,
      exclusionReasons: e.exclusionReasons || e.customerHistoryExclusionReasons,
      provenance: e.provenance,
      clockLabel: e.clockLabel,
    })),
  };
}

function classifyExclusion(row) {
  if (row.prospectHotelFacing === false && row.customerVisible === true) {
    return {
      category: "scope_mismatch_broad_vs_list",
      rule: "customer_visible_but_not_prospect_hotel_facing",
      exactReason:
        "Counted in broad stamp (customerVisible||prospectHotelFacing) but fails reviewed/list gate requiring prospectHotelFacing for overflow/reviewed path",
    };
  }
  if (row.prospectHotelFacing === true && !row.prospectPublicationBasis) {
    return {
      category: "scope_mismatch_broad_vs_list",
      rule: "missing_publication_basis",
      exactReason:
        "prospectHotelFacing true but prospectPublicationBasis null — broad stamp includes; isCustomerFacingOpportunity / reviewed publication excludes",
    };
  }
  if (String(row.id).startsWith("gdi_pe_") && !row.listFacing) {
    return {
      category: "scope_mismatch_broad_vs_list",
      rule: "reviewed_visibility_gate_excluded",
      exactReason:
        "PE preferred-lodging row present in broad stamp; reviewed-prospect customer visibility gate excludes from list/freeze",
    };
  }
  if (row.listFacing) {
    return {
      category: "currently_list_facing",
      rule: "isCustomerFacingOpportunity",
      exactReason: "Passes current customer list gate",
    };
  }
  return {
    category: "not_list_facing_other",
    rule: "isCustomerFacingOpportunity_false",
    exactReason:
      "Not list-facing under current gate; see visibility fields for detail",
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const freeze = loadFreeze();
  const scratch = loadScratch();
  const generatedAt = new Date().toISOString();

  const hotelResults = {};

  for (const h of HOTELS) {
    const localList = await loadList(LOCAL, h.id);
    const prodList = await loadList(PROD, h.id);
    const localSum = await loadSummary(LOCAL, h.id);
    const prodSum = await loadSummary(PROD, h.id);

    const localSummaries = localList.opportunities.map((o) =>
      summarizeOpp(o, h.id)
    );
    // Local list API already returns customer-facing only — recompute broad from returned rows is wrong.
    // For full canonical we need Airtable total via summary.opportunitiesDiscovered if present.
    const localFacingIds = localList.ids;
    const prodFacingIds = prodList.ids;
    const freezeIds = freeze.hotels[h.id]?.ids || [];

    const localVsFreeze = setDiff(localFacingIds, freezeIds);
    const prodVsFreeze = setDiff(prodFacingIds, freezeIds);
    const prodVsLocal = setDiff(prodFacingIds, localFacingIds);

    const byId = Object.fromEntries(localSummaries.map((r) => [r.id, r]));
    // For IDs only on prod, attach stub from prod payload
    for (const o of prodList.opportunities) {
      const id = oppId(o);
      if (!byId[id]) byId[id] = summarizeOpp(o, h.id);
    }

    const discrepantIds = [
      ...new Set([
        ...localVsFreeze.onlyA,
        ...localVsFreeze.onlyB,
        ...prodVsFreeze.onlyA,
        ...prodVsFreeze.onlyB,
        ...prodVsLocal.onlyA,
        ...prodVsLocal.onlyB,
      ]),
    ].sort();

    const discrepantRows = discrepantIds.map((id) => {
      const row = byId[id] || {
        id,
        title: null,
        organizationName: null,
        opportunityType: null,
        customerVisible: null,
        prospectHotelFacing: null,
        prospectPublicationBasis: null,
        listFacing: null,
        reviewedProspectVisible: null,
        broadStamp: null,
        updatedAt: null,
      };
      const exclusion = classifyExclusion(row);
      return {
        hotelId: h.id,
        hotelLabel: h.label,
        ...row,
        inLocalListApi: localFacingIds.includes(id),
        inProdListApi: prodFacingIds.includes(id),
        inFreeze: freezeIds.includes(id),
        inPeSevenHypothesis: PE_SEVEN.includes(id),
        isShipyardHypothesis: id === SHIPYARD,
        ...exclusion,
        comparisonNote:
          !byId[id]
            ? "ID not present in local or production list API payloads — canonical Airtable field dump not fetched in this pass"
            : null,
      };
    });

    const history = historyProbe(
      h.id,
      localList.opportunities,
      { opportunities: localList.opportunities, hotelId: h.id }
    );

    hotelResults[h.id] = {
      label: h.label,
      local: {
        status: localList.status,
        count: localList.count,
        ids: localFacingIds,
        summary: localSum.json?.summary || localSum.json,
        summaryStatus: localSum.status,
      },
      production: {
        status: prodList.status,
        count: prodList.count,
        ids: prodFacingIds,
        summary: prodSum.json?.summary || prodSum.json,
        summaryStatus: prodSum.status,
      },
      freeze: {
        exists: freeze.exists,
        frozenAt: freeze.hotels[h.id]?.frozenAt || freeze.frozenAt,
        count: freezeIds.length,
        ids: freezeIds,
        path: freeze.path,
      },
      diffs: {
        localVsFreeze,
        prodVsFreeze,
        prodVsLocal,
      },
      discrepantRows,
      history,
      peSevenOnProd: PE_SEVEN.map((id) => ({
        id,
        onProd: prodFacingIds.includes(id),
        onLocal: localFacingIds.includes(id),
        onFreeze: freezeIds.includes(id),
      })),
      shipyard: {
        id: SHIPYARD,
        onProd: prodFacingIds.includes(SHIPYARD),
        onLocal: localFacingIds.includes(SHIPYARD),
        onFreeze: freezeIds.includes(SHIPYARD),
      },
    };
  }

  const out = {
    generatedAt,
    persistenceHint: "GDI_OPPORTUNITIES_PERSISTENCE=airtable (local diagnostic)",
    bases: { local: LOCAL, production: PROD },
    freezeMeta: {
      exists: freeze.exists,
      path: freeze.path,
      frozenAt: freeze.frozenAt,
      rawTopKeys: freeze.rawTopKeys || [],
      note: freeze.exists
        ? "Freeze lives on held worktree gdi-release-main; not in deploy-repo reports path"
        : "Freeze missing",
    },
    scratchMeta: {
      exists: scratch.exists,
      path: scratch.path,
      generatedAt: scratch.generatedAt || null,
    },
    hotels: hotelResults,
  };

  const outPath = path.join(OUT_DIR, "inspection-raw.json");
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify({
    wrote: outPath,
    mayLocal: hotelResults.rec0eTEnCqlsGJJt9.local.count,
    mayProd: hotelResults.rec0eTEnCqlsGJJt9.production.count,
    mayFreeze: hotelResults.rec0eTEnCqlsGJJt9.freeze.count,
    sdLocal: hotelResults.recnPB3Q02NO8Ma1o.local.count,
    sdProd: hotelResults.recnPB3Q02NO8Ma1o.production.count,
    sdFreeze: hotelResults.recnPB3Q02NO8Ma1o.freeze.count,
    sdProdOnly: hotelResults.recnPB3Q02NO8Ma1o.diffs.prodVsLocal.onlyA,
    sdLocalOnly: hotelResults.recnPB3Q02NO8Ma1o.diffs.prodVsLocal.onlyB,
  }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
