#!/usr/bin/env node
/**
 * Reconcile Bethesda HE50 field-gap matrix against live HPC Airtable.
 * Read-only. Marks rows already persisted so factory does not re-research.
 */
import dotenv from "dotenv";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { classifyGapAction } from "../lib/hotel-intelligence/he-explorer-publication-gates-v1.js";
import { HE50_BETHESDA_HPC_ID } from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";
import { buildGuestRoomsMeetingSpaceFromHpc } from "../lib/hotel-intelligence/he-hpc-guest-rooms-meeting-space-v1.js";

dotenv.config({ path: "C:/Dev/dealality/my-operators-backend/.env", override: false });
dotenv.config({ override: false });

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = "appCCUsuGsE1ifoLk";
const TABLE = "tbl9aY5ijiuIzzWam";
const OUT_DIR = path.join(ROOT, "reports/hotel-explorer/he50-completion-v1");

/** fieldKey → how to prove live HPC persistence */
const HPC_PROOF = Object.freeze({
  yearBuilt: (f) => Boolean(f["Opening Date"]),
  totalMeetingSpace: (f) => f["Total Meeting Space (sq ft)"] != null,
  suiteInventory: (f) => f["Guestroom Suites Count"] != null,
  guestroomInventory: (f) => f["Rooms / Keys"] != null,
});

function graphProof(fieldKey, graph) {
  if (!graph?.ok) return false;
  const proj = graph.projection || {};
  const rels = graph.relationships || [];
  const hasRel = (re) =>
    rels.some((r) => re.test(String(r.relationshipType || r.relationship_type || "")));
  const contacts = graph.contacts || graph.orgContacts || [];
  switch (fieldKey) {
    case "legalOwner":
      return Boolean(proj.property_company?.legalName || proj.property_company?.tradingName);
    case "economicOwner":
      return Boolean(proj.economic_owner?.legalName || proj.economic_owner?.tradingName);
    case "assetManager":
      return hasRel(/Asset Manager/i);
    case "ocnEdges":
      return (graph.relationshipCount || rels.length) > 0;
    case "ownerPortfolio":
      return Array.isArray(graph.portfolio) && graph.portfolio.length > 0;
    case "keyPeople":
      return contacts.some((c) => c.displayName || c.fullName || c.name);
    case "publicChannels":
      // Named people persisted; direct emails often absent (Surfe session-only).
      return contacts.length > 0;
    case "evidenceRegister":
      return (graph.evidenceCount || (graph.evidence || []).length) > 0;
    default:
      return false;
  }
}

async function fetchHpc(id, token) {
  const res = await fetch(`https://api.airtable.com/v0/${BASE}/${TABLE}/${id}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`HPC ${res.status}: ${JSON.stringify(body).slice(0, 200)}`);
  return body;
}

function mainDisposition(row, fields, roomsDto, graph, geo) {
  const plan = classifyGapAction(row);
  if (plan.action === "DISABLED_BY_FOUNDER") {
    return { ...plan, liveStatus: "DISABLED_BY_FOUNDER", hpcPersisted: false, graphPersisted: false };
  }
  const prover = HPC_PROOF[row.fieldKey];
  if (prover && prover(fields)) {
    return {
      action: "VERIFY_API_LIVE_UI",
      paidResearch: false,
      liveStatus: "PERSISTED_ON_HPC",
      hpcPersisted: true,
      graphPersisted: false,
      note: "Live HPC already holds value — confirm API hydration + Live tab render.",
    };
  }
  if (
    (row.fieldKey === "totalMeetingSpace" || row.fieldKey === "suiteInventory") &&
    roomsDto
  ) {
    return {
      action: "VERIFY_API_LIVE_UI",
      paidResearch: false,
      liveStatus: "MAPPED_FROM_HPC_DTO",
      hpcPersisted: true,
      graphPersisted: false,
      note: "HPC→guestRoomsMeetingSpace DTO available.",
    };
  }
  if (graphProof(row.fieldKey, graph)) {
    return {
      action: "VERIFY_API_LIVE_UI",
      paidResearch: false,
      liveStatus: "PERSISTED_ON_HI_GRAPH",
      hpcPersisted: false,
      graphPersisted: true,
      note: "HI Organizations / relationships / contacts / evidence present — confirm Live tab render.",
    };
  }
  if (row.fieldKey === "demandAnchors" && (geo?.demandCount || 0) > 0) {
    return {
      action: "VERIFY_API_LIVE_UI",
      paidResearch: false,
      liveStatus: "DEMAND_ANCHORS_API_HYDRATED",
      hpcPersisted: false,
      graphPersisted: false,
      note: `Demand Anchors API returned ${geo.demandCount} items — confirm Live Demand tab.`,
    };
  }
  if (row.fieldKey === "travelInfrastructure" && (geo?.accessCount || 0) > 0) {
    return {
      action: "VERIFY_API_LIVE_UI",
      paidResearch: false,
      liveStatus: "TRAVEL_INFRA_API_HYDRATED",
      hpcPersisted: false,
      graphPersisted: false,
      note: `Access API returned ${geo.accessCount} items — confirm Live Access tab.`,
    };
  }
  return {
    ...plan,
    liveStatus: "STILL_OPEN",
    hpcPersisted: false,
    graphPersisted: false,
  };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  if (!token) {
    console.error(JSON.stringify({ ok: false, error: "MISSING_AIRTABLE_TOKEN" }));
    process.exit(2);
  }

  const matrix = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "reports/hotel-explorer/he-5-0-pilot/HE50-FIELD-GAP-MATRIX.json"),
      "utf8",
    ),
  );

  const rec = await fetchHpc(HE50_BETHESDA_HPC_ID, token);
  const fields = rec.fields || {};
  const roomsDto = buildGuestRoomsMeetingSpaceFromHpc(fields, {
    hpcId: HE50_BETHESDA_HPC_ID,
    hotelName: fields["Property Name"],
  });

  let graph = { ok: false };
  try {
    const { readHiGraphForHotel } = await import(
      "../lib/hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js"
    );
    graph = await readHiGraphForHotel(HE50_BETHESDA_HPC_ID, { includePortfolio: true });
  } catch (err) {
    console.warn("[reconcile] HI graph read failed", err?.message || err);
  }

  const geo = { demandCount: 0, accessCount: 0 };
  try {
    const {
      buildHeDemandDriversRead,
      buildHeAccessConnectivityRead,
    } = await import("../lib/hotel-intelligence/he-demand-access-read-v1.js");
    const demand = await buildHeDemandDriversRead(HE50_BETHESDA_HPC_ID);
    const access = await buildHeAccessConnectivityRead(HE50_BETHESDA_HPC_ID);
    geo.demandCount = Array.isArray(demand?.items) ? demand.items.length : 0;
    geo.accessCount = Array.isArray(access?.items) ? access.items.length : 0;
  } catch (err) {
    console.warn("[reconcile] demand/access read failed", err?.message || err);
  }

  const rows = (matrix.rows || []).map((row) => {
    const disp = mainDisposition(row, fields, roomsDto, graph, geo);
    return {
      hotelId: HE50_BETHESDA_HPC_ID,
      tabId: row.tabId,
      fieldKey: row.fieldKey,
      label: row.label,
      classification: row.classification,
      matrixNotes: row.notes || "",
      ...disp,
    };
  });

  const open = rows.filter((r) =>
    ["CONNECT_PERSIST_READBACK", "FREE_FIRST_THEN_PAID_GATE", "HUMAN_RECONCILE", "ACCEPT_GAP_OR_ESCALATE", "REVIEW"].includes(
      r.action,
    ),
  );

  const report = {
    version: "he50-bethesda-live-gap-reconcile-v1",
    at: new Date().toISOString(),
    hotelId: HE50_BETHESDA_HPC_ID,
    hotelName: fields["Property Name"] || "Bethesda Marriott",
    hpcSnapshot: {
      "Rooms / Keys": fields["Rooms / Keys"] ?? null,
      "Opening Date": fields["Opening Date"] ?? null,
      "Total Meeting Space (sq ft)": fields["Total Meeting Space (sq ft)"] ?? null,
      "Meeting Rooms Count": fields["Meeting Rooms Count"] ?? null,
      "Guestroom Single Count": fields["Guestroom Single Count"] ?? null,
      "Guestroom Double Count": fields["Guestroom Double Count"] ?? null,
      "Guestroom Suites Count": fields["Guestroom Suites Count"] ?? null,
      "Largest Meeting Room Name": fields["Largest Meeting Room Name"] ?? null,
      "Largest Meeting Room (sq ft)": fields["Largest Meeting Room (sq ft)"] ?? null,
      "Hotel Headline": fields["Hotel Headline"] ?? null,
      "Hotel Description": fields["Hotel Description"] ?? null,
    },
    hiGraphSnapshot: {
      ok: graph.ok === true,
      relationshipCount: graph.relationshipCount || 0,
      evidenceCount: graph.evidenceCount || 0,
      orgCount: (graph.organizations || []).length,
      contactCount: (graph.contacts || graph.orgContacts || []).length,
      controlStatus: graph.projection?.control_status || null,
      legalOwner: graph.projection?.property_company?.tradingName || null,
      economicOwner: graph.projection?.economic_owner?.tradingName || null,
      operator: graph.projection?.operator?.tradingName || null,
      relTypes: (graph.relationships || []).map((r) => r.relationshipType),
    },
    roomsDtoPresent: Boolean(roomsDto),
    roomsDtoSource: roomsDto?.source || null,
    geoApiSnapshot: geo,
    totals: {
      rows: rows.length,
      persistedOnHpc: rows.filter((r) => r.hpcPersisted).length,
      persistedOnHiGraph: rows.filter((r) => r.graphPersisted).length,
      disabledByFounder: rows.filter((r) => r.action === "DISABLED_BY_FOUNDER").length,
      stillConnectPersist: rows.filter((r) => r.action === "CONNECT_PERSIST_READBACK").length,
      verifyApiLive: rows.filter((r) => r.action === "VERIFY_API_LIVE_UI").length,
      researchRequired: rows.filter((r) => r.action === "FREE_FIRST_THEN_PAID_GATE").length,
      conflicts: rows.filter((r) => r.action === "HUMAN_RECONCILE").length,
      openFactoryWork: open.length,
    },
    rows,
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const outPath = path.join(OUT_DIR, "BETHESDA_LIVE_GAP_RECONCILE_V1.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

  // Refresh action queue from live reconcile (sentiment = DISABLED_BY_FOUNDER).
  const queue = rows
    .filter((r) => !["SKIP_REUSE", "VERIFY_API_LIVE_UI"].includes(r.action))
    .map((r) => ({
      hotelId: r.hotelId,
      tabId: r.tabId,
      fieldKey: r.fieldKey,
      label: r.label,
      classification: r.classification,
      action: r.action,
      paidResearch: r.paidResearch,
      note: r.note || r.matrixNotes || "",
      liveStatus: r.liveStatus,
    }));
  const queuePath = path.join(OUT_DIR, "BETHESDA_GAP_ACTION_QUEUE_V1.json");
  fs.writeFileSync(
    queuePath,
    JSON.stringify(
      {
        version: "he50-bethesda-gap-action-queue-v1",
        at: report.at,
        hotelId: HE50_BETHESDA_HPC_ID,
        hotelName: report.hotelName,
        source: "live_hpc_reconcile",
        totals: {
          rows: rows.length,
          open: queue.length,
          connectPersist: queue.filter((q) => q.action === "CONNECT_PERSIST_READBACK").length,
          researchRequired: queue.filter((q) => q.action === "FREE_FIRST_THEN_PAID_GATE").length,
          conflicts: queue.filter((q) => q.action === "HUMAN_RECONCILE").length,
          disabledByFounder: queue.filter((q) => q.action === "DISABLED_BY_FOUNDER").length,
        },
        paidResearchSpendUsdThisRun: 0,
        applyAuthorized: false,
        queue,
      },
      null,
      2,
    ),
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        out: outPath,
        queue: queuePath,
        totals: report.totals,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
