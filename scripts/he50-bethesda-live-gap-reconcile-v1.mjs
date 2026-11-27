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

async function fetchHpc(id, token) {
  const res = await fetch(`https://api.airtable.com/v0/${BASE}/${TABLE}/${id}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`HPC ${res.status}: ${JSON.stringify(body).slice(0, 200)}`);
  return body;
}

function mainDisposition(row, fields, roomsDto) {
  const plan = classifyGapAction(row);
  if (plan.action === "DISABLED_BY_FOUNDER") {
    return { ...plan, liveStatus: "DISABLED_BY_FOUNDER", hpcPersisted: false };
  }
  const prover = HPC_PROOF[row.fieldKey];
  if (prover && prover(fields)) {
    return {
      action: "VERIFY_API_LIVE_UI",
      paidResearch: false,
      liveStatus: "PERSISTED_ON_HPC",
      hpcPersisted: true,
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
      note: "HPC→guestRoomsMeetingSpace DTO available.",
    };
  }
  return { ...plan, liveStatus: "STILL_OPEN", hpcPersisted: false };
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

  const rows = (matrix.rows || []).map((row) => {
    const disp = mainDisposition(row, fields, roomsDto);
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

  const open = rows.filter(
    (r) =>
      !["SKIP_REUSE", "VERIFY_API_LIVE_UI", "DISABLED_BY_FOUNDER"].includes(r.action) ||
      r.action === "CONNECT_PERSIST_READBACK",
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
    roomsDtoPresent: Boolean(roomsDto),
    roomsDtoSource: roomsDto?.source || null,
    totals: {
      rows: rows.length,
      persistedOnHpc: rows.filter((r) => r.hpcPersisted).length,
      disabledByFounder: rows.filter((r) => r.action === "DISABLED_BY_FOUNDER").length,
      stillConnectPersist: rows.filter((r) => r.action === "CONNECT_PERSIST_READBACK").length,
      verifyApiLive: rows.filter((r) => r.action === "VERIFY_API_LIVE_UI").length,
      researchRequired: rows.filter((r) => r.action === "FREE_FIRST_THEN_PAID_GATE").length,
      conflicts: rows.filter((r) => r.action === "HUMAN_RECONCILE").length,
      openFactoryWork: open.filter((r) => r.action !== "DISABLED_BY_FOUNDER").length,
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
