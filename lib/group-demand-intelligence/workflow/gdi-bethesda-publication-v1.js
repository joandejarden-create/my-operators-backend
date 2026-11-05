/**
 * Bethesda-only publication scope for the V4.7 snapshot.
 * The parent snapshot file is read, never rewritten.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { authorizeProspectPublicationPromotion } from "../prospect-publication-eligibility-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
export const BETHESDA_HOTEL_ID = "recLuxvwwxID7U2B8";
export const BETHESDA_PARENT_SNAPSHOT = "GDI_THREE_HOTEL_PILOT_SNAPSHOT_V4_7";
export const BETHESDA_PARENT_HASH = "c0b84264e6725bb19f2701ee7d94d7c29093612a2570bef3c7bffd15a28fbe2f";
const snapshotPath = path.join(root, "reports/gdi-three-hotel-pilot-release-v4-7/REVISED_RELEASE_CANDIDATE.json");
const projectionPath = path.join(root, "reports/gdi-three-hotel-pilot-release-v4-7/staging/bethesda-pilot-list.html");
const approvalLedgerPath = path.join(root, "data/group-demand-intelligence/gdi-founder-approval-v1/approval-ledger-v1.json");
const receiptPath = path.join(root, "data/group-demand-intelligence/gdi-founder-approval-v1/bethesda-publication-receipt-v1.json");

function stable(value) {
  if (Array.isArray(value)) return `[${value.map(stable).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stable(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function projectionByOrganization() {
  const html = fs.readFileSync(projectionPath, "utf8");
  const articles = [...html.matchAll(/<article[\s\S]*?<\/article>/g)].map((match) => match[0]);
  const out = new Map();
  for (const article of articles) {
    const name = (article.match(/<h2>(.*?)<\/h2>/) || [])[1] || "";
    out.set(name, article.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim());
  }
  return out;
}

export function pinBethesdaPublicationSubset() {
  const snapshot = JSON.parse(fs.readFileSync(snapshotPath, "utf8"));
  if (snapshot.pilotSnapshotVersion !== BETHESDA_PARENT_SNAPSHOT || snapshot.finalHash !== BETHESDA_PARENT_HASH) {
    return { ok: false, reason: "PARENT_SNAPSHOT_MISMATCH" };
  }
  const rows = (snapshot.opportunities || []).filter((row) => row.hotel === "Bethesda");
  if (rows.length !== 12 || rows.some((row) => row.openBookingClaimed !== false || row.section !== "EXPLORATORY")) {
    return { ok: false, reason: "BETHESDA_SUBSET_INVALID" };
  }
  const projections = projectionByOrganization();
  const pinned = rows.map((row) => ({
    ...row,
    hotelId: BETHESDA_HOTEL_ID,
    customerProjection: projections.get(row.organization) || "",
  }));
  if (pinned.some((row) => !row.customerProjection || /open booking claimed: yes/i.test(row.customerProjection))) {
    return { ok: false, reason: "CUSTOMER_PROJECTION_MISSING" };
  }
  const subsetHash = sha256(stable(pinned.map((row) => ({
    opportunityId: row.opportunityId,
    organization: row.organization,
    section: row.section,
    demandMechanism: row.demandMechanism,
    openBookingClaimed: row.openBookingClaimed,
    customerProjection: row.customerProjection,
  }))));
  return {
    ok: true,
    parentSnapshotVersion: BETHESDA_PARENT_SNAPSHOT,
    parentSnapshotHash: BETHESDA_PARENT_HASH,
    subsetHash,
    hotelId: BETHESDA_HOTEL_ID,
    customerAccessScope: "bethesda_customer_opportunity_list",
    opportunityIds: pinned.map((row) => row.opportunityId),
    rows: pinned,
  };
}

export function loadLiveBethesdaPublicationApproval() {
  const pin = pinBethesdaPublicationSubset();
  if (!pin.ok) return pin;
  if (!fs.existsSync(approvalLedgerPath)) return { ok: false, reason: "MEMBERSTACK_PUBLICATION_APPROVAL_REQUIRED", pin };
  const ledger = JSON.parse(fs.readFileSync(approvalLedgerPath, "utf8"));
  const record = (ledger.records || []).find((row) => row.operation === "PUBLICATION" && row.status === "APPROVED" && row.hotelIds?.length === 1 && row.hotelIds[0] === BETHESDA_HOTEL_ID);
  if (!record) return { ok: false, reason: "MEMBERSTACK_PUBLICATION_APPROVAL_REQUIRED", pin };
  if (record.verifiedVia !== "rest" && record.verifiedVia !== "jwks") return { ok: false, reason: "SESSION_NOT_LIVE", pin };
  if (record.expiresAt && Date.parse(record.expiresAt) < Date.now()) return { ok: false, reason: "APPROVAL_EXPIRED", pin };
  const ids = [...(record.opportunityIds || [])].sort().join("|");
  const expected = [...pin.opportunityIds].sort().join("|");
  if (record.sourceHash !== pin.parentSnapshotHash || record.subsetHash !== pin.subsetHash || ids !== expected) {
    return { ok: false, reason: "PUBLICATION_SCOPE_MISMATCH", pin };
  }
  return { ok: true, approval: record, pin };
}

export function classifyBethesdaPublicationRow(record, pinned) {
  const id = record.id || record.opportunityId;
  const visible = record.customerVisible === true;
  const basis = String(record.prospectPublicationBasis || "");
  const blob = [record.title, record.venueSourcingRationale, record.roomDemandRationale, record.summaryWhat, record.whyNow, record.eventDate].join(" ");
  const projectionDiffers = /RFP|room block|active sourcing|VERIFIED_HOUSING/i.test(blob) || !blob.includes("account-development");
  if (!visible) return { opportunityId: id, action: "initial_publication", weeklyNew: true, projectionDiffers };
  if (!basis) return { opportunityId: id, action: "basis_repair", weeklyNew: false, projectionDiffers };
  if (projectionDiffers) return { opportunityId: id, action: "content_update", weeklyNew: false, projectionDiffers };
  return { opportunityId: id, action: "no_action", weeklyNew: false, projectionDiffers: false, organization: pinned.organization };
}

export function loadBethesdaPublicationReceipt() {
  if (!fs.existsSync(receiptPath)) return { ok: false, reason: "PUBLICATION_NOT_EXECUTED" };
  return { ok: true, receipt: JSON.parse(fs.readFileSync(receiptPath, "utf8")) };
}

export function applyApprovedBethesdaCustomerProjection(opportunities = []) {
  const receipt = loadBethesdaPublicationReceipt();
  if (!receipt.ok) return opportunities;
  const byId = new Map((receipt.receipt.rows || []).map((row) => [row.opportunityId, row]));
  return opportunities.map((row) => {
    const pinned = byId.get(row.id || row.opportunityId);
    if (!pinned) return row;
    return {
      ...row,
      venueSourcingRationale: pinned.customerProjection,
      roomDemandRationale: "",
      roomDemandStatus: row.roomDemandStatus === "VERIFIED_ROOM_BLOCK" ? row.roomDemandStatus : "UNKNOWN",
      summaryWhat: pinned.customerProjection,
      openBookingClaimed: false,
    };
  });
}

export async function executeBethesdaPublication({ rows = [], upsert } = {}) {
  const live = loadLiveBethesdaPublicationApproval();
  if (!live.ok) return { executed: false, written: false, reason: live.reason };
  const byId = new Map(rows.map((row) => [row.id || row.opportunityId, row]));
  const missing = live.pin.opportunityIds.filter((id) => !byId.has(id));
  if (missing.length) return { executed: false, written: false, reason: "OPPORTUNITY_MISSING", missing };
  const repairs = [];
  for (const pinned of live.pin.rows) {
    const current = byId.get(pinned.opportunityId);
    if (current.prospectPublicationBasis === "REVIEWED_PACKAGE_V1") continue;
    const promoted = authorizeProspectPublicationPromotion(current, {
      authorizePublication: true,
      authorizedBy: live.approval.approvalId,
      reason: "bethesda_v4_7_subset",
    });
    if (!promoted.ok) return { executed: false, written: false, reason: promoted.reason, opportunityId: pinned.opportunityId };
    repairs.push(promoted.patched);
  }
  if (typeof upsert === "function") {
    for (const patched of repairs) await upsert(patched);
  }
  const receipt = {
    approvalId: live.approval.approvalId,
    parentSnapshotHash: live.pin.parentSnapshotHash,
    subsetHash: live.pin.subsetHash,
    opportunityIds: live.pin.opportunityIds,
    rows: live.pin.rows,
    writtenAt: new Date().toISOString(),
  };
  fs.mkdirSync(path.dirname(receiptPath), { recursive: true });
  fs.writeFileSync(receiptPath, JSON.stringify(receipt, null, 2));
  return { executed: true, written: true, repaired: repairs.length, receipt };
}
