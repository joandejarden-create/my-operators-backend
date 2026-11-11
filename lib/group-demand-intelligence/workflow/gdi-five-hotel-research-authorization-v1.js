/**
 * Pending and active GDI research authorization for the five-hotel certification.
 * This module does not create an approval. An active ledger is accepted only
 * when a live founder identity recorded it.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isLiveFounderApprover } from "../../hotel-census/hpc-authorized-create-reuse-v1.js";
import { verifiedProviderBilling } from "./gdi-provider-billing-guard-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export const FIVE_HOTEL_RESEARCH_AUTH_VERSION = "gdi-five-hotel-research-authorization-v1";
export const FIVE_HOTEL_RESEARCH_WAVE_ID = "gdi_five_hotel_live_research_v1";

export const FIVE_HOTEL_RESEARCH_PROPOSALS = Object.freeze([
  {
    hotelId: "rec0eTEnCqlsGJJt9",
    displayName: "The Mayflower Hotel, Autograph Collection",
    proposedCapUsd: 10,
  },
  {
    hotelId: "recLuxvwwxID7U2B8",
    displayName: "Bethesda Marriott",
    proposedCapUsd: 10,
  },
  {
    hotelId: "recnPB3Q02NO8Ma1o",
    displayName: "Wyndham San Diego Bayside",
    proposedCapUsd: 15,
  },
  {
    hotelId: "recRkGGNKslJptFIL",
    displayName: "Falls Church Marriott Fairview Park",
    proposedCapUsd: 25,
  },
  {
    hotelId: "recterKmNPdntCm0v",
    displayName: "Grand Fiesta Americana Chapultepec",
    proposedCapUsd: 30,
  },
]);

export function fiveHotelResearchLedgerPath() {
  return path.join(root, "data/group-demand-intelligence/gdi-five-hotel-research-authorization-v1/authorization-ledger-v1.json");
}

export function pendingResearchApprovals() {
  return {
    version: FIVE_HOTEL_RESEARCH_AUTH_VERSION,
    status: "PENDING_FOUNDER",
    granted: false,
    product: "GDI",
    waveId: FIVE_HOTEL_RESEARCH_WAVE_ID,
    approverIdentity: null,
    approvedAt: null,
    validUntil: null,
    semantics: "cap_is_a_ceiling_until_revoked_or_exhausted_not_a_single_use_token",
    noAdp: true,
    noSharedHistoricalBalance: true,
    noCrossHotelTransfer: true,
    noPublication: true,
    noAirtableWrite: true,
    hotels: FIVE_HOTEL_RESEARCH_PROPOSALS.map((hotel) => ({
      ...hotel,
      authorizationId: null,
      product: "GDI",
      scope: "official_page_research_only_until_provider_prices_are_verified",
      permittedProviders: ["official_page"],
      disabledProviders: ["parallel", "serpapi", "surfe", "jev"],
      hardCapUsd: hotel.proposedCapUsd,
      status: "PENDING_FOUNDER",
    })),
  };
}

const HPC_CREATE_AUTH_ID = "hpc_create_auth_5f70fe679b52f30a";

export function buildFiveHotelResearchLedger({
  approverIdentity,
  approvedAt,
  supersededRecords = [],
} = {}) {
  const billing = verifiedProviderBilling();
  return {
    version: FIVE_HOTEL_RESEARCH_AUTH_VERSION,
    status: "ACTIVE",
    granted: true,
    product: "GDI",
    waveId: FIVE_HOTEL_RESEARCH_WAVE_ID,
    approverIdentity,
    approvedAt,
    validUntil: null,
    semantics: "cap_is_a_ceiling_until_revoked_or_exhausted_not_a_single_use_token",
    noAdp: true,
    noSharedHistoricalBalance: true,
    noCrossHotelTransfer: true,
    noPublication: true,
    noAirtableWrite: true,
    simulation: false,
    providers: {
      official_page: { enabled: true, maxChargeUsd: billing.official_page.verifiedChargeUsd },
      parallel: { enabled: false, reason: billing.parallel.reason },
      serpapi: { enabled: false, reason: billing.serpapi.reason },
      surfe: { enabled: false, reason: billing.surfe.reason },
      jev: { enabled: false, reason: billing.jev.reason },
    },
    hotels: FIVE_HOTEL_RESEARCH_PROPOSALS.map((hotel) => ({
      hotelId: hotel.hotelId,
      displayName: hotel.displayName,
      product: "GDI",
      authorizationId: `auth_gdi_five_hotel_live_research_v1_${hotel.hotelId}`,
      hardCapUsd: hotel.proposedCapUsd,
      permittedProviders: ["official_page"],
      disabledProviders: ["parallel", "serpapi", "surfe", "jev"],
      status: "ACTIVE",
    })),
    supersededRecords,
  };
}

export function validateFiveHotelResearchLedger(doc = {}) {
  const errors = [];
  if (doc.status !== "ACTIVE" || doc.granted !== true) errors.push("ledger_not_granted");
  if (doc.product !== "GDI" || doc.waveId !== FIVE_HOTEL_RESEARCH_WAVE_ID) errors.push("wave_or_product");
  if (doc.simulation === true || !isLiveFounderApprover(doc.approverIdentity, doc)) errors.push("approver");
  if (
    doc.noAdp !== true ||
    doc.noSharedHistoricalBalance !== true ||
    doc.noCrossHotelTransfer !== true ||
    doc.noPublication !== true ||
    doc.noAirtableWrite !== true
  ) {
    errors.push("scope_flags");
  }
  if (doc.providers?.official_page?.enabled !== true || Number(doc.providers?.official_page?.maxChargeUsd) !== 0) {
    errors.push("official_page_billing");
  }
  for (const name of ["parallel", "serpapi", "surfe", "jev"]) {
    if (doc.providers?.[name]?.enabled !== false) errors.push(`${name}_not_disabled`);
  }
  const rows = Array.isArray(doc.hotels) ? doc.hotels : [];
  if (rows.length !== FIVE_HOTEL_RESEARCH_PROPOSALS.length) errors.push("hotel_count");
  for (const proposal of FIVE_HOTEL_RESEARCH_PROPOSALS) {
    const row = rows.find((item) => item.hotelId === proposal.hotelId);
    if (!row) {
      errors.push(`missing_${proposal.hotelId}`);
      continue;
    }
    const expectedId = `auth_gdi_five_hotel_live_research_v1_${proposal.hotelId}`;
    if (row.displayName !== proposal.displayName) errors.push(`name_${proposal.hotelId}`);
    if (Number(row.hardCapUsd) !== proposal.proposedCapUsd) errors.push(`cap_${proposal.hotelId}`);
    if (row.product !== "GDI" || row.status !== "ACTIVE") errors.push(`row_scope_${proposal.hotelId}`);
    if (row.authorizationId !== expectedId) errors.push(`authorization_id_${proposal.hotelId}`);
    if (row.authorizationId === HPC_CREATE_AUTH_ID || doc.authorizationId === HPC_CREATE_AUTH_ID) {
      errors.push("hpc_authorization_reused");
    }
    const permitted = row.permittedProviders || [];
    if (permitted.length !== 1 || permitted[0] !== "official_page") errors.push(`providers_${proposal.hotelId}`);
  }
  return { ok: errors.length === 0, errors };
}

/**
 * Replace a non-live ledger in memory. A live valid ledger is reused.
 * A different live ledger is a conflict and is not overwritten.
 */
export function prepareFiveHotelResearchLedgerReplacement(existing, approverIdentity, approvedAt) {
  if (existing && validateFiveHotelResearchLedger(existing).ok) {
    return { ok: true, reused: true, ledger: existing };
  }
  if (existing && isLiveFounderApprover(existing.approverIdentity, existing) && existing.status === "ACTIVE") {
    return { ok: false, reason: "CONFLICTING_ACTIVE_AUTHORIZATION" };
  }
  const prior = Array.isArray(existing?.supersededRecords) ? existing.supersededRecords : [];
  const supersededRecords = existing
    ? prior.concat([
        {
          ...existing,
          status: "SUPERSEDED",
          supersededAt: approvedAt,
          supersededReason: "INVALID_APPROVER_IDENTITY",
          hotels: (existing.hotels || []).map((row) => ({ ...row, status: "SUPERSEDED" })),
          supersededRecords: undefined,
        },
      ])
    : prior;
  const ledger = buildFiveHotelResearchLedger({ approverIdentity, approvedAt, supersededRecords });
  const validation = validateFiveHotelResearchLedger(ledger);
  if (!validation.ok) return { ok: false, reason: "LEDGER_VALIDATION_FAILED", errors: validation.errors };
  return { ok: true, reused: false, ledger, validation };
}

export function loadActiveResearchAuthorization(hotelId, opts = {}) {
  const file = opts.ledgerPath || fiveHotelResearchLedgerPath();
  if (!fs.existsSync(file)) return { ok: false, reason: "LEDGER_RECORD_ABSENT", hotelId };
  const doc = JSON.parse(fs.readFileSync(file, "utf8"));
  if (doc.status !== "ACTIVE") return { ok: false, reason: "LEDGER_NOT_ACTIVE", status: doc.status || null };
  if (!isLiveFounderApprover(doc.approverIdentity, doc)) {
    return { ok: false, reason: "FOUNDER_IDENTITY_NOT_LIVE" };
  }
  const validation = validateFiveHotelResearchLedger(doc);
  if (!validation.ok) return { ok: false, reason: "LEDGER_VALIDATION_FAILED", errors: validation.errors };
  const row = (doc.hotels || []).find((item) => item.hotelId === hotelId);
  if (!row) return { ok: false, reason: "HOTEL_NOT_IN_AUTHORIZATION" };
  return {
    ok: true,
    authorizationId: row.authorizationId,
    hardCapUsd: row.hardCapUsd,
    displayName: row.displayName,
    providers: doc.providers || {},
  };
}
