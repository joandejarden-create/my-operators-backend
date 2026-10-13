/**
 * Durable HI domain-status ledger (file-backed).
 * Path: data/hotel-intelligence/domain-status/<hpcHotelId>.json
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HI_DOMAIN,
  REQUIRED_HI_DOMAINS,
  HI_DOMAIN_STATUS,
  deriveDomainStatus,
  summarizeOverallHiStatus,
  domainCoverageScore,
} from "./domain-status-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");
export const DOMAIN_STATUS_DIR = path.join(
  REPO_ROOT,
  "data",
  "hotel-intelligence",
  "domain-status"
);

export function domainStatusPath(hpcHotelId) {
  return path.join(DOMAIN_STATUS_DIR, `${String(hpcHotelId)}.json`);
}

export function loadDomainStatusLedger(hpcHotelId) {
  const p = domainStatusPath(hpcHotelId);
  if (!fs.existsSync(p)) {
    return {
      hpcHotelId,
      schemaVersion: "hi_domain_status_v1",
      domains: {},
      overallStatus: null,
      updatedAt: null,
    };
  }
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return {
      hpcHotelId,
      schemaVersion: "hi_domain_status_v1",
      domains: {},
      overallStatus: null,
      updatedAt: null,
      loadError: true,
    };
  }
}

export function saveDomainStatusLedger(ledger) {
  fs.mkdirSync(DOMAIN_STATUS_DIR, { recursive: true });
  const next = {
    ...ledger,
    schemaVersion: "hi_domain_status_v1",
    updatedAt: new Date().toISOString(),
  };
  const p = domainStatusPath(ledger.hpcHotelId);
  fs.writeFileSync(p, JSON.stringify(next, null, 2) + "\n", "utf8");
  return next;
}

export function upsertDomainStatus(hpcHotelId, domain, entry) {
  const ledger = loadDomainStatusLedger(hpcHotelId);
  ledger.hpcHotelId = hpcHotelId;
  ledger.domains = ledger.domains || {};
  ledger.domains[domain] = {
    ...(ledger.domains[domain] || {}),
    ...entry,
    domain,
    updatedAt: new Date().toISOString(),
  };
  ledger.overallStatus = summarizeOverallHiStatus(ledger.domains);
  ledger.coverage = domainCoverageScore(ledger.domains);
  return saveDomainStatusLedger(ledger);
}

/**
 * Evaluate all required domains from profile snapshot + optional ledger.
 */
export function evaluateHotelDomainStatuses(hpcHotelId, profile = {}, opts = {}) {
  const ledger = opts.ledger || loadDomainStatusLedger(hpcHotelId);
  const commercialRows = profile.commercialProfile?.fromHiAirtable
    ? 1
    : profile.commercialProfile?.rooms != null
      ? 1
      : 0;
  const eventRows = (profile.eventSpaces || []).filter((e) => e.fromHiAirtable !== false || e.sqFt != null).length
    ? (profile.eventSpaces || []).length
    : (profile.eventSpaces || []).length;
  // Prefer Airtable-backed counts when evidenceSummary present
  const eventCount =
    profile.evidenceSummary?.eventSpaceAirtable
      ? (profile.eventSpaces || []).filter((e) => e.fromHiAirtable).length
      : (profile.eventSpaces || []).length;
  const demandCount =
    profile.evidenceSummary?.demandNodeAirtable
      ? (profile.demandNodes || []).filter((d) => d.fromHiAirtable).length
      : (profile.demandNodes || []).length;
  // GDI-config-only demand nodes without HI airtable = not yet researched into HI
  const demandFromGdiOnly =
    !profile.evidenceSummary?.demandNodeAirtable &&
    (profile.demandNodes || []).length > 0 &&
    profile.evidenceSummary?.legacyJsonFallback?.demandFromGdi;

  const seasonalityCount = (profile.seasonality || []).length;
  const needCount = (profile.needPeriods || []).length;
  const seasonalityNeedRows = seasonalityCount + needCount;
  const evidenceCount = profile.evidenceSummary?.evidenceAirtable || 0;
  const adpAttrCount = opts.adpAttributeActiveCount ?? null;

  const domains = {};

  domains[HI_DOMAIN.COMMERCIAL_PROFILE] = deriveDomainStatus(
    HI_DOMAIN.COMMERCIAL_PROFILE,
    {
      ledgerEntry: ledger.domains?.[HI_DOMAIN.COMMERCIAL_PROFILE],
      rowCount:
        profile.evidenceSummary?.commercialAirtable || commercialRows > 0
          ? Math.max(commercialRows, profile.evidenceSummary?.commercialAirtable ? 1 : 0)
          : 0,
      evidenceCount,
    }
  );

  domains[HI_DOMAIN.EVENT_SPACES] = deriveDomainStatus(HI_DOMAIN.EVENT_SPACES, {
    ledgerEntry: ledger.domains?.[HI_DOMAIN.EVENT_SPACES],
    rowCount: eventCount,
    evidenceCount,
  });

  domains[HI_DOMAIN.DEMAND_NODES] = deriveDomainStatus(HI_DOMAIN.DEMAND_NODES, {
    ledgerEntry: ledger.domains?.[HI_DOMAIN.DEMAND_NODES],
    // GDI fallback alone does not count as HI POPULATED
    rowCount: demandFromGdiOnly ? 0 : demandCount,
    evidenceCount,
    notes: demandFromGdiOnly
      ? "demand_nodes_only_from_gdi_config_fallback"
      : undefined,
  });

  domains[HI_DOMAIN.SEASONALITY_NEED_PERIODS] = deriveDomainStatus(
    HI_DOMAIN.SEASONALITY_NEED_PERIODS,
    {
      ledgerEntry: ledger.domains?.[HI_DOMAIN.SEASONALITY_NEED_PERIODS],
      rowCount: seasonalityNeedRows,
      evidenceCount,
    }
  );

  domains[HI_DOMAIN.HI_EVIDENCE] = deriveDomainStatus(HI_DOMAIN.HI_EVIDENCE, {
    ledgerEntry: ledger.domains?.[HI_DOMAIN.HI_EVIDENCE],
    rowCount: evidenceCount,
  });

  domains[HI_DOMAIN.ADP_ATTRIBUTES] = deriveDomainStatus(HI_DOMAIN.ADP_ATTRIBUTES, {
    ledgerEntry: ledger.domains?.[HI_DOMAIN.ADP_ATTRIBUTES],
    rowCount: adpAttrCount != null ? adpAttrCount : ledger.domains?.[HI_DOMAIN.ADP_ATTRIBUTES]?.rowCount || 0,
  });

  const overallStatus = summarizeOverallHiStatus(domains);
  const coverage = domainCoverageScore(domains);

  return {
    hpcHotelId,
    hotelName: profile.identity?.hotelName || null,
    adpPropertyId: profile.identity?.adpPropertyId || null,
    domains,
    overallStatus,
    coverage,
    falseCompletenessFlags: Object.values(domains)
      .map((d) => d.falseCompletenessFlag)
      .filter(Boolean),
    evaluatedAt: new Date().toISOString(),
  };
}

export { HI_DOMAIN, REQUIRED_HI_DOMAINS, HI_DOMAIN_STATUS };
