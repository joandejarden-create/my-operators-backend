/**
 * Canonical Need-Date source precedence contract V2.
 *
 * 1. Verified hotel-entered Airtable records (mirror)
 * 2. Authorized hotel-confirmed canonical calendar store
 * 3. Evidence-supported market estimates
 * 4. NOT_ESTABLISHED
 *
 * Test scenarios never qualify as confirmed.
 * Conflicting confirmed sources are quarantined, not silently merged.
 * No Airtable writes. No paid research. No publish.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HOTEL_NEED_DATE_CONFIG_STATUS,
  NEED_PERIOD_PROVENANCE,
  loadAirtableNeedPeriodMirror,
  loadMarketEstimatedNeedPeriods,
  deriveHotelNeedDateConfigStatus,
} from "./gdi-universal-need-date-intelligence-v1.js";
import { listNeedPeriodsForHotel } from "./need-period-hotel-store-v1.js";

export const GDI_NEED_DATE_SOURCE_CONTRACT_V2 =
  "GDI_NEED_DATE_SOURCE_CONTRACT_V2";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

export function isTestScenarioPeriod(period = {}) {
  const status = text(period.status);
  const source = text(period.source);
  return (
    period.isTestScenario === true ||
    /TEST_SCENARIO/i.test(status) ||
    /founder_test|lifecycle_isolate|fixture|synthetic|demo/i.test(source)
  );
}

export function isAuthoritativeHotelConfirmedPeriod(period = {}) {
  if (!period || isTestScenarioPeriod(period)) return false;
  if (/Cancelled|Superseded|Filled/i.test(text(period.status))) return false;
  const source = upper(period.source);
  const provenance = upper(
    period.provenance || period.needPeriodProvenance || ""
  );
  if (provenance === NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED) return false;
  if (provenance === NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED) return true;
  return (
    /HOTEL_REPORTED|HOTEL_SALES|HOTEL_PORTAL|HOTEL_CONFIRMED/i.test(source) ||
    period.hotelConfirmed === true
  );
}

function periodKey(p = {}) {
  const start = text(p.startDate).slice(0, 10);
  const end = text(p.endDate).slice(0, 10);
  const label = text(p.periodLabel || p.periodName).toLowerCase();
  return `${start}|${end}|${label}`;
}

function quarantinePath(hotelId) {
  return path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    text(hotelId),
    "need-periods.conflicts-quarantine.json"
  );
}

/**
 * Resolve confirmed periods with explicit precedence and conflict quarantine.
 */
export function resolveAuthoritativeNeedDatePeriods(hotelId, opts = {}) {
  const id = text(hotelId);
  const mirror = loadAirtableNeedPeriodMirror(id);
  const airtableConfirmed = (mirror.periods || []).filter(
    isAuthoritativeHotelConfirmedPeriod
  );

  let localPeriods = [];
  let localIsDemoStub = false;
  try {
    const listed = listNeedPeriodsForHotel(id, opts);
    localIsDemoStub = /demo periods for universal|UI verification/i.test(
      text(listed?.note)
    );
    const raw = Array.isArray(listed)
      ? listed
      : Array.isArray(listed?.periods)
        ? listed.periods
        : [];
    if (!localIsDemoStub || opts.allowDemoLocalStub === true) {
      localPeriods = raw.filter(isAuthoritativeHotelConfirmedPeriod);
    }
  } catch {
    localPeriods = [];
  }

  const conflicts = [];
  let chosen = [];
  let source = "NOT_ESTABLISHED";

  if (airtableConfirmed.length > 0) {
    chosen = airtableConfirmed.map((p) => ({
      ...p,
      provenance: NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED,
      hotelConfirmed: true,
      sourceAuthority: "AIRTABLE_MIRROR",
    }));
    source = "AIRTABLE_MIRROR";

    // Quarantine local confirmed periods that conflict with Airtable by overlapping months.
    const airKeys = new Set(chosen.map(periodKey));
    for (const loc of localPeriods) {
      const key = periodKey(loc);
      const sameId = chosen.some(
        (a) => text(a.needPeriodId) === text(loc.needPeriodId)
      );
      if (sameId) continue;
      if (airKeys.has(key)) {
        conflicts.push({
          type: "DUPLICATE_MONTH_LOCAL_VS_AIRTABLE",
          localNeedPeriodId: loc.needPeriodId,
          periodKey: key,
        });
      } else {
        conflicts.push({
          type: "EXTRA_LOCAL_CONFIRMED_WHILE_AIRTABLE_PRESENT",
          localNeedPeriodId: loc.needPeriodId,
          periodKey: key,
          action: "QUARANTINED_NOT_MERGED",
        });
      }
    }
  } else if (localPeriods.length > 0) {
    chosen = localPeriods.map((p) => ({
      ...p,
      provenance: NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED,
      hotelConfirmed: true,
      sourceAuthority: "LOCAL_CANONICAL_STORE",
    }));
    source = "LOCAL_CANONICAL_STORE";
  }

  const market = loadMarketEstimatedNeedPeriods(id, opts);
  const estimates = (market.periods || []).filter(
    (p) => !isTestScenarioPeriod(p)
  );

  if (conflicts.length && opts.persistConflicts !== false) {
    try {
      const file = quarantinePath(id);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(
        file,
        JSON.stringify(
          {
            version: GDI_NEED_DATE_SOURCE_CONTRACT_V2,
            hotelId: id,
            quarantinedAt: new Date().toISOString(),
            conflicts,
          },
          null,
          2
        ) + "\n"
      );
    } catch {
      /* non-fatal */
    }
  }

  const statusDoc = deriveHotelNeedDateConfigStatus({
    hotelId: id,
    hotelConfirmedPeriods: chosen,
    marketEstimatedPeriods: estimates,
  });

  return {
    version: GDI_NEED_DATE_SOURCE_CONTRACT_V2,
    hotelId: id,
    precedenceSource: source,
    airtableMirrorPresent: Boolean(mirror.present),
    airtableConfirmedCount: airtableConfirmed.length,
    localConfirmedCount: localPeriods.length,
    localDemoStubIgnored: localIsDemoStub && airtableConfirmed.length > 0,
    marketEstimatedCount: estimates.length,
    conflicts,
    statusDoc,
    periods: chosen,
    estimates,
  };
}

export function assertSourceContractInvariants(resolved = {}) {
  const failures = [];
  for (const p of resolved.periods || []) {
    if (isTestScenarioPeriod(p)) {
      failures.push(`confirmed_contains_test_scenario:${p.needPeriodId}`);
    }
    if (upper(p.provenance) === NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED) {
      failures.push(`confirmed_has_market_provenance:${p.needPeriodId}`);
    }
  }
  if (
    resolved.statusDoc?.status === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED &&
    !(resolved.periods || []).length
  ) {
    failures.push("confirmed_status_without_periods");
  }
  if (
    resolved.statusDoc?.status ===
      HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED &&
    (resolved.periods || []).length > 0
  ) {
    failures.push("estimated_status_with_confirmed_periods");
  }
  return { ok: failures.length === 0, failures };
}
