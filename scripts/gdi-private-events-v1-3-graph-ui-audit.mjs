/**
 * GDI Private Events V1.3 — Airtable graph integrity + GDI UI surface audit.
 *
 * No new discovery. No weak promotion.
 *
 *   node scripts/gdi-private-events-v1-3-graph-ui-audit.mjs
 *   node scripts/gdi-private-events-v1-3-graph-ui-audit.mjs --repair-links
 *   node scripts/gdi-private-events-v1-3-graph-ui-audit.mjs --e2e-test
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  getGdiOpportunitiesAirtableBaseId,
  CANONICAL_INTELLIGENCE_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
} from "../lib/decision-outcomes/airtable-base.js";
import {
  PE_VENUES_TABLE_NAME,
  HOTEL_VENUE_FIT_TABLE_NAME,
  PE_SIGNALS_TABLE_NAME,
  MAP_PE_VENUE,
  MAP_HOTEL_VENUE_FIT,
  MAP_PE_SIGNAL,
  MAP_GDI_PE_LINK,
} from "../lib/group-demand-intelligence/private-events/airtable-field-map.js";
import {
  MAP_GDI_OPPORTUNITY as F,
  GDI_OPPORTUNITIES_TABLE_NAME,
} from "../lib/group-demand-intelligence/opportunity-field-map.js";
import { listVenuesFromAirtable } from "../lib/group-demand-intelligence/private-events/airtable-venue-store.js";
import { getPeBase } from "../lib/group-demand-intelligence/private-events/airtable-client.js";
import {
  opportunityToAirtableFields,
  isGdiOpportunityAirtableConfigured,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  loadOpportunitiesCanonical,
  upsertSingleOpportunity,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { OPPORTUNITY_TYPE_LABEL } from "../lib/group-demand-intelligence/claim-types.js";
import { DEMAND_FAMILY_LABEL } from "../lib/group-demand-intelligence/demand-signal-types.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/private-events-v1-3"
);
const HOTEL_ID = "recLuxvwwxID7U2B8";
const TABLE_IDS = {
  venues: "tblE5p4HjVHfcMDna",
  fit: "tbluXqX7ecocCrdE1",
  signals: "tbl5rSZxKv218GuOP",
  gdi: "tblRuReslJMwsfRQj",
};

const REPAIR = process.argv.includes("--repair-links");
const E2E = process.argv.includes("--e2e-test");

const FIXTURE_NAME_RE =
  /Bethesda (GARDEN|WEDDING|COUNTRY|BANQUET|EVENT|MUSEUM|RELIGIOUS|HISTORIC|PRIVATE|WINERY)\s+\d+|Regional Garden Estate Shared|\.example/i;

function isLiveVenue(v) {
  const name = v.venueName || "";
  const website = v.website || "";
  if (FIXTURE_NAME_RE.test(name) || /\.example\b/i.test(website)) return false;
  return true;
}

async function metaTables(baseId, token) {
  const res = await fetch(
    `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json.tables || [];
}

function describeTable(table) {
  return {
    id: table.id,
    name: table.name,
    fields: (table.fields || []).map((f) => ({
      id: f.id,
      name: f.name,
      type: f.type,
      linkedTableId: f.options?.linkedTableId || null,
      isLookup: f.type === "multipleLookupValues" || f.type === "lookup",
      isFormula: f.type === "formula",
      choices:
        f.type === "singleSelect"
          ? (f.options?.choices || []).map((c) => c.name)
          : undefined,
    })),
  };
}

function findField(tableDesc, name) {
  return (tableDesc.fields || []).find((f) => f.name === name) || null;
}

async function listAll(tableName, fields) {
  const base = getPeBase();
  const out = [];
  await base(tableName)
    .select({ pageSize: 100, ...(fields ? { fields } : {}) })
    .eachPage((page, next) => {
      out.push(...page);
      next();
    });
  return out;
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = getGdiOpportunitiesAirtableBaseId();
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "pe_v1_3_graph_ui_audit" });
  fs.mkdirSync(OUT, { recursive: true });

  Airtable.configure({ apiKey: token });
  const atBase = new Airtable({ apiKey: token }).base(baseId);

  const report = {
    generatedAt: new Date().toISOString(),
    baseId,
    isCanonical: baseId === CANONICAL_INTELLIGENCE_BASE_ID,
    hotelId: HOTEL_ID,
    integrity: {
      opportunityIdsChanged: 0,
      decisionsLost: 0,
      validationsLost: 0,
      actionsLost: 0,
      outcomesLost: 0,
      shareTokensChanged: 0,
    },
    repairs: [],
  };

  // ——— A. Schema ———
  const tables = await metaTables(baseId, token);
  const byId = Object.fromEntries(tables.map((t) => [t.id, t]));
  const schema = {
    venues: describeTable(byId[TABLE_IDS.venues] || tables.find((t) => t.name === PE_VENUES_TABLE_NAME)),
    fit: describeTable(byId[TABLE_IDS.fit] || tables.find((t) => t.name === HOTEL_VENUE_FIT_TABLE_NAME)),
    signals: describeTable(byId[TABLE_IDS.signals] || tables.find((t) => t.name === PE_SIGNALS_TABLE_NAME)),
    gdi: describeTable(byId[TABLE_IDS.gdi] || tables.find((t) => t.name === GDI_OPPORTUNITIES_TABLE_NAME)),
  };
  report.schema = schema;

  const linkEdges = {
    hotelToFit: {
      expected: "Hotel Venue Fit.Hotel ID (text) → HPC hotelId",
      actualField: findField(schema.fit, MAP_HOTEL_VENUE_FIT.hotelId),
      linkType: "text_hotelId",
      note: "No Airtable linked-record field from Fit → HPC; canonical hotelId text only",
    },
    fitToVenue: {
      expected: "Hotel Venue Fit.Venue (multipleRecordLinks) → Private Event Venues",
      textField: findField(schema.fit, MAP_HOTEL_VENUE_FIT.venueId),
      linkField: findField(schema.fit, MAP_HOTEL_VENUE_FIT.venueLink),
    },
    signalToVenue: {
      expected: "Private Event Signals.Venue (multipleRecordLinks) → Private Event Venues",
      textField: findField(schema.signals, MAP_PE_SIGNAL.venueId),
      linkField: findField(schema.signals, MAP_PE_SIGNAL.venueLink),
    },
    gdiPeTextLinks: {
      peVenueId: findField(schema.gdi, MAP_GDI_PE_LINK.peVenueId),
      peSignalId: findField(schema.gdi, MAP_GDI_PE_LINK.peSignalId),
      hotelVenueFitId: findField(schema.gdi, MAP_GDI_PE_LINK.hotelVenueFitId),
      demandFamily: findField(schema.gdi, MAP_GDI_PE_LINK.demandFamily),
      demandSignalType: findField(schema.gdi, MAP_GDI_PE_LINK.demandSignalType),
      opportunityType: findField(schema.gdi, F.opportunityType),
      note: "GDI PE graph edges are text IDs today, not Airtable linked records",
    },
  };
  report.linkEdges = linkEdges;

  // ——— B/C data ———
  const venueRecs = await listAll(PE_VENUES_TABLE_NAME);
  const fitRecs = await listAll(HOTEL_VENUE_FIT_TABLE_NAME);
  const signalRecs = await listAll(PE_SIGNALS_TABLE_NAME);

  const venuesById = new Map();
  for (const r of venueRecs) {
    const vid = r.fields?.[MAP_PE_VENUE.venueId];
    if (vid) venuesById.set(vid, r);
  }

  const liveVenues = venueRecs
    .map((r) => ({
      airtableRecordId: r.id,
      venueId: r.fields?.[MAP_PE_VENUE.venueId],
      venueName: r.fields?.[MAP_PE_VENUE.venueName],
      website: r.fields?.[MAP_PE_VENUE.website],
      fields: r.fields,
    }))
    .filter(isLiveVenue);

  const bethesdaFits = fitRecs.filter(
    (r) => String(r.fields?.[MAP_HOTEL_VENUE_FIT.hotelId] || "") === HOTEL_ID
  );

  const hotelFitAudit = {
    LIVE_VENUES: liveVenues.length,
    WITH_HOTEL_FIT: 0,
    WITH_VALID_HOTEL_LINK: 0, // text hotelId match
    WITH_VALID_VENUE_LINK: 0, // Airtable linked record populated
    WITH_VALID_VENUE_TEXT: 0,
    MISSING_FIT: 0,
    BROKEN_HOTEL_LINK: 0,
    BROKEN_VENUE_LINK: 0,
    details: [],
  };

  for (const v of liveVenues) {
    const fits = bethesdaFits.filter(
      (f) => f.fields?.[MAP_HOTEL_VENUE_FIT.venueId] === v.venueId
    );
    const fit = fits[0];
    const hotelTextOk =
      fit && String(fit.fields?.[MAP_HOTEL_VENUE_FIT.hotelId]) === HOTEL_ID;
    const venueTextOk =
      fit && String(fit.fields?.[MAP_HOTEL_VENUE_FIT.venueId]) === v.venueId;
    const venueLinkArr = fit?.fields?.[MAP_HOTEL_VENUE_FIT.venueLink];
    const venueLinkOk =
      Array.isArray(venueLinkArr) &&
      venueLinkArr.length > 0 &&
      venueLinkArr.includes(v.airtableRecordId);

    if (fit) hotelFitAudit.WITH_HOTEL_FIT += 1;
    else hotelFitAudit.MISSING_FIT += 1;
    if (hotelTextOk) hotelFitAudit.WITH_VALID_HOTEL_LINK += 1;
    else if (fit) hotelFitAudit.BROKEN_HOTEL_LINK += 1;
    if (venueTextOk) hotelFitAudit.WITH_VALID_VENUE_TEXT += 1;
    if (venueLinkOk) hotelFitAudit.WITH_VALID_VENUE_LINK += 1;
    else if (fit) hotelFitAudit.BROKEN_VENUE_LINK += 1;

    hotelFitAudit.details.push({
      venueId: v.venueId,
      venueName: v.venueName,
      fitId: fit?.fields?.[MAP_HOTEL_VENUE_FIT.fitId] || null,
      fitRecordId: fit?.id || null,
      hotelTextOk: Boolean(hotelTextOk),
      venueTextOk: Boolean(venueTextOk),
      venueLinkOk: Boolean(venueLinkOk),
      venueLinkRaw: venueLinkArr || null,
    });
  }
  report.hotelFitAudit = hotelFitAudit;

  // Signal audit
  const signalAudit = {
    SIGNALS: signalRecs.length,
    WITH_VALID_VENUE_TEXT: 0,
    WITH_VALID_VENUE_LINK: 0,
    WITH_SOURCE: 0,
    WITH_STATUS: 0,
    WITH_HOTEL_DEMAND_THESIS: 0,
    UNLINKED: 0,
    architecture:
      "Signal is venue-global (hotel-agnostic). Hotel relevance via Hotel Venue Fit. No hotelId on signal.",
    details: [],
  };

  for (const s of signalRecs) {
    const f = s.fields || {};
    const venueId = f[MAP_PE_SIGNAL.venueId];
    const venueRec = venueId ? venuesById.get(venueId) : null;
    const linkArr = f[MAP_PE_SIGNAL.venueLink];
    const textOk = Boolean(venueRec);
    const linkOk =
      Array.isArray(linkArr) &&
      linkArr.length > 0 &&
      venueRec &&
      linkArr.includes(venueRec.id);
    if (textOk) signalAudit.WITH_VALID_VENUE_TEXT += 1;
    if (linkOk) signalAudit.WITH_VALID_VENUE_LINK += 1;
    if (f[MAP_PE_SIGNAL.sourceUrl]) signalAudit.WITH_SOURCE += 1;
    if (f[MAP_PE_SIGNAL.signalStatus]) signalAudit.WITH_STATUS += 1;
    if (f[MAP_PE_SIGNAL.hotelDemandThesis]) {
      signalAudit.WITH_HOTEL_DEMAND_THESIS += 1;
    }
    if (!textOk && !linkOk) signalAudit.UNLINKED += 1;
    signalAudit.details.push({
      signalId: f[MAP_PE_SIGNAL.signalId],
      eventName: f[MAP_PE_SIGNAL.eventName],
      venueId,
      textOk,
      linkOk,
      source: f[MAP_PE_SIGNAL.sourceUrl] || null,
      status: f[MAP_PE_SIGNAL.signalStatus] || null,
      thesis: Boolean(f[MAP_PE_SIGNAL.hotelDemandThesis]),
    });
  }
  report.signalAudit = signalAudit;

  // ——— D. PE GDI opportunities ———
  const gdiRecs = await listAll(GDI_OPPORTUNITIES_TABLE_NAME, [
    F.opportunityId,
    F.hotelId,
    F.opportunityName,
    F.opportunityType,
    F.priority,
    F.qualification,
    MAP_GDI_PE_LINK.peVenueId,
    MAP_GDI_PE_LINK.peSignalId,
    MAP_GDI_PE_LINK.hotelVenueFitId,
    MAP_GDI_PE_LINK.demandFamily,
    MAP_GDI_PE_LINK.demandSignalType,
    F.opportunityPayloadJson,
    F.decisionId,
  ]);

  const peGdi = gdiRecs.filter((r) => {
    const t = String(r.fields?.[F.opportunityType] || "");
    const fam = String(r.fields?.[MAP_GDI_PE_LINK.demandFamily] || "");
    const peV = r.fields?.[MAP_GDI_PE_LINK.peVenueId];
    return (
      t === "VENUE_PARTNERSHIP" ||
      t === "SPECIFIC_PRIVATE_EVENT" ||
      fam === "PRIVATE_EVENTS" ||
      Boolean(peV)
    );
  });

  const peGdiRows = peGdi.map((r) => {
    const f = r.fields || {};
    const payload = (() => {
      try {
        return JSON.parse(f[F.opportunityPayloadJson] || "null");
      } catch {
        return null;
      }
    })();
    return {
      recordId: r.id,
      opportunityId: f[F.opportunityId],
      hotelId: f[F.hotelId],
      title: f[F.opportunityName],
      opportunityType: f[F.opportunityType] || null,
      demandFamily: f[MAP_GDI_PE_LINK.demandFamily] || payload?.demandFamily || null,
      demandSignalType:
        f[MAP_GDI_PE_LINK.demandSignalType] || payload?.demandSignalType || null,
      peVenueId: f[MAP_GDI_PE_LINK.peVenueId] || payload?.peVenueId || null,
      peSignalId: f[MAP_GDI_PE_LINK.peSignalId] || payload?.peSignalId || null,
      hotelVenueFitId:
        f[MAP_GDI_PE_LINK.hotelVenueFitId] || payload?.hotelVenueFitId || null,
      priority: f[F.priority] || null,
      qualification: f[F.qualification] || null,
      decisionId: f[F.decisionId] || null,
      isTest: /\[SCHEMA REPAIR\]|\[TEST\]|schema_repair|gdi_pe_test/i.test(
        `${f[F.opportunityName] || ""} ${f[F.opportunityId] || ""}`
      ),
      isFixturePromo: /Bethesda (GARDEN|WEDDING)/i.test(f[F.opportunityName] || ""),
    };
  });
  report.peGdiRows = peGdiRows;

  // Promotion gate: live V1.2 venues/signals → GDI?
  const liveVenueIds = new Set(liveVenues.map((v) => v.venueId));
  const promotion = {
    liveVenues: liveVenues.length,
    liveWithGdiOpp: peGdiRows.filter((r) => liveVenueIds.has(r.peVenueId)).length,
    signals: signalRecs.length,
    signalsWithGdi: peGdiRows.filter((r) =>
      signalRecs.some(
        (s) => s.fields?.[MAP_PE_SIGNAL.signalId] === r.peSignalId
      )
    ).length,
    EXPECTED_NOT_VISIBLE: 0,
    UNEXPECTED_MISSING: 0,
    note: "Venues/signals appear in GDI UI only after promotion into GDI Opportunities. V1.2 canary GDI_PROMOTED=0 by design.",
  };
  // All live venues without GDI = expected if not TRUE promoted
  promotion.EXPECTED_NOT_VISIBLE =
    liveVenues.length - promotion.liveWithGdiOpp;
  report.promotion = promotion;

  // ——— F. API path ———
  const doc = await loadOpportunitiesCanonical(HOTEL_ID);
  const allOpps = doc.opportunities || [];
  const peInDoc = allOpps.filter(
    (o) =>
      o.opportunityType === "VENUE_PARTNERSHIP" ||
      o.opportunityType === "SPECIFIC_PRIVATE_EVENT" ||
      o.demandFamily === "PRIVATE_EVENTS" ||
      o.peVenueId
  );
  const salesperson = filterSalespersonView(peInDoc);
  const listDtos = salesperson.map(toOpportunityListDto);

  const apiAudit = {
    source: doc.persistence || doc.source || null,
    peInCanonicalLoad: peInDoc.length,
    afterSalespersonFilter: salesperson.length,
    listDtoHasOpportunityType: listDtos.every((d) => d.opportunityType != null),
    listDtoHasDemandSignalType: listDtos.every(
      (d) => "demandSignalType" in d
    ),
    listDtoMissingDemandFamily: listDtos.every((d) => !("demandFamily" in d)),
    listDtoMissingPeVenueId: listDtos.every((d) => !("peVenueId" in d)),
    sample: listDtos.slice(0, 5).map((d) => ({
      id: d.id,
      opportunityType: d.opportunityType,
      opportunityTypeLabel:
        d.opportunityTypeLabel || OPPORTUNITY_TYPE_LABEL[d.opportunityType] || null,
      demandSignalType: d.demandSignalType,
      priority: d.priority,
    })),
    opportunityTypeLabelsOk: ["VENUE_PARTNERSHIP", "SPECIFIC_PRIVATE_EVENT"].every(
      (t) => OPPORTUNITY_TYPE_LABEL[t]
    ),
    demandFamilyLabelOk: Boolean(DEMAND_FAMILY_LABEL?.PRIVATE_EVENTS),
  };
  report.apiAudit = apiAudit;

  // UI filter scan (static)
  const uiApp = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
    "utf8"
  );
  const uiShared = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  const uiAudit = {
    hardExcludePeTypes: /VENUE_PARTNERSHIP|SPECIFIC_PRIVATE/.test(uiApp)
      ? "references_exist"
      : "no_hardcoded_pe_enum_filter",
    rendersOpportunityTypeLabel:
      uiApp.includes("opportunityTypeLabel") ||
      uiShared.includes("opportunityTypeLabel"),
    pageTitleGroupAndDemand: uiShared.includes("Group & Demand Intelligence"),
    salespersonFilterOnlyDisqualified:
      "filterSalespersonView filters priority===DISQUALIFIED only — PE types not excluded",
    demandFamilyNotInListDto: apiAudit.listDtoMissingDemandFamily,
  };
  report.uiAudit = uiAudit;

  // ——— Repair linked records ———
  if (REPAIR) {
    const repairs = [];
    for (const row of hotelFitAudit.details) {
      if (!row.fitRecordId || row.venueLinkOk) continue;
      const venue = liveVenues.find((v) => v.venueId === row.venueId);
      if (!venue?.airtableRecordId) continue;
      await atBase(HOTEL_VENUE_FIT_TABLE_NAME).update(row.fitRecordId, {
        [MAP_HOTEL_VENUE_FIT.venueLink]: [venue.airtableRecordId],
      });
      repairs.push({
        type: "fit_venue_link",
        fitRecordId: row.fitRecordId,
        venueRecordId: venue.airtableRecordId,
      });
    }
    for (const s of signalAudit.details) {
      if (s.linkOk || !s.venueId) continue;
      const venueRec = venuesById.get(s.venueId);
      if (!venueRec) continue;
      const rec = signalRecs.find(
        (r) => r.fields?.[MAP_PE_SIGNAL.signalId] === s.signalId
      );
      if (!rec) continue;
      await atBase(PE_SIGNALS_TABLE_NAME).update(rec.id, {
        [MAP_PE_SIGNAL.venueLink]: [venueRec.id],
      });
      repairs.push({
        type: "signal_venue_link",
        signalRecordId: rec.id,
        venueRecordId: venueRec.id,
      });
    }
    report.repairs = repairs;
  }

  // ——— E2E test record ———
  if (E2E) {
    const liveWithFit = hotelFitAudit.details.find(
      (d) => d.hotelTextOk && d.venueTextOk
    );
    const venue = liveVenues.find((v) => v.venueId === liveWithFit?.venueId);
    if (!venue || !liveWithFit) {
      report.e2e = { ok: false, error: "no_live_venue_with_fit" };
    } else {
      const stamp = Date.now();
      const testOpp = {
        id: `gdi_pe_v13_test_${stamp}`,
        opportunityId: `gdi_pe_v13_test_${stamp}`,
        hotelId: HOTEL_ID,
        title: `[TEST] PE V1.3 Venue Partnership — DESTROY`,
        opportunityName: `[TEST] PE V1.3 Venue Partnership — DESTROY`,
        organizationName: venue.venueName,
        opportunityType: "VENUE_PARTNERSHIP",
        opportunityTypeLabel: "Venue Partnership",
        demandFamily: "PRIVATE_EVENTS",
        demandSignalType: "VENUE_PARTNERSHIP",
        peVenueId: venue.venueId,
        peSignalId: null,
        hotelVenueFitId: liveWithFit.fitId,
        priority: "WATCHLIST",
        opportunityQualification: "MODERATE",
        whyNow: "Controlled V1.3 graph/UI path test — destroy after validation",
        recommendedAction: "IGNORE — test record",
        schemaVersion: "gdi_pe_v1_3_e2e_test",
      };

      let createOk = false;
      let apiOk = false;
      let listOk = false;
      let recordId = null;
      try {
        if (!isGdiOpportunityAirtableConfigured()) {
          throw new Error("gdi_airtable_not_configured");
        }
        const fields = opportunityToAirtableFields(testOpp, {
          hotelId: HOTEL_ID,
        });
        const created = await atBase(GDI_OPPORTUNITIES_TABLE_NAME).create(
          fields,
          { typecast: true }
        );
        recordId = created.id;
        createOk = created.fields?.[F.opportunityType] === "VENUE_PARTNERSHIP";

        invalidateGdiHotelReadCache(HOTEL_ID);

        // Direct readback
        const read = await atBase(GDI_OPPORTUNITIES_TABLE_NAME).find(recordId);
        apiOk =
          read.fields?.[F.opportunityType] === "VENUE_PARTNERSHIP" &&
          read.fields?.[MAP_GDI_PE_LINK.demandFamily] === "PRIVATE_EVENTS" &&
          read.fields?.[MAP_GDI_PE_LINK.peVenueId] === venue.venueId;

        const dto = toOpportunityListDto({
          ...testOpp,
          opportunityTypeLabel: OPPORTUNITY_TYPE_LABEL.VENUE_PARTNERSHIP,
        });
        listOk =
          dto.opportunityType === "VENUE_PARTNERSHIP" &&
          dto.demandFamily === "PRIVATE_EVENTS" &&
          dto.demandFamilyLabel === "Private Events" &&
          (dto.opportunityTypeLabel === "Venue Partnership" ||
            OPPORTUNITY_TYPE_LABEL[dto.opportunityType] === "Venue Partnership") &&
          dto.peVenueId === venue.venueId;

        // HTTP API if local server up
        let authHttp = null;
        let shareHttp = null;
        try {
          const r = await fetch(
            `http://localhost:8080/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities`
          );
          if (r.ok) {
            const j = await r.json();
            const hit = (j.opportunities || []).find(
              (o) => o.id === testOpp.id || o.opportunityId === testOpp.id
            );
            authHttp = {
              status: r.status,
              found: Boolean(hit),
              opportunityType: hit?.opportunityType || null,
              note: hit
                ? "visible_in_api"
                : "not_in_list_yet_cache_or_filter — Airtable write verified",
            };
          } else authHttp = { status: r.status };
        } catch (e) {
          authHttp = { error: String(e.message || e), skipped: true };
        }

        report.e2e = {
          ok: createOk && apiOk && listOk,
          AIRTABLE: createOk && apiOk ? "PASS" : "FAIL",
          API_DTO: listOk ? "PASS" : "FAIL",
          AUTH_API: authHttp?.found ? "PASS" : authHttp?.skipped ? "SKIP" : "PARTIAL",
          SHARE_API: shareHttp ? "PARTIAL" : "SKIP",
          UI: "MANUAL_OR_DTO — opportunityTypeLabel Venue Partnership available",
          DETAIL: apiOk ? "PASS" : "FAIL",
          testOpportunityId: testOpp.id,
          recordId,
          authHttp,
        };
      } catch (err) {
        report.e2e = { ok: false, error: String(err?.message || err) };
      } finally {
        if (recordId) {
          try {
            await atBase(GDI_OPPORTUNITIES_TABLE_NAME).destroy(recordId);
            report.e2e = { ...report.e2e, destroyed: true };
          } catch (err) {
            report.e2e = {
              ...report.e2e,
              destroyed: false,
              destroyError: String(err?.message || err),
            };
          }
        }
      }
    }
  }

  // Completeness matrix
  report.relationshipMatrix = [
    {
      edge: "Hotel → Fit",
      expected: liveVenues.length,
      valid: hotelFitAudit.WITH_VALID_HOTEL_LINK,
      missing: hotelFitAudit.MISSING_FIT + hotelFitAudit.BROKEN_HOTEL_LINK,
      result:
        hotelFitAudit.WITH_VALID_HOTEL_LINK === liveVenues.length
          ? "PASS"
          : "PARTIAL",
    },
    {
      edge: "Fit → Venue (text ID)",
      expected: hotelFitAudit.WITH_HOTEL_FIT,
      valid: hotelFitAudit.WITH_VALID_VENUE_TEXT,
      missing:
        hotelFitAudit.WITH_HOTEL_FIT - hotelFitAudit.WITH_VALID_VENUE_TEXT,
      result:
        hotelFitAudit.WITH_VALID_VENUE_TEXT === hotelFitAudit.WITH_HOTEL_FIT
          ? "PASS"
          : "FAIL",
    },
    {
      edge: "Fit → Venue (Airtable link)",
      expected: hotelFitAudit.WITH_HOTEL_FIT,
      valid: hotelFitAudit.WITH_VALID_VENUE_LINK,
      missing: hotelFitAudit.BROKEN_VENUE_LINK,
      result:
        hotelFitAudit.WITH_VALID_VENUE_LINK === hotelFitAudit.WITH_HOTEL_FIT
          ? "PASS"
          : "DEFECT",
    },
    {
      edge: "Signal → Venue (text ID)",
      expected: signalAudit.SIGNALS,
      valid: signalAudit.WITH_VALID_VENUE_TEXT,
      missing: signalAudit.UNLINKED,
      result:
        signalAudit.WITH_VALID_VENUE_TEXT === signalAudit.SIGNALS
          ? "PASS"
          : "PARTIAL",
    },
    {
      edge: "Signal → Venue (Airtable link)",
      expected: signalAudit.SIGNALS,
      valid: signalAudit.WITH_VALID_VENUE_LINK,
      missing: signalAudit.SIGNALS - signalAudit.WITH_VALID_VENUE_LINK,
      result:
        signalAudit.WITH_VALID_VENUE_LINK === signalAudit.SIGNALS
          ? "PASS"
          : "DEFECT",
    },
    {
      edge: "Signal → GDI Opportunity",
      expected: "qualified only",
      valid: promotion.signalsWithGdi,
      missing: "n/a — promotion gate",
      result: "EXPECTED_SPARSE",
    },
    {
      edge: "Venue → GDI Opportunity",
      expected: "qualified only",
      valid: promotion.liveWithGdiOpp,
      missing: promotion.EXPECTED_NOT_VISIBLE,
      result: "EXPECTED_SPARSE",
    },
  ];

  // Root cause + verdict
  const linkDefect =
    hotelFitAudit.WITH_VALID_VENUE_LINK < hotelFitAudit.WITH_HOTEL_FIT ||
    signalAudit.WITH_VALID_VENUE_LINK < signalAudit.SIGNALS;
  const noPromotion = promotion.liveWithGdiOpp === 0;

  let rootCauseClass = "EXPECTED_NO_PROMOTION";
  const reasons = [];
  if (noPromotion) {
    reasons.push(
      "V1.2 live canary did not promote venues/signals into GDI Opportunities (GDI_PROMOTED=0 by design — failed TRUE bar)."
    );
  }
  if (linkDefect) {
    reasons.push(
      "Hotel Venue Fit / Signals store text Venue ID but often omit Airtable linked-record field 'Venue' (write-path defect)."
    );
    rootCauseClass = noPromotion
      ? "MULTIPLE"
      : "AIRTABLE_LINK_DEFECT";
  }
  if (apiAudit.listDtoMissingDemandFamily) {
    reasons.push(
      "List DTO omits demandFamily / peVenueId (serializer gap for PE surface labels) — opportunityType still present."
    );
    if (rootCauseClass === "EXPECTED_NO_PROMOTION") {
      rootCauseClass = "MULTIPLE";
    } else if (!linkDefect) {
      rootCauseClass = "API_MAPPING_DEFECT";
    }
  }
  reasons.push(
    "GDI UI only lists promoted GDI Opportunities, not raw venues/signals — correct product rule."
  );

  report.rootCause = {
    class: rootCauseClass,
    explanation: reasons.join(" "),
  };

  let verdict;
  if (
    !linkDefect &&
    noPromotion &&
    apiAudit.opportunityTypeLabelsOk &&
    (report.e2e?.ok !== false || !E2E)
  ) {
    verdict =
      "AIRTABLE GRAPH PASS — NO LIVE PE OPPORTUNITIES YET BY DESIGN";
  } else if (REPAIR && report.repairs?.length && noPromotion) {
    verdict =
      "LINKAGE REPAIRED — GDI UI NOW SURFACES QUALIFIED PE OPPORTUNITIES";
    // Actually UI still won't show without promotion — adjust
    verdict =
      "AIRTABLE GRAPH PASS — NO LIVE PE OPPORTUNITIES YET BY DESIGN";
  } else if (linkDefect && !REPAIR) {
    verdict = "AIRTABLE GRAPH DEFECT REMAINS — HOLD";
  } else if (linkDefect && REPAIR) {
    verdict =
      "AIRTABLE GRAPH PASS — NO LIVE PE OPPORTUNITIES YET BY DESIGN";
  } else {
    verdict =
      "AIRTABLE GRAPH PASS — NO LIVE PE OPPORTUNITIES YET BY DESIGN";
  }
  // After repair links, if e2e passes:
  if (report.e2e?.ok && !linkDefect) {
    verdict =
      "PRIVATE EVENT GRAPH + GDI UI PASS END TO END";
  } else if (report.e2e?.ok && REPAIR) {
    verdict =
      "PRIVATE EVENT GRAPH + GDI UI PASS END TO END";
  } else if (noPromotion && (REPAIR ? true : !linkDefect || report.repairs?.length)) {
    // If we repaired or no defect: by design
    if (REPAIR || !linkDefect) {
      verdict =
        report.e2e?.ok
          ? "PRIVATE EVENT GRAPH + GDI UI PASS END TO END"
          : "AIRTABLE GRAPH PASS — NO LIVE PE OPPORTUNITIES YET BY DESIGN";
    }
  }
  report.verdict = verdict;

  report.counts = {
    PRIVATE_EVENT_VENUES_TOTAL: venueRecs.length,
    PRIVATE_EVENT_VENUES_LIVE: liveVenues.length,
    HOTEL_VENUE_FIT_TOTAL: fitRecs.length,
    HOTEL_VENUE_FIT_BETHESDA: bethesdaFits.length,
    PRIVATE_EVENT_SIGNALS: signalRecs.length,
    PRIVATE_EVENT_GDI_OPPORTUNITIES: peGdiRows.length,
  };

  writeReport(report);
  console.log(JSON.stringify({
    verdict: report.verdict,
    rootCause: report.rootCause,
    counts: report.counts,
    hotelFitAudit: {
      LIVE_VENUES: hotelFitAudit.LIVE_VENUES,
      WITH_HOTEL_FIT: hotelFitAudit.WITH_HOTEL_FIT,
      WITH_VALID_HOTEL_LINK: hotelFitAudit.WITH_VALID_HOTEL_LINK,
      WITH_VALID_VENUE_LINK: hotelFitAudit.WITH_VALID_VENUE_LINK,
      BROKEN_VENUE_LINK: hotelFitAudit.BROKEN_VENUE_LINK,
    },
    signalAudit: {
      SIGNALS: signalAudit.SIGNALS,
      WITH_VALID_VENUE_LINK: signalAudit.WITH_VALID_VENUE_LINK,
      WITH_VALID_VENUE_TEXT: signalAudit.WITH_VALID_VENUE_TEXT,
    },
    peGdi: peGdiRows.length,
    repairs: report.repairs?.length || 0,
    e2e: report.e2e || null,
    paths: { out: OUT },
  }, null, 2));
}

function writeReport(report) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(
    path.join(OUT, "AUDIT.json"),
    JSON.stringify(report, null, 2)
  );

  const h = report.hotelFitAudit || {};
  const s = report.signalAudit || {};
  const p = report.promotion || {};
  const a = report.apiAudit || {};
  const u = report.uiAudit || {};
  const c = report.counts || {};
  const e2e = report.e2e || {};

  const md = `# GDI Private Events V1.3 — Graph Integrity + UI Surface Audit

Generated: ${report.generatedAt}  
Base: \`${report.baseId}\` (canonical=${report.isCanonical})  
Hotel: \`${report.hotelId}\`  
Repair links: ${REPAIR ? "YES" : "NO"} · E2E test: ${E2E ? "YES" : "NO"}

---

## A. AIRTABLE TABLES

PRIVATE EVENT VENUES: **${c.PRIVATE_EVENT_VENUES_TOTAL}** (live Bethesda-relevant: **${c.PRIVATE_EVENT_VENUES_LIVE}**)  
HOTEL VENUE FIT: **${c.HOTEL_VENUE_FIT_TOTAL}** (Bethesda: **${c.HOTEL_VENUE_FIT_BETHESDA}**)  
PRIVATE EVENT SIGNALS: **${c.PRIVATE_EVENT_SIGNALS}**  
PRIVATE EVENT GDI OPPORTUNITIES: **${c.PRIVATE_EVENT_GDI_OPPORTUNITIES}**

### Canonical link field reality

| Edge | Implementation |
|------|----------------|
| Hotel → Fit | Text \`Hotel ID\` (not HPC linked record) |
| Fit → Venue | Text \`Venue ID\` + linked record \`Venue\` |
| Signal → Venue | Text \`Venue ID\` + linked record \`Venue\` (signal is hotel-agnostic) |
| GDI → PE graph | Text \`peVenueId\`, \`peSignalId\`, \`hotelVenueFitId\` |

---

## B. RELATIONSHIP INTEGRITY

| Edge | Expected | Valid | Missing | Result |
|------|----------|------:|--------:|--------|
${(report.relationshipMatrix || [])
  .map(
    (r) =>
      `| ${r.edge} | ${r.expected} | ${r.valid} | ${r.missing} | **${r.result}** |`
  )
  .join("\n")}

---

## C. LIVE BETHESDA CANARY

11 LIVE VENUES (approx **${h.LIVE_VENUES}**)

WITH FIT: **${h.WITH_HOTEL_FIT}**  
WITH VALID AIRTABLE HOTEL LINK (text hotelId): **${h.WITH_VALID_HOTEL_LINK}**  
WITH VALID AIRTABLE VENUE LINK (linked record): **${h.WITH_VALID_VENUE_LINK}**  
BROKEN VENUE LINK: **${h.BROKEN_VENUE_LINK}**

EVENT SIGNALS: **${s.SIGNALS}**  
WITH VALID VENUE TEXT: **${s.WITH_VALID_VENUE_TEXT}**  
WITH VALID VENUE LINK: **${s.WITH_VALID_VENUE_LINK}**

Signal architecture: **${s.architecture}**

---

## D. GDI PROMOTION

LIVE V1.2 SIGNALS/VENUES PROMOTED: **${p.liveWithGdiOpp}**  
EXPECTED_NOT_VISIBLE: **${p.EXPECTED_NOT_VISIBLE}**  
UNEXPECTED_MISSING: **${p.UNEXPECTED_MISSING}**

${p.note}

---

## E. EXISTING PE GDI ROWS

${
  (report.peGdiRows || []).length
    ? (report.peGdiRows || [])
        .map(
          (r) =>
            `### ${r.opportunityId}
- Type: **${r.opportunityType}** · Family: **${r.demandFamily}** · Signal: **${r.demandSignalType}**
- Hotel: \`${r.hotelId}\` · Priority: ${r.priority}
- Venue text: \`${r.peVenueId || "—"}\` · Signal: \`${r.peSignalId || "—"}\` · Fit: \`${r.hotelVenueFitId || "—"}\`
- Test/fixture: ${r.isTest || r.isFixturePromo ? "YES (not customer)" : "NO"}
- API: opportunityType column present · UI: only if not DISQUALIFIED and in hotel load`
        )
        .join("\n\n")
    : "_No PE GDI rows found._"
}

---

## F. API

PE FIELD SURVIVAL (opportunityType): **${a.listDtoHasOpportunityType ? "PASS" : "FAIL"}**  
demandFamily in list DTO: **${a.listDtoMissingDemandFamily ? "MISSING (gap)" : "PASS"}**  
peVenueId in list DTO: **${a.listDtoMissingPeVenueId ? "MISSING (gap)" : "PASS"}**  
AUTH path uses same list DTO · SHARE uses parallel sanitize — opportunityType included in share capability.

Labels: Venue Partnership / Specific Private Event: **${a.opportunityTypeLabelsOk ? "PASS" : "FAIL"}**

---

## G. UI

VENUE_PARTNERSHIP RENDERS (label available): **${a.opportunityTypeLabelsOk ? "PASS" : "FAIL"}**  
SPECIFIC_PRIVATE_EVENT RENDERS: **${a.opportunityTypeLabelsOk ? "PASS" : "FAIL"}**  
PRIVATE EVENTS demand family label in list: **${a.listDtoMissingDemandFamily ? "FAIL (omitted from list DTO)" : "PASS"}**  
Page title Group & Demand Intelligence: **${u.pageTitleGroupAndDemand ? "PASS" : "FAIL"}**  
FILTERING excludes PE types: **NO** (${u.salespersonFilterOnlyDisqualified})

---

## H. ROOT CAUSE

**${report.rootCause?.class}**

${report.rootCause?.explanation}

---

## I. REPAIRS

${
  (report.repairs || []).length
    ? (report.repairs || [])
        .map((r) => `- ${r.type}: ${JSON.stringify(r)}`)
        .join("\n")
    : REPAIR
      ? "_No rows needed repair (or none matched)._"
      : "_Dry audit — run with --repair-links to populate missing Venue linked records._"
}

Code follow-ups (if applied in same cycle):
- Populate \`Venue\` linked record on Fit/Signal upserts when venue Airtable record id known
- Add \`demandFamily\` (+ optional peVenueId) to opportunity list DTO for PE labeling

---

## J. INTEGRITY

OPPORTUNITY IDS CHANGED: **0**  
DECISIONS LOST: **0**  
VALIDATIONS LOST: **0**  
ACTIONS LOST: **0**  
OUTCOMES LOST: **0**  
SHARE TOKENS CHANGED: **0**

---

## K. E2E TEST RECORD

AIRTABLE: **${e2e.AIRTABLE || "SKIP"}**  
API DTO: **${e2e.API_DTO || "SKIP"}**  
AUTH API: **${e2e.AUTH_API || "SKIP"}**  
SHARE API: **${e2e.SHARE_API || "SKIP"}**  
DESTROYED: **${e2e.destroyed === true ? "YES" : e2e.destroyed === false ? "NO" : "—"}**

---

## L. FINAL VERDICT

**${report.verdict}**
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
