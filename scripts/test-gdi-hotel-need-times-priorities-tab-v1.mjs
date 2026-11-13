/**
 * Regression: Hotel Need Times & Priorities restored as GDI Tab 2.
 * Exercises contract enums, preference explanations, local persistence,
 * and portal API handlers without paid research.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
process.chdir(ROOT);

process.env.GROUP_DEMAND_INTELLIGENCE_PILOT_READ =
  process.env.GROUP_DEMAND_INTELLIGENCE_PILOT_READ || "1";
process.env.GDI_NEED_DATES_PORTAL_LOCAL_WRITE = "1";
process.env.GDI_NEED_DATES_AUTHORIZED_HOTELS =
  process.env.GDI_NEED_DATES_AUTHORIZED_HOTELS ||
  "recLuxvwwxID7U2B8,recgMYovrrZDJMqzX,recAAAATESTHOTEL1,recBBBTESTHOTEL2";

const {
  getCommercialPrioritiesEnums,
  emptyCommercialProfile,
  saveLocalCommercialProfile,
  loadLocalCommercialProfile,
  createNeedPeriodsFromInput,
  listNeedPeriodsForHotel,
  buildNeedDatesManagementDashboard,
  buildHotelPreferenceMatchExplanations,
  assertNeedDatesHotelAccess,
  NEED_DATES_PORTAL_ISOLATION_NAMESPACE,
  clearIsolatedNeedPeriodStore,
} = await import(
  "../lib/group-demand-intelligence/need-date-demand/index.js"
);

const hotelA = "recAAAATESTHOTEL1";
const hotelB = "recBBBTESTHOTEL2";

clearIsolatedNeedPeriodStore(hotelA);
clearIsolatedNeedPeriodStore(hotelB);

const enums = getCommercialPrioritiesEnums();
assert.ok(enums.revenuePriorities.length >= 8, "revenue priorities recovered");
assert.ok(enums.prioritySegments.length >= 8, "priority segments recovered");
assert.ok(enums.dayPreference.some((d) => d.id === "WEEKDAYS"));
assert.ok(enums.dayPreference.some((d) => d.id === "WEEKENDS"));
assert.ok(enums.demandGenerators?.length >= 8, "demand audiences recovered");
assert.ok(enums.businessToAvoid?.length >= 5, "business-to-avoid recovered");
assert.ok(enums.peakRooms?.length >= 5, "peak rooms recovered");
assert.equal(enums.prioritySegments.length, 14, "fourteen GDI segments");

const tabSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/hotel-need-times-priorities-tab.js"),
  "utf8",
);
assert.match(tabSrc, /Weekday \/ weekend targeting/);
assert.match(tabSrc, /Local demand audiences \/ participant sources/);
assert.match(tabSrc, /Demand segments \(14 GDI strategies\)/);
assert.match(tabSrc, /hcpDemandGen/);
assert.match(tabSrc, /hcpExcludeSegment/);
assert.match(tabSrc, /peakRoomsPreset/);
assert.match(tabSrc, /commercialDraftDirty/);

const prefs = buildHotelPreferenceMatchExplanations({
  commercialProfile: {
    revenuePriorities: ["LOCAL_CATERING"],
    dayPreference: "WEEKDAYS",
    specificDays: ["Tue", "Wed"],
  },
  opportunity: {
    title: "Local weekday catering meeting for association board",
    summary: "Tuesday midweek banquet and catering",
  },
});
assert.ok(
  prefs.some((p) => /local catering/i.test(p)),
  "local catering preference explanation",
);
assert.ok(
  prefs.some((p) => /weekday/i.test(p)),
  "weekday preference explanation",
);

const created = await createNeedPeriodsFromInput(
  hotelA,
  {
    months: [{ year: 2027, month: 2 }],
    revenueNeed: ["GROUP_GUESTROOMS", "LOCAL_CATERING"],
    priority: "HIGH",
    salesNotes: "Need Tue/Wed rooms",
    restrictions: "No late-night events",
  },
  {
    isolation: NEED_DATES_PORTAL_ISOLATION_NAMESPACE,
    useAirtable: false,
    actorUser: { id: "test", email: "test@dealality.local" },
    actorSource: "hotel_sales",
  },
);
assert.ok(created.ok !== false);
assert.ok((created.created || created.periods || []).length >= 1 || created.periods);

const listedA = listNeedPeriodsForHotel(hotelA, {
  isolation: NEED_DATES_PORTAL_ISOLATION_NAMESPACE,
});
assert.equal(listedA.periods.length >= 1, true, "hotel A periods persist");

const listedB = listNeedPeriodsForHotel(hotelB, {
  isolation: NEED_DATES_PORTAL_ISOLATION_NAMESPACE,
});
assert.equal(listedB.periods.length, 0, "hotel B stays independent");

const profilePatch = {
  ...emptyCommercialProfile(hotelA),
  revenuePriorities: ["LOCAL_CATERING", "GROUP_ROOM_NIGHTS"],
  dayPreference: "WEEKDAYS",
  specificDays: ["Tue", "Wed"],
  prioritySegments: ["local_catering_day_meetings"],
  restrictionNotes: "Prefer midweek groups",
  provenance: "Hotel Confirmed",
};
saveLocalCommercialProfile(hotelA, profilePatch);
const loaded = loadLocalCommercialProfile(hotelA);
assert.equal(loaded.dayPreference, "WEEKDAYS");
assert.ok(loaded.revenuePriorities.includes("LOCAL_CATERING"));

const other = loadLocalCommercialProfile(hotelB);
assert.notEqual(other.dayPreference, "WEEKDAYS");

const dash = await buildNeedDatesManagementDashboard(hotelA, {
  isolation: NEED_DATES_PORTAL_ISOLATION_NAMESPACE,
  useAirtable: false,
  year: 2027,
});
assert.equal(dash.ok, true);
assert.ok(dash.enums?.commercialPriorities?.revenuePriorities?.length >= 1);
assert.ok(dash.commercialProfile);

const denied = (() => {
  try {
    assertNeedDatesHotelAccess(
      { dealalityUser: { authorizedHotelIds: [hotelA] } },
      hotelB,
      { write: true },
    );
    return false;
  } catch (err) {
    return err.status === 403 || err.code === "HOTEL_ACCESS_DENIED";
  }
})();
assert.equal(denied, true, "cross-hotel write denied");

const uiPath = path.join(
  ROOT,
  "public/js/group-demand-intelligence/hotel-need-times-priorities-tab.js",
);
const uiSrc = fs.readFileSync(uiPath, "utf8");
assert.match(uiSrc, /DealalityGdiNeedTimesPrioritiesTab/);
assert.match(uiSrc, /<h1>Commercial Priorities<\/h1>/);

const appSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
assert.match(appSrc, /GDI_CUSTOMER_TABS/);
assert.match(appSrc, /gdiNeedTimesPrioritiesRoot/);

const uiLib = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8",
);
assert.match(uiLib, /GDI_PRIORITIES_TAB/);
assert.match(uiLib, /id: "priorities"/);
assert.match(uiLib, /label: "Commercial Priorities"/);
assert.match(uiLib, /id: "opportunities"/);

const html = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);
assert.match(html, /hotel-need-times-priorities-tab\.js/);
assert.match(html, /hotel-need-times-priorities\.css/);

const recoveredBranchNote =
  "feat/gdi-universal-need-date-intelligence-v1 @ 7fa87ebfd";
assert.ok(recoveredBranchNote.includes("7fa87ebfd"));

console.log(
  JSON.stringify(
    {
      ok: true,
      recoveredFrom: recoveredBranchNote,
      hotelAPeriods: listedA.periods.length,
      hotelBPeriods: listedB.periods.length,
      preferenceExplanations: prefs,
      dashboardMode: dash.dataMode,
      commercialProfileDayPreference: loaded.dayPreference,
    },
    null,
    2,
  ),
);
