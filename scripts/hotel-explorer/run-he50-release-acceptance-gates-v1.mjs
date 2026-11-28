#!/usr/bin/env node
/**
 * HE 5.0 release acceptance gates (Bethesda-focused).
 * Runs Contact V3 unit gates + Live API probes. Browser is NOT_TESTED unless --browser evidence file is supplied.
 *
 * Does NOT set FULL_HE_E2E_VALIDATED=YES.
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const HOTEL_ID = process.env.HE_ACCEPTANCE_HOTEL_ID || "recLuxvwwxID7U2B8";
const BASE = process.env.DEALALITY_LOCAL_BASE_URL || "http://127.0.0.1:8080";
const browserEnabled = process.argv.includes("--browser");
const skipBrowser = process.argv.includes("--skip-browser") || !browserEnabled;

function runNpm(script) {
  const r = spawnSync("npm", ["run", script], {
    cwd: ROOT,
    encoding: "utf8",
    shell: true,
  });
  return {
    script,
    ok: r.status === 0,
    status: r.status,
    stdout: r.stdout || "",
    stderr: r.stderr || "",
  };
}

async function getJson(url) {
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }
  return { ok: res.ok, status: res.status, json, url };
}

function gate(yes) {
  return yes ? "YES" : "NO";
}

function browserGate(valueWhenTested) {
  if (skipBrowser) return "NOT_TESTED";
  return valueWhenTested;
}

const contactTests = [
  "test:he-contact-intelligence-v3-directory",
  "test:he-contact-intelligence-cx-v2",
  "test:he-contact-intelligence-directory-ux-v1",
  "test:he-loading-ux-toast-v1",
].map(runNpm);

const contactOk = contactTests.every((t) => t.ok);

const hciSrc = fs.readFileSync(
  path.join(ROOT, "public/js/hotel-contact-intelligence.js"),
  "utf8"
);
const radarHtml = fs.readFileSync(
  path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html"),
  "utf8"
);
const themeCssExists = fs.existsSync(
  path.join(ROOT, "public/css/hotel-explorer-theme-tokens-v1.css")
);

const contactDirectory =
  /data-hci-cx="v3"/.test(hciSrc) &&
  /hci-directory-table/.test(hciSrc) &&
  !/Recommended First Contact/.test(hciSrc);
const contactPresentation =
  /All Contacts/.test(hciSrc) &&
  /Hotel Leadership/.test(hciSrc) &&
  !/Inferred · not property-confirmed/.test(hciSrc);
const heLoadingShared =
  /HeLoadingUx/.test(hciSrc) &&
  /sectionHtml/.test(hciSrc) &&
  /he-async-loading/.test(hciSrc) &&
  !/class="hci-loading"/.test(hciSrc);
const cacheBustOk =
  /hotel-contact-intelligence\.js\?v=\d+/.test(radarHtml) &&
  /hotel-contact-intelligence\.css\?v=\d+/.test(radarHtml);

let loadingBrowser = {
  status: skipBrowser ? "NOT_TESTED" : "PENDING",
  ok: false,
};
let ownershipGraphBrowser = {
  status: skipBrowser ? "NOT_TESTED" : "PENDING",
  ok: false,
};
if (!skipBrowser) {
  const r = spawnSync(
    "npm",
    ["run", "verify:he-loading-component-browser-v1", "--", "--require-server"],
    {
      cwd: ROOT,
      encoding: "utf8",
      shell: true,
      env: {
        ...process.env,
        DEALALITY_LOCAL_ADMIN_BASE: BASE,
        HE_LOADING_VERIFY_HOTEL_ID: HOTEL_ID,
      },
    }
  );
  const reportPath = path.join(
    ROOT,
    "reports/hotel-explorer/HE-LOADING-COMPONENT-BROWSER-V1.json"
  );
  let report = null;
  try {
    report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  } catch {
    report = null;
  }
  loadingBrowser = {
    status: report?.status || (r.status === 0 ? "PASS" : "FAIL"),
    ok: r.status === 0 && report?.status === "PASS",
    exitCode: r.status,
    report,
  };

  const ocnRun = spawnSync(
    "npm",
    ["run", "verify:he-ownership-graph-layout-browser-v1", "--", "--require-server"],
    {
      cwd: ROOT,
      encoding: "utf8",
      shell: true,
      env: {
        ...process.env,
        DEALALITY_LOCAL_ADMIN_BASE: BASE,
        HE_OCN_VERIFY_HOTEL_ID: HOTEL_ID,
      },
    }
  );
  const ocnReportPath = path.join(
    ROOT,
    "reports/hotel-explorer/HE-OWNERSHIP-GRAPH-LAYOUT-BROWSER-V1.json"
  );
  let ocnReport = null;
  try {
    ocnReport = JSON.parse(fs.readFileSync(ocnReportPath, "utf8"));
  } catch {
    ocnReport = null;
  }
  ownershipGraphBrowser = {
    status: ocnReport?.status || (ocnRun.status === 0 ? "PASS" : "FAIL"),
    ok: ocnRun.status === 0 && ocnReport?.status === "PASS",
    exitCode: ocnRun.status,
    report: ocnReport,
  };
}

const ownership = await getJson(
  `${BASE}/api/golden-demo/ownership/hotel/${encodeURIComponent(HOTEL_ID)}`
);
const contacts = await getJson(
  `${BASE}/api/contact-intelligence/hotels/${HOTEL_ID}?audience=internal`
);

const ownershipPayload = ownership.json || {};
const hotel = ownershipPayload.hotel || {};
const shared = ownershipPayload.sharedHotelView || {};
const grms =
  hotel.guestRoomsMeetingSpace ||
  ownershipPayload.guestRoomsMeetingSpace ||
  {};
const guestRooms = grms.guestRooms || {};
const meetingSummary = grms.meetingSpaceSummary || {};
const rooms = Number(
  shared.rooms ||
    guestRooms.totalGuestrooms?.value ||
    hotel.rooms ||
    0
);
const yearBuilt = hotel.yearBuilt ?? hotel.year_built ?? null;
const lastRenovated = hotel.lastRenovated ?? hotel.last_renovated ?? null;
const meetingTotal = meetingSummary.totalMeetingSpaceSqFt?.value ?? null;
const meetingRooms = meetingSummary.meetingRoomCount?.value ?? null;
const ballroom = meetingSummary.largestRoom?.areaSqFt ?? null;
const guestroomMix = {
  kings: guestRooms.singleBedRooms?.value ?? null,
  doubles: guestRooms.doubleBedRooms?.value ?? null,
  suites: guestRooms.suites?.value ?? null,
  sourceOfTruth: grms.sourceOfTruth || null,
  persisted: grms.persisted === true,
};

const people = contacts.json?.package?.people || [];
const byName = Object.fromEntries(
  people.map((p) => [String(p.display_name || p.name || "").toLowerCase(), p])
);
function orgOf(name) {
  const p = byName[name.toLowerCase()];
  return p?.organization_name || p?.organization || null;
}
function channelsLeakMullen(name) {
  const p = byName[name.toLowerCase()];
  const ch = p?.channels || [];
  return ch.some((c) => /mullencap/i.test(String(c.value || "")));
}

const bethContactsOk =
  people.length >= 5 &&
  /marriott/i.test(String(orgOf("Meinrad Lang III") || "")) &&
  /marriott/i.test(String(orgOf("Jenny Pyne") || "")) &&
  /mullen/i.test(String(orgOf("Katherine Button") || "")) &&
  /mullen/i.test(String(orgOf("Patrick Reece") || "")) &&
  /mullen/i.test(String(orgOf("Jeff Mullen") || "")) &&
  !channelsLeakMullen("Meinrad Lang III") &&
  !channelsLeakMullen("Jenny Pyne");

const corpContacts =
  ownershipPayload.report?.corporate_contacts ||
  ownershipPayload.corporate_contacts ||
  {};
const headerWebsite =
  String(hotel.website || hotel.hotelWebsite || shared.website || "").trim() || null;
const headerPhone =
  String(
    hotel.telephone || hotel.phone || shared.telephone || shared.phone || corpContacts.property_phone || ""
  ).trim() || null;
const headerWebsiteOk =
  Boolean(headerWebsite) &&
  /^https?:\/\//i.test(headerWebsite) &&
  !/mullencap/i.test(headerWebsite);
const headerPhoneOk =
  Boolean(headerPhone) &&
  /301/.test(headerPhone.replace(/\D/g, "")) &&
  /8979400/.test(headerPhone.replace(/\D/g, ""));
const headerActionsHydrated = headerWebsiteOk && headerPhoneOk;

const economicClass = shared.economicOwnerClassification || null;
const propCo = shared.propertyCompany || null;

const physicalFacts = {
  rooms407: rooms === 407,
  yearBuilt1979: String(yearBuilt || "") === "1979" || Number(yearBuilt) === 1979,
  lastRenovated2015:
    String(lastRenovated || "").includes("2015") || Number(lastRenovated) === 2015,
  meetingTotal18719:
    Number(String(meetingTotal || "").replace(/,/g, "")) === 18719,
  meetingRooms27: Number(meetingRooms) === 27,
  ballroom4592: Number(String(ballroom || "").replace(/,/g, "")) === 4592,
  guestroomMix:
    guestroomMix &&
    (Number(guestroomMix.kings || guestroomMix.king) === 171 ||
      /171/.test(JSON.stringify(guestroomMix))) &&
    (Number(guestroomMix.doubles || guestroomMix.double) === 236 ||
      /236/.test(JSON.stringify(guestroomMix))) &&
    (Number(guestroomMix.suites || guestroomMix.suite) === 8 ||
      /8/.test(JSON.stringify(guestroomMix))),
};

const persistence = {
  rooms: {
    source: "Hotel Property Census (Airtable)",
    canonicalPersisted: rooms === 407 && guestroomMix.persisted === true,
    value: rooms,
  },
  yearBuilt: {
    source: "HPC Opening Date → yearBuilt DTO mapping (Airtable read-back via ownership hotel)",
    canonicalPersisted: physicalFacts.yearBuilt1979,
    value: yearBuilt,
    note: physicalFacts.yearBuilt1979
      ? "Live ownership hotel.yearBuilt=1979 from HPC mapping — not a staged overlay"
      : "Missing on Live API — not canonically persisted for product read-back",
  },
  lastRenovated: {
    source: "HPC Renovation/Conversion Date → lastRenovated DTO mapping",
    canonicalPersisted: physicalFacts.lastRenovated2015,
    value: lastRenovated,
  },
  meetingSpace: {
    source: "HPC physical-product adapter (sourceOfTruth=HOTEL_PROPERTY_CENSUS)",
    canonicalPersisted:
      physicalFacts.meetingTotal18719 &&
      physicalFacts.meetingRooms27 &&
      guestroomMix.sourceOfTruth === "HOTEL_PROPERTY_CENSUS",
    value: { meetingTotal, meetingRooms, ballroom },
  },
  guestroomMix: {
    source: "HPC physical-product schema (171/236/8 brand-materials reference on census)",
    canonicalPersisted: Boolean(physicalFacts.guestroomMix) && guestroomMix.persisted === true,
    value: guestroomMix,
  },
  ownershipClassification: {
    source: "Product overlay / shared VM (founder Proposed) vs HI graph Validated",
    canonicalPersisted: false,
    stagedOverlay: true,
    value: economicClass,
    note: "Proposed sponsorship must remain a product classification; HI Validated ≠ customer Proposed",
  },
  contacts: {
    source: "HI Org Contacts + HI Organizations (+ org website channels)",
    canonicalPersisted: bethContactsOk,
    people: people.map((p) => ({
      name: p.display_name || p.name,
      organization: p.organization_name || p.organization,
      property_relevance: p.property_relevance,
    })),
  },
};

const gates = {
  HE_ACTIVE_TAB_COUNT: gate(
    fs.existsSync(
      path.join(
        ROOT,
        "lib/hotel-intelligence/guest-sentiment/he-guest-sentiment-feature-flag-v1.js"
      )
    ) &&
      /false|0|off/i.test(String(process.env.HE_GUEST_SENTIMENT_ENABLED || "false"))
  ),
  HE_CONTACT_DIRECTORY_VALIDATED: gate(contactOk && contactDirectory),
  HE_CONTACT_ORG_MAPPING_VALIDATED: gate(contactOk && bethContactsOk),
  HE_CONTACT_CHANNEL_PROVENANCE_VALIDATED: gate(contactOk && bethContactsOk),
  HE_CONTACT_PRESENTATION_VALIDATED: gate(contactOk && contactPresentation),
  HE_LOADING_COMPONENT_CONSISTENT: gate(
    contactOk &&
      heLoadingShared &&
      (skipBrowser || loadingBrowser.ok)
  ),
  HE_LOADING_COMPONENT_BROWSER: skipBrowser
    ? "NOT_TESTED"
    : loadingBrowser.ok
      ? "PASS"
      : "FAIL",
  HE_OWNERSHIP_GRAPH_LAYOUT_BROWSER: skipBrowser
    ? "NOT_TESTED"
    : ownershipGraphBrowser.ok
      ? "PASS"
      : "FAIL",
  HE_PHYSICAL_PRODUCT_ATTRIBUTE_COVERAGE: gate(
    physicalFacts.rooms407 &&
      physicalFacts.yearBuilt1979 &&
      physicalFacts.lastRenovated2015 &&
      physicalFacts.meetingTotal18719 &&
      physicalFacts.meetingRooms27 &&
      physicalFacts.ballroom4592 &&
      Boolean(physicalFacts.guestroomMix)
  ),
  HE_OWNERSHIP_VERIFICATION_STATE_PARITY: gate(
    /proposed/i.test(String(economicClass || "")) &&
      /marbeth/i.test(String(propCo || ""))
  ),
  HE_UNIFIED_THEME_CONSISTENCY: gate(themeCssExists && cacheBustOk),
  HE_GLOBAL_BACKGROUND_CONSISTENCY: null, // filled after browser/static gate below
  HE_HEADER_ACTIONS_HYDRATED: gate(headerActionsHydrated),
  HE_CANONICAL_AIRTABLE_READBACK: gate(
    persistence.rooms.canonicalPersisted &&
      persistence.yearBuilt.canonicalPersisted &&
      persistence.lastRenovated.canonicalPersisted &&
      persistence.meetingSpace.canonicalPersisted &&
      persistence.guestroomMix.canonicalPersisted &&
      persistence.contacts.canonicalPersisted
  ),
  HE_AUTHENTICATED_LIVE_RENDERING: browserGate("NO"),
  HE_CONTACT_CX_REGRESSION: null,
  HE_OWNERSHIP_GRAPH_REGRESSION: null,
  HE_PORTFOLIO_LAYOUT_REGRESSION: null,
  HE_SUBMARKET_PRESENTATION_REGRESSION: null,
  HE_FOUNDER_UI_CONTRACT: null,
  HE_CANONICAL_BROWSER_PARITY: skipBrowser ? "NOT_TESTED" : null,
  FULL_HE_E2E_VALIDATED: "NO",
};

// Founder four-tab UI contract (static; browser parity separate / NOT_TESTED when skipped).
const founderContractRun = spawnSync(
  "npm",
  ["run", "audit:he-founder-ui-contract-v1", "--", skipBrowser ? "--skip-browser" : "--browser"],
  { cwd: ROOT, encoding: "utf8", shell: true }
);
let founderReport = null;
try {
  founderReport = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "reports/hotel-explorer/HE-FOUNDER-UI-CONTRACT-FOUR-TABS-V1.json"),
      "utf8"
    )
  );
} catch {
  founderReport = null;
}
gates.HE_CONTACT_CX_REGRESSION =
  founderReport?.gates?.HE_CONTACT_CX_REGRESSION || (founderContractRun.status === 0 ? "PASS" : "FAIL");
gates.HE_OWNERSHIP_GRAPH_REGRESSION =
  founderReport?.gates?.HE_OWNERSHIP_GRAPH_REGRESSION ||
  (founderContractRun.status === 0 ? "PASS" : "FAIL");
gates.HE_PORTFOLIO_LAYOUT_REGRESSION =
  founderReport?.gates?.HE_PORTFOLIO_LAYOUT_REGRESSION ||
  (founderContractRun.status === 0 ? "PASS" : "FAIL");
gates.HE_SUBMARKET_PRESENTATION_REGRESSION =
  founderReport?.gates?.HE_SUBMARKET_PRESENTATION_REGRESSION ||
  (founderContractRun.status === 0 ? "PASS" : "FAIL");
gates.HE_FOUNDER_UI_CONTRACT =
  founderReport?.gates?.HE_FOUNDER_UI_CONTRACT ||
  (founderContractRun.status === 0 ? "PASS" : "FAIL");
if (!skipBrowser) {
  gates.HE_CANONICAL_BROWSER_PARITY =
    founderReport?.gates?.HE_CANONICAL_BROWSER_PARITY || "FAIL";
}

// Permanent global background consistency (static always; browser when enabled).
const bgGateArgs = skipBrowser
  ? ["run", "test:he-global-background-consistency-v1"]
  : ["run", "test:he-global-background-consistency-v1", "--", "--require-browser"];
const bgGateRun = spawnSync("npm", bgGateArgs, {
  cwd: ROOT,
  encoding: "utf8",
  shell: true,
  env: { ...process.env, DEALALITY_LOCAL_ADMIN_BASE: BASE },
});
let bgGateReport = null;
try {
  bgGateReport = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "reports/hotel-explorer/HE-GLOBAL-BACKGROUND-CONSISTENCY-V1.json"),
      "utf8"
    )
  );
} catch {
  bgGateReport = null;
}
if (skipBrowser) {
  gates.HE_GLOBAL_BACKGROUND_CONSISTENCY =
    bgGateRun.status === 0 || bgGateRun.status === 2
      ? bgGateReport?.static?.forbiddenActiveBgCleared
        ? "STATIC_PASS"
        : "FAIL"
      : "FAIL";
} else {
  gates.HE_GLOBAL_BACKGROUND_CONSISTENCY =
    bgGateReport?.gates?.HE_GLOBAL_BACKGROUND_CONSISTENCY ||
    (bgGateRun.status === 0 ? "PASS" : "FAIL");
  if (bgGateReport?.gates?.HE_ELEVEN_TAB_THEME_PARITY) {
    gates.HE_ELEVEN_TAB_THEME_PARITY = bgGateReport.gates.HE_ELEVEN_TAB_THEME_PARITY;
  }
  if (bgGateReport?.gates?.HE_NO_UNINTENDED_BACKGROUND_SEAMS) {
    gates.HE_NO_UNINTENDED_BACKGROUND_SEAMS =
      bgGateReport.gates.HE_NO_UNINTENDED_BACKGROUND_SEAMS;
  }
  if (bgGateReport?.gates?.HE_SCROLL_VISUAL_STABILITY) {
    gates.HE_SCROLL_VISUAL_STABILITY = bgGateReport.gates.HE_SCROLL_VISUAL_STABILITY;
  }
  if (bgGateReport?.gates?.HE_CANONICAL_BROWSER_VALIDATED) {
    gates.HE_CANONICAL_BROWSER_VALIDATED =
      bgGateReport.gates.HE_CANONICAL_BROWSER_VALIDATED;
  }
}

const ok =
  gates.HE_CONTACT_DIRECTORY_VALIDATED === "YES" &&
  gates.HE_CONTACT_ORG_MAPPING_VALIDATED === "YES" &&
  gates.HE_CONTACT_PRESENTATION_VALIDATED === "YES" &&
  gates.HE_LOADING_COMPONENT_CONSISTENT === "YES" &&
  gates.HE_HEADER_ACTIONS_HYDRATED === "YES" &&
  gates.HE_CONTACT_CX_REGRESSION === "PASS" &&
  gates.HE_OWNERSHIP_GRAPH_REGRESSION === "PASS" &&
  gates.HE_PORTFOLIO_LAYOUT_REGRESSION === "PASS" &&
  gates.HE_SUBMARKET_PRESENTATION_REGRESSION === "PASS" &&
  gates.HE_FOUNDER_UI_CONTRACT === "PASS" &&
  (skipBrowser ||
    (ownershipGraphBrowser.ok && gates.HE_CANONICAL_BROWSER_PARITY === "PASS")) &&
  gates.FULL_HE_E2E_VALIDATED === "NO";

const outDir = path.join(ROOT, "reports/hotel-explorer");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, "HE50-RELEASE-ACCEPTANCE-GATES-V1.json");
const report = {
  version: "he50-release-acceptance-gates-v1",
  hotelId: HOTEL_ID,
  base: BASE,
  headHint: process.env.GIT_HEAD || null,
  browserEnabled: !skipBrowser,
  loadingBrowser,
  ownershipGraphBrowser,
  ok,
  gates,
  contactTests: contactTests.map((t) => ({
    script: t.script,
    ok: t.ok,
    status: t.status,
  })),
  physicalFacts,
  persistence,
  liveProbes: {
    ownershipStatus: ownership.status,
    contactsStatus: contacts.status,
    rooms,
    yearBuilt,
    lastRenovated,
    meetingTotal,
    meetingRooms,
    ballroom,
    guestroomMix,
    economicClass,
    propCo,
    contactCount: people.length,
    headerWebsite,
    headerPhone,
    headerActionsHydrated,
  },
  generatedAt: new Date().toISOString(),
};
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ ok, outPath, gates }, null, 2));
process.exit(ok ? 0 : 1);
