#!/usr/bin/env node
/**
 * Read-only product-wide GDI hotel readiness audit.
 * NO_PAID_RESEARCH · NO_AIRTABLE · NO_STAGED_ACTIVATE · NO_CURRENT_PUBLISH
 *
 *   node scripts/gdi-product-wide-hotel-readiness-audit-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import {
  GDI_CANONICAL_PUBLICATION_LIFECYCLE,
  GDI_OPERATIONAL_STATUS,
} from "../lib/group-demand-intelligence/publication-package/gdi-canonical-e2e-to-staged-lifecycle-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-product-wide-hotel-readiness-audit-v1",
);
fs.mkdirSync(OUT, { recursive: true });

const HOTELS = [
  ["rec2PVBDavppGpenm", "AC Hotel A Coruña"],
  ["recLuxvwwxID7U2B8", "Bethesda Marriott"],
  ["recIwaP1etgx2g9nA", "Cambridge Beaches Resort & Spa"],
  ["rec82D9zpB8fede1I", "Castillo Hotel Son Vida"],
  ["rece0NdV9FCnaFlVS", "Condesa DF Hotel"],
  ["recRkGGNKslJptFIL", "Falls Church Marriott Fairview Park"],
  ["rec35fExUxCClpOP6", "Hilton New York Times Square"],
  ["recaokE8psIbuVeuH", "LaLuna Boutique Hotels & Villas"],
  ["rec1t35e3sPerzxYu", "Memmo Príncipe Real"],
  ["recC2kbCaS48jFjFF", "Nobu Hotel London Portman Square"],
  ["recGkME49yYuxQl0u", "NOW NOW NOHO"],
  ["recH9h3McgMNFQEla", "Radisson Blu Toronto Downtown"],
  ["recUOyzOXn2Zdp98I", "Radisson Hotel Santo Domingo"],
  ["recG66DQJKP2c0UNh", "Renaissance New York Times Square Hotel"],
  ["recCAGqkERSXgiku4", "Sheraton Guadalajara Expo"],
  ["recjhQdAUiSyxfCqE", "Sheraton Mallorca Arabella Golf Hotel"],
  ["recKRJjcPnb4tVDDS", "Spice Island Beach Resort"],
  ["rec0eTEnCqlsGJJt9", "The Mayflower Hotel, Autograph Collection"],
  ["recFaxTEFF9ILHWC9", "The Westin Grand München"],
  ["recdmo0CnhIZAwHGC", "The Westin Tampa Waterside"],
  ["rece0or38cxo3Fymb", "W Rome"],
  ["recgMYovrrZDJMqzX", "Waterstone Resort & Marina Boca Raton"],
  ["recXh4NgF8sDbi9cQ", "Wyndham Alltra Punta Cana"],
  ["recnPB3Q02NO8Ma1o", "Wyndham San Diego Bayside"],
  ["recrPQcZg7SFARRb2", "YOTEL Geneva Lake"],
];

const PILOT_FOCUS = new Set([
  "recLuxvwwxID7U2B8",
  "recnPB3Q02NO8Ma1o",
  "recrPQcZg7SFARRb2",
  "recG66DQJKP2c0UNh",
  "recCAGqkERSXgiku4",
]);

const store = createGdiPublicationPackageStore({});
const rows = [];

for (const [id, name] of HOTELS) {
  const stagedPtr = store.readPointer(id, "STAGED");
  const currentPtr = store.readPointer(id, "CURRENT");
  let stagedCount = null;
  let currentCount = null;
  let stagedLoadOk = null;
  let currentLoadOk = null;
  if (stagedPtr?.publicationVersion) {
    const loaded = store.loadPackage(id, stagedPtr.publicationVersion);
    stagedLoadOk = loaded.ok === true;
    stagedCount = loaded.ok
      ? loaded.package.opportunityCount ||
        (loaded.package.opportunities || []).length
      : null;
  }
  if (currentPtr?.publicationVersion && currentPtr.withheld !== true) {
    const loaded = store.loadPackage(id, currentPtr.publicationVersion);
    currentLoadOk = loaded.ok === true;
    currentCount = loaded.ok
      ? loaded.package.opportunityCount ||
        (loaded.package.opportunities || []).length
      : null;
  }

  let readiness = "NO_PUBLICATION";
  let operationalStatus = GDI_OPERATIONAL_STATUS.RESEARCH_IN_PROGRESS;
  if (currentPtr?.publicationVersion && currentPtr.withheld !== true) {
    readiness = "CURRENT";
    operationalStatus = GDI_OPERATIONAL_STATUS.CURRENT;
  } else if (stagedPtr?.publicationVersion) {
    readiness = "STAGED_AWAITING_FOUNDER_APPROVAL";
    operationalStatus = GDI_OPERATIONAL_STATUS.STAGED;
  }

  const notes = [];
  if (stagedPtr && stagedLoadOk === false) {
    notes.push("STAGED_POINTER_PACKAGE_LOAD_FAIL");
  }
  if (currentPtr && currentLoadOk === false) {
    notes.push("CURRENT_POINTER_PACKAGE_LOAD_FAIL");
  }
  if (
    stagedCount != null &&
    currentCount != null &&
    Number(stagedCount) !== Number(currentCount)
  ) {
    notes.push("STAGED_CURRENT_COUNT_DIVERGENCE");
  }
  if (id === "recnPB3Q02NO8Ma1o" && stagedCount === 26) {
    notes.push("WYNDHAM_LOCKED_26_STAGED_OK");
  }
  if (id === "recLuxvwwxID7U2B8" && stagedCount === 68) {
    notes.push("BETHESDA_STAGED_68_IS_HISTORICAL_68_SOURCE");
  }
  if (!stagedPtr && !currentPtr) {
    notes.push("MISSING_PUBLICATION_POINTERS");
  }

  rows.push({
    hotelId: id,
    hotelName: name,
    pilotFocus: PILOT_FOCUS.has(id),
    readiness,
    operationalStatus,
    stagedVersion: stagedPtr?.publicationVersion || null,
    stagedCount,
    currentVersion: currentPtr?.publicationVersion || null,
    currentCount,
    currentWithheld: currentPtr?.withheld === true,
    notes,
  });
}

const remediation = rows
  .filter(
    (r) =>
      r.readiness === "NO_PUBLICATION" ||
      r.notes.includes("STAGED_POINTER_PACKAGE_LOAD_FAIL") ||
      r.notes.includes("CURRENT_POINTER_PACKAGE_LOAD_FAIL") ||
      (r.pilotFocus && r.readiness === "NO_PUBLICATION"),
  )
  .map((r) => ({
    hotelId: r.hotelId,
    hotelName: r.hotelName,
    pilotFocus: r.pilotFocus,
    action:
      r.readiness === "NO_PUBLICATION"
        ? "AUTHORIZE_FULL_E2E_THEN_AUTO_STAGE"
        : "REPAIR_PUBLICATION_POINTER_OR_PACKAGE",
    freezeCheckRequired: true,
    autoMutate: false,
  }));

const report = {
  contract: "gdi-product-wide-hotel-readiness-audit-v1",
  at: new Date().toISOString(),
  lifecycle: GDI_CANONICAL_PUBLICATION_LIFECYCLE,
  mode: "READ_ONLY",
  hotelCount: rows.length,
  summary: {
    current: rows.filter((r) => r.readiness === "CURRENT").length,
    stagedAwaitingApproval: rows.filter(
      (r) => r.readiness === "STAGED_AWAITING_FOUNDER_APPROVAL",
    ).length,
    noPublication: rows.filter((r) => r.readiness === "NO_PUBLICATION").length,
  },
  pilotFocus: rows.filter((r) => r.pilotFocus),
  hotels: rows,
  remediationManifest: remediation,
  governance: {
    noPaidResearch: true,
    noAirtableWrites: true,
    noStagedActivate: true,
    noCurrentPublish: true,
    freezesHonored: true,
    note: "Remediation requires separate founder authorization per hotel; this audit does not mutate.",
  },
};

fs.writeFileSync(
  path.join(OUT, "00-HOTEL_READINESS_AUDIT.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "01-REMEDIATION_MANIFEST.json"),
  `${JSON.stringify({ at: report.at, items: remediation }, null, 2)}\n`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      outDir: OUT,
      summary: report.summary,
      remediationCount: remediation.length,
      wyndham: rows.find((r) => r.hotelId === "recnPB3Q02NO8Ma1o"),
      bethesda: rows.find((r) => r.hotelId === "recLuxvwwxID7U2B8"),
    },
    null,
    2,
  ),
);
