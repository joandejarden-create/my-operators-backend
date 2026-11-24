#!/usr/bin/env node
/**
 * Forensic historical-vs-current GDI inventory reconciliation ($0).
 * Does not write STAGED/CURRENT, delete packages, or spend providers.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import { filterPackageOpportunitiesToFqo } from "../lib/group-demand-intelligence/publication-package/gdi-founder-fqo-presentation-filter-v1.js";
import {
  filterLegacyApprovedPackageOpportunities,
  classifyLegacyRowAgainstNewFqo,
} from "../lib/group-demand-intelligence/publication-package/gdi-legacy-approved-inventory-compat-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-historical-opportunity-recovery-v1",
);

const FOCUS = [
  {
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    expectedLkg: 67,
  },
  {
    hotelId: "rec35fExUxCClpOP6",
    hotelName: "Hilton New York Times Square",
    expectedLkgResearch: 45,
  },
  {
    hotelId: "recH9h3McgMNFQEla",
    hotelName: "Radisson Blu Toronto Downtown",
    expectedLkg: 66,
  },
  {
    hotelId: "recrPQcZg7SFARRb2",
    hotelName: "YOTEL Geneva Lake",
    expectedLkg: 42,
  },
  {
    hotelId: "recXh4NgF8sDbi9cQ",
    hotelName: "Wyndham Alltra Punta Cana",
    expectedLkg: 27,
  },
  {
    hotelId: "recnPB3Q02NO8Ma1o",
    hotelName: "Wyndham San Diego Bayside",
  },
  {
    hotelId: "recgMYovrrZDJMqzX",
    hotelName: "Waterstone Resort & Marina Boca Raton",
  },
];

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function readResearch(hotelId) {
  const file = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(file)) return { ok: false, count: 0, opportunities: [], path: file };
  try {
    const doc = JSON.parse(fs.readFileSync(file, "utf8"));
    const opps = Array.isArray(doc.opportunities)
      ? doc.opportunities
      : Array.isArray(doc)
        ? doc
        : [];
    return { ok: true, count: opps.length, opportunities: opps, path: file };
  } catch (e) {
    return { ok: false, count: 0, opportunities: [], path: file, error: String(e) };
  }
}

function channelOf(o) {
  const pc = o?.primaryContact || {};
  return {
    name: text(pc.name),
    email: text(pc.email) || null,
    phone: text(pc.phone || pc.directPhone) || null,
    linkedin: text(pc.linkedinUrl || pc.linkedin || pc.profileUrl) || null,
  };
}

function reconcileHotel(store, hotel) {
  const stagedPtr = store.readPointer(hotel.hotelId, "STAGED");
  const currentPtr = store.readPointer(hotel.hotelId, "CURRENT");
  const research = readResearch(hotel.hotelId);
  const load = (ver) => {
    if (!ver) return null;
    const loaded = store.loadPackage(hotel.hotelId, ver);
    return loaded?.ok ? loaded.package : null;
  };
  const stagedPkg = load(stagedPtr?.publicationVersion);
  const currentPkg = load(currentPtr?.publicationVersion);
  const lkgPkg = stagedPkg || currentPkg;
  const lkgSource = stagedPkg
    ? "STAGED"
    : currentPkg
      ? "CURRENT"
      : research.ok
        ? "RESEARCH_STORE"
        : "NONE";
  const lkgOpps = lkgPkg?.opportunities || (lkgSource === "RESEARCH_STORE" ? research.opportunities : []);
  const lkgCount = lkgOpps.length;

  const fqo = filterPackageOpportunitiesToFqo(lkgOpps);
  const legacy = filterLegacyApprovedPackageOpportunities(lkgOpps);

  const suppressions = [];
  for (const o of lkgOpps) {
    const cls = classifyLegacyRowAgainstNewFqo(o);
    if (cls.classification === "STILL_VISIBLE_UNDER_NEW_FQO") continue;
    const ch = channelOf(o);
    suppressions.push({
      opportunityId: text(o.id || o.opportunityId),
      title: text(o.title || o.displayTitle),
      previouslyDisplayedContact: ch.name || null,
      previouslyDisplayedChannel:
        ch.email || ch.phone || ch.linkedin || null,
      channelDetail: ch,
      classification: cls.classification,
      rejectionReasons: cls.reasons,
      stillKeptUnderLegacyCompat: legacy.opportunities.some(
        (k) => text(k.id) === text(o.id || o.opportunityId),
      ),
    });
  }

  const hotelDir = path.join(
    store.root,
    hotel.hotelId,
  );
  const versions = fs.existsSync(hotelDir)
    ? fs
        .readdirSync(hotelDir)
        .filter((n) =>
          fs.existsSync(path.join(hotelDir, n, "package.json")),
        )
    : [];

  return {
    hotelId: hotel.hotelId,
    hotelName: hotel.hotelName,
    lastKnownGood: {
      source: lkgSource,
      stagedVersion: stagedPtr?.publicationVersion || null,
      currentVersion: currentPtr?.publicationVersion || null,
      count: lkgCount,
      namedContacts: lkgOpps.filter((o) => channelOf(o).name).length,
      withAnyChannel: lkgOpps.filter(
        (o) => channelOf(o).email || channelOf(o).phone || channelOf(o).linkedin,
      ).length,
    },
    researchStore: {
      count: research.count,
      path: research.path,
      ok: research.ok,
    },
    packageVersionsOnDisk: versions,
    runtimeStrictFqo: {
      stillVisible: fqo.afterCount,
      filteredByNewFqo: fqo.droppedCount,
    },
    runtimeLegacyCompat: {
      stillVisible: legacy.afterCount,
      softRemoved: legacy.droppedCount,
      removed: legacy.removed,
    },
    expectedLkg: hotel.expectedLkg || hotel.expectedLkgResearch || null,
    suppressions,
    classificationCounts: suppressions.reduce((acc, s) => {
      acc[s.classification] = (acc[s.classification] || 0) + 1;
      return acc;
    }, {}),
    rootCauseHint:
      lkgSource === "STAGED" || lkgSource === "CURRENT"
        ? "DATA_HIDING_VIA_RUNTIME_FQO_ON_LEGACY_APPROVED_PACKAGE"
        : lkgSource === "RESEARCH_STORE"
          ? "RESEARCH_VIEW_REMOVAL_NO_STAGED_PACKAGE"
          : "NO_LKG_PACKAGE_OR_RESEARCH",
  };
}

fs.mkdirSync(OUT, { recursive: true });
const store = createGdiPublicationPackageStore();
const hotels = FOCUS.map((h) => reconcileHotel(store, h));
const matrix = {
  contract: "gdi-historical-opportunity-recovery-reconcile-v1",
  at: new Date().toISOString(),
  firstBreakingCommitForRuntimeFqoWipe:
    "284c7c795 (fix(gdi): founder localhost serves FQO-only inventory)",
  priorProductChange:
    "#593/#594 removed Research View; STAGED/CURRENT became founder default",
  freeze: {
    noStagedActivation: true,
    noCurrentOverwrite: true,
    noPackageDeletion: true,
    noPaidResearch: true,
  },
  hotels,
  totals: {
    hotels: hotels.length,
    lkgOpportunities: hotels.reduce((n, h) => n + h.lastKnownGood.count, 0),
    strictFqoVisible: hotels.reduce(
      (n, h) => n + h.runtimeStrictFqo.stillVisible,
      0,
    ),
    legacyCompatVisible: hotels.reduce(
      (n, h) => n + h.runtimeLegacyCompat.stillVisible,
      0,
    ),
  },
};

fs.writeFileSync(
  path.join(OUT, "PORTFOLIO_RECONCILIATION_MATRIX.json"),
  `${JSON.stringify(matrix, null, 2)}\n`,
);
for (const h of hotels) {
  fs.writeFileSync(
    path.join(OUT, `${h.hotelId}.suppressions.json`),
    `${JSON.stringify(h, null, 2)}\n`,
  );
}

console.log(
  JSON.stringify(
    {
      ok: true,
      outDir: OUT,
      totals: matrix.totals,
      summary: hotels.map((h) => ({
        hotel: h.hotelName,
        lkg: h.lastKnownGood.count,
        source: h.lastKnownGood.source,
        strictFqo: h.runtimeStrictFqo.stillVisible,
        legacyCompat: h.runtimeLegacyCompat.stillVisible,
        rootCause: h.rootCauseHint,
      })),
    },
    null,
    2,
  ),
);
