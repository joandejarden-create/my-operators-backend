#!/usr/bin/env node
/**
 * Portfolio audit — Finalized Qualified Opportunity impact matrix.
 * Read-only. No pointer writes. No paid research.
 *
 *   node scripts/gdi-finalized-qualified-opportunity-portfolio-audit-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateFinalizedQualifiedOpportunity,
  evaluatePersonSpecificOutreachChannel,
} from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT = path.join(
  ROOT,
  "reports/gdi-finalized-qualified-opportunity-v1",
);
const HOTEL_NAMES = {
  rec35fExUxCClpOP6: "Hilton New York Times Square",
  rece0NdV9FCnaFlVS: "Condesa DF",
  recaokE8psIbuVeuH: "LaLuna Cancún",
  rec1t35e3sPerzxYu: "Memmo Príncipe Real",
  recLuxvwwxID7U2B8: "Bethesda Marriott",
  recH9h3McgMNFQEla: "Waterstone / related",
  recrPQcZg7SFARRb2: "YOTEL Geneva Lake",
  recXh4NgF8sDbi9cQ: "Wyndham Alltra",
  recCAGqkERSXgiku4: "Package hotel recCAGqkERSXgiku4",
};

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function writeJson(file, body) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(body, null, 2)}\n`, "utf8");
}
function text(v) {
  return String(v == null ? "" : v).trim();
}

function loadResearch(hotelId) {
  const file = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(file)) return [];
  try {
    const doc = JSON.parse(fs.readFileSync(file, "utf8"));
    return Array.isArray(doc.opportunities)
      ? doc.opportunities
      : Array.isArray(doc)
        ? doc
        : [];
  } catch {
    return [];
  }
}

function packageOpps(store, hotelId, kind) {
  const ptr = store.readPointer(hotelId, kind);
  if (!ptr?.publicationVersion) return { pointer: null, opportunities: [] };
  const loaded = store.loadPackage(hotelId, ptr.publicationVersion);
  if (!loaded?.ok) {
    return { pointer: ptr, opportunities: [], error: loaded?.error || null };
  }
  const pkg = loaded.package;
  return {
    pointer: ptr,
    opportunities: pkg.opportunities || pkg.payload?.opportunities || [],
  };
}

function classifyResearch(opps) {
  let demandQualified = 0;
  let namedPeople = 0;
  let personChannels = 0;
  let fqo = 0;
  const blockers = {};
  for (const o of opps) {
    if (o.disqualified === true || /DISQUALIFIED/i.test(String(o.status || ""))) {
      continue;
    }
    const demand =
      text(o.organizationName || o.title) &&
      (Array.isArray(o.sourceUrls) ? o.sourceUrls.length > 0 : false);
    if (demand) demandQualified += 1;
    const name = text(o.primaryContact?.name || o.primaryContactName);
    if (looksLikePersonName(name)) namedPeople += 1;
    const ch = evaluatePersonSpecificOutreachChannel(o);
    if (ch.ok) personChannels += 1;
    const evaled = evaluateFinalizedQualifiedOpportunity(o);
    if (evaled.finalizedQualified) fqo += 1;
    else {
      for (const r of evaled.reasons || ["UNKNOWN"]) {
        blockers[r] = (blockers[r] || 0) + 1;
      }
    }
  }
  return {
    researchInventory: opps.length,
    demandQualifiedApprox: demandQualified,
    namedCommerciallyRelevantPeople: namedPeople,
    personSpecificChannels: personChannels,
    finalizedQualifiedCount: fqo,
    topBlockers: Object.entries(blockers)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8)
      .map(([reason, count]) => ({ reason, count })),
  };
}

function main() {
  const store = createGdiPublicationPackageStore();
  const researchRoot = path.join(ROOT, "data/group-demand-intelligence/hotels");
  const researchHotels = fs.existsSync(researchRoot)
    ? fs
        .readdirSync(researchRoot, { withFileTypes: true })
        .filter((d) => d.isDirectory())
        .map((d) => d.name)
    : [];
  const pkgHotels = fs.existsSync(store.root)
    ? fs
        .readdirSync(store.root, { withFileTypes: true })
        .filter((d) => d.isDirectory())
        .map((d) => d.name)
    : [];
  const hotelIds = [...new Set([...researchHotels, ...pkgHotels])].sort();

  const focus = [
    "rec35fExUxCClpOP6",
    ...pkgHotels.filter((id) => id !== "rec35fExUxCClpOP6"),
  ];

  const matrix = [];
  for (const hotelId of hotelIds) {
    const research = loadResearch(hotelId);
    const researchStats = classifyResearch(research);
    const staged = packageOpps(store, hotelId, "STAGED");
    const current = packageOpps(store, hotelId, "CURRENT");
    const stagedFqo = staged.opportunities.filter(
      (o) => evaluateFinalizedQualifiedOpportunity(o).finalizedQualified,
    ).length;
    const currentFqo = current.opportunities.filter(
      (o) => evaluateFinalizedQualifiedOpportunity(o).finalizedQualified,
    ).length;
    matrix.push({
      hotelId,
      hotelName: HOTEL_NAMES[hotelId] || hotelId,
      focus: focus.includes(hotelId),
      researchInventory: researchStats.researchInventory,
      demandQualifiedApprox: researchStats.demandQualifiedApprox,
      namedCommerciallyRelevantPeople: researchStats.namedCommerciallyRelevantPeople,
      personSpecificChannels: researchStats.personSpecificChannels,
      finalizedQualifiedFromResearch: researchStats.finalizedQualifiedCount,
      existingStagedCount: staged.opportunities.length,
      existingStagedFqoPassCount: stagedFqo,
      existingStagedVersion: staged.pointer?.publicationVersion || null,
      existingCurrentCount: current.opportunities.length,
      existingCurrentFqoPassCount: currentFqo,
      existingCurrentVersion: current.pointer?.publicationVersion || null,
      remainingBlockers: researchStats.topBlockers,
      notes:
        hotelId === "rec35fExUxCClpOP6"
          ? "Hilton: no STAGED pointer; paid research candidate package may exist on parallel PR #592 — not activated"
          : !staged.pointer
            ? "No STAGED pointer — local presentation shows empty finalized inventory"
            : "Historical STAGED preserved; FQO not retroactively stripping pointer inventory",
    });
  }

  const missingStaged = matrix.filter((r) => !r.existingStagedVersion);
  const audit = {
    contract: "gdi-finalized-qualified-opportunity-portfolio-audit-v1",
    asOf: "2026-11-24",
    hotelCount: matrix.length,
    hotelsWithStaged: matrix.filter((r) => r.existingStagedVersion).length,
    hotelsWithCurrent: matrix.filter((r) => r.existingCurrentVersion).length,
    hotelsMissingStaged: missingStaged.map((r) => ({
      hotelId: r.hotelId,
      hotelName: r.hotelName,
      researchInventory: r.researchInventory,
      finalizedQualifiedFromResearch: r.finalizedQualifiedFromResearch,
    })),
    hilton: matrix.find((r) => r.hotelId === "rec35fExUxCClpOP6") || null,
    matrix,
    preservation: {
      noStagedPointerWrites: true,
      noCurrentPointerWrites: true,
      noAirtable: true,
      historicalPackagesUntouched: true,
    },
  };

  writeJson(path.join(REPORT, "03_PORTFOLIO_IMPACT_MATRIX.json"), audit);
  writeJson(path.join(REPORT, "08_STAGED_CURRENT_PRESERVATION.json"), {
    ok: true,
    stagedPointers: matrix
      .filter((r) => r.existingStagedVersion)
      .map((r) => ({
        hotelId: r.hotelId,
        publicationVersion: r.existingStagedVersion,
        count: r.existingStagedCount,
      })),
    currentPointers: matrix
      .filter((r) => r.existingCurrentVersion)
      .map((r) => ({
        hotelId: r.hotelId,
        publicationVersion: r.existingCurrentVersion,
        count: r.existingCurrentCount,
      })),
    hiltonStagedActivated: false,
  });

  console.log(
    JSON.stringify(
      {
        hotels: matrix.length,
        missingStaged: missingStaged.length,
        hiltonFqoFromResearch:
          audit.hilton?.finalizedQualifiedFromResearch ?? null,
        hiltonStaged: audit.hilton?.existingStagedCount ?? 0,
      },
      null,
      2,
    ),
  );
}

main();
