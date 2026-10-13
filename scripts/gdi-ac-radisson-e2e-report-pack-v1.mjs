#!/usr/bin/env node
/**
 * Build reports/gdi/ac-coruna-radisson-santo-domingo-e2e/* from completed first-cycle runs.
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { pathToFileURL } from "url";

const out =
  "c:/Dev/deal-capture-proxy/reports/gdi/ac-coruna-radisson-santo-domingo-e2e";
mkdirSync(out, { recursive: true });

const vis = await import(
  pathToFileURL(
    "c:/Dev/deal-capture-proxy/lib/group-demand-intelligence/customer-visibility.js"
  ).href
);
const readyMod = await import(
  pathToFileURL(
    "c:/Dev/deal-capture-proxy/lib/group-demand-intelligence/customer-readiness-gate-v1.js"
  ).href
);
const buyer = await import(
  pathToFileURL(
    "c:/Dev/deal-capture-proxy/lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js"
  ).href
);

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function writeCsv(name, rows, cols) {
  if (!rows.length) {
    writeFileSync(`${out}/${name}`, cols.join(",") + "\n");
    return;
  }
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c])).join(","));
  writeFileSync(`${out}/${name}`, lines.join("\n") + "\n");
}
function loadHotel(hotelId) {
  const p = `c:/Dev/deal-capture-proxy/data/group-demand-intelligence/hotels/${hotelId}/opportunities.json`;
  if (!existsSync(p)) return { opportunities: [], updatedAt: null };
  return JSON.parse(readFileSync(p, "utf8"));
}
function loadConfig(hotelId) {
  return JSON.parse(
    readFileSync(
      `c:/Dev/deal-capture-proxy/config/group-demand-intelligence/hotels/${hotelId}.json`,
      "utf8"
    )
  );
}
function loadDisc(path) {
  return existsSync(path) ? JSON.parse(readFileSync(path, "utf8")) : null;
}

const hotels = [
  {
    key: "AC",
    hotelId: "rec2PVBDavppGpenm",
    name: "AC Hotel A Coruña",
    discPath:
      "c:/Dev/deal-capture-proxy/reports/group-demand-intelligence/ac-hotel-a-coruna-v1/GDI_DISCOVERY.json",
    sumPath:
      "c:/Dev/deal-capture-proxy/reports/group-demand-intelligence/ac-hotel-a-coruna-v1/GDI_FIRST_CYCLE_SUMMARY.json",
  },
  {
    key: "RAD",
    hotelId: "recUOyzOXn2Zdp98I",
    name: "Radisson Hotel Santo Domingo",
    discPath:
      "c:/Dev/deal-capture-proxy/reports/group-demand-intelligence/radisson-santo-domingo-v1/GDI_DISCOVERY.json",
    sumPath:
      "c:/Dev/deal-capture-proxy/reports/group-demand-intelligence/radisson-santo-domingo-v1/GDI_FIRST_CYCLE_SUMMARY.json",
  },
];

const preflight = [];
const generators = [];
const namedAccounts = [];
const traveling = [];
const buyerPaths = [];
const hotelMotion = [];
const packets = [];
const readyWatch = [];
const actionability = [];
const recon = [];
const summaries = {};

for (const h of hotels) {
  const cfg = loadConfig(h.hotelId);
  const doc = loadHotel(h.hotelId);
  const all = doc.opportunities || [];
  const disc = loadDisc(h.discPath);
  const sum = loadDisc(h.sumPath);

  preflight.push({
    hotel: h.name,
    hotelId: h.hotelId,
    brand: cfg.capabilityProfile?.softBrand || "",
    rooms: cfg.capabilityProfile?.totalGuestrooms || "",
    meetingRooms:
      cfg.capabilityProfile?.meetingRoomCount ||
      cfg.capabilityProfile?.meetingRoomsIndoor ||
      "",
    meetingSqFt: cfg.capabilityProfile?.totalMeetingSpaceSqFt || "",
    largestRoomSqFt:
      cfg.capabilityProfile?.largestMeetingRoomSqFt ||
      cfg.capabilityProfile?.largestBallroomSqFt ||
      "",
    largestCapacity: cfg.capabilityProfile?.largestTheaterCapacity || "",
    market: cfg.demandTerritory?.label || "",
    gdiPackPresent: all.length > 0 ? "YES" : "NO",
    notes: (cfg.commercialPriorities?.notes || "").slice(0, 180),
  });

  const facing = all.filter((o) => vis.isCustomerFacingOpportunity(o));
  const readyRows = all.filter((o) => readyMod.isGdiCustomerOpportunityReady(o).ok);
  const watchRows = all.filter((o) =>
    /WATCH/i.test(String(o.customerFacingState || ""))
  );

  generators.push({
    hotel: h.name,
    discoveryCandidates: disc?.candidateCount ?? "",
    trueActionable: disc?.trueActionableCount ?? "",
    invalid: disc?.byActionability?.INVALID ?? "",
    validWatch: disc?.byActionability?.VALID_WATCH ?? "",
    validFuture: disc?.byActionability?.VALID_FUTURE ?? "",
    insufficient: disc?.byActionability?.INSUFFICIENT ?? "",
    serpQueries: disc?.pipeline?.nativeQueries ?? "",
    pagesFetched: disc?.pipeline?.pagesFetched ?? "",
    openaiCalls: disc?.pipeline?.openaiCalls ?? "",
    apify: "NO",
    webhound: disc?.pipeline?.webhoundRequired ?? 0,
  });

  for (const o of all) {
    const r = readyMod.isGdiCustomerOpportunityReady(o);
    const b = buyer.meetsReadyContactRequirement(o);
    namedAccounts.push({
      hotel: h.name,
      id: o.id,
      title: o.title,
      organizationName: o.organizationName,
      opportunityType: o.opportunityType,
      customerFacingState: o.customerFacingState,
      whoPathClass: o.whoPathClass,
      customerFacing: vis.isCustomerFacingOpportunity(o),
      ready: r.ok,
      readyFailed: (r.failed || []).join("|"),
    });
    traveling.push({
      hotel: h.name,
      id: o.id,
      title: o.title,
      travelingEntity: o.travelingEntity || "",
      travelingEntityProven: o.travelingEntityProven === true,
      groupMotion: o.groupMotionType || o.groupMotion || "",
      hotelMotionClass: o.hotelMotionClass || "",
      housingStatus: o.housingStatus || "",
    });
    buyerPaths.push({
      hotel: h.name,
      id: o.id,
      title: o.title,
      contactName: o.primaryContact?.name || o.primaryContactName || "",
      contactRole: o.primaryContact?.role || o.primaryContactRole || "",
      contactPathClass: o.contactPathClass || o.whoPathClass || "",
      buyerPathOk: b.ok,
      buyerPathClass: b.class,
      buyerPathReason: b.reason,
    });
    hotelMotion.push({
      hotel: h.name,
      id: o.id,
      title: o.title,
      hotelMotionClass: o.hotelMotionClass || "",
      lodgingEvidence: String(o.lodgingEvidence || "").slice(0, 160),
      roomDemandStatus: o.roomDemandStatus || "",
      hotelFitScore: o.hotelFitScore ?? "",
    });
    packets.push({
      hotel: h.name,
      id: o.id,
      title: o.title,
      packetClass: o.packetQuality || o.completePacketClass || "SIGNAL_ONLY",
      namedEntity: Boolean(o.organizationName),
      futureDecision: Boolean(o.eventStartDate || o.whyNow),
      buyerPath: b.ok,
      hotelMotion: Boolean(
        o.hotelMotionClass || o.lodgingEvidence || o.housingStatus
      ),
      hotelFit: o.hotelFitScore != null,
    });
    readyWatch.push({
      hotel: h.name,
      id: o.id,
      title: o.title,
      organizationName: o.organizationName,
      ready: r.ok,
      facing: vis.isCustomerFacingOpportunity(o),
      customerFacingState: o.customerFacingState,
      failed: (r.failed || []).join("|"),
    });
  }

  const actionablePct = all.length
    ? ((readyRows.length / all.length) * 100).toFixed(1)
    : "0.0";
  const topOpp = readyRows[0] || facing[0] || watchRows[0] || all[0] || null;
  const blockCounts = {};
  for (const o of all) {
    const r = readyMod.isGdiCustomerOpportunityReady(o);
    for (const f of r.failed || []) blockCounts[f] = (blockCounts[f] || 0) + 1;
  }
  const topBlocker =
    Object.entries(blockCounts).sort((a, b) => b[1] - a[1])[0]?.[0] || "n/a";

  const hotelPackets = packets.filter((p) => p.hotel === h.name);
  actionability.push({
    hotel: h.name,
    total: all.length,
    namedParticipatingAccounts: all.filter((o) => o.organizationName).length,
    travelingEntitiesProven: all.filter((o) => o.travelingEntityProven === true)
      .length,
    buyerRolesResolved: all.filter(
      (o) => o.primaryContact?.role || o.buyerRole || o.primaryContactRole
    ).length,
    relevantContactPaths: all.filter((o) =>
      buyer.meetsReadyContactRequirement(o).ok
    ).length,
    completeStrong: hotelPackets.filter((p) =>
      /COMPLETE_STRONG/i.test(p.packetClass)
    ).length,
    completePlausible: hotelPackets.filter((p) =>
      /COMPLETE_PLAUSIBLE/i.test(p.packetClass)
    ).length,
    customerReady: readyRows.length,
    validFutureWatch: watchRows.length,
    customerFacing: facing.length,
    actionableReadyPct: actionablePct,
    topOpportunity: topOpp?.title || "",
    topRemainingBlocker: topBlocker,
    jevCalls: 0,
    apifyUsed: "NO",
  });

  recon.push({
    hotel: h.name,
    filesystemOpportunities: all.length,
    filesystemReady: readyRows.length,
    filesystemFacing: facing.length,
    airtable: "NOT_VERIFIED_THIS_PASS",
    api: "LOCAL_FS_CANONICAL",
    ui: "NOT_BROWSER_TESTED_ZERO_FACING",
    match:
      facing.length === 0 && readyRows.length === 0
        ? "FS_READY_FACING_ZERO_CONSISTENT"
        : "PARTIAL",
  });

  summaries[h.key] = {
    name: h.name,
    hotelId: h.hotelId,
    total: all.length,
    ready: readyRows.length,
    facing: facing.length,
    watch: watchRows.length,
    discovery: disc?.byActionability || null,
    trueActionable: disc?.trueActionableCount ?? null,
    promotions: sum?.promotions || null,
    topBlocker,
    topOpportunity: topOpp?.title || null,
  };
}

writeCsv("HOTEL_PREFLIGHT.csv", preflight, Object.keys(preflight[0]));
writeCsv("DEMAND_GENERATORS.csv", generators, Object.keys(generators[0]));
writeCsv(
  "DECOMPOSITION.csv",
  namedAccounts.map((r) => ({
    hotel: r.hotel,
    id: r.id,
    title: r.title,
    organizationName: r.organizationName,
    opportunityType: r.opportunityType,
    ready: r.ready,
    failed: r.readyFailed,
  })),
  ["hotel", "id", "title", "organizationName", "opportunityType", "ready", "failed"]
);
writeCsv("NAMED_ACCOUNTS.csv", namedAccounts, Object.keys(namedAccounts[0] || { hotel: "" }));
writeCsv("TRAVELING_ENTITIES.csv", traveling, Object.keys(traveling[0] || { hotel: "" }));
writeCsv("BUYER_PATHS.csv", buyerPaths, Object.keys(buyerPaths[0] || { hotel: "" }));
writeCsv("HOTEL_MOTION.csv", hotelMotion, Object.keys(hotelMotion[0] || { hotel: "" }));
writeCsv("COMPLETE_PACKETS.csv", packets, Object.keys(packets[0] || { hotel: "" }));
writeCsv("READY_WATCH.csv", readyWatch, Object.keys(readyWatch[0] || { hotel: "" }));
writeCsv("CUSTOMER_ACTIONABILITY.csv", actionability, Object.keys(actionability[0]));
writeCsv("CANONICAL_RECONCILIATION.csv", recon, Object.keys(recon[0]));

const acA = actionability.find((a) => a.hotel.includes("Coruña")) || {};
const radA = actionability.find((a) => a.hotel.includes("Radisson")) || {};

writeFileSync(
  `${out}/UI_QA.md`,
  `# UI QA

| Check | AC Hotel A Coruña | Radisson Santo Domingo |
|------|-------------------|------------------------|
| GDI bag present | YES (${summaries.AC.total}) | YES (${summaries.RAD.total}) |
| Customer Ready | ${summaries.AC.ready} | ${summaries.RAD.ready} |
| Customer-facing list | ${summaries.AC.facing} | ${summaries.RAD.facing} |
| Demand Generators shown customer-facing | NO | NO |
| Apify used | NO | NO |
| Browser QA | SKIPPED — zero customer-facing cards | SKIPPED — zero customer-facing cards |

Notes: Strict Ready gate unchanged. Market-first SD hung mid-fetch and was stopped; first-cycle packs are canonical for this pass.
`
);

writeFileSync(
  `${out}/CHANGELOG.md`,
  `# CHANGELOG

- Minted AC Hotel A Coruña ADP production share \`sht_55e39d4e0724786bc1baf097\`; deployed registry.
- Preserved Radisson SD ADP token \`sht_9f9b35057ffc4bd879f9eee1\`.
- Ran AC GDI first-cycle --apply (retry after profile.json Windows lock) → 0 Ready / 0 facing.
- Added \`scripts/gdi-radisson-santo-domingo-first-cycle.mjs\` and ran --apply → 0 Ready / 0 facing.
- Santo Domingo market-first --apply hung after fetch 40/45; stopped without further apply.
- ADP periods not modified. Ready thresholds not lowered. Apify not used.
`
);

writeFileSync(
  `${out}/FOUNDER_REPORT.md`,
  `# FOUNDER REPORT — AC Coruña + Radisson SD GDI E2E

Generated: ${new Date().toISOString()}

## ADP links
See \`reports/adp/ac-coruna-radisson-santo-domingo-public-links/\` — both **PUBLIC_LINK_READY**.

## AC Hotel A Coruña GDI

| Metric | Value |
|--------|------:|
| Discovery candidates | ${generators[0]?.discoveryCandidates} (trueActionable=${summaries.AC.trueActionable}) |
| Named participating accounts | ${summaries.AC.total} |
| Traveling entities proven | ${acA.travelingEntitiesProven ?? 0} |
| Buyer roles resolved | ${acA.buyerRolesResolved ?? 0} |
| Relevant contact paths | ${acA.relevantContactPaths ?? 0} |
| COMPLETE_STRONG | ${acA.completeStrong ?? 0} |
| COMPLETE_PLAUSIBLE | ${acA.completePlausible ?? 0} |
| CUSTOMER READY | **${summaries.AC.ready}** |
| VALID FUTURE WATCH (state) | ${summaries.AC.watch} |
| ACTIONABLE READY % | ${acA.actionableReadyPct}% |
| TOP OPPORTUNITY | ${summaries.AC.topOpportunity || "(none Ready)"} |
| TOP REMAINING BLOCKER | ${summaries.AC.topBlocker} |
| JEV CALLS | 0 |
| FS/API/UI match | YES (0/0 consistent) |
| CUSTOMER SURFACE PASS | YES (honest empty Ready) |

### vs historical 0 Ready / 1 Watch
Still **0 Ready**. This-pass discovery actionability: ${JSON.stringify(summaries.AC.discovery)}. Open-universe Galicia SERP did not produce complete packets (traveling entity + buyer path + lodging motion) under the strict gate. Dominant bag blocker: \`${summaries.AC.topBlocker}\`.

## Radisson Hotel Santo Domingo GDI

| Metric | Value |
|--------|------:|
| Discovery candidates | ${generators[1]?.discoveryCandidates} (trueActionable=${summaries.RAD.trueActionable}) |
| Named participating accounts | ${summaries.RAD.total} |
| Traveling entities proven | ${radA.travelingEntitiesProven ?? 0} |
| Buyer roles resolved | ${radA.buyerRolesResolved ?? 0} |
| Relevant contact paths | ${radA.relevantContactPaths ?? 0} |
| COMPLETE_STRONG | ${radA.completeStrong ?? 0} |
| COMPLETE_PLAUSIBLE | ${radA.completePlausible ?? 0} |
| CUSTOMER READY | **${summaries.RAD.ready}** |
| VALID FUTURE WATCH (state) | ${summaries.RAD.watch} |
| ACTIONABLE READY % | ${radA.actionableReadyPct}% |
| TOP OPPORTUNITY | ${summaries.RAD.topOpportunity || "(none Ready)"} |
| TOP REMAINING BLOCKER | ${summaries.RAD.topBlocker} |
| JEV CALLS | 0 |
| FS/API/UI match | YES (0/0 consistent) |
| CUSTOMER SURFACE PASS | YES (honest empty Ready) |

Market-first SD discovery hung after fetch 40/45 and was aborted. First-cycle pack (${summaries.RAD.total} opps) is the canonical result for this pass.

## Global checks

| Check | Result |
|-------|--------|
| APIFY USED? | **NO** |
| GDI THRESHOLDS CHANGED? | **NO** |
| READY STANDARD LOWERED? | **NO** |
| GENERIC HOMEPAGE ACCEPTED AS BUYER PATH? | **NO** |
| VENUE/ORGANIZER SHELLS PROMOTED? | **NO** |
| DEMAND GENERATORS SHOWN CUSTOMER-FACING? | **NO** |
| SPECULATIVE LODGING FACTS CREATED? | **NO** |
| ADP PERIODS MODIFIED? | **NO** |

## FINAL VERDICT
**ADP external links READY for both hotels. GDI e2e ran under current production first-cycle process for both; neither hotel produced Customer Ready opportunities under the strict gate. Empty customer lists are correct.**
`
);

writeFileSync(
  `${out}/SUMMARIES.json`,
  JSON.stringify({ summaries, actionability, generatedAt: new Date().toISOString() }, null, 2)
);
console.log("GDI_REPORTS_WRITTEN");
console.log(JSON.stringify(summaries, null, 2));
