#!/usr/bin/env node
/**
 * $0 audit of Design Hotels V3 STAGED opportunities — contact fate classification.
 * Read-only. Does not mutate STAGED pointers or packages.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/gdi-design-hotels-full-e2e-commercial-v3-paid",
);
const OUT = path.join(REPORT, "contact-restore-audit-v1");

const HOTELS = [
  {
    hotelId: "rece0NdV9FCnaFlVS",
    hotelName: "Condesa DF",
    stagedVersion: "gdi_dh_v3_staged_rece0NdV9FCnaFlVS_20261124",
  },
  {
    hotelId: "recaokE8psIbuVeuH",
    hotelName: "LaLuna Boutique Hotels & Villas",
    stagedVersion: "gdi_dh_v3_staged_recaokE8psIbuVeuH_20261124",
  },
  {
    hotelId: "rec1t35e3sPerzxYu",
    hotelName: "Memmo Príncipe Real",
    stagedVersion: "gdi_dh_v3_staged_rec1t35e3sPerzxYu_20261124",
  },
];

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function writeJson(file, body) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(body, null, 2)}\n`, "utf8");
}

function plausiblePerson(name) {
  const n = text(name);
  if (!n || !looksLikePersonName(n)) return false;
  if (
    /organization contact desk|member\s*(select|log)|website feedback|pennsylvania|place brand|belgium\s*tel|survey software|amsterdam|the netherlands|mexico city|george grenada|reduction forum|developers|conference|congress|exhibition|software|committee|buyer not yet|contact research/i.test(
      n,
    )
  ) {
    return false;
  }
  return true;
}

function isGenericOrgRoute(label, url) {
  const l = text(label);
  const u = text(url);
  if (!l && !u) return true;
  if (/Official .+ organizing \/ lodging channel/i.test(l)) {
    // Homepage-only routes are generic unless URL proves contact/housing dept.
    if (!u) return true;
    if (
      /contact|organiser|organizer|committee|housing|lodging|accommodation|planner|sales@|events@|meetings@|groups@/i.test(
        u,
      )
    ) {
      return false;
    }
    // Bare domain / homepage → generic
    try {
      const parsed = new URL(u);
      const pathOnly = parsed.pathname.replace(/\/+$/, "") || "/";
      if (pathOnly === "/" || pathOnly.length < 2) return true;
    } catch {
      return true;
    }
    return true;
  }
  return false;
}

function loadStagedPackage(hotel) {
  const pkgPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotel.hotelId,
    hotel.stagedVersion,
    "package.json",
  );
  return JSON.parse(fs.readFileSync(pkgPath, "utf8"));
}

function pickRun(hotelId) {
  const base = path.join(REPORT, "hotels", hotelId);
  if (!fs.existsSync(base)) return null;
  return fs
    .readdirSync(base)
    .map((n) => path.join(base, n))
    .sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs)[0];
}

function loadChainContacts(runDir) {
  const map = new Map();
  if (!runDir) return map;
  const tracePath = path.join(runDir, "chain-execution-trace.json");
  if (!fs.existsSync(tracePath)) return map;
  const trace = JSON.parse(fs.readFileSync(tracePath, "utf8"));
  for (const row of trace.processed || []) {
    const id = text(row.opportunityId || row.id);
    if (!id) continue;
    const name = text(row.primaryContact?.name || row.primaryContactName);
    map.set(id, {
      name,
      role: text(row.primaryContact?.role || row.primaryContact?.title) || null,
      plausible: plausiblePerson(name),
      org: text(row.organizationName) || null,
      salesReadyContactNames: (row.salesReadyContactNames || []).filter(
        plausiblePerson,
      ),
    });
  }
  return map;
}

function loadPriorPilotContacts(hotelId) {
  const base = path.join(
    ROOT,
    "reports/gdi-design-hotels-live-research-pilot-v1/hotels",
    hotelId,
  );
  const map = new Map();
  if (!fs.existsSync(base)) return map;
  const runs = fs
    .readdirSync(base)
    .filter((n) => n.startsWith("gdi_dh_live_"))
    .map((n) => path.join(base, n));
  let best = null;
  for (const run of runs) {
    const tracePath = path.join(run, "chain-execution-trace.json");
    if (!fs.existsSync(tracePath)) continue;
    const trace = JSON.parse(fs.readFileSync(tracePath, "utf8"));
    const n = (trace.processed || []).length;
    if (!best || n > best.n) best = { run, trace, n };
  }
  if (!best) return map;
  for (const row of best.trace.processed || []) {
    const id = text(row.opportunityId || row.id);
    const name = text(row.primaryContact?.name || row.primaryContactName);
    if (!id || !plausiblePerson(name)) continue;
    map.set(id, {
      name,
      role: text(row.primaryContact?.role) || null,
      org: text(row.organizationName) || null,
      sourceRun: best.run,
    });
  }
  return map;
}

function loadInventory(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) return [];
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return doc.opportunities || [];
}

function classifyRow({
  stagedOpp,
  inv,
  chainContact,
  priorPilotContact,
}) {
  const id = text(stagedOpp.id || stagedOpp.opportunityId);
  const stagedName = text(
    stagedOpp.primaryContact?.name ||
      stagedOpp.contactName ||
      stagedOpp.tileContactPreview?.name ||
      stagedOpp.canonicalCustomerPresentation?.contact?.name,
  );
  const stagedPlausible = plausiblePerson(stagedName);
  const invName = text(inv?.primaryContact?.name);
  const invPlausible = plausiblePerson(invName);
  const chainPlausible = chainContact?.plausible === true;
  const priorPlausible = Boolean(priorPilotContact?.name);
  const routeLabel = text(
    inv?.approvedCommercialRoute ||
      stagedOpp.approvedCommercialRoute ||
      stagedOpp.commercialContactRoute,
  );
  const routeUrl = text(
    inv?.commercialContactRoute || stagedOpp.commercialContactRoute,
  );
  const genericRoute = isGenericOrgRoute(routeLabel, routeUrl);

  let fate = "F_GENUINELY_UNAVAILABLE";
  let detail = "No plausible named person in staged, inventory, chain, or prior pilot";

  if (stagedPlausible) {
    fate = "A_FOUND_AND_SELECTED";
    detail = `Staged displays ${stagedName}`;
  } else if (invPlausible && !stagedPlausible) {
    fate = "B_FOUND_BUT_LOST_IN_TRANSFORM";
    detail = `Inventory has ${invName} but staged package lost/omitted name`;
  } else if (chainPlausible && !stagedPlausible) {
    fate = "B_FOUND_BUT_LOST_IN_TRANSFORM";
    detail = `Chain processed has ${chainContact.name} but not in staged`;
  } else if (priorPlausible && !stagedPlausible) {
    fate = "D_AVAILABLE_IN_PRIOR_NOT_REUSED";
    detail = `Prior pilot had ${priorPilotContact.name} (different opportunity identity may block reuse)`;
  } else if (
    chainContact?.name &&
    !chainPlausible &&
    text(chainContact.name)
  ) {
    fate = "C_FOUND_BUT_WRONGLy_REJECTED_OR_JUNK";
    detail = `Chain emitted non-person contact string: ${chainContact.name}`;
  } else if (!chainContact && !inv?.deepResearched) {
    fate = "E_NEVER_INVESTIGATED_ADEQUATELY";
    detail = "No chain contact row and not deep-researched in inventory";
  } else if (routeLabel && !genericRoute) {
    fate = "F_GENUINELY_UNAVAILABLE_VALID_ROUTE_ONLY";
    detail = `No named person; non-generic org route present: ${routeLabel}`;
  } else if (routeLabel && genericRoute) {
    fate = "E_GENERIC_ROUTE_USED_AS_COMPLETION";
    detail = `Generic org-route/homepage used as contact completion: ${routeLabel} | ${routeUrl}`;
  }

  const eligibleNamed = stagedPlausible || invPlausible || chainPlausible;
  const eligibleValidRoute = Boolean(routeLabel) && !genericRoute;
  const finalizedReadyStrict = eligibleNamed; // named person required for restored model
  const finalizedReadyWithValidRoute = eligibleNamed || eligibleValidRoute;

  return {
    opportunityId: id,
    title: text(stagedOpp.title || stagedOpp.displayTitle),
    organizationName: text(
      stagedOpp.organizationName || stagedOpp.displayCompanyName || inv?.organizationName,
    ),
    fate,
    detail,
    stagedName: stagedPlausible ? stagedName : null,
    inventoryName: invPlausible ? invName : null,
    chainName: chainPlausible ? chainContact.name : null,
    priorPilotName: priorPlausible ? priorPilotContact.name : null,
    routeLabel: routeLabel || null,
    routeUrl: routeUrl || null,
    genericRoute,
    finalizedReadyStrict,
    finalizedReadyWithValidRoute,
  };
}

function main() {
  ensureDir(OUT);
  const hotels = [];
  const allRows = [];

  for (const hotel of HOTELS) {
    const pkg = loadStagedPackage(hotel);
    const opps = pkg.opportunities || [];
    const runDir = pickRun(hotel.hotelId);
    const chainMap = loadChainContacts(runDir);
    const priorMap = loadPriorPilotContacts(hotel.hotelId);
    const inventory = loadInventory(hotel.hotelId);
    const invById = new Map(
      inventory.map((o) => [text(o.opportunityId || o.id), o]),
    );

    const rows = opps.map((stagedOpp) => {
      const id = text(stagedOpp.id || stagedOpp.opportunityId);
      return classifyRow({
        stagedOpp,
        inv: invById.get(id) || null,
        chainContact: chainMap.get(id) || null,
        priorPilotContact: priorMap.get(id) || null,
      });
    });

    const fateCounts = {};
    for (const r of rows) {
      fateCounts[r.fate] = (fateCounts[r.fate] || 0) + 1;
      allRows.push({ hotelId: hotel.hotelId, hotelName: hotel.hotelName, ...r });
    }

    const namedRecoverable = rows.filter(
      (r) =>
        r.stagedName ||
        r.inventoryName ||
        r.chainName ||
        r.priorPilotName,
    );
    const hotelSummary = {
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      stagedVersion: hotel.stagedVersion,
      stagedCount: opps.length,
      fateCounts,
      namedOnStaged: rows.filter((r) => r.stagedName).length,
      namedRecoverableFromEvidence: namedRecoverable.length,
      genericRouteCount: rows.filter((r) => r.genericRoute && r.routeLabel)
        .length,
      strictFinalizedReady: rows.filter((r) => r.finalizedReadyStrict).length,
      validRouteFinalizedReady: rows.filter(
        (r) => r.finalizedReadyWithValidRoute,
      ).length,
      recoverableNamedSample: namedRecoverable.slice(0, 20).map((r) => ({
        id: r.opportunityId,
        title: r.title,
        staged: r.stagedName,
        inventory: r.inventoryName,
        chain: r.chainName,
        prior: r.priorPilotName,
        fate: r.fate,
      })),
      runDir,
      chainContactMapSize: chainMap.size,
      priorPilotNamedSize: priorMap.size,
    };
    hotels.push(hotelSummary);
    writeJson(path.join(OUT, `${hotel.hotelId}.json`), {
      ...hotelSummary,
      rows,
    });
  }

  const summary = {
    contract: "gdi-design-hotels-staged-contact-audit-v1",
    at: new Date().toISOString(),
    portfolioStaged: allRows.length,
    hotels,
    fateTotals: allRows.reduce((a, r) => {
      a[r.fate] = (a[r.fate] || 0) + 1;
      return a;
    }, {}),
    strictFinalizedReadyTotal: allRows.filter((r) => r.finalizedReadyStrict)
      .length,
    validRouteFinalizedReadyTotal: allRows.filter(
      (r) => r.finalizedReadyWithValidRoute,
    ).length,
    namedRecoverableTotal: allRows.filter(
      (r) => r.stagedName || r.inventoryName || r.chainName || r.priorPilotName,
    ).length,
  };
  writeJson(path.join(OUT, "SUMMARY.json"), summary);
  writeJson(path.join(OUT, "ALL_ROWS.json"), allRows);
  console.log(JSON.stringify(summary, null, 2));
}

main();
