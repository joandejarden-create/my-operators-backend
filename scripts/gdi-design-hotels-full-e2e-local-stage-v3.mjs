#!/usr/bin/env node
/**
 * Design Hotels Full E2E Commercial V3 — local finalize → STAGED connector.
 *
 * After CohortExecute:
 *  1. Merge chain persistence / processed contacts into local opportunities.json
 *  2. Evaluate finalized eligibility (demand + evidence + contact/route + action)
 *  3. Build customer publication package from finalized rows only
 *  4. stageForProduction (local STAGED pointer only — no CURRENT / Airtable / publish)
 *
 * Usage:
 *   node scripts/gdi-design-hotels-full-e2e-local-stage-v3.mjs
 *   node scripts/gdi-design-hotels-full-e2e-local-stage-v3.mjs --report-root reports/gdi-design-hotels-full-e2e-commercial-v3-paid
 *   node scripts/gdi-design-hotels-full-e2e-local-stage-v3.mjs --dry-run
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  runPublicationQualityGate,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import {
  evaluateContactCompletenessForFinalization,
  evaluateVerifiedCommercialRoute,
} from "../lib/group-demand-intelligence/presentation/gdi-verified-commercial-route-v1.js";
import { loadOpportunitiesCanonical, saveOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const HOTELS = Object.freeze([
  { hotelId: "rece0NdV9FCnaFlVS", hotelName: "Condesa DF" },
  { hotelId: "recaokE8psIbuVeuH", hotelName: "LaLuna Boutique Hotels & Villas" },
  { hotelId: "rec1t35e3sPerzxYu", hotelName: "Memmo Príncipe Real" },
]);

function arg(flag, fallback = null) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] || fallback : fallback;
}
function has(flag) {
  return process.argv.includes(flag);
}
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

const PLACEHOLDER_CONTACT =
  /^(organization contact desk|world sailing|n\/a|unknown|tbd|unidentified|contact desk|info@|sales@)$/i;

/** Reject non-person extractions commonly emitted by thin contact passes. */
function isPlausiblePersonName(name) {
  const n = text(name);
  if (!n || n.length < 5 || n.length > 80) return false;
  if (PLACEHOLDER_CONTACT.test(n)) return false;
  if (/@|\.com|\.org|http/i.test(n)) return false;
  if (
    /member\s*(select|log)|website feedback|pennsylvania|place brand|belgium\s*tel|reduction forum|survey software|amsterdam|the netherlands|mexico city|^george grenada$/i.test(
      n,
    )
  ) {
    return false;
  }
  if (
    /\b(netherlands|mexico|portugal|conference|congress|exhibition|software|committee|developers|forum)\b/i.test(
      n,
    ) &&
    !/\b(rev\.|dr\.|mr\.|ms\.|mrs\.)\b/i.test(n)
  ) {
    return false;
  }
  const parts = n.split(/\s+/).filter(Boolean);
  if (parts.length < 2 || parts.length > 5) return false;
  // Accented Latin letters allowed (Portuguese/Spanish names).
  const personish = parts.filter((p) =>
    /^[\p{L}][\p{L}'\-]{1,30}$/u.test(p),
  );
  return personish.length >= 2;
}

function inferOfficialCommercialRoute(opp = {}) {
  const org = text(opp.organizationName || opp.organization || opp.programmeName);
  if (!org) return null;
  if (!opp.deepResearched && opp.researchLifecycleStatus !== "DEEP_RESEARCHED") {
    return null;
  }
  const urls = [
    ...(Array.isArray(opp.sourceUrls) ? opp.sourceUrls : []),
    ...(Array.isArray(opp.investigatedUrls) ? opp.investigatedUrls : []),
    ...(Array.isArray(opp.seedUrls) ? opp.seedUrls : []),
    ...(Array.isArray(opp.evidence)
      ? opp.evidence.map((e) => e?.url).filter(Boolean)
      : []),
  ]
    .map(String)
    .filter((u) => /^https?:\/\//i.test(u));
  if (!urls.length) return null;
  const preferred = urls.find((u) =>
    /contact|organiser|organizer|committee|speakers?|team|about|lodging|accommodation|housing|hotel/i.test(
      u,
    ),
  );
  // Prefer a functional contact/accommodation path; never promote a bare homepage.
  const routeUrl = preferred || null;
  if (!routeUrl) return null;
  const candidate = {
    kind: "OFFICIAL_ORG_EVENT_CHANNEL",
    organization: org,
    url: routeUrl,
    label: `Official ${org} organizing / lodging channel`,
  };
  const routeGate = evaluateVerifiedCommercialRoute(candidate);
  if (!routeGate.ok) return null;
  return { ...candidate, routeGateCode: routeGate.code };
}

/**
 * Finalized eligibility for local STAGED founder review (not auto Sales Ready).
 * Mirrors founder A–G requirements without inventing readiness.
 */
export function evaluateFinalizedStagedGate(opp = {}) {
  const reasons = [];
  const demand = text(
    opp.organizationName ||
      opp.organization ||
      opp.programmeName ||
      opp.demandGenerator ||
      opp.title,
  );
  if (!demand) reasons.push("MISSING_DEMAND_GENERATOR");

  const urls = [
    ...(Array.isArray(opp.sourceUrls) ? opp.sourceUrls : []),
    ...(Array.isArray(opp.seedUrls) ? opp.seedUrls : []),
    ...(Array.isArray(opp.investigatedUrls) ? opp.investigatedUrls : []),
    ...(Array.isArray(opp.evidence)
      ? opp.evidence.map((e) => e?.url).filter(Boolean)
      : []),
  ].filter((u) => /^https?:\/\//i.test(String(u)));
  if (!urls.length) reasons.push("MISSING_EVIDENCE_URLS");

  const thesis = text(
    opp.hotelOpportunityThesis ||
      opp.whyThisHotel ||
      opp.summary ||
      opp.whatIsHappening,
  );
  if (thesis.length < 40) reasons.push("MISSING_HOTEL_FIT_THESIS");

  const primary = opp.primaryContact || {};
  const namedPeople = [
    text(primary.name),
    ...(Array.isArray(opp.namedStakeholders)
      ? opp.namedStakeholders.map((n) => text(n?.name))
      : []),
    ...(Array.isArray(opp.salesReadyContactNames)
      ? opp.salesReadyContactNames.map(text)
      : []),
  ].filter((n) => isPlausiblePersonName(n));

  const inferredRoute = inferOfficialCommercialRoute(opp);
  if (inferredRoute && !text(opp.approvedCommercialRoute)) {
    opp = {
      ...opp,
      approvedCommercialRoute: inferredRoute.label,
      commercialContactRoute: inferredRoute.url,
    };
  }

  const contactEval = evaluateContactCompletenessForFinalization(opp, {
    looksLikePersonName: isPlausiblePersonName,
  });
  // Restored contract: named person OR verified functional org route only.
  // A generic HTTPS homepage / "Official {org} organizing / lodging channel"
  // label alone does NOT satisfy finalization.
  const contactOk = contactEval.complete === true;
  if (!contactOk) reasons.push("CONTACT_RESEARCH_REQUIRED");

  const action = text(
    opp.recommendedNextStep ||
      opp.recommendedSalesOrMarketingAction ||
      opp.customerNarrative ||
      opp.whyActNow ||
      opp.cardWhyNowLine,
  );
  if (action.length < 20 && thesis.length < 80) {
    reasons.push("MISSING_COMMERCIAL_ACTION");
  }

  const placeholders = /CONTACT_RESEARCH_REQUIRED|TODO_RESEARCH|PLACEHOLDER|TBD_CONTACT/i;
  if (placeholders.test(JSON.stringify({ thesis, action, title: opp.title }))) {
    reasons.push("INTERNAL_PLACEHOLDER_PRESENT");
  }

  return {
    finalizedReady: reasons.length === 0,
    reasons,
    namedPeople,
    contactOk,
    salesReady: opp.salesReady === true,
    urls: urls.slice(0, 8),
  };
}

function latestHotelRunDir(reportRoot, hotelId) {
  const base = path.join(reportRoot, "hotels", hotelId);
  if (!fs.existsSync(base)) return null;
  const dirs = fs
    .readdirSync(base, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => {
      const full = path.join(base, d.name);
      const trace = path.join(full, "chain-execution-trace.json");
      let processed = 0;
      if (fs.existsSync(trace)) {
        try {
          const j = JSON.parse(fs.readFileSync(trace, "utf8"));
          processed = Array.isArray(j.processed) ? j.processed.length : 0;
        } catch {
          processed = 0;
        }
      }
      return { full, processed, mtime: fs.statSync(full).mtimeMs };
    })
    .sort((a, b) => b.processed - a.processed || b.mtime - a.mtime);
  return dirs[0]?.full || null;
}

function mergeContactsFromRun(opportunities, runDir) {
  if (!runDir) return { merged: 0, mapSize: 0, deepMarked: 0 };
  const tracePath = path.join(runDir, "chain-execution-trace.json");
  if (!fs.existsSync(tracePath)) return { merged: 0, mapSize: 0, deepMarked: 0 };
  const trace = JSON.parse(fs.readFileSync(tracePath, "utf8"));
  const map = new Map();
  for (const row of trace.processed || []) {
    const id = text(row.opportunityId || row.id);
    if (!id) continue;
    const name = text(row.primaryContact?.name || row.primaryContactName);
    const personOk = isPlausiblePersonName(name);
    map.set(id, {
      primaryContact: personOk
        ? {
            name,
            role:
              text(row.primaryContact?.role || row.primaryContact?.title) || null,
            organization: text(row.organizationName) || null,
            customerFacingNamed: true,
            source: "cohort_execute_chain_trace_v3",
          }
        : null,
      namedStakeholders: personOk
        ? [{ name, role: text(row.primaryContact?.role) || null }]
        : [],
      salesReadyContactNames: (row.salesReadyContactNames || []).filter(
        isPlausiblePersonName,
      ),
      deepResearched: true,
      researchLifecycleStatus: "DEEP_RESEARCHED",
      disposition: row.disposition || null,
    });
  }
  let merged = 0;
  let deepMarked = 0;
  for (const o of opportunities) {
    const id = text(o.opportunityId || o.id);
    const c = map.get(id);
    if (!c) continue;
    let changed = false;
    if (!o.deepResearched) {
      o.deepResearched = true;
      o.researchLifecycleStatus = "DEEP_RESEARCHED";
      deepMarked++;
      changed = true;
    }
    if (c.primaryContact?.name && !isPlausiblePersonName(o.primaryContact?.name)) {
      o.primaryContact = c.primaryContact;
      changed = true;
    }
    if (c.namedStakeholders.length && !(o.namedStakeholders || []).some((n) => isPlausiblePersonName(n?.name))) {
      o.namedStakeholders = c.namedStakeholders;
      changed = true;
    }
    if (c.salesReadyContactNames?.length) {
      o.salesReadyContactNames = c.salesReadyContactNames;
      changed = true;
    }
    if (!o.approvedCommercialRoute) {
      const route = inferOfficialCommercialRoute(o);
      if (route) {
        o.approvedCommercialRoute = route.label;
        o.commercialContactRoute = route.url;
        o.commercialRouteMeta = route;
        changed = true;
      }
    }
    if (changed) {
      o.localStageMerge = {
        at: new Date().toISOString(),
        runDir,
      };
      merged++;
    }
  }
  return { merged, mapSize: map.size, deepMarked };
}

function synthesizeCommercialAction(opp) {
  const name = text(opp.primaryContact?.name);
  const role = text(opp.primaryContact?.role);
  const org = text(opp.organizationName || opp.organization);
  if (name && isPlausiblePersonName(name)) {
    return `Research outreach to ${name}${role ? ` (${role})` : ""}${
      org ? ` at ${org}` : ""
    } — confirm lodging / meeting decision role, propose property-fit options, and request timing for a short commercial conversation.`;
  }
  const route = text(opp.approvedCommercialRoute || opp.commercialContactRoute);
  if (route) {
    return `Engage via verified commercial route (${route}) — confirm group lodging / meeting needs for ${
      org || "this demand source"
    }, introduce the property fit, and request the appropriate organizer / planner contact for follow-up.`;
  }
  return text(opp.hotelOpportunityThesis).slice(0, 280);
}

async function stageHotel(hotel, reportRoot, { dryRun = false } = {}) {
  const runDir = latestHotelRunDir(reportRoot, hotel.hotelId);
  const doc = await loadOpportunitiesCanonical(hotel.hotelId, {
    bypassCache: true,
  });
  const opportunities = Array.isArray(doc.opportunities)
    ? doc.opportunities
    : [];
  const merge = mergeContactsFromRun(opportunities, runDir);

  // Ensure commercial action + official route where deep research supports it
  for (const o of opportunities) {
    if (!o.approvedCommercialRoute) {
      const route = inferOfficialCommercialRoute(o);
      if (route) {
        o.approvedCommercialRoute = route.label;
        o.commercialContactRoute = route.url;
        o.commercialRouteMeta = route;
      }
    }
    const hasPerson = isPlausiblePersonName(o.primaryContact?.name);
    const hasRoute = Boolean(text(o.approvedCommercialRoute || o.commercialContactRoute));
    if ((hasPerson || hasRoute) && text(o.recommendedNextStep).length < 20) {
      o.recommendedNextStep = synthesizeCommercialAction(o);
    }
  }

  if (!dryRun) {
    await saveOpportunitiesCanonical(hotel.hotelId, {
      ...doc,
      opportunities,
      updatedAt: new Date().toISOString(),
      note: "gdi-design-hotels-full-e2e-local-stage-v3 merge",
    });
  }

  const gated = opportunities.map((o) => ({
    opportunityId: text(o.opportunityId || o.id),
    title: o.title,
    gate: evaluateFinalizedStagedGate(o),
    opp: o,
  }));
  const finalized = gated.filter((g) => g.gate.finalizedReady).map((g) => g.opp);
  const excluded = gated
    .filter((g) => !g.gate.finalizedReady)
    .map((g) => ({
      opportunityId: g.opportunityId,
      title: g.title,
      reasons: g.gate.reasons,
    }));

  const dtos = finalized.map((o) => {
    const projected = projectOpportunityForPresentation(o, {
      hotelId: hotel.hotelId,
      surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
      view: "detail",
    });
    return projected.dto;
  });

  const publicationVersion = `gdi_dh_v3_staged_${hotel.hotelId}_${new Date()
    .toISOString()
    .slice(0, 10)
    .replace(/-/g, "")}`;

  let pkg = null;
  let integrity = { ok: finalized.length === 0, skippedEmpty: finalized.length === 0 };
  let quality = { ok: true, blockingCount: 0 };
  let staged = { ok: false, skipped: true, reason: finalized.length === 0 ? "NO_FINALIZED" : null };
  let storePath = null;

  if (finalized.length > 0) {
    pkg = buildCustomerPublicationPackage({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      opportunities: dtos,
      publicationVersion,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
      approvalStatus: "CANDIDATE",
      applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
      notes:
        "Design Hotels Full E2E Commercial V3 — local STAGED founder review only; not CURRENT / not customer-published",
    });
    integrity = verifyPublicationPackageIntegrity(pkg);
    quality = runPublicationQualityGate({
      opportunities: pkg.opportunities,
    });
    if (!dryRun && integrity.ok) {
      const store = createGdiPublicationPackageStore();
      staged = store.stageForProduction({
        ...pkg,
        lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
        gates: {
          integrity: { ok: integrity.ok },
          quality: { ok: quality.ok, blockingCount: quality.blockingCount },
          finalizedOnly: true,
          finalizedCount: finalized.length,
        },
      });
      storePath = staged.path || null;
    }
  }

  return {
    hotelId: hotel.hotelId,
    hotelName: hotel.hotelName,
    runDir,
    inventoryCount: opportunities.length,
    merge,
    deepResearched: opportunities.filter((o) => o.deepResearched).length,
    withPrimaryContact: opportunities.filter((o) => o.primaryContact?.name)
      .length,
    finalizedCount: finalized.length,
    salesReadyCount: finalized.filter((o) => o.salesReady === true).length,
    excludedCount: excluded.length,
    excludedSample: excluded.slice(0, 25),
    publicationVersion: pkg?.publicationVersion || null,
    publicationId: pkg?.publicationId || null,
    contentFingerprintSha256: pkg?.contentFingerprintSha256 || null,
    integrityOk: integrity.ok !== false,
    qualityOk: quality.ok !== false,
    qualityBlocking: quality.blockingCount || 0,
    staged: staged.ok === true,
    storePath,
    dryRun,
  };
}

async function main() {
  const reportRoot = path.resolve(
    ROOT,
    arg(
      "--report-root",
      "reports/gdi-design-hotels-full-e2e-commercial-v3-paid",
    ),
  );
  const dryRun = has("--dry-run");
  const outDir = path.join(reportRoot, "local-stage");
  ensureDir(outDir);

  const hotels = [];
  for (const hotel of HOTELS) {
    const row = await stageHotel(hotel, reportRoot, { dryRun });
    hotels.push(row);
    writeJson(path.join(outDir, `${hotel.hotelId}.json`), row);
  }

  const summary = {
    contract: "gdi-design-hotels-full-e2e-local-stage-v3",
    at: new Date().toISOString(),
    reportRoot,
    dryRun,
    hotels,
    totals: {
      finalized: hotels.reduce((s, h) => s + h.finalizedCount, 0),
      salesReady: hotels.reduce((s, h) => s + h.salesReadyCount, 0),
      stagedHotels: hotels.filter((h) => h.staged).length,
    },
    safety: {
      airtableWrites: false,
      currentPublication: false,
      findymail: false,
      surfe: false,
    },
  };
  writeJson(path.join(outDir, "SUMMARY.json"), summary);
  console.log(JSON.stringify(summary, null, 2));
  const ok = dryRun || hotels.every((h) => h.integrityOk !== false);
  process.exit(ok ? 0 : 2);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
