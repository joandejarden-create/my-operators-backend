/**
 * Seed Brand Deal Requests (+ Deal Activity Log) so My Deals / workspace KPI strips
 * match the home-dashboard mock targets (brand persona):
 *
 *   Flow:  New (7d) 5, In review 4†, Awaiting owner 3, At risk 2
 *   Pipe:  New 3, Under review 4, Bid submitted 5, Negotiation 2, Closed 1, Passed 2
 *
 *   †Home mock shows "In review" 6 vs pipeline "Under review" 4 — workspace uses one
 *     active-review bucket for both; this seed matches pipeline (4).
 *
 * *Workspace "Brand action" uses isAwaitingBrand rules; with pipeline 3+4 it reads ≥7.
 *   Home "Needs your action" (4) uses different mock semantics until /api/dashboard/home
 *   is wired to live BDRs. This script optimizes pipeline + 7d / at-risk / awaiting-owner.
 *
 * Prerequisites: AIRTABLE_API_KEY, AIRTABLE_BASE_ID in .env; ≥20 deals recommended.
 *
 * Usage:
 *   node scripts/seed-workspace-kpi-dashboard-targets.mjs --dry-run
 *   node scripts/seed-workspace-kpi-dashboard-targets.mjs
 *   node scripts/seed-workspace-kpi-dashboard-targets.mjs --clean
 *
 * Env:
 *   KPI_SEED_DEAL_IDS=recA,recB   force deal record IDs (first 20 used)
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  auditWorkspaceKpiMirror,
  computeWorkspaceKpiSnapshot,
  enrichWorkspaceRow,
  isoWeekKey,
  prevIsoWeekKey,
} from "../lib/deal-workspace-pipeline.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

const BDR_TABLE = process.env.AIRTABLE_TABLE_BRAND_DEAL_REQUESTS || "Brand Deal Requests";
const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";
const ACTIVITY_LOG_TABLE = process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log";
const KPI_HISTORY_FILE = path.join(ROOT, "data", "brand-workspace-kpi-history.json");

const DEMO_PREFIX = "KPI Demo";
const SEED_MARKER = "[seed:kpi-dashboard-targets]";

/** Home-dashboard mock (brand) — workspace keys where they align. */
export const TARGETS = {
  pipeline: { newInbound: 3, underReview: 4, bidSubmitted: 5, negotiation: 2, closed: 1, passed: 2 },
  flow: { newRolling7d: 5, inReview: 4, awaitingCounterparty: 3, atRisk: 2 },
  /** Informational — workspace needsAction often higher than home mock. */
  homeNeedsAction: 4,
};

function escapeFormula(s) {
  return String(s ?? "")
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'");
}

function isoDaysAgo(days) {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString();
}

function dateOnlyDaysAgo(days) {
  return isoDaysAgo(days).slice(0, 10);
}

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env");
  }
  return new Airtable({ apiKey }).base(baseId);
}

/**
 * Build BDR specs: 17 pipeline rows + 3 awaiting-info (not in pipeline columns).
 * @returns {{ brand: string, label: string, fields: Record<string, unknown> }[]}
 */
export function buildKpiTargetSpecs() {
  const ownerNotes = `${SEED_MARKER} — dashboard KPI target seed`;
  const specs = [];

  const add = (suffix, label, fields) => {
    specs.push({
      brand: `${DEMO_PREFIX} ${suffix}`,
      label,
      fields: { "Owner Notes": ownerNotes, ...fields },
    });
  };

  // —— Pipeline: new inbound (3) ——
  for (let i = 1; i <= 3; i++) {
    add(`New-${i}`, `Pipeline · new inbound ${i}`, {
      Status: "New",
      "NDA Status": "Not Required",
      "Deal Room Access": "Blocked",
      "Match Score": 70 + i,
      "Request Sent At": isoDaysAgo(i),
      "Created At": isoDaysAgo(i + 2),
      "Last Updated": isoDaysAgo(1),
    });
  }

  // —— Pipeline: under review (4) — meta: 1 new-in-7d, 1 active-in-7d, 1 stalled ——
  add("Review-1", "Pipeline · under review (fresh)", {
    Status: "Brand Viewed",
    "NDA Status": "Not Required",
    "Deal Room Access": "Blocked",
    "Match Score": 74,
    "Request Sent At": isoDaysAgo(3),
    "Last Updated": isoDaysAgo(2),
  });
  add("Review-2", "Pipeline · under review (active 7d)", {
    Status: "Brand Viewed",
    "NDA Status": "Not Required",
    "Deal Room Access": "Blocked",
    "Match Score": 76,
    "Request Sent At": isoDaysAgo(12),
    "Last Updated": isoDaysAgo(4),
  });
  add("Review-3", "Pipeline · under review (stalled)", {
    Status: "Brand Viewed",
    "NDA Status": "Not Required",
    "Deal Room Access": "Blocked",
    "Match Score": 72,
    "Request Sent At": isoDaysAgo(20),
    "Last Updated": isoDaysAgo(18),
    "Next Follow-up Date": dateOnlyDaysAgo(2),
    "Next Follow-up Header": "Brand committee follow-up overdue",
  });
  add("Review-4", "Pipeline · under review", {
    Status: "Brand Viewed",
    "NDA Status": "Not Required",
    "Deal Room Access": "Blocked",
    "Match Score": 78,
    "Request Sent At": isoDaysAgo(25),
    "Last Updated": isoDaysAgo(10),
  });

  // —— Pipeline: bid submitted / terms (5) ——
  for (let i = 1; i <= 5; i++) {
    const stalled = i === 5;
    add(`Terms-${i}`, `Pipeline · bid submitted ${i}`, {
      Status: "Pre-LOI",
      "NDA Status": "Signed - Owner Confirmed",
      "NDA Signed At": isoDaysAgo(30),
      "Deal Room Access": "Granted",
      "Access Granted At": isoDaysAgo(28),
      "Proposal Status": i <= 2 ? "Draft" : "Submitted",
      "Match Score": 80 + i,
      "Request Sent At": isoDaysAgo(10 + i),
      "Last Updated": stalled ? isoDaysAgo(20) : isoDaysAgo(5),
      ...(stalled
        ? {
            "Next Follow-up Date": dateOnlyDaysAgo(4),
            "Next Follow-up Header": "Owner term comparison overdue",
          }
        : {}),
    });
  }

  // —— Pipeline: negotiation (2) — nda-room + advanced ——
  add("Neg-NDA", "Pipeline · negotiation (NDA)", {
    Status: "Accepted",
    "NDA Status": "Sent",
    "NDA Sent At": isoDaysAgo(14),
    "Deal Room Access": "Blocked",
    "Match Score": 83,
    "Request Sent At": isoDaysAgo(9),
    "Last Updated": isoDaysAgo(6),
  });
  add("Neg-Adv", "Pipeline · negotiation (finalist)", {
    Status: "Finalist",
    "NDA Status": "Signed - Owner Confirmed",
    "NDA Signed At": isoDaysAgo(40),
    "Deal Room Access": "Granted",
    "Access Granted At": isoDaysAgo(38),
    "Match Score": 90,
    "Request Sent At": isoDaysAgo(15),
    "Last Updated": isoDaysAgo(4),
  });

  // —— Pipeline: closed (1) + passed (2) ——
  add("Closed-1", "Pipeline · closed (archived)", {
    Status: "Archived",
    "Response Notes": `${DEMO_PREFIX} — Owner selected another path; archived for reporting.`,
    "Match Score": 65,
    "Request Sent At": isoDaysAgo(60),
    "Last Updated": isoDaysAgo(30),
  });
  for (let i = 1; i <= 2; i++) {
    add(`Passed-${i}`, `Pipeline · passed ${i}`, {
      Status: "Declined",
      "Response Date": isoDaysAgo(20 + i),
      "Response Notes": `${DEMO_PREFIX} — Brand declined after internal gate ${i}.`,
      "Match Score": 60 + i,
      "Request Sent At": isoDaysAgo(45),
      "Last Updated": isoDaysAgo(18),
    });
  }

  // —— Flow: awaiting owner (3) — awaiting-info bucket, not in pipeline tiles ——
  add("Await-1", "Flow · awaiting owner (accepted)", {
    Status: "Accepted",
    "NDA Status": "Not Required",
    "Deal Room Access": "Blocked",
    "Match Score": 81,
    "Request Sent At": isoDaysAgo(6),
    "Last Updated": isoDaysAgo(2),
  });
  add("Await-2", "Flow · awaiting owner (more info)", {
    Status: "More Info Requested",
    "Response Notes": `${DEMO_PREFIX} — Requested capex phasing and ownership entity.`,
    "Match Score": 79,
    "Request Sent At": isoDaysAgo(11),
    "Last Updated": isoDaysAgo(3),
  });
  add("Await-3", "Flow · awaiting owner (more info 2)", {
    Status: "More Info Requested",
    "Response Notes": `${DEMO_PREFIX} — Requested indicative opening window.`,
    "Match Score": 77,
    "Request Sent At": isoDaysAgo(14),
    "Last Updated": isoDaysAgo(5),
  });

  return specs;
}

/** Map Airtable-shaped rows for pipeline verify. */
export function specsToVerifyRows(specs) {
  return specs.map((s, idx) => ({
    id: `kpi-spec-${idx}`,
    status: s.fields.Status,
    ndaStatus: s.fields["NDA Status"],
    dealRoomAccess: s.fields["Deal Room Access"],
    proposalStatus: s.fields["Proposal Status"],
    requestSentAt: s.fields["Request Sent At"],
    lastUpdated: s.fields["Last Updated"],
    nextFollowupDate: s.fields["Next Follow-up Date"] ?? null,
  }));
}

export function verifyTargets(specs) {
  const rows = specsToVerifyRows(specs);
  const brand = computeWorkspaceKpiSnapshot(rows, "brand");
  const audit = auditWorkspaceKpiMirror(rows);
  const report = {
    targets: TARGETS,
    actual: {
      pipeline: brand.pipeline,
      flow: {
        needsAction: brand.needsAction,
        newRolling7d: brand.newRolling7d,
        inReview: brand.inReview,
        awaitingCounterparty: brand.awaitingCounterparty,
        atRisk: brand.atRisk,
      },
    },
    auditOk: audit.ok,
    violations: audit.violations,
  };
  return report;
}

function printReport(report) {
  const t = TARGETS;
  const a = report.actual;
  console.log("\n--- KPI target verification (brand persona, demo rows only) ---");
  console.log(
    "Pipeline  new/under/bid/neg/closed/passed:",
    `${a.pipeline.newInbound}/${a.pipeline.underReview}/${a.pipeline.bidSubmitted}/${a.pipeline.negotiation}/${a.pipeline.closed}/${a.pipeline.passed}`,
    "→ target",
    `${t.pipeline.newInbound}/${t.pipeline.underReview}/${t.pipeline.bidSubmitted}/${t.pipeline.negotiation}/${t.pipeline.closed}/${t.pipeline.passed}`
  );
  console.log(
    "Flow      needsAction/new7d/inReview/awaitOwner/atRisk:",
    `${a.flow.needsAction}/${a.flow.newRolling7d}/${a.flow.inReview}/${a.flow.awaitingCounterparty}/${a.flow.atRisk}`,
    "→ target",
    `${t.homeNeedsAction}*/${t.flow.newRolling7d}/${t.flow.inReview}/${t.flow.awaitingCounterparty}/${t.flow.atRisk}`
  );
  if (!report.auditOk) {
    console.warn("Audit violations:", report.violations);
  } else {
    console.log("Mirror / partition audit: OK");
  }
  console.log("---\n");
}

async function findBdrByBrand(base, brandName) {
  const formula = `{Brand Name} = '${escapeFormula(brandName)}'`;
  const rows = await base(BDR_TABLE).select({ filterByFormula: formula, maxRecords: 1 }).firstPage();
  return rows[0] || null;
}

async function deleteDemoRows(base) {
  const bdrIds = [];
  await base(BDR_TABLE)
    .select({
      filterByFormula: `LEFT({Brand Name}, ${DEMO_PREFIX.length}) = '${escapeFormula(DEMO_PREFIX)}'`,
      pageSize: 100,
    })
    .eachPage((records, next) => {
      bdrIds.push(...records.map((r) => r.id));
      next();
    });
  for (let i = 0; i < bdrIds.length; i += 10) {
    const chunk = bdrIds.slice(i, i + 10);
    if (chunk.length) await base(BDR_TABLE).destroy(chunk);
  }
  console.log(`Deleted ${bdrIds.length} KPI demo Brand Deal Request(s).`);

  const actIds = [];
  await base(ACTIVITY_LOG_TABLE)
    .select({
      filterByFormula: `FIND('${escapeFormula(SEED_MARKER)}', {Details})`,
      pageSize: 100,
    })
    .eachPage((records, next) => {
      actIds.push(...records.map((r) => r.id));
      next();
    });
  for (let i = 0; i < actIds.length; i += 10) {
    const chunk = actIds.slice(i, i + 10);
    if (chunk.length) await base(ACTIVITY_LOG_TABLE).destroy(chunk);
  }
  if (actIds.length) console.log(`Deleted ${actIds.length} KPI demo activity row(s).`);
}

async function resolveDealIds(base) {
  const envIds = (process.env.KPI_SEED_DEAL_IDS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (envIds.length > 0) return envIds;

  const records = await base(DEALS_TABLE).select({ pageSize: 50 }).firstPage();
  return records.map((r) => r.id);
}

const ACTIVITY_ACTION_FALLBACKS = [
  "Request Sent",
  "Notes updated",
  "Marked interested",
  "Information requested",
  "Follow-up scheduled",
  "NDA Sent",
  "Deal Room Active",
  "Pre-LOI",
  "Finalist",
  "Declined",
];

async function createActivity(base, dealId, brandName, action, details, daysAgo) {
  const attempts = [action, ...ACTIVITY_ACTION_FALLBACKS.filter((a) => a !== action)];
  let lastErr;
  for (const act of attempts) {
    const fields = {
      Deal: [dealId],
      "Brand Name": brandName,
      Action: act,
      Details: `${SEED_MARKER} ${details}`,
      "Created At": isoDaysAgo(daysAgo),
    };
    try {
      await base(ACTIVITY_LOG_TABLE).create([{ fields }]);
      return;
    } catch (e) {
      lastErr = e;
      const msg = String(e?.message || e);
      if (/Unknown field|does not exist|Stakeholder/i.test(msg) && fields.Stakeholder != null) {
        delete fields.Stakeholder;
        try {
          await base(ACTIVITY_LOG_TABLE).create([{ fields }]);
          return;
        } catch (e2) {
          lastErr = e2;
        }
      } else if (/Created At|READONLY|cannot/i.test(msg)) {
        delete fields["Created At"];
        try {
          await base(ACTIVITY_LOG_TABLE).create([{ fields }]);
          return;
        } catch (e2) {
          lastErr = e2;
        }
      }
      if (!/Insufficient permissions to create new select option|INVALID_MULTIPLE_CHOICE/i.test(msg)) {
        throw e;
      }
    }
  }
  throw lastErr;
}

function seedKpiHistoryFile(brandSnap) {
  const scopeKey = "v2|brand|";
  const wk = isoWeekKey(new Date());
  const prev = prevIsoWeekKey(wk);
  if (!prev) return;

  const prevSnap = {
    persona: "brand",
    needsAction: (brandSnap.needsAction ?? 0) + 1,
    awaitingCounterparty: Math.max(0, (brandSnap.awaitingCounterparty ?? 0) - 1),
    atRisk: brandSnap.atRisk ?? 0,
    newRolling7d: Math.max(0, (brandSnap.newRolling7d ?? 0) - 2),
    inReview: brandSnap.inReview ?? 0,
    pipeline: brandSnap.pipeline,
  };

  let store = {};
  try {
    if (fs.existsSync(KPI_HISTORY_FILE)) {
      store = JSON.parse(fs.readFileSync(KPI_HISTORY_FILE, "utf8"));
    }
  } catch {
    store = {};
  }
  if (!store[scopeKey] || typeof store[scopeKey] !== "object") store[scopeKey] = {};
  store[scopeKey][prev] = prevSnap;
  store[scopeKey][wk] = brandSnap;
  fs.mkdirSync(path.dirname(KPI_HISTORY_FILE), { recursive: true });
  fs.writeFileSync(KPI_HISTORY_FILE, JSON.stringify(store, null, 2), "utf8");
  console.log(`[kpi-history] Wrote week ${prev} (prev) + ${wk} (cur) → ${path.relative(ROOT, KPI_HISTORY_FILE)}`);
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const clean = process.argv.includes("--clean");
  const specs = buildKpiTargetSpecs();
  const report = verifyTargets(specs);
  printReport(report);

  if (dryRun) {
    console.log(`Dry run: would upsert ${specs.length} Brand Deal Requests (${DEMO_PREFIX}*).`);
    console.log("Run without --dry-run to write to Airtable.");
    return;
  }

  const base = getBase();

  if (clean) {
    await deleteDemoRows(base);
    console.log("Clean complete.");
    return;
  }

  let dealIds = await resolveDealIds(base);
  if (dealIds.length === 0) {
    console.error(`No deals in "${DEALS_TABLE}". Create deals or set KPI_SEED_DEAL_IDS.`);
    process.exit(1);
  }
  if (dealIds.length < specs.length) {
    console.warn(`Only ${dealIds.length} deal(s); reusing IDs in rotation for ${specs.length} BDR rows.`);
  }

  let created = 0;
  let updated = 0;
  let failed = 0;

  for (let i = 0; i < specs.length; i++) {
    const spec = specs[i];
    const dealId = dealIds[i % dealIds.length];
    const payload = {
      Deal: [dealId],
      "Brand Name": spec.brand,
      ...spec.fields,
    };
    const existing = await findBdrByBrand(base, spec.brand);
    try {
      if (existing) {
        await base(BDR_TABLE).update([
          { id: existing.id, fields: { Deal: [dealId], ...spec.fields } },
        ]);
        updated += 1;
        console.log(`[update] ${spec.label} → ${existing.id}`);
      } else {
        const [rec] = await base(BDR_TABLE).create([{ fields: payload }]);
        created += 1;
        console.log(`[create] ${spec.label} → ${rec.id}`);
      }
    } catch (e) {
      failed += 1;
      console.error(`[fail] ${spec.label}:`, e.message || e);
    }
  }

  for (const spec of specs.filter((s) => ["New-1", "Review-1", "Terms-1", "Await-1"].some((x) => s.brand.endsWith(x))) ) {
    const dealId = dealIds[0];
    try {
      await createActivity(
        base,
        dealId,
        spec.brand,
        "Request Sent",
        `Owner outreach for ${spec.label}.`,
        2
      );
      await createActivity(
        base,
        dealId,
        spec.brand,
        "Notes updated",
        "Brand opened workspace and reviewed fit.",
        1
      );
    } catch (e) {
      console.warn(`[activity] ${spec.brand}:`, e.message || e);
    }
  }

  try {
    const snap = computeWorkspaceKpiSnapshot(specsToVerifyRows(specs), "brand");
    seedKpiHistoryFile(snap);
  } catch (e) {
    console.warn("[kpi-history] file seed skipped:", e.message || e);
  }

  console.log(`\nDone. created=${created} updated=${updated} failed=${failed}`);
  console.log(`Reload My Deals / Brand Development Workspace (filter or scan for "${DEMO_PREFIX}").`);
  console.log(`Remove: node scripts/seed-workspace-kpi-dashboard-targets.mjs --clean`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
