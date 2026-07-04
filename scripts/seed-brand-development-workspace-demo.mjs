/**
 * Seed realistic Brand Deal Requests (+ sample Deal Activity Log rows) so the
 * Brand Development Workspace tabs (new, active review, awaiting info, NDA/room,
 * terms, advanced, declined/archived, activity) have data to review.
 *
 * Prerequisites:
 * - AIRTABLE_API_KEY, AIRTABLE_BASE_ID in .env (same as the app server).
 * - At least one deal in "Deals" (or AIRTABLE_TABLE_DEALS). Prefer 8+ deals so
 *   each demo row can use a different property; otherwise deals are reused in rotation.
 * - Brand Deal Requests single-selects include statuses used here (see api/brand-deal-requests.js).
 *
 * Usage:
 *   node scripts/seed-brand-development-workspace-demo.mjs
 *   node scripts/seed-brand-development-workspace-demo.mjs --clean   # delete prior BDD Demo* requests only
 *
 * Optional env:
 *   BDD_SEED_DEAL_IDS=recAAA,recBBB   # force which deal record IDs to use (first N in order).
 */

import "dotenv/config";
import Airtable from "airtable";

const BDR_TABLE = process.env.AIRTABLE_TABLE_BRAND_DEAL_REQUESTS || "Brand Deal Requests";
const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";
const ACTIVITY_LOG_TABLE = process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log";

const DEMO_PREFIX = "BDD Demo";

function escapeFormula(s) {
  return String(s ?? "")
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'");
}

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env");
  }
  return new Airtable({ apiKey }).base(baseId);
}

async function findBdrByDealAndBrand(base, dealId, brandName) {
  const formula = `AND(FIND('${escapeFormula(dealId)}', ARRAYJOIN({Deal})) > 0, {Brand Name} = '${escapeFormula(brandName)}')`;
  const rows = await base(BDR_TABLE)
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

async function deleteDemoBdrs(base) {
  const toDelete = [];
  await base(BDR_TABLE)
    .select({
      filterByFormula: `LEFT({Brand Name}, ${DEMO_PREFIX.length}) = '${escapeFormula(DEMO_PREFIX)}'`,
      pageSize: 100,
    })
    .eachPage((records, fetchNextPage) => {
      toDelete.push(...records.map((r) => r.id));
      fetchNextPage();
    });
  if (toDelete.length === 0) {
    console.log("No demo Brand Deal Requests to delete.");
    return;
  }
  for (let i = 0; i < toDelete.length; i += 10) {
    const chunk = toDelete.slice(i, i + 10);
    await base(BDR_TABLE).destroy(chunk);
    console.log("Deleted BDR ids:", chunk.join(", "));
  }
}

async function deleteDemoActivity(base) {
  const toDelete = [];
  await base(ACTIVITY_LOG_TABLE)
    .select({
      filterByFormula: `LEFT({Brand Name}, ${DEMO_PREFIX.length}) = '${escapeFormula(DEMO_PREFIX)}'`,
      pageSize: 100,
    })
    .eachPage((records, fetchNextPage) => {
      toDelete.push(...records.map((r) => r.id));
      fetchNextPage();
    });
  for (let i = 0; i < toDelete.length; i += 10) {
    const chunk = toDelete.slice(i, i + 10);
    if (chunk.length) await base(ACTIVITY_LOG_TABLE).destroy(chunk);
  }
  if (toDelete.length) console.log("Deleted demo activity rows:", toDelete.length);
}

function isoDaysFromNow(days) {
  const d = new Date();
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10);
}

const now = new Date().toISOString();

/** Each row: stable Brand Name (for re-run upsert), human label, Airtable fields (excluding Deal + Brand Name). */
function demoSpecs() {
  return [
    {
      brand: `${DEMO_PREFIX} New`,
      label: "New / intake",
      fields: {
        Status: "New",
        "Match Score": 74,
        "Request Sent At": now,
        "Created At": now,
        "Last Updated": now,
      },
    },
    {
      brand: `${DEMO_PREFIX} ActiveReview`,
      label: "Brand review (viewed)",
      fields: {
        Status: "Brand Viewed",
        "Match Score": 69,
        "Request Sent At": now,
        "Created At": now,
        "Last Updated": now,
      },
    },
    {
      brand: `${DEMO_PREFIX} AwaitingInfo`,
      label: "Engaged — awaiting owner info (accepted, NDA not blocking)",
      fields: {
        Status: "Accepted",
        "NDA Status": "Not Required",
        "Match Score": 82,
        "Request Sent At": now,
        "Created At": now,
        "Last Updated": now,
      },
    },
    {
      brand: `${DEMO_PREFIX} NdaPending`,
      label: "NDA / room — NDA not yet sent",
      fields: {
        Status: "Accepted",
        "NDA Status": "Not Sent",
        "Deal Room Access": "Blocked",
        "Match Score": 77,
        "Request Sent At": now,
        "Created At": now,
        "Last Updated": now,
      },
    },
    {
      brand: `${DEMO_PREFIX} NdaSent`,
      label: "NDA / room — NDA sent, awaiting signature",
      fields: {
        Status: "Accepted",
        "NDA Status": "Sent",
        "NDA Sent At": now,
        "Deal Room Access": "Blocked",
        "Match Score": 80,
        "Request Sent At": now,
        "Created At": now,
        "Last Updated": now,
      },
    },
    {
      brand: `${DEMO_PREFIX} DealRoom`,
      label: "NDA / room — deal room active",
      fields: {
        Status: "Deal Room Active",
        "NDA Status": "Signed - Owner Confirmed",
        "NDA Signed At": now,
        "Deal Room Access": "Granted",
        "Access Granted At": now,
        "Match Score": 85,
        "Request Sent At": now,
        "Created At": now,
        "Last Updated": now,
      },
    },
    {
      brand: `${DEMO_PREFIX} Terms`,
      label: "Terms / proposal — Pre-LOI + draft",
      fields: {
        Status: "Pre-LOI",
        "NDA Status": "Signed - Owner Confirmed",
        "NDA Signed At": now,
        "Deal Room Access": "Granted",
        "Access Granted At": now,
        "Proposal Status": "Draft",
        "Match Score": 88,
        "Request Sent At": now,
        "Created At": now,
        "Last Updated": now,
      },
    },
    {
      brand: `${DEMO_PREFIX} Advanced`,
      label: "Finalist / advanced pipeline",
      fields: {
        Status: "Finalist",
        "NDA Status": "Signed - Owner Confirmed",
        "NDA Signed At": now,
        "Deal Room Access": "Granted",
        "Access Granted At": now,
        "Proposal Status": "Submitted",
        "Match Score": 91,
        "Request Sent At": now,
        "Created At": now,
        "Last Updated": now,
      },
    },
    {
      brand: `${DEMO_PREFIX} Declined`,
      label: "Declined / closed",
      fields: {
        Status: "Declined",
        "Response Date": now,
        "Response Notes": `${DEMO_PREFIX} — Owner prioritizing flagged market; brand paused for this cycle.`,
        "Match Score": 62,
        "Request Sent At": now,
        "Created At": now,
        "Last Updated": now,
      },
    },
    {
      brand: `${DEMO_PREFIX} Archived`,
      label: "Archived",
      fields: {
        Status: "Archived",
        "Response Notes": `${DEMO_PREFIX} — Archived after owner selected another operator path.`,
        "Match Score": 58,
        "Request Sent At": now,
        "Created At": now,
        "Last Updated": now,
      },
    },
    {
      brand: `${DEMO_PREFIX} MoreInfo`,
      label: "More info requested",
      fields: {
        Status: "More Info Requested",
        "Response Notes": `${DEMO_PREFIX} — Requested: capex phasing, flag ownership entity, and indicative opening window.`,
        "Match Score": 76,
        "Request Sent At": now,
        "Created At": now,
        "Last Updated": now,
      },
    },
    {
      brand: `${DEMO_PREFIX} Revisit`,
      label: "Revisit later (parked)",
      fields: {
        Status: "Revisit Later",
        "Next Follow-up Date": isoDaysFromNow(45),
        "Next Follow-up Header": "Revisit — zoning update expected",
        "Next Follow-up Notes": `${DEMO_PREFIX} — City council vote Q3; hold until clarity on height cap.`,
        "Response Notes": `${DEMO_PREFIX} — Parked pending municipality guidance.`,
        "Match Score": 73,
        "Request Sent At": now,
        "Created At": now,
        "Last Updated": now,
      },
    },
  ];
}

async function createActivity(base, dealId, brandName, action, details, stakeholder = "Brand") {
  const fields = {
    Deal: [dealId],
    "Brand Name": brandName,
    Action: action,
    Details: details,
    "Created At": now,
  };
  if (stakeholder) fields.Stakeholder = stakeholder;
  try {
    await base(ACTIVITY_LOG_TABLE).create([{ fields }]);
  } catch (e) {
    const msg = String(e?.message || e);
    if (/Unknown field|does not exist/i.test(msg) && fields.Stakeholder) {
      delete fields.Stakeholder;
      await base(ACTIVITY_LOG_TABLE).create([{ fields }]);
    } else {
      throw e;
    }
  }
}

async function seedActivityForFirstDeal(base, dealId) {
  const brand = `${DEMO_PREFIX} New`;
  await createActivity(base, dealId, brand, "Request Sent", "Owner outreach sent to brand team for airport corridor site.", "Owner");
  await createActivity(base, dealId, brand, "Opportunity reviewed", "Brand opened workspace and reviewed fit summary.", "Brand");
  await createActivity(base, dealId, brand, "Information requested", "Asked owner to confirm room count and capex envelope.", "Brand");
  await createActivity(base, dealId, brand, "Follow-up scheduled", "Next touch: call with owner advisor — 30 minutes.", "Brand");
}

async function resolveDealIds(base) {
  const envIds = (process.env.BDD_SEED_DEAL_IDS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (envIds.length > 0) return envIds;

  const records = await base(DEALS_TABLE).select({ pageSize: 30 }).firstPage();
  return records.map((r) => r.id);
}

async function main() {
  const base = getBase();
  const clean = process.argv.includes("--clean");

  if (clean) {
    await deleteDemoActivity(base);
    await deleteDemoBdrs(base);
    console.log("Clean complete. Run without --clean to insert demo data.");
    return;
  }

  let dealIds = await resolveDealIds(base);
  if (dealIds.length === 0) {
    console.error(`No deals found in "${DEALS_TABLE}". Create deals first, or set BDD_SEED_DEAL_IDS.`);
    process.exit(1);
  }
  if (dealIds.length < 4) {
    console.warn(`Only ${dealIds.length} deal(s): demo rows will reuse deal IDs in rotation.`);
  }

  const specs = demoSpecs();
  const usedDealIds = [];

  for (let i = 0; i < specs.length; i++) {
    const spec = specs[i];
    const dealId = dealIds[i % dealIds.length];
    usedDealIds.push(dealId);
    const payload = {
      Deal: [dealId],
      "Brand Name": spec.brand,
      ...spec.fields,
    };
    const existing = await findBdrByDealAndBrand(base, dealId, spec.brand);
    try {
      if (existing) {
        await base(BDR_TABLE).update([{ id: existing.id, fields: spec.fields }]);
        console.log(`[update] ${spec.label} → ${existing.id} (deal ${dealId.slice(0, 8)}…)`);
      } else {
        const [created] = await base(BDR_TABLE).create([{ fields: payload }]);
        console.log(`[create] ${spec.label} → ${created.id} (deal ${dealId.slice(0, 8)}…)`);
      }
    } catch (e) {
      console.error(`[fail] ${spec.label} (${spec.brand}):`, e.message || e);
      console.error(
        "  → Check Airtable single-select options (Status, NDA Status, Deal Room Access, Proposal Status) match api/brand-deal-requests.js pipeline."
      );
    }
  }

  const firstDeal = dealIds[0];
  try {
    await seedActivityForFirstDeal(base, firstDeal);
    console.log(`[activity] Sample timeline rows on deal ${firstDeal.slice(0, 8)}… (${DEMO_PREFIX} New)`);
  } catch (e) {
    console.warn("[activity] Skipped or partial:", e.message || e);
  }

  console.log("\nDone. Reload Brand Development Workspace (all contacted projects).");
  console.log(`To remove demo data: node scripts/seed-brand-development-workspace-demo.mjs --clean`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
