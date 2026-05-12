/**
 * Seed Brand Deal Requests + Deal Activity Log using REAL deals and (when available)
 * preferred brand names from each deal — so My Brand Deals / Deal Log tabs show plausible data.
 *
 * Prerequisites:
 *   - .env: AIRTABLE_API_KEY, AIRTABLE_BASE_ID (same as server)
 *   - Tables: "Brand Deal Requests", "Deal Activity Log", "Deals" (or env overrides)
 *   - Status / NDA / Deal Room / Proposal Status single-select options must match your base
 *     (same set as scripts/seed-brand-development-workspace-demo.mjs / api/brand-deal-requests.js).
 *
 * Usage:
 *   node scripts/seed-bdr-activity-from-existing-deals.mjs
 *   node scripts/seed-bdr-activity-from-existing-deals.mjs --dry-run
 *   node scripts/seed-bdr-activity-from-existing-deals.mjs --clean   # remove only rows tagged [seed:bdd-v2]
 *
 * Env (optional):
 *   SEED_MAX_DEALS=35              max deals to scan (default 40)
 *   SEED_BRANDS_PER_DEAL=2       BDRs to create per deal (default 2), uses preferred brands first
 *   SEED_DEAL_IDS=recA,recB      force these deal IDs only (overrides max scan)
 *   SEED_SKIP_EXISTING=1         skip deal+brand if a BDR already exists (default 1)
 */

import "dotenv/config";
import Airtable from "airtable";

const BDR_TABLE = process.env.AIRTABLE_TABLE_BRAND_DEAL_REQUESTS || "Brand Deal Requests";
const ACTIVITY_LOG_TABLE = process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log";
const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";

const SEED_MARKER = "[seed:bdd-v2]";

const FALLBACK_BRANDS = [
  "Marriott",
  "Hilton",
  "Hyatt",
  "IHG Hotels & Resorts",
  "Accor",
  "Wyndham Hotels & Resorts",
  "Choice Hotels",
  "Best Western",
  "Radisson Hotel Group",
  "Rosewood",
];

/** Pipeline slices so different deals surface across workspace tabs. */
const STATUS_TEMPLATES = [
  {
    label: "New",
    fields: { Status: "New", "NDA Status": "Not Sent", "Deal Room Access": "Blocked" },
  },
  {
    label: "Brand viewed",
    fields: { Status: "Brand Viewed", "NDA Status": "Not Sent", "Deal Room Access": "Blocked" },
  },
  {
    label: "Accepted / NDA optional",
    fields: { Status: "Accepted", "NDA Status": "Not Required", "Deal Room Access": "Blocked" },
  },
  {
    label: "NDA sent",
    fields: { Status: "Accepted", "NDA Status": "Sent", "Deal Room Access": "Blocked" },
  },
  {
    label: "Deal room active",
    fields: {
      Status: "Deal Room Active",
      "NDA Status": "Signed - Owner Confirmed",
      "Deal Room Access": "Granted",
    },
  },
  {
    label: "Pre-LOI draft",
    fields: {
      Status: "Pre-LOI",
      "NDA Status": "Signed - Owner Confirmed",
      "Deal Room Access": "Granted",
      "Proposal Status": "Draft",
    },
  },
  {
    label: "Finalist",
    fields: {
      Status: "Finalist",
      "NDA Status": "Signed - Owner Confirmed",
      "Deal Room Access": "Granted",
      "Proposal Status": "Submitted",
    },
  },
  {
    label: "More info",
    fields: {
      Status: "More Info Requested",
      "NDA Status": "Not Sent",
      "Response Notes": "Please share latest feasibility and indicative key terms.",
    },
  },
  {
    label: "Revisit later",
    fields: {
      Status: "Revisit Later",
      "NDA Status": "Not Sent",
      "Next Follow-up Date": isoDaysFromNow(60),
      "Next Follow-up Header": "Check back after market update",
      "Next Follow-up Notes": "Parked pending sponsor guidance.",
    },
  },
  {
    label: "Declined",
    fields: {
      Status: "Declined",
      "NDA Status": "Not Required",
      "Response Notes": "Passing on this cycle — portfolio fit.",
    },
  },
];

function isoDaysFromNow(days) {
  const d = new Date();
  d.setDate(d.getDate() + Number(days) || 0);
  return d.toISOString().slice(0, 10);
}

function isoDaysAgo(days) {
  const d = new Date();
  d.setDate(d.getDate() - Number(days) || 0);
  return d.toISOString();
}

function escapeFormula(s) {
  return String(s ?? "")
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'");
}

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env");
  return new Airtable({ apiKey }).base(baseId);
}

function fieldToString(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (Array.isArray(v)) {
    return v
      .map((x) => (typeof x === "string" ? x : x && typeof x === "object" && x.name ? String(x.name) : String(x)))
      .map((s) => s.trim())
      .filter(Boolean)
      .join(", ");
  }
  if (typeof v === "object" && v.name) return String(v.name).trim();
  return String(v).trim();
}

function parseBrandNames(raw) {
  const s = fieldToString(raw);
  if (!s) return [];
  const parts = s
    .split(/[,;]|(?:\s+and\s+)/i)
    .map((x) => x.trim())
    .filter(Boolean);
  const seen = new Set();
  const out = [];
  for (const p of parts) {
    const key = p.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(p);
    if (out.length >= 6) break;
  }
  return out;
}

function markOwnerNote(baseText) {
  const t = String(baseText || "").trim();
  return t ? `${t} ${SEED_MARKER}` : SEED_MARKER;
}

async function findBdr(base, dealId, brandName) {
  const formula = `AND(FIND('${escapeFormula(dealId)}', ARRAYJOIN({Deal})) > 0, {Brand Name} = '${escapeFormula(brandName)}')`;
  const rows = await base(BDR_TABLE).select({ filterByFormula: formula, maxRecords: 1 }).firstPage();
  return rows[0] || null;
}

async function deleteSeededBdrs(base) {
  const toDelete = [];
  const formula = `FIND('${escapeFormula(SEED_MARKER)}', {Owner Notes})`;
  await base(BDR_TABLE)
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((records, next) => {
      toDelete.push(...records.map((r) => r.id));
      next();
    });
  for (let i = 0; i < toDelete.length; i += 10) {
    const chunk = toDelete.slice(i, i + 10);
    if (chunk.length) await base(BDR_TABLE).destroy(chunk);
  }
  console.log(`Deleted ${toDelete.length} seeded Brand Deal Request(s).`);
}

async function deleteSeededActivity(base) {
  const toDelete = [];
  const formula = `FIND('${escapeFormula(SEED_MARKER)}', {Details})`;
  await base(ACTIVITY_LOG_TABLE)
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((records, next) => {
      toDelete.push(...records.map((r) => r.id));
      next();
    });
  for (let i = 0; i < toDelete.length; i += 10) {
    const chunk = toDelete.slice(i, i + 10);
    if (chunk.length) await base(ACTIVITY_LOG_TABLE).destroy(chunk);
  }
  console.log(`Deleted ${toDelete.length} seeded Deal Activity Log row(s).`);
}

async function createActivity(base, fields) {
  try {
    await base(ACTIVITY_LOG_TABLE).create([{ fields }]);
  } catch (e) {
    const msg = String(e?.message || e);
    if (/Unknown field|does not exist/i.test(msg) && fields.Stakeholder != null) {
      const f2 = { ...fields };
      delete f2.Stakeholder;
      await base(ACTIVITY_LOG_TABLE).create([{ fields: f2 }]);
    } else if (/Created At|READONLY|cannot/i.test(msg)) {
      const f2 = { ...fields };
      delete f2["Created At"];
      await base(ACTIVITY_LOG_TABLE).create([{ fields: f2 }]);
    } else {
      throw e;
    }
  }
}

async function seedActivityTimeline(base, dealId, brandName, templateLabel, status) {
  const detailsSuffix = ` ${SEED_MARKER}`;
  const mk = (action, details, stakeholder, daysAgo) => ({
    Deal: [dealId],
    "Brand Name": brandName,
    Action: action,
    Details: String(details) + detailsSuffix,
    Stakeholder: stakeholder,
    "Created At": isoDaysAgo(daysAgo),
  });

  await createActivity(
    base,
    mk("Request Sent", `Intro sent for ${templateLabel} track.`, "Owner", 18),
  );

  if (status === "New") return;

  await createActivity(
    base,
    mk("Opportunity reviewed", "Brand team reviewed project summary and match context.", "Brand", 16),
  );

  if (["Accepted", "Brand Viewed", "More Info Requested", "Revisit Later", "Declined"].includes(status)) {
    await createActivity(
      base,
      mk(
        status === "Declined" ? "Declined" : "Marked interested",
        status === "More Info Requested"
          ? "Brand requested additional underwriting inputs."
          : status === "Revisit Later"
            ? "Brand asked to revisit next quarter."
            : status === "Declined"
              ? "Brand declined for this opportunity."
              : "Brand confirmed interest to proceed.",
        "Brand",
        14,
      ),
    );
  }

  if (["Deal Room Active", "Pre-LOI", "Finalist"].includes(status)) {
    await createActivity(
      base,
      mk("NDA Sent", "Standard mutual NDA issued for data room review.", "Owner", 12),
    );
    await createActivity(
      base,
      mk("NDA Signed (Owner Confirmed)", "Countersigned NDA on file.", "Owner", 10),
    );
    await createActivity(
      base,
      mk("Deal Room Access Granted", "VDR access enabled for brand diligence team.", "Owner", 9),
    );
  }

  if (status === "Pre-LOI" || status === "Finalist") {
    await createActivity(
      base,
      mk(
        "Terms preparation started",
        status === "Finalist" ? "Shortlisted for final comparison." : "Draft commercial terms under review.",
        "Brand",
        7,
      ),
    );
  }
}

async function fetchDealIds(base) {
  const env = (process.env.SEED_DEAL_IDS || "")
    .split(",")
    .map((s) => s.trim())
    .filter((id) => id.startsWith("rec"));
  if (env.length) return env;

  const max = Math.min(100, Math.max(1, parseInt(process.env.SEED_MAX_DEALS || "40", 10) || 40));
  const ids = [];
  await base(DEALS_TABLE)
    .select({
      pageSize: 100,
      fields: ["Property Name", "Preferred Brands (up to 4)", "Preferred Brands"],
    })
    .eachPage((records, next) => {
      for (const r of records) {
        ids.push(r.id);
        if (ids.length >= max) break;
      }
      next();
    });
  return ids.slice(0, max);
}

async function fetchDealRecord(base, dealId) {
  const rec = await base(DEALS_TABLE).find(dealId);
  return rec;
}

async function main() {
  const dry = process.argv.includes("--dry-run");
  const clean = process.argv.includes("--clean");
  const base = getBase();

  if (clean) {
    await deleteSeededActivity(base);
    await deleteSeededBdrs(base);
    console.log("Clean complete.");
    return;
  }

  const brandsPerDeal = Math.min(4, Math.max(1, parseInt(process.env.SEED_BRANDS_PER_DEAL || "2", 10) || 2));
  const skipExisting = (process.env.SEED_SKIP_EXISTING || "1") !== "0";

  const dealIds = await fetchDealIds(base);
  if (dealIds.length === 0) {
    console.error(`No deals found in "${DEALS_TABLE}".`);
    process.exit(1);
  }
  console.log(`Using ${dealIds.length} deal(s). dry-run=${dry}`);

  let createdBdr = 0;
  let skipped = 0;
  let failed = 0;
  let fb = 0;

  for (let di = 0; di < dealIds.length; di++) {
    const dealId = dealIds[di];
    let dealRec;
    try {
      dealRec = await fetchDealRecord(base, dealId);
    } catch (e) {
      console.warn(`Skip deal ${dealId}:`, e.message);
      continue;
    }
    const f = dealRec.fields || {};
    const prop = fieldToString(f["Property Name"]) || "Project";
    let brands = parseBrandNames(f["Preferred Brands (up to 4)"] || f["Preferred Brands"]);
    while (brands.length < brandsPerDeal) {
      brands.push(FALLBACK_BRANDS[fb % FALLBACK_BRANDS.length]);
      fb++;
    }
    brands = brands.slice(0, brandsPerDeal);

    for (let bi = 0; bi < brands.length; bi++) {
      const brandName = brands[bi];
      const tmpl = STATUS_TEMPLATES[(di + bi) % STATUS_TEMPLATES.length];
      const status = tmpl.fields.Status;

      if (skipExisting) {
        const ex = await findBdr(base, dealId, brandName);
        if (ex) {
          skipped++;
          if (di < 3) console.log(`  skip existing BDR ${brandName} @ ${dealId.slice(0, 10)}…`);
          continue;
        }
      }

      const now = new Date().toISOString();
      const matchScore = 62 + ((di * 7 + bi * 13) % 28);
      const bdrFields = {
        Deal: [dealId],
        "Brand Name": brandName,
        "Match Score": matchScore,
        "Request Sent At": isoDaysAgo(18 - bi),
        "Created At": isoDaysAgo(18 - bi),
        "Last Updated": now,
        "Owner Notes": markOwnerNote(`Seeded for "${prop}" — ${tmpl.label}.`),
        ...tmpl.fields,
      };

      if (tmpl.fields.Status === "Declined" || tmpl.fields.Status === "More Info Requested" || tmpl.fields.Status === "Revisit Later") {
        if (!bdrFields["Response Notes"]) bdrFields["Response Notes"] = markOwnerNote(`Auto seed — ${tmpl.label}.`);
        else bdrFields["Response Notes"] = markOwnerNote(bdrFields["Response Notes"]);
      }
      if (tmpl.fields["NDA Status"] === "Sent") {
        bdrFields["NDA Sent At"] = isoDaysAgo(11);
      }
      if (tmpl.fields["NDA Status"] === "Signed - Owner Confirmed") {
        bdrFields["NDA Signed At"] = isoDaysAgo(9);
      }
      if (tmpl.fields["Deal Room Access"] === "Granted") {
        bdrFields["Access Granted At"] = isoDaysAgo(8);
      }
      if (tmpl.fields.Status === "Declined") {
        bdrFields["Response Date"] = isoDaysAgo(5);
      }

      if (dry) {
        console.log(`[dry-run] ${prop} | ${brandName} | ${tmpl.label}`);
        continue;
      }

      try {
        let created;
        try {
          [created] = await base(BDR_TABLE).create([{ fields: bdrFields }]);
        } catch (e1) {
          const msg = String(e1?.message || e1);
          if (/Created At|READONLY|cannot|unknown field/i.test(msg)) {
            const f2 = { ...bdrFields };
            delete f2["Created At"];
            [created] = await base(BDR_TABLE).create([{ fields: f2 }]);
          } else {
            throw e1;
          }
        }
        createdBdr++;
        console.log(`[bdr] ${created.id} ${brandName} / ${dealId.slice(0, 10)}… / ${tmpl.label}`);
        await seedActivityTimeline(base, dealId, brandName, tmpl.label, status);
      } catch (e) {
        failed++;
        console.error(`[fail] ${prop} | ${brandName}:`, e.message || e);
        if (/Unknown field|invalid|select|permission/i.test(String(e.message))) {
          console.error(
            "  → Align Airtable single-select options with api/brand-deal-requests.js (Status, NDA Status, Deal Room Access, Proposal Status).",
          );
        }
      }
    }
  }

  console.log("\nSummary:", { createdBdr, skipped, failed, dry });
  if (!dry) {
    console.log(`Remove seeded rows: node scripts/seed-bdr-activity-from-existing-deals.mjs --clean`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
