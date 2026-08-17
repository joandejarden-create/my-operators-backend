/**
 * Seed submitted LOI/proposal data on Brand Deal Requests for Deal Compare demo.
 *
 * Creates or updates 4 realistic submitted proposals on one deal (default: Hilton Garden Inn Medellin)
 * so My Deals → Deal Compare shows a full side-by-side comparison table.
 *
 * Prerequisites: AIRTABLE_API_KEY, AIRTABLE_BASE_ID in .env
 *
 * Usage:
 *   node scripts/seed-deal-compare-proposals.mjs --dry-run
 *   node scripts/seed-deal-compare-proposals.mjs
 *   node scripts/seed-deal-compare-proposals.mjs --clean
 *   node scripts/seed-deal-compare-proposals.mjs --demote-empty-kpi-demo
 *
 * Env:
 *   DEAL_COMPARE_SEED_DEAL_ID=recXXX     force deal record ID
 *   DEAL_COMPARE_SEED_DEAL_NAME=Medellin   substring match on deal name (default: Medellin)
 */

import "dotenv/config";
import Airtable from "airtable";

const BDR_TABLE = process.env.AIRTABLE_TABLE_BRAND_DEAL_REQUESTS || "Brand Deal Requests";
const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";
const SEED_MARKER = "[seed:deal-compare-proposals]";

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

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env");
  }
  return new Airtable({ apiKey }).base(baseId);
}

/** Shared pipeline fields for submitted LOI-stage requests. */
function baseBdrFields(submittedDaysAgo, matchScore) {
  const submittedAt = isoDaysAgo(submittedDaysAgo);
  return {
    Status: "Pre-LOI",
    "NDA Status": "Signed - Owner Confirmed",
    "NDA Signed At": isoDaysAgo(submittedDaysAgo + 14),
    "Deal Room Access": "Granted",
    "Access Granted At": isoDaysAgo(submittedDaysAgo + 12),
    "Proposal Status": "Submitted",
    "Proposal Submitted At": submittedAt,
    "Match Score": matchScore,
    "Request Sent At": isoDaysAgo(submittedDaysAgo + 21),
    "Last Updated": submittedAt,
    "Owner Notes": `${SEED_MARKER} — Deal Compare demo LOI submission`,
  };
}

/**
 * Four varied franchise proposals for an urban Colombia select-service new-build (~140 keys).
 * Brand names must match Brand Setup - Brand Basics for library fallback (loyalty, footprint, etc.).
 */
export function buildDealCompareProposalSpecs() {
  return [
    {
      brand: "Hilton Garden Inn",
      label: "HGI — strongest key money, moderate fees",
      fields: {
        ...baseBdrFields(8, 88),
        "Proposal Agreement Type": "Franchise",
        "Proposal Initial Term Quantity": 1,
        "Proposal Initial Term Length": 20,
        "Proposal Initial Term Duration": "Years",
        "Proposal Renewal Option Quantity": 2,
        "Proposal Renewal Option Length": 5,
        "Proposal Renewal Option Duration": "Years",
        "Proposal Renewal Conditions": "Automatic renewal if in good standing and no uncured defaults",
        "Proposal Renewal Options":
          "2 x 5 Years. Automatic renewal if in good standing and no uncured defaults",
        "Proposal Royalty Pct": 5.5,
        "Proposal Royalty Basis": "% of Rooms Revenue",
        "Proposal Marketing Pct": 2.5,
        "Proposal Marketing Basis": "% of Gross Revenue",
        "Proposal Application Fee": 75000,
        "Proposal Application Fee Basis": "Per Property",
        "Proposal Initial Franchise Fee": 75000,
        "Proposal Initial Franchise Fee Basis": "Per Property",
        "Proposal Reservation Basis": "Per Reservation / Per Booking",
        "Proposal Reservation Basis Other": "2.5",
        "Proposal Tech Platform Fees": "85",
        "Proposal Tech Fee Basis": "Per Room / Year",
        "Proposal Training Fees": "12000",
        "Proposal Training Fee Basis": "One-Time",
        "Proposal Key Money": "Yes",
        "Proposal Key Money Amount": 500000,
        "Proposal Key Money Terms": "50% at franchise agreement execution; 50% at certificate of occupancy",
        "Proposal PIP Capex": "$18,000/room (est.) — lobby, guestroom soft goods, F&B refresh",
        "Proposal Territorial Restriction":
          "Exclusive 3-mile radius from subject site; no additional HGI in El Poblado submarket during initial term",
        "Proposal Incentive Types": [
          "Territorial Exclusivity / Radius",
          "Reduced Royalty Period",
          "Application Fee Credit",
        ],
        "Proposal Incentive Details": [
          "Territorial Exclusivity / Radius: 3-mile exclusive radius in El Poblado",
          "Reduced Royalty Period: 4.0% royalty for first 12 operating months",
          "Application Fee Credit: $25,000 credit applied to initial franchise fee",
        ].join("\n"),
        "Proposal Approval Timeline": "45–60 days from complete submission to franchise approval",
        "Proposal Brand Standards Flexibility":
          "Standard HGI prototype; local artwork package and rooftop bar concept review permitted",
        "Proposal Required Programs": "Honors participation; eConcierge; OnQ PMS",
        "Proposal Support Summary":
          "Dedicated CALA development manager; opening task force (GM + 2 ops trainers); revenue management onboarding year 1",
        "Proposal Notes":
          "Proposal assumes 142-key urban select-service; owner responsible for hard costs and FF&E outside key money.",
      },
    },
    {
      brand: "Courtyard by Marriott",
      label: "Courtyard — lowest royalty, longer term",
      fields: {
        ...baseBdrFields(11, 85),
        "Proposal Agreement Type": "Franchise",
        "Proposal Initial Term Quantity": 1,
        "Proposal Initial Term Length": 25,
        "Proposal Initial Term Duration": "Years",
        "Proposal Renewal Option Quantity": 3,
        "Proposal Renewal Option Length": 5,
        "Proposal Renewal Option Duration": "Years",
        "Proposal Renewal Conditions": "Owner must not be in default; property must pass QA inspection",
        "Proposal Renewal Options":
          "3 x 5 Years. Owner must not be in default; property must pass QA inspection",
        "Proposal Royalty Pct": 5.0,
        "Proposal Royalty Basis": "% of Rooms Revenue",
        "Proposal Marketing Pct": 2.0,
        "Proposal Marketing Basis": "% of Gross Revenue",
        "Proposal Application Fee": 60000,
        "Proposal Application Fee Basis": "Per Property",
        "Proposal Initial Franchise Fee": 60000,
        "Proposal Initial Franchise Fee Basis": "Per Property",
        "Proposal Reservation Basis": "% of Gross Revenue",
        "Proposal Reservation Basis Other": "1.5",
        "Proposal Tech Platform Fees": "7200",
        "Proposal Tech Fee Basis": "Per Property",
        "Proposal Training Fees": "Included",
        "Proposal Training Fee Basis": "Included",
        "Proposal Key Money": "No",
        "Proposal PIP Capex": "$15,500/room — focused guestroom and public area conversion scope",
        "Proposal Territorial Restriction":
          "Non-exclusive with 2-mile same-brand restriction in Laureles–Estadio corridor",
        "Proposal Incentive Types": [
          "Reduced Marketing Fee Period",
          "Reduced / Waived Tech Fee Period",
          "Sign-on / Conversion Incentive",
        ],
        "Proposal Incentive Details": [
          "Reduced Marketing Fee Period: 1.0% marketing fee for first 18 months",
          "Reduced / Waived Tech Fee Period: Tech fees waived for first 6 months post-opening",
          "Sign-on / Conversion Incentive: $150,000 conversion allowance for pre-opening marketing",
        ].join("\n"),
        "Proposal Approval Timeline": "60–75 days; subject to Marriott Design Review Committee",
        "Proposal Brand Standards Flexibility":
          "Next Gen Courtyard prototype preferred; alternate F&B kiosk layout negotiable",
        "Proposal Required Programs": "Marriott Bonvoy; MARSHA; MDS revenue management",
        "Proposal Support Summary":
          "Regional franchise services team; pre-opening GM certification; digital marketing launch package",
        "Proposal Notes": "No key money; competitive fee stack with strong Bonvoy distribution offset.",
      },
    },
    {
      brand: "Hyatt Place",
      label: "Hyatt Place — escalating royalty schedule",
      fields: {
        ...baseBdrFields(6, 82),
        "Proposal Agreement Type": "Franchise",
        "Proposal Initial Term Quantity": 1,
        "Proposal Initial Term Length": 15,
        "Proposal Initial Term Duration": "Years",
        "Proposal Renewal Option Quantity": 2,
        "Proposal Renewal Option Length": 5,
        "Proposal Renewal Option Duration": "Years",
        "Proposal Renewal Conditions": "Mutual agreement; performance test at year 10 optional",
        "Proposal Renewal Options": "2 x 5 Years. Mutual agreement; performance test at year 10 optional",
        "Proposal Royalty Pct": 5,
        "Proposal Royalty Year 1": 4.0,
        "Proposal Royalty Year 2": 4.25,
        "Proposal Royalty Year 3": 4.5,
        "Proposal Royalty Year 4": 4.75,
        "Proposal Royalty Year 5 Plus": 5.0,
        "Proposal Royalty Basis": "% of Rooms Revenue",
        "Proposal Marketing Pct": 2.25,
        "Proposal Marketing Basis": "% of Gross Revenue",
        "Proposal Application Fee": 65000,
        "Proposal Application Fee Basis": "Per Property",
        "Proposal Initial Franchise Fee": 65000,
        "Proposal Initial Franchise Fee Basis": "Per Property",
        "Proposal Reservation Basis": "Per Reservation / Per Booking",
        "Proposal Reservation Basis Other": "3.0",
        "Proposal Tech Platform Fees": "9500",
        "Proposal Tech Fee Basis": "Per Property",
        "Proposal Training Fees": "8500",
        "Proposal Training Fee Basis": "One-Time",
        "Proposal Key Money": "Yes",
        "Proposal Key Money Amount": 350000,
        "Proposal Key Money Terms": "Payable at opening upon meeting brand opening standards",
        "Proposal PIP Capex": "$16,800/room — brand-standard guestroom and bath package",
        "Proposal Territorial Restriction": "Exclusive 2.5-mile radius; Hyatt Place only (excludes other Hyatt brands)",
        "Proposal Incentive Types": [
          "PIP Contribution by Brand",
          "Opening / FF&E Support",
        ],
        "Proposal Incentive Details": [
          "PIP Contribution by Brand: Up to $400,000 toward guestroom casegoods and soft goods",
          "Opening / FF&E Support: $75,000 pre-opening FF&E allowance",
        ].join("\n"),
        "Proposal Approval Timeline": "30–45 days for franchise committee review",
        "Proposal Brand Standards Flexibility":
          "Hyatt Place 2.0 prototype; flexible meeting room count for urban infill",
        "Proposal Required Programs": "World of Hyatt; Enspire PMS; Hyatt digital booking",
        "Proposal Support Summary":
          "CALA franchise onboarding; opening support team (5 on-site days); year-1 revenue support",
        "Proposal Notes": "Escalating royalty schedule rewards early ramp; shorter initial term than peers.",
      },
    },
    {
      brand: "Sleep Inn",
      label: "Sleep Inn — lowest fee stack, Choice CALA footprint",
      fields: {
        ...baseBdrFields(14, 79),
        "Proposal Agreement Type": "Franchise",
        "Proposal Initial Term Quantity": 1,
        "Proposal Initial Term Length": 20,
        "Proposal Initial Term Duration": "Years",
        "Proposal Renewal Option Quantity": 2,
        "Proposal Renewal Option Length": 5,
        "Proposal Renewal Option Duration": "Years",
        "Proposal Renewal Conditions": "Automatic if franchisee in good standing",
        "Proposal Renewal Options": "2 x 5 Years. Automatic if franchisee in good standing",
        "Proposal Royalty Pct": 4.5,
        "Proposal Royalty Basis": "% of Rooms Revenue",
        "Proposal Marketing Pct": 1.8,
        "Proposal Marketing Basis": "% of Gross Revenue",
        "Proposal Application Fee": 45000,
        "Proposal Application Fee Basis": "Per Property",
        "Proposal Initial Franchise Fee": 45000,
        "Proposal Initial Franchise Fee Basis": "Per Property",
        "Proposal Reservation Basis": "Per Reservation / Per Booking",
        "Proposal Reservation Basis Other": "1.75",
        "Proposal Tech Platform Fees": "65",
        "Proposal Tech Fee Basis": "Per Room / Year",
        "Proposal Training Fees": "5000",
        "Proposal Training Fee Basis": "One-Time",
        "Proposal Key Money": "No",
        "Proposal PIP Capex": "$12,500/room — efficient select-service scope for new build",
        "Proposal Territorial Restriction":
          "3-mile Sleep Inn exclusivity; Choice upper-midscale brands may coexist with approval",
        "Proposal Incentive Types": [
          "Reduced Royalty Period",
          "Application Fee Credit",
          "Marketing Allowance (one-time or recurring)",
        ],
        "Proposal Incentive Details": [
          "Reduced Royalty Period: 3.5% royalty for first 24 months",
          "Application Fee Credit: Full application fee credited to initial franchise fee",
          "Marketing Allowance (one-time or recurring): $100,000 opening marketing co-op in year 1",
        ].join("\n"),
        "Proposal Approval Timeline": "21–30 days typical for CALA new-build franchise",
        "Proposal Brand Standards Flexibility":
          "Sleep Inn 2.0 prototype; breakfast area sizing flexible for urban footprint",
        "Proposal Required Programs": "Choice Privileges; SkyTouch PMS; Choice revenue tools",
        "Proposal Support Summary":
          "Choice CALA development contact; remote pre-opening training; co-op marketing templates",
        "Proposal Notes":
          "Lowest ongoing fee stack; limited key money but aggressive ramp incentives for CALA expansion.",
      },
    },
  ];
}

async function resolveDealId(base) {
  const envId = (process.env.DEAL_COMPARE_SEED_DEAL_ID || "").trim();
  if (envId.startsWith("rec")) return envId;

  const nameNeedle = (process.env.DEAL_COMPARE_SEED_DEAL_NAME || "Medellin").trim().toLowerCase();
  const deals = await base(DEALS_TABLE).select({ pageSize: 100 }).firstPage();
  const match = deals.find((r) => {
    const blob = JSON.stringify(r.fields).toLowerCase();
    return blob.includes(nameNeedle);
  });
  if (!match) {
    throw new Error(`No deal matching "${nameNeedle}" in ${DEALS_TABLE}. Set DEAL_COMPARE_SEED_DEAL_ID.`);
  }
  return match.id;
}

async function findBdrByDealAndBrand(base, dealId, brandName) {
  const formula = `AND(FIND('${escapeFormula(dealId)}', ARRAYJOIN({Deal})) > 0, {Brand Name} = '${escapeFormula(brandName)}')`;
  const rows = await base(BDR_TABLE).select({ filterByFormula: formula, maxRecords: 1 }).firstPage();
  return rows[0] || null;
}

async function demoteKpiDemoOnDeal(base, dealId) {
  const formula = `AND(FIND('${escapeFormula(dealId)}', ARRAYJOIN({Deal})) > 0, SEARCH('KPI Demo', {Brand Name}) = 1)`;
  const rows = await base(BDR_TABLE).select({ filterByFormula: formula, pageSize: 50 }).firstPage();
  for (const row of rows) {
    if (String(row.fields["Proposal Status"] || "").toLowerCase() !== "submitted") continue;
    await base(BDR_TABLE).update([
      {
        id: row.id,
        fields: {
          "Proposal Status": "Draft",
          "Owner Notes": `${SEED_MARKER} — demoted; replaced by Compare demo brands`,
        },
      },
    ]);
    console.log(`[demote] ${row.fields["Brand Name"]} → Draft (${row.id})`);
  }
  if (rows.length === 0) {
    console.log("[demote] no KPI Demo rows on target deal");
  }
}

function hasProposalEconomics(fields) {
  const keys = [
    "Proposal Royalty Pct",
    "Proposal Royalty Year 1",
    "Proposal Marketing Pct",
    "Proposal Application Fee",
  ];
  return keys.some((k) => fields[k] != null && fields[k] !== "");
}

/** Demote KPI Demo rows marked Submitted but with no proposal economics (empty Compare columns). */
export async function demoteEmptyKpiDemoSubmissions(base) {
  const rows = await base(BDR_TABLE)
    .select({ filterByFormula: "SEARCH('KPI Demo', {Brand Name}) = 1", pageSize: 100 })
    .firstPage();
  let demoted = 0;
  for (const row of rows) {
    if (String(row.fields["Proposal Status"] || "").toLowerCase() !== "submitted") continue;
    if (hasProposalEconomics(row.fields)) continue;
    await base(BDR_TABLE).update([
      {
        id: row.id,
        fields: {
          "Proposal Status": "Draft",
          "Owner Notes": `${SEED_MARKER} — empty submitted proposal demoted; use Medellin deal for Compare demo`,
        },
      },
    ]);
    demoted += 1;
    console.log(`[demote-empty] ${row.fields["Brand Name"]} → Draft (${row.id})`);
  }
  console.log(`Demoted ${demoted} empty KPI Demo submission(s).`);
  return demoted;
}

async function deleteSeededRows(base) {
  const ids = [];
  await base(BDR_TABLE)
    .select({
      filterByFormula: `FIND('${escapeFormula(SEED_MARKER)}', {Owner Notes})`,
      pageSize: 100,
    })
    .eachPage((records, next) => {
      ids.push(...records.map((r) => r.id));
      next();
    });
  for (let i = 0; i < ids.length; i += 10) {
    const chunk = ids.slice(i, i + 10);
    if (chunk.length) await base(BDR_TABLE).destroy(chunk);
  }
  console.log(`Deleted ${ids.length} seeded Compare proposal row(s).`);
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const clean = process.argv.includes("--clean");
  const demoteEmpty = process.argv.includes("--demote-empty-kpi-demo");
  const specs = buildDealCompareProposalSpecs();

  if (demoteEmpty) {
    const base = getBase();
    await demoteEmptyKpiDemoSubmissions(base);
    return;
  }

  if (dryRun) {
    console.log(`Dry run: would upsert ${specs.length} submitted proposals:`);
    for (const s of specs) {
      console.log(`  • ${s.brand} — ${s.label}`);
    }
    return;
  }

  const base = getBase();

  if (clean) {
    await deleteSeededRows(base);
    console.log("Clean complete.");
    return;
  }

  const dealId = await resolveDealId(base);
  console.log(`Target deal: ${dealId}`);

  let created = 0;
  let updated = 0;
  let failed = 0;

  for (const spec of specs) {
    const payload = {
      Deal: [dealId],
      "Brand Name": spec.brand,
      ...spec.fields,
    };
    const existing = await findBdrByDealAndBrand(base, dealId, spec.brand);
    try {
      if (existing) {
        await base(BDR_TABLE).update([{ id: existing.id, fields: payload }]);
        updated += 1;
        console.log(`[update] ${spec.brand} → ${existing.id}`);
      } else {
        const [rec] = await base(BDR_TABLE).create([{ fields: payload }]);
        created += 1;
        console.log(`[create] ${spec.brand} → ${rec.id}`);
      }
    } catch (e) {
      failed += 1;
      console.error(`[fail] ${spec.brand}:`, e.message || e);
    }
  }

  try {
    await demoteKpiDemoOnDeal(base, dealId);
  } catch (e) {
    console.warn("[demote] KPI Demo rows:", e.message || e);
  }

  console.log(`\nDone. created=${created} updated=${updated} failed=${failed}`);
  console.log("Reload My Deals → Deal Compare → select Hilton Garden Inn Medellin.");
  console.log("Remove: node scripts/seed-deal-compare-proposals.mjs --clean");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
