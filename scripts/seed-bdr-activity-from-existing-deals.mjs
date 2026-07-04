/**
 * Seed Brand Deal Requests + Deal Activity Log using REAL deals and (when available)
 * preferred brand names from each deal — so My Brand Deals / Deal Log tabs show plausible data.
 *
 * Prerequisites:
 *   - .env: AIRTABLE_API_KEY, AIRTABLE_BASE_ID (same as server)
 *   - Tables: "Brand Deal Requests", "Deal Activity Log", "Deals" (or env overrides)
 *   - Brand Deal Request single-selects (Status, NDA Status, Deal Room Access, Proposal Status)
 *     must match your base (same set as api/brand-deal-requests.js / workspace demo seeds).
 *   - Deal Activity Log → Action is often a restricted single-select; this script uses the same
 *     verb labels as api/brand-deal-requests.js, then falls back to the BDR pipeline Status string
 *     when the API would (e.g. "Deal Room Active") if the primary label is not an option.
 *
 * Usage:
 *   node scripts/seed-bdr-activity-from-existing-deals.mjs
 *   node scripts/seed-bdr-activity-from-existing-deals.mjs --dry-run
 *   node scripts/seed-bdr-activity-from-existing-deals.mjs --clean   # remove only rows tagged [seed:bdd-v2]
 *
 * Env (optional):
 *   SEED_COVER_TABS=1            default: one BDR per workspace tab (sparse); set 0 for legacy “many rows” mode
 *   SEED_EXTRA_NEW_OPPORTUNITIES=5  after tab coverage, add more Status "New" rows (default 5 when SEED_COVER_TABS=1; 0 skips)
 *   SEED_MAX_DEALS=25            max deals to scan (default 25)
 *   SEED_BRANDS_PER_DEAL=1       default 1 in tab-coverage mode; legacy mode default 2
 *   SEED_DEAL_IDS=recA,recB      force these deal IDs only (overrides max scan)
 *   SEED_SKIP_EXISTING=1         skip deal+brand if a BDR already exists (default 1)
 *
 * Workspace buckets follow public/brand-development-dashboard.js → deriveWorkspaceBucket (NDA state runs before status).
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

/**
 * One row per My Brand Deals workspace tab (deriveWorkspaceBucket in brand-development-dashboard.js).
 * Use NDA "Not Required" (not "Not Sent") for New / Brand Viewed so rows land in New / Active review, not NDA tab.
 */
const STATUS_TEMPLATES = [
  {
    label: "New opportunities",
    workspaceTab: "bdd-new",
    fields: { Status: "New", "NDA Status": "Not Required", "Deal Room Access": "Blocked" },
  },
  {
    label: "Active brand review",
    workspaceTab: "bdd-active-review",
    fields: { Status: "Brand Viewed", "NDA Status": "Not Required", "Deal Room Access": "Blocked" },
  },
  {
    label: "Awaiting owner info",
    workspaceTab: "bdd-awaiting-info",
    fields: { Status: "Accepted", "NDA Status": "Not Required", "Deal Room Access": "Blocked" },
  },
  {
    label: "NDA / Deal room",
    workspaceTab: "bdd-nda-room",
    fields: { Status: "Accepted", "NDA Status": "Sent", "Deal Room Access": "Blocked" },
  },
  {
    label: "Terms / proposal",
    workspaceTab: "bdd-terms-proposal",
    fields: {
      Status: "Pre-LOI",
      "NDA Status": "Signed - Owner Confirmed",
      "Deal Room Access": "Granted",
      "Proposal Status": "Draft",
    },
  },
  {
    label: "Advanced pipeline",
    workspaceTab: "bdd-advanced",
    fields: {
      Status: "Finalist",
      "NDA Status": "Signed - Owner Confirmed",
      "Deal Room Access": "Granted",
    },
  },
  {
    label: "Archived / declined",
    workspaceTab: "bdd-archived",
    fields: {
      Status: "Declined",
      "NDA Status": "Not Required",
      "Response Notes": "Passing on this cycle — portfolio fit.",
    },
  },
];

/** Extra Action single-select values to try after pipeline Status (restricted Airtable keys). */
const ACTION_VERB_FALLBACKS = ["Notes updated", "Follow-up scheduled", "Request Sent"];

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

/** Mirrors deriveWorkspaceBucket (brand-development-dashboard.js) using BDR-shaped field names. */
function deriveWorkspaceBucketFromSeedFields(fields) {
  if (!fields || typeof fields !== "object") return "new";
  const st = String(fields.Status || "").trim();
  const nda = String(fields["NDA Status"] || "").trim();
  const dra = String(fields["Deal Room Access"] || "").trim();
  const prop = String(fields["Proposal Status"] || "").trim();
  if (["Declined", "Responded - Declined", "Archived"].includes(st)) return "archived";
  if (st === "Revisit Later") return "advanced";
  if (st === "More Info Requested") return "awaiting-info";
  const inNdaFlow =
    st === "Deal Room Active" ||
    nda === "Not Sent" ||
    nda === "Sent" ||
    (nda === "Signed - Owner Confirmed" && dra && dra !== "Granted");
  if (inNdaFlow) return "nda-room";
  if (["Pre-LOI", "Pre-LOI / Term Comparison"].includes(st) || prop === "Draft" || prop === "Submitted") return "terms-proposal";
  if (["Finalist", "Feasibility", "Feasibility In Progress", "LOI Signed", "LOI Signed / Platform Exit"].includes(st)) return "advanced";
  if (["Accepted", "Responded - Accepted"].includes(st)) return "awaiting-info";
  if (["Brand Viewed", "Viewed"].includes(st)) return "active-review";
  if (["New", "Sent / Awaiting Response"].includes(st) || !st) return "new";
  return "awaiting-info";
}

/**
 * Distinct Deal Activity Log Action verbs (aligned with api/brand-deal-requests.js) for UI testing.
 */
function getActivityPlan(tmpl) {
  const f = tmpl.fields || {};
  const status = f.Status;
  const nda = f["NDA Status"];
  const label = tmpl.label || "Seed";
  /** @type {{ action: string, details: string, stakeholder: string, daysAgo: number }[]} */
  const rows = [];
  const push = (action, details, stakeholder, daysAgo) => {
    rows.push({ action, details, stakeholder, daysAgo });
  };

  push("Request Sent", `Intro sent — ${label}.`, "Owner", 24);

  if (status === "New") {
    push("Notes updated", "Sponsor context and timeline refreshed for routing.", "Owner", 21);
    return rows;
  }

  push("Opportunity reviewed", "Brand team reviewed project summary and match context.", "Brand", 20);

  if (status === "Brand Viewed") {
    push("Notes updated", "Brand committee reviewing portfolio fit vs. current pipeline.", "Brand", 16);
    return rows;
  }

  if (status === "Accepted" && nda === "Sent") {
    push("Marked interested", "Brand confirmed appetite to proceed under mutual NDA.", "Brand", 18);
    push("NDA Sent", "Standard mutual NDA issued for confidential review.", "Owner", 16);
    push("Follow-up scheduled", "Weekly reminder — countersignature still outstanding.", "Owner", 13);
    push("Notes updated", "Owner uploaded draft scope outline to deal room (pending access).", "Owner", 11);
    return rows;
  }

  if (status === "Accepted") {
    push("Marked interested", "Brand confirmed interest; awaiting owner underwriting inputs.", "Brand", 18);
    push("Information requested", "Requesting feasibility memo and indicative key terms.", "Brand", 16);
    push("Follow-up scheduled", "Owner / brand working session on data gaps.", "Owner", 14);
    push("Notes updated", "Match score drivers validated against latest sponsor brief.", "Owner", 12);
    return rows;
  }

  if (status === "Pre-LOI") {
    push("Marked interested", "Brand advancing to structured diligence.", "Brand", 19);
    push("NDA Sent", "Mutual NDA issued for data room review.", "Owner", 17);
    push("NDA Signed (Owner Confirmed)", "Countersigned NDA on file.", "Owner", 15);
    push("Deal Room Access Granted", "VDR access enabled for brand diligence team.", "Owner", 14);
    push("Notes updated", "Owner indexed core data room folders for brand reviewers.", "Owner", 12);
    push("Terms preparation started", "Draft commercial terms under internal review.", "Brand", 10);
    push("Follow-up scheduled", "Terms working group — target decision date.", "Owner", 8);
    return rows;
  }

  if (status === "Finalist") {
    push("Marked interested", "Brand shortlisted for final comparison.", "Brand", 19);
    push("NDA Sent", "Mutual NDA issued.", "Owner", 17);
    push("NDA Signed (Owner Confirmed)", "Countersigned NDA on file.", "Owner", 15);
    push("Deal Room Access Granted", "Full diligence library unlocked.", "Owner", 14);
    push("Terms preparation started", "Commercial term sheet iterations in progress.", "Brand", 12);
    push("Proposal Submitted", "Brand proposal submitted for owner evaluation.", "Brand", 9);
    push("Notes updated", "Owner consolidated finalist scorecard.", "Owner", 7);
    return rows;
  }

  if (status === "Declined") {
    push("Notes updated", "Brand requested one clarification pass on scope before decision.", "Brand", 16);
    push("Declined", "Brand declined for this opportunity after internal gate.", "Brand", 13);
    push("Notes updated", "Decline rationale logged for CRM continuity.", "Owner", 11);
    return rows;
  }

  return rows;
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

async function attemptActivityCreate(base, fields) {
  try {
    await base(ACTIVITY_LOG_TABLE).create([{ fields }]);
    return;
  } catch (e) {
    const msg = String(e?.message || e);
    if (/Unknown field|does not exist/i.test(msg) && fields.Stakeholder != null) {
      const f2 = { ...fields };
      delete f2.Stakeholder;
      await attemptActivityCreate(base, f2);
      return;
    }
    if (/Created At|READONLY|cannot/i.test(msg)) {
      const f2 = { ...fields };
      delete f2["Created At"];
      await attemptActivityCreate(base, f2);
      return;
    }
    throw e;
  }
}

/**
 * Write Deal Activity Log row. If Action is a single-select and the primary label is not
 * allowed in the base (common with restricted API keys), retry with `statusFallback`
 * — same pattern as api/brand-deal-requests.js (raw pipeline status as Action when unmapped).
 */
async function createActivity(base, fields, statusFallback = "", extraPreferred = []) {
  const primary = String(fields.Action || "").trim();
  const alt = String(statusFallback || "").trim();
  const secondary = alt && alt !== primary ? alt : "";
  const mergedExtras = [...(extraPreferred || []), ...ACTION_VERB_FALLBACKS]
    .map((x) => String(x || "").trim())
    .filter(Boolean);
  const chain = [];
  const seen = new Set();
  for (const a of [primary, secondary, ...mergedExtras]) {
    if (!a || seen.has(a)) continue;
    seen.add(a);
    chain.push(a);
  }
  const isSelectPermissionError = (msg) =>
    /select option|UNKNOWN_MULTIPLE_CHOICE|Insufficient permissions|invalid.*choice|INVALID_MULTIPLE_CHOICE_OPTIONS/i.test(
      String(msg || ""),
    );

  for (const action of chain) {
    try {
      await attemptActivityCreate(base, { ...fields, Action: action });
      return;
    } catch (e) {
      const msg = String(e?.message || e);
      if (isSelectPermissionError(msg)) continue;
      throw e;
    }
  }
  console.warn(`[activity] skip exhausted (${chain.join(" → ")}): no Action option matched this base`);
}

async function seedActivityTimeline(base, dealId, brandName, tmpl) {
  const f = tmpl.fields || {};
  const status = f.Status;
  const detailsSuffix = ` ${SEED_MARKER}`;
  const plan = getActivityPlan(tmpl);
  for (const step of plan) {
    const fields = {
      Deal: [dealId],
      "Brand Name": brandName,
      Action: step.action,
      Details: String(step.details) + detailsSuffix,
      Stakeholder: step.stakeholder,
      "Created At": isoDaysAgo(step.daysAgo),
    };
    await createActivity(base, fields, status);
  }
}

async function fetchDealIds(base) {
  const env = (process.env.SEED_DEAL_IDS || "")
    .split(",")
    .map((s) => s.trim())
    .filter((id) => id.startsWith("rec"));
  if (env.length) return env;

  const max = Math.min(100, Math.max(1, parseInt(process.env.SEED_MAX_DEALS || "25", 10) || 25));
  const ids = [];
  await base(DEALS_TABLE)
    .select({
      pageSize: 100,
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

function getPreferredBrandsRaw(fields) {
  if (!fields || typeof fields !== "object") return "";
  const keys = [
    "Preferred Brands (up to 4)",
    "Preferred Brands",
    "Brand Preference",
    "Desired Brand",
    "Preferred brand(s)",
  ];
  for (const k of keys) {
    const v = fields[k];
    const s = fieldToString(v);
    if (s) return s;
  }
  return "";
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

  const coverTabs = (process.env.SEED_COVER_TABS || "1") !== "0";
  const brandsPerDeal = Math.min(
    4,
    Math.max(
      1,
      parseInt(
        process.env.SEED_BRANDS_PER_DEAL || (coverTabs ? "1" : "2"),
        10,
      ) || (coverTabs ? 1 : 2),
    ),
  );
  const skipExisting = (process.env.SEED_SKIP_EXISTING || "1") !== "0";

  const dealIds = await fetchDealIds(base);
  if (dealIds.length === 0) {
    console.error(`No deals found in "${DEALS_TABLE}".`);
    process.exit(1);
  }
  console.log(
    `Using ${dealIds.length} deal(s). dry-run=${dry} coverTabs=${coverTabs} templates=${STATUS_TEMPLATES.length}`,
  );

  const extraNewTarget =
    String(process.env.SEED_EXTRA_NEW_OPPORTUNITIES || "").trim() !== ""
      ? Math.max(0, parseInt(process.env.SEED_EXTRA_NEW_OPPORTUNITIES, 10) || 0)
      : coverTabs
        ? 5
        : 0;
  if (extraNewTarget > 0) {
    console.log(`Extra "New" tab rows target: ${extraNewTarget} (SEED_EXTRA_NEW_OPPORTUNITIES)`);
  }

  let createdBdr = 0;
  let skipped = 0;
  let failed = 0;
  let fb = 0;
  let templateIndex = 0;
  let extraNewCreated = 0;

  for (let di = 0; di < dealIds.length; di++) {
    if (coverTabs && templateIndex >= STATUS_TEMPLATES.length) break;

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
    let brands = parseBrandNames(getPreferredBrandsRaw(f));
    while (brands.length < brandsPerDeal) {
      brands.push(FALLBACK_BRANDS[fb % FALLBACK_BRANDS.length]);
      fb++;
    }
    brands = brands.slice(0, brandsPerDeal);

    const brandSlots = coverTabs ? 1 : brands.length;
    for (let bi = 0; bi < brandSlots; bi++) {
      const brandName = brands[bi];
      const tmpl = coverTabs ? STATUS_TEMPLATES[templateIndex] : STATUS_TEMPLATES[(di + bi) % STATUS_TEMPLATES.length];

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
        const bucket = deriveWorkspaceBucketFromSeedFields(tmpl.fields);
        console.log(
          `[dry-run] ${tmpl.workspaceTab || "—"} | bucket:${bucket} | ${prop} | ${brandName} | ${tmpl.label}`,
        );
        if (coverTabs) templateIndex++;
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
        const bucket = deriveWorkspaceBucketFromSeedFields(tmpl.fields);
        console.log(
          `[bdr] ${created.id} ${brandName} / ${dealId.slice(0, 10)}… / ${tmpl.label} → ${bucket}`,
        );
        await seedActivityTimeline(base, dealId, brandName, tmpl);
        if (coverTabs) templateIndex++;
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

  if (extraNewTarget > 0 && dry && coverTabs) {
    let n = 0;
    for (let di = 0; di < dealIds.length && n < extraNewTarget; di++) {
      const dealId = dealIds[di];
      let dealRec;
      try {
        dealRec = await fetchDealRecord(base, dealId);
      } catch {
        continue;
      }
      const f = dealRec.fields || {};
      const prop = fieldToString(f["Property Name"]) || "Project";
      let brands = parseBrandNames(getPreferredBrandsRaw(f));
      while (brands.length < 8) {
        brands.push(FALLBACK_BRANDS[fb % FALLBACK_BRANDS.length]);
        fb++;
      }
      for (let j = 0; j < brands.length && n < extraNewTarget; j++) {
        console.log(`[dry-run] bdd-new | bucket:new | ${prop} | ${brands[j]} | New opportunities (extra)`);
        n++;
      }
    }
  }

  if (extraNewTarget > 0 && !dry) {
    const newTmpl = {
      label: "New opportunities",
      workspaceTab: "bdd-new",
      fields: { ...STATUS_TEMPLATES[0].fields },
    };
    for (let di = 0; di < dealIds.length && extraNewCreated < extraNewTarget; di++) {
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
      let brands = parseBrandNames(getPreferredBrandsRaw(f));
      while (brands.length < 8) {
        brands.push(FALLBACK_BRANDS[fb % FALLBACK_BRANDS.length]);
        fb++;
      }
      for (let j = 0; j < brands.length && extraNewCreated < extraNewTarget; j++) {
        const brandName = brands[j];
        if (skipExisting) {
          const ex = await findBdr(base, dealId, brandName);
          if (ex) {
            skipped++;
            continue;
          }
        }
        try {
          const now = new Date().toISOString();
          const matchScore = 58 + ((di * 5 + j * 11) % 32);
          const bdrFields = {
            Deal: [dealId],
            "Brand Name": brandName,
            "Match Score": matchScore,
            "Request Sent At": isoDaysAgo(14 - (j % 3)),
            "Created At": isoDaysAgo(14 - (j % 3)),
            "Last Updated": now,
            "Owner Notes": markOwnerNote(`Seeded for "${prop}" — ${newTmpl.label} (extra New tab).`),
            ...newTmpl.fields,
          };
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
          extraNewCreated++;
          console.log(`[bdr+extra-new] ${created.id} ${brandName} / ${dealId.slice(0, 10)}… → new`);
          await seedActivityTimeline(base, dealId, brandName, newTmpl);
        } catch (e) {
          failed++;
          console.error(`[fail extra-new] ${prop} | ${brandName}:`, e.message || e);
        }
      }
    }
    if (extraNewCreated < extraNewTarget) {
      console.warn(
        `[seed] Extra New opportunities: created ${extraNewCreated}/${extraNewTarget}. Increase SEED_MAX_DEALS or clear existing BDRs for more brands per deal.`,
      );
    }
  }

  console.log("\nSummary:", {
    createdBdr,
    skipped,
    failed,
    dry,
    coverTabs,
    templatesTarget: STATUS_TEMPLATES.length,
    extraNewCreated,
    extraNewTarget,
  });
  if (coverTabs && !dry && templateIndex < STATUS_TEMPLATES.length) {
    console.warn(
      `Workspace tab coverage incomplete: ${templateIndex}/${STATUS_TEMPLATES.length} templates written. Try SEED_MAX_DEALS=30, clear conflicting BDRs, or SEED_SKIP_EXISTING=0 (careful).`,
    );
  }
  if (!dry) {
    console.log(`Remove seeded rows: node scripts/seed-bdr-activity-from-existing-deals.mjs --clean`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
