/**
 * Seed realistic Operator Deal Requests (+ activity) so My Operator Deals tabs
 * mirror Brand Development Workspace demo coverage (new, active review, awaiting
 * info, NDA/shared workspace, terms, advanced, declined/archived).
 *
 * Prerequisites: AIRTABLE_API_KEY, AIRTABLE_BASE_ID; GHL operator scope configured.
 * Deals should have linked Location & Property + Contact & Uploads (CALA samples do).
 *
 * Usage:
 *   node scripts/seed-operator-development-workspace-demo.mjs
 *   node scripts/seed-operator-development-workspace-demo.mjs --clean
 *
 * Optional env:
 *   ODR_DEMO_COMPANY=GHL Hoteles (GHL Holding)
 *   ODD_SEED_DEAL_IDS=recAAA,recBBB
 */

import "../load-env.js";
import Airtable from "airtable";
import { MAP_ODR_AIRTABLE } from "../api/operator-deal-requests-fields.js";
import { createOdrRow } from "../lib/dealality/odr-owner-create.js";
import { enrichWorkspaceRow } from "../lib/deal-workspace-pipeline.js";
import { buildDealMetaFromFields, mergeDealContextFields } from "../lib/dealality/operator-deal-meta.js";

const ODR_TABLE = MAP_ODR_AIRTABLE.table;
const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";
const ACTIVITY_LOG_TABLE = process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log";
const LOCATION_TABLE = process.env.AIRTABLE_TABLE_LOCATION_PROPERTY || "Location & Property";
const CONTACT_TABLE = process.env.AIRTABLE_TABLE_CONTACT_UPLOADS || "Contact & Uploads";

const DEMO_PREFIX = "ODD Demo";
const COMPANY = process.env.ODR_DEMO_COMPANY || process.env.OPERATOR_DEALS_DEMO_COMPANY || "GHL Hoteles (GHL Holding)";
const MASTER_ID = process.env.ODR_DEMO_OPERATOR_SETUP_ID || "reciI2tYQBfMoMK9G";

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

function isoDaysFromNow(days) {
  const d = new Date();
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10);
}

function isoDaysAgo(days) {
  return isoDaysFromNow(-days);
}

const now = new Date().toISOString();

function demoOwnerNote(key, body) {
  return `${DEMO_PREFIX} | ${key} — ${body}`;
}

function demoSpecs() {
  return [
    {
      key: "New",
      label: "New opportunities",
      ownerMessage:
        "We shortlisted GHL for our airport-corridor reflag. Please review fit on revenue management, pre-opening support, and owner reporting cadence before we share the full data room.",
      fields: {
        [MAP_ODR_AIRTABLE.status]: "Sent / Awaiting Response",
        [MAP_ODR_AIRTABLE.alignmentScore]: 74,
        [MAP_ODR_AIRTABLE.alignmentBand]: "Moderate",
        [MAP_ODR_AIRTABLE.dataConfidence]: "Operator-provided",
        [MAP_ODR_AIRTABLE.requestSentAt]: now,
        [MAP_ODR_AIRTABLE.lastUpdated]: now,
      },
    },
    {
      key: "ActiveReview",
      label: "Active operator review",
      ownerMessage:
        "Owner team confirmed 190-key select-service reflag with moderate PIP. Looking for your preliminary read on operating model and key money expectations.",
      fields: {
        [MAP_ODR_AIRTABLE.status]: "Operator Viewed",
        [MAP_ODR_AIRTABLE.alignmentScore]: 69,
        [MAP_ODR_AIRTABLE.alignmentBand]: "Moderate",
        [MAP_ODR_AIRTABLE.dataConfidence]: "Operator-provided",
        [MAP_ODR_AIRTABLE.requestSentAt]: isoDaysAgo(3),
        [MAP_ODR_AIRTABLE.lastUpdated]: isoDaysAgo(1),
      },
    },
    {
      key: "AwaitingInfo",
      label: "Awaiting owner info (accepted)",
      ownerMessage:
        "Accepted your interest in principle — we can share STR summary and capex phasing once NDA mechanics are confirmed on your side.",
      fields: {
        [MAP_ODR_AIRTABLE.status]: "Accepted",
        [MAP_ODR_AIRTABLE.ndaStatus]: "Not Required",
        [MAP_ODR_AIRTABLE.alignmentScore]: 82,
        [MAP_ODR_AIRTABLE.alignmentBand]: "Strong",
        [MAP_ODR_AIRTABLE.dataConfidence]: "Operator-provided",
        [MAP_ODR_AIRTABLE.requestSentAt]: isoDaysAgo(8),
        [MAP_ODR_AIRTABLE.lastUpdated]: isoDaysAgo(2),
      },
    },
    {
      key: "MoreInfo",
      label: "More info requested",
      ownerMessage:
        "Owner paused shortlist pending answers on union status, FF&E reserve policy, and who holds the management agreement draft.",
      fields: {
        [MAP_ODR_AIRTABLE.status]: "More Info Requested",
        [MAP_ODR_AIRTABLE.responseNotes]:
          "Please confirm union coverage, FF&E reserve policy, and whether GHL can work with our existing third-party asset manager.",
        [MAP_ODR_AIRTABLE.alignmentScore]: 76,
        [MAP_ODR_AIRTABLE.alignmentBand]: "Moderate",
        [MAP_ODR_AIRTABLE.dataConfidence]: "Incomplete",
        [MAP_ODR_AIRTABLE.requestSentAt]: isoDaysAgo(10),
        [MAP_ODR_AIRTABLE.lastUpdated]: isoDaysAgo(1),
      },
    },
    {
      key: "NdaPending",
      label: "NDA / shared workspace — NDA not sent",
      ownerMessage:
        "Shortlisted after initial call. Owner ready to exchange OM and PIP once mutual NDA is issued.",
      fields: {
        [MAP_ODR_AIRTABLE.status]: "Accepted",
        [MAP_ODR_AIRTABLE.ndaStatus]: "Not Sent",
        [MAP_ODR_AIRTABLE.dealRoomAccess]: "Blocked",
        [MAP_ODR_AIRTABLE.alignmentScore]: 77,
        [MAP_ODR_AIRTABLE.alignmentBand]: "Moderate",
        [MAP_ODR_AIRTABLE.dataConfidence]: "Operator-provided",
        [MAP_ODR_AIRTABLE.requestSentAt]: isoDaysAgo(14),
        [MAP_ODR_AIRTABLE.lastUpdated]: isoDaysAgo(3),
      },
    },
    {
      key: "NdaSent",
      label: "NDA / shared workspace — NDA sent",
      ownerMessage:
        "Owner signed NDA on their side; awaiting countersignature to unlock lender memo and contractor bids.",
      fields: {
        [MAP_ODR_AIRTABLE.status]: "Accepted",
        [MAP_ODR_AIRTABLE.ndaStatus]: "Sent",
        [MAP_ODR_AIRTABLE.dealRoomAccess]: "Blocked",
        [MAP_ODR_AIRTABLE.alignmentScore]: 80,
        [MAP_ODR_AIRTABLE.alignmentBand]: "Strong",
        [MAP_ODR_AIRTABLE.dataConfidence]: "Operator-provided",
        [MAP_ODR_AIRTABLE.requestSentAt]: isoDaysAgo(18),
        [MAP_ODR_AIRTABLE.lastUpdated]: isoDaysAgo(2),
      },
    },
    {
      key: "DealRoom",
      label: "NDA / shared workspace — deal room active",
      ownerMessage:
        "Confidentiality complete. Owner shared lender model, STR, and preliminary PIP in shared workspace for operator review.",
      fields: {
        [MAP_ODR_AIRTABLE.status]: "Deal Room Active",
        [MAP_ODR_AIRTABLE.ndaStatus]: "Signed - Owner Confirmed",
        [MAP_ODR_AIRTABLE.dealRoomAccess]: "Granted",
        [MAP_ODR_AIRTABLE.alignmentScore]: 85,
        [MAP_ODR_AIRTABLE.alignmentBand]: "Strong",
        [MAP_ODR_AIRTABLE.dataConfidence]: "Operator-provided",
        [MAP_ODR_AIRTABLE.requestSentAt]: isoDaysAgo(21),
        [MAP_ODR_AIRTABLE.lastUpdated]: isoDaysAgo(1),
      },
    },
    {
      key: "Terms",
      label: "Terms review",
      ownerMessage:
        "Owner requested preliminary management terms and incentive structure ahead of finalist down-select.",
      fields: {
        [MAP_ODR_AIRTABLE.status]: "Pre-LOI",
        [MAP_ODR_AIRTABLE.ndaStatus]: "Signed - Owner Confirmed",
        [MAP_ODR_AIRTABLE.dealRoomAccess]: "Granted",
        [MAP_ODR_AIRTABLE.alignmentScore]: 88,
        [MAP_ODR_AIRTABLE.alignmentBand]: "Strong",
        [MAP_ODR_AIRTABLE.dataConfidence]: "Operator-provided",
        [MAP_ODR_AIRTABLE.requestSentAt]: isoDaysAgo(25),
        [MAP_ODR_AIRTABLE.lastUpdated]: now,
      },
    },
    {
      key: "Advanced",
      label: "Finalist / advanced",
      ownerMessage:
        "Owner narrowed to two operators; feasibility and opening timeline are the next decision gates.",
      fields: {
        [MAP_ODR_AIRTABLE.status]: "Finalist",
        [MAP_ODR_AIRTABLE.ndaStatus]: "Signed - Owner Confirmed",
        [MAP_ODR_AIRTABLE.dealRoomAccess]: "Granted",
        [MAP_ODR_AIRTABLE.alignmentScore]: 91,
        [MAP_ODR_AIRTABLE.alignmentBand]: "Strong",
        [MAP_ODR_AIRTABLE.dataConfidence]: "Operator-provided",
        [MAP_ODR_AIRTABLE.requestSentAt]: isoDaysAgo(30),
        [MAP_ODR_AIRTABLE.lastUpdated]: isoDaysAgo(2),
      },
    },
    {
      key: "Revisit",
      label: "Revisit later (advanced bucket)",
      ownerMessage:
        "Owner pausing until municipality height ruling; wants GHL to stay warm for Q4 re-engagement.",
      fields: {
        [MAP_ODR_AIRTABLE.status]: "Revisit Later",
        [MAP_ODR_AIRTABLE.nextFollowupDate]: isoDaysFromNow(45),
        [MAP_ODR_AIRTABLE.nextFollowupHeader]: "Revisit — zoning update expected",
        [MAP_ODR_AIRTABLE.nextFollowupNotesExternal]:
          "City council vote expected Q3; owner will re-open operator shortlist after clarity on height cap.",
        [MAP_ODR_AIRTABLE.alignmentScore]: 73,
        [MAP_ODR_AIRTABLE.alignmentBand]: "Moderate",
        [MAP_ODR_AIRTABLE.dataConfidence]: "Incomplete",
        [MAP_ODR_AIRTABLE.requestSentAt]: isoDaysAgo(40),
        [MAP_ODR_AIRTABLE.lastUpdated]: isoDaysAgo(5),
      },
    },
    {
      key: "Declined",
      label: "Declined",
      ownerMessage:
        "Owner selected another operator path for this cycle; keeping GHL in mind for sister assets in the portfolio.",
      fields: {
        [MAP_ODR_AIRTABLE.status]: "Declined",
        [MAP_ODR_AIRTABLE.responseDate]: isoDaysAgo(7),
        [MAP_ODR_AIRTABLE.responseNotes]: "Owner prioritized incumbent operator for speed-to-market on this asset.",
        [MAP_ODR_AIRTABLE.alignmentScore]: 62,
        [MAP_ODR_AIRTABLE.alignmentBand]: "Limited",
        [MAP_ODR_AIRTABLE.dataConfidence]: "Operator-provided",
        [MAP_ODR_AIRTABLE.requestSentAt]: isoDaysAgo(35),
        [MAP_ODR_AIRTABLE.lastUpdated]: isoDaysAgo(7),
      },
    },
    {
      key: "Archived",
      label: "Archived",
      ownerMessage:
        "Owner archived after deal moved to hold — unrelated to operator quality; asset may reopen next year.",
      fields: {
        [MAP_ODR_AIRTABLE.status]: "Archived",
        [MAP_ODR_AIRTABLE.responseNotes]: "Project paused pending capital stack revision.",
        [MAP_ODR_AIRTABLE.alignmentScore]: 58,
        [MAP_ODR_AIRTABLE.alignmentBand]: "Limited",
        [MAP_ODR_AIRTABLE.dataConfidence]: "Incomplete",
        [MAP_ODR_AIRTABLE.requestSentAt]: isoDaysAgo(50),
        [MAP_ODR_AIRTABLE.lastUpdated]: isoDaysAgo(12),
      },
    },
  ];
}

async function fetchDealContext(base, dealId) {
  const deal = await base(DEALS_TABLE).find(dealId);
  const f = deal.fields || {};
  let lp = null;
  let cu = null;
  const locId = Array.isArray(f["Location & Property"]) ? f["Location & Property"][0] : null;
  const cuId = Array.isArray(f["Contact & Uploads"]) ? f["Contact & Uploads"][0] : null;
  if (locId) {
    try {
      lp = (await base(LOCATION_TABLE).find(locId)).fields;
    } catch (_e) {
      /* optional */
    }
  }
  if (cuId) {
    try {
      cu = (await base(CONTACT_TABLE).find(cuId)).fields;
    } catch (_e) {
      /* optional */
    }
  }
  const merged = mergeDealContextFields(f, lp, cu);
  return buildDealMetaFromFields(merged);
}

async function findDemoRow(base, demoKey) {
  const formula = `AND({${MAP_ODR_AIRTABLE.operatingCompanyName}} = '${escapeFormula(COMPANY)}', FIND('${escapeFormula(DEMO_PREFIX + " | " + demoKey)}', {${MAP_ODR_AIRTABLE.ownerNotes}}) > 0)`;
  const rows = await base(ODR_TABLE).select({ filterByFormula: formula, maxRecords: 1 }).firstPage();
  return rows[0] || null;
}

async function archiveNonDemoGhlRows(base) {
  const formula = `AND({${MAP_ODR_AIRTABLE.operatingCompanyName}} = '${escapeFormula(COMPANY)}', {${MAP_ODR_AIRTABLE.status}} != 'Archived', FIND('${escapeFormula(DEMO_PREFIX)}', {${MAP_ODR_AIRTABLE.ownerNotes}}) = 0)`;
  const toArchive = [];
  await base(ODR_TABLE)
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((records, next) => {
      toArchive.push(...records);
      next();
    });
  if (!toArchive.length) return 0;
  for (let i = 0; i < toArchive.length; i += 10) {
    const batch = toArchive.slice(i, i + 10).map((r) => ({
      id: r.id,
      fields: { [MAP_ODR_AIRTABLE.status]: "Archived" },
    }));
    await base(ODR_TABLE).update(batch);
  }
  return toArchive.length;
}

async function deleteDemoRows(base) {
  const formula = `FIND('${escapeFormula(DEMO_PREFIX)}', {${MAP_ODR_AIRTABLE.ownerNotes}}) > 0`;
  const ids = [];
  await base(ODR_TABLE)
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((records, next) => {
      ids.push(...records.map((r) => r.id));
      next();
    });
  for (let i = 0; i < ids.length; i += 10) {
    await base(ODR_TABLE).destroy(ids.slice(i, i + 10));
  }
  return ids.length;
}

async function deleteDemoActivity(base) {
  const formula = `AND({${MAP_ODR_AIRTABLE.operatingCompanyName}} = '${escapeFormula(COMPANY)}', FIND('${escapeFormula(DEMO_PREFIX)}', {Details}) > 0)`;
  const ids = [];
  await base(ACTIVITY_LOG_TABLE)
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((records, next) => {
      ids.push(...records.map((r) => r.id));
      next();
    });
  for (let i = 0; i < ids.length; i += 10) {
    await base(ACTIVITY_LOG_TABLE).destroy(ids.slice(i, i + 10));
  }
  return ids.length;
}

async function createActivity(base, dealId, action, details) {
  const fields = {
    Deal: [dealId],
    [MAP_ODR_AIRTABLE.operatingCompanyName]: COMPANY,
    Action: action,
    Details: `${DEMO_PREFIX} — ${details}`,
    "Created At": now,
    Stakeholder: "Owner",
  };
  try {
    await base(ACTIVITY_LOG_TABLE).create([{ fields }]);
  } catch (e) {
    if (/Unknown field|does not exist/i.test(String(e.message || "")) && fields.Stakeholder) {
      delete fields.Stakeholder;
      await base(ACTIVITY_LOG_TABLE).create([{ fields }]);
    } else {
      throw e;
    }
  }
}

async function resolveDealIds(base) {
  const envIds = (process.env.ODD_SEED_DEAL_IDS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (envIds.length) return envIds;

  const records = await base(DEALS_TABLE).select({ pageSize: 30, sort: [{ field: "Project Name", direction: "asc" }] }).all();
  return records.map((r) => r.id);
}

async function main() {
  const base = getBase();
  const clean = process.argv.includes("--clean");

  if (clean) {
    const nAct = await deleteDemoActivity(base);
    const nOdr = await deleteDemoRows(base);
    console.log(`Clean complete. Removed ${nOdr} ODR row(s), ${nAct} activity row(s).`);
    return;
  }

  let dealIds = await resolveDealIds(base);
  if (!dealIds.length) {
    console.error("No deals found. Import CALA sample deals or set ODD_SEED_DEAL_IDS.");
    process.exit(1);
  }

  const archived = await archiveNonDemoGhlRows(base);
  if (archived) console.log(`Archived ${archived} prior non-demo GHL row(s).`);

  const specs = demoSpecs();
  const bucketCounts = {};

  for (let i = 0; i < specs.length; i++) {
    const spec = specs[i];
    const dealId = dealIds[i % dealIds.length];
    const meta = await fetchDealContext(base, dealId);
    const ownerNotes = demoOwnerNote(spec.key, spec.ownerMessage);
    const fields = {
      ...spec.fields,
      [MAP_ODR_AIRTABLE.ownerNotes]: ownerNotes,
      [MAP_ODR_AIRTABLE.operatingCompanyName]: COMPANY,
      [MAP_ODR_AIRTABLE.operatorSetup]: [MASTER_ID],
    };

    const existing = await findDemoRow(base, spec.key);
    let recordId;
    try {
      if (existing) {
        await base(ODR_TABLE).update(existing.id, fields);
        recordId = existing.id;
        console.log(`[update] ${spec.label} → ${recordId} (${meta.projectName || dealId.slice(0, 8)}…)`);
      } else {
        const created = await createOdrRow(base, {
          dealId,
          operatingCompanyName: COMPANY,
          operatorSetupId: MASTER_ID,
          status: fields[MAP_ODR_AIRTABLE.status],
          alignmentScore: fields[MAP_ODR_AIRTABLE.alignmentScore],
          alignmentBand: fields[MAP_ODR_AIRTABLE.alignmentBand],
          dataConfidence: fields[MAP_ODR_AIRTABLE.dataConfidence],
          ownerNotes,
        });
        await base(ODR_TABLE).update(created.id, fields);
        recordId = created.id;
        console.log(`[create] ${spec.label} → ${recordId} (${meta.projectName || dealId.slice(0, 8)}…)`);
      }

      const row = {
        id: recordId,
        dealId,
        status: fields[MAP_ODR_AIRTABLE.status],
        ndaStatus: fields[MAP_ODR_AIRTABLE.ndaStatus] || "",
        dealRoomAccess: fields[MAP_ODR_AIRTABLE.dealRoomAccess] || "",
        requestSentAt: fields[MAP_ODR_AIRTABLE.requestSentAt],
        lastUpdated: fields[MAP_ODR_AIRTABLE.lastUpdated],
      };
      const bucket = enrichWorkspaceRow(row).workspaceBucket;
      bucketCounts[bucket] = (bucketCounts[bucket] || 0) + 1;
      console.log(`         bucket=${bucket} | ${meta.ownerCompany || "—"} | ${meta.country || "—"} | ${meta.rooms ?? "—"} keys`);
    } catch (e) {
      console.error(`[fail] ${spec.label}:`, e.message || e);
    }
  }

  const activityDealId = dealIds[0];
  try {
    await createActivity(
      base,
      activityDealId,
      "Request Sent",
      "Owner outreach sent to GHL for operating review — airport corridor reflag.",
    );
    await createActivity(
      base,
      activityDealId,
      "Brand Viewed",
      "GHL operator team opened workspace and reviewed alignment summary.",
    );
    await createActivity(
      base,
      activityDealId,
      "Notes updated",
      "Operator asked owner to confirm capex phasing and opening window.",
    );
    console.log(`[activity] Sample timeline on deal ${activityDealId.slice(0, 8)}…`);
  } catch (e) {
    console.warn("[activity] Skipped or partial:", e.message || e);
  }

  console.log("\nPipeline bucket counts:", bucketCounts);
  console.log("Done. Hard-refresh My Operator Deals.");
  console.log("To remove: node scripts/seed-operator-development-workspace-demo.mjs --clean");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
