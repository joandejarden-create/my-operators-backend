/**
 * Audit all Live Memberstack members vs Airtable Users.
 * Reports duplicates, missing links, and wrong resolveDealalityUser targets.
 *
 * Usage:
 *   node scripts/audit-memberstack-airtable-users.mjs
 *   node scripts/audit-memberstack-airtable-users.mjs --fix-duplicates
 *   node scripts/audit-memberstack-airtable-users.mjs --json report.json
 */
import "../load-env.js";
import fs from "fs";
import Airtable from "airtable";
import axios from "axios";
import {
  INTAKE_USERS_UNIQUE_WEBFLOW_ID,
  INTAKE_USERS_EMAIL,
} from "../api/schemas/intake-deal-fields.js";
import { cellToString, extractLinkedRecordIds } from "../lib/airtable-utils.js";
import { resolveDealalityUser } from "../lib/dealality/resolve-user.js";
import { dealRecordAllowedForUser } from "../lib/dealality/deal-record-access.js";

const USERS_TABLE = "tbl6shiyz2wdUqE5F";
const DEALS_TABLE = "tblbvSxjiIhXzW6XW";
const SLUG_FIELD = process.env.AIRTABLE_USERS_SLUG_FIELD || "fldEgbHu5MvfyrxgE";
const FIX = process.argv.includes("--fix-duplicates");
const jsonOut = process.argv.find((a) => a.startsWith("--json"));
const jsonPath = jsonOut ? process.argv[process.argv.indexOf(jsonOut) + 1] : null;

function memberEmail(member) {
  return (
    (typeof member?.auth?.email === "string" && member.auth.email.trim().toLowerCase()) ||
    (typeof member?.email === "string" && member.email.trim().toLowerCase()) ||
    ""
  );
}

function readMsIdFromFields(fields) {
  return (
    cellToString(fields?.Unique_Webflow_ID) ||
    cellToString(fields?.["Unique Webflow ID"]) ||
    cellToString(fields?.[INTAKE_USERS_UNIQUE_WEBFLOW_ID]) ||
    cellToString(fields?.Slug) ||
    cellToString(fields?.slug) ||
    ""
  );
}

async function listAllMemberstackMembers(apiKey) {
  const BASE = (process.env.MEMBERSTACK_BASE_URL || "https://admin.memberstack.com").replace(
    /\/$/,
    ""
  );
  const headers = { "X-API-KEY": apiKey, "Content-Type": "application/json" };
  const all = [];
  let after = undefined;
  let page = 0;

  while (page < 200) {
    const params = { limit: 100, order: "ASC" };
    if (after != null) params.after = after;
    const res = await axios.get(`${BASE}/members`, {
      headers,
      params,
      validateStatus: () => true,
    });
    if (res.status !== 200) {
      throw new Error(`Memberstack list failed HTTP ${res.status}: ${JSON.stringify(res.data).slice(0, 200)}`);
    }
    const body = res.data || {};
    const batch = Array.isArray(body.data) ? body.data : [];
    all.push(...batch);
    if (!body.hasNextPage) break;
    after = body.endCursor;
    page += 1;
    if (after == null) break;
  }
  return all;
}

function summarizeUserRow(rec) {
  const f = rec.fields || {};
  return {
    recordId: rec.id,
    email: cellToString(f.Email) || cellToString(f[INTAKE_USERS_EMAIL]) || "",
    memberstackId: readMsIdFromFields(f),
    slug: cellToString(f.Slug) || cellToString(f.slug) || "",
    dealsCount: Array.isArray(f.Deals) ? f.Deals.length : 0,
    companyIds: extractLinkedRecordIds(f["Company Profile"]),
    platformRole: cellToString(f["Platform Role"]) || cellToString(f["User Type"]) || "",
  };
}

async function countAllowedDeals(userRecordId, companyIds) {
  const dealalityUser = {
    isAdmin: false,
    isOwner: true,
    userRecordId,
    companyId: companyIds[0] || null,
    companyIds,
  };
  let allowed = 0;
  await new Airtable({ apiKey: process.env.AIRTABLE_API_KEY })
    .base(process.env.AIRTABLE_BASE_ID)(DEALS_TABLE)
    .select({ pageSize: 100 })
    .eachPage((records, next) => {
      for (const rec of records) {
        if (dealRecordAllowedForUser(rec.fields, dealalityUser)) allowed += 1;
      }
      next();
    });
  return allowed;
}

async function findUsersByMemberstackId(base, memberstackId) {
  const esc = String(memberstackId).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  const formula = `OR({${INTAKE_USERS_UNIQUE_WEBFLOW_ID}} = '${esc}', {Slug} = '${esc}')`;
  return base(USERS_TABLE).select({ filterByFormula: formula, maxRecords: 20 }).firstPage();
}

async function findUsersByEmail(base, email) {
  const lit = String(email).trim().toLowerCase().replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  if (!lit) return [];
  const formula = `LOWER({Email}) = '${lit}'`;
  return base(USERS_TABLE).select({ filterByFormula: formula, maxRecords: 20 }).firstPage();
}

async function clearMemberstackIdFromRow(base, recordId) {
  const patch = {
    [INTAKE_USERS_UNIQUE_WEBFLOW_ID]: "",
    [SLUG_FIELD]: "",
    Unique_Webflow_ID: "",
    Slug: "",
  };
  await base(USERS_TABLE).update(recordId, patch, { typecast: true });
}

function pickKeepRow(rows) {
  const summaries = rows.map(summarizeUserRow);
  summaries.sort((a, b) => {
    if (b.dealsCount !== a.dealsCount) return b.dealsCount - a.dealsCount;
    if (Boolean(a.email) !== Boolean(b.email)) return a.email ? -1 : 1;
    if (Boolean(a.platformRole) !== Boolean(b.platformRole)) return a.platformRole ? -1 : 1;
    return a.recordId.localeCompare(b.recordId);
  });
  return summaries[0];
}

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;
const msKey = (process.env.MEMBERSTACK_SECRET_KEY || "").trim();

if (!apiKey || !baseId) {
  console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
  process.exit(1);
}
if (!msKey || msKey.startsWith("sk_sb_")) {
  console.error("Set MEMBERSTACK_SECRET_KEY to Live (sk_…) for production member audit.");
  process.exit(1);
}

const base = new Airtable({ apiKey }).base(baseId);
console.log("Fetching Live Memberstack members…");
const members = await listAllMemberstackMembers(msKey);
console.log("Memberstack members:", members.length);

const report = {
  auditedAt: new Date().toISOString(),
  memberstackCount: members.length,
  ok: [],
  issues: [],
  fixed: [],
};

for (const member of members) {
  const msId = member.id;
  const email = memberEmail(member);
  const cf = member.customFields || {};
  const airtableCf =
    cf["air-table-user-id"] || cf["airtable-user-id"] || cf["AirTable User ID"] || cf.airtableUserId || "";

  const byMs = await findUsersByMemberstackId(base, msId);
  const byEmail = email ? await findUsersByEmail(base, email) : [];
  const resolved = await resolveDealalityUser({ memberstackId: msId, email: email || null });

  const msSummaries = byMs.map(summarizeUserRow);
  const emailSummaries = byEmail.map(summarizeUserRow);

  let allowedDeals = null;
  if (resolved.found && resolved.userRecordId) {
    allowedDeals = await countAllowedDeals(
      resolved.userRecordId,
      resolved.companyIds || (resolved.companyId ? [resolved.companyId] : [])
    );
  }

  const entry = {
    memberstackId: msId,
    email: email || null,
    verified: member.verified === true,
    plans: (member.planConnections || member.plans || [])
      .map((p) => p.planId || p.id || p.name)
      .filter(Boolean),
    memberstackAirtableCf: airtableCf || null,
    airtableMatchesByMsId: msSummaries,
    airtableMatchesByEmail: emailSummaries,
    resolvedUserRecordId: resolved.found ? resolved.userRecordId : null,
    resolvedCanAccessOwner: resolved.found ? resolved.canAccessOwnerWorkspace : null,
    myDealsCount: allowedDeals,
  };

  const problems = [];

  if (!resolved.found) {
    problems.push("no_airtable_user_for_login");
  }
  if (byMs.length === 0) {
    problems.push("memberstack_id_not_on_any_users_row");
  }
  if (byMs.length > 1) {
    problems.push("duplicate_memberstack_id_on_users");
  }
  if (email && byEmail.length > 1) {
    problems.push("duplicate_email_on_users");
  }
  if (email && byEmail.length === 1 && byMs.length >= 1) {
    const emailId = byEmail[0].id;
    const msIds = new Set(byMs.map((r) => r.id));
    if (!msIds.has(emailId) && byMs.length === 1) {
      problems.push("email_row_differs_from_memberstack_id_row");
    }
  }
  if (airtableCf && resolved.found && airtableCf !== resolved.userRecordId) {
    problems.push("memberstack_custom_field_points_to_different_row");
  }
  if (resolved.found && allowedDeals === 0 && msSummaries.some((s) => s.dealsCount > 0)) {
    problems.push("deals_on_non_resolved_row");
  }
  if (resolved.found && !resolved.canAccessOwnerWorkspace && !resolved.isAdmin) {
    problems.push("no_owner_workspace_access");
  }

  if (problems.length) {
    entry.problems = problems;
    report.issues.push(entry);

    if (FIX && problems.includes("duplicate_memberstack_id_on_users")) {
      const keep = pickKeepRow(byMs);
      const toClear = msSummaries.filter((s) => s.recordId !== keep.recordId);
      for (const row of toClear) {
        await clearMemberstackIdFromRow(base, row.recordId);
        report.fixed.push({
          action: "cleared_memberstack_id",
          memberstackId: msId,
          clearedRecordId: row.recordId,
          keptRecordId: keep.recordId,
        });
        console.log(
          `FIXED duplicate ${msId}: cleared ${row.recordId}, kept ${keep.recordId} (${keep.dealsCount} deals)`
        );
      }
    }
  } else {
    report.ok.push({
      memberstackId: msId,
      email,
      resolvedUserRecordId: resolved.userRecordId,
      myDealsCount: allowedDeals,
    });
  }
}

// Airtable rows with mem_ id that don't exist in Memberstack Live (stale/orphan ids)
const msIdSet = new Set(members.map((m) => m.id));
const staleAirtable = [];
await base(USERS_TABLE)
  .select({ pageSize: 100 })
  .eachPage((records, next) => {
    for (const rec of records) {
      const ms = readMsIdFromFields(rec.fields);
      if (ms && ms.startsWith("mem_") && !ms.startsWith("mem_sb_") && !msIdSet.has(ms)) {
        staleAirtable.push(summarizeUserRow(rec));
      }
    }
    next();
  });

report.staleAirtableMemberstackIds = staleAirtable;

console.log("\n=== Audit summary ===");
console.log("OK:", report.ok.length);
console.log("Issues:", report.issues.length);
console.log("Fixed:", report.fixed.length);
console.log("Stale Airtable mem_ ids (not in Live Memberstack):", staleAirtable.length);

if (report.issues.length) {
  console.log("\n--- Issues ---");
  for (const item of report.issues) {
    console.log(`\n${item.email || "(no email)"} | ${item.memberstackId}`);
    console.log("  problems:", item.problems.join(", "));
    console.log(
      "  MS matches:",
      item.airtableMatchesByMsId.map((r) => `${r.recordId} (deals:${r.dealsCount}, email:${r.email || "-"})`).join("; ") ||
        "(none)"
    );
    console.log("  resolves to:", item.resolvedUserRecordId || "(not found)", "| My Deals:", item.myDealsCount);
    if (item.memberstackAirtableCf) {
      console.log("  MS custom field AirTable User ID:", item.memberstackAirtableCf);
    }
  }
}

if (staleAirtable.length) {
  console.log("\n--- Stale Airtable Memberstack IDs ---");
  for (const row of staleAirtable) {
    console.log(`  ${row.recordId} | ${row.email || "-"} | ${row.memberstackId} | deals:${row.dealsCount}`);
  }
}

if (jsonPath) {
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  console.log("\nWrote", jsonPath);
}

process.exit(report.issues.length && !FIX ? 1 : 0);
