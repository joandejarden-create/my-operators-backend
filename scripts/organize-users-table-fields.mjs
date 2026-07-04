#!/usr/bin/env node
/**
 * Organize Users table fields into logical sections.
 *
 * Airtable does not expose table-wide field reorder via API. This script:
 * 1. Writes docs/users-table-field-order.md (drag order for Manage fields in Airtable)
 * 2. Optionally PATCHes field descriptions with section labels (Manage fields sidebar)
 *
 *   node scripts/organize-users-table-fields.mjs
 *   node scripts/organize-users-table-fields.mjs --apply-descriptions
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { dirname } from "path";
import { PLATFORM_USERS_TABLE_ID } from "../lib/airtable/platform-users-table.js";

const SECTIONS = [
  {
    title: "1 — Identity & login",
    names: [
      "User_ID",
      "Record_ID",
      "Email",
      "Company Email",
      "Unique_Webflow_ID",
      "Slug",
      "Memberstack ID",
      "First Name",
      "Last Name",
      "Profile",
    ],
  },
  {
    title: "2 — Company & role",
    names: [
      "Company",
      "Company Profile",
      "Company Title",
      "Title",
      "Platform Role",
      "User Type",
      "Company Name",
    ],
  },
  {
    title: "3 — Contact",
    names: ["Phone Number", "Based (Country)"],
  },
  {
    title: "4 — Access & visibility",
    names: ["Contact Visibility", "Deal Access", "Document Access"],
  },
  {
    title: "5 — Regions & coverage",
    names: [
      "Region - America",
      "Region - Caribbean & Latin America",
      "Region - Europe",
      "Region - Middle East & Africa",
      "Region - Asia Pacific",
      "Coverage Territories",
      "Languages",
      "HO - PI - Regions Where You Operate / Invest",
    ],
  },
  {
    title: "6 — Partner directory metrics",
    names: ["Closed Deals", "Unique Brands (Deals)", "Submitted Bids", "Brands Supported"],
  },
  {
    title: "7 — Responsiveness badge",
    names: [
      "responsiveness_response_time_category",
      "responsiveness_response_time_icon",
      "responsiveness_frequency_category",
      "responsiveness_frequency_icon",
      "responsiveness_combined_badge",
    ],
  },
  {
    title: "8 — People links",
    names: ["Added_By_User", "From field: Added_By_User"],
  },
  {
    title: "9 — Deals & workflow (linked records)",
    names: [
      "Deals",
      "Active Deals",
      "Archived Deals",
      "Deals Visited",
      "Received Deals",
      "Declined Deals",
      "Deal Interactions",
      "Deal Status History",
      "Deal Actions",
      "Deal Actions 2",
      "Outreach Setup",
      "Hotel Ownership",
    ],
  },
  {
    title: "10 — Brands & favorites",
    names: [
      "Brand Setup - Brand Basics",
      "Brand Explorer Favorites",
      "User_Favorites",
      "User_Favorites 2",
      "Brand Deal Preferences",
    ],
  },
  {
    title: "11 — Signup / profile (general)",
    names: [
      "Reason to Join Platform",
      "How Did You Hear About Us",
      "Company Overview",
      "Units of Measurement",
    ],
  },
  {
    title: "12 — Hotel owner intake (HO)",
    match: (name) => name.startsWith("HO "),
  },
  {
    title: "13 — Hotel brand intake (HB)",
    match: (name) => name.startsWith("HB "),
  },
  {
    title: "14 — Legacy (hide in views when possible)",
    names: [
      "User Management",
      "Company Profile 2",
      "Operator Setup - Profile & Positioning",
    ],
    description: "Legacy User Management era — prefer Company + Users links. Hide in daily views.",
  },
  {
    title: "15 — System",
    names: ["Created", "Last Modified"],
  },
];

async function metaFetch(path, init = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

function buildFieldOrder(allFields) {
  const byName = new Map(allFields.map((f) => [f.name, f]));
  const used = new Set();
  const ordered = [];
  const sectionRows = [];

  const addField = (f, sectionTitle, descNote) => {
    if (!f || used.has(f.id)) return;
    used.add(f.id);
    ordered.push(f);
    sectionRows.push({ section: sectionTitle, name: f.name, id: f.id, note: descNote || "" });
  };

  for (const section of SECTIONS) {
    if (section.names) {
      for (const name of section.names) {
        addField(byName.get(name), section.title, section.description);
      }
    }
    if (section.match) {
      const matches = allFields
        .filter((f) => !used.has(f.id) && section.match(f.name))
        .sort((a, b) => a.name.localeCompare(b.name));
      for (const f of matches) {
        addField(f, section.title, section.description);
      }
    }
  }

  const remaining = allFields
    .filter((f) => !used.has(f.id))
    .sort((a, b) => a.name.localeCompare(b.name));
  for (const f of remaining) {
    addField(f, "16 — Other", "");
  }

  return { ordered, sectionRows };
}

function markdownDoc(sectionRows) {
  const lines = [
    "# Users table — recommended field order",
    "",
    "Use this order in Airtable: open **Users** → **Manage fields** (or field dropdown) → drag fields to match the list below.",
    "",
    "Then duplicate **Grid view** as **Platform users** and hide sections 12–14 if you only manage platform accounts daily.",
    "",
  ];
  let lastSection = "";
  let n = 0;
  for (const row of sectionRows) {
    if (row.section !== lastSection) {
      lines.push(`## ${row.section}`, "");
      lastSection = row.section;
    }
    n++;
    lines.push(`${n}. **${row.name}** \`${row.id}\`${row.note ? ` — ${row.note}` : ""}`);
  }
  lines.push("", "---", "", "Generated by `node scripts/organize-users-table-fields.mjs`.");
  return lines.join("\n");
}

async function applyDescriptions(tableId, sectionRows) {
  const byName = new Map(sectionRows.map((r) => [r.id, r]));
  const { res: listRes, json: listJson } = await metaFetch("/tables");
  if (!listRes.ok) throw new Error(JSON.stringify(listJson));
  const table = (listJson.tables || []).find((t) => t.id === tableId);
  if (!table) throw new Error("Users table not found");

  let updated = 0;
  for (const field of table.fields || []) {
    const row = byName.get(field.id);
    if (!row) continue;
    const sectionLabel = row.section.replace(/^[\d\s—-]+/, "").trim();
    const desc = row.note
      ? `[${row.section}] ${row.note}`
      : `[${row.section}] ${sectionLabel}`;
    if (field.description === desc) continue;

    const { res, json } = await metaFetch(`/tables/${tableId}/fields/${field.id}`, {
      method: "PATCH",
      body: JSON.stringify({ description: desc }),
    });
    if (!res.ok) {
      console.warn("Skip description", field.name, json);
      continue;
    }
    console.log("Description:", field.name);
    updated++;
    await new Promise((r) => setTimeout(r, 220));
  }
  console.log(`Updated ${updated} field description(s).`);
}

async function main() {
  const apply = process.argv.includes("--apply-descriptions");
  const { res, json } = await metaFetch("/tables");
  if (!res.ok) throw new Error(JSON.stringify(json));

  const table = (json.tables || []).find((t) => t.id === PLATFORM_USERS_TABLE_ID);
  if (!table) throw new Error(`Table ${PLATFORM_USERS_TABLE_ID} not found`);

  const { ordered, sectionRows } = buildFieldOrder(table.fields || []);
  const docPath = "docs/users-table-field-order.md";
  mkdirSync(dirname(docPath), { recursive: true });
  writeFileSync(docPath, markdownDoc(sectionRows), "utf8");

  console.log(`Wrote ${docPath} (${ordered.length} fields in ${SECTIONS.length} sections).`);
  console.log("\nAirtable UI: Users table → Manage fields → drag to match the doc order.\n");

  if (apply) {
    await applyDescriptions(table.id, sectionRows);
  } else {
    console.log("Optional: node scripts/organize-users-table-fields.mjs --apply-descriptions");
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
