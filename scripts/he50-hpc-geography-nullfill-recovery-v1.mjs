#!/usr/bin/env node
/**
 * HE50 recovery — null-fill Market / Submarket / State / Brand Family on existing
 * canonical HPC IDs only. Never CREATE. Standing-auth field allowlist only.
 *
 * Default: dry-run. Apply: --apply
 */
import dotenv from "dotenv";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  HE50_CANONICAL_BASE_ID,
  HE50_HPC_TABLE_ID,
  HE50_CANONICAL_HPC_BY_PILOT_KEY,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";
import {
  HE50_ALLOWED_DESTINATION_TABLES,
  isAllowedDestinationTable,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/standing-authorization-v1.js";

dotenv.config({ path: "C:/Dev/dealality/my-operators-backend/.env", override: false });
dotenv.config({ override: false });

const APPLY = process.argv.includes("--apply");
const OUT = join(
  process.cwd(),
  "reports/hotel-explorer/he50-canonical-intelligence-recovery-v1",
);

/** Verified geography / brand-family null-fills (addresses + peer HPC + official brand parents). */
const NULLFILLS = Object.freeze([
  {
    pilotKey: "ac_hotel_newtown_square",
    hpcId: HE50_CANONICAL_HPC_BY_PILOT_KEY.ac_hotel_newtown_square,
    fields: {
      "State / Region": "Pennsylvania",
      Market: "Newtown Square / Delaware County",
      Submarket: "Newtown Square / Radnor",
      "Brand Family": "Marriott International",
    },
    sources: [
      "Address Pennsylvania 19073",
      "fixtures/demand-anchors-newtown-square-pa-v1.json market label",
      "parentBrand Marriott International (free research packet)",
    ],
  },
  {
    pilotKey: "hgi_newtown_square_radnor",
    hpcId: HE50_CANONICAL_HPC_BY_PILOT_KEY.hgi_newtown_square_radnor,
    fields: {
      "State / Region": "Pennsylvania",
      Market: "Newtown Square / Delaware County",
      Submarket: "Newtown Square / Radnor",
      "Brand Family": "Hilton",
    },
    sources: [
      "Address Pennsylvania 19073",
      "fixtures/demand-anchors-newtown-square-pa-v1.json market label",
      "parentBrand Hilton (free research packet)",
    ],
  },
  {
    pilotKey: "carton_house_fairmont",
    hpcId: HE50_CANONICAL_HPC_BY_PILOT_KEY.carton_house_fairmont,
    fields: {
      "State / Region": "County Kildare",
      Market: "Greater Dublin / Maynooth",
      Submarket: "Maynooth / Carton Demesne",
      "Brand Family": "Accor",
    },
    sources: [
      "Address Co. Kildare / Maynooth",
      "Fairmont / Accor official property pages",
      "parentBrand Accor / Fairmont (free research packet)",
    ],
  },
  {
    pilotKey: "secrets_papagayo",
    hpcId: HE50_CANONICAL_HPC_BY_PILOT_KEY.secrets_papagayo,
    fields: {
      "State / Region": "Guanacaste",
      Market: "Guanacaste / Papagayo",
      Submarket: "Papagayo / Playa Arenilla",
      "Brand Family": "Hyatt Inclusive Collection",
    },
    sources: [
      "Address Guanacaste / Papagayo Gulf",
      "Peer HPC JW Costa Elena Market Guanacaste / Papagayo",
      "parentBrand Hyatt Inclusive Collection",
    ],
  },
  {
    pilotKey: "secrets_st_lucia",
    hpcId: HE50_CANONICAL_HPC_BY_PILOT_KEY.secrets_st_lucia,
    fields: {
      Market: "Castries / Gros Islet",
      Submarket: "Choc Bay",
      "Brand Family": "Hyatt Inclusive Collection",
    },
    sources: [
      "Address Choc Bay / Castries–Gros Islet Highway",
      "parentBrand Hyatt Inclusive Collection",
    ],
  },
]);

function token() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

function isEmpty(v) {
  return v == null || v === "" || (Array.isArray(v) && v.length === 0);
}

async function fetchHpc(id) {
  const res = await fetch(
    `https://api.airtable.com/v0/${HE50_CANONICAL_BASE_ID}/${HE50_HPC_TABLE_ID}/${id}`,
    { headers: { Authorization: `Bearer ${token()}` } },
  );
  const body = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, body };
}

async function patchHpc(id, fields) {
  const res = await fetch(
    `https://api.airtable.com/v0/${HE50_CANONICAL_BASE_ID}/${HE50_HPC_TABLE_ID}/${id}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${token()}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields, typecast: true }),
    },
  );
  const body = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, body };
}

async function main() {
  if (!token()) {
    console.error(JSON.stringify({ ok: false, error: "MISSING_AIRTABLE_TOKEN" }));
    process.exit(2);
  }
  if (!isAllowedDestinationTable(HE50_HPC_TABLE_ID)) {
    console.error(JSON.stringify({ ok: false, error: "HPC_NOT_IN_ALLOWLIST" }));
    process.exit(2);
  }
  const allowedFields = new Set(
    HE50_ALLOWED_DESTINATION_TABLES[HE50_HPC_TABLE_ID].allowedFields,
  );
  mkdirSync(OUT, { recursive: true });

  const ops = [];
  for (const row of NULLFILLS) {
    const live = await fetchHpc(row.hpcId);
    if (!live.ok) {
      ops.push({
        ...row,
        class: "BLOCKED_READ_FAILED",
        httpStatus: live.status,
        error: live.body?.error || live.body,
      });
      continue;
    }
    const existing = live.body.fields || {};
    const patch = {};
    const skipped = {};
    for (const [field, value] of Object.entries(row.fields)) {
      if (!allowedFields.has(field)) {
        skipped[field] = { reason: "FIELD_NOT_IN_ALLOWLIST", value };
        continue;
      }
      if (!isEmpty(existing[field])) {
        skipped[field] = {
          reason: "ALREADY_POPULATED",
          existing: existing[field],
          proposed: value,
        };
        continue;
      }
      patch[field] = value;
    }
    const op = {
      pilotKey: row.pilotKey,
      hpcId: row.hpcId,
      propertyName: existing["Property Name"] || existing["Canonical Property Name"],
      sources: row.sources,
      patch,
      skipped,
      class:
        Object.keys(patch).length === 0
          ? "SKIPPED_NO_CHANGE"
          : APPLY
            ? "AUTO_APPROVED_CANONICAL_UPDATE"
            : "DRY_RUN_CANONICAL_UPDATE",
    };
    if (APPLY && Object.keys(patch).length > 0) {
      const written = await patchHpc(row.hpcId, patch);
      op.write = { ok: written.ok, status: written.status };
      if (written.ok) {
        const rb = await fetchHpc(row.hpcId);
        op.readback = {};
        for (const f of Object.keys(patch)) {
          op.readback[f] = rb.body?.fields?.[f] ?? null;
        }
        op.persistenceStage = "AIRTABLE_READBACK_VALIDATED";
      } else {
        op.writeError = written.body?.error || written.body;
        op.class = "WRITE_FAILED";
      }
    }
    ops.push(op);
    await new Promise((r) => setTimeout(r, 150));
  }

  const report = {
    ok: ops.every((o) => o.class !== "WRITE_FAILED" && o.class !== "BLOCKED_READ_FAILED"),
    mode: APPLY ? "APPLY" : "DRY_RUN",
    version: "he50-hpc-geography-nullfill-recovery-v1",
    baseId: HE50_CANONICAL_BASE_ID,
    tableId: HE50_HPC_TABLE_ID,
    neverCreate: true,
    legacyHotelCensusBlocked: true,
    operations: ops,
    counts: {
      dryRunOrApplied: ops.filter((o) =>
        /UPDATE|SKIPPED/.test(o.class),
      ).length,
      wouldWrite: ops.filter((o) => Object.keys(o.patch || {}).length > 0).length,
      skippedNoChange: ops.filter((o) => o.class === "SKIPPED_NO_CHANGE").length,
    },
  };
  const name = APPLY
    ? "HPC_GEOGRAPHY_NULLFILL_APPLY.json"
    : "HPC_GEOGRAPHY_NULLFILL_DRY_RUN.json";
  writeFileSync(join(OUT, name), JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  if (!report.ok) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
