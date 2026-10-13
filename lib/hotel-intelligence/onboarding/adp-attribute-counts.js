/**
 * Count active Hotel ADP Attributes for an HPC hotel (intelligence base).
 */

import Airtable from "airtable";
import { CANONICAL_INTELLIGENCE_BASE_ID } from "../../decision-outcomes/airtable-base.js";
import {
  HOTEL_ADP_ATTRIBUTES_TABLE,
  MAP_HOTEL_ADP_ATTRIBUTE as M,
} from "../adp-attributes/field-map.js";

function getBase() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
  const baseId = (
    process.env.ADP_AIRTABLE_BASE_ID ||
    process.env.AIRTABLE_INTELLIGENCE_BASE_ID ||
    CANONICAL_INTELLIGENCE_BASE_ID ||
    "appa2cE7FTRmIbB32"
  ).trim();
  if (!token) throw new Error("missing_airtable_token");
  return new Airtable({ apiKey: token }).base(baseId);
}

export async function countActiveAdpAttributes(hpcHotelId) {
  const base = getBase();
  const table = base(HOTEL_ADP_ATTRIBUTES_TABLE);
  const rows = [];
  const formula = `AND({${M.hpcHotelId}} = "${String(hpcHotelId).replace(/"/g, '\\"')}", {${M.active}} = TRUE())`;
  await table
    .select({ filterByFormula: formula, pageSize: 100, fields: [M.hpcHotelId, M.active, M.attributeKey] })
    .eachPage((records, next) => {
      for (const r of records) rows.push(r);
      next();
    });
  return rows.length;
}
