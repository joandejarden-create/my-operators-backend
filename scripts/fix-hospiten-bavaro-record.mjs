#!/usr/bin/env node
import "../load-env.js";
import { DEMAND_ANCHORS_FIELDS as F } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import { getDemandAnchorsAirtableConfig, resolveDemandAnchorsTableName } from "../lib/demand-anchors/demand-anchors-base.js";

const cfg = getDemandAnchorsAirtableConfig();
const table = await resolveDemandAnchorsTableName(cfg.baseId, cfg.apiKey);
const recs = await cfg
  .base(table)
  .select({
    filterByFormula: "FIND('Hospiten', {" + F.name + "})",
    maxRecords: 5,
  })
  .all();

if (!recs.length) {
  console.error("No Hospiten record found");
  process.exit(1);
}

for (const r of recs) {
  await cfg.base(table).update(
    r.id,
    {
      [F.name]: "Hospiten Bávaro",
      [F.lat]: 18.5989,
      [F.lng]: -68.4143,
      [F.city]: "Punta Cana",
      [F.address]: "Carretera Higüey - Punta Cana km 106, Verón",
      [F.sourceReference]: "https://hospiten.com/en/hospitals/hospiten-bavaro",
      [F.lastVerified]: "2026-06-22",
    },
    { typecast: true }
  );
  console.log("Updated:", r.fields[F.name], "→ Hospiten Bávaro");
}
