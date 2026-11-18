import "dotenv/config";
import Airtable from "airtable";
import {
  MAP_HOTEL_FIELDS,
  formatMapHotelRecord,
} from "../../../../lib/hotel-census/map-hotel-dto.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const id = "recLuxvwwxID7U2B8";

const rec = await base("Hotel Census").find(id);
const f = rec.fields || {};
console.log(
  JSON.stringify(
    {
      allFieldCount: Object.keys(f).length,
      hasLowerName: Object.prototype.hasOwnProperty.call(f, "name"),
      lowerName: f.name ?? null,
      propertyName: f["Property Name"] ?? null,
      mapFields: Object.fromEntries(
        MAP_HOTEL_FIELDS.map((k) => [
          k,
          Object.prototype.hasOwnProperty.call(f, k)
            ? typeof f[k] === "string"
              ? f[k]
              : typeof f[k]
            : "MISSING",
        ])
      ),
      formattedFromFind: formatMapHotelRecord(rec),
    },
    null,
    2
  )
);

const formula = `RECORD_ID()='${id}'`;
const projected = await base("Hotel Census")
  .select({ fields: [...MAP_HOTEL_FIELDS], filterByFormula: formula, pageSize: 1 })
  .all();
console.log(
  JSON.stringify(
    {
      projectedCount: projected.length,
      projectedKeys: projected[0] ? Object.keys(projected[0].fields) : null,
      projectedFormatted: projected[0]
        ? formatMapHotelRecord(projected[0])
        : null,
    },
    null,
    2
  )
);

const usTitle = await base("Hotel Census")
  .select({
    filterByFormula: "{Country}='United States'",
    fields: ["Property Name", "Country", "City"],
    maxRecords: 5,
  })
  .all();
console.log(
  JSON.stringify(
    {
      usTitleCount: usTitle.length,
      sample: usTitle.map((r) => r.fields["Property Name"]),
    },
    null,
    2
  )
);

try {
  const usLower = await base("Hotel Census")
    .select({
      filterByFormula: "{country}='United States'",
      fields: ["name", "country", "city"],
      maxRecords: 5,
    })
    .all();
  console.log(
    JSON.stringify(
      {
        usLowerCount: usLower.length,
        sample: usLower.map((r) => r.fields.name),
      },
      null,
      2
    )
  );
} catch (err) {
  console.log(
    JSON.stringify({ usLowerError: String(err.message || err).slice(0, 200) })
  );
}
