import "dotenv/config";

const key = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
const id = "recLuxvwwxID7U2B8";

async function get(path) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${path}`, {
    headers: { Authorization: `Bearer ${key}` },
  });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = { raw: text.slice(0, 300) };
  }
  return { status: res.status, json };
}

const table = encodeURIComponent("Hotel Census");
const byId = await get(`${table}/${id}`);
console.log(
  JSON.stringify(
    {
      byIdStatus: byId.status,
      byIdName:
        byId.json?.fields?.name ||
        byId.json?.fields?.["Property Name"] ||
        byId.json?.error,
      fieldKeys: byId.json?.fields
        ? Object.keys(byId.json.fields).sort().slice(0, 25)
        : null,
    },
    null,
    2
  )
);

const formula = encodeURIComponent(`RECORD_ID()='${id}'`);
const listed = await get(`${table}?filterByFormula=${formula}&pageSize=1`);
console.log(
  JSON.stringify(
    {
      listStatus: listed.status,
      listCount: listed.json?.records?.length ?? null,
      listError: listed.json?.error || listed.json?.message || null,
      firstId: listed.json?.records?.[0]?.id || null,
    },
    null,
    2
  )
);

const formula2 = encodeURIComponent(`RECORD_ID() = '${id}'`);
const listed2 = await get(`${table}?filterByFormula=${formula2}&pageSize=1`);
console.log(
  JSON.stringify(
    {
      list2Status: listed2.status,
      list2Count: listed2.json?.records?.length ?? null,
      firstId: listed2.json?.records?.[0]?.id || null,
    },
    null,
    2
  )
);

// Total pages estimate: one page unfiltered
const page = await get(`${table}?pageSize=100&fields%5B%5D=name&fields%5B%5D=Latitude`);
console.log(
  JSON.stringify(
    {
      pageStatus: page.status,
      pageLen: page.json?.records?.length ?? null,
      hasBethesda: (page.json?.records || []).some((r) => r.id === id),
      error: page.json?.error || null,
    },
    null,
    2
  )
);
