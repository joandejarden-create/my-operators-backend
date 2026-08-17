import "dotenv/config";

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const table = "Brand Setup - Brand Explorer Presentation";
const rec = "reclHRYT05I4VkWJN";
const src =
  "https://cache.marriott.com/content/dam/marriott-renditions/MSPAK/mspak-exterior-3494-hor-wide.jpg";

async function tryUrl(u, label) {
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${rec}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: { Image: [{ url: u }] } }),
    }
  );
  const j = await res.json();
  await new Promise((r) => setTimeout(r, 1200));
  const g = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${rec}`,
    { headers: { Authorization: `Bearer ${apiKey}` } }
  );
  const gj = await g.json();
  console.log(label, {
    patchHas: Boolean(j.fields?.Image),
    getHas: Boolean(gj.fields?.Image),
    patchErr: j.error,
    url: (gj.fields?.Image?.[0]?.url || "").slice(0, 80),
  });
}

await tryUrl(`https://wsrv.nl/?url=${encodeURIComponent(src)}&w=1600&output=jpg`, "wsrv");
await tryUrl(
  `https://images.weserv.nl/?url=${encodeURIComponent(src.replace(/^https?:\/\//, ""))}&w=1600`,
  "weserv"
);
