import "dotenv/config";

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const table = "Brand Setup - Brand Explorer Presentation";
const ids = ["reclHRYT05I4VkWJN", "receLLDZVsAA1ZMtJ", "recxDGhforwwQ7gFg"];

for (const id of ids) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${id}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const j = await res.json();
  const f = j.fields || {};
  console.log(
    JSON.stringify(
      {
        id,
        status: res.status,
        slot: f["Slot Key"],
        title: f.Title,
        image: f.Image,
        err: j.error,
      },
      null,
      2
    )
  );
}

// Try a single PATCH with known working Marriott URL
const testUrl =
  "https://cache.marriott.com/content/dam/marriott-renditions/MSPAK/mspak-exterior-3494-hor-wide.jpg?output-quality=70&interpolation=progressive-bilinear&downsize=1336px:*";
const patchRes = await fetch(
  `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/reclHRYT05I4VkWJN`,
  {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields: { Image: [{ url: testUrl }] } }),
  }
);
const patchJson = await patchRes.json();
console.log(
  "repatch status",
  patchRes.status,
  "image",
  JSON.stringify(patchJson.fields?.Image)?.slice(0, 300),
  "err",
  patchJson.error
);
