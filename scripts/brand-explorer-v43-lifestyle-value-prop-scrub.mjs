#!/usr/bin/env node
/**
 * Post-v43: rewrite stub Brand Value Proposition / Key Brand Differentiators
 * that render as forbidden incomplete chips on external full profiles.
 */
import "dotenv/config";

const TABLE = "Brand Setup - Brand Basics";

const UPDATES = {
  "hotel-indigo": {
    recordId: "recegXrqaPiSLGCIe",
    fields: {
      "Brand Value Proposition":
        "Hotel Indigo is an IHG lifestyle brand for owners who want neighborhood storytelling and local discovery—guest experience shaped by place, not a shared InterContinental shell.",
      "Key Brand Differentiators":
        "Neighborhood-led design and local discovery; IHG One Rewards distribution; lifestyle boutique character within the IHG system; conversion-capable urban assets with a clear local narrative.",
    },
  },
  "mgallery-collection": {
    recordId: "recrWCD1LMqu864oU",
    fields: {
      "Brand Value Proposition":
        "MGallery Collection is Accor’s soft collection for distinctive hotels that keep local character while gaining Accor distribution and ALL loyalty—collection fit over a standardized full-service prototype.",
      "Key Brand Differentiators":
        "Property-unique identity within Accor; ALL loyalty participation; soft-collection conversion and repositioning path; design review that protects place-specific character.",
    },
  },
  "small-luxury-hotels-of-the-world": {
    recordId: "recjjSnY2opb8P4DG",
    fields: {
      "Brand Value Proposition":
        "Small Luxury Hotels of the World is an independent luxury consortium for intimate-scale, owner-operated hotels seeking selective membership, quality recognition, and distribution without a franchise chain prototype.",
      "Key Brand Differentiators":
        "Independent ownership and intimate scale; selective membership quality bar; consortium distribution and recognition; property-level character over chain standardization.",
    },
  },
};

async function main() {
  const dryRun = !process.argv.includes("--apply");
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  // Forbidden substring check
  const forbidden = ["neighborhood focus", "boutique design", "conversion-friendly."];
  for (const [slug, u] of Object.entries(UPDATES)) {
    const blob = Object.values(u.fields).join("\n");
    for (const f of forbidden) {
      if (blob.toLowerCase().includes(f.toLowerCase())) {
        throw new Error(`${slug} still contains forbidden stub phrase: ${f}`);
      }
    }
  }

  console.log(`[vp-scrub] dryRun=${dryRun}`);
  for (const [slug, u] of Object.entries(UPDATES)) {
    console.log(`  ${slug}:`, u.fields);
    if (dryRun) continue;
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}/${u.recordId}`;
    const res = await fetch(url, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: u.fields }),
    });
    if (!res.ok) {
      throw new Error(`${slug} PATCH failed: ${res.status} ${(await res.text()).slice(0, 400)}`);
    }
    console.log(`  wrote ${slug}`);
    await new Promise((r) => setTimeout(r, 250));
  }
  console.log(dryRun ? "Dry-run only." : "Apply complete.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
