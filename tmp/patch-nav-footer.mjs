import fs from "fs";

const live = JSON.parse(fs.readFileSync("tmp/set-footer-args.json", "utf8"));
const content = live.actions[0].set_site_freeform_code.content;

const next = content
  .replace(
    "<!-- ohNavCleanup30d: How It Works → Platform → FAQs → Insights -->",
    "<!-- ohNavCleanup01a: How It Works → Benefits → Platform → FAQs → Insights -->"
  )
  .replace(
    "6a6cb2a7a1f107bfa024522b_dealality-old-home-nav-cleanup.v20260730d.js",
    "6a6d7f7556632564b9f5e1a7_dealality-old-home-nav-cleanup.v20260801a.js"
  )
  .replace(
    "sha384-f1CeGOPO3LNQbUk7tZ3wZ2SbR5IlF+sn5wtAWpdu4fyle03MEJq/Rk+ZDKRQDZCb",
    "sha384-QO2axCI10Rg0ZCV4HxK/PRrlhuPCZo4wzeysmISBk8eybtImNE2ttNQ2glSrPg18"
  );

if (!next.includes("v20260801a.js") || !next.includes("modules-copy.v20260730i.js")) {
  console.error("patch failed", {
    has01a: next.includes("v20260801a.js"),
    has30i: next.includes("modules-copy.v20260730i.js"),
    has30h: next.includes("modules-copy.v20260730h.js"),
    has30d: next.includes("v20260730d.js"),
  });
  process.exit(1);
}

fs.writeFileSync("tmp/site-footer-freeform-updated.html", next);
fs.writeFileSync(
  "tmp/set-footer-nav-payload.json",
  JSON.stringify({
    actions: [
      {
        label: "set_footer_nav",
        set_site_freeform_code: {
          site_id: "68108c29063eeb5d1bd7ae4a",
          location: "footer",
          content: next,
        },
      },
    ],
    context:
      "Updates site footer nav-cleanup script to restore Benefits link on published Old Home.",
  })
);
console.log("ok", next.length);
