import fs from "fs";
import path from "path";
import crypto from "crypto";

const root = process.cwd();
const hotel =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bde85c014ee4e80e65c24_deal-desk-coastal-hotel-480.jpg";

const probe = `<!-- polish-v2 probe -->
<link id="oh-deal-desk" rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bdf56570ac7789fe12096_oh-deal-desk-polish-v2.css">
<div class="dealality-problem-desk" data-visual="polish-v2">probe</div>`;

const stamp = "20260731";
const backupPath = path.join(
  root,
  `docs/old-home-problem-deal-desk-pre-cinematic-embed-backup-${stamp}.json`
);
const backupHtmlPath = path.join(
  root,
  `docs/old-home-problem-deal-desk-pre-cinematic-embed-backup-${stamp}.html`
);

fs.writeFileSync(backupHtmlPath, probe, "utf8");
fs.writeFileSync(
  backupPath,
  JSON.stringify(
    {
      savedAt: new Date().toISOString(),
      pageId: "68108c2a063eeb5d1bd7ae90",
      elementId: {
        component: "68108c2a063eeb5d1bd7ae90",
        element: "a64ef2f7-2f5f-ab92-9711-5f43f9eeb3fa",
      },
      domId: "oh-deal-desk-embed",
      note: "Temporary polish-v2 probe captured before cinematic-v1 replacement",
      probeMatchesCleanupPlan: true,
      code: probe,
    },
    null,
    2
  ),
  "utf8"
);

const cssRaw = fs.readFileSync(
  path.join(root, "public/marketing/old-home-problem-deal-desk.v1.css"),
  "utf8"
);
const cssMin = cssRaw
  .replace(/\/\*[\s\S]*?\*\//g, "")
  .replace(/\s+/g, " ")
  .replace(/\s*([{}:;,>~+])\s*/g, "$1")
  .replace(/;}/g, "}")
  .trim();

let html = fs.readFileSync(
  path.join(root, "public/marketing/old-home-problem-deal-desk.v1.html"),
  "utf8"
);
html = html.replace(/src="deal-desk-assets\/coastal-hotel-480\.jpg"/g, `src="${hotel}"`);
html = html.replace(/src="data:image\/[^"]+"/g, `src="${hotel}"`);
html = html.replace(/src="[^"]*coastal-hotel[^"]*"/g, `src="${hotel}"`);

// Ensure no script tags
if (/<script/i.test(html) || /@keyframes|animation\s*:/.test(cssMin)) {
  throw new Error("Refusing embed with script or animation");
}

const out = `<style id="oh-deal-desk">${cssMin}</style>\n${html}`;
const outPath = path.join(
  root,
  "docs/old-home-problem-deal-desk-embed-cinematic-v1.html"
);
fs.writeFileSync(outPath, out, "utf8");

const payload = {
  siteId: "68108c29063eeb5d1bd7ae4a",
  pageId: "68108c2a063eeb5d1bd7ae90",
  context:
    "Replace temporary polish-v2 probe with cinematic-v1 Deal Desk HtmlEmbed markup and scoped CSS.",
  actions: [
    {
      label: "set_cinematic_v1",
      set_settings: {
        operations: [
          {
            label: "embed_code",
            element_id: {
              component: "68108c2a063eeb5d1bd7ae90",
              element: "a64ef2f7-2f5f-ab92-9711-5f43f9eeb3fa",
            },
            settings: [{ key: "code", static_text: { value: out } }],
          },
        ],
      },
    },
  ],
};
const argsPath = path.join(root, "docs/_mcp-set-cinematic-embed-args.json");
fs.writeFileSync(argsPath, JSON.stringify(payload), "utf8");

console.log(
  JSON.stringify(
    {
      backupPath,
      backupHtmlPath,
      outPath,
      argsPath,
      cssRaw: cssRaw.length,
      cssMin: cssMin.length,
      embedChars: out.length,
      payloadBytes: Buffer.byteLength(JSON.stringify(payload)),
      md5: crypto.createHash("md5").update(out).digest("hex"),
      hasCinematic: out.includes('data-visual="cinematic-v1"'),
      hasHotelCdn: out.includes("6a6bde85c014ee4e80e65c24"),
      hasProbe: out.includes("polish-v2 probe"),
      hasScript: /<script/i.test(out),
      hasAnimation: /@keyframes|animation\s*:/.test(out),
      hasStrip: out.includes("dpd-strip"),
      defaultState: (out.match(/data-story-state="([^"]+)"/) || [])[1],
    },
    null,
    2
  )
);
