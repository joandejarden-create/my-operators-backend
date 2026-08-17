import fs from "fs";

const hotel =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bde85c014ee4e80e65c24_deal-desk-coastal-hotel-480.jpg";
const cssUrl =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bdf56570ac7789fe12096_oh-deal-desk-polish-v2.css";

const mode = process.argv.includes("--link") ? "link" : "inline";

const cssRaw = fs.readFileSync(
  "public/marketing/old-home-problem-deal-desk.v1.css",
  "utf8"
);
const cssMin = cssRaw
  .replace(/\/\*[\s\S]*?\*\//g, "")
  .replace(/\s+/g, " ")
  .replace(/\s*([{}:;,>~+])\s*/g, "$1")
  .replace(/;}/g, "}")
  .trim();

let html = fs.readFileSync(
  "public/marketing/old-home-problem-deal-desk.v1.html",
  "utf8"
);
html = html.replace(/src="data:image\/[^"]+"/g, `src="${hotel}"`);
html = html.replace(/src="[^"]*coastal-hotel[^"]*"/g, `src="${hotel}"`);

const out =
  mode === "link"
    ? `<link id="oh-deal-desk" rel="stylesheet" href="${cssUrl}">\n${html}`
    : `<style id="oh-deal-desk">${cssMin}</style>\n${html}`;

const outPath =
  mode === "link"
    ? "docs/old-home-problem-deal-desk-embed-polish-v2-link.html"
    : "docs/old-home-problem-deal-desk-embed-polish-v2.html";

fs.writeFileSync(outPath, out);

const payload = {
  siteId: "68108c29063eeb5d1bd7ae4a",
  pageId: "68108c2a063eeb5d1bd7ae90",
  context:
    mode === "link"
      ? "Replace Deal Desk HtmlEmbed code with polish-v2 HTML and CDN stylesheet link."
      : "Replace Deal Desk HtmlEmbed with polish-v2 HTML and inlined scoped CSS for Designer parity.",
  actions: [
    {
      label: "set_polish_v2",
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

const argsPath =
  mode === "link"
    ? "docs/_mcp-set-embed-args-link.json"
    : "docs/_mcp-set-embed-args.json";
fs.writeFileSync(argsPath, JSON.stringify(payload));

console.log(
  JSON.stringify({
    mode,
    outPath,
    argsPath,
    cssRaw: cssRaw.length,
    cssMin: cssMin.length,
    embed: out.length,
    payloadBytes: Buffer.byteLength(JSON.stringify(payload)),
    hasAnimation: /@keyframes|animation\s*:/.test(out),
    hasScript: /<script/.test(out),
    hasStrip: out.includes("dpd-strip"),
    hasHotel: out.includes("6a6bde85c014ee4e80e65c24"),
    hasPolish: out.includes("polish-v2"),
  })
);
