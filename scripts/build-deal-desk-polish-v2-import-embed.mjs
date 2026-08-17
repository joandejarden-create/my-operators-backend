import fs from "fs";

const hotel =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bde85c014ee4e80e65c24_deal-desk-coastal-hotel-480.jpg";
const cssUrl =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bdf56570ac7789fe12096_oh-deal-desk-polish-v2.css";

let html = fs.readFileSync(
  "public/marketing/old-home-problem-deal-desk.v1.html",
  "utf8"
);
html = html.replace(/src="data:image\/[^"]+"/g, `src="${hotel}"`);
html = html.replace(/src="[^"]*coastal-hotel[^"]*"/g, `src="${hotel}"`);

const out = `<style id="oh-deal-desk">@import url("${cssUrl}");</style>\n${html}`;
fs.writeFileSync("docs/old-home-problem-deal-desk-embed-polish-v2-import.html", out);

const args = {
  siteId: "68108c29063eeb5d1bd7ae4a",
  pageId: "68108c2a063eeb5d1bd7ae90",
  context:
    "Replace Deal Desk HtmlEmbed with polish-v2 HTML and CSS @import for Designer parity.",
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
fs.writeFileSync("docs/_mcp-set-embed-args-import.json", JSON.stringify(args));
console.log(
  JSON.stringify({
    chars: out.length,
    payload: Buffer.byteLength(JSON.stringify(args)),
    hasImport: out.includes("@import"),
    hasPolish: out.includes("polish-v2"),
    hasHotel: out.includes("6a6bde85c014ee4e80e65c24"),
  })
);
