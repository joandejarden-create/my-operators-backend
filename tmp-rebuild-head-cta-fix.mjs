import fs from "fs";

const css = fs.readFileSync("public/marketing/dealality-old-home-premium.css", "utf8");
const start = css.indexOf("/* Hero — copy left / video poster right */");
const end = css.indexOf("/* Nav + Hero:");
if (start < 0 || end < 0) throw new Error("markers missing");
let heroCss = css.slice(start, end).trim();
heroCss = heroCss.replace(/([^:;{}]+):([^;{}!]+);/g, (m, prop, val) => {
  if (String(val).includes("!important")) return m;
  return `${prop}:${val}!important;`;
});

let head = fs.readFileSync("tmp-old-home-head-globe.txt", "utf8");
// Replace from first hero video marker or globe marker through </style>
const markers = [
  "/* Hero — copy left / video poster right */",
  "/* Hero + Finsweet 3D globe (left column) */",
];
let cut = -1;
for (const m of markers) {
  const i = head.indexOf(m);
  if (i >= 0) cut = cut < 0 ? i : Math.min(cut, i);
}
const styleEnd = head.lastIndexOf("</style>");
if (cut < 0) throw new Error("no hero css marker in head");
head = head.slice(0, cut) + heroCss + "\n\n" + head.slice(styleEnd);
if (!head.includes("#fsw-btn-wrap{position:static")) {
  throw new Error("cta fix missing from head");
}
fs.writeFileSync("tmp-old-home-head-globe.txt", head);
fs.writeFileSync(
  "tmp-set-head-args.json",
  JSON.stringify({
    server: "user-webflow",
    toolName: "data_scripts_tool",
    arguments: {
      context: "Fix Old Home hero CTA stacking and video poster freeform CSS.",
      actions: [
        {
          label: "set-head-hero-video-fix",
          set_page_freeform_code: {
            page_id: "68108c2a063eeb5d1bd7ae90",
            location: "head",
            content: head,
          },
        },
      ],
    },
  })
);
console.log("ok", Buffer.byteLength(head), head.includes("position:static!important"));
