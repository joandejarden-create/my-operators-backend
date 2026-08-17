import fs from "fs";
import crypto from "crypto";

const css = fs.readFileSync("public/marketing/dealality-old-home-premium.css", "utf8");
// Extract hero video block from local CSS for freeform injection (lines after hero signup through mobile)
const start = css.indexOf("/* Hero — copy left / video poster right */");
const end = css.indexOf("/* Nav + Hero:");
if (start < 0 || end < 0) throw new Error("hero css markers missing");
let heroCss = css.slice(start, end).trim();
// Make freeform overrides stronger
heroCss = heroCss
  .replace(/\{/g, "{")
  .split("\n")
  .map((line) => {
    if (!line.includes("{") && !line.includes("}") && !line.includes("@") && line.includes(":")) {
      return line.replace(/;(\s*)$/, "!important;$1").replace(/:([^;!]+)(!important)?;/, (m, v) => {
        if (String(v).includes("!important")) return m;
        return `:${v}!important;`;
      });
    }
    // crude: append !important to declarations missing it
    return line.replace(/([^:;{}]+):([^;{}!]+);/g, (m, prop, val) => {
      if (String(val).includes("!important")) return m;
      return `${prop}:${val}!important;`;
    });
  })
  .join("\n");

let head = fs.readFileSync("tmp-old-home-head-globe.txt", "utf8");
// Drop finsweet link optional keep; replace globe hero CSS block
const marker = "/* Hero + Finsweet 3D globe (left column) */";
const styleEnd = head.lastIndexOf("</style>");
const markerIdx = head.indexOf(marker);
if (markerIdx < 0) {
  // insert before </style>
  head =
    head.slice(0, styleEnd) +
    "\n" +
    heroCss +
    "\n" +
    head.slice(styleEnd);
} else {
  // replace from marker to </style>
  head = head.slice(0, markerIdx) + heroCss + "\n\n" + head.slice(styleEnd);
}

// Ensure hide globe/signals even if old rules remain earlier
if (!head.includes("#hero-globe,#hero-signals{display:none")) {
  head = head.replace(
    "</style>",
    `#hero-globe,#hero-signals{display:none!important}\n</style>`
  );
}

fs.writeFileSync("tmp-old-home-head-globe.txt", head);
fs.writeFileSync(
  "tmp-set-head-args.json",
  JSON.stringify({
    server: "user-webflow",
    toolName: "data_scripts_tool",
    arguments: {
      context:
        "Update Old Home freeform head for copy-left video-poster-right hero layout.",
      actions: [
        {
          label: "set-head-hero-video",
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

const js = fs.readFileSync(
  "public/marketing/dealality-old-home-hero-video.js",
  "utf8"
);
const hash = crypto.createHash("sha256").update(js).digest("base64");
fs.writeFileSync(
  "tmp-hero-video-sri.txt",
  "sha256-" + hash
);
console.log({
  headBytes: Buffer.byteLength(head),
  hasVideoCard: heroCss.includes("#hero-video-card"),
  sri: "sha256-" + hash.slice(0, 16) + "...",
  jsBytes: Buffer.byteLength(js),
});
