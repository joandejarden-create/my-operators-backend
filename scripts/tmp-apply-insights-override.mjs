import fs from "fs";

const headPath = "tmp-old-home-freeform-head.json";
const overridePath = "tmp-insights-css-override.css";
const outPath = "tmp-old-home-freeform-head-patched.txt";

const payload = JSON.parse(fs.readFileSync(headPath, "utf8"));
let head = typeof payload === "string" ? payload : payload.content || payload.result?.content;
if (!head) throw new Error("No head content");

const override = fs.readFileSync(overridePath, "utf8").trim();
const startMark = "/* Insights H2 + carousel overflow fix */";
const endMark = "/* /Insights H2 + carousel overflow fix */";

if (head.includes(startMark)) {
  const start = head.indexOf(startMark);
  const end = head.indexOf(endMark);
  if (end > start) {
    head = head.slice(0, start) + head.slice(end + endMark.length);
  }
}

const block = `\n${startMark}\n${override}\n${endMark}\n`;
if (!head.includes("</style>")) throw new Error("No </style>");
head = head.replace("</style>", `${block}</style>`);
fs.writeFileSync(outPath, head);
console.log(JSON.stringify({ length: head.length, hasOverride: head.includes(startMark) }));
