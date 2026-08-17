import fs from "fs";

const headPath = process.argv[2];
const overridePath = process.argv[3];
const outPath = process.argv[4];

let head = fs.readFileSync(headPath, "utf8");
const override = fs.readFileSync(overridePath, "utf8").trim();

const startMark = "/* Insights H2 + carousel overflow fix */";
const endMark = "/* /Insights H2 + carousel overflow fix */";

if (head.includes(startMark)) {
  const start = head.indexOf(startMark);
  const end = head.indexOf(endMark);
  if (end > start) {
    head = head.slice(0, start) + head.slice(end + endMark.length);
  } else {
    // remove from mark to next closing style if present
    const styleClose = head.indexOf("</style>", start);
    if (styleClose > start) {
      // remove only the marked block roughly
      head = head.replace(/\n?\/\* Insights H2 \+ carousel overflow fix \*\/[\s\S]*?(?=<\/style>)/, "\n");
    }
  }
}

const block = `\n${startMark}\n${override}\n${endMark}\n`;
if (!head.includes("</style>")) {
  throw new Error("No </style> in freeform head");
}
head = head.replace("</style>", `${block}</style>`);
fs.writeFileSync(outPath, head);
console.log(JSON.stringify({ outPath, length: head.length, hasOverride: head.includes(startMark) }));
