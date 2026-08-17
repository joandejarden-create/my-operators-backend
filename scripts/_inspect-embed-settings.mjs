import fs from "fs";
const j = fs.readFileSync(
  "C:/Users/joand/.cursor/projects/c-Dev-deal-capture-proxy/agent-tools/8d6a48e3-3936-41d2-9002-b8bb065a6d42.txt",
  "utf8"
);
const o = JSON.parse(j);
const str = JSON.stringify(o);
const keys = [...str.matchAll(/"key":"([^"]+)"/g)].map((m) => m[1]);
console.log({ uniqueKeys: [...new Set(keys)], hasCode: keys.includes("code"), len: str.length });
const codeIdx = str.indexOf('"key":"code"');
console.log(str.slice(Math.max(0, codeIdx - 80), codeIdx + 200));
