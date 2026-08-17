import fs from "fs";

const html = fs.readFileSync("tmp-old-home-verify.html", "utf8");
console.log("title", (html.match(/<title>[^<]+/) || [])[0]);
console.log("has modules", html.includes('id="modules"'));
console.log("has dc-page", html.includes("dc-page"));
console.log("has w-button", html.includes("w-button"));
console.log("script count", (html.match(/<script/g) || []).length);

// All cdn website-files links
const cdn = [...html.matchAll(/cdn\.prod\.website-files\.com\/[^"']+/g)].map(
  (m) => m[0]
);
console.log("cdn count", cdn.length);
cdn.slice(0, 40).forEach((u) => console.log(u.slice(0, 120)));

// Check if freeform was stripped - look near head end
const i = html.indexOf("</head>");
console.log("\n--- last 2500 of head ---");
console.log(html.slice(Math.max(0, i - 2500), i + 10));
