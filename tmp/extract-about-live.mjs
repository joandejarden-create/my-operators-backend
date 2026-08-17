import fs from "fs";
import os from "os";
import path from "path";

const html = fs.readFileSync(path.join(os.tmpdir(), "oh-now.html"), "utf8");
const i = html.indexOf('id="about"');
const j = html.indexOf('id="perspectives"');
console.log({ about: i, persp: j, len: html.length });
const chunk = html.slice(Math.max(0, i - 120), j > i ? j : i + 40000);
fs.writeFileSync(path.join(os.tmpdir(), "oh-about-chunk.html"), chunk);
console.log("chunk", chunk.length);

const hrefs = [...chunk.matchAll(/https:\/\/cdn\.prod\.website-files\.com[^"'>\s]+/g)].map((m) => m[0]);
console.log("cdn in about chunk:\n" + [...new Set(hrefs)].join("\n"));

const scripts = [...html.matchAll(/[a-z0-9._-]*(?:manual-process|problem-storyboard|problem-v2|deal-desk)[a-z0-9._-]*/gi)].map((m) => m[0]);
console.log("global asset names:\n" + [...new Set(scripts)].join("\n"));

const m = chunk.match(/<section[^>]*id="about"[^>]*>/i);
console.log("section open:", m && m[0]);

// Strip tags for readable text sample
const text = chunk
  .replace(/<script[\s\S]*?<\/script>/gi, " ")
  .replace(/<style[\s\S]*?<\/style>/gi, " ")
  .replace(/<[^>]+>/g, " ")
  .replace(/\s+/g, " ")
  .trim();
console.log("text sample:\n", text.slice(0, 1800));
