import dotenv from "dotenv";
dotenv.config();
import Airtable from "airtable";

// Inline scrub for orphan country-inn row not returned by listPresentationRowsLight.
const ADR_REPLACEMENTS = [
  { re: /\bcorridor['’]s ADR support\b/gi, replace: "corridor's rate support" },
  { re: /\bADR\b/g, replace: "rate support" },
  { re: /\badr\b/g, replace: "rate support" },
];

function scrub(text) {
  let out = String(text || "").trim();
  for (const rule of ADR_REPLACEMENTS) out = out.replace(rule.re, rule.replace);
  return out.replace(/[ \t]{2,}/g, " ").replace(/\s+\./g, ".").trim();
}

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const TABLE = "Brand Setup - Brand Explorer Presentation";
const id = "recl8DwI5VkdnJu9V";
const r = await base(TABLE).find(id);
const before = r.fields.Body || "";
const after = scrub(before);
console.log({ beforeHas: /ADR/i.test(before), afterHas: /ADR/i.test(after), before: before.slice(-80), after: after.slice(-80) });
if (before !== after) {
  await base(TABLE).update(id, { Body: after });
  console.log("patched", id);
} else {
  console.log("no change — dumping codepoints around ADR");
  const i = before.search(/ADR/i);
  console.log([...before.slice(i - 5, i + 5)].map((c) => `${c} U+${c.codePointAt(0).toString(16)}`));
}
