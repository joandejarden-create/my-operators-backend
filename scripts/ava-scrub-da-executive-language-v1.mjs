import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { replaceInternalLanguage } from "../lib/ai-visibility-audit-action-plan/customer-facing-language-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FILES = [
  "castillo-hotel-son-vida-v1.json",
  "ac-hotel-a-coruna-v1.json",
  "waterstone-boca-raton-v1.json",
];

function scrubDaExecutive(da, bl, ment) {
  if (!da) return;
  const raw = String(da.executiveFinding || "");
  if (
    !raw ||
    /Public V2|first-party brand-family|low-quality referrers/i.test(raw)
  ) {
    da.executiveFinding =
      bl > 0
        ? `Trusted third-party websites currently link to this hotel's official pages (${bl} verified link${bl === 1 ? "" : "s"} in this review). ${ment} additional third-party page${ment === 1 ? "" : "s"} mention the hotel without a clear property link. Strengthening destination and industry links can improve discoverability — it does not, by itself, change measured AI visibility.`
        : `This public review did not confirm third-party websites linking to the hotel's official pages. ${ment} third-party reference${ment === 1 ? "" : "s"} mention the hotel without a property hyperlink. That is a visibility opportunity to pursue selectively — not proof of weak AI performance.`;
  } else {
    da.executiveFinding = replaceInternalLanguage(raw);
  }
  for (const a of da.actions || []) {
    if (a.whatWeFound) a.whatWeFound = replaceInternalLanguage(a.whatWeFound);
    if (a.whyItMatters) a.whyItMatters = replaceInternalLanguage(a.whyItMatters);
  }
  for (const s of da.sources || []) {
    if (
      s.commercialRelevance &&
      /A_|B_|C_|D_|DIRECT_HOTEL|HOTEL_MENTIONS/i.test(s.commercialRelevance)
    ) {
      s.commercialRelevance =
        "Third-party site that can help travelers discover this hotel.";
    }
  }
}

for (const f of FILES) {
  const payloadPath = path.join(
    ROOT,
    "lib/ai-visibility-audit-action-plan/payloads",
    f
  );
  const p = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const da = p.digitalAuthority || {};
  const bl = (da.sources || []).filter(
    (s) => s.findingClass === "VERIFIED_BACKLINK"
  ).length;
  const ment = (da.sources || []).filter(
    (s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION"
  ).length;
  scrubDaExecutive(da, bl, ment);
  for (const a of p.actions || []) {
    if (a.whatWeFound) a.whatWeFound = replaceInternalLanguage(a.whatWeFound);
  }
  fs.writeFileSync(payloadPath, `${JSON.stringify(p, null, 2)}\n`);
  console.log(f, "ok", da.executiveFinding.slice(0, 90));
}
