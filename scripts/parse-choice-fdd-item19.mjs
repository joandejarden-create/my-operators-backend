/**
 * Parse Item 19 loyalty / enterprise (or proprietary) % from fixtures/choice-fdd-text/*.txt
 * node scripts/parse-choice-fdd-item19.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DIR = path.join(__dirname, "..", "fixtures", "choice-fdd-text");

const AIRTABLE = {
  Cambria: "Cambria Hotels",
  "Country Inn & Suites": "Country Inn & Suites by Radisson (Choice)",
  "Country Inn & Suites by Radisson": "Country Inn & Suites by Radisson (Choice)",
  "Park Inn": "Park Inn by Radisson (Choice)",
  "Radisson Individuals": "Radisson Individual (Choice)",
  "Radisson Blu": "Radisson Blu (Choice)",
  Radisson: "Radisson (Choice)",
  Everhome: "Everhome Suites",
  "Ascend Hotel Collection": "Ascend Hotel Collection",
  "Clarion Pointe": "Clarion Pointe",
  "Comfort Inn & Suites": "Comfort Inn & Suites",
  "Econo Lodge": "Econo Lodge",
  "MainStay Suites": "MainStay Suites",
  "Quality Inn": "Quality Inn",
  "Rodeway Inn": "Rodeway Inn",
  "Sleep Inn": "Sleep Inn",
  "Suburban Studios": "Suburban Studios",
  "WoodSpring Suites": "WoodSpring Suites",
  Clarion: "Clarion",
};

function brandFromText(t) {
  const h = t.slice(0, 4000);
  const m = h.match(/([^\n]{2,80})\s*[–-]\s*Franchise Disclosure Document/i);
  if (m) {
    const raw = m[1].replace(/\s+/g, " ").trim();
    for (const [k, v] of Object.entries(AIRTABLE)) {
      if (raw.toLowerCase().includes(k.toLowerCase())) return v;
    }
    return raw;
  }
  return "?";
}

function parseItem19(t) {
  const noPerf =
    /do not make any (?:financial performance )?representations|do not make any representations about a franchisee/i.test(
      t.slice(t.search(/ITEM\s*19/i), t.search(/ITEM\s*19/i) + 8000)
    );
  if (noPerf && !/Choice Privileges Contribution/i.test(t)) {
    return { noItem19: true };
  }

  const idx = t.search(
    /Choice Privileges Contribution|Total Choice Enterprise Contribution|Total Choice Proprietary Contribution/i
  );
  if (idx < 0) return { noItem19: true };

  const chunk = t.slice(idx, idx + 4000);
  const nums = [...chunk.matchAll(/(\d{1,2}\.\d)%/g)].map((m) => parseFloat(m[1]));

  let enterprisePct;
  let proprietaryPct;
  let loyaltyPct;

  const entM = chunk.match(
    /(?:Total Choice )?(?:Enterprise|Proprietary)\s+Contribution\s+([\d.]+)%/i
  );
  if (entM) {
    const v = parseFloat(entM[1]);
    if (/Proprietary/i.test(entM[0])) proprietaryPct = v;
    else enterprisePct = v;
  }

  const loyM = chunk.match(/Choice\s+Privileges\s+Contribution\s+([\d.]+)%/i);
  if (loyM) loyaltyPct = parseFloat(loyM[1]);

  if (!loyaltyPct) {
    const afterLoy = chunk.split(/Privileges\s+Contribution/i)[1];
    if (afterLoy) {
      const m = afterLoy.match(/([\d.]+)%/);
      if (m) loyaltyPct = parseFloat(m[1]);
    }
  }

  if (!enterprisePct && !proprietaryPct) {
    const ent = nums.find((p) => p >= 55 && p <= 99);
    if (ent) enterprisePct = ent;
    const prop = nums.find((p) => p >= 25 && p <= 55 && p !== loyaltyPct);
    if (prop && /Proprietary/i.test(chunk)) proprietaryPct = prop;
  }

  return { loyaltyPct, enterprisePct, proprietaryPct, noItem19: false };
}

const byBrand = {};
for (const f of fs.readdirSync(DIR).filter((x) => x.endsWith(".txt"))) {
  const t = fs.readFileSync(path.join(DIR, f), "utf8");
  const brand = brandFromText(t);
  const m = parseItem19(t);
  const key = brand;
  const entry = { file: f, brand, ...m };
  if (!byBrand[key] || f > byBrand[key].file) byBrand[key] = entry;
}

console.log(JSON.stringify(byBrand, null, 2));
