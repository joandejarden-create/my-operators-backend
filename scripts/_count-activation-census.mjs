import { readFileSync } from "node:fs";

function parse(line) {
  const o = [];
  let c = "";
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (q) {
      if (ch === '"' && line[i + 1] === '"') {
        c += '"';
        i++;
      } else if (ch === '"') q = false;
      else c += ch;
    } else if (ch === '"') q = true;
    else if (ch === ",") {
      o.push(c);
      c = "";
    } else c += ch;
  }
  o.push(c);
  return o;
}

const CALA =
  /Mexico|Colombia|Panama|Peru|Barbados|Puerto Rico|Honduras|Cayman|Argentina|Brazil|Jamaica|Dominican|Costa Rica|Chile|Bahamas|Aruba|Guatemala|Ecuador/i;
const csv = readFileSync("reports/census-amenities-blank-rows.csv", "utf8").split(/\r?\n/);
const brands = {};
for (const line of csv.slice(1)) {
  if (!line.trim()) continue;
  const f = parse(line);
  const name = f[1] || "";
  const status = f[3] || "";
  const country = f[4] || "";
  if (!CALA.test(country)) continue;
  let b = null;
  if (/\bAvani\b|\bAVANI\b/i.test(name)) b = "Avani";
  else if (/Tapestry/i.test(name)) b = "Tapestry";
  else if (/Four Points Flex/i.test(name)) b = "Four Points Flex";
  else if (/Radisson Collection/i.test(name)) b = "Radisson Collection";
  else if (/Bunkhouse/i.test(name)) b = "Bunkhouse";
  else if (/Hotel Indigo/i.test(name) && !/NOI/i.test(name)) b = "Hotel Indigo";
  else if (/Kimpton/i.test(name)) b = "Kimpton";
  else if (/Curio/i.test(name)) b = "Curio";
  if (!b) continue;
  brands[b] = brands[b] || { n: 0, mx: 0, pipeline: 0, open: 0, sample: [] };
  brands[b].n++;
  if (/Mexico/i.test(country)) brands[b].mx++;
  if (/Pipeline/i.test(status)) brands[b].pipeline++;
  else brands[b].open++;
  if (brands[b].sample.length < 3) brands[b].sample.push(`${status}|${country}|${name.slice(0, 60)}`);
}
console.log(JSON.stringify(brands, null, 2));
