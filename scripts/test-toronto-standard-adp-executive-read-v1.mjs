import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";

const r = loadPublishedReport("adp_radisson_blu_toronto_downtown");
if (!r) {
  console.error("FAIL missing published report");
  process.exit(1);
}
const rate = r.executiveMetrics?.considerationRate?.rate;
if (rate !== 33.2) {
  console.error("FAIL consideration rate", rate);
  process.exit(1);
}
const er = JSON.stringify(r.executiveRead || {});
if (/AI Consideration is 29%|appeared 29%|consideration only 29%|"29%"/.test(er)) {
  console.error("FAIL stale 29% in executiveRead");
  process.exit(1);
}
if (!er.includes("33.2%")) {
  console.error("FAIL missing 33.2% in executiveRead");
  process.exit(1);
}
console.log(
  JSON.stringify(
    { ok: true, gate: "test:toronto-standard-adp-executive-read-v1", rate },
    null,
    2
  )
);
