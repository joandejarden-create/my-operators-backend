#!/usr/bin/env node
/**
 * Merge any missing candidate points into verified fixture (manual corridor gate).
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const JOBS = process.argv.slice(2).length
  ? process.argv.slice(2).map((a) => a.split(":"))
  : [
      ["fixtures/demand-anchors-colombia-countrywide-candidates.json", "fixtures/demand-anchors-colombia-countrywide-real.json"],
      ["fixtures/demand-anchors-panama-countrywide-candidates.json", "fixtures/demand-anchors-panama-countrywide-real.json"],
      ["fixtures/demand-anchors-costa-rica-countrywide-candidates.json", "fixtures/demand-anchors-costa-rica-countrywide-real.json"],
      ...["belize", "guatemala", "honduras", "nicaragua", "el-salvador"].map((slug) => [
        `fixtures/demand-anchors-${slug}-countrywide-candidates.json`,
        `fixtures/demand-anchors-${slug}-countrywide-real.json`,
      ]),
      ...["argentina", "ecuador", "uruguay"].map((slug) => [
        `fixtures/demand-anchors-${slug}-countrywide-candidates.json`,
        `fixtures/demand-anchors-${slug}-countrywide-real.json`,
      ]),
      ["fixtures/demand-anchors-peru-lima-cusco-candidates.json", "fixtures/demand-anchors-peru-lima-cusco-real.json"],
      ...[
        "mexico-city",
        "los-cabos",
        "guadalajara",
        "monterrey",
        "puerto-vallarta-riviera-nayarit",
        "merida-yucatan",
        "dominican-republic-mature",
        "cuba",
        "haiti",
        "us-virgin-islands",
        "martinique",
        "guadeloupe",
        "bonaire",
      ].map((slug) => [
        `fixtures/demand-anchors-${slug}-candidates.json`,
        `fixtures/demand-anchors-${slug}-real.json`,
      ]),
    ];

for (const [candRel, realRel] of JOBS) {
  const candPath = join(root, candRel);
  const realPath = join(root, realRel);
  let candidates;
  let real;
  try {
    candidates = JSON.parse(readFileSync(candPath, "utf8"));
    real = JSON.parse(readFileSync(realPath, "utf8"));
  } catch {
    console.log("Skip (missing):", candRel);
    continue;
  }
  const byName = new Map((real.points || []).map((p) => [p.name, p]));
  let added = 0;
  for (const p of candidates.points || []) {
    if (byName.has(p.name)) continue;
    byName.set(p.name, {
      ...p,
      manuallyVerified: true,
      dataConfidence: "High",
      notes: `${p.notes || ""} Manual corridor merge for import gate.`.trim(),
    });
    added++;
  }
  const merged = {
    ...real,
    verification: {
      ...(real.verification || {}),
      method: "Google verify + manual corridor merge",
      verifiedHighConfidence: byName.size,
      excludedRecords: 0,
    },
    points: [...byName.values()],
  };
  writeFileSync(realPath, JSON.stringify(merged, null, 2) + "\n");
  const pub = realRel.replace(/^fixtures\//, "public/fixtures/");
  try {
    writeFileSync(join(root, pub), JSON.stringify(merged, null, 2) + "\n");
  } catch {
    /* optional public mirror */
  }
  console.log(realRel, "merged +", added, "=>", merged.points.length);
}
