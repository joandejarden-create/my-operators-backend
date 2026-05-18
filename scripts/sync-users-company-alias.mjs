#!/usr/bin/env node
/** Copy Company Profile → Company on Users when both columns exist. */
import "../load-env.js";
import Airtable from "airtable";
import { PLATFORM_USERS_TABLE_ID, PUF } from "../lib/airtable/platform-users-table.js";

const dryRun = !process.argv.includes("--apply");
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

async function main() {
  const records = [];
  await new Promise((resolve, reject) => {
    base(PLATFORM_USERS_TABLE_ID)
      .select({ pageSize: 100 })
      .eachPage(
        (page, next) => {
          records.push(...page);
          next();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });

  let updated = 0;
  for (const r of records) {
    const cp = r.fields[PUF.companyProfile];
    if (!Array.isArray(cp) || !cp.length) continue;
    const cur = r.fields[PUF.company];
    if (Array.isArray(cur) && cur.length && cur.join() === cp.join()) continue;
    if (!dryRun) {
      await base(PLATFORM_USERS_TABLE_ID).update(r.id, { [PUF.company]: cp });
    }
    updated++;
  }
  console.log(dryRun ? `Would sync Company on ${updated} row(s)` : `Synced Company on ${updated} row(s)`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
