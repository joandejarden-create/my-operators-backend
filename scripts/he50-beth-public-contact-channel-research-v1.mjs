#!/usr/bin/env node
/**
 * Bounded Exa public-channel research for Bethesda HI Org Contacts.
 * Reuses GDI Public Contact Discovery 2.0. No Surfe. No invented emails.
 *
 *   node scripts/he50-beth-public-contact-channel-research-v1.mjs
 *   node scripts/he50-beth-public-contact-channel-research-v1.mjs --authorize-paid-search
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  discoverPublicContactChannels,
  inspectPublicContactSearchCredentials,
} from "../lib/hotel-intelligence/contact-intelligence/he-reuse-gdi-public-contact-discovery-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p });
    break;
  }
}

const AUTHORIZE = process.argv.includes("--authorize-paid-search");
const OUT = path.join(
  ROOT,
  "reports/hotel-explorer/he50-hi-org-contacts/BETH-PUBLIC-CHANNEL-RESEARCH-V1.json",
);

const PEOPLE = [
  {
    name: "Katherine Button",
    title: "VP, Asset Management and Investments",
    organization: "Mullen Real Estate Capital",
    domain: "mullencap.com",
  },
  {
    name: "Jeff Mullen",
    title: "President and CEO",
    organization: "Mullen Real Estate Capital",
    domain: "mullencap.com",
  },
  {
    name: "Patrick Reece",
    title: "Director, Asset Management",
    organization: "Mullen Real Estate Capital",
    domain: "mullencap.com",
  },
];

async function main() {
  const creds = inspectPublicContactSearchCredentials();
  const results = [];
  let spendEstimateUsd = 0;

  for (const person of PEOPLE) {
    // eslint-disable-next-line no-await-in-loop
    const discovered = await discoverPublicContactChannels({
      opportunity: {
        id: `he50_beth_${person.name.toLowerCase().replace(/\s+/g, "_")}`,
        organizationName: person.organization,
        organizationDomain: person.domain,
        primaryContact: {
          name: person.name,
          title: person.title,
          organization: person.organization,
        },
      },
      candidateUrls: [
        "https://mullencap.com/about/",
        "https://mullencap.com/contact/",
        "https://mullencap.com/team/",
        "https://mullencap.com/",
      ],
      authorizePaidSearch: AUTHORIZE,
      maxQueries: 4,
      maxPages: 2,
      seedRegressionFixtures: false,
    });
    const email = discovered?.found?.email || discovered?.channels?.email || null;
    const phone = discovered?.found?.phone || discovered?.channels?.phone || null;
    const status = discovered?.status || null;
    spendEstimateUsd += Number(discovered?.metrics?.searchCostUsd || 0) || 0;
    results.push({
      person: person.name,
      title: person.title,
      status,
      ok: discovered?.ok === true,
      email,
      phone,
      phoneKind: discovered?.found?.phoneKind || null,
      sourceUrls: discovered?.found?.sourceUrl
        ? [discovered.found.sourceUrl]
        : [],
      namedDirectEmail: Boolean(email && !/^(info|contact|hello)@/i.test(String(email))),
      namedDirectPhone: Boolean(phone),
      surfeUsed: false,
      metrics: discovered?.metrics || null,
      reason: discovered?.reason || null,
    });
  }

  const report = {
    at: new Date().toISOString(),
    authorizePaidSearch: AUTHORIZE,
    credentials: creds,
    hotelId: "recLuxvwwxID7U2B8",
    spendEstimateUsd,
    results,
    namedEmailsRecovered: results.filter((r) => r.namedDirectEmail).length,
    namedPhonesRecovered: results.filter((r) => r.namedDirectPhone).length,
    note:
      "Leads require independent verification before HI Contact Channels promotion as VERIFIED_NAMED_*. Org switchboard already persisted separately.",
  };
  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
