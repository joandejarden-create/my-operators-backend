#!/usr/bin/env node
/**
 * V2.4 Phase 3–4 — hotel-specific EX-10 / EX-21 / schedule manager research.
 *
 *   node scripts/sec-edgar-v24-hotel-exhibits-research.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import {
  discoverExhibitsForFiling,
  filterHotelRelevantExhibits,
  scoreHotelSpecificExhibit,
  parseHotelAgreementsFromExhibitIndex,
} from "../lib/hotel-intelligence/sec-edgar/filing-package-exhibits-v2.js";
import { extractFranchiseExpirationSchedule } from "../lib/hotel-intelligence/sec-edgar/franchise-schedule-extractor-v2.js";
import { extractSubsidiariesFromExhibit21 } from "../lib/hotel-intelligence/sec-edgar/ex21-subsidiary-extractor-v24.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const OUT = path.join(ROOT, "reports", "sec-edgar-v24-hotel-exhibits");
fs.mkdirSync(OUT, { recursive: true });

/** Correct accessions from live submissions (not stale inventory guesses). */
const TARGETS = [
  {
    slug: "diamondrock-hospitality",
    cik: "0001298946",
    filings: [
      { acc: "0001298946-14-000021", date: "2014-02-25", form: "10-K", primary: "drh_10kx12312013.htm" },
      { acc: "0001298946-15-000019", date: "2015-02-27", form: "10-K", primary: "drh_10kx12312014.htm" },
      { acc: "0001445305-13-000424", date: "2013-03-01", form: "10-K", primary: "drh_10kx12312012.htm" },
    ],
  },
  {
    slug: "sunstone-hotel-investors",
    cik: "0001295810",
    filings: [
      { acc: "0001193125-08-035335", date: "2008-02-21", form: "10-K", primary: "d10k.htm" },
      { acc: "0001193125-10-037652", date: "2010-02-23", form: "10-K", primary: "d10k.htm" },
      { acc: "0001104659-12-013838", date: "2012-02-28", form: "10-K", primary: "a12-1033_110k.htm" },
      { acc: "0001104659-14-013102", date: "2014-02-25", form: "10-K", primary: "a13-26277_110k.htm" },
    ],
  },
  {
    slug: "host-hotels-resorts",
    cik: "0001070750",
    filings: [
      { acc: "0001564590-16-012990", date: "2016-02-22", form: "10-K", primary: "hst-10k_20151231.htm" },
    ],
  },
  {
    slug: "rlj-lodging-trust",
    cik: "0001511337",
    filings: [
      { acc: "0001511337-16-000282", date: "2016-02-25", form: "10-K", primary: "rlj-2015123110k.htm" },
    ],
  },
  {
    slug: "apple-hospitality-reit",
    cik: "0001418121",
    filings: [
      { acc: "0001185185-16-003772", date: "2016-02-25", form: "10-K", primary: "ahr10k123115.htm" },
    ],
  },
];

function uniq(arr) {
  return [...new Set(arr.filter(Boolean))];
}

async function main() {
  const client = createSecEdgarClient();
  const report = {
    version: "sec-edgar-v24-hotel-exhibits-research",
    generatedAt: new Date().toISOString(),
    filingsInspected: [],
    hotelAgreementsFromExhibitIndex: [],
    hotelAgreementsProcessed: [],
    exhibitsExamined: 0,
    exhibitsSkippedNonHotel: 0,
    managers: [],
    contractDates: [],
    subsidiaries: [],
    hotelSpeCandidates: [],
    scheduleHotels: [],
    errors: [],
    researchSpendUsd: 0,
  };

  for (const t of TARGETS) {
    for (const f of t.filings) {
      console.error(`[exhibits] ${t.slug} ${f.form} ${f.date} ${f.acc}`);
      const doc = await client.getFilingDocument(t.cik, f.acc, f.primary);
      if (!doc.ok || !doc.text) {
        report.errors.push({ slug: t.slug, acc: f.acc, error: doc.error || doc.status || "empty" });
        continue;
      }
      const url = client.filingDocumentUrl(t.cik, f.acc, f.primary);
      const meta = {
        cik: t.cik,
        accessionNumber: f.acc,
        form: f.form,
        filingDate: f.date,
        companyName: t.slug,
        documentUrl: url,
      };

      const indexAgreements = parseHotelAgreementsFromExhibitIndex(doc.text, meta);
      report.hotelAgreementsFromExhibitIndex.push(...indexAgreements);
      for (const a of indexAgreements) {
        if (a.expirationMention) {
          report.contractDates.push({
            hotel: null,
            date: a.expirationMention,
            applicability: "HISTORICALLY_VERIFIED",
            agreementType: "EXHIBIT_INDEX_DISCLOSURE",
            sourceUrl: url,
            parentAccession: f.acc,
            exhibit: `Exhibit ${a.exhibitNumber}`,
            description: a.description.slice(0, 160),
          });
        }
      }

      const sch = extractFranchiseExpirationSchedule(doc, meta);
      for (const m of sch.managerRelationships || []) report.managers.push(m);
      for (const find of sch.findings || []) {
        if (find.primaryDate || find.initialExpirationYear) {
          report.contractDates.push({
            hotel: find.attributedHotelName || find.hotelName,
            date: find.primaryDate || find.initialExpirationYear,
            applicability: find.historicalApplicability,
            agreementType: find.agreementType,
            sourceUrl: url,
            parentAccession: f.acc,
            exhibit: f.primary,
          });
        }
      }
      for (const h of sch.hotels || []) {
        report.scheduleHotels.push({
          name: h.hotelName,
          city: h.city,
          state: h.stateRegion,
          company: t.slug,
          acc: f.acc,
          date: f.date,
        });
      }

      const discovered = await discoverExhibitsForFiling(client, meta);
      if (discovered.ok) {
        const filtered = filterHotelRelevantExhibits(discovered.exhibits);
        for (const ex of filtered.exhibits.slice(0, 6)) {
          report.exhibitsExamined += 1;
          const edoc = await client.getFilingDocument(t.cik, f.acc, ex.fileName);
          if (!edoc.ok) continue;
          const score = scoreHotelSpecificExhibit(ex, edoc.text.slice(0, 4000));
          if (score.skip || score.priority === "SKIP") {
            report.exhibitsSkippedNonHotel += 1;
            continue;
          }
          if (/EX-21/i.test(ex.exhibitType)) {
            const subx = extractSubsidiariesFromExhibit21(edoc, {
              ...meta,
              documentUrl: ex.sourceUrl,
              exhibitFileName: ex.fileName,
            });
            report.subsidiaries.push(...subx.entities.slice(0, 50));
            report.hotelSpeCandidates.push(
              ...subx.entities.filter((e) => e.roleHint === "PROPERTY_SPE_CANDIDATE").slice(0, 30),
            );
          } else if (score.reason === "hotel_contract_signal") {
            report.hotelAgreementsProcessed.push({
              company: t.slug,
              fileName: ex.fileName,
              exhibitType: ex.exhibitType,
              sourceUrl: ex.sourceUrl,
              parentAccession: f.acc,
              relevance: score,
              preview: edoc.text
                .replace(/<[^>]+>/g, " ")
                .replace(/\s+/g, " ")
                .slice(0, 300),
            });
          }
          await new Promise((r) => setTimeout(r, 120));
        }
      }

      report.filingsInspected.push({
        slug: t.slug,
        accession: f.acc,
        form: f.form,
        filingDate: f.date,
        primary: f.primary,
        scheduleHotels: sch.hotelCount,
        managers: (sch.managerRelationships || []).length,
        contractFindings: (sch.findings || []).length,
        exhibitIndexHotelAgreements: indexAgreements.length,
        packageExhibits: discovered.exhibitCount || 0,
      });
      await new Promise((r) => setTimeout(r, 150));
    }
  }

  const mgrPairs = report.managers.map((m) => ({
    name: m.entityName,
    hotel: m.attributedHotelName || m.hotelNameHints?.[0],
    city: m.city,
    state: m.stateRegion,
    company: m.source?.companyName,
    acc: m.source?.accessionNumber,
    url: m.source?.documentUrl,
    status: m.relationshipStatus,
  }));

  report.summary = {
    filingsInspected: report.filingsInspected.length,
    companies: uniq(report.filingsInspected.map((f) => f.slug)).length,
    scheduleHotels: report.scheduleHotels.length,
    uniqueScheduleHotels: uniq(report.scheduleHotels.map((h) => `${h.name}|${h.city}|${h.state}`.toLowerCase()))
      .length,
    exhibitIndexHotelAgreements: report.hotelAgreementsFromExhibitIndex.length,
    hotelAgreementsProcessed: report.hotelAgreementsProcessed.length,
    exhibitsExamined: report.exhibitsExamined,
    exhibitsSkippedNonHotel: report.exhibitsSkippedNonHotel,
    uniqueManagers: uniq(report.managers.map((m) => m.entityName)).length,
    managerNames: uniq(report.managers.map((m) => m.entityName)).slice(0, 40),
    managerHotelPairs: mgrPairs.slice(0, 50),
    contractDates: report.contractDates.length,
    sampleContractDates: report.contractDates.slice(0, 20),
    sampleExhibitIndex: report.hotelAgreementsFromExhibitIndex.slice(0, 15),
    subsidiaryEntities: report.subsidiaries.length,
    hotelSpeCandidates: report.hotelSpeCandidates.length,
    speNames: uniq(report.hotelSpeCandidates.map((e) => e.entityName)).slice(0, 30),
    errors: report.errors,
    researchSpendUsd: 0,
    guarantees: [
      "Employment/credit EX-10s filtered when hotel signals absent",
      "EX-21 subsidiary ≠ ownership of a named hotel without more evidence",
      "Registrant not auto-classified as owner",
      "Schedule managers are HISTORICALLY_VERIFIED only — do not replace current HPC managers",
      "No HPC writes / customer publication remains blocked",
    ],
  };

  fs.writeFileSync(path.join(OUT, "HOTEL_EXHIBITS_REPORT.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(
    path.join(OUT, "HOTEL_EXHIBITS_REPORT.md"),
    [
      `# SEC EDGAR V2.4 — Hotel-Specific Exhibit + Schedule Research`,
      ``,
      `**Generated:** ${report.generatedAt}`,
      ``,
      `| Metric | Value |`,
      `|---|---|`,
      `| Companies | ${report.summary.companies} |`,
      `| Filings inspected | ${report.summary.filingsInspected} |`,
      `| Schedule hotels | ${report.summary.scheduleHotels} (${report.summary.uniqueScheduleHotels} unique) |`,
      `| Exhibit-index hotel agreements | ${report.summary.exhibitIndexHotelAgreements} |`,
      `| Attached hotel EX-10s processed | ${report.summary.hotelAgreementsProcessed} |`,
      `| EX-10s skipped (employment/credit) | ${report.summary.exhibitsSkippedNonHotel} |`,
      `| Unique managers (HISTORICALLY_VERIFIED) | ${report.summary.uniqueManagers} |`,
      `| Contract dates | ${report.summary.contractDates} |`,
      `| Hotel SPE candidates | ${report.summary.hotelSpeCandidates} |`,
      `| Spend | $0 |`,
      ``,
      `## Managers`,
      ...report.summary.managerNames.map((n) => `- ${n}`),
      ``,
      `## Sample manager↔hotel pairs`,
      ...report.summary.managerHotelPairs
        .slice(0, 25)
        .map((p) => `- ${p.hotel} (${p.city}, ${p.state}) → ${p.name} [${p.status}]`),
      ``,
      `## Exhibit-index hotel agreements`,
      ...report.summary.sampleExhibitIndex.map(
        (a) =>
          `- ${a.companyName || ""} Ex ${a.exhibitNumber}: ${a.description.slice(0, 140)}${a.priorAccession ? ` [IBR ${a.priorAccession}]` : ""}`,
      ),
      ``,
      `## SPE candidates`,
      ...report.summary.speNames.map((n) => `- ${n}`),
      ``,
    ].join("\n"),
  );

  console.log(JSON.stringify({ ok: true, outDir: OUT, ...report.summary }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
