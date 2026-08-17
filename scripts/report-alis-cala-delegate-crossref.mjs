/**
 * Cross-reference ALIS CALA 2026 delegate roster against GTM owner targets.
 *
 *   node scripts/report-alis-cala-delegate-crossref.mjs
 *   node scripts/report-alis-cala-delegate-crossref.mjs --roster="path/to/roster.xlsx"
 */
import { readFileSync, writeFileSync, mkdirSync, copyFileSync, existsSync, readdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports");
const DATA_DIR = join(ROOT, "data", "internal", "gtm-conference-rosters");

const rosterArg = process.argv.find((a) => a.startsWith("--roster="));
const DEFAULT_ROSTER =
  "c:\\Users\\joand\\Downloads\\ALISCALA26 Final Delegate Roster for Distribution 4-30.xlsx";
const ROSTER_PATH = rosterArg ? rosterArg.split("=")[1].replace(/^"|"$/g, "") : DEFAULT_ROSTER;

const STRIKE_JSON = join(REPORTS, "gtm-owner-strike-list.json");
const BRANDING_JSON = join(REPORTS, "gtm-branding-decision-targets.json");
const ENRICHMENT_DIR = join(ROOT, "data", "internal", "gtm-registry-enrichments");
const COMPANY_PROFILES = join(ROOT, "lib", "gtm-owner-target", "company-profile-enrichments.js");

/** Brand / advisor companies — deprioritize as net-new owner leads */
const EXCLUDE_COMPANY_RE = [
  /\bmarriott\b/i,
  /\bhilton\b/i,
  /\bhyatt\b/i,
  /\bihg\b/i,
  /\baccor\b/i,
  /\bchoice\b/i,
  /\bwyndham\b/i,
  /\bradisson\b/i,
  /\bfour seasons\b/i,
  /\bstarwood\b/i,
  /\bintercontinental\b/i,
  /\bdevelopment\b.*\b(region|caribbean|latin)\b/i,
  /\bfranchise\b/i,
  /\bbroker\b/i,
  /\badvisor\b/i,
  /\bconsultant\b/i,
  /\barchitect\b/i,
  /\bdesign\b/i,
  /\bcbre\b/i,
  /\bjll\b/i,
  /\bhvs\b/i,
  /\bbank\b/i,
  /\bcapital markets\b/i,
  /\blaw firm\b/i,
  /\battorney\b/i,
  /\bmedia\b/i,
  /\bpress\b/i,
  /\bishc\b/i,
  /\bconference\b/i,
];

const OWNER_TITLE_RE =
  /\b(owner|president|ceo|chief executive|founder|principal|chairman|co-founder|managing director|director general|gerente general|propietario|desarrollador|developer|investor|family office|asset manager|vice president.*hotel|hotelier|hospitality.*president)\b/i;

function normalizeLoose(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function normalizeCompany(value) {
  return normalizeLoose(value)
    .replace(/\b(s a de c v|s a b|s l|srl|sa|inc|llc|ltd|corp|corporation|group|grupo|hotels|resorts|hoteles)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function loadJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function parseDelegatesViaPython(xlsxPath) {
  const py = `
import json, sys
from openpyxl import load_workbook
path = sys.argv[1]
wb = load_workbook(path, read_only=True, data_only=True)
ws = wb[wb.sheetnames[0]]
rows = list(ws.iter_rows(values_only=True))
wb.close()
header = rows[0]
cols = {str(h).strip(): i for i, h in enumerate(header) if h}
out = []
for r in rows[1:]:
    if not any(r): continue
    fn = (r[cols.get('First Name', 1)] or '').strip()
    ln = (r[cols.get('Last Name', 3)] or '').strip()
    if not fn and not ln: continue
    out.append({
        'firstName': fn,
        'lastName': ln,
        'fullName': (fn + ' ' + ln).strip(),
        'title': (r[cols.get('Title', 4)] or '').strip(),
        'company': (r[cols.get('Company Name', 5)] or '').strip(),
        'city': (r[cols.get('Work City', 8)] or '').strip(),
        'state': (r[cols.get('Work State/Province', 9)] or '').strip(),
        'country': (r[cols.get('Work Country', 11)] or '').strip(),
        'workPhone': str(r[cols.get('Work Phone Number', 12)] or '').strip(),
        'mobilePhone': str(r[cols.get('Mobile Phone Number', 13)] or '').strip(),
        'registered': str(r[0] or '').lower() == 'x',
    })
print(json.dumps(out))
`;
  const result = spawnSync("python", ["-c", py, xlsxPath], {
    encoding: "utf8",
    maxBuffer: 20 * 1024 * 1024,
  });
  if (result.status !== 0) {
    console.error(result.stderr || result.stdout);
    throw new Error("Failed to parse roster via Python/openpyxl");
  }
  return JSON.parse(result.stdout.trim());
}

function loadEnrichmentContacts() {
  /** @type {object[]} */
  const enrichmentContacts = [];
  try {
    for (const file of readdirSync(ENRICHMENT_DIR).filter((f) => f.endsWith(".json"))) {
      try {
        const rec = JSON.parse(readFileSync(join(ENRICHMENT_DIR, file), "utf8"));
        if (rec.contact?.name) {
          enrichmentContacts.push({
            ownerName: rec.ownerName,
            contactName: rec.contact.name,
            tier: rec.contact.verificationTier,
            file,
          });
        }
      } catch {
        /* skip */
      }
    }
  } catch {
    /* enrichment dir optional */
  }
  return enrichmentContacts;
}

async function main() {
  if (!existsSync(ROSTER_PATH)) {
    console.error(`Roster not found: ${ROSTER_PATH}`);
    process.exit(1);
  }

  mkdirSync(DATA_DIR, { recursive: true });
  const archivedRoster = join(DATA_DIR, "alis-cala-2026-delegate-roster.xlsx");
  if (ROSTER_PATH !== archivedRoster) {
    copyFileSync(ROSTER_PATH, archivedRoster);
  }

  const delegates = parseDelegatesViaPython(ROSTER_PATH);
  const strike = loadJson(STRIKE_JSON);
  const branding = loadJson(BRANDING_JSON);

  /** @type {object[]} */
  const brandingItems = branding.items || [];

  /** Build owner index from strike + branding + enrichments */
  /** @type {object[]} */
  const knownOwners = [];

  for (const row of strike.strikeList || []) {
    knownOwners.push({
      ownerTargetId: row.id,
      ownerName: row.ownerName,
      source: "strike_list",
      priorityTier: row.priorityTier,
      contactName: row.primaryContactName || "",
      contactEmail: row.primaryContactEmail || "",
      hasVerifiedContact: row.hasVerifiedContact,
      calaPropertyCount: row.calaPropertyCount,
    });
  }

  const strikeIds = new Set(knownOwners.map((o) => o.ownerTargetId));
  for (const row of brandingItems) {
    if (strikeIds.has(row.ownerTargetId)) continue;
    if ((row.intentScore || 0) < 40) continue;
    knownOwners.push({
      ownerTargetId: row.ownerTargetId,
      ownerName: row.ownerName,
      source: "branding_targets",
      priorityTier: row.priorityTier,
      contactName: row.contactName || "",
      contactEmail: row.contactEmail || "",
      hasVerifiedContact: row.hasVerifiedContact,
      calaPropertyCount: row.calaPropertyCount,
      intentScore: row.intentScore,
      outreachReady: row.outreachReady,
    });
  }

  const enrichmentContacts = loadEnrichmentContacts();

  /** Match delegates to known owners/contacts */
  /** @type {object[]} */
  const attendeeMatches = [];

  for (const d of delegates) {
    const personNorm = normalizeLoose(d.fullName);
    const companyNorm = normalizeCompany(d.company);
    let best = null;

    for (const owner of knownOwners) {
      const ownerNorm = normalizeLoose(owner.ownerName);
      const ownerCompanyNorm = normalizeCompany(owner.ownerName);
      const contactNorm = normalizeLoose(owner.contactName);

      let score = 0;
      let matchType = [];

      if (contactNorm && personNorm === contactNorm) {
        score += 100;
        matchType.push("contact_name_exact");
      } else if (contactNorm && personNorm.includes(contactNorm.split(" ")[0]) && contactNorm.includes(d.lastName.toLowerCase())) {
        score += 70;
        matchType.push("contact_name_partial");
      }

      if (companyNorm && (companyNorm.includes(ownerCompanyNorm) || ownerCompanyNorm.includes(companyNorm))) {
        score += 50;
        matchType.push("company_fuzzy");
      }
      if (companyNorm && ownerNorm && (companyNorm.includes(ownerNorm) || ownerNorm.includes(companyNorm))) {
        score += 40;
        matchType.push("owner_company_fuzzy");
      }

      // enrichment contact match
      for (const ec of enrichmentContacts) {
        if (normalizeLoose(ec.contactName) === personNorm) {
          score += 90;
          matchType.push(`enrichment_contact:${ec.file}`);
        }
        if (normalizeCompany(ec.ownerName) && companyNorm.includes(normalizeCompany(ec.ownerName))) {
          score += 30;
        }
      }

      if (score >= 70 && (!best || score > best.score)) {
        best = { ...owner, score, matchType: [...new Set(matchType)] };
      }
    }

    if (best) {
      attendeeMatches.push({
        delegate: d,
        matchedOwner: best.ownerName,
        ownerTargetId: best.ownerTargetId,
        ownerSource: best.source,
        priorityTier: best.priorityTier,
        knownContact: best.contactName,
        matchScore: best.score,
        matchType: best.matchType,
        outreachReady: best.outreachReady,
        hasVerifiedContact: best.hasVerifiedContact,
      });
    }
  }

  /** Net-new owner leads from roster */
  const matchedDelegateKeys = new Set(
    attendeeMatches.map((m) => normalizeLoose(m.delegate.fullName + m.delegate.company))
  );

  /** @type {object[]} */
  const netNewLeads = [];
  for (const d of delegates) {
    const key = normalizeLoose(d.fullName + d.company);
    if (matchedDelegateKeys.has(key)) continue;
    if (!OWNER_TITLE_RE.test(d.title || "")) continue;
    if (EXCLUDE_COMPANY_RE.some((re) => re.test(d.company || "") || re.test(d.title || ""))) continue;
    if (!d.company) continue;
    netNewLeads.push({
      ...d,
      leadReason: "owner_title_at_alis_cala_2026",
      inStrikeList: false,
    });
  }

  // dedupe net new by company
  const seenCompany = new Set();
  const netNewDeduped = netNewLeads.filter((l) => {
    const c = normalizeCompany(l.company);
    if (seenCompany.has(c)) return false;
    seenCompany.add(c);
    return true;
  });

  const summary = {
    rosterPath: ROSTER_PATH,
    archivedRoster,
    generatedAt: new Date().toISOString(),
    delegateCount: delegates.length,
    registeredCount: delegates.filter((d) => d.registered).length,
    strikeListSize: (strike.strikeList || []).length,
    brandingTargetsScored: brandingItems.length,
    attendeeMatches: attendeeMatches.length,
    netNewOwnerLeads: netNewDeduped.length,
  };

  const output = {
    summary,
    attendeeMatches: attendeeMatches.sort((a, b) => b.matchScore - a.matchScore),
    netNewOwnerLeads: netNewDeduped.sort((a, b) => (b.registered ? 1 : 0) - (a.registered ? 1 : 0)),
  };

  writeFileSync(join(REPORTS, "gtm-alis-cala-2026-delegate-crossref.json"), JSON.stringify(output, null, 2));

  /** CSV for attendee matches */
  const matchCsv = [
    "matchScore,registered,delegateName,delegateTitle,delegateCompany,delegateCountry,matchedOwner,ownerTargetId,priorityTier,knownContact,matchType,outreachReady,hasVerifiedContact",
    ...attendeeMatches.map((m) =>
      [
        m.matchScore,
        m.delegate.registered,
        `"${m.delegate.fullName.replace(/"/g, '""')}"`,
        `"${(m.delegate.title || "").replace(/"/g, '""')}"`,
        `"${(m.delegate.company || "").replace(/"/g, '""')}"`,
        `"${(m.delegate.country || "").replace(/"/g, '""')}"`,
        `"${m.matchedOwner.replace(/"/g, '""')}"`,
        m.ownerTargetId,
        m.priorityTier,
        `"${(m.knownContact || "").replace(/"/g, '""')}"`,
        `"${m.matchType.join("|")}"`,
        m.outreachReady ?? "",
        m.hasVerifiedContact ?? "",
      ].join(",")
    ),
  ].join("\n");
  writeFileSync(join(REPORTS, "gtm-alis-cala-2026-delegate-matches.csv"), matchCsv);

  const leadsCsv = [
    "registered,fullName,title,company,country,city,leadReason",
    ...netNewDeduped.map((l) =>
      [
        l.registered,
        `"${l.fullName.replace(/"/g, '""')}"`,
        `"${(l.title || "").replace(/"/g, '""')}"`,
        `"${(l.company || "").replace(/"/g, '""')}"`,
        `"${(l.country || "").replace(/"/g, '""')}"`,
        `"${(l.city || "").replace(/"/g, '""')}"`,
        l.leadReason,
      ].join(",")
    ),
  ].join("\n");
  writeFileSync(join(REPORTS, "gtm-alis-cala-2026-net-new-owner-leads.csv"), leadsCsv);

  /** Markdown summary */
  const md = [
    "# ALIS CALA 2026 — Delegate Cross-Reference",
    "",
    `Generated: ${summary.generatedAt}`,
    "",
    "## Summary",
    "",
    `- Delegates in roster: **${summary.delegateCount}** (${summary.registeredCount} marked registered)`,
    `- Matches to strike list / branding targets: **${summary.attendeeMatches}**`,
    `- Net-new owner-like leads (not in target list): **${summary.netNewOwnerLeads}**`,
    "",
    "## Strike list / target attendees (high confidence)",
    "",
    ...attendeeMatches
      .filter((m) => m.matchScore >= 90)
      .slice(0, 40)
      .map(
        (m) =>
          `- **${m.delegate.fullName}** (${m.delegate.title}) — ${m.delegate.company} → matched **${m.matchedOwner}** [${m.matchType.join(", ")}]${m.delegate.registered ? " ✓ registered" : ""}`
      ),
    "",
    "## Net-new owner leads to research (sample)",
    "",
    ...netNewDeduped.slice(0, 30).map(
      (l) =>
        `- **${l.fullName}** — ${l.title} @ ${l.company} (${l.country})${l.registered ? " ✓" : ""}`
    ),
    "",
    "## Commands",
    "",
    "```bash",
    "node scripts/report-alis-cala-delegate-crossref.mjs",
    'node scripts/report-alis-cala-delegate-crossref.mjs --roster="path/to/roster.xlsx"',
    "```",
  ].join("\n");
  writeFileSync(join(REPORTS, "gtm-alis-cala-2026-delegate-crossref.md"), md);

  console.log("ALIS CALA 2026 delegate cross-reference");
  console.log(`  Delegates: ${summary.delegateCount} (${summary.registeredCount} registered)`);
  console.log(`  Target list matches: ${summary.attendeeMatches}`);
  console.log(`  Net-new owner leads: ${summary.netNewOwnerLeads}`);
  console.log(`Wrote ${join(REPORTS, "gtm-alis-cala-2026-delegate-crossref.json")}`);
  console.log(`Wrote ${join(REPORTS, "gtm-alis-cala-2026-delegate-matches.csv")}`);
  console.log(`Wrote ${join(REPORTS, "gtm-alis-cala-2026-net-new-owner-leads.csv")}`);
  console.log(`Wrote ${join(REPORTS, "gtm-alis-cala-2026-delegate-crossref.md")}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
