#!/usr/bin/env node
/**
 * HIF V6 Track B Wave 2 — deepen property links for Active SIC 55100 CH entities
 * + remaining candidate cohort. Free public HTML only unless API key present.
 * Never logs API key. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [
  path.join(ROOT, ".env"),
  "C:/Dev/dealality/my-operators-backend/.env",
  "C:/Dev/dealality/ws-hotel-explorer/.env",
]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-b-wave2");
const RUN = process.argv.includes("--run");
const UA =
  process.env.SEC_EDGAR_USER_AGENT ||
  process.env.EDGAR_USER_AGENT ||
  "DealalityHIFResearch/6.0 (research@dealality.com)";

const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function getChKey() {
  return String(process.env.COMPANIES_HOUSE_API_KEY || "").trim() || null;
}

async function fetchText(url, headers = {}) {
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": UA, Accept: "text/html,application/json;q=0.9,*/*;q=0.8", ...headers },
      redirect: "follow",
      signal: AbortSignal.timeout(35000),
    });
    const text = await res.text();
    return { ok: res.ok, status: res.status, text };
  } catch (err) {
    return { ok: false, status: null, text: "", error: String(err?.message || err) };
  }
}

function parseCompanyNumber(label) {
  const m = String(label || "").match(/\[([A-Z0-9]{6,8})\]\s*$/);
  return m ? m[1] : null;
}

function loadAllCandidates() {
  const fp = path.join(ROOT, "reports/sec-edgar-hif-v6-track-b-regulatory/ENTITY_CANDIDATES.json");
  const j = JSON.parse(fs.readFileSync(fp, "utf8"));
  return (j.entities || []).map((e) => ({
    companyNumber: parseCompanyNumber(e.legalNameOrLabel),
    legalName: String(e.legalNameOrLabel || "").replace(/\s*\[[A-Z0-9]+\]\s*$/, "").trim(),
    sourceUrl: e.sourceUrl,
  })).filter((e) => e.companyNumber);
}

function extractNamedHotels(text) {
  const hotels = new Set();
  const patterns = [
    /\b((?:The\s+)?(?:Marriott|Hilton|Holiday Inn|Premier Inn|Travelodge|Ibis|Novotel|Mercure|Crowne Plaza|DoubleTree|Courtyard|Residence Inn|AC Hotel|Moxy|W Hotel|Sheraton|Westin|Renaissance|Hyatt|Radisson|Best Western)[A-Za-z0-9 &,'\-]{0,60})\b/gi,
    /\b([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,3}\s+Hotel)\b/g,
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(text))) {
      const name = m[1].replace(/\s+/g, " ").trim();
      if (name.length >= 8 && name.length <= 80) hotels.add(name);
    }
  }
  return [...hotels].slice(0, 30);
}

function extractChargePropertyHints(text) {
  const hints = [];
  const re = /(?:freehold|leasehold|land|property|hotel)[^\n.]{0,120}/gi;
  let m;
  while ((m = re.exec(text))) {
    const s = m[0].replace(/\s+/g, " ").trim();
    if (s.length > 20) hints.push(s.slice(0, 160));
    if (hints.length >= 15) break;
  }
  return hints;
}

async function deepenCompany(c, key) {
  const row = {
    companyNumber: c.companyNumber,
    legalName: c.legalName,
    companyStatus: null,
    hasHotelsSic: false,
    filingsInspected: 0,
    namedHotelsFromFilings: [],
    chargePropertyHints: [],
    propertyLinkStatus: "NOT_VERIFIED",
    ownerRole: null,
    operatorRole: null,
    evidence: [],
    contractualExpiration: null,
  };

  const profileUrl = key
    ? `https://api.company-information.service.gov.uk/company/${c.companyNumber}`
    : `https://find-and-update.company-information.service.gov.uk/company/${c.companyNumber}`;

  if (key) {
    const auth = `Basic ${Buffer.from(`${key}:`, "utf8").toString("base64")}`;
    const res = await fetchText(profileUrl, {
      Authorization: auth,
      Accept: "application/json",
    });
    if (res.ok) {
      try {
        const p = JSON.parse(res.text);
        row.companyStatus = p.company_status;
        row.hasHotelsSic = (p.sic_codes || []).includes("55100");
        row.companyName = p.company_name;
        row.evidence.push({ kind: "CH_API_PROFILE", url: profileUrl });
      } catch {
        /* ignore */
      }
    }
    // filing history
    const fh = await fetchText(
      `https://api.company-information.service.gov.uk/company/${c.companyNumber}/filing-history?items_per_page=20`,
      { Authorization: auth, Accept: "application/json" },
    );
    if (fh.ok) {
      try {
        const hist = JSON.parse(fh.text);
        row.filingsInspected = (hist.items || []).length;
        row.evidence.push({
          kind: "CH_API_FILING_HISTORY",
          url: `https://api.company-information.service.gov.uk/company/${c.companyNumber}/filing-history`,
        });
      } catch {
        /* ignore */
      }
    }
    // charges
    const ch = await fetchText(
      `https://api.company-information.service.gov.uk/company/${c.companyNumber}/charges`,
      { Authorization: auth, Accept: "application/json" },
    );
    if (ch.ok) {
      try {
        const charges = JSON.parse(ch.text);
        for (const item of charges.items || []) {
          const blob = JSON.stringify(item);
          row.chargePropertyHints.push(...extractChargePropertyHints(blob));
        }
        row.evidence.push({
          kind: "CH_API_CHARGES",
          url: `https://api.company-information.service.gov.uk/company/${c.companyNumber}/charges`,
          count: (charges.items || []).length,
        });
      } catch {
        /* ignore */
      }
    }
  } else {
    const res = await fetchText(profileUrl);
    if (res.ok) {
      const text = res.text;
      const statusMatch =
        text.match(/Company status<\/dt>\s*<dd[^>]*>\s*([^<]+)/i) ||
        text.match(/\b(Active|Dissolved|Liquidation)\b/);
      row.companyStatus = statusMatch ? statusMatch[1].replace(/\s+/g, " ").trim() : null;
      row.hasHotelsSic = /55100/.test(text);
      const nameMatch = text.match(/<p class="heading-xlarge">\s*([^<]+)/i);
      row.companyName = nameMatch ? nameMatch[1].replace(/\s+/g, " ").trim() : c.legalName;
      row.evidence.push({ kind: "CH_PUBLIC_HTML_PROFILE", url: profileUrl });
    }

    // Filing history HTML
    const fhUrl = `https://find-and-update.company-information.service.gov.uk/company/${c.companyNumber}/filing-history`;
    const fh = await fetchText(fhUrl);
    if (fh.ok) {
      row.filingsInspected = (fh.text.match(/filing-history/gi) || []).length;
      row.namedHotelsFromFilings = extractNamedHotels(fh.text);
      row.evidence.push({ kind: "CH_PUBLIC_FILING_HISTORY", url: fhUrl });
    }
    await sleep(100);

    // Charges HTML
    const chUrl = `https://find-and-update.company-information.service.gov.uk/company/${c.companyNumber}/charges`;
    const ch = await fetchText(chUrl);
    if (ch.ok) {
      row.chargePropertyHints = extractChargePropertyHints(ch.text.replace(/<[^>]+>/g, " "));
      row.namedHotelsFromFilings = [
        ...new Set([...row.namedHotelsFromFilings, ...extractNamedHotels(ch.text)]),
      ].slice(0, 30);
      row.evidence.push({ kind: "CH_PUBLIC_CHARGES", url: chUrl });
    }
  }

  // Cross-border free EDGAR probe for known brand entities (Marriott UK)
  if (/marriott/i.test(row.companyName || c.legalName)) {
    const edgarUrl =
      "https://efts.sec.gov/LATEST/search-index?q=%22Marriott%20Hotels%20Limited%22&dateRange=custom&startdt=2018-01-01&enddt=2026-11-21";
    const er = await fetchText(edgarUrl, { Accept: "application/json" });
    if (er.ok) {
      row.evidence.push({ kind: "SEC_EFTS_PROBE", url: edgarUrl, bytes: er.text.length });
      const hotels = extractNamedHotels(er.text);
      if (hotels.length) {
        row.namedHotelsFromFilings = [...new Set([...row.namedHotelsFromFilings, ...hotels])].slice(0, 30);
      }
    }
  }

  // Verification gate: named hotel from charges/filings + Active = RESEARCH_LINKED not full ownership verify
  if (row.namedHotelsFromFilings.length > 0 || row.chargePropertyHints.length > 0) {
    row.propertyLinkStatus = "EVIDENCE_HINTS_PRESENT_NOT_TITLE_VERIFIED";
    row.ownerRole = "UNKNOWN_REQUIRES_TITLE_OR_ACCOUNTS_ASSET_NOTE";
    row.operatorRole = /management/i.test(row.companyName || c.legalName)
      ? "POSSIBLE_OPERATOR_UNVERIFIED"
      : "UNKNOWN";
  } else if (row.hasHotelsSic && /active/i.test(String(row.companyStatus || ""))) {
    row.propertyLinkStatus = "RESEARCH_CANDIDATE_SIC_ACTIVE_ONLY";
  }

  return row;
}

async function main() {
  const key = getChKey();
  const all = loadAllCandidates();
  // Prioritize active hospitality names, then remainder up to 61
  const prioritize = (c) => {
    const n = c.legalName.toLowerCase();
    if (n.includes("marriott")) return 0;
    if (n.includes("hilton")) return 1;
    if (n.includes("hotel management international")) return 2;
    if (n.includes("hotel management")) return 3;
    return 9;
  };
  const ordered = [...all].sort((a, b) => prioritize(a) - prioritize(b));
  const target = ordered.slice(0, RUN ? 40 : 5);

  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, target: target.length, keyPresent: Boolean(key) }, null, 2));
    return;
  }

  const deepened = [];
  for (const c of target) {
    deepened.push(await deepenCompany(c, key));
    await sleep(180);
  }

  const withHints = deepened.filter(
    (d) => d.namedHotelsFromFilings.length > 0 || d.chargePropertyHints.length > 0,
  );
  const linked = deepened.filter((d) => d.propertyLinkStatus === "EVIDENCE_HINTS_PRESENT_NOT_TITLE_VERIFIED");

  // International portal: try to extract issuer names containing hotel from retrieved CVM/SGX bodies if cached
  const portalPath = path.join(OUT, "PORTAL_FILING_PROBES.json");
  let portalHotelIssuers = [];
  if (fs.existsSync(portalPath)) {
    // Re-fetch one Brazil CVM dataset search and extract dataset titles
    const cvm = await fetchText("https://dados.cvm.gov.br/dataset?q=hotel");
    if (cvm.ok) {
      const titles = [...cvm.text.matchAll(/href="\/dataset\/([^"]+)"[^>]*>\s*([^<]{5,120})/gi)]
        .map((m) => ({ slug: m[1], title: m[2].replace(/\s+/g, " ").trim() }))
        .filter((t) => /hotel|hospeda|resort|turismo/i.test(t.title + t.slug))
        .slice(0, 20);
      portalHotelIssuers = titles.map((t) => ({
        portal: "BR_CVM",
        ...t,
        sourceUrl: `https://dados.cvm.gov.br/dataset/${t.slug}`,
        hotelPropertyExtracted: false,
        agreementDateExtracted: false,
        note: "Dataset/listing title recovered — not yet a property-level agreement fact",
      }));
    }
  }

  writeJson(path.join(OUT, "CH_PROPERTY_LINK_DEEPEN.json"), {
    count: deepened.length,
    withEvidenceHints: withHints.length,
    deepened,
  });
  writeJson(path.join(OUT, "PORTAL_HOTEL_ISSUER_HINTS.json"), {
    count: portalHotelIssuers.length,
    issuers: portalHotelIssuers,
  });

  // Update metrics honestly
  const metricsPath = path.join(OUT, "TRACK_B_WAVE2_METRICS.json");
  const metrics = fs.existsSync(metricsPath)
    ? JSON.parse(fs.readFileSync(metricsPath, "utf8"))
    : {};
  metrics.candidatesInvestigated = Math.max(metrics.candidatesInvestigated || 0, deepened.length);
  metrics.legalEntitiesVerified = deepened.filter((d) => d.companyStatus).length;
  metrics.evidenceHintEntities = withHints.length;
  metrics.physicalHotelsLinked = 0; // still no title/accounts asset-verified property links
  metrics.propertyOwnersVerified = 0;
  metrics.operatorsVerified = 0;
  metrics.portalHotelIssuerHints = portalHotelIssuers.length;
  metrics.chApiKeyPresent = Boolean(key);
  metrics.chApiAuthOutcome = key ? metrics.chApiAuthOutcome || "KEY_PRESENT" : "KEY_ABSENT_PUBLIC_HTML_ONLY";
  metrics.note =
    "Property owners/operators remain 0 until title, named-asset accounts, or executed contract evidence confirms hotel identity";
  writeJson(metricsPath, metrics);

  // Append founder section
  const md = [
    "",
    "## Property-link deepen",
    `- Entities deepened: ${deepened.length}`,
    `- Evidence hints (charges/filings lexicon): ${withHints.length}`,
    `- Title/accounts-verified hotel links: 0`,
    `- Portal hotel issuer/dataset hints (BR CVM): ${portalHotelIssuers.length}`,
    "",
    "### Hint examples (not verified ownership)",
    ...withHints.slice(0, 8).map(
      (d) =>
        `- ${d.companyName || d.legalName} [${d.companyNumber}] · hotels=${(d.namedHotelsFromFilings || []).slice(0, 3).join("; ") || "—"} · charges=${(d.chargePropertyHints || []).length}`,
    ),
  ].join("\n");
  fs.appendFileSync(path.join(OUT, "FOUNDER_TRACK_B_WAVE2.md"), md, "utf8");

  console.log(
    JSON.stringify(
      {
        ok: true,
        deepened: deepened.length,
        withHints: withHints.length,
        linkedNotVerified: linked.length,
        portalHotelIssuers: portalHotelIssuers.length,
        keyPresent: Boolean(key),
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
