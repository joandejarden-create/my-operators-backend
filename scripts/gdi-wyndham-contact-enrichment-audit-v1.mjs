#!/usr/bin/env node
/**
 * Read-only Wyndham STAGED contact audit for enrichment completion.
 * No Findymail/Surfe/Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";

const ROOT = process.cwd();
const HOTEL = "recnPB3Q02NO8Ma1o";
const VERSION = "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_6e217b8a";
const PKG = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL,
  VERSION,
  "package.json",
);
const OUT_DIR = path.join(ROOT, "reports/gdi-contact-enrichment-completion-v1");

function text(v) {
  return String(v == null ? "" : v).trim();
}

function isLinkedIn(u) {
  return /^https?:\/\/([a-z0-9-]+\.)?linkedin\.com\/in\//i.test(text(u));
}

function personKey(p) {
  return [text(p.fullName || p.name).toLowerCase(), text(p.organization || p.org || p.company).toLowerCase()].join(
    "|",
  );
}

function incompleteName(name) {
  const n = text(name);
  if (!n || n === "—") return false;
  return /^[A-Z][a-z]+\s+[A-Z]\.?$/.test(n) || /\b[A-Z]\.$/.test(n);
}

function collectContacts(o) {
  const all = [];
  const push = (c) => {
    if (c && typeof c === "object") all.push(c);
  };
  push(o.primaryContact);
  push(o.whoToContact);
  push(o.contact);
  for (const c of o.alternativeContacts || []) push(c);
  for (const c of o.buyerInfluencers || []) push(c);
  for (const c of o.alternativeEntryPoints || []) push(c);
  const wh = o.whoHow || o.whoHowResolution || {};
  push(wh.primary);
  push(wh.primaryContact);
  for (const c of wh.alternatives || []) push(c);
  for (const c of wh.alternativeEntryPoints || []) push(c);
  for (const c of wh.contacts || []) push(c);
  for (const c of o.contacts || []) push(c);

  const seen = new Set();
  const uniq = [];
  for (const c of all) {
    const k = personKey(c);
    if (!k || k === "|" || seen.has(k)) continue;
    seen.add(k);
    uniq.push(c);
  }
  return uniq;
}

function channelOf(c) {
  const email = text(c.email || c.professionalEmail || c.workEmail);
  const phone = text(c.phone || c.directPhone || c.professionalPhone || c.workPhone);
  const li =
    text(c.linkedinUrl || c.linkedInUrl) ||
    (isLinkedIn(c.sourceUrl) ? text(c.sourceUrl) : "") ||
    (isLinkedIn(c.personSourceUrl) ? text(c.personSourceUrl) : "");
  return { email, phone, li };
}

function classifyPerson(p) {
  if (incompleteName(p.name)) return "NEEDS_IDENTITY_RESOLUTION";
  if (p.email && p.phone && p.li) return "COMPLETE_THREE_CHANNEL";
  if (p.email || p.phone || p.li) {
    if (!p.email || !p.phone) return "NEEDS_TARGETED_ENRICHMENT";
    return "PARTIAL_USABLE";
  }
  return "NOT_RESEARCHED_OR_UNAVAILABLE";
}

const pkg = JSON.parse(fs.readFileSync(PKG, "utf8"));
const opps = pkg.opportunities || [];
const people = new Map();
const rows = [];

for (const o of opps) {
  const contacts = collectContacts(o);
  const primary = contacts[0] || o.primaryContact || {};
  const ch = channelOf(primary || {});
  const name = text(primary.fullName || primary.name) || "—";
  const stage4 = o.contactResearch || o.stage4ContactResearch || o.professionalContactResearch || {};
  rows.push({
    opportunityId: o.opportunityId || o.id,
    title: text(o.title || o.name).slice(0, 80),
    primaryName: name,
    primaryTitle: text(primary.title || primary.role || primary.professionalTitle).slice(0, 80),
    primaryOrg: text(primary.organization || primary.org || primary.company).slice(0, 80),
    email: Boolean(ch.email),
    phone: Boolean(ch.phone),
    linkedin: Boolean(ch.li),
    emailValuePresent: Boolean(ch.email),
    contactCount: contacts.length,
    incompletePrimary: incompleteName(name),
    stage4Annotated: Boolean(stage4 && Object.keys(stage4).length),
    findymailUsed: stage4.findymailUsed === true || o.findymailUsed === true,
    contactRoles: contacts.map((c) => ({
      name: text(c.fullName || c.name),
      role: text(c.commercialRole || c.roleType || c.entryPointType || c.role),
      ...Object.fromEntries(
        Object.entries(channelOf(c)).map(([k, v]) => [k, Boolean(v)]),
      ),
    })),
  });

  for (const c of contacts) {
    const k = personKey(c);
    const chP = channelOf(c);
    const cur = people.get(k) || {
      name: text(c.fullName || c.name),
      org: text(c.organization || c.org || c.company),
      title: text(c.title || c.role || c.professionalTitle),
      email: "",
      phone: "",
      li: "",
      opps: [],
      sourceHints: [],
    };
    cur.email = cur.email || chP.email;
    cur.phone = cur.phone || chP.phone;
    cur.li = cur.li || chP.li;
    cur.title = cur.title || text(c.title || c.role || c.professionalTitle);
    cur.opps.push(o.opportunityId || o.id);
    if (c.sourceUrl) cur.sourceHints.push(text(c.sourceUrl).slice(0, 120));
    people.set(k, cur);
  }
}

const plist = [...people.values()].map((p) => ({
  ...p,
  classification: classifyPerson(p),
  incomplete: incompleteName(p.name),
}));

const beforePath = path.join(ROOT, "reports/gdi-contact-research-hydration-v1/03-COVERAGE_SCORECARD.json");
let before = null;
if (fs.existsSync(beforePath)) {
  before = JSON.parse(fs.readFileSync(beforePath, "utf8"));
}

const summary = {
  hotelId: HOTEL,
  publicationVersion: VERSION,
  auditedAt: new Date().toISOString(),
  opportunities: rows.length,
  namedOps: rows.filter((r) => r.primaryName && r.primaryName !== "—").length,
  emailOps: rows.filter((r) => r.email).length,
  phoneOps: rows.filter((r) => r.phone).length,
  linkedinOps: rows.filter((r) => r.linkedin).length,
  incompleteNameOps: rows.filter((r) => r.incompletePrimary).length,
  uniquePeople: plist.length,
  withFullIdentity: plist.filter((p) => p.name && !incompleteName(p.name)).length,
  withTitle: plist.filter((p) => p.title).length,
  withEmail: plist.filter((p) => p.email).length,
  withPhone: plist.filter((p) => p.phone).length,
  withLinkedIn: plist.filter((p) => p.li).length,
  emailOrPhone: plist.filter((p) => p.email || p.phone).length,
  allThreeChannels: plist.filter((p) => p.email && p.phone && p.li).length,
  atLeastOneUsableChannel: plist.filter((p) => p.email || p.phone || p.li).length,
  incompletePeople: plist.filter((p) => p.incomplete),
  byClassification: plist.reduce((acc, p) => {
    acc[p.classification] = (acc[p.classification] || 0) + 1;
    return acc;
  }, {}),
  findymailProductionIntegration: "NOT_WIRED_FOR_AUTO_PERSIST — separate founder GO required",
  priorScorecardBefore: before?.before || null,
  priorScorecardAfterHydration: before?.after || null,
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(
  path.join(OUT_DIR, "00-CONTACT_AUDIT.json"),
  JSON.stringify({ summary, opportunities: rows, people: plist }, null, 2),
);

const md = `# Wyndham Contact Enrichment Audit (read-only)

- Hotel: \`${HOTEL}\`
- Package: \`${VERSION}\`
- Audited: ${summary.auditedAt}

## Opportunity grain (primary contact)

| Metric | Count |
|---|---:|
| Qualified opportunities | ${summary.opportunities} |
| Named primary | ${summary.namedOps} |
| With email | ${summary.emailOps} |
| With phone | ${summary.phoneOps} |
| With LinkedIn | ${summary.linkedinOps} |
| Incomplete primary name | ${summary.incompleteNameOps} |

## Unique-person grain

| Metric | Count |
|---|---:|
| Unique commercial contacts | ${summary.uniquePeople} |
| Full identity | ${summary.withFullIdentity} |
| With title | ${summary.withTitle} |
| Professional email | ${summary.withEmail} |
| Professional phone | ${summary.withPhone} |
| Personal LinkedIn | ${summary.withLinkedIn} |
| Email or phone | ${summary.emailOrPhone} |
| All three channels | ${summary.allThreeChannels} |
| ≥1 usable outreach channel | ${summary.atLeastOneUsableChannel} |

## Classification

${Object.entries(summary.byClassification)
  .map(([k, v]) => `- **${k}**: ${v}`)
  .join("\n")}

## Incomplete identities

${
  summary.incompletePeople.length
    ? summary.incompletePeople.map((p) => `- ${p.name} (${p.org || "—"}) · opps: ${p.opps.join(", ")}`).join("\n")
    : "_None detected by initialism heuristic._"
}

## Findymail

Production Findymail auto-persist is **not wired**. Prior V2 analysis: public-first optimization preferred; Findymail requires separate founder credit authorization. Do not spend until enrichment manifest + budget GO.

## Next actions (no spend yet)

1. Rematerialize / recover any EXISTS_NOT_DISPLAYED LinkedIn/email already in sourceUrl or WHO/HOW artifacts.
2. Resolve incomplete names via public WHO research (not paid enrichment).
3. Prepare Findymail manifest only for verified full identities missing email after public Stage 4.
4. Unbox List Actions SVG (UI) and enforce three-channel List/Drawer parity.
`;

fs.writeFileSync(path.join(OUT_DIR, "01-AUDIT_SUMMARY.md"), md);
console.log(JSON.stringify(summary, null, 2));
console.log(`Wrote ${path.join(OUT_DIR, "00-CONTACT_AUDIT.json")}`);
