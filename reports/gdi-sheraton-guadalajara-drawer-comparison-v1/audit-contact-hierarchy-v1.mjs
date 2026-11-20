/**
 * Sheraton Guadalajara Expo — GDI 3.0 contact email/phone hierarchy audit.
 * Read-only public recovery + dry-run Airtable proposals. No production writes.
 * No Surfe. No paid enrichment.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildSheratonDrawerComparisonInventory } from "../../lib/group-demand-intelligence/presentation/gdi-sheraton-drawer-comparison-v1.js";
import {
  recoverPublicContactChannels,
  buildContactChannelDisplayState,
  proposeAirtablePublicContactUpsert,
  GDI_PUBLIC_CONTACT_CHANNEL_HIERARCHY_V1,
} from "../../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const outJson = path.join(__dirname, "CONTACT_HIERARCHY_MATRIX.json");
const outMd = path.join(__dirname, "CONTACT_HIERARCHY_FOUNDER_REVIEW.md");

const inv = buildSheratonDrawerComparisonInventory();
const rows = [];
const upserts = [];
const concurrency = 4;
let idx = 0;

async function processOne(item) {
  const raw = item.raw || {};
  const before = buildContactChannelDisplayState(raw);
  const recovered = await recoverPublicContactChannels(raw, {
    timeoutMs: 7000,
    maxUrls: 3,
  });
  const afterOpp = recovered.opportunity || raw;
  const after = buildContactChannelDisplayState(afterOpp);
  const proposal = proposeAirtablePublicContactUpsert(afterOpp, after);
  if (proposal && recovered.changed) upserts.push(proposal);

  return {
    opportunityId: item.id,
    title: raw.title || raw.displayTitle || item.title || null,
    inCurrentStaged: Boolean(item.inCurrentStaged),
    recoveryReason: recovered.reason || null,
    changed: Boolean(recovered.changed),
    before,
    after,
    airtablePublicContactPresent: false, // not queried live in this pass
    airtableUpsertProposed: Boolean(proposal && recovered.changed),
    insufficientRoleOrSource: Boolean(
      after.namedContact &&
        !after.publicDirectEmail &&
        !after.publicDirectPhone &&
        !after.organizationalEmail &&
        !after.linkedInOrProfile,
    ),
  };
}

const queue = [...inv.items];
const workers = Array.from({ length: concurrency }, async () => {
  while (idx < queue.length) {
    const i = idx++;
    const item = queue[i];
    try {
      rows.push(await processOne(item));
    } catch (err) {
      rows.push({
        opportunityId: item.id,
        title: item.raw?.title || null,
        error: String(err?.message || err),
      });
    }
  }
});
await Promise.all(workers);

rows.sort((a, b) => String(a.opportunityId).localeCompare(String(b.opportunityId)));

const euroguss = rows.find(
  (r) =>
    /euroguss/i.test(String(r.title || "")) ||
    /gissella/i.test(String(r.after?.namedContact || r.before?.namedContact || "")),
);

const summary = {
  version: GDI_PUBLIC_CONTACT_CHANNEL_HIERARCHY_V1,
  hotelId: "recCAGqkERSXgiku4",
  auditedAt: new Date().toISOString(),
  total: rows.length,
  namedContact: rows.filter((r) => r.after?.namedContact).length,
  namedDirectBuyer: rows.filter(
    (r) => r.after?.roleClassification?.code === "NAMED_DIRECT_BUYER",
  ).length,
  namedInfluencer: rows.filter(
    (r) => r.after?.roleClassification?.code === "NAMED_RELEVANT_INFLUENCER",
  ).length,
  publicDirectEmailAfter: rows.filter((r) => r.after?.publicDirectEmail).length,
  publicDirectPhoneAfter: rows.filter((r) => r.after?.publicDirectPhone).length,
  organizationalFallbackAfter: rows.filter(
    (r) => r.after?.organizationalEmail || r.after?.organizationalPhone,
  ).length,
  linkedInOrProfileAfter: rows.filter((r) => r.after?.linkedInOrProfile).length,
  channelsRecoveredThisPass: rows.filter((r) => r.changed).length,
  proposedAirtableUpserts: upserts.length,
  surfeEligibleAfter: rows.filter((r) => r.after?.surfeEnrichmentEligible).length,
  demoStateCounts: rows.reduce((acc, r) => {
    const k = r.after?.demoState || "UNKNOWN";
    acc[k] = (acc[k] || 0) + 1;
    return acc;
  }, {}),
  eurogussRegression: euroguss
    ? {
        opportunityId: euroguss.opportunityId,
        title: euroguss.title,
        before: {
          email: euroguss.before?.publicDirectEmail || null,
          phone: euroguss.before?.publicDirectPhone || null,
          org: euroguss.before?.organizationalEmail || null,
          surfe: euroguss.before?.surfeCtaLabel || null,
        },
        after: {
          email: euroguss.after?.publicDirectEmail || null,
          phone: euroguss.after?.publicDirectPhone || null,
          org: euroguss.after?.organizationalEmail || null,
          surfe: euroguss.after?.surfeCtaLabel || null,
          source: euroguss.after?.channelSourceUrl || null,
          role: euroguss.after?.roleClassification?.code || null,
        },
        changed: euroguss.changed,
        recoveryReason: euroguss.recoveryReason,
      }
    : null,
};

const payload = { summary, rows, proposedAirtableUpserts: upserts };
fs.writeFileSync(outJson, JSON.stringify(payload, null, 2));

const md = `# GDI 3.0 — Contact email & phone hierarchy (Founder review)

**Hotel:** Sheraton Guadalajara Expo (\`recCAGqkERSXgiku4\`)  
**Audited:** ${summary.auditedAt}  
**Inventory:** ${summary.total} opportunities  
**PR gate:** #462 remains OPEN — local-only; **no production Airtable writes**, no Surfe, no paid enrichment, no competing PR.

## Hierarchy applied

1. Verified named direct commercial buyer / decision-maker  
2. Verified named commercially relevant influencer / referral  
3. Verified organizational / department route  
4. Published company switchboard / contact form / business route  
5. On-demand enrichment only for unresolved channels (Surfe — session-only)

Display labels (no card redesign): **Direct Business Email**, **Direct Business Phone**, **Organizational Contact Route**, Find Email / Find Phone / Find Email & Phone.

## Sheraton results (after native public recovery)

| Metric | Count |
| --- | ---: |
| Named contact | ${summary.namedContact} |
| Classed as direct buyer | ${summary.namedDirectBuyer} |
| Classed as relevant influencer/referral | ${summary.namedInfluencer} |
| Public direct email | ${summary.publicDirectEmailAfter} |
| Public direct phone | ${summary.publicDirectPhoneAfter} |
| Organizational fallback | ${summary.organizationalFallbackAfter} |
| LinkedIn / profile URL | ${summary.linkedInOrProfileAfter} |
| Channels recovered this pass | ${summary.channelsRecoveredThisPass} |
| Proposed Airtable upserts (dry-run) | ${summary.proposedAirtableUpserts} |
| Still Surfe-eligible (missing direct channel) | ${summary.surfeEligibleAfter} |

Demo-state distribution (A–D): ${JSON.stringify(summary.demoStateCounts)}

## EUROGUSS MEXICO 2027 / Gissella Palma (regression)

${
  euroguss
    ? `| | Before | After |
| --- | --- | --- |
| Email | ${euroguss.before?.publicDirectEmail || "—"} | ${euroguss.after?.publicDirectEmail || "—"} |
| Phone | ${euroguss.before?.publicDirectPhone || "—"} | ${euroguss.after?.publicDirectPhone || "—"} |
| Org route | ${euroguss.before?.organizationalEmail || "—"} | ${euroguss.after?.organizationalEmail || "—"} |
| Surfe CTA | ${euroguss.before?.surfeCtaLabel || "—"} | ${euroguss.after?.surfeCtaLabel || "none (complete)"} |
| Source | — | ${euroguss.after?.channelSourceUrl || "—"} |
| Role | ${euroguss.before?.roleClassification?.code || "—"} | ${euroguss.after?.roleClassification?.code || "—"} |
| Recovery | | ${euroguss.recoveryReason} / changed=${euroguss.changed} |`
    : "_EUROGUSS row not found in inventory._"
}

Public source: [euroguss-mexico.com/contacts](https://www.euroguss-mexico.com/contacts) lists Gissella Palma — Sales Manager — \`gissella.palma@nmna.us\` — \`+1 (770) 618-5833\`, plus org \`info@euroguss-mexico.com\`.

## Validation states (drawer)

| State | Meaning | How to demo |
| --- | --- | --- |
| A | Public email + phone | EUROGUSS / Gissella after recovery — both shown, **no** Find Email & Phone |
| B | Email only | Any row with email recovered, phone still missing → Find Phone |
| C | Phone only | Phone recovered, email missing → Find Email |
| D | Named, no direct channel | Organizational route + Find Email & Phone when eligible |
| E | Influencer, not assumed buyer | Role class \`NAMED_RELEVANT_INFLUENCER\` with honest why-relevant copy |
| F | Surfe session-only | Unchanged gated CTA; never persisted |
| G | Existing Airtable verified | Reuse when present — this pass did **not** live-query Airtable |

## Proposed Airtable upserts (DRY-RUN ONLY)

- Count: **${upserts.length}**
- Target: existing GDI opportunity / embedded contact intelligence (no new Contacts table)
- Includes: person, company, role, opportunity association, classification, public email/phone, org route, source URL, verification status, last verified date, provenance=\`PUBLIC_SOURCE\`, \`surfeDerived=false\`
- Policy: do not overwrite newer verified data with stale/blank; never guess email formats
- **Execute:** blocked until founder-authorized persistence gate

Machine-readable: \`CONTACT_HIERARCHY_MATRIX.json\`

## STOP

Awaiting founder review. No production Airtable writes, no Surfe runs, no publication, no competing PR while #462 is open.
`;

fs.writeFileSync(outMd, md);
console.log(JSON.stringify(summary, null, 2));
console.log("Wrote", outJson);
console.log("Wrote", outMd);
