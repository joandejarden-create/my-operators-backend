/**
 * Bethesda free/public-source contact recovery (Phase 2).
 *
 * Authorization: $0 paid provider spend. Automated free HTTP only.
 * Does NOT write publication packages, Airtable, or CURRENT pointers.
 * Does NOT invent source URLs — only records URLs where email+name evidence
 * was observed on the fetched page.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { fetchPublicHtmlSafe } from "../lib/group-demand-intelligence/presentation/gdi-safe-outbound-fetch-v1.js";
import {
  classifyContactActionability,
  classifyEmailChannel,
} from "../lib/group-demand-intelligence/presentation/gdi-contact-actionability-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports",
  "gdi-contact-actionability-implementation-v1",
);
const BETHESDA = "recLuxvwwxID7U2B8";
const MAX_URLS_PER_CONTACT = 5;
const MAX_CONTACTS = 34;
const PAID_SPEND_USD = 0;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function writeJson(file, data) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function emailOf(c = {}) {
  const e = text(c.email || c.workEmail).toLowerCase();
  return e.includes("@") ? e : null;
}

function domainOf(email) {
  if (!email) return null;
  return email.split("@")[1] || null;
}

function candidateUrls(contact, opp) {
  const email = emailOf(contact);
  const domain = domainOf(email);
  const urls = [];
  const add = (u) => {
    const s = text(u);
    if (!/^https?:\/\//i.test(s)) return;
    if (!urls.includes(s)) urls.push(s);
  };
  // Existing opportunity URLs only (never invent paths beyond domain roots/common contact)
  const walk = (node, depth = 0) => {
    if (depth > 5 || node == null) return;
    if (typeof node === "string") {
      add(node);
      return;
    }
    if (Array.isArray(node)) {
      for (const x of node) walk(x, depth + 1);
      return;
    }
    if (typeof node === "object") {
      for (const [k, v] of Object.entries(node)) {
        if (/url|href|source|link/i.test(k)) add(v);
        else if (typeof v === "object") walk(v, depth + 1);
      }
    }
  };
  walk(opp.sources);
  walk(opp.evidence);
  walk(opp.supportingEvidenceDrawer);
  add(opp.eventDateEvidenceUrl);
  add(opp.officialSource);
  add(contact.sourceUrl);
  add(contact.channelSourceUrl);

  if (domain && !/gmail|yahoo|hotmail|outlook|icloud|aol\./i.test(domain)) {
    add(`https://${domain}/`);
    add(`https://www.${domain}/`);
    add(`https://${domain}/contact`);
    add(`https://www.${domain}/contact`);
    add(`https://${domain}/about`);
  }
  return urls.slice(0, MAX_URLS_PER_CONTACT);
}

function htmlHasEmailAndName(html, email, name) {
  const h = String(html || "").toLowerCase();
  if (!h || !email) return { emailHit: false, nameHit: false };
  const emailHit = h.includes(email.toLowerCase());
  const parts = text(name)
    .toLowerCase()
    .split(/\s+/)
    .filter((p) => p.length >= 3);
  const nameHit =
    parts.length >= 2
      ? parts.every((p) => h.includes(p))
      : parts.length === 1
        ? h.includes(parts[0])
        : false;
  return { emailHit, nameHit, both: emailHit && nameHit };
}

function priorityScore(opp, contact) {
  let s = 0;
  const pri = text(opp.priority || opp.priorityFilterKey).toUpperCase();
  if (/HIGH/.test(pri)) s += 100;
  else if (/MEDIUM/.test(pri)) s += 50;
  if (contact.phone || contact.directPhone) s += 40;
  const title = text(contact.title || contact.role);
  if (/\b(housing|hotel|meeting|conference|procurement|travel|planner)\b/i.test(title)) {
    s += 25;
  }
  if (classifyEmailChannel(contact).status === "PERSON_SPECIFIC_WITHOUT_SOURCE_URL") {
    s += 10;
  }
  return s;
}

async function recoverOne(row) {
  const started = Date.now();
  const { opp, contact, slot } = row;
  const email = emailOf(contact);
  const name = text(contact.name);
  const before = classifyContactActionability(opp);
  const urls = candidateUrls(contact, opp);
  const attempts = [];
  let foundUrl = null;
  let stopReason = null;

  if (!email || !name) {
    return {
      opportunityId: text(opp.opportunityId || opp.id),
      title: text(opp.title || opp.name),
      personName: name || null,
      slot,
      outcome: "SKIP_INSUFFICIENT_IDENTITY",
      stopReason: "missing_email_or_name",
      paidSpendUsd: 0,
      elapsedMs: Date.now() - started,
      beforeTier: before.contactActionabilityTier,
      afterTier: before.contactActionabilityTier,
      upgraded: false,
    };
  }

  for (const url of urls) {
    if (attempts.length >= MAX_URLS_PER_CONTACT) {
      stopReason = "max_url_reverify_fetches";
      break;
    }
    let fetchResult;
    try {
      fetchResult = await fetchPublicHtmlSafe(url, {
        timeoutMs: 8000,
        maxBytes: 350_000,
      });
    } catch (err) {
      attempts.push({
        url,
        ok: false,
        reason: text(err?.message || err).slice(0, 160),
      });
      continue;
    }
    const html = fetchResult?.html || fetchResult?.body || "";
    const ok = fetchResult?.ok === true || (html && html.length > 50);
    const hits = htmlHasEmailAndName(html, email, name);
    attempts.push({
      url,
      ok: Boolean(ok),
      httpStatus: fetchResult?.status || fetchResult?.httpStatus || null,
      emailHit: hits.emailHit,
      nameHit: hits.nameHit,
      both: hits.both,
    });
    if (hits.both) {
      foundUrl = url;
      break;
    }
  }

  if (!foundUrl && !stopReason) {
    stopReason =
      attempts.length === 0
        ? "no_candidate_urls"
        : attempts.every((a) => !a.ok)
          ? "all_fetches_failed"
          : "email_or_name_not_co_located_on_fetched_pages";
  }

  // Simulate post-recovery classification WITHOUT writing — only if evidence found
  let afterTier = before.contactActionabilityTier;
  let upgraded = false;
  if (foundUrl) {
    const simulated = {
      ...opp,
      primaryContact:
        slot === "secondary"
          ? opp.primaryContact
          : {
              ...contact,
              sourceUrl: foundUrl,
              channelSourceUrl: foundUrl,
              channelVerificationStatus: "PUBLICLY_VERIFIED",
              contactDataOrigin: "PUBLIC_SOURCE",
            },
      secondaryContact:
        slot === "secondary"
          ? {
              ...contact,
              sourceUrl: foundUrl,
              channelSourceUrl: foundUrl,
              channelVerificationStatus: "PUBLICLY_VERIFIED",
              contactDataOrigin: "PUBLIC_SOURCE",
            }
          : opp.secondaryContact,
    };
    const after = classifyContactActionability(simulated);
    afterTier = after.contactActionabilityTier;
    upgraded =
      afterTier === "SALES_READY" && before.contactActionabilityTier !== "SALES_READY";
  }

  return {
    opportunityId: text(opp.opportunityId || opp.id),
    title: text(opp.title || opp.name),
    personName: name,
    organization: text(contact.organization) || null,
    emailFingerprint: createHash("sha256").update(email).digest("hex").slice(0, 12),
    emailDomain: domainOf(email),
    phonePresent: Boolean(contact.phone || contact.directPhone),
    priorityScore: row.priorityScore,
    slot,
    candidateUrlCount: urls.length,
    attempts,
    evidenceSupportedSourceUrl: foundUrl,
    outcome: foundUrl ? "EVIDENCE_SUPPORTED_SOURCE_FOUND" : "NO_ATTRIBUTABLE_SOURCE",
    stopReason: foundUrl ? null : stopReason,
    paidSpendUsd: 0,
    elapsedMs: Date.now() - started,
    beforeTier: before.contactActionabilityTier,
    afterTierIfApplied: afterTier,
    upgradedIfApplied: upgraded,
    publicationWritten: false,
    note: foundUrl
      ? "Source URL observed with name+email co-location. NOT applied to CURRENT (await founder)."
      : "No attributable public page found within free fetch budget.",
  };
}

async function main() {
  const store = createGdiPublicationPackageStore();
  const loaded = store.loadCustomerVisiblePackage(BETHESDA);
  if (!loaded.ok) {
    throw new Error(`Bethesda CURRENT load failed: ${loaded.error}`);
  }

  const cohort = [];
  for (const opp of loaded.package.opportunities || []) {
    for (const slot of ["primary", "secondary"]) {
      const contact =
        slot === "secondary" ? opp.secondaryContact : opp.primaryContact;
      if (!contact) continue;
      const ch = classifyEmailChannel(contact);
      if (ch.status !== "PERSON_SPECIFIC_WITHOUT_SOURCE_URL") continue;
      cohort.push({
        opp,
        contact,
        slot,
        priorityScore: priorityScore(opp, contact),
      });
    }
  }

  cohort.sort((a, b) => b.priorityScore - a.priorityScore);
  const selected = cohort.slice(0, MAX_CONTACTS);

  const results = [];
  for (const row of selected) {
    // Sequential to avoid stampedes; free HTTP only
    // eslint-disable-next-line no-await-in-loop
    results.push(await recoverOne(row));
  }

  const summary = {
    schema: "gdi-bethesda-free-contact-recovery-v1",
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA,
    hotelName: "Bethesda Marriott",
    paidSpendUsd: PAID_SPEND_USD,
    cohortSize: selected.length,
    evidenceSupportedSourceFound: results.filter(
      (r) => r.outcome === "EVIDENCE_SUPPORTED_SOURCE_FOUND",
    ).length,
    noAttributableSource: results.filter(
      (r) => r.outcome === "NO_ATTRIBUTABLE_SOURCE",
    ).length,
    upgradesToSalesReadyIfApplied: results.filter((r) => r.upgradedIfApplied)
      .length,
    publicationWrites: 0,
    totalElapsedMs: results.reduce((n, r) => n + (r.elapsedMs || 0), 0),
    stop: true,
  };

  fs.mkdirSync(OUT, { recursive: true });
  writeJson(path.join(OUT, "BETHESDA_FREE_RECOVERY_RESULTS.json"), {
    summary,
    results,
  });
  writeJson(path.join(OUT, "BETHESDA_FREE_RECOVERY_SUMMARY.json"), summary);

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
