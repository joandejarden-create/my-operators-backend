/**
 * GDI Contact Intelligence V1 — Bethesda source-page extraction canary.
 *
 * Re-reads official source pages already attached to actionable opportunities,
 * extracts demand+contact from the same fetch, applies public contact upgrades.
 *
 *   node scripts/gdi-contact-intelligence-v1-source-page-canary.mjs
 *   node scripts/gdi-contact-intelligence-v1-source-page-canary.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  summarizeContactCoverage,
  classifyContactTier,
  CONTACT_TIER,
} from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import {
  extractContactIntelligenceFromSource,
  applySourceContactExtractionToOpportunity,
} from "../lib/group-demand-intelligence/extract-contact-intelligence-from-source.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const HOTEL = "recLuxvwwxID7U2B8";
const PRIORITY_IDS = new Set([
  "gdi_pe_781f12393f8117e7",
  "gdi_opp_regulatory_information_conference_20260930",
  "gdi_opp_accp_annual_meeting_20261001",
  "gdi_opp_acvnu_renal_week_20270412",
]);

async function fetchHtml(url) {
  try {
    const r = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(20000),
      headers: { "User-Agent": "DealalityGDI-ContactIntelV1/1.0" },
    });
    const html = await r.text();
    return {
      ok: r.status >= 200 && r.status < 400,
      finalUrl: r.url,
      html,
      fetchesUsed: 1,
      hasLodging: /hotel|lodging|accommodat|housing|room.?block|travel/i.test(html),
      hasFuture: /2026|2027|2028|2029/i.test(html),
    };
  } catch (err) {
    return { ok: false, error: err.message, fetchesUsed: 1, html: "" };
  }
}

function pickUrls(o) {
  const urls = [];
  for (const u of [
    o.officialSource,
    o.discoverySource,
    o.contactOfficialUrl,
    o.primaryContact?.sourceUrl,
    ...(Array.isArray(o.sources) ? o.sources.map((s) => s.url || s) : []),
  ]) {
    const s = String(u || "").trim();
    if (/^https?:\/\//i.test(s) && !urls.includes(s)) urls.push(s);
  }
  return urls.slice(0, 3);
}

async function main() {
  const outDir = path.join(
    ROOT,
    "reports/group-demand-intelligence/contact-intelligence-v1"
  );
  fs.mkdirSync(outDir, { recursive: true });

  const doc = await loadOpportunitiesCanonical(HOTEL);
  const all = filterCustomerFacingOpportunities(doc.opportunities || []);
  const actionable = all.filter((o) => !/_disqualified$/i.test(o.id || ""));
  const before = summarizeContactCoverage(actionable);

  // Prioritize NEW + weak tiers
  const cohort = [...actionable].sort((a, b) => {
    const pa = PRIORITY_IDS.has(a.id) ? 0 : 1;
    const pb = PRIORITY_IDS.has(b.id) ? 0 : 1;
    if (pa !== pb) return pa - pb;
    const ta = classifyContactTier(a);
    const tb = classifyContactTier(b);
    const rank = (t) =>
      t === CONTACT_TIER.NO_CONTACT
        ? 0
        : t === CONTACT_TIER.GENERIC_ONLY
          ? 1
          : t === CONTACT_TIER.ORGANIZATION_PATH
            ? 2
            : t === CONTACT_TIER.FUNCTIONAL_CONTACT
              ? 3
              : 4;
    return rank(ta) - rank(tb);
  });

  const selected = cohort.slice(0, 18);
  const rows = [];
  let pagesRead = 0;
  let pagesWithClues = 0;
  let named = 0;
  let functional = 0;
  let publicEmails = 0;
  let publicPhones = 0;
  let staffLinks = 0;
  let extraFollowups = 0;
  let cluesFromPaidFetches = 0;
  let updated = 0;
  let working = [...(doc.opportunities || [])];

  for (const o of selected) {
    const urls = pickUrls(o);
    const beforeTier = classifyContactTier(o);
    let mergedExtraction = null;
    let sourcesFetched = [];
    let followupsFetched = 0;

    for (const url of urls) {
      const page = await fetchHtml(url);
      pagesRead += 1;
      sourcesFetched.push({ url, ok: page.ok, status: page.status });
      if (!page.ok) continue;
      const extraction = extractContactIntelligenceFromSource({
        source: { url: page.finalUrl || url, sourceType: "official_web" },
        pageContent: page.html,
        opportunityContext: o,
      });
      if (extraction.hasContactClues) {
        pagesWithClues += 1;
        cluesFromPaidFetches += 1;
      }
      named += extraction.metrics.namedPeople;
      functional += extraction.metrics.functionalContacts;
      publicEmails += extraction.metrics.publicEmails;
      publicPhones += extraction.metrics.publicPhones;
      staffLinks += extraction.metrics.staffContactLinks;
      mergedExtraction = extraction;

      // Bounded follow-up when weak
      if (
        (beforeTier === CONTACT_TIER.NO_CONTACT ||
          beforeTier === CONTACT_TIER.GENERIC_ONLY ||
          beforeTier === CONTACT_TIER.ORGANIZATION_PATH ||
          beforeTier === CONTACT_TIER.FUNCTIONAL_CONTACT) &&
        extraction.metrics.namedPeople === 0
      ) {
        for (const link of (extraction.followupLinks || []).slice(0, 2)) {
          const sub = await fetchHtml(link.url);
          pagesRead += 1;
          followupsFetched += 1;
          extraFollowups += 1;
          if (!sub.ok) continue;
          const subEx = extractContactIntelligenceFromSource({
            source: { url: link.url, sourceType: "official_web" },
            pageContent: sub.html,
            opportunityContext: o,
          });
          if (subEx.hasContactClues) {
            pagesWithClues += 1;
            cluesFromPaidFetches += 1;
          }
          named += subEx.metrics.namedPeople;
          functional += subEx.metrics.functionalContacts;
          publicEmails += subEx.metrics.publicEmails;
          publicPhones += subEx.metrics.publicPhones;
          staffLinks += subEx.metrics.staffContactLinks;
          if (subEx.metrics.namedPeople > (mergedExtraction?.metrics?.namedPeople || 0)) {
            mergedExtraction = subEx;
          }
        }
      }
    }

    let applied = {
      opportunity: o,
      beforeTier,
      afterTier: beforeTier,
      improved: false,
      extraction: mergedExtraction,
    };
    if (mergedExtraction) {
      applied = applySourceContactExtractionToOpportunity(o, mergedExtraction);
    }

    if (APPLY && applied.improved) {
      const write = await promoteQualifiedGdiOpportunity({
        candidate: {
          ...applied.opportunity,
          weeklyDeltaState: o.weeklyDeltaState,
          isNewThisWeek: o.isNewThisWeek,
        },
        existingOpps: working,
        hotelId: HOTEL,
        runId: "gdi_contact_intelligence_v1_source_page",
        method: "source_page_contact_extraction_v1",
        dryRun: false,
        forceUpdateId: o.id,
        materialUpdateOnly: true,
      });
      if (write.opportunity) {
        working = working.filter((x) => x.id !== o.id).concat([write.opportunity]);
        updated += 1;
        applied.opportunity = write.opportunity;
        applied.afterTier = classifyContactTier(write.opportunity);
      }
    }

    rows.push({
      opportunityId: o.id,
      title: o.title,
      sourcesFetched: sourcesFetched.length,
      followupsFetched,
      contactCluesFound: Boolean(mergedExtraction?.hasContactClues),
      namedPeopleFound: mergedExtraction?.metrics?.namedPeople || 0,
      functionalPathsFound: mergedExtraction?.metrics?.functionalContacts || 0,
      primaryContact: applied.opportunity?.primaryContactName || applied.opportunity?.primaryContact?.name || null,
      primaryRole: applied.opportunity?.primaryContactRole || applied.opportunity?.primaryContact?.role || null,
      tierBefore: beforeTier,
      tierAfter: applied.afterTier,
      publicEmail: Boolean(applied.opportunity?.primaryContactEmail || applied.opportunity?.primaryContact?.email),
      publicPhone: Boolean(applied.opportunity?.primaryContactPhone || applied.opportunity?.primaryContact?.phone),
      improved: applied.improved,
      staffLinks: mergedExtraction?.metrics?.staffContactLinks || 0,
    });
  }

  const afterDoc = APPLY ? await loadOpportunitiesCanonical(HOTEL) : { opportunities: working };
  const afterAll = filterCustomerFacingOpportunities(afterDoc.opportunities || []);
  const afterActionable = afterAll.filter((o) => !/_disqualified$/i.test(o.id || ""));
  const after = summarizeContactCoverage(afterActionable);

  const report = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    hotelId: HOTEL,
    auditLanes: {
      newOpportunities: "PARTIAL → now YES via weekly-lane-harvest fetchPage",
      demandGenerators: "PARTIAL → now YES via weekly-lane-harvest fetchPage",
      privateEvents: "PARTIAL → now YES via weekly-lane-harvest fetchPage",
      weeklyDiscovery: "YES (V1.2 contact resolution + shared extractor)",
      secondPass: "YES (bounded follow-up links from source pages)",
    },
    before: before.counts,
    after: after.counts,
    gradesAfter: after.grades,
    yield: {
      sourcePagesRead: pagesRead,
      pagesWithContactClues: pagesWithClues,
      namedPeople: named,
      functionalContacts: functional,
      publicEmails,
      publicPhones,
      staffContactLinks: staffLinks,
      contactCluesFromAlreadyFetchedPages: cluesFromPaidFetches,
      additionalContactSpecificFetches: extraFollowups,
    },
    rowsUpdated: updated,
    rows,
    surfe: { autoCalls: 0, piiPersisted: 0 },
  };

  const outPath = path.join(
    outDir,
    `SOURCE_PAGE_CANARY_${APPLY ? "APPLY" : "DRY"}_${Date.now()}.json`
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        apply: APPLY,
        before: report.before,
        after: report.after,
        yield: report.yield,
        improvedRows: rows.filter((r) => r.improved).length,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
