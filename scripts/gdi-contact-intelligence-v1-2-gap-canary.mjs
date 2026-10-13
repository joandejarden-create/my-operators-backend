/**
 * GDI Contact Intelligence V1.2 — remaining gap canary (Bethesda).
 *
 * Audits the weekly-era NO_CONTACT set (including disqualified skips),
 * then runs bounded recovery on actionable weak contacts only.
 *
 *   node scripts/gdi-contact-intelligence-v1-2-gap-canary.mjs
 *   node scripts/gdi-contact-intelligence-v1-2-gap-canary.mjs --apply
 *   node scripts/gdi-contact-intelligence-v1-2-gap-canary.mjs --network-domain
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
  buildContactGapAuditRow,
  classifyContactGapReason,
  shouldSkipDeepContactResearch,
  resolveContactGapsBatch,
  CONTACT_GAP_REASON,
} from "../lib/group-demand-intelligence/contact-intelligence-v1-2.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const NETWORK_DOMAIN = process.argv.includes("--network-domain");
const HOTEL = "recLuxvwwxID7U2B8";

/** Weekly V1.2 snapshot blanks (historical 10 NO_CONTACT + related) — audit only. */
const WEEKLY_ERA_BLANK_IDS = [
  "gdi_opp_aan_2027_disqualified",
  "gdi_opp_ntca_2027_disqualified",
  "gdi_opp_ptab_2027_disqualified",
  "gdi_opp_agb_2027_disqualified",
  "gdi_opp_ada_2027_disqualified",
  "gdi_opp_asa_advance_2027_disqualified",
  "gdi_opp_fba_qui_tam_2027_disqualified",
  "gdi_opp_marriott_hq_adjacent_corporate_watch",
  "gdi_opp_umd_alumni_weekend_watch",
  "gdi_pe_781f12393f8117e7",
];

function isActionable(o) {
  if (!o) return false;
  if (/_disqualified$/i.test(o.id || "")) return false;
  if (String(o.priority || "").toUpperCase() === "DISQUALIFIED") return false;
  if (o.opportunityType === "CLOSED_DISQUALIFIED") return false;
  return true;
}

function tierRank(t) {
  return {
    [CONTACT_TIER.NO_CONTACT]: 0,
    [CONTACT_TIER.GENERIC_ONLY]: 1,
    [CONTACT_TIER.ORGANIZATION_PATH]: 2,
    [CONTACT_TIER.FUNCTIONAL_CONTACT]: 3,
    [CONTACT_TIER.NAMED_PARTIAL]: 4,
    [CONTACT_TIER.NAMED_DIRECT]: 5,
  }[t] ?? 0;
}

async function main() {
  const outDir = path.join(
    ROOT,
    "reports/group-demand-intelligence/contact-intelligence-v1-2"
  );
  fs.mkdirSync(outDir, { recursive: true });

  const doc = await loadOpportunitiesCanonical(HOTEL);
  const all = doc.opportunities || [];
  const cf = filterCustomerFacingOpportunities(all);
  const actionable = cf.filter(isActionable);
  const before = summarizeContactCoverage(actionable);

  // Part A — classify weekly-era blanks (truthful vs live)
  const byId = Object.fromEntries(all.map((o) => [o.id, o]));
  const weeklyEraAudit = WEEKLY_ERA_BLANK_IDS.map((id) => {
    const o = byId[id];
    if (!o) {
      return { opportunityId: id, missing: true };
    }
    const skip = shouldSkipDeepContactResearch(o);
    return {
      ...buildContactGapAuditRow(o),
      liveTier: classifyContactTier(o),
      skipDeepResearch: skip.skip,
      softStop: skip.softStop,
      gapReason: classifyContactGapReason(o),
    };
  });

  // Live actionable weak cohort (NO_CONTACT / GENERIC / ORG_PATH)
  const weak = actionable
    .filter((o) => {
      const t = classifyContactTier(o);
      return (
        t === CONTACT_TIER.NO_CONTACT ||
        t === CONTACT_TIER.GENERIC_ONLY ||
        t === CONTACT_TIER.ORGANIZATION_PATH
      );
    })
    .sort((a, b) => tierRank(classifyContactTier(a)) - tierRank(classifyContactTier(b)));

  const batch = await resolveContactGapsBatch(weak, {
    allowNetworkDomainResolution: NETWORK_DOMAIN,
    jevShadow: true,
  });

  let working = [...all];
  let publicUpdates = 0;
  let publicEmails = 0;
  let publicPhones = 0;
  let unresolvedPersisted = 0;

    if (APPLY) {
    for (const row of batch.rows) {
      const next = row.opportunity;
      if (!next?.id) continue;
      // Ensure top-level contact fields sync from primaryContact (Airtable field map).
      const candidate = {
        ...next,
        weeklyDeltaState: next.weeklyDeltaState,
        isNewThisWeek: next.isNewThisWeek,
        primaryContactName:
          next.primaryContactName || next.primaryContact?.name || null,
        primaryContactRole:
          next.primaryContactRole || next.primaryContact?.role || null,
        primaryContactEmail:
          next.primaryContactEmail || next.primaryContact?.email || null,
        primaryContactPhone:
          next.primaryContactPhone || next.primaryContact?.phone || null,
      };
      const write = await promoteQualifiedGdiOpportunity({
        candidate,
        existingOpps: working,
        hotelId: HOTEL,
        runId: "gdi_contact_intelligence_v1_2_gap_canary",
        method: "contact_intelligence_v1_2_gap_closure",
        dryRun: false,
        forceUpdateId: next.id,
        materialUpdateOnly: true,
      });
      const persisted = write.opportunity || candidate;
      const idx = working.findIndex((o) => o.id === persisted.id);
      if (idx >= 0) working[idx] = persisted;
      else working.push(persisted);
      publicUpdates += 1;
      if (persisted.primaryContactEmail || persisted.primaryContact?.email) publicEmails += 1;
      if (persisted.primaryContactPhone || persisted.primaryContact?.phone) publicPhones += 1;
      if (persisted.unresolvedContactReason) unresolvedPersisted += 1;
      row.opportunity = persisted;
      row.afterTier = classifyContactTier(persisted);
      row.improved =
        row.beforeTier !== row.afterTier &&
        [
          CONTACT_TIER.NAMED_DIRECT,
          CONTACT_TIER.NAMED_PARTIAL,
          CONTACT_TIER.FUNCTIONAL_CONTACT,
          CONTACT_TIER.ORGANIZATION_PATH,
        ].includes(row.afterTier);
    }
  } else {
    // Dry-run still counts planned persistence fields
    for (const row of batch.rows) {
      if (row.opportunity?.unresolvedContactReason) unresolvedPersisted += 1;
      if (row.improved) publicUpdates += 1;
      if (row.publicEmails) publicEmails += row.publicEmails;
      if (row.publicPhones) publicPhones += row.publicPhones;
    }
  }

  const afterList = APPLY
    ? working.filter(isActionable)
    : actionable.map((o) => {
        const row = batch.rows.find((r) => r.opportunityId === o.id);
        return row?.opportunity || o;
      });
  const after = summarizeContactCoverage(afterList);

  const upgrades = {
    noContactToNamed: 0,
    noContactToFunctional: 0,
    noContactToOrgPath: 0,
    stillNoContact: 0,
    orgPathToFunctionalOrNamed: 0,
  };
  for (const row of batch.rows) {
    if (row.beforeTier === CONTACT_TIER.NO_CONTACT) {
      if (
        row.afterTier === CONTACT_TIER.NAMED_DIRECT ||
        row.afterTier === CONTACT_TIER.NAMED_PARTIAL
      ) {
        upgrades.noContactToNamed += 1;
      } else if (row.afterTier === CONTACT_TIER.FUNCTIONAL_CONTACT) {
        upgrades.noContactToFunctional += 1;
      } else if (row.afterTier === CONTACT_TIER.ORGANIZATION_PATH) {
        upgrades.noContactToOrgPath += 1;
      } else if (row.afterTier === CONTACT_TIER.NO_CONTACT) {
        upgrades.stillNoContact += 1;
      }
    }
    if (
      row.beforeTier === CONTACT_TIER.ORGANIZATION_PATH &&
      tierRank(row.afterTier) > tierRank(CONTACT_TIER.ORGANIZATION_PATH)
    ) {
      upgrades.orgPathToFunctionalOrNamed += 1;
    }
  }

  const out = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    networkDomain: NETWORK_DOMAIN,
    hotelId: HOTEL,
    weeklyEraAudit,
    weeklyEraNote:
      "Weekly run NO_CONTACT:10 included 7 disqualified records. Live actionable NO_CONTACT is the research cohort; disqualified are classified LOW_VALUE_NO_DEEP_RESEARCH and skipped.",
    before,
    after,
    weakAttempted: weak.length,
    batchSummary: batch.summary,
    upgrades,
    rows: batch.rows.map((r) => {
      const { opportunity, ...rest } = r;
      return {
        ...rest,
        finalContact: opportunity?.primaryContactName || opportunity?.primaryContact?.name || null,
        finalEmail: opportunity?.primaryContactEmail || opportunity?.primaryContact?.email || null,
        finalPhone: opportunity?.primaryContactPhone || opportunity?.primaryContact?.phone || null,
        officialSource: opportunity?.officialSource || opportunity?.contactOfficialUrl || null,
        contactResearchStatus: opportunity?.contactResearchStatus || null,
      };
    }),
    persistence: {
      publicUpdates,
      publicEmails,
      publicPhones,
      unresolvedPersisted,
      surfeAutoCalls: 0,
      rawProviderPayloads: 0,
    },
    gapReasonCounts: Object.fromEntries(
      Object.values(CONTACT_GAP_REASON).map((k) => [
        k,
        weeklyEraAudit.filter((r) => r.gapReason === k).length,
      ])
    ),
  };

  const file = path.join(
    outDir,
    `GAP_CANARY_${APPLY ? "APPLY" : "DRY"}_${Date.now()}.json`
  );
  fs.writeFileSync(file, JSON.stringify(out, null, 2));
  console.log(JSON.stringify({ ok: true, outPath: file, apply: APPLY, summary: batch.summary, before: before.counts, after: after.counts, upgrades }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
