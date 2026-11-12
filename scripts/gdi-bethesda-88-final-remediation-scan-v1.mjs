#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadHotelOpportunityDoc,
  BETHESDA_HOTEL_ID,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  extractContactSnapshot,
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
  mergeContactAntiClobber,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-quality-completion-v1.js";
import { proposeOpportunityRemediation } from "../lib/group-demand-intelligence/gdi-opportunity-full-e2e-quality-orchestrator-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "../lib/group-demand-intelligence/research/bethesda-88-commercial-event-family-v1.js";
import { evaluateSharedCommercialEligibility } from "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js";
import { evaluateAccountDevelopmentEligibility } from "../lib/group-demand-intelligence/account-development-eligibility-v1.js";
import { evaluateOpportunityQualityContract } from "../lib/group-demand-intelligence/gdi-opportunity-quality-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-88-final-full-e2e-remediation-v1"
);
fs.mkdirSync(OUT, { recursive: true });

const store = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID);
const union = mergeBethesdaInternalResearchOpportunities(
  BETHESDA_HOTEL_ID,
  store.opportunities
);
const fam = applyBethesdaCommercialEventFamilyPresentation(
  union.opportunities,
  { hideLinkedFromList: true }
);
const roleMap = new Map(
  (fam.allAnnotated || []).map((r) => [r.id, r.commercialEventFamilyRole])
);

const focusIds = [
  "gdi_opp_aao_2027_annual_session_9",
  "gdi_opp_aao_2028_annual_meeting_10",
  "gdi_opp_48th_iscb_annual_conference_2027_12",
  "gdi_opp_2027_nci_rna_biology_symposium_7",
  "gdi_opp_acvnu_renal_week_20270412",
  "gdi_opp_acts_ts27",
  "gdi_opp_loudoun_college_showcase_2027",
  "gdi_opp_nado_ddaa_washcon_2028",
];

const focus = [];
for (const id of focusIds) {
  const o = union.opportunities.find((x) => x.id === id);
  if (!o) {
    focus.push({ id, missing: true });
    continue;
  }
  const s = extractContactSnapshot(o);
  focus.push({
    id,
    title: o.title,
    city: o.city || o.eventCity || o.venueCity || null,
    venue: o.venue || o.venueName || o.primaryVenue || null,
    location: o.location || o.eventLocation || null,
    hotelFitHonesty: o.hotelFitHonesty || null,
    commercialMaturity: o.commercialMaturity || o.maturity || null,
    priority: o.priority || null,
    status: o.status || null,
    named: s.named,
    name: s.name,
    coverage: s.coverage,
    what: String(
      o.whatIsHappening || o.hotelOpportunityThesis || o.description || ""
    ).slice(0, 220),
  });
}

const contactUpgrades = [];
const repairable = [];
const srIds = [];
const adIds = [];
const qualityById = [];

for (const o of union.opportunities) {
  const id = o.id;
  const snap = extractContactSnapshot(o);
  const role = roleMap.get(id) || "STANDALONE";
  const ev = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id];
  const rem = proposeOpportunityRemediation(o, { familyRole: role, evidence: ev });
  const qc = evaluateOpportunityQualityContract(o, { familyRole: role });
  const srE = evaluateSharedCommercialEligibility(o);
  const adE = evaluateAccountDevelopmentEligibility(o);
  const srPass =
    srE?.eligible === true ||
    srE?.ok === true ||
    srE?.result?.eligible === true ||
    srE?.salesReadyEligible === true ||
    String(srE?.outcome || srE?.status || "").includes("PASS") ||
    String(srE?.certification || "").includes("SALES_READY");
  const adPass =
    adE?.eligible === true ||
    adE?.ok === true ||
    adE?.result?.eligible === true ||
    adE?.accountDevelopmentEligible === true ||
    String(adE?.outcome || adE?.status || "").includes("PASS");
  if (srPass) srIds.push(id);
  if (adPass) adIds.push(id);

  if (
    ev &&
    !ev.rejected &&
    ev.founderValidation !== "FAIL" &&
    ev.primaryContact?.name
  ) {
    const after = extractContactSnapshot(mergeContactAntiClobber(o, ev));
    if (!snap.named && after.named) {
      contactUpgrades.push({
        id,
        name: after.name,
        coverage: after.coverage,
        email: after.email,
        org: after.orgEmail || after.orgRoute,
        inventory: ev.inventory,
        hotelFit: ev.hotelFitHonesty || null,
      });
    }
  }

  const issues = [];
  const displayName = o.primaryContactName || o.primaryContact?.name || "";
  if (displayName && !snap.named) issues.push("PLACEHOLDER_PRIMARY_DISPLAY");
  for (const c of rem.changes || []) issues.push(`REM:${c.field}`);
  const blockers = qc?.blockers || qc?.commercialBlockers || [];
  for (const b of blockers) {
    const code = String(b.code || b || "");
    if (
      /TITLE|CONTACT|CATEGORY|PLACEHOLDER|ORG_ROUTE|EMAIL|HOTEL_FIT|DESCRIPTION/i.test(
        code
      ) &&
      !/PUBLICATION|PAID/i.test(code)
    ) {
      issues.push(`QC:${code}`);
    }
  }
  if (
    issues.length &&
    role !== "LINKED_RESEARCH" &&
    role !== "QUARANTINED"
  ) {
    repairable.push({
      id,
      title: String(o.title || "").slice(0, 80),
      role,
      issues: [...new Set(issues)],
      coverage: snap.coverage,
      changes: (rem.changes || []).map((c) => c.field),
    });
  }

  qualityById.push({
    id,
    role,
    researchIntegrity: qc?.researchIntegrity || qc?.integrity || null,
    commercialCertification:
      qc?.commercialCertification || qc?.commercial || null,
    named: snap.named,
    coverage: snap.coverage,
  });
}

const report = {
  generatedAt: new Date().toISOString(),
  storeSha: store.sha256,
  union: union.opportunities.length,
  uniqueCommercial: fam.counts.uniqueCommercialOpportunities,
  focus,
  contactUpgrades,
  repairableCount: repairable.length,
  repairable,
  srEngine: { count: srIds.length, ids: srIds },
  adEngine: { count: adIds.length },
  qualitySummary: qualityById.reduce((acc, row) => {
    const k = row.commercialCertification || "UNKNOWN";
    acc[k] = (acc[k] || 0) + 1;
    return acc;
  }, {}),
};

fs.writeFileSync(
  path.join(OUT, "01-repairable-defect-scan.json"),
  JSON.stringify(report, null, 2)
);
console.log(
  JSON.stringify(
    {
      ok: true,
      contactUpgrades: contactUpgrades.length,
      upgrades: contactUpgrades,
      repairableCount: repairable.length,
      remFields: [...new Set(repairable.flatMap((r) => r.changes))],
      placeholderDisplay: repairable.filter((r) =>
        r.issues.includes("PLACEHOLDER_PRIMARY_DISPLAY")
      ).length,
      sr: srIds.length,
      ad: adIds.length,
      focus: focus.map((f) => ({
        id: f.id,
        venue: f.venue,
        city: f.city,
        hotelFitHonesty: f.hotelFitHonesty,
        title: f.title,
      })),
    },
    null,
    2
  )
);
