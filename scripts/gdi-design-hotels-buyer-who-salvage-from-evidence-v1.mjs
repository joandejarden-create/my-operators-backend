#!/usr/bin/env node
/**
 * $0 salvage — re-qualify named buyers from already-paid Exa evidence.
 * No new provider calls. Writes RECOVERED_OPPORTUNITIES.json for restage.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-buyer-who-live-recovery-v1");

const {
  isPlausibleBuyerPersonName,
  employerAffiliationPlausible,
  buyerRoleTitlePlausible,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js"
);
const {
  selectBestSuggestedBuyer,
  applyVerifiedBuyerToOpportunity,
  evaluateNamedBuyerFinalizationGate,
} = await import(
  "../lib/group-demand-intelligence/workflow/gdi-buyer-who-permanent-full-e2e-v1.js"
);
const { personIdentityKey } = await import(
  "../lib/group-demand-intelligence/workflow/contact-field-provenance-v1.js"
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function evidenceUrlForPerson(opp, person) {
  if (person?.sourceUrl && /^https?:\/\//i.test(person.sourceUrl)) return person.sourceUrl;
  const name = text(person?.name).toLowerCase();
  const ev = opp?.evidence || [];
  for (const e of ev) {
    const blob = text(e.statement || e.quote || "").toLowerCase();
    if (name && blob.includes(name.split(/\s+/)[0]) && e.url) return e.url;
  }
  return ev.find((e) => e.url)?.url || null;
}

function main() {
  const reportPath = path.join(OUT, "LIVE_RECOVERY_REPORT.json");
  const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  const recovered = [];
  const rejected = [];

  for (const hotel of report.hotelDetails || []) {
    for (const row of hotel.rows || []) {
      const opp = row.opportunity;
      if (!opp) continue;
      const people = (opp.namedStakeholders || []).map((p) => ({
        ...p,
        sourceUrl: evidenceUrlForPerson(opp, p) || p.sourceUrl || null,
        currentOrganization:
          p.currentOrganization ||
          (/at\s+([^,.]+)/i.exec(text(p.title || p.role)) || [])[1] ||
          null,
        relevanceEvidence: text(p.title || p.role || p.relevanceEvidence || ""),
      }));

      const sel = selectBestSuggestedBuyer(people, {
        looksLikePersonName: isPlausibleBuyerPersonName,
        employerAffiliationPlausible,
        organizationName: row.organizationName || opp.organizationName,
        requireSourceUrl: true,
      });

      if (
        !sel.selected ||
        !buyerRoleTitlePlausible(sel.selected.title || sel.selected.role) ||
        !employerAffiliationPlausible(
          sel.selected,
          row.organizationName || opp.organizationName,
        )
      ) {
        if (people.some((p) => isPlausibleBuyerPersonName(p.name))) {
          rejected.push({
            hotelId: hotel.hotelId,
            opportunityId: row.opportunityId,
            organizationName: row.organizationName,
            candidates: people
              .filter((p) => isPlausibleBuyerPersonName(p.name))
              .map((p) => ({
                name: p.name,
                title: p.title || p.role,
                sourceUrl: p.sourceUrl,
                employerOk: employerAffiliationPlausible(
                  p,
                  row.organizationName || opp.organizationName,
                ),
                roleOk: buyerRoleTitlePlausible(p.title || p.role),
              })),
            reason: "FAILED_STRICT_SALVAGE_GATE",
          });
        }
        continue;
      }

      const pik = personIdentityKey({
        name: sel.selected.name,
        organization: row.organizationName || opp.organizationName,
      });
      const enriched = applyVerifiedBuyerToOpportunity(
        {
          ...opp,
          hotelId: hotel.hotelId,
          hotelName: hotel.hotelName,
          id: row.opportunityId,
          opportunityId: row.opportunityId,
          organizationName: row.organizationName || opp.organizationName,
        },
        {
          ...sel.selected,
          title: text(sel.selected.title || sel.selected.role).slice(0, 120),
          role: text(sel.selected.title || sel.selected.role).slice(0, 120),
        },
        { personIdentityKey: pik },
      );
      const gate = evaluateNamedBuyerFinalizationGate(enriched, {
        requireNamedPerson: true,
        looksLikePersonName: isPlausibleBuyerPersonName,
      });
      if (!gate.pass) {
        rejected.push({
          hotelId: hotel.hotelId,
          opportunityId: row.opportunityId,
          reason: "FINALIZATION_GATE_FAIL",
          named: sel.selected.name,
        });
        continue;
      }
      recovered.push({
        hotelId: hotel.hotelId,
        opportunityId: row.opportunityId,
        named: gate.name,
        role: enriched.primaryContact?.role,
        organizationName: row.organizationName || opp.organizationName,
        personIdentityKey: pik,
        sourceUrl: enriched.primaryContact?.sourceUrl,
        status: "VERIFIED_NAMED_BUYER_SALVAGED",
        paidUsd: 0,
        score: sel.score,
        opportunity: enriched,
      });
    }
  }

  fs.writeFileSync(
    path.join(OUT, "SALVAGE_FROM_EVIDENCE.json"),
    JSON.stringify(
      {
        at: new Date().toISOString(),
        paidCalls: 0,
        recoveredCount: recovered.length,
        rejectedCount: rejected.length,
        recovered: recovered.map((r) => ({
          hotelId: r.hotelId,
          opportunityId: r.opportunityId,
          named: r.named,
          role: r.role,
          organizationName: r.organizationName,
          sourceUrl: r.sourceUrl,
          score: r.score,
        })),
        rejected,
      },
      null,
      2,
    ),
  );
  fs.writeFileSync(
    path.join(OUT, "RECOVERED_OPPORTUNITIES.json"),
    JSON.stringify(
      {
        at: new Date().toISOString(),
        count: recovered.length,
        source: "salvage_from_paid_evidence_v1",
        opportunities: recovered.map((r) => r.opportunity),
      },
      null,
      2,
    ),
  );
  console.log(
    `SALVAGED ${recovered.length} named buyers ($0). Rejected ${rejected.length}.`,
  );
  for (const r of recovered) {
    console.log(`  ${r.organizationName}: ${r.named} — ${r.role}`);
  }
}

main();
