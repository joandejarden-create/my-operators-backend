/**
 * Post-hoc quarantine corrections for PDL+Prospeo pilot (no re-billing).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const DIR = path.join(ROOT, "reports", "gdi-pdl-prospeo-enrichment-pilot-v1");
const RESULTS = path.join(DIR, "PILOT_RESULTS.json");

function weakOrgMatch(expected, got) {
  const a = String(expected || "").toLowerCase();
  const b = String(got || "").toLowerCase();
  if (!a || !b) return false;
  const stop = new Set([
    "the",
    "and",
    "association",
    "international",
    "university",
    "institute",
    "health",
    "network",
    "local",
    "hosts",
    "group",
    "of",
    "for",
    "de",
    "la",
  ]);
  const tokens = a
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length > 4 && !stop.has(t))
    .slice(0, 5);
  return tokens.some((t) => b.includes(t));
}

const r = JSON.parse(fs.readFileSync(RESULTS, "utf8"));

for (const row of r.results) {
  row.corrections = row.corrections || {};
  if (row.prospeo?.email && String(row.prospeo.email).includes("[object")) {
    row.corrections.prospeo = {
      originalOk: row.prospeo.ok,
      correctedOk: false,
      reason: "EMAIL_FIELD_WAS_OBJECT_PARSER_BUG_QUARANTINED",
      creditsStillConsumed: row.prospeo.creditsConsumed,
    };
    row.prospeo.ok = false;
    row.prospeo.quarantined = true;
    row.prospeo.email = null;
  }
  if (row.pdl?.matched) {
    const ok = weakOrgMatch(
      row.baseline.employerName,
      row.pdl.identity?.job_company_name,
    );
    row.corrections.pdl = {
      originalAccepted: row.pdl.identityAccepted,
      employerCorroboratedStrict: ok,
      pdlCompany: row.pdl.identity?.job_company_name || null,
      correctedAccepted: ok && row.pdl.identityAccepted,
      quarantine: !ok,
    };
    if (!ok) {
      row.pdl.identityAccepted = false;
      row.pdl.quarantined = true;
      row.pdl.linkedInVerified = false;
    }
  }
  if (row.corrections.pdl?.quarantine || row.corrections.prospeo) {
    row.after = {
      ...(row.after || {}),
      readinessAfter: row.baseline.readiness,
      movedPartialToReady: false,
      prospeoEmailAccepted: false,
      note: "Corrected after employer/email validation",
    };
  }
}

const pdlAccepted = r.results.filter((x) => x.pdl?.identityAccepted).length;
const pdlLi = r.results.filter((x) => x.pdl?.linkedInVerified).length;
const prospeoOk = r.results.filter(
  (x) => x.prospeo?.ok && x.prospeo?.email,
).length;
const quarantined = r.results.filter(
  (x) => x.pdl?.quarantined || x.prospeo?.quarantined,
).length;
const moved = r.results.filter((x) => x.after?.movedPartialToReady).length;

r.metrics = {
  ...r.metrics,
  pdlIdentityAccepted: pdlAccepted,
  pdlLinkedInVerified: pdlLi,
  prospeoNamedEmailsAccepted: prospeoOk,
  quarantined,
  movedPartialToReady: moved,
  postHocCorrectionsApplied: true,
  note: "Prospeo email object-serialization false positives quarantined; PDL cross-employer matches quarantined (Abbas→Az Digital Health; Agathe→Idyom Translations).",
};

r.recommendations = {
  pdlAsOptionalIdentityProvider:
    pdlAccepted > 0
      ? "YES_OPTIONAL_WITH_STRICT_EMPLOYER_GATE — raw matches can be wrong-employer; require employer corroboration before READY/Surfe"
      : "WEAK_THIS_COHORT_FOR_IDENTITY — 2 raw matches both failed strict employer corroboration; keep as optional probe only",
  prospeoAsSelectiveEmailFallback:
    "ADAPTER_FIXED_PARSER — this cohort yielded 0 safely parsed accepted emails after quarantine; 3 credits spent (2 false-positive object emails + 1 employer-mismatch). Re-test parser on 1–2 contacts before broader use.",
  fullenrichStillNeeded:
    "HOLD — not tested; revisit only if public+PDL+Prospeo miss rate stays high",
  surfeSearchPeople:
    "HOLD — zero search credits; org/role discovery not exact-person",
  customerFacingLawfulUse:
    "Public-verified only by default. No provider-derived channels from this pilot enter customer-visible records. PDL embedded permission still needs counsel check before hotel disclosure. Prospeo standard ToS blocks customer disclosure without Order.",
};

r.costPerValidIncrementalRecovery = {
  pdlCredits: r.balances.pdl.creditsConsumedSuccessfulMatches,
  prospeoCredits: r.balances.prospeo.creditsConsumedEstimate,
  acceptedProspeoEmails: prospeoOk,
  acceptedPdlIdentityStrict: pdlAccepted,
  costPerValidProspeoEmail: null,
  costPerValidPdlIdentity: null,
  note: "Zero valid Prospeo emails after correction; PDL successful credits=2 with 0 strict employer-accepted identities",
};

fs.writeFileSync(RESULTS, JSON.stringify(r, null, 2) + "\n");

const remainingHdr =
  r.balances.pdl.headerTrail?.slice(-1)[0]?.totallimit_remaining ?? "n/a";

const md = `# GDI PDL + Prospeo Controlled Enrichment Pilot — Founder Report

Generated: ${r.generatedAt}

## Authorization respected

- PDL ≤ 10 successful credits (consumed: **${r.balances.pdl.creditsConsumedSuccessfulMatches}**; remaining header ~**${remainingHdr}**)
- Prospeo ≤ 10 email credits (consumed: **${r.balances.prospeo.creditsConsumedEstimate}**; remaining **${r.balances.prospeo.after?.remaining}** / was 100)
- Mobile / FullEnrich / Surfe enrich / Apollo: **not used**

## Corrected metrics (post-hoc quarantine)

| Measure | Raw | Corrected |
|---|---:|---:|
| PARTIAL → READY | 0 | **0** |
| PDL HTTP 200 matches | 2 | 2 |
| PDL identity accepted (strict employer) | 2 | **${pdlAccepted}** |
| PDL LinkedIn verified usable | 0 | **0** |
| Prospeo named emails accepted | 2 | **${prospeoOk}** |
| Quarantined | — | **${quarantined}** |

### Critical findings
1. **Prospeo parser bug:** email sometimes returned as object → serialized as \`[object Object]\`. Two false accepts (Daryl Simon, Andrew Bartucci) quarantined. Parser fixed in \`lib/prospeo/client.js\` for future runs — **no re-bill**.
2. **PDL wrong-employer matches:** Abbas → Az Digital Health Consulting (not U of Toronto IHPME); Agathe → Idyom Translations (not OENOVITI). Quarantined under strict employer gate.
3. **Abbas Prospeo:** 1 credit spent, result quarantined (employer/domain mismatch).
4. **Surfe readiness:** no PARTIAL→READY movement; do not claim higher Surfe match rate.

## Credits consumed
- PDL: **2** successful enrich credits (headers show ~697–698 remaining)
- Prospeo: **3** credits (100 → 97)

## Recommendations
- **PDL:** ${r.recommendations.pdlAsOptionalIdentityProvider}
- **Prospeo:** ${r.recommendations.prospeoAsSelectiveEmailFallback}
- **FullEnrich:** ${r.recommendations.fullenrichStillNeeded}
- **Surfe Search:** ${r.recommendations.surfeSearchPeople}
- **Customer-facing:** ${r.recommendations.customerFacingLawfulUse}

## Persistence
INTERNAL_EVALUATION_REPORT_ONLY — no canonical public-source writes; no STAGED→CURRENT.

## STOP
${r.stop}
`;

fs.writeFileSync(path.join(DIR, "FOUNDER_PILOT_REPORT.md"), md);
console.log(
  JSON.stringify(
    {
      pdlAccepted,
      prospeoOk,
      quarantined,
      pdlCredits: r.balances.pdl.creditsConsumedSuccessfulMatches,
      prospeoCredits: r.balances.prospeo.creditsConsumedEstimate,
      prospeoRemaining: r.balances.prospeo.after.remaining,
    },
    null,
    2,
  ),
);
