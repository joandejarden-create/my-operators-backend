/**
 * Select 30–50 representative franchise agreements for terms/renewals pilot.
 * Management agreements are a SEPARATE discovery subset (never relabel franchises).
 */

export const CONTRACT_TERMS_COHORT_V1 = "contract-terms-cohort-v1";

function yearFromIso(iso) {
  if (!iso) return null;
  const y = Number(String(iso).slice(0, 4));
  return Number.isFinite(y) ? y : null;
}

function filingYearFromAccessionOrUrl(a) {
  const s = String((a.sourceAccessions || [])[0] || (a.sourceUrls || [])[0] || "");
  const m = s.match(/(\d{2})(\d{6})-/); // YYMMDD accession start often in path differently
  const m2 = s.match(/\/(\d{4})\//);
  if (m2) return Number(m2[1]);
  // accession YYYY often not present; try filing year from URL nXXXX
  const m3 = s.match(/(\d{4})-\d{2}-\d{2}/);
  return m3 ? Number(m3[1]) : null;
}

function primaryDate(a) {
  const ms = a.milestones || [];
  const d = ms[0]?.primaryDate || a.scheduledExpiration || null;
  return d;
}

function bucketExpiration(iso, asOfYear) {
  const y = yearFromIso(iso);
  if (y == null) return "UNKNOWN_EXP";
  if (y < asOfYear) return "HISTORICALLY_EXPIRED";
  if (y <= asOfYear + 3) return "UPCOMING_0_3Y";
  if (y <= asOfYear + 8) return "MID_4_8Y";
  return "LONG_DATED";
}

/**
 * @param {object[]} verifiedAgreements — VERIFIED franchise rows from migration audit/reconcile
 * @param {object} [opts]
 */
export function selectContractTermsCohort(verifiedAgreements, opts = {}) {
  const target = Math.min(50, Math.max(30, Number(opts.targetSize) || 40));
  const asOfYear = Number(opts.asOfYear) || new Date().getUTCFullYear();
  const rows = (verifiedAgreements || [])
    .filter((a) => String(a.quality || "").toUpperCase() === "VERIFIED")
    .filter((a) => String(a.agreementType || "FRANCHISE").toUpperCase() === "FRANCHISE")
    .map((a) => {
      const snip = (a.evidenceSnippets || []).join(" ");
      const exp = primaryDate(a);
      const termsRich =
        /\brenewal\s+options?\b/i.test(snip) ||
        /\b\d+\s*[-–]?\s*year\s+(?:renewal|extension|option)/i.test(snip) ||
        /\bfranchise\s+expir/i.test(snip) ||
        /\bterminat(?:e|ion)\s+for\s+cause\b/i.test(snip) ||
        /\boption(?:s)?\s+to\s+renew\b/i.test(snip);
      return {
        ...a,
        _exp: exp,
        _expBucket: bucketExpiration(exp, asOfYear),
        _brand: String(a.brandFamily || "UNKNOWN").toUpperCase(),
        _filingYear: filingYearFromAccessionOrUrl(a),
        _hasUrl: Boolean((a.sourceUrls || [])[0]),
        _termsRich: termsRich,
        _score:
          (termsRich ? 100 : 0) +
          (yearFromIso(exp) >= asOfYear ? 40 : yearFromIso(exp) >= asOfYear - 5 ? 20 : 0) +
          ((a.sourceUrls || []).length > 1 ? 5 : 0),
      };
    })
    .filter((a) => a._hasUrl)
    .sort((a, b) => b._score - a._score);

  const brandCaps = {
    HILTON: 12,
    MARRIOTT: 12,
    IHG: 8,
    CHOICE: 4,
    HYATT: 3,
    UNKNOWN: 4,
  };
  const expCaps = {
    HISTORICALLY_EXPIRED: 14,
    UPCOMING_0_3Y: 12,
    MID_4_8Y: 10,
    LONG_DATED: 8,
    UNKNOWN_EXP: 4,
  };
  const brandCount = Object.fromEntries(Object.keys(brandCaps).map((k) => [k, 0]));
  const expCount = Object.fromEntries(Object.keys(expCaps).map((k) => [k, 0]));
  const selected = [];
  const used = new Set();

  // Prefer diversity: round-robin by expiration bucket then brand
  const order = [
    "UPCOMING_0_3Y",
    "HISTORICALLY_EXPIRED",
    "MID_4_8Y",
    "LONG_DATED",
    "UNKNOWN_EXP",
  ];

  function pushSelected(a, reasons) {
    const brand = brandCaps[a._brand] != null ? a._brand : "UNKNOWN";
    used.add(a.contractId);
    brandCount[brand] += 1;
    expCount[a._expBucket] += 1;
    selected.push({
      contractId: a.contractId,
      hotelName: a.hotelName,
      city: a.city,
      stateRegion: a.stateRegion,
      agreementType: "FRANCHISE",
      brandFamily: a._brand,
      sourceUrls: a.sourceUrls || [],
      sourceAccessions: a.sourceAccessions || [],
      evidenceSnippets: a.evidenceSnippets || [],
      findingIds: a.findingIds || [],
      milestones: a.milestones || [],
      scheduledExpiration: a._exp,
      expirationBucket: a._expBucket,
      filingYearHint: a._filingYear,
      termsRichSnippet: Boolean(a._termsRich),
      liveContractRecordId: a.liveContractRecordId || null,
      liveContractId: a.liveContractId || a.contractId,
      selectionReasons: reasons,
    });
  }

  function tryPick(pred, softCaps = true) {
    for (const a of rows) {
      if (used.has(a.contractId)) continue;
      if (!pred(a)) continue;
      const brand = brandCaps[a._brand] != null ? a._brand : "UNKNOWN";
      if (softCaps && brandCount[brand] >= brandCaps[brand]) continue;
      if (softCaps && expCount[a._expBucket] >= expCaps[a._expBucket]) continue;
      pushSelected(a, [
        "VERIFIED_FRANCHISE",
        a._expBucket,
        a._brand,
        a._termsRich ? "TERMS_RICH_SNIPPET" : "BASELINE",
      ]);
      if (selected.length >= target) return true;
    }
    return false;
  }

  // Prefer evidence-rich snippets first (renewal/option/termination language)
  tryPick((a) => a._termsRich, false);
  for (const bucket of order) {
    tryPick((a) => a._expBucket === bucket);
    if (selected.length >= target) break;
  }
  // Fill remainder ignoring soft caps
  if (selected.length < target) {
    for (const a of rows) {
      if (used.has(a.contractId)) continue;
      used.add(a.contractId);
      selected.push({
        contractId: a.contractId,
        hotelName: a.hotelName,
        city: a.city,
        stateRegion: a.stateRegion,
        agreementType: "FRANCHISE",
        brandFamily: a._brand,
        sourceUrls: a.sourceUrls || [],
        sourceAccessions: a.sourceAccessions || [],
        evidenceSnippets: a.evidenceSnippets || [],
        findingIds: a.findingIds || [],
        milestones: a.milestones || [],
        scheduledExpiration: a._exp,
        expirationBucket: a._expBucket,
        filingYearHint: a._filingYear,
        liveContractRecordId: a.liveContractRecordId || null,
        liveContractId: a.liveContractId || a.contractId,
        selectionReasons: ["VERIFIED_FRANCHISE", "FILL", a._expBucket, a._brand],
      });
      if (selected.length >= target) break;
    }
  }

  const brandDist = {};
  const expDist = {};
  for (const s of selected) {
    brandDist[s.brandFamily] = (brandDist[s.brandFamily] || 0) + 1;
    expDist[s.expirationBucket] = (expDist[s.expirationBucket] || 0) + 1;
  }

  return {
    version: CONTRACT_TERMS_COHORT_V1,
    targetSize: target,
    selectedCount: selected.length,
    franchiseOnly: true,
    managementRelabelForbidden: true,
    brandDistribution: brandDist,
    expirationBucketDistribution: expDist,
    cohort: selected,
  };
}

/**
 * Separate management-agreement discovery queries (not franchise relabels).
 */
export function managementAgreementDiscoveryPlan() {
  return {
    version: CONTRACT_TERMS_COHORT_V1,
    subset: "MANAGEMENT_AGREEMENT_DISCOVERY",
    note: "Verified inventory is FRANCHISE-only; do not relabel. Discover management agreements separately.",
    queries: [
      {
        provider: "SEC_EDGAR_FREE",
        query: "hotel management agreement \"initial term\" expiration EX-10",
        formHints: ["EX-10", "10-K", "10-Q", "8-K"],
      },
      {
        provider: "SEC_API_IO",
        query: '"hotel management agreement" OR "management agreement" hotel (term OR expiration OR renewal)',
        formHints: ["EX-10"],
      },
      {
        provider: "EXA",
        query: "hotel management agreement renewal term years SEC exhibit",
        numResults: 5,
      },
    ],
    maxPaidDiscoveryUsd: 5,
  };
}

export default {
  selectContractTermsCohort,
  managementAgreementDiscoveryPlan,
};
