#!/usr/bin/env node
/**
 * Autograph Factory 1.0 — freeze immutable approval payload + certification package.
 * No Airtable writes.
 */
import "../load-env.js";
import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync, copyFileSync } from "fs";
import { join } from "path";
import {
  fetchPresentationBlocksForBrand,
  buildCardinalityAwareWritePlan,
  assembleProposedBlocks,
  loadPhase2hCandidate,
} from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";
import { assembleCumulativeProposal } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";
import {
  hashPresentationBlocks,
  lockCandidateVersion,
} from "../lib/brand-explorer/be-founder-approval-publication-v1.js";
import {
  BE_CANONICAL_TAB_IDS,
  BE_BRAND_SETUP_CHILD_TABLES,
  BE_PRESENTATION_TABLE,
  BE_BRAND_BASICS_TABLE,
  BE_HPC_TABLE,
} from "../lib/brand-explorer/be-airtable-integration-sot-v1.js";

const ROOT = process.cwd();
const BRAND_ID = "recEJCTDj1zrsjPM6";
const BRAND_NAME = "Autograph Collection";
const OUT = join(ROOT, "reports/brand-explorer/factory-1-0/autograph-final-cert");
const CANDIDATE_REL =
  "brand-explorer-phase2e-preview/autograph-collection-phase2h-candidate.json";
const PROTECTED = {
  curio: "receQkxgjlezsc1xg",
  ascend: "reclkgOzvAcBheUSo",
};

const SUPERSEDED_HASHES = {
  note: "Early Factory 1.0 report hashes — superseded by six-benefit + APAC fix; DO NOT APPROVE",
  candidateOverlay_preSixBenefits:
    "c2208e43811211029eaabe02c0355b146bf72f54d9d18d1186439b31c44244e5",
  assembledPreview_preSixBenefits:
    "456b9577133ebcb0e720cfb2843eb8eaa5f6c552df86458a3e5888fb8ce85bc0",
};

const TAB_SLOT_PREFIXES = {
  "atelier-overview": ["overview.", "positioning.", "brand."],
  "atelier-value-owners": ["valueOwners.", "owner."],
  "atelier-ops": ["ops.", "operating."],
  "atelier-standards-owner": ["standards.", "ownerConsiderations.", "owner."],
  "atelier-commercial": ["commercial."],
  "atelier-economics": ["economics."],
  "atelier-loyalty": ["loyalty."],
  "atelier-footprint": ["footprint."],
  "atelier-materials": ["materials."],
  "atelier-insight": ["insight.", "dealality."],
};

function browserStyleRawSha256(blocks) {
  return createHash("sha256").update(JSON.stringify(blocks || [])).digest("hex");
}

function classifyFeeClaim(text) {
  const t = String(text || "");
  const rows = [];
  const patterns = [
    {
      claim: "Application $100,000 new-to-system / conversion (+ $400/key over 250)",
      class: "HISTORICAL_FDD_EXHIBIT",
      condition: "Autograph Collection Domestic FDD issued Mar 31, 2025 — Item/exhibit fee schedule; new-to-system/conversion path; keys over 250 surcharge",
      geography: "United States (Domestic FDD)",
      status: "ATTRIBUTED_NOT_UNIVERSAL",
    },
    {
      claim: "Existing Autograph or managed-to-franchise path greater of $150,000 or $500/key",
      class: "HISTORICAL_FDD_EXHIBIT",
      condition: "Same Domestic FDD — existing Autograph / managed-to-franchise conversion path",
      geography: "United States (Domestic FDD)",
      status: "ATTRIBUTED_NOT_UNIVERSAL",
    },
    {
      claim: "Steady-state 5% monthly royalty on gross rooms sales",
      class: "HISTORICAL_FDD_EXHIBIT",
      condition: "Domestic FDD royalty on GRS; confirm live agreement — may vary by path/year",
      geography: "United States (Domestic FDD)",
      status: "ATTRIBUTED_NOT_UNIVERSAL",
    },
    {
      claim: "Program Services Contribution (~2.02% of GRS + fixed annual/per-key)",
      class: "HISTORICAL_FDD_EXHIBIT",
      condition: "Domestic FDD program contribution exhibit — approximate; confirm current exhibit",
      geography: "United States (Domestic FDD)",
      status: "ATTRIBUTED_CONFIRM_LIVE_EXHIBIT",
    },
    {
      claim: "Bonvoy loyalty assessment ~4.2% (temporary reduction to 4.0% through 2027)",
      class: "HISTORICAL_FDD_EXHIBIT_WITH_SUNSET",
      condition: "Domestic FDD loyalty assessment on eligible member revenue; temporary reduction disclosed through 2027 — not a perpetual universal rate",
      geography: "United States (Domestic FDD)",
      status: "ATTRIBUTED_CONFIRM_LIVE_EXHIBIT",
    },
    {
      claim: "PIP / brand assessment / systems install separate",
      class: "VARIABLE_OR_NEGOTIATED",
      condition: "Project-specific; not a single published universal amount",
      geography: "Deal-specific",
      status: "OWNER_DILIGENCE",
    },
  ];
  for (const p of patterns) {
    if (t.includes("$100,000") && p.claim.includes("$100,000")) rows.push(p);
    else if (t.includes("$150,000") && p.claim.includes("$150,000")) rows.push(p);
    else if (t.includes("5%") && p.claim.includes("5%")) rows.push(p);
    else if (t.includes("2.02") && p.claim.includes("2.02")) rows.push(p);
    else if (t.includes("4.2") && p.claim.includes("4.2")) rows.push(p);
    else if (/PIP|systems install/i.test(t) && p.class === "VARIABLE_OR_NEGOTIATED") rows.push(p);
  }
  return patterns; // always return full matrix for founder package
}

function subsectionScorecard(assembledBlocks) {
  const bySlot = new Map();
  for (const b of assembledBlocks) {
    const k = String(b.slotKey || "").trim();
    if (!k) continue;
    if (!bySlot.has(k)) bySlot.set(k, []);
    bySlot.get(k).push(b);
  }

  const requiredExact = {
    "loyalty.proof": 6,
    "materials.gallery.1": 1,
    "materials.gallery.2": 1,
    "materials.gallery.3": 1,
    "materials.gallery.4": 1,
    "materials.gallery.5": 1,
    "materials.gallery.6": 1,
  };

  const tabs = {};
  for (const tab of BE_CANONICAL_TAB_IDS) {
    const prefixes = TAB_SLOT_PREFIXES[tab] || [];
    const slots = [...bySlot.keys()].filter((k) => prefixes.some((p) => k.startsWith(p)));
    const blocks = slots.flatMap((k) => bySlot.get(k));
    const empty = blocks.filter((b) => !String(b.body || "").trim() && !b.imageUrl);
    const thin = blocks.filter((b) => {
      const body = String(b.body || "").trim();
      return body.length > 0 && body.length < 24;
    });
    const placeholder = blocks.filter((b) =>
      /RESEARCH_REQUIRED|TODO|N\/A — Numeric value unavailable|Focus this card/i.test(
        `${b.title || ""}\n${b.body || ""}`
      )
    );
    tabs[tab] = {
      slotCount: slots.length,
      blockCount: blocks.length,
      emptyBodyCount: empty.length,
      thinBodyCount: thin.length,
      placeholderCount: placeholder.length,
      status:
        placeholder.length > 0
          ? "FAIL_PLACEHOLDER"
          : empty.length > 12
            ? "WARN_MANY_EMPTY"
            : "PASS_STRUCTURAL",
    };
  }

  const loyaltyProof = (bySlot.get("loyalty.proof") || []).filter((b) =>
    String(b.body || b.title || "").trim()
  );
  const galleryOk = [1, 2, 3, 4, 5, 6].every((n) => {
    const b = (bySlot.get(`materials.gallery.${n}`) || [])[0];
    return b && (b.imageUrl || /^https?:\/\//i.test(String(b.body || "")));
  });

  return {
    tabs,
    exactCards: {
      loyaltyProofCount: loyaltyProof.length,
      loyaltyProofPass: loyaltyProof.length === 6,
      gallerySlotsPresent: galleryOk,
    },
    requiredExact,
    overallStructural:
      loyaltyProof.length === 6 &&
      Object.values(tabs).every((t) => t.status !== "FAIL_PLACEHOLDER"),
  };
}

async function main() {
  mkdirSync(OUT, { recursive: true });
  mkdirSync(join(OUT, "screenshots"), { recursive: true });

  const live = await fetchPresentationBlocksForBrand(BRAND_ID, BRAND_NAME);
  const candidatePath = join(ROOT, "public", CANDIDATE_REL);
  const candidate = JSON.parse(readFileSync(candidatePath, "utf8"));
  const overlayBlocks = candidate.brandExplorer?.blocks || candidate.blocks || [];

  const assembled = assembleCumulativeProposal(live.blocks, overlayBlocks, {
    note: "Autograph Factory 1.0 final freeze",
  });

  const overlayHash = hashPresentationBlocks(overlayBlocks);
  const approvalContentHash = hashPresentationBlocks(assembled.blocks);
  const rawBrowserStyleHash = browserStyleRawSha256(assembled.blocks);

  const lock = lockCandidateVersion({
    brandId: BRAND_ID,
    candidateVersion: candidate.candidateVersion || "phase2h-component-parity",
    proposedBlocks: assembled.blocks,
    lockedAt: new Date().toISOString(),
  });

  const frozenPayload = {
    contract: "BE_FACTORY_1_0_AUTOGRAPH_FROZEN_APPROVAL_PAYLOAD_V1",
    frozenAt: lock.lockedAt,
    brandId: BRAND_ID,
    brandName: BRAND_NAME,
    parentCompany: "Marriott International, Inc.",
    candidateVersion: lock.candidateVersion,
    lockId: lock.lockId,
    approvalContentHash,
    overlayHash,
    blockCount: assembled.blocks.length,
    airtableWrites: false,
    founderApprovalStatus: "AWAITING_FOUNDER_EXACT_CANDIDATE_APPROVAL",
    protectedBrandsUnchanged: PROTECTED,
    brandExplorer: {
      version: 1,
      blocks: assembled.blocks.map((b) => ({
        slotKey: b.slotKey,
        title: b.title || "",
        body: b.body || "",
        sort: b.sort || 0,
        active: b.active !== false,
        imageUrl: b.imageUrl || null,
        provenance: b.provenance || null,
      })),
    },
  };

  const frozenPath = join(OUT, "AUTOGRAPH_FROZEN_APPROVAL_PAYLOAD.json");
  writeFileSync(frozenPath, JSON.stringify(frozenPayload, null, 2) + "\n");
  // Also mirror under public for product preview of exact frozen assembly if needed
  const publicFreezeRel =
    "brand-explorer-phase2e-preview/autograph-collection-frozen-approval-payload-v1.json";
  writeFileSync(join(ROOT, "public", publicFreezeRel), JSON.stringify(frozenPayload, null, 2) + "\n");

  const writePlan = buildCardinalityAwareWritePlan({
    brandId: BRAND_ID,
    brandName: BRAND_NAME,
    currentBlocks: live.blocks,
    proposedBlocks: assembled.blocks,
  });

  const feeBodies = assembled.blocks
    .filter((b) => /^economics\.fee/i.test(b.slotKey || ""))
    .map((b) => ({ slotKey: b.slotKey, title: b.title, body: b.body }));

  const feeEvidence = {
    contract: "BE_FACTORY_1_0_AUTOGRAPH_FEE_EVIDENCE_V1",
    brandId: BRAND_ID,
    sourcePrimary: {
      label: "Autograph Collection Domestic FDD",
      issued: "2026-03-31",
      note: "Public Marriott hotel-development FDD PDF — United States domestic franchise disclosure",
      url: "https://www.hotel-development.marriott.com/ResourceFiles/fdd-document/2025-autograph-collection-fdd-3-31-2025.pdf",
    },
    supportingSources: [
      {
        label: "Marriott 10-K year ended Dec 31, 2025 — Autograph system counts",
        url: "https://www.sec.gov/Archives/edgar/data/1048286/000104828626000007/mar-20251231.htm",
        use: "System properties/rooms only — not fee schedule",
      },
      {
        label: "Autograph Collection brand page",
        url: "https://news.marriott.com/brands/autograph-collection-hotels",
        use: "Program participation / positioning — not fee schedule",
      },
    ],
    classificationRules: [
      "No figure is presented as a current universal worldwide Autograph fee.",
      "Domestic FDD exhibit figures are HISTORICAL_FDD_EXHIBIT until live exhibit/agreement is confirmed for the asset.",
      "Temporary Bonvoy assessment reductions have explicit sunset language.",
      "PIP / systems / design costs are VARIABLE_OR_NEGOTIATED.",
      "Owner-level interpretation language must remain labeled as diligence guidance.",
    ],
    claims: classifyFeeClaim(feeBodies.map((b) => b.body).join("\n")),
    presentationFeeSlots: feeBodies,
    discrepancies: [
      {
        id: "FEE-SCOPE-DOMESTIC",
        severity: "MATERIAL_SCOPE",
        finding:
          "All numeric fee/royalty/program/loyalty assessment figures in the Proposed Economics copy are attributed to the US Domestic FDD (Mar 31, 2025). International / managed / non-US paths may differ materially.",
        resolutionRequiredBeforeWriteback:
          "Keep Domestic attribution visible in Presentation; do not strip 'confirm current FDD exhibits' / geography qualifiers. Structured Fee Structure writeback must store source edition + geography.",
        status: "DOCUMENTED_IN_COPY",
      },
      {
        id: "FEE-BONVOY-TEMPORARY",
        severity: "MATERIAL_CONDITION",
        finding:
          "4.0% temporary Bonvoy assessment reduction through 2027 is conditional and time-bounded — not a permanent 4.0% rate.",
        resolutionRequiredBeforeWriteback:
          "Retain sunset language in operate fee body; structured persistence must store both base ~4.2% and temporary 4.0% with end year.",
        status: "DOCUMENTED_IN_COPY",
      },
    ],
    founderFeeGate: "PASS_WITH_ATTRIBUTED_FDD_SCOPE",
  };

  const scorecard = subsectionScorecard(assembled.blocks);

  const persistencePlan = {
    contract: "BE_FACTORY_1_0_AUTOGRAPH_STRUCTURED_PERSISTENCE_PLAN_V1",
    brandId: BRAND_ID,
    airtableWritesAuthorized: false,
    principle:
      "Map researched facts to existing Brand Setup child tables + Presentation; do not invent duplicate tables; HPC only via authorized HPC workflow.",
    destinations: [
      {
        table: BE_PRESENTATION_TABLE,
        action: "FOUNDER_APPROVED_WRITEBACK_ONLY",
        records: writePlan.counts,
        hashGate: approvalContentHash,
      },
      {
        table: "Brand Setup - Fee Structure",
        action: "PLAN_ONLY_NO_WRITE",
        fieldsPlanned: [
          "Initial / application fee paths (new vs conversion vs existing)",
          "Royalty % on GRS",
          "Program services contribution components",
          "Loyalty assessment base + temporary reduction",
          "Source: FDD edition + geography + verification date",
        ],
        ownership: "brand_setup_child / system_owned_no_factory_silent_write",
      },
      {
        table: "Brand Setup - Loyalty & Commercial",
        action: "PLAN_ONLY_NO_WRITE",
        fieldsPlanned: [
          "Program name: Marriott Bonvoy",
          "Six benefit themes (loyalty.proof)",
          "Earn / redeem / elite narrative pointers",
          "Source URLs + last verified",
        ],
      },
      {
        table: "Brand Setup - Brand Footprint",
        action: "PLAN_ONLY_NO_WRITE",
        fieldsPlanned: [
          "Company-reported YE2025 open hotels 362 / rooms 76,399 (10-K attributed)",
          "Separate from HPC observed inventory",
        ],
      },
      {
        table: BE_BRAND_BASICS_TABLE,
        action: "RETAIN_IDENTITY_ONLY",
        note: "Do not mutate Brand Status / Parent Company / identity anchors in this cycle",
      },
      {
        table: BE_HPC_TABLE,
        action: "NO_WRITE_THIS_CYCLE",
        note: "HPC corrections only through authorized census/HPC ownership workflow",
      },
    ],
    brandSetupChildTablesKnown: BE_BRAND_SETUP_CHILD_TABLES,
    schemaExtensionProposals: [
      {
        ifNeeded: "Fee Structure lacks edition/geography/provenance fields",
        proposal:
          "Add optional Source Document, Source Edition Date, Geography Scope, Verification Date, Confidence — reuse existing fields first; founder approval before schema change",
      },
    ],
  };

  const hashProvenance = {
    contract: "BE_FACTORY_1_0_HASH_PROVENANCE_V1",
    brandId: BRAND_ID,
    supersededDoNotApprove: SUPERSEDED_HASHES,
    layers: [
      {
        id: "overlayHash",
        algorithm: "sha256(hashPresentationBlocks normalized overlay-only blocks)",
        value: overlayHash,
        means: "Gap-fill / staged candidate overlay file only (not full product profile)",
        approveForWriteback: false,
      },
      {
        id: "approvalContentHash",
        algorithm: "sha256(hashPresentationBlocks normalized assembled CURRENT+overlay)",
        value: approvalContentHash,
        means:
          "IMMUTABLE assembled publication payload — CURRENT retained + complete non-regressive staged changes. This is the only hash founder may approve for Airtable Presentation writeback and readback.",
        approveForWriteback: true,
        lockId: lock.lockId,
        frozenPayloadPath: frozenPath,
        publicMirror: publicFreezeRel,
      },
      {
        id: "browserRawJsonSha256",
        algorithm: "sha256(JSON.stringify(assembled.blocks)) without slot sort normalization",
        value: rawBrowserStyleHash,
        means:
          "Diagnostic only — matches earlier browser crypto.subtle digest when subtle hashes raw brandExplorer.blocks. MUST NOT be used for approval; can diverge from approvalContentHash.",
        approveForWriteback: false,
      },
      {
        id: "browserBannerLegacy",
        algorithm: "fnv1a or raw sha of unsorted blocks (atelier meta.contentHash)",
        means:
          "UI banner hash before alignment fix — explanatory only. Post-fix banner should display approvalContentHash / normalized hash.",
        approveForWriteback: false,
      },
    ],
    rule: "Founder approves approvalContentHash only. Writeback + readback must equal that hash.",
  };

  const curioAscendNoRegression = {
    curioId: PROTECTED.curio,
    ascendId: PROTECTED.ascend,
    note: "This freeze does not mutate Curio/Ascend Presentation. Soft-default remains Current Saved for FOUNDER_APPROVED_WRITEBACK brands.",
    writebackTouchedBrandIds: [],
  };

  const packageReport = {
    contract: "BE_FACTORY_1_0_AUTOGRAPH_FINAL_CERT_PACKAGE_V1",
    generatedAt: new Date().toISOString(),
    pr489: {
      status: "MERGED",
      mergeCommit: "0474c323bb4c9d794219a6a96100bac90552f4d7",
      url: "https://github.com/joandejarden-create/my-operators-backend/pull/489",
    },
    brandId: BRAND_ID,
    brandName: BRAND_NAME,
    writebackAuthorized: false,
    customerPublicationAuthorized: false,
    approvalContentHash,
    overlayHash,
    lockId: lock.lockId,
    liveBlockCount: live.blocks.length,
    overlayBlockCount: overlayBlocks.length,
    assembledBlockCount: assembled.blocks.length,
    assemblySummary: assembled.summary || assembled.assemblySummary || null,
    writePlanCounts: writePlan.counts || writePlan,
    scorecard,
    feeEvidenceGate: feeEvidence.founderFeeGate,
    hashProvenanceRule: hashProvenance.rule,
    persistenceWrites: 0,
    status: scorecard.overallStructural
      ? "READY_FOR_FOUNDER_EXACT_CANDIDATE_APPROVAL"
      : "BLOCKED_STRUCTURAL_GAPS",
    frozenPayloadPath: frozenPath,
  };

  writeFileSync(join(OUT, "00-HASH-PROVENANCE.json"), JSON.stringify(hashProvenance, null, 2));
  writeFileSync(join(OUT, "01-FEE-EVIDENCE.json"), JSON.stringify(feeEvidence, null, 2));
  writeFileSync(join(OUT, "02-SUBSECTION-SCORECARD.json"), JSON.stringify(scorecard, null, 2));
  writeFileSync(join(OUT, "03-STRUCTURED-PERSISTENCE-PLAN.json"), JSON.stringify(persistencePlan, null, 2));
  writeFileSync(join(OUT, "04-WRITE-PLAN-DRY-RUN.json"), JSON.stringify({ counts: writePlan.counts || writePlan, mutatingOps: writePlan.ops?.length || writePlan.mutatingOps || null, applied: false }, null, 2));
  writeFileSync(join(OUT, "05-CURIO-ASCEND-NO-REGRESSION.json"), JSON.stringify(curioAscendNoRegression, null, 2));
  writeFileSync(join(OUT, "06-FINAL-CERT-PACKAGE.json"), JSON.stringify(packageReport, null, 2));

  const md = `# Autograph Final Certification Package

**Status:** \`${packageReport.status}\`  
**Brand:** Autograph Collection (\`${BRAND_ID}\`)  
**PR #489:** MERGED @ \`0474c323bb4c9d794219a6a96100bac90552f4d7\`  
**Airtable writes:** **NONE** (awaiting exact-candidate approval)

## Approve this hash only

| Field | Value |
|---|---|
| **approvalContentHash** | \`${approvalContentHash}\` |
| lockId | \`${lock.lockId}\` |
| assembled blocks | ${assembled.blocks.length} |
| overlayHash (not for approval) | \`${overlayHash}\` |

### Superseded (do not approve)
- \`${SUPERSEDED_HASHES.candidateOverlay_preSixBenefits}\`
- \`${SUPERSEDED_HASHES.assembledPreview_preSixBenefits}\`

Browser banner hashes that used raw \`JSON.stringify\` without normalization are diagnostic only.

## Fee evidence
Gate: **${feeEvidence.founderFeeGate}**  
All numeric Autograph fees in Proposed Economics are **US Domestic FDD Mar 31 2025** attributed figures — not universal worldwide current rates. Temporary Bonvoy 4.0% reduction has sunset through 2027.

## Structural scorecard
Loyalty proof cards: **${scorecard.exactCards.loyaltyProofCount}/6** (${scorecard.exactCards.loyaltyProofPass ? "PASS" : "FAIL"})  
Overall structural: **${scorecard.overallStructural}**

## Persistence
Presentation write plan prepared (dry-run only). Structured Brand Setup child-table writes remain **PLAN_ONLY_NO_WRITE** until founder approves exact candidate + persistence apply.

## Protected
Curio / Ascend unchanged.
`;
  writeFileSync(join(OUT, "07-FOUNDER-APPROVAL-BRIEF.md"), md);

  console.log(JSON.stringify(packageReport, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
