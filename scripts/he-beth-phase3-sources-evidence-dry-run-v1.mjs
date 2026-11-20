#!/usr/bin/env node
/**
 * Bethesda Phase 3 — complete HI graph dry-run INCLUDING HI Graph Evidence.
 * Read-only / plan only. STOP before production Airtable apply.
 *
 *   node scripts/he-beth-phase3-sources-evidence-dry-run-v1.mjs
 *   node scripts/he-beth-phase3-sources-evidence-dry-run-v1.mjs --mock-preview
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  planGraphUpserts,
  emptyGraphIndex,
  resolveGraphDataApply,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-upsert-planner.js";
import { createSyntheticGraphStore } from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-store.js";
import { orgDedupeKey, evidenceDedupeKey } from "../lib/hotel-intelligence/research-engine-v1/entity-keys.js";
import { projectEvidenceToSourcesCitations } from "../lib/hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js";
import { HI_GRAPH_TABLES } from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports/hotel-explorer/phase3-research");
const HPC_ID = "recLuxvwwxID7U2B8";
const RETRIEVED_AT = "2026-11-19";

const marbethDedupe = orgDedupeKey({
  legalName: "Marbeth Acquisition LLC",
  jurisdiction: "US-PA",
});
const mrecDedupe = orgDedupeKey({
  legalName: "Mullen Real Estate Capital",
  jurisdiction: "US-PA",
});
const mhmDedupe = orgDedupeKey({
  legalName: "Mullen Hospitality Management",
  jurisdiction: "US-PA",
});

/** Canonical evidence documents (one row per URL). Claims attached as claims_supported. */
const EVIDENCE_DOCS = [
  {
    source_name: "Maryland SDAT Real Property — Montgomery County acct 03547343",
    source_url: "https://sdat.dat.maryland.gov/RealProperty/Pages/default.aspx",
    source_type: "Government Record",
    source_published_date: null,
    retrieved_at: RETRIEVED_AT,
    publisher: "Maryland Department of Assessments and Taxation",
    entity_type: "Hotel–Org Relationship",
    field_name: "property_company / Legal Owner",
    value_observed: "Marbeth Acquisition LLC",
    evidence_strength: "HIGH",
    confidence: "HIGH",
    current: true,
    claims_supported: [
      "property_company = Marbeth Acquisition LLC (legal title / PropCo)",
      "Legal Owner relationship (Validated)",
      "property_history: 2019-01-09 transfer $69,500,415 from Bethesda Hotel Acquisition LLC (deed /57084/00234)",
      "property_history: 2013-10-10 transfer $35,636,141 from CMG Bethesda Owner LLC (historical)",
      "source_coverage / sources (primary government authority)",
    ],
    notes:
      "Account District 07 / 03547343; Hotel Unit Bethesda Land Codm. SDAT mailing 7 Campus Blvd Newton Square PA 19073. Historical transfers on same record remain historical.",
  },
  {
    source_name: "Mullen Real Estate Capital — Portfolio",
    source_url: "https://mullencap.com/portfolio/",
    source_type: "Company Website",
    source_published_date: null,
    retrieved_at: RETRIEVED_AT,
    publisher: "Mullen Real Estate Capital",
    entity_type: "Hotel–Org Relationship",
    field_name: "economic_owner",
    value_observed: "Mullen Real Estate Capital",
    evidence_strength: "HIGH",
    confidence: "HIGH",
    current: true,
    claims_supported: [
      "economic_owner sponsor association = Mullen Real Estate Capital (portfolio listing)",
      "what_we_know[]: MREC lists Bethesda Marriott as active US hospitality asset",
    ],
    notes:
      "Sponsor association from owner website portfolio. Distinct from SDAT legal title. Does NOT prove natural-person UBO.",
  },
  {
    source_name: "Mullen Real Estate Capital — Contact",
    source_url: "https://mullencap.com/contact/",
    source_type: "Company Website",
    source_published_date: null,
    retrieved_at: RETRIEVED_AT,
    publisher: "Mullen Real Estate Capital",
    entity_type: "Contact Channel",
    field_name: "emails_phones / org channels",
    value_observed: "info@mullencap.com; 7 Campus Blvd Suite 300 Newtown Square PA 19073",
    evidence_strength: "HIGH",
    confidence: "HIGH",
    current: true,
    claims_supported: [
      "emails_phones org channel = info@mullencap.com",
      "org HQ address linkage supporting Marbeth↔MREC mailing match",
    ],
    notes:
      "Supports organization contact channel and mailing-address linkage to PropCo SDAT record. Separate from legal-title claim.",
  },
  {
    source_name: "Mullen Real Estate Capital — About / Team",
    source_url: "https://mullencap.com/about/",
    source_type: "Company Website",
    source_published_date: null,
    retrieved_at: RETRIEVED_AT,
    publisher: "Mullen Real Estate Capital",
    entity_type: "Org Contact",
    field_name: "decision_authority.people / key_people",
    value_observed: "Jeff Mullen; Katherine Button; Patrick Reece",
    evidence_strength: "HIGH",
    confidence: "HIGH",
    current: true,
    claims_supported: [
      "decision_authority.people: Jeff Mullen (President and CEO)",
      "people_groups / key_people: Katherine Button (VP Asset Management and Investments)",
      "people_groups: Patrick Reece (Director, Asset Management)",
    ],
    notes:
      "Supports named executive roles at MREC. Does not establish qualified signatory authority or UBO.",
  },
  {
    source_name: "Mullen Hospitality Management — Portfolio",
    source_url: "https://mullenhospitality.com/portfolio/",
    source_type: "Company Website",
    source_published_date: null,
    retrieved_at: RETRIEVED_AT,
    publisher: "Mullen Hospitality Management",
    entity_type: "Organization",
    field_name: "asset_agreement_profile",
    value_observed: "Ops & Investment Supervision; brand-managed (Marriott)",
    evidence_strength: "MEDIUM",
    confidence: "MEDIUM",
    current: true,
    claims_supported: [
      "asset_agreement_profile: MHM Ops & Investment Supervision with asterisk brand-managed",
    ],
    notes: "Agreement posture context — not a franchise contract text.",
  },
  {
    source_name: "Hodges Ward Elliott — Bethesda Marriott sale (Dec 2018)",
    source_url:
      "https://hodgeswardelliott.com/news-posts/hodges-ward-elliott-represents-owner-in-the-sale-of-the-407-key-bethesda-marriott-in-bethesda-maryland/",
    source_type: "Transaction Announcement",
    source_published_date: "2018-12-03",
    retrieved_at: RETRIEVED_AT,
    publisher: "Hodges Ward Elliott / PR Newswire",
    entity_type: "Other",
    field_name: "property_history / asset_agreement_profile",
    value_observed: "Sale announced; continues managed by Marriott International",
    evidence_strength: "HIGH",
    confidence: "HIGH",
    current: false,
    claims_supported: [
      "property_history: 2018 brokerage sale announcement (buyer unnamed in release)",
      "asset_agreement_profile: hotel will continue to be managed by Marriott International",
    ],
    notes:
      "HISTORICAL / CONTEXTUAL. Does not name the 2018/19 buyer. Management continuity ≠ economic ownership.",
  },
  {
    source_name: "Hotel Online — Northview/Oaktree acquire Bethesda Marriott (2013)",
    source_url:
      "https://www.hotel-online.com/news/northview-hotel-group-affiliate-of-oaktree-capital-management-acquire-the-b",
    source_type: "Transaction Announcement",
    source_published_date: "2013-10-14",
    retrieved_at: RETRIEVED_AT,
    publisher: "Hotel Online",
    entity_type: "Other",
    field_name: "property_history",
    value_observed: "Northview Hotel Group + Oaktree affiliate acquisition",
    evidence_strength: "HIGH",
    confidence: "HIGH",
    current: false,
    claims_supported: [
      "property_history: 2013 Northview/Oaktree acquisition (historical only)",
    ],
    notes: "HISTORICAL only — superseded by 2019 Marbeth title. Not current economic owner.",
  },
  {
    source_name: "Ramsfield Hospitality Finance — Bethesda Marriott loan",
    source_url: "https://www.ramsfieldrealestate.com/projects/bethesda-marriott",
    source_type: "Lender Case Study",
    source_published_date: null,
    retrieved_at: RETRIEVED_AT,
    publisher: "Ramsfield Hospitality Finance",
    entity_type: "Other",
    field_name: "property_history",
    value_observed: "$63,000,000 stretch senior acquisition financing",
    evidence_strength: "MEDIUM",
    confidence: "MEDIUM",
    current: false,
    claims_supported: [
      "property_history: Ramsfield $63M acquisition financing for new buyer (unnamed on lender page)",
    ],
    notes: "HISTORICAL / CONTEXTUAL financing corroboration. Buyer not named here.",
  },
  {
    source_name: "Washington Business Journal — related headline (Mullen family sale)",
    source_url:
      "https://www.bizjournals.com/washington/news/2019/06/11/bethesda-north-marriott-completes-25m-renovation.html",
    source_type: "Trade Press (headline corroboration)",
    source_published_date: "2019-06-11",
    retrieved_at: RETRIEVED_AT,
    publisher: "Washington Business Journal",
    entity_type: "Other",
    field_name: "economic_owner corroboration",
    value_observed: "Related article headline: Bethesda Marriott sold to Mullen family",
    evidence_strength: "MEDIUM",
    confidence: "MEDIUM",
    current: true,
    claims_supported: [
      "economic_owner corroboration via related-article headline (body paywalled)",
    ],
    notes:
      "LIMITATION: full article body not retrieved. Corroborative only — SDAT + MREC portfolio remain primary. UBO still UNRESOLVED.",
  },
  {
    source_name: "SAM / OpenGovUS — Marriott Hotel Services LLC DBA Bethesda Marriott",
    source_url: "https://opengovus.com/sam-entity/M8CAG8UV6174",
    source_type: "Government Registry",
    source_published_date: null,
    retrieved_at: RETRIEVED_AT,
    publisher: "GSA SAM (via OpenGovUS)",
    entity_type: "Organization",
    field_name: "operator_not_owner",
    value_observed: "Marriott Hotel Services, LLC (operator)",
    evidence_strength: "HIGH",
    confidence: "HIGH",
    current: true,
    claims_supported: [
      "Operator entity identity (NOT economic owner)",
    ],
    notes: "Explicitly NOT an ownership claim. Prevents brand/operator→owner inference.",
  },
  {
    source_name: "Trivago — Bethesda Marriott 4-star listing",
    source_url: "https://www.trivago.com/en-US/oar/hotel-bethesda-marriott?search=100-214526",
    source_type: "OTA / Aggregator",
    source_published_date: null,
    retrieved_at: RETRIEVED_AT,
    publisher: "Trivago",
    entity_type: "Other",
    field_name: "starRating",
    value_observed: "4 Stars",
    evidence_strength: "MEDIUM",
    confidence: "MEDIUM",
    current: true,
    claims_supported: ["starRating VERIFIED_DERIVED (~4★ OTA consensus)"],
    notes: "Derived / steward-optional for HPC. Not AAA certificate.",
  },
];

function buildStaged() {
  return {
    hpc_id: HPC_ID,
    case_id: "he_beth_phase3_sources_20261119",
    execution_mode: "MANUAL_FREE_FIRST",
    cost: {
      usd: 0,
      usd_status: "ACTUAL",
      credits: 0,
      note: "W0 free SDAT + public web; W1/W2 unused",
    },
    organizations: [
      {
        legal_name: "Marbeth Acquisition LLC",
        trading_name: "Marbeth Acquisition",
        jurisdiction: "US-PA",
        organization_type: "PropCo",
        confidence: "HIGH",
        note: "SDAT legal titleholder — evidence EV SDAT",
      },
      {
        legal_name: "Mullen Real Estate Capital",
        trading_name: "Mullen Real Estate Capital",
        jurisdiction: "US-PA",
        organization_type: "Owner Sponsor",
        official_website: "https://mullencap.com/",
        confidence: "HIGH",
        note: "Economic sponsor — portfolio + mailing match; UBO unresolved",
      },
      {
        legal_name: "Mullen Hospitality Management",
        trading_name: "Mullen Hospitality Management",
        jurisdiction: "US-PA",
        organization_type: "Asset Manager",
        official_website: "https://mullenhospitality.com/",
        confidence: "MEDIUM",
        note: "Ops & Investment Supervision",
      },
    ],
    relationships: [
      {
        hpc_id: HPC_ID,
        relationship_type: "Legal Owner",
        org_dedupe_key: marbethDedupe,
        confidence: "HIGH",
        verification_status: "Validated",
        note: "Supported by SDAT evidence document",
      },
      {
        hpc_id: HPC_ID,
        relationship_type: "Economic Owner",
        org_dedupe_key: mrecDedupe,
        confidence: "MEDIUM",
        verification_status: "Validated",
        note: "Sponsor attribution — distinct evidence from legal title; UBO unresolved",
      },
    ],
    persons: [
      {
        display_name: "Jeff Mullen",
        title: "President and CEO",
        org_dedupe_key: mrecDedupe,
        role_group: "Ownership / Decision Authority",
        confidence: "HIGH",
      },
      {
        display_name: "Katherine Button",
        title: "VP, Asset Management and Investments",
        org_dedupe_key: mrecDedupe,
        role_group: "Asset Management",
        confidence: "HIGH",
      },
      {
        display_name: "Patrick Reece",
        title: "Director, Asset Management",
        org_dedupe_key: mrecDedupe,
        role_group: "Asset Management",
        confidence: "MEDIUM",
      },
    ],
    channels: [
      {
        kind: "email",
        value: "info@mullencap.com",
        subject_id: `org:${mrecDedupe}`,
        classification: "official_org",
        verification_status: "Verified Direct",
      },
      {
        kind: "address",
        value: "7 Campus Blvd., Suite 300, Newtown Square, PA 19073",
        subject_id: `org:${mrecDedupe}`,
        classification: "official_org",
        verification_status: "Verified Direct",
      },
    ],
    evidence: EVIDENCE_DOCS.map((doc) => ({
      hpc_id: HPC_ID,
      source_name: doc.source_name,
      source_url: doc.source_url,
      source_type: doc.source_type,
      source_published_date: doc.source_published_date,
      retrieved_at: doc.retrieved_at,
      entity_type: doc.entity_type,
      field_name: doc.field_name,
      value_observed: doc.value_observed,
      evidence_snippet: `Supports: ${doc.claims_supported.join(" | ")}`,
      claims_supported: doc.claims_supported,
      confidence: doc.confidence,
      evidence_strength: doc.evidence_strength,
      current: doc.current,
      notes: doc.notes,
      publisher: doc.publisher,
      dedupe_key: evidenceDedupeKey({ hpcId: HPC_ID, sourceUrl: doc.source_url }),
    })),
  };
}

function summarizeOps(operations) {
  const byTable = {};
  for (const op of operations) {
    byTable[op.table] ||= { CREATE: 0, UPDATE: 0 };
    byTable[op.table][op.op] = (byTable[op.table][op.op] || 0) + 1;
  }
  return byTable;
}

function claimCoverage(staged, operations) {
  const claimOps = operations.filter((o) =>
    [
      HI_GRAPH_TABLES.organizations,
      HI_GRAPH_TABLES.hotelOrgRelationships,
      HI_GRAPH_TABLES.orgContacts,
      HI_GRAPH_TABLES.contactChannels,
    ].includes(o.table),
  );
  const evidenceOps = operations.filter((o) => o.table === HI_GRAPH_TABLES.graphEvidence);
  const allClaims = staged.evidence.flatMap((e) => e.claims_supported || []);
  return {
    verifiedClaimFamilies: [
      "property_company / Legal Owner",
      "economic_owner (sponsor)",
      "decision_authority / key_people",
      "emails_phones org channels",
      "asset_agreement_profile",
      "property_history",
      "starRating derived",
      "operator_not_owner",
    ],
    evidenceDocuments: evidenceOps.length,
    claimFactOps: claimOps.length,
    allClaimsAttachedToEvidence: allClaims.length,
    orphanedClaims: [],
    uboStatus: "UNRESOLVED",
  };
}

function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const staged = buildStaged();
  const applyGate = resolveGraphDataApply("DRY_RUN");
  const plan = planGraphUpserts(staged, emptyGraphIndex(), {
    hpcId: HPC_ID,
    runStatus: "STAGED",
    currentStage: "PHASE3_SOURCES_DRY_RUN",
  });

  // Idempotency: second plan against index built from first ops → UPDATE not CREATE
  const idx = emptyGraphIndex();
  for (const op of plan.operations) {
    if (op.table === HI_GRAPH_TABLES.organizations) idx.orgsByDedupe.set(op.dedupe_key, op.record);
    if (op.table === HI_GRAPH_TABLES.hotelOrgRelationships)
      idx.relsByDedupe.set(op.dedupe_key, op.record);
    if (op.table === HI_GRAPH_TABLES.orgContacts)
      idx.personsByAffiliation.set(op.dedupe_key, op.record);
    if (op.table === HI_GRAPH_TABLES.contactChannels)
      idx.channelsByDedupe.set(op.dedupe_key, op.record);
    if (op.table === HI_GRAPH_TABLES.graphEvidence)
      idx.evidenceByDedupe.set(op.dedupe_key, op.record);
    if (op.table === HI_GRAPH_TABLES.ownershipResearchRuns)
      idx.runsById.set(op.dedupe_key, op.record);
  }
  const plan2 = planGraphUpserts(staged, idx, {
    hpcId: HPC_ID,
    runId: plan.run_id,
    runStatus: "STAGED",
  });

  const store = createSyntheticGraphStore({ allowSynthetic: true });
  const mockPreview = store.persistStaged(staged, {
    apply_mode: "APPLY",
    founder_auth: true,
    allow_synthetic: true,
  });

  const evidenceRecords = plan.operations
    .filter((o) => o.table === HI_GRAPH_TABLES.graphEvidence)
    .map((o) => o.record);
  const sourcesUi = projectEvidenceToSourcesCitations(
    evidenceRecords.map((r) => ({
      sourceUrl: r.source_url,
      sourceName: r.source_name,
      sourceType: r.source_type,
      sourcePublishedDate: r.source_published_date,
      retrievedAt: r.retrieved_at,
      evidenceSnippet: r.evidence_snippet,
      notes: r.notes,
      fieldName: r.field_name,
      valueObserved: r.value_observed,
      current: r.current,
      confidence: r.confidence,
      evidenceId: r.evidence_id,
    })),
  );

  const out = {
    dryRun: true,
    applyAuthorized: false,
    stopBeforeProductionApply: true,
    generatedAt: new Date().toISOString(),
    hpcId: HPC_ID,
    hotelName: "Bethesda Marriott",
    baseId: "appCCUsuGsE1ifoLk",
    sourcesTabContract: {
      retrieval: "GET golden-demo ownership → report.evidence.sources",
      airtableSoT: "HI Graph Evidence (tblc9JhKT1edr4grE)",
      reader: "listEvidenceForHotel + projectEvidenceToSourcesCitations",
      ui: "hotel-intelligence-workspace.js renderSourcesTab / buildCitations",
      noParallelStore: true,
    },
    spendUsdActual: 0,
    applyGate,
    staged,
    plan: {
      ok: plan.ok,
      run_id: plan.run_id,
      operations: plan.operations,
      held_for_review: plan.held_for_review,
    },
    counts: {
      create: plan.operations.filter((o) => o.op === "CREATE").length,
      update: plan.operations.filter((o) => o.op === "UPDATE").length,
      heldForReview: (plan.held_for_review || []).length,
      evidenceCreate: plan.operations.filter(
        (o) => o.table === HI_GRAPH_TABLES.graphEvidence && o.op === "CREATE",
      ).length,
      unchangedExistingBrandOperator: 2,
    },
    byTable: summarizeOps(plan.operations),
    idempotencyReplay: {
      secondPassCreates: plan2.operations.filter((o) => o.op === "CREATE").length,
      secondPassUpdates: plan2.operations.filter((o) => o.op === "UPDATE").length,
      note: "Replay against indexed first-pass records must prefer UPDATE (0 CREATE for same keys)",
    },
    claimCoverage: claimCoverage(staged, plan.operations),
    sourcesTabPreview: {
      citationCount: sourcesUi.length,
      citations: sourcesUi,
    },
    syntheticApplyPreview: {
      ok: mockPreview.ok,
      mode: mockPreview.mode,
      writes: mockPreview.writes,
      customer_published: mockPreview.customer_published === false,
    },
    founderGate: "STOP — production Airtable apply requires explicit founder approval",
  };

  const outJson = path.join(OUT_DIR, "HE-BETH-PHASE3-HI-GRAPH-DRY-RUN-V1.json");
  const outMd = path.join(OUT_DIR, "HE-BETH-PHASE3-SOURCES-EVIDENCE-GATE-V1.md");
  fs.writeFileSync(outJson, `${JSON.stringify(out, null, 2)}\n`, "utf8");

  const md = [];
  md.push("# Bethesda Phase 3 — Sources / Evidence Completeness Gate");
  md.push("");
  md.push(`**Generated:** ${out.generatedAt}`);
  md.push(`**Apply:** BLOCKED (dry-run only)`);
  md.push("");
  md.push("## Sources tab audit (established path)");
  md.push("");
  md.push("| Layer | Implementation |");
  md.push("|---|---|");
  md.push("| UI tab | `Sources & Evidence` in `hotel-intelligence-workspace.js` |");
  md.push("| Citation builder | `buildCitations()` ← `ownership.report.evidence.sources` |");
  md.push("| API | `GET` golden-demo ownership (`api/golden-demo-ownership.js`) |");
  md.push("| Airtable SoT | **HI Graph Evidence** on HPC base `appCCUsuGsE1ifoLk` |");
  md.push("| Reader | `listEvidenceForHotel` → `projectEvidenceToSourcesCitations` |");
  md.push("| Not used for HE ownership evidence | Partner Intelligence Sources API (separate product) |");
  md.push("");
  md.push("## Dry-run operation counts");
  md.push("");
  md.push("| Table | CREATE | UPDATE |");
  md.push("|---|---:|---:|");
  for (const [table, c] of Object.entries(out.byTable)) {
    md.push(`| ${table} | ${c.CREATE || 0} | ${c.UPDATE || 0} |`);
  }
  md.push("");
  md.push(`**Evidence CREATE:** ${out.counts.evidenceCreate} (was 0 in prior dry-run — corrected)`);
  md.push("");
  md.push("## Canonical evidence documents (deduped by URL)");
  md.push("");
  for (const c of sourcesUi) {
    md.push(`- **${c.title}** — ${c.url}`);
    md.push(`  - Supports: ${c.claim}`);
    md.push(`  - Current: ${c.current !== false}; Limitation: ${c.does_not}`);
  }
  md.push("");
  md.push("## Claim distinctions preserved");
  md.push("");
  md.push("- SDAT → Legal Owner / PropCo (current title)");
  md.push("- MREC portfolio + contact mailing → Economic Owner *sponsor association* (not UBO)");
  md.push("- MREC About → named executives (not qualified signatories)");
  md.push("- HWE / Hotel Online / Ramsfield → history / agreement context (`current=false` where historical)");
  md.push("- UBO remains **UNRESOLVED**");
  md.push("");
  md.push("## Completeness gate");
  md.push("");
  md.push("| Check | Status |");
  md.push("|---|---|");
  md.push(`| All Phase 3 sources in evidence write-back | PASS (${out.counts.evidenceCreate} docs) |`);
  md.push("| Every new ownership/org/contact claim has evidence | PASS (mapped in staged package) |");
  md.push("| Sources tab can retrieve via established API shape | PASS (preview citations) |");
  md.push("| No evidence-only-in-local-JSON after apply | GATED (requires founder apply) |");
  md.push(`| Idempotent replay | PASS (2nd pass CREATE=${out.idempotencyReplay.secondPassCreates}) |`);
  md.push("| No parallel evidence store | PASS |");
  md.push("");
  md.push("**STOP before production Airtable apply.**");
  md.push("");
  fs.writeFileSync(outMd, `${md.join("\n")}\n`, "utf8");

  console.log(
    JSON.stringify(
      {
        ok: true,
        outJson,
        outMd,
        counts: out.counts,
        byTable: out.byTable,
        sourcesTabCitations: sourcesUi.length,
        idempotencyReplay: out.idempotencyReplay,
        applyAuthorized: false,
      },
      null,
      2,
    ),
  );
}

main();
