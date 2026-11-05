/**
 * GDI V37 — backfill discovery ledger from existing four-hotel Full E2E artifacts.
 * Never fabricates rejection reasons. Marks provenance gaps explicitly.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyDispositionToRecord,
  createDiscoveryLedgerRecord,
  normalizeOrgKey,
} from "./discovery-candidate-ledger-schema-v1.js";
import { upsertDiscoveryCandidates } from "./discovery-candidate-ledger-v1.js";
import { DISPOSITION_REASON_CODE } from "./disposition-reason-codes-v1.js";
import {
  RESEARCH_STATUS,
  researchStatusFromAcceptanceDisposition,
} from "./research-status-transitions-v1.js";
import { aggregateResearchFunnel } from "./research-funnel-aggregate-v1.js";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../..",
);

export const FOUR_HOTEL_CAMPAIGN_ID = "four-hotel-14-workflow-full-e2e-v1";
export const FOUR_HOTEL_LIVE_ROOT = path.join(
  ROOT,
  "reports/gdi-four-hotel-14-workflow-full-e2e/live-execution",
);

export const FOUR_HOTELS = Object.freeze([
  {
    key: "sheraton-guadalajara-expo",
    hotelId: "recCAGqkERSXgiku4",
    hotelName: "Sheraton Guadalajara Expo",
  },
  {
    key: "voco-cancun-zona-hotelera",
    hotelId: "recTk5I8VOzsPhxDz",
    hotelName: "voco Cancún Zona Hotelera",
  },
  {
    key: "wyndham-alltra-punta-cana",
    hotelId: "recXh4NgF8sDbi9cQ",
    hotelName: "Wyndham Alltra Punta Cana",
  },
  {
    key: "radisson-blu-toronto-downtown",
    hotelId: "recH9h3McgMNFQEla",
    hotelName: "Radisson Blu Toronto Downtown",
  },
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function readJson(fp, fb = null) {
  if (!fs.existsSync(fp)) return fb;
  return JSON.parse(fs.readFileSync(fp, "utf8").replace(/^\uFEFF/, ""));
}

function listFilesRecursive(dir, pred) {
  const out = [];
  if (!fs.existsSync(dir)) return out;
  for (const name of fs.readdirSync(dir)) {
    const fp = path.join(dir, name);
    const st = fs.statSync(fp);
    if (st.isDirectory()) out.push(...listFilesRecursive(fp, pred));
    else if (!pred || pred(fp, name)) out.push(fp);
  }
  return out;
}

function orgKeyFromOpp(o = {}) {
  return normalizeOrgKey(
    o.organizationName || o.organisationName || o.displayTitle || o.title,
  );
}

function acceptanceFields(o = {}) {
  const nested =
    o.independentAcceptance && typeof o.independentAcceptance === "object"
      ? o.independentAcceptance
      : null;
  return (
    nested?.disposition ||
    nested?.readinessDisposition ||
    o.v34Disposition ||
    o.acceptanceDisposition ||
    o.commercialAcceptanceDisposition ||
    o.customerReadinessDisposition ||
    o.readinessDisposition ||
    o.disposition ||
    o.opportunityQualification ||
    null
  );
}

function loadPromotionSet() {
  const manifest = readJson(
    path.join(FOUR_HOTEL_LIVE_ROOT, "18-FOUNDER_PROMOTION_MANIFEST.json"),
    { promoteExactIds: [] },
  );
  return new Set(manifest.promoteExactIds || []);
}

function indexDeepArtifacts(hotelDir) {
  const deepRoot = path.join(hotelDir, "artifacts", "deep");
  const files = listFilesRecursive(deepRoot, (fp) => fp.endsWith(".json"));
  const byWorkflowOrg = new Map();
  for (const fp of files) {
    const rel = path.relative(hotelDir, fp).replace(/\\/g, "/");
    const parts = rel.split("/");
    // artifacts/deep/<workflow>/<slug>.json
    const workflowId = parts[2] || null;
    const doc = readJson(fp, {});
    const org = normalizeOrgKey(
      doc.organizationName ||
        doc.organisationName ||
        doc.organiser ||
        doc.org ||
        path.basename(fp, ".json").replace(/-/g, " "),
    );
    if (!workflowId || !org) continue;
    const key = `${workflowId}|${org}`;
    byWorkflowOrg.set(key, {
      path: rel,
      costUsd: doc.costUsd ?? doc.researchCostUsd ?? null,
      opportunityId: doc.opportunityId || doc.id || null,
      doc,
    });
  }
  return byWorkflowOrg;
}

function indexStagedOpportunities(hotelDir) {
  const staged = readJson(path.join(hotelDir, "03-STAGED_OPPORTUNITIES.json"), {
    opportunities: [],
  });
  const list = staged.opportunities || staged || [];
  const byOrgWorkflow = new Map();
  const byId = new Map();
  for (const o of list) {
    if (!o || typeof o !== "object") continue;
    const id = o.id || o.opportunityId;
    if (id) byId.set(id, o);
    const wf =
      o.demandSegmentId || o.researchSegment || o.workflowId || o.segmentId || "";
    const org = orgKeyFromOpp(o);
    if (org) {
      const k = `${wf}|${org}`;
      if (!byOrgWorkflow.has(k)) byOrgWorkflow.set(k, []);
      byOrgWorkflow.get(k).push(o);
      // also index without workflow for fuzzy match
      const k2 = `|${org}`;
      if (!byOrgWorkflow.has(k2)) byOrgWorkflow.set(k2, []);
      byOrgWorkflow.get(k2).push(o);
    }
  }
  return { list, byOrgWorkflow, byId };
}

function matchStaged(index, workflowId, organizationName) {
  const org = normalizeOrgKey(organizationName);
  if (!org) return null;
  const exact = index.byOrgWorkflow.get(`${workflowId}|${org}`);
  if (exact?.length) return exact[0];
  const fuzzy = index.byOrgWorkflow.get(`|${org}`);
  if (fuzzy?.length) return fuzzy[0];
  // Contained org-family match (e.g. "Clio" vs "Clio Inc")
  for (const [k, arr] of index.byOrgWorkflow.entries()) {
    if (!k.startsWith("|") && !k.startsWith(`${workflowId}|`)) continue;
    const other = k.split("|").slice(1).join("|");
    if (!other) continue;
    if (other === org || other.includes(org) || org.includes(other)) {
      if (arr?.length) return arr[0];
    }
  }
  return null;
}

function matchDeep(deepIndex, workflowId, organizationName) {
  const org = normalizeOrgKey(organizationName);
  return (
    deepIndex.get(`${workflowId}|${org}`) ||
    [...deepIndex.entries()].find(([k]) => k.startsWith(`${workflowId}|`) && (k.includes(org) || org.includes(k.split("|")[1] || "")))?.[1] ||
    null
  );
}

/**
 * Backfill one hotel's discovery passes into ledger records (in memory + persist).
 */
export function backfillHotelDiscoveryLedger(hotel, opts = {}) {
  const hotelDir = path.join(FOUR_HOTEL_LIVE_ROOT, hotel.key);
  const campaignId = opts.campaignId || FOUR_HOTEL_CAMPAIGN_ID;
  const promoteIds = opts.promoteIds || loadPromotionSet();
  const deepIndex = indexDeepArtifacts(hotelDir);
  const stagedIndex = indexStagedOpportunities(hotelDir);
  const discoveryRoot = path.join(hotelDir, "artifacts", "discovery");
  const passFiles = listFilesRecursive(discoveryRoot, (_fp, name) =>
    /^pass-\d+\.json$/i.test(name),
  );

  const seenOrgWorkflow = new Map(); // first discoveryId kept
  const records = [];
  const passSummaries = [];

  for (const fp of passFiles.sort()) {
    const doc = readJson(fp, { candidates: [], entry: {}, run: {} });
    const entry = doc.entry || {};
    const workflowId = entry.workflowId || path.basename(path.dirname(fp));
    const pass =
      entry.pass != null
        ? Number(entry.pass)
        : Number(String(path.basename(fp)).replace(/\D/g, "")) || 1;
    const exaRunId = entry.runId || doc.run?.runId || null;
    const at = entry.at || null;
    const cost = entry.costUsd ?? null;
    const powershell = entry.powershell || null;
    const candidates = Array.isArray(doc.candidates) ? doc.candidates : [];
    const artifactPath = path.relative(ROOT, fp).replace(/\\/g, "/");

    passSummaries.push({
      hotelId: hotel.hotelId,
      workflowId,
      pass,
      exaRunId,
      candidateCount: candidates.length,
      uniqueEvidenceUrls: [
        ...new Set(candidates.flatMap((c) => c.evidenceUrls || [])),
      ].length,
      artifactPath,
      ok: entry.ok !== false && doc.run?.ok !== false,
      costUsd: cost,
      at,
    });

    for (const c of candidates) {
      let rec = createDiscoveryLedgerRecord({
        hotelId: hotel.hotelId,
        hotelKey: hotel.key,
        hotelName: hotel.hotelName,
        workflowId,
        powershell,
        pass,
        exaRunId,
        queryTimestamp: at,
        queryLanguage: null,
        queryText: null,
        candidate: c,
        discoveryCostUsd:
          cost == null ? null : Number(cost) / Math.max(1, candidates.length),
        artifactPath,
        researchRunId: FOUR_HOTEL_CAMPAIGN_ID,
        provenanceAvailable: true,
        provenanceGaps: [
          "raw_exa_agent_text_not_persisted",
          "raw_exa_grounding_not_persisted",
          "query_text_not_persisted_in_pass_artifact",
          "query_language_not_persisted_in_pass_artifact",
        ],
      });

      const orgKey = normalizeOrgKey(rec.organizationName);
      const dedupeKey = `${workflowId}|${orgKey}`;
      if (orgKey && seenOrgWorkflow.has(dedupeKey)) {
        const primaryId = seenOrgWorkflow.get(dedupeKey);
        rec = applyDispositionToRecord(
          rec,
          {
            code: DISPOSITION_REASON_CODE.DUPLICATE_SAME_ORG_CROSS_PASS,
            stage: "deduplication",
            evidence: [`primaryDiscoveryId=${primaryId}`, `pass=${pass}`],
            nextAction: "Use primary discovery row; do not treat as new demand",
            decidedAt: at || new Date().toISOString(),
          },
          RESEARCH_STATUS.DUPLICATE,
        );
        records.push(rec);
        continue;
      }
      if (orgKey) seenOrgWorkflow.set(dedupeKey, rec.discoveryId);

      // Surviving unique → screened eligible by default (pass artifact already extracted)
      rec = applyDispositionToRecord(
        {
          ...rec,
          researchStatus: RESEARCH_STATUS.SCREENED_ELIGIBLE,
        },
        {
          code: null,
          stage: "commercial_screening",
          explanation:
            "Parsed from Exa discovery pass; no separate screening reject artifact was retained for this row.",
          decidedAt: at || new Date().toISOString(),
        },
        RESEARCH_STATUS.SCREENED_ELIGIBLE,
      );
      // clear null-code disposition until final
      rec.disposition = null;

      const deep = matchDeep(deepIndex, workflowId, rec.organizationName);
      const staged = matchStaged(
        stagedIndex,
        workflowId,
        rec.organizationName,
      );

      if (deep) {
        rec.deepArtifactPath = deep.path;
        if (deep.costUsd != null) rec.deepResearchCostUsd = Number(deep.costUsd);
        if (deep.opportunityId) rec.opportunityId = deep.opportunityId;
      }

      if (staged) {
        rec.staged = true;
        rec.opportunityId = staged.id || staged.opportunityId || rec.opportunityId;
        const disp = acceptanceFields(staged);
        const statusFromDisp = researchStatusFromAcceptanceDisposition(disp);
        if (statusFromDisp === RESEARCH_STATUS.SALES_READY_INTERNAL) {
          const awaiting = promoteIds.has(rec.opportunityId);
          const status = awaiting
            ? RESEARCH_STATUS.AWAITING_FOUNDER_PROMOTION
            : RESEARCH_STATUS.SALES_READY_INTERNAL;
          rec.awaitingFounderPromotion = awaiting;
          rec.customerFacing = false;
          rec = applyDispositionToRecord(
            rec,
            {
              code: awaiting
                ? DISPOSITION_REASON_CODE.AWAITING_FOUNDER_PROMOTION
                : DISPOSITION_REASON_CODE.STAGED_INTERNAL_ONLY,
              stage: "customer_facing_acceptance",
              evidence: [
                `opportunityId=${rec.opportunityId}`,
                `acceptance=${disp}`,
              ],
              nextAction: awaiting
                ? "Await founder selective promotion GO"
                : "Remain internal until founder promotion",
              supportingOpportunityId: rec.opportunityId,
              decidedAt: at || new Date().toISOString(),
            },
            status,
          );
        } else if (statusFromDisp === RESEARCH_STATUS.RESEARCH_CONTINUATION) {
          rec = applyDispositionToRecord(
            rec,
            {
              code: DISPOSITION_REASON_CODE.RESEARCH_CONTINUATION_GAPS,
              stage: "qualification",
              evidence: [`opportunityId=${rec.opportunityId}`, `acceptance=${disp}`],
              nextAction: "Close evidence gaps before Sales Ready reconsideration",
              supportingOpportunityId: rec.opportunityId,
              decidedAt: at || new Date().toISOString(),
            },
            RESEARCH_STATUS.RESEARCH_CONTINUATION,
          );
        } else if (statusFromDisp === RESEARCH_STATUS.WATCH) {
          rec = applyDispositionToRecord(
            rec,
            {
              code: DISPOSITION_REASON_CODE.WATCH_INSUFFICIENT_EVIDENCE,
              stage: "qualification",
              evidence: [`opportunityId=${rec.opportunityId}`, `acceptance=${disp}`],
              nextAction: "Monitor; do not promote",
              supportingOpportunityId: rec.opportunityId,
              decidedAt: at || new Date().toISOString(),
            },
            RESEARCH_STATUS.WATCH,
          );
        } else if (statusFromDisp === RESEARCH_STATUS.REJECTED) {
          rec = applyDispositionToRecord(
            rec,
            {
              code: DISPOSITION_REASON_CODE.REJECTED_COMMERCIAL,
              stage: "qualification",
              evidence: [`opportunityId=${rec.opportunityId}`, `acceptance=${disp}`],
              nextAction: "Do not pursue unless new evidence arrives",
              supportingOpportunityId: rec.opportunityId,
              decidedAt: at || new Date().toISOString(),
            },
            RESEARCH_STATUS.REJECTED,
          );
        } else if (deep) {
          // Deep done but disposition label unclear — incomplete, not rejected
          rec = applyDispositionToRecord(
            rec,
            {
              code: DISPOSITION_REASON_CODE.RESEARCH_INCOMPLETE,
              stage: "qualification",
              evidence: [
                `deepArtifact=${deep.path}`,
                staged ? `stagedId=${rec.opportunityId}` : "not_staged",
              ],
              nextAction: "Review staged opportunity fields / rematerialize disposition",
              decidedAt: at || new Date().toISOString(),
            },
            RESEARCH_STATUS.DEEP_RESEARCH_INCOMPLETE,
          );
        }
      } else if (deep) {
        rec = applyDispositionToRecord(
          rec,
          {
            code: DISPOSITION_REASON_CODE.RESEARCH_INCOMPLETE,
            stage: "deep_research",
            evidence: [`deepArtifact=${deep.path}`, "no_staged_opportunity_match"],
            nextAction: "Map deep artifact to staged opportunity or re-run acceptance",
            decidedAt: at || new Date().toISOString(),
          },
          RESEARCH_STATUS.DEEP_RESEARCH_INCOMPLETE,
        );
      } else {
        // Undeepened — NOT commercially rejected
        rec = applyDispositionToRecord(
          rec,
          {
            code: DISPOSITION_REASON_CODE.DEEP_RESEARCH_NOT_STARTED,
            stage: "deep_research_allocation",
            evidence: [
              "No deep artifact matched by organization+workflow",
              "Likely adaptive allocation cap, budget, or priority deferral — not a commercial reject",
            ],
            nextAction:
              "If commercially promising, queue for deep research under a new authorized budget",
            decidedAt: at || new Date().toISOString(),
          },
          RESEARCH_STATUS.QUEUED_FOR_DEEP_RESEARCH,
        );
      }

      records.push(rec);
    }
  }

  const write = upsertDiscoveryCandidates(hotel.hotelId, records, {
    campaignId,
    hotelKey: hotel.key,
    hotelName: hotel.hotelName,
  });

  return {
    hotel,
    passSummaries,
    records,
    write,
    funnel: aggregateResearchFunnel(records),
    stagedCount: stagedIndex.list.length,
    deepCount: deepIndex.size,
  };
}

export function backfillFourHotelCampaign(opts = {}) {
  const promoteIds = loadPromotionSet();
  const hotels = [];
  for (const hotel of FOUR_HOTELS) {
    hotels.push(backfillHotelDiscoveryLedger(hotel, { ...opts, promoteIds }));
  }
  const allRecords = hotels.flatMap((h) => h.records);
  return {
    campaignId: FOUR_HOTEL_CAMPAIGN_ID,
    hotels,
    allRecords,
    funnel: aggregateResearchFunnel(allRecords),
    promoteExactIdCount: promoteIds.size,
  };
}

/**
 * Row-by-row reconciliation for Corporate Celebrations voco pass-1 (8/9).
 */
export function reconcileCorporateCelebrationsVocoPass1(backfillHotelResult = null) {
  const hotel = FOUR_HOTELS.find((h) => h.key === "voco-cancun-zona-hotelera");
  const fp = path.join(
    FOUR_HOTEL_LIVE_ROOT,
    hotel.key,
    "artifacts/discovery/corporate_celebrations_private_events/pass-1.json",
  );
  const doc = readJson(fp, null);
  if (!doc) {
    return {
      ok: false,
      provenanceGap: "PASS1_ARTIFACT_MISSING",
      path: path.relative(ROOT, fp).replace(/\\/g, "/"),
    };
  }

  const hotelResult =
    backfillHotelResult ||
    backfillHotelDiscoveryLedger(hotel, { promoteIds: loadPromotionSet() });

  const rows = [];
  const candidates = doc.candidates || [];
  const evidenceUrls = [
    ...new Set(candidates.flatMap((c) => c.evidenceUrls || [])),
  ];

  for (let i = 0; i < candidates.length; i++) {
    const c = candidates[i];
    const org = text(c.organizationName);
    const match = (hotelResult.records || []).find(
      (r) =>
        r.workflowId === "corporate_celebrations_private_events" &&
        r.discoveryPass === 1 &&
        normalizeOrgKey(r.organizationName) === normalizeOrgKey(org),
    );
    rows.push({
      index: i + 1,
      organizationName: org,
      programmeName: c.programmeName || null,
      evidenceUrls: c.evidenceUrls || [],
      parsed: Boolean(org),
      persisted: Boolean(match),
      discoveryId: match?.discoveryId || null,
      deduplicated: match?.researchStatus === RESEARCH_STATUS.DUPLICATE,
      excluded: match?.researchStatus === RESEARCH_STATUS.SCREENED_EXCLUDED,
      deepResearched: Boolean(match?.deepArtifactPath),
      staged: Boolean(match?.staged),
      opportunityId: match?.opportunityId || null,
      customerFacing: Boolean(match?.customerFacing),
      researchStatus: match?.researchStatus || null,
      dispositionCode: match?.disposition?.code || null,
      dispositionExplanation: match?.disposition?.explanation || null,
      provenanceGaps: match?.provenance?.gaps || [
        "raw_exa_agent_text_not_persisted",
      ],
    });
  }

  return {
    ok: true,
    hotelId: hotel.hotelId,
    hotelName: hotel.hotelName,
    workflowId: "corporate_celebrations_private_events",
    powershell: "Invoke-GdiCorporateCelebrations.ps1",
    artifactPath: path.relative(ROOT, fp).replace(/\\/g, "/"),
    entry: doc.entry || null,
    exaRunId: doc.entry?.runId || doc.run?.runId || null,
    candidateCount: candidates.length,
    uniqueEvidenceUrlCount: evidenceUrls.length,
    note: "Exa UI may show N sources; artifact retains per-candidate evidenceUrls (here 8 candidates / 9 unique URLs).",
    rawProviderEnvelopeAvailable: false,
    provenanceGap:
      "Full Exa Agent text/grounding/HTTP body was not persisted in four-hotel discovery pass JSON — only runId + structured candidates.",
    rows,
  };
}
