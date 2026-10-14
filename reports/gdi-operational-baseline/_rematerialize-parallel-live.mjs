/**
 * Rematerialize known Parallel live run (no resubmit).
 * Run: node reports/gdi-operational-baseline/_rematerialize-parallel-live.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createParallelClient } from "../../lib/hotel-intelligence/research/providers/parallel/client.js";
import { mapLabCardToDiscoveryCandidate } from "../../lib/group-demand-intelligence/workflow/parallel-discovery-adapter-v1.js";
import { assessIndependentHotelFit } from "../../lib/group-demand-intelligence/workflow/independent-hotel-fit-v1.js";
import { buildDraftReviewPackage } from "../../lib/group-demand-intelligence/workflow/draft-review-package-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "san-diego-parallel-rematerialized");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const PROVIDER_RUN_ID = "trun_567f3335d0c140ad8e87120211c6f61f";
const PROBE_RUN_ID = "trun_567f3335d0c140ad9269500c06dc2b38";

fs.mkdirSync(OUT, { recursive: true });

// Re-export helper if not exported — local parse fallback
async function loadCandidates() {
  const client = createParallelClient();
  const result = await client.getRunResult(PROVIDER_RUN_ID, { timeoutMs: 60000 });
  const structured = result.structured || {};
  let output = structured.output || structured;
  if (output?.content?.output) output = output.content.output;
  if (typeof output === "string") {
    try {
      output = JSON.parse(output);
    } catch {
      output = {};
    }
  }
  const list = Array.isArray(output?.candidates) ? output.candidates : [];
  const candidates = list.map((raw, i) => {
    const c = mapLabCardToDiscoveryCandidate(
      {
        cardId: `par_live_${i}`,
        account: raw.organization || raw.organizationName,
        timing: {
          start: raw.startDate,
          end: raw.endDate,
          raw: raw.eventName,
        },
        whyWyndhamFits: raw.whyRelevantToHotel,
        sourcingStatus: raw.sourcingStatus,
        recommendedFirstAction: raw.recommendedAction,
        evidenceLinks: [
          raw.officialSource,
          ...(raw.supportingSources || []),
        ].filter(Boolean),
        materialLimitations: raw.materialLimitations,
      },
      HOTEL_ID
    );
    const fit = assessIndependentHotelFit(
      {
        ...c,
        location: raw.location,
        title: raw.eventName,
        organizationName: raw.organization,
      },
      HOTEL_ID
    );
    return {
      ...c,
      location: raw.location || null,
      rawParallel: raw,
      independentHotelFit: fit,
      publicationBlocked: true,
      gdiClassifierResult: "NOT_AUTO_QUALIFIED",
    };
  });
  return { result, candidates };
}

const { result, candidates } = await loadCandidates();

const nonPublication = candidates.map((c) => {
  const fit = c.independentHotelFit?.outcome || null;
  const loc = String(c.location || c.rawParallel?.location || "").toLowerCase();
  const reasons = [];
  if (/bethesda|maryland|dmv|nih|pooks hill|rockville|chevy chase/.test(loc)) {
    reasons.push("out_of_territory_wrong_market_vs_san_diego_bayside");
  }
  if (fit === "OUT_OF_TERRITORY") reasons.push("independent_hotel_fit_out_of_territory");
  if (fit === "UNSUPPORTED_ORG") reasons.push("unsupported_org_for_this_hotel");
  if (!c.officialSource && !(c.evidenceLinks || []).length) {
    reasons.push("missing_supporting_source");
  }
  const contact = c.primaryContact || {};
  if (!contact.email && !contact.phone) {
    reasons.push("no_usable_contact_route_yet");
  }
  reasons.push("human_review_required_not_auto_published");
  reasons.push("zero_confirmed_buying_does_not_block_credibility_assessment");
  return {
    candidateId: c.candidateId,
    organizationName: c.organizationName,
    title: c.title,
    location: c.location || c.rawParallel?.location || null,
    fitOutcome: fit,
    publicationStatus: "not_published",
    reasons,
    note:
      "Live Parallel returned this row; geography/fit may exclude San Diego publication",
  };
});

const inTerritory = candidates.filter(
  (c) =>
    c.independentHotelFit?.outcome !== "OUT_OF_TERRITORY" &&
    !/bethesda|maryland|dmv|nih|pooks hill|rockville|chevy chase/i.test(
      String(c.location || c.rawParallel?.location || "")
    )
);

const programDraftsPath = path.join(
  __dirname,
  "san-diego-parallel-live-v2/runs/gdi_workflow_recnPB3Q_2026-10-14T22-05-42-720Z_234335/reuse_prior_intelligence/programs-occasion-drafts.json"
);
const programDrafts = fs.existsSync(programDraftsPath)
  ? JSON.parse(fs.readFileSync(programDraftsPath, "utf8"))
  : { drafts: [] };

const draftDir = path.join(OUT, "draft_review_package");
const draft = buildDraftReviewPackage({
  hotelId: HOTEL_ID,
  hotelKey: "sd",
  hotelLabel: "Wyndham San Diego Bayside",
  opportunities: [],
  peDraftOpportunities: programDrafts.drafts || [],
  outDir: draftDir,
});

fs.writeFileSync(
  path.join(OUT, "parallel-live-rematerialized.json"),
  JSON.stringify(
    {
      hotelId: HOTEL_ID,
      providerRunId: PROVIDER_RUN_ID,
      probeRunId: PROBE_RUN_ID,
      rematerializedAt: new Date().toISOString(),
      paidResubmit: false,
      candidateCount: candidates.length,
      inTerritoryCount: inTerritory.length,
      outOfTerritoryOrWrongMarket: candidates.length - inTerritory.length,
      candidates,
      nonPublicationReasons: nonPublication,
      costFromProvider: result.structured?.cost || result.structured?.usage || null,
      note: "Parsed known live run — no duplicate Parallel submit. Bethesda-market rows are not San Diego prospects.",
    },
    null,
    2
  ) + "\n"
);

fs.writeFileSync(
  path.join(OUT, "san-diego-non-publication-reasons.json"),
  JSON.stringify(
    {
      hotelId: HOTEL_ID,
      asOf: new Date().toISOString(),
      rows: nonPublication,
      programDraftCount: (programDrafts.drafts || []).length,
      publicationPolicy:
        "Credible account/travel occasion + hotel fit + evidence + usable contact route; confirmed sourcing not required. Zero confirmed buying ≠ zero credible prospects.",
    },
    null,
    2
  ) + "\n"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      out: OUT,
      providerRunId: PROVIDER_RUN_ID,
      candidates: candidates.length,
      inTerritory: inTerritory.length,
      programDrafts: (programDrafts.drafts || []).length,
      draftRegister: draft.registerCount,
      pendingHumanReviews: draft.pendingHumanReviews,
    },
    null,
    2
  )
);
