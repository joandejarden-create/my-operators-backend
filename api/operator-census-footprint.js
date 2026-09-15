import {
  loadNewBaseOperatorBundle,
  buildPrefillObjectFromNewBaseRows,
} from "./lib/operator-setup-new-base-read.js";
import { buildOperatorCensusFootprint } from "../lib/hotel-census/build-operator-census-footprint.js";
import { buildOperatorHpcCensusFootprint } from "../lib/hotel-census/operator-explorer-hpc-metrics.js";
import {
  shouldUseHpcOperatorExplorerMetrics,
} from "../lib/hotel-census/brand-presence-hpc-request.js";
import { operatorExplorerCensusReadCounters } from "../lib/hotel-census/operator-explorer-hpc-metrics.js";

/**
 * GET /api/intake/third-party-operators/:recordId/census-footprint
 * Hotel Census portfolio rollup for an operator Master record (read-only).
 * HPC when OPERATOR_EXPLORER_HPC_V2=1 (+ Brand Presence HPC).
 */
export default async function getOperatorCensusFootprint(req, res) {
  try {
    const recordId = String((req.params && req.params.recordId) || "").trim();
    if (!recordId) {
      return res.status(400).json({ success: false, error: "Missing recordId" });
    }

    let prefill = {};
    const bundle = await loadNewBaseOperatorBundle(recordId);
    if (bundle?.master) {
      prefill = buildPrefillObjectFromNewBaseRows(
        bundle.master,
        bundle.profile,
        bundle.platform,
        bundle.commercial,
        bundle.governance
      );
    }

    const useHpc = shouldUseHpcOperatorExplorerMetrics(req);
    let censusFootprint;
    if (useHpc) {
      censusFootprint = await buildOperatorHpcCensusFootprint({
        masterId: recordId,
        prefill,
      });
    } else {
      operatorExplorerCensusReadCounters.legacy += 1;
      censusFootprint = await buildOperatorCensusFootprint({
        masterId: recordId,
        prefill,
      });
    }

    return res.json({
      success: true,
      recordId,
      censusFootprint,
      censusSource: useHpc ? "hpc" : "legacy",
    });
  } catch (e) {
    const status = e?.statusCode && Number(e.statusCode) >= 400 ? Number(e.statusCode) : 500;
    return res.status(status).json({
      success: false,
      error: e?.message || String(e),
    });
  }
}
