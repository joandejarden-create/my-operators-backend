import {
  loadNewBaseOperatorBundle,
  buildPrefillObjectFromNewBaseRows,
} from "./lib/operator-setup-new-base-read.js";
import { buildOperatorCensusFootprint } from "../lib/hotel-census/build-operator-census-footprint.js";

/**
 * GET /api/intake/third-party-operators/:recordId/census-footprint
 * Hotel Census portfolio rollup for an operator Master record (read-only).
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

    const censusFootprint = await buildOperatorCensusFootprint({
      masterId: recordId,
      prefill,
    });

    return res.json({
      success: true,
      recordId,
      censusFootprint,
    });
  } catch (e) {
    const status = e?.statusCode && Number(e.statusCode) >= 400 ? Number(e.statusCode) : 500;
    return res.status(status).json({
      success: false,
      error: e?.message || String(e),
    });
  }
}
