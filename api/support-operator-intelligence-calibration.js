/**
 * GET /api/support/operator-intelligence-calibration
 * Internal/admin — serves local calibration UI payload (read-only).
 */
import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PAYLOAD = join(
  __dirname,
  "..",
  "reports",
  "operator-intelligence-calibration-ui-payload.json"
);

export function getOperatorIntelligenceCalibrationHandler(_req, res) {
  try {
    if (!existsSync(PAYLOAD)) {
      return res.status(404).json({
        ok: false,
        error: "Run npm run operator-intelligence-calibration-evaluate",
      });
    }
    const payload = JSON.parse(readFileSync(PAYLOAD, "utf8"));
    return res.status(200).json({
      ok: true,
      success: true,
      mode: "read-only",
      writeMethods: [],
      ...payload,
    });
  } catch (err) {
    console.error("[oi-calibration-api]", err?.message || err);
    return res.status(500).json({ ok: false, error: "Failed to load calibration payload" });
  }
}
