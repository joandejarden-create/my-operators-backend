/**
 * GET /api/support/operator-fit-data-readiness
 * Internal/admin only — serves last read-only readiness payload from reports/.
 * Does not call Airtable write APIs.
 */
import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PAYLOAD_PATH = join(
  __dirname,
  "..",
  "reports",
  "operator-fit-data-readiness-ui-payload.json"
);

export function getOperatorFitDataReadinessHandler(_req, res) {
  try {
    if (!existsSync(PAYLOAD_PATH)) {
      return res.status(404).json({
        ok: false,
        success: false,
        error:
          "Readiness payload not found. Run: npm run operator-fit-data-readiness",
      });
    }
    const raw = readFileSync(PAYLOAD_PATH, "utf8");
    const payload = JSON.parse(raw);
    return res.status(200).json({
      ok: true,
      success: true,
      mode: "read-only",
      writeMethods: [],
      ...payload,
    });
  } catch (err) {
    console.error("[operator-fit-data-readiness-api]", err?.message || err);
    return res.status(500).json({
      ok: false,
      success: false,
      error: "Failed to load readiness payload",
    });
  }
}
