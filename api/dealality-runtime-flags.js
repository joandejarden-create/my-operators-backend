/**
 * P8.7 — public runtime flags for Radar client production default.
 * No secrets. Scout does not consume this for bulk census.
 */
import { getDealalityRuntimeFlags } from "../lib/hotel-census/brand-presence-hpc-request.js";

export async function getDealalityRuntimeFlagsHandler(req, res) {
  try {
    res.setHeader("Cache-Control", "no-store");
    return res.status(200).json(getDealalityRuntimeFlags(process.env));
  } catch (err) {
    console.error("[dealality-runtime-flags]", err?.message || err);
    return res.status(500).json({ error: "runtime_flags_failed" });
  }
}
