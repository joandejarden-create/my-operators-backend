/**
 * GET /api/operator-match/scoring-config
 * Read-only operator match weights UI config (bands + aggregation metadata).
 */
import { getOperatorMatchScoreBrowserPayload } from "../lib/operator-alignment-score-ui-utils.js";

export function getOperatorMatchScoringConfigHandler(_req, res) {
  return res.status(200).json({
    ok: true,
    success: true,
    ...getOperatorMatchScoreBrowserPayload(),
  });
}

export function getOperatorMatchScoringConfigBrowserScript(_req, res) {
  const payload = getOperatorMatchScoreBrowserPayload();
  const body =
    "/* Generated from lib/operator-alignment-scoring-weight-config.js — do not edit */\n" +
    "window.DcOperatorMatchScoreConfig=" +
    JSON.stringify(payload) +
    ";\n";
  res.setHeader("Content-Type", "application/javascript; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache");
  return res.status(200).send(body);
}
