/**
 * Server-side Apify HTTP client (local token only).
 * Never logs tokens. Prefer usageTotalUsd from finished run payloads.
 */

import { DEFAULT_TRIPADVISOR_ACTOR } from "../apify-usage/constants.js";

export function getApifyToken(env = process.env) {
  return String(env.APIFY_TOKEN || env.APIFY_API_TOKEN || "").trim();
}

/**
 * @param {object} opts
 * @param {string} [opts.actorId] act id or username~name
 * @param {object} opts.input
 * @param {number} [opts.waitSecs]
 * @param {number} [opts.memoryMbytes]
 * @param {number} [opts.maxTotalChargeUsd]
 * @param {object} [opts.env]
 */
export async function runApifyActor(opts = {}) {
  const env = opts.env || process.env;
  const token = getApifyToken(env);
  if (!token) {
    throw new Error("APIFY_TOKEN_or_APIFY_API_TOKEN_missing");
  }

  const actorRef =
    opts.actorRef ||
    opts.actorId ||
    `${DEFAULT_TRIPADVISOR_ACTOR.actor_name.replace("/", "~")}`;
  const waitSecs = Math.min(Number(opts.waitSecs ?? 120) || 120, 300);
  const memory = opts.memoryMbytes || 2048;
  const maxCharge = opts.maxTotalChargeUsd ?? null;

  const startUrl = new URL(
    `https://api.apify.com/v2/acts/${encodeURIComponent(actorRef)}/runs`
  );
  startUrl.searchParams.set("waitForFinish", String(waitSecs));
  if (memory) startUrl.searchParams.set("memory", String(memory));
  if (maxCharge != null) {
    startUrl.searchParams.set("maxTotalChargeUsd", String(maxCharge));
  }

  async function readJsonResponse(res, label) {
    const text = await res.text();
    const trimmed = String(text || "").trim();
    if (!trimmed) {
      throw new Error(`${label}:empty_body:${res.status}`);
    }
    if (trimmed.startsWith("<") || /<!DOCTYPE|<html/i.test(trimmed.slice(0, 64))) {
      throw new Error(
        `${label}:non_json_html:${res.status}:${trimmed.slice(0, 120).replace(/\s+/g, " ")}`
      );
    }
    try {
      return JSON.parse(trimmed);
    } catch (err) {
      throw new Error(
        `${label}:invalid_json:${res.status}:${err.message}:${trimmed.slice(0, 120).replace(/\s+/g, " ")}`
      );
    }
  }

  const startRes = await fetch(startUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(opts.input || {}),
  });
  const startJson = await readJsonResponse(startRes, "apify_run_start");
  if (!startRes.ok) {
    throw new Error(
      `apify_run_start_failed:${startRes.status}:${JSON.stringify(startJson).slice(0, 300)}`
    );
  }

  let run = startJson.data;
  // Poll if not finished
  let guard = 0;
  while (
    run &&
    !["SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"].includes(run.status) &&
    guard < 60
  ) {
    guard += 1;
    await new Promise((r) => setTimeout(r, 5000));
    const pollRes = await fetch(
      `https://api.apify.com/v2/actor-runs/${run.id}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const pollJson = await readJsonResponse(pollRes, "apify_run_poll");
    if (!pollRes.ok) {
      throw new Error(`apify_run_poll_failed:${pollRes.status}`);
    }
    run = pollJson.data;
  }

  // Fresh run payload for usageTotalUsd
  const detailRes = await fetch(
    `https://api.apify.com/v2/actor-runs/${run.id}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const detailJson = await readJsonResponse(detailRes, "apify_run_detail");
  const detail = detailJson.data || run;

  const datasetId = detail.defaultDatasetId;
  let items = [];
  if (datasetId) {
    const dsRes = await fetch(
      `https://api.apify.com/v2/datasets/${datasetId}/items?format=json&clean=true`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    if (dsRes.ok) {
      const dsJson = await readJsonResponse(dsRes, "apify_dataset_items");
      items = Array.isArray(dsJson) ? dsJson : [];
    }
  }

  return {
    run: detail,
    run_id: detail.id,
    status: detail.status,
    dataset_id: datasetId,
    usage_total_usd:
      detail.usageTotalUsd != null ? Number(detail.usageTotalUsd) : null,
    items,
  };
}
