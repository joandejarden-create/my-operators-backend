#!/usr/bin/env node
/**
 * HIF V6 Track A Wave 2 — free public recorded-MA / court exhibit probes.
 * No paid PACER. Measures actual retrieval yield. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-wave2");
const RUN = process.argv.includes("--run");
const UA =
  process.env.SEC_EDGAR_USER_AGENT ||
  process.env.EDGAR_USER_AGENT ||
  "DealalityHIFResearch/6.0 (research@dealality.com)";

const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};

async function fetchText(url) {
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": UA, Accept: "text/html,application/pdf;q=0.9,*/*;q=0.8" },
      redirect: "follow",
      signal: AbortSignal.timeout(30000),
    });
    const text = await res.text();
    return { ok: res.ok, status: res.status, text, bytes: text.length };
  } catch (err) {
    return { ok: false, status: null, text: "", bytes: 0, error: String(err?.message || err) };
  }
}

function countHits(text, re) {
  return (String(text || "").match(re) || []).length;
}

async function main() {
  const probes = [
    {
      family: "COURT_LISTENER_RECAP",
      url: "https://www.courtlistener.com/?q=%22management+agreement%22+hotel&type=o&order_by=score+desc",
      note: "Free RECAP/opinions search — not PACER paid",
    },
    {
      family: "COURT_LISTENER_RECAP",
      url: "https://www.courtlistener.com/?q=%22franchise+agreement%22+hotel+exhibit&type=r&order_by=score+desc",
      note: "RECAP docket search",
    },
    {
      family: "GOVINFO_BANKRUPTCY",
      url: "https://www.govinfo.gov/app/search/%7B%22query%22%3A%22hotel%20management%20agreement%22%2C%22offset%22%3A0%7D",
      note: "GovInfo free search",
    },
    {
      family: "SEC_EDGAR_EX10_HMA",
      url: "https://efts.sec.gov/LATEST/search-index?q=%22hotel%20management%20agreement%22&forms=EX-10&dateRange=custom&startdt=2020-01-01&enddt=2026-11-21",
      note: "Free EDGAR full-text EX-10 HMA",
    },
    {
      family: "SEC_EDGAR_MEMORANDUM_MA",
      url: "https://efts.sec.gov/LATEST/search-index?q=%22memorandum%20of%20management%20agreement%22&dateRange=custom&startdt=2015-01-01&enddt=2026-11-21",
      note: "Free EDGAR recorded-MA style phrase",
    },
  ];

  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, probes: probes.length }, null, 2));
    return;
  }

  const results = [];
  let recordedMaFound = 0;
  let courtExhibitsFound = 0;
  const candidates = [];

  for (const p of probes) {
    const res = await fetchText(p.url);
    const maHits = countHits(res.text, /management\s+agreement|memorandum\s+of\s+management/gi);
    const franchiseHits = countHits(res.text, /franchise\s+agreement/gi);
    const hotelHits = countHits(res.text, /\bhotel\b|\bresort\b/gi);
    const row = {
      ...p,
      ok: res.ok,
      status: res.status,
      bytes: res.bytes,
      maHits,
      franchiseHits,
      hotelHits,
      verifiedContractFacts: 0,
      note2:
        res.ok && (maHits > 0 || franchiseHits > 0)
          ? "Search surface returned lexicon hits — document-level verification still required"
          : res.ok
            ? "Retrieved but low lexicon yield"
            : "Retrieval failed",
    };
    if (res.ok && /COURT|GOVINFO/i.test(p.family) && maHits > 0) {
      courtExhibitsFound += 0; // do not inflate — zero verified exhibits until docs pulled
      candidates.push({
        family: p.family,
        url: p.url,
        status: "SEARCH_HIT_CANDIDATE_NOT_VERIFIED",
      });
    }
    if (res.ok && /MEMORANDUM/i.test(p.family) && maHits > 0) {
      recordedMaFound += 0;
      candidates.push({
        family: p.family,
        url: p.url,
        status: "SEARCH_HIT_CANDIDATE_NOT_VERIFIED",
      });
    }
    results.push(row);
  }

  writeJson(path.join(OUT, "RECORDED_MA_COURT_PROBES.json"), {
    results,
    candidates,
    recordedMaVerified: recordedMaFound,
    courtExhibitsVerified: courtExhibitsFound,
    note: "Zero verified recorded-MA / court exhibits this wave — search surfaces probed only",
  });

  const metricsPath = path.join(OUT, "TRACK_A_WAVE2_METRICS.json");
  if (fs.existsSync(metricsPath)) {
    const metrics = JSON.parse(fs.readFileSync(metricsPath, "utf8"));
    metrics.recordedMaFound = recordedMaFound;
    metrics.courtExhibitsFound = courtExhibitsFound;
    metrics.recordedMaCourtSearchCandidates = candidates.length;
    metrics.recordedMaCourtProbesOk = results.filter((r) => r.ok).length;
    fs.writeFileSync(metricsPath, JSON.stringify(metrics, null, 2), "utf8");
  }

  console.log(
    JSON.stringify(
      {
        ok: true,
        probesOk: results.filter((r) => r.ok).length,
        candidates: candidates.length,
        recordedMaVerified: recordedMaFound,
        courtExhibitsVerified: courtExhibitsFound,
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
