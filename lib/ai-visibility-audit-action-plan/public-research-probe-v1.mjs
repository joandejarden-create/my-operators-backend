/**
 * Safe public research probe for AI Visibility Audit modules M02–M04.
 * No paid providers. No Airtable writes. No ADP remeasurement.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

async function fetchText(url, opts = {}) {
  const started = Date.now();
  try {
    const res = await fetch(url, {
      method: opts.method || "GET",
      redirect: "follow",
      headers: {
        "User-Agent":
          "DealalityResearchBot/1.0 (+https://www.dealality.com; audit-probe; read-only)",
        Accept: opts.accept || "text/html,application/xhtml+xml,*/*",
        ...(opts.headers || {}),
      },
      signal: AbortSignal.timeout(opts.timeoutMs || 25000),
    });
    const text = opts.method === "HEAD" ? "" : await res.text();
    return {
      ok: res.ok,
      status: res.status,
      url: res.url,
      headers: Object.fromEntries([...res.headers.entries()].slice(0, 40)),
      body: text,
      elapsedMs: Date.now() - started,
      error: null,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      headers: {},
      body: "",
      elapsedMs: Date.now() - started,
      error: String(err && err.message ? err.message : err),
    };
  }
}

function originOf(website) {
  try {
    return new URL(website).origin;
  } catch {
    return null;
  }
}

function hostOf(website) {
  try {
    return new URL(website).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

export async function runPublicResearchProbe({ propertyId, name, website, outDir }) {
  const origin = originOf(website);
  if (!origin) throw new Error(`INVALID_WEBSITE: ${website}`);
  fs.mkdirSync(outDir, { recursive: true });

  const robots = await fetchText(`${origin}/robots.txt`, {
    accept: "text/plain,*/*",
  });
  fs.writeFileSync(path.join(outDir, "robots.txt"), robots.body || "", "utf8");
  fs.writeFileSync(
    path.join(outDir, "robots.meta.json"),
    JSON.stringify({ ...robots, body: undefined, bodyBytes: (robots.body || "").length }, null, 2)
  );

  const sitemap = await fetchText(`${origin}/sitemap.xml`, {
    accept: "application/xml,text/xml,*/*",
  });
  fs.writeFileSync(path.join(outDir, "sitemap.xml"), sitemap.body || "", "utf8");

  const head = await fetchText(origin + "/", { method: "HEAD" });
  fs.writeFileSync(path.join(outDir, "http-head-probe.json"), JSON.stringify(head, null, 2));

  const home = await fetchText(origin + "/");
  fs.writeFileSync(path.join(outDir, "homepage.html"), home.body || "", "utf8");
  fs.writeFileSync(
    path.join(outDir, "http-get-probe.json"),
    JSON.stringify(
      {
        ...home,
        body: undefined,
        bodyBytes: (home.body || "").length,
        titleMatch: (home.body || "").match(/<title[^>]*>([^<]*)<\/title>/i)?.[1] || null,
      },
      null,
      2
    )
  );

  const lower = (home.body || "").toLowerCase();
  const signals = {
    mentionsMeetings: /meeting|conference|event|wedding|group/.test(lower),
    mentionsDining: /dining|restaurant|bar|cuisine/.test(lower),
    mentionsSpa: /spa|wellness|fitness/.test(lower),
    mentionsGolf: /golf/.test(lower),
    mentionsMarina: /marina|yacht|boat|waterfront/.test(lower),
    hasJsonLd: /application\/ld\+json/.test(lower),
    michelinMention: /michelin/.test(lower),
  };

  const index = {
    propertyId,
    name,
    website,
    canonicalHost: hostOf(website),
    origin,
    probedAt: new Date().toISOString(),
    paidCostUsd: 0,
    robots: { status: robots.status, ok: robots.ok, error: robots.error },
    sitemap: { status: sitemap.status, ok: sitemap.ok, error: sitemap.error },
    homepage: {
      status: home.status,
      ok: home.ok,
      finalUrl: home.url,
      error: home.error,
      signals,
    },
    head: { status: head.status, ok: head.ok, error: head.error },
  };
  fs.writeFileSync(path.join(outDir, "PROBE_INDEX.json"), JSON.stringify(index, null, 2));
  return index;
}

async function main() {
  const get = (flag) => {
    const i = process.argv.indexOf(flag);
    return i >= 0 ? process.argv[i + 1] : null;
  };
  const propertyId = get("--property-id");
  const name = get("--name") || propertyId;
  const website = get("--website");
  const outDir =
    get("--out-dir") ||
    path.join(
      ROOT,
      "data/ai-visibility-audit-action-plan-pilot-v1",
      propertyId || "unknown"
    );
  if (!propertyId || !website) {
    console.error("Usage: --property-id --website [--name] [--out-dir]");
    process.exit(1);
  }
  const result = await runPublicResearchProbe({ propertyId, name, website, outDir });
  console.log(JSON.stringify({ ok: true, result }, null, 2));
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
