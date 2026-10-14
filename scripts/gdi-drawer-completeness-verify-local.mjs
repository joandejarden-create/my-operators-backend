#!/usr/bin/env node
/**
 * Programmatic local drawer verification against repaired store + optional live API.
 *
 *   node scripts/gdi-drawer-completeness-verify-local.mjs
 *   node scripts/gdi-drawer-completeness-verify-local.mjs --api http://127.0.0.1:8080
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateDrawerContentCompleteness,
  reportDrawerCompleteness,
} from "../lib/group-demand-intelligence/workflow/drawer-content-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-drawer-completeness");
const STORE = path.join(OUT, "_local-repaired-store/hotels");

const HOTELS = [
  { id: "recLuxvwwxID7U2B8", key: "bethesda", label: "Bethesda Marriott" },
  { id: "rec0eTEnCqlsGJJt9", key: "mayflower", label: "Mayflower" },
];

const SAMPLE_CARD_IDS = [
  "may-nado-2028",
  "beth-nado-2028",
  "may-ndss-2027",
  "beth-ndss-2027",
  "may-amwa-2027", // functional contact group 3
  "beth-amwa-annual-2027",
  "may-nado-2027", // exploratory
  "beth-nado-2027-overflow",
];

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}

function loadHotel(hotelId) {
  const p = path.join(STORE, hotelId, "opportunities.json");
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function pickFacing(doc) {
  return (doc.opportunities || []).filter(
    (o) =>
      o.prospectHotelFacing === true ||
      (o.prospectReviewCardId && o.prospectPublicationBasis === "REVIEWED_PACKAGE_V1")
  );
}

async function fetchApi(base, hotelId) {
  const listUrl = `${base}/api/group-demand-intelligence/opportunities?hotelId=${encodeURIComponent(hotelId)}`;
  const listRes = await fetch(listUrl, {
    headers: { Accept: "application/json" },
  });
  const listJson = await listRes.json().catch(() => ({}));
  const items = listJson.opportunities || listJson.items || listJson.results || [];
  const details = [];
  for (const cardId of SAMPLE_CARD_IDS) {
    const match = items.find(
      (o) =>
        o.prospectReviewCardId === cardId ||
        o.id?.includes?.(cardId) ||
        o.cardId === cardId
    );
    if (!match?.id) continue;
    const detailUrl = `${base}/api/group-demand-intelligence/opportunities/${encodeURIComponent(match.id)}?hotelId=${encodeURIComponent(hotelId)}`;
    const dRes = await fetch(detailUrl, { headers: { Accept: "application/json" } });
    const dJson = await dRes.json().catch(() => ({}));
    details.push({
      cardId,
      listId: match.id,
      listThesis: match.hotelOpportunityThesis || match.summaryWhyHotel || null,
      detailThesis:
        dJson.opportunity?.hotelOpportunityThesis ||
        dJson.hotelOpportunityThesis ||
        null,
      listFitScore: match.hotelFitScore ?? null,
      detailFitScore:
        dJson.opportunity?.hotelFitScore ?? dJson.hotelFitScore ?? null,
      listSources: (match.sources || []).map((s) => s.url || s).slice(0, 5),
      detailSources: (
        dJson.opportunity?.sources ||
        dJson.sources ||
        []
      )
        .map((s) => s.url || s)
        .slice(0, 5),
      parityThesis:
        String(match.hotelOpportunityThesis || "") ===
        String(
          dJson.opportunity?.hotelOpportunityThesis ||
            dJson.hotelOpportunityThesis ||
            ""
        ),
      status: dRes.status,
    });
  }
  return {
    ok: listRes.status === 200,
    status: listRes.status,
    count: items.length,
    reviewedCount: items.filter((o) => o.prospectReviewCardId).length,
    details,
  };
}

async function main() {
  const apiBase = arg("--api", process.env.GDI_LOCAL_API || "");
  const byHotel = {};
  const samples = [];

  for (const h of HOTELS) {
    const doc = loadHotel(h.id);
    const facing = pickFacing(doc).filter((o) => o.prospectHotelFacing !== false);
    const report = reportDrawerCompleteness(facing, {
      hotelId: h.id,
      facingOnly: true,
    });
    byHotel[h.id] = {
      label: h.label,
      storePath: path.join(STORE, h.id, "opportunities.json"),
      drawer: { pass: report.pass, fail: report.fail, total: report.total },
      sampleCards: SAMPLE_CARD_IDS.filter((id) =>
        facing.some((o) => o.prospectReviewCardId === id)
      ).map((cardId) => {
        const o = facing.find((x) => x.prospectReviewCardId === cardId);
        const ev = evaluateDrawerContentCompleteness(o);
        const row = {
          cardId,
          id: o.id,
          section: o.prospectReviewSection,
          opportunityType: o.opportunityType,
          location: o.destinationStatus || o.eventCity,
          thesis: (o.hotelOpportunityThesis || "").slice(0, 160),
          fitScore: o.hotelFitScore,
          sourcing: o.venueSourcingStatusLabel || o.venueSourcingStatus,
          contact:
            o.primaryContact?.name ||
            o.functionalContactEmail ||
            o.primaryContact?.email ||
            null,
          contactRelevance: (o.primaryContact?.relevanceEvidence || "").slice(
            0,
            120
          ),
          sources: (o.sources || []).map((s) => s.url).filter(Boolean).slice(0, 4),
          nextAction: (o.recommendedAction || "").slice(0, 140),
          validationPreserved: Boolean(
            o.validationFeedback || o.hotelValidation
          ),
          drawerOk: ev.ok,
          failed: ev.failed,
        };
        samples.push({ hotelId: h.id, ...row });
        return row;
      }),
    };
  }

  let api = null;
  if (apiBase) {
    api = {};
    for (const h of HOTELS) {
      try {
        api[h.id] = await fetchApi(apiBase.replace(/\/$/, ""), h.id);
      } catch (err) {
        api[h.id] = { ok: false, error: err.message };
      }
    }
  }

  const out = {
    asOf: new Date().toISOString(),
    storeRoot: STORE,
    apiBase: apiBase || null,
    byHotel,
    samples,
    api,
    verdict: {
      storePass:
        Object.values(byHotel).reduce((n, h) => n + h.drawer.pass, 0) ===
        Object.values(byHotel).reduce((n, h) => n + h.drawer.total, 0),
      nadoMayflower: samples.find((s) => s.cardId === "may-nado-2028") || null,
      note: apiBase
        ? "API probe attempted — live Airtable may still show pre-repair payloads until production apply"
        : "Store-only verification (repaired local filesystem)",
    },
  };

  fs.writeFileSync(
    path.join(OUT, "03-local-browser-evidence.json"),
    JSON.stringify(out, null, 2) + "\n"
  );

  const md = `# Local drawer verification evidence

## Store (repaired filesystem)
${HOTELS.map(
  (h) =>
    `- **${h.label}**: ${byHotel[h.id].drawer.pass}/${byHotel[h.id].drawer.total} drawer contract pass`
).join("\n")}

## Representative cards (store)
${samples
  .map(
    (s) =>
      `### ${s.cardId} (\`${s.id}\`)
- Section: ${s.section}
- Type: ${s.opportunityType}
- Location: ${s.location}
- Fit score: ${s.fitScore === null ? "null (not scored)" : s.fitScore}
- Sourcing: ${s.sourcing}
- Contact: ${s.contact}
- Sources: ${(s.sources || []).join(" | ") || "—"}
- Thesis: ${s.thesis}
- Next: ${s.nextAction}
- Drawer OK: ${s.drawerOk}
`
  )
  .join("\n")}

## API / browser
${
  apiBase
    ? Object.entries(api || {})
        .map(
          ([id, a]) =>
            `- \`${id}\`: status=${a.status ?? "err"} count=${a.count ?? "?"} reviewed=${a.reviewedCount ?? "?"} ${a.error || ""}`
        )
        .join("\n")
    : "Store-only this run. Open local GDI with filesystem data root pointing at `_local-repaired-store` for visual QA:\n\n```\nGDI_DATA_ROOT=reports/gdi-drawer-completeness/_local-repaired-store GDI_OPPORTUNITIES_PERSISTENCE=filesystem GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1 node server.js\n```\n\nPages:\n- `/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8`\n- `/group-demand-intelligence.html?hotelId=rec0eTEnCqlsGJJt9`\n"
}

**Visibility alone is not completion** — contract scores above are the completion gate.
`;
  fs.writeFileSync(path.join(OUT, "03-local-browser-evidence.md"), md);
  console.log(
    JSON.stringify(
      {
        ok: out.verdict.storePass,
        byHotel: Object.fromEntries(
          Object.entries(byHotel).map(([id, h]) => [id, h.drawer])
        ),
        apiProbed: Boolean(apiBase),
        out: path.join(OUT, "03-local-browser-evidence.md"),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
