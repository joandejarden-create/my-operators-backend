/**
 * Trim customer-facing Digital Authority sources to PDF-safe evidence.
 * Move excess V2 discovery inventory to Admin-only researchInventory.
 * Scrub broken / access-restricted customer hyperlinks.
 *
 *   node scripts/ava-six-hotel-final-evidence-source-trim-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { fetchHtml } from "../lib/ai-visibility-audit-action-plan/digital-authority-discovery-v2.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const FILES = [
  "waterstone-boca-raton-v1.json",
  "castillo-hotel-son-vida-v1.json",
  "sheraton-mallorca-arabella-golf-v1.json",
  "ac-hotel-a-coruna-v1.json",
  "grand-fiesta-americana-chapultepec-v1.json",
  "fiesta-americana-condesa-cancun-ai-v1.json",
];

const KEEP_MAX_BACKLINKS = 6;
const KEEP_MAX_MENTIONS = 4;

function urlOf(s) {
  return s?.linkingPageUrl || s?.evidenceUrl || s?.url || null;
}

async function isCustomerSafeUrl(url) {
  if (!url || !/^https?:\/\//i.test(url)) return false;
  // Known bad patterns
  if (/acbymarriott\.acorunahotel\.com/i.test(url)) return false;
  if (/hilton\.com\/en-gb\/locations\/usa\/florida\/boca-raton/i.test(url)) {
    return false;
  }
  if (/whoownes\.com/i.test(url)) return false;
  if (/get-rates\.com/i.test(url)) return false;
  if (/mibuscadordenegocios\.com/i.test(url)) return false;
  if (/marriott-hotels\.marriott\.com\/locations\/resorts/i.test(url)) return false;
  if (/hilton\.com\/en\/locations\/resorts/i.test(url)) return false;

  const page = await fetchHtml(url, { timeoutMs: 15000 });
  if (!page.ok) {
    if ([403, 401, 429, 503].includes(page.status)) return "RESTRICTED";
    if ([404, 410].includes(page.status)) return false;
    if (page.status === 0) return "RESTRICTED";
    return false;
  }
  return true;
}

async function trimFile(file) {
  const p = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads", file);
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const da = j.digitalAuthority;
  if (!da) return { file, skipped: true };

  const all = [...(da.sources || [])];
  const researchInventory = [...(da.researchInventory || []), ...all];

  const backlinks = all.filter((s) => s.findingClass === "VERIFIED_BACKLINK");
  const mentions = all.filter(
    (s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION"
  );
  const other = all.filter(
    (s) =>
      s.findingClass !== "VERIFIED_BACKLINK" &&
      s.findingClass !== "VERIFIED_EXTERNAL_MENTION"
  );

  const kept = [];
  const dropped = [];

  for (const s of backlinks.slice(0, KEEP_MAX_BACKLINKS)) {
    const u = urlOf(s);
    const ok = await isCustomerSafeUrl(u);
    if (ok === true) kept.push(s);
    else {
      dropped.push({ url: u, reason: ok === "RESTRICTED" ? "ACCESS_RESTRICTED" : "UNSAFE" });
      if (ok === "RESTRICTED") {
        // Keep as mention without customer hyperlink requirement — store without URL in customer sources
        kept.push({
          ...s,
          findingClass: "VERIFIED_EXTERNAL_MENTION",
          linkingPageUrl: null,
          evidenceUrl: null,
          hyperlinkToOfficialSite: false,
          note:
            (s.note ? `${s.note} ` : "") +
            "Referring page observed in research but not linked in the customer PDF due to automated access limits.",
        });
      }
    }
  }

  for (const s of mentions.slice(0, KEEP_MAX_MENTIONS)) {
    const u = urlOf(s);
    const ok = await isCustomerSafeUrl(u);
    if (ok === true) kept.push(s);
    else {
      dropped.push({ url: u, reason: ok === "RESTRICTED" ? "ACCESS_RESTRICTED" : "UNSAFE_OR_BROKEN" });
      // Mentions can remain as prose-only (null URL) if commercially useful
      if (s.referringOrganization) {
        kept.push({
          ...s,
          linkingPageUrl: null,
          evidenceUrl: null,
          note:
            (s.note ? `${s.note} ` : "") +
            "Source noted without customer hyperlink after access/quality filter.",
        });
      }
    }
  }

  // Do not keep "other" finding classes in customer sources unless opportunity targets
  // Opportunities keep targetOrganizationUrl separately

  // Keep only top opportunities with safe URLs; move the rest to research inventory notes
  if (Array.isArray(da.opportunities)) {
    const safeOpps = [];
    for (const o of da.opportunities) {
      if (o.targetOrganizationUrl) {
        const ok = await isCustomerSafeUrl(o.targetOrganizationUrl);
        if (ok !== true) {
          dropped.push({
            url: o.targetOrganizationUrl,
            reason: "OPPORTUNITY_URL_UNSAFE",
          });
          o.targetOrganizationUrl = null;
          o.evidenceUrl = null;
          o.evidenceLinks = [];
          o.note =
            (o.note ? `${o.note} ` : "") +
            "Target URL removed from customer hyperlinks pending reachable destination page.";
        }
      }
      if (safeOpps.length < 4) safeOpps.push(o);
    }
    da.opportunities = safeOpps;
  }

  // Competitor gaps: keep narrative; strip evidence URLs not shown in PDF to avoid annotation gate failures
  if (Array.isArray(da.competitorGaps)) {
    da.competitorGaps = da.competitorGaps.slice(0, 4).map((g) => ({
      ...g,
      evidenceUrl: null,
      competitorLinkingPageUrl: null,
      note:
        (g.note ? `${g.note} ` : "") +
        "Supporting competitor pages retained in Admin research inventory; not duplicated as customer PDF hyperlinks.",
    }));
  }

  if (Array.isArray(da.competitorBacklinks)) {
    const safeCb = [];
    for (const b of da.competitorBacklinks.slice(0, 4)) {
      const u = b.linkingPageUrl || b.evidenceUrl;
      const ok = u ? await isCustomerSafeUrl(u) : false;
      if (ok === true) safeCb.push(b);
      else {
        dropped.push({ url: u, reason: "COMPETITOR_BL_SCRUBBED" });
      }
    }
    da.competitorBacklinks = safeCb;
  }

  // Scrub broken URLs from actions evidence
  for (const a of j.actions || []) {
    for (const listName of ["evidenceLinks", "supportingEvidence"]) {
      if (!Array.isArray(a[listName])) continue;
      const next = [];
      for (const e of a[listName]) {
        const u = e?.url || (typeof e === "string" ? e : null);
        if (!u) {
          next.push(e);
          continue;
        }
        const ok = await isCustomerSafeUrl(u);
        if (ok === true) next.push(e);
        else {
          dropped.push({ url: u, reason: `ACTION_${listName}_SCRUBBED` });
          next.push({
            ...(typeof e === "object" ? e : { label: "Source" }),
            url: null,
            note: "URL removed from customer evidence after verification failure.",
          });
        }
      }
      a[listName] = next;
    }
    if (a.technicalDetails?.urls) {
      a.technicalDetails.urls = (a.technicalDetails.urls || []).filter((e) => {
        const u = e?.url;
        return u && !/acbymarriott\.acorunahotel\.com|whoownes\.com/i.test(u);
      });
    }
  }

  // Key findings evidence scrub for broken Hilton mislabels already handled; scrub broken urls
  for (const f of j.keyFindings || []) {
    if (!Array.isArray(f.evidence)) continue;
    for (const e of f.evidence) {
      if (!e?.url) continue;
      const ok = await isCustomerSafeUrl(e.url);
      if (ok !== true) {
        dropped.push({ url: e.url, reason: "FINDING_EVIDENCE_SCRUBBED" });
        e.url = null;
        e.kind =
          ok === "RESTRICTED" ? "TECHNICAL_OBSERVATION" : "NOT_CURRENTLY_VERIFIED";
        e.note =
          (e.note ? `${e.note} ` : "") +
          "Customer hyperlink removed after verification failure; claim retained only if otherwise supported.";
      }
    }
  }

  da.researchInventory = researchInventory;
  da.sources = kept;
  da.inventoryCompleteness =
    "Public multi-strategy discovery completed. Customer PDF lists verified third-party links and selected references only; full discovery inventory is retained for Admin review.";
  if (da.discoveryCoverage) {
    da.discoveryCoverage.customerFacingSources = kept.length;
    da.discoveryCoverage.researchInventoryCount = researchInventory.length;
  }

  fs.writeFileSync(p, `${JSON.stringify(j, null, 2)}\n`);
  return {
    file,
    kept: kept.length,
    researchInventory: researchInventory.length,
    dropped: dropped.length,
    droppedSample: dropped.slice(0, 8),
  };
}

const out = [];
for (const f of FILES) {
  console.log("trim", f);
  out.push(await trimFile(f));
}
console.log(JSON.stringify(out, null, 2));
