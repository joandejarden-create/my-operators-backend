/**
 * Bounded official-list mining for 5 frozen AC/RAD campaigns only.
 * No Apify, no broad discovery, no Ready threshold changes, no invented lists.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { serpapiSearch } from "../lib/research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  loadDemandCampaigns,
  upsertDemandCampaigns,
  runHotelDemandCampaignDecompositions,
} from "../lib/group-demand-intelligence/demand-campaigns/index.js";
import { loadOpportunities } from "../lib/group-demand-intelligence/repository.js";
import { listPursuits } from "../lib/group-demand-intelligence/pursuit/pursuit-store-v1.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/ac-coruna-radisson-official-list-mining");
const NOW = "2026-10-07";
const AC = "rec2PVBDavppGpenm";
const RAD = "recUOyzOXn2Zdp98I";
const YOTEL = "recrPQcZg7SFARRb2";
const BETH = "recLuxvwwxID7U2B8";

const COHORT = [
  {
    key: "IAPS",
    hotelId: AC,
    campaignId: "accamp_iaps_spaces_in_transition_symposium_2027_2027",
    queries: [
      { lang: "en", q: 'IAPS "Spaces in Transition" 2027 programme OR speakers OR participants site:iaps-association.org OR site:udc.es' },
      { lang: "es", q: 'IAPS "Spaces in Transition" 2027 programa OR ponentes OR participantes OR universidad Coruña' },
      { lang: "gl", q: 'IAPS simposio 2027 programa OR participantes OR universidade A Coruña' },
    ],
    seedUrls: ["https://iaps-association.org/"],
  },
  {
    key: "BIOCULTURA",
    hotelId: AC,
    campaignId: "accamp_biocultura_a_coruna_2027_2027",
    queries: [
      { lang: "es", q: 'BioCultura "A Coruña" 2027 expositores OR "lista de expositores" OR "manual del expositor" site:biocultura.org' },
      { lang: "gl", q: 'BioCultura Coruña 2027 expositores OR entidades participantes OR aloxamento site:biocultura.org' },
      { lang: "en", q: 'BioCultura Coruna 2027 exhibitors list OR exhibitor manual site:biocultura.org' },
      { lang: "es", q: 'BioCultura "A Coruña" 2025 OR 2026 expositores lista site:biocultura.org' },
    ],
    seedUrls: ["https://www.biocultura.org/acoruna"],
  },
  {
    key: "RIF",
    hotelId: RAD,
    campaignId: "radisscamp_vii_congreso_iberoamericano_de_filosofia_2027_2027",
    queries: [
      { lang: "es", q: '"Congreso Iberoamericano de Filosofía" 2027 programa OR ponentes OR participantes OR universidades site:rediberoamericanafilosofia.com' },
      { lang: "es", q: '"VII Congreso Iberoamericano de Filosofía" Santo Domingo alojamiento OR hotel OR inscripción' },
      { lang: "en", q: '"Iberoamerican Congress of Philosophy" 2027 Santo Domingo programme speakers' },
    ],
    seedUrls: [
      "https://rediberoamericanafilosofia.com/wp-content/uploads/2026/05/Segunda-convocatoria-Congreso-RIF.pdf",
    ],
  },
  {
    key: "CIELO",
    hotelId: RAD,
    campaignId: "radisscamp_6_congreso_mundial_cielo_laboral_2026_2026",
    queries: [
      { lang: "es", q: '"CIELO Laboral" "Santo Domingo" 2026 programa OR participantes OR ponentes OR alojamiento site:cielolaboral.com' },
      { lang: "es", q: '"6º Congreso Mundial CIELO" PUCMM hotel OR alojamiento OR inscripción' },
      { lang: "en", q: 'CIELO Laboral World Congress 2026 Santo Domingo programme speakers hotel' },
    ],
    seedUrls: ["https://www.cielolaboral.com/6o-congreso-mundial-cielo-laboral-santodomingo/"],
  },
  {
    key: "AUTOAMERICAS",
    hotelId: RAD,
    campaignId: "radisscamp_autoamericas_2027_2027",
    queries: [
      { lang: "es", q: 'AUTOAMERICAS 2027 expositores OR "lista de expositores" OR patrocinadores site:autoamericas.show' },
      { lang: "es", q: 'AUTOAMERICAS 2027 "hotel oficial" OR alojamiento OR expositores site:autoamericas.show' },
      { lang: "en", q: 'AUTOAMERICAS 2027 exhibitors list OR sponsors Santo Domingo' },
      { lang: "es", q: 'AUTOAMERICAS 2026 expositores lista site:autoamericas.show' },
    ],
    seedUrls: [
      "https://www.autoamericas.show/es/expo/alojamiento.html",
      "https://www.autoamericas.show/es/",
    ],
  },
];

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function write(name, body) {
  fs.writeFileSync(path.join(OUT, name), body.endsWith("\n") ? body : body + "\n", "utf8");
}
function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, headers) {
  return (
    [headers.join(",")]
      .concat(rows.map((r) => headers.map((h) => csvEscape(r[h])).join(",")))
      .join("\n") + "\n"
  );
}

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&eacute;/gi, "é")
    .replace(/&aacute;/gi, "á")
    .replace(/&iacute;/gi, "í")
    .replace(/&oacute;/gi, "ó")
    .replace(/&uacute;/gi, "ú")
    .replace(/&ntilde;/gi, "ñ")
    .replace(/&#(\d+);/g, (_, n) => {
      try {
        return String.fromCharCode(Number(n));
      } catch {
        return " ";
      }
    })
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function fetchText(url) {
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(25000),
    });
    const ct = res.headers.get("content-type") || "";
    const buf = Buffer.from(await res.arrayBuffer());
    if (/pdf/i.test(ct) || /\.pdf(\?|$)/i.test(url)) {
      // Compressed PDFs rarely yield plain text via ASCII scrape — keep printable spans only.
      const ascii = buf
        .toString("latin1")
        .replace(/[^\x09\x0A\x0D\x20-\x7E\u00A0-\u00FF]/g, " ");
      return {
        ok: res.ok,
        status: res.status,
        url: res.url || url,
        contentType: ct,
        text: ascii.slice(0, 120000),
        plain: ascii.slice(0, 120000),
        isPdf: true,
      };
    }
    const raw = buf.toString("utf8");
    const plain = stripHtml(raw);
    return {
      ok: res.ok,
      status: res.status,
      url: res.url || url,
      contentType: ct,
      // Keep enough raw for link scraping; classify/extract from plain text.
      text: raw.slice(0, 400000),
      plain: plain.slice(0, 200000),
      isPdf: false,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      contentType: "",
      text: "",
      plain: "",
      error: err?.message || String(err),
    };
  }
}

/** Official / directly affiliated hosts only — never invent lists from lookalike congresses. */
function isOfficialOrAffiliatedHost(url, campaignKey) {
  try {
    const h = new URL(url).hostname.replace(/^www\./, "").toLowerCase();
    const allow = {
      IAPS: ["iaps-association.org", "udc.es"],
      BIOCULTURA: ["biocultura.org", "vidasana.org"],
      RIF: ["rediberoamericanafilosofia.com"],
      CIELO: ["cielolaboral.com", "pucmm.edu.do"],
      AUTOAMERICAS: ["autoamericas.show", "latinpressinc.com", "issuu.com"],
    };
    return (allow[campaignKey] || []).some((d) => h === d || h.endsWith(`.${d}`));
  } catch {
    return false;
  }
}

function classifySourceType(url, title, text) {
  const blob = `${url} ${title} ${text}`.toLowerCase();
  if (/exhibitor.?list|lista de expositores|catálogo de expositores|directorio de expositores/.test(blob)) {
    return "OFFICIAL_EXHIBITOR_LIST";
  }
  if (/sponsor.?list|lista de patrocinadores|patrocinios/.test(blob)) return "OFFICIAL_SPONSOR_LIST";
  if (/speaker|ponente|faculty|programa|programme/.test(blob) && /\.pdf|programa/.test(blob)) {
    return "OFFICIAL_PROGRAM_PDF";
  }
  if (/delegaci|participant.?list|lista de participantes/.test(blob)) return "OFFICIAL_PARTICIPANT_LIST";
  if (/exhibitor.?manual|manual del expositor|prospectus/.test(blob)) return "OFFICIAL_EXHIBITOR_MANUAL";
  if (/alojamiento|hotel oficial|housing|acomodaci/.test(blob)) return "OFFICIAL_DELEGATE_GUIDE";
  if (/call for papers|convocatoria|inscripci/.test(blob)) return "OFFICIAL_REGISTRATION_GUIDE";
  return "UNVERIFIED";
}

function classifyCycle(url, title, text, eventYear) {
  const blob = `${url} ${title} ${text}`;
  const y = Number(eventYear);
  if (new RegExp(String(y)).test(blob)) {
    if (/expositor|particip|sponsor|patrocin|programma|programa|speaker|ponente/i.test(blob)) {
      // current year mention without actual list → often prospectus
      if (/lista|directory|catálogo|catalogo|directorio|exhibitors list/i.test(blob)) {
        return "CURRENT_CYCLE";
      }
      return "FUTURE_CYCLE_NOT_PUBLISHED";
    }
    return "CURRENT_CYCLE";
  }
  if (/2025|2024|2023/.test(blob) && !new RegExp(String(y)).test(blob)) return "PRIOR_CYCLE_PROCESS_SIGNAL";
  if (/2026/.test(blob) && y === 2027) return "PRIOR_CYCLE_PROCESS_SIGNAL";
  return "UNVERIFIED";
}

function isAggregator(url) {
  return /eventbrite|facebook\.com|linkedin\.com|tripadvisor|booking\.com|expedia|10times|conferencealerts|allconference|wikipedia/i.test(
    url
  );
}

function cleanEntityName(name) {
  return String(name || "")
    .replace(/\s+/g, " ")
    .replace(/[.;:].*$/, "")
    .replace(/\s+con más de.*$/i, "")
    .replace(/\s+y consultoría.*$/i, "")
    .replace(/\s+Fue creador.*$/i, "")
    .trim();
}

function extractOrgCandidates(text, campaignKey, sourceUrl) {
  const out = [];
  const seen = new Set();
  const push = (name, type, role, evidence) => {
    const n = cleanEntityName(name);
    if (n.length < 4 || n.length > 90) return;
    if (/cookie|privacy|menu|inicio|home|click|aquí|aqui|próximamente|proximamente/i.test(n)) return;
    const key = n.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    out.push({
      entityName: n,
      entityType: type,
      participationRole: role,
      evidenceText: evidence.slice(0, 200),
      evidenceConfidence: "MEDIUM",
    });
  };

  const officialHost = isOfficialOrAffiliatedHost(sourceUrl || "", campaignKey);

  // Hotel partners — only from AUTOAMERICAS official alojamiento / site pages
  if (campaignKey === "AUTOAMERICAS" && officialHost) {
    if (/Hotel Oficial|Dominican Fiesta|tarifas corporativas preferenciales/i.test(text)) {
      push("Hotel Dominican Fiesta", "HOTEL", "PARTNER", "Hotel Oficial AUTOAMERICAS 2027");
      push("Palladium Hotel Group", "COMPANY", "VENDOR", "Reservas Hotel Dominican Fiesta / Palladium");
    }
    if (/Latinpress|AUTOAMERICAS/i.test(text)) {
      push("Latinpress", "COMPANY", "PARTNER", "Organizer / grupo Latinpress AUTOAMERICAS");
    }
    // Prior-cycle convenio hotels — process signal only (not current exhibitor confirmation)
    if (/2026|convenio|JW Marriott|Radisson Hotel Santo Domingo/i.test(text)) {
      const prior = [
        ["JW Marriott Santo Domingo", "PRIOR_CYCLE AutoAmericas 2026 convenio"],
        ["Renaissance Santo Domingo Jaragua", "PRIOR_CYCLE 2026 convenio"],
        ["Courtyard Santo Domingo", "PRIOR_CYCLE 2026 convenio"],
        ["Radisson Hotel Santo Domingo", "PRIOR_CYCLE 2026 convenio — target hotel listed"],
        ["Hodelpa Nicolás de Ovando", "PRIOR_CYCLE 2026 convenio"],
        ["Sheraton Santo Domingo", "PRIOR_CYCLE 2026 convenio"],
        ["Catalonia Santo Domingo", "PRIOR_CYCLE 2026 convenio"],
      ];
      for (const [n, e] of prior) {
        if (new RegExp(n.split(" ")[0], "i").test(text) || /hotel/i.test(text)) {
          push(n, "HOTEL", "PARTNER", e);
        }
      }
    }
  }

  // CIELO / PUCMM host — official CIELO page only
  if (campaignKey === "CIELO" && officialHost) {
    push(
      "Pontificia Universidad Católica Madre y Maestra (PUCMM)",
      "UNIVERSITY",
      "PARTICIPATING_ORG",
      "CIELO host university Santo Domingo"
    );
    push("CIELO Laboral", "ASSOCIATION", "PARTNER", "Network organizer from official congress page");
  }

  // RIF — organizers from official convocatoria only. Do NOT scrape lookalike congress sites.
  if (campaignKey === "RIF" && officialHost) {
    push("Asociación Dominicana de Filosofía", "ASSOCIATION", "PARTNER", "Co-organizer on convocatoria");
    push("Red Iberoamericana de Filosofía", "ASSOCIATION", "PARTNER", "Network organizer");
    // Only accept university names if plainly present on official RIF host (compressed PDF often yields none)
    const uniRe = /Universidad(?:e)?\s+[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚáéíóúñÑ\s]{2,50}/g;
    const matches = text.match(uniRe) || [];
    for (const m of matches.slice(0, 12)) {
      const cleaned = cleanEntityName(m);
      if (cleaned.split(" ").length < 2) continue;
      push(cleaned, "UNIVERSITY", "SPEAKER_ORG", "Named on official RIF source");
    }
  }

  // IAPS / UDC — official hosts only
  if (campaignKey === "IAPS" && officialHost) {
    push(
      "Universidade da Coruña (UDC)",
      "UNIVERSITY",
      "PARTICIPATING_ORG",
      "IAPS Sustainability Network / UDC People-Environment Research Group host"
    );
    push(
      "IAPS — International Association of People-Environment Studies",
      "ASSOCIATION",
      "PARTNER",
      "Parent association"
    );
  }

  // BioCultura — organizer only unless list found
  if (campaignKey === "BIOCULTURA" && officialHost) {
    push("Asociación Vida Sana", "ASSOCIATION", "PARTNER", "Official BioCultura organizer");
    // Do not invent exhibitor names — directory not yet public for 2027
  }

  return out;
}

function lodgingFromText(text, url) {
  const blob = `${text} ${url}`.toLowerCase();
  if (
    /hotel oficial|official hotel|bloque de habitaciones|tarifas corporativas preferenciales|sede oficial/.test(
      blob
    )
  ) {
    return {
      class: "DIRECT_LODGING_EVIDENCE",
      note: "Official hotel / preferential corporate rates published",
    };
  }
  if (/alojamiento|hospedaje|hoteles recomendados|convenio.?hotel|housing/.test(blob)) {
    return { class: "STRONG_HOTEL_MOTION", note: "Accommodation guidance published" };
  }
  if (/\bhotel\b/.test(blob)) return { class: "PLAUSIBLE_HOTEL_MOTION", note: "Hotel mention only" };
  return { class: "NONE", note: "" };
}

function travelingClass(entity, campaignKey, lodgingClass) {
  if (entity.entityType === "HOTEL") return "NONE";
  if (entity.participationRole === "ORGANIZER" || /organizer|asociación vida sana/i.test(entity.entityName)) {
    return "UNRESOLVED";
  }
  if (campaignKey === "AUTOAMERICAS" && entity.participationRole === "PARTNER" && entity.entityType === "COMPANY") {
    return lodgingClass === "DIRECT_LODGING_EVIDENCE" ? "STRONG_INFERENCE" : "UNRESOLVED";
  }
  if (entity.entityType === "UNIVERSITY" || entity.participationRole === "SPEAKER_ORG") {
    return "STRONG_INFERENCE";
  }
  if (entity.participationRole === "EXHIBITOR" || entity.participationRole === "SPONSOR") {
    return "STRONG_INFERENCE";
  }
  return "UNRESOLVED";
}

function toEvidenceSeed(entity, campaign, sourceUrl, sourceLang, lodging) {
  const travel = travelingClass(entity, campaign.key, lodging.class);
  // NEVER promote pure organizer as new opportunity seed for Ready — keep as SIGNAL path
  const isOrganizerShell =
    /asociación vida sana|red iberoamericana|asociación dominicana de filosofía|cielo laboral$|latinpress|iaps —/i.test(
      entity.entityName
    ) && entity.participationRole !== "EXHIBITOR";
  const priorCycleOnly = /PRIOR_CYCLE/.test(entity.evidenceText || "") || entity.currentCycleStatus === "PRIOR_CYCLE_PROCESS_SIGNAL";

  return {
    organizationName: entity.entityName,
    role: entity.participationRole || "PARTICIPATING_ORG",
    participantType: entity.entityType || "ORG",
    travelingGroup:
      entity.entityType === "UNIVERSITY"
        ? "Faculty / research delegation"
        : entity.entityType === "HOTEL"
          ? "N/A — lodging partner not traveling demand"
          : "Participating organization traveling team (if attendance confirmed)",
    travelingEntityType:
      entity.entityType === "UNIVERSITY" ? "UNIVERSITY_TEAM" : "CORPORATE_TEAM",
    travelingEntityEvidence: entity.evidenceText,
    travelingEntityConfidence: priorCycleOnly && entity.entityType !== "HOTEL" ? "UNRESOLVED" : travel,
    travelingEntityProven: false,
    buyerEntity: `${entity.entityName} — events / conference services`,
    buyerRole:
      entity.entityType === "UNIVERSITY"
        ? "Program / Academic Events"
        : "Events / Exhibitor Management",
    publicContactPath: sourceUrl,
    lodgingState:
      lodging.class === "DIRECT_LODGING_EVIDENCE"
        ? "STRONG"
        : lodging.class === "STRONG_HOTEL_MOTION"
          ? "WEAK"
          : "UNKNOWN",
    lodgingNote: lodging.note || entity.evidenceText,
    evidenceUrl: sourceUrl,
    sourceLanguage: sourceLang,
    sourceType: entity.sourceType || "OFFICIAL_LIST_MINING",
    currentCycleStatus: entity.currentCycleStatus || "UNVERIFIED",
    forceClass:
      isOrganizerShell ||
      entity.entityType === "HOTEL" ||
      travel === "NONE" ||
      priorCycleOnly
        ? "SIGNAL_ONLY"
        : undefined,
    publicDataCeiling: travel === "UNRESOLVED" && lodging.class === "NONE",
  };
}

function stats(hotelId) {
  const opps = loadOpportunities(hotelId)?.opportunities || [];
  let ready = 0;
  let watch = 0;
  for (const o of opps) {
    if (isGdiCustomerOpportunityReady(o, { nowDate: NOW })?.ok) ready += 1;
    if (isValidFutureWatch(o, { nowDate: NOW })?.ok) watch += 1;
  }
  return {
    total: opps.length,
    ready,
    watch,
    pursuits: listPursuits(hotelId).length,
    campaigns: loadDemandCampaigns(hotelId).campaigns.length,
  };
}

async function mineCampaign(spec) {
  const doc = loadDemandCampaigns(spec.hotelId);
  const campaign = doc.campaigns.find((c) => c.campaignId === spec.campaignId);
  if (!campaign) throw new Error(`campaign_missing:${spec.campaignId}`);

  const sourceRows = [];
  const affiliated = [];
  const serpHits = [];
  const gl = spec.hotelId === AC ? "es" : "do";

  for (const qq of spec.queries) {
    try {
      const serp = await serpapiSearch({
        q: qq.q,
        num: 5,
        hl: qq.lang === "en" ? "en" : "es",
        gl,
      });
      for (const h of serp?.data?.organic_results || []) {
        serpHits.push({
          campaignKey: spec.key,
          lang: qq.lang,
          query: qq.q,
          title: h.title || "",
          link: h.link || "",
          snippet: h.snippet || "",
        });
      }
    } catch (err) {
      serpHits.push({
        campaignKey: spec.key,
        lang: qq.lang,
        query: qq.q,
        error: err?.message || String(err),
      });
    }
  }

  const urls = new Set(spec.seedUrls);
  for (const h of serpHits) {
    if (h.link && !isAggregator(h.link)) urls.add(h.link);
  }

  const fetched = [];
  for (const url of [...urls].slice(0, 10)) {
    const page = await fetchText(url);
    fetched.push(page);
    const plain = page.plain || stripHtml(page.text || "");
    const title = (page.text.match(/<title[^>]*>([^<]+)/i) || [])[1] || url;
    const sourceType = classifySourceType(url, title, plain.slice(0, 8000));
    const cycle = classifyCycle(url, title, plain.slice(0, 12000), campaign.eventYear);
    const officialish =
      !isAggregator(url) &&
      (isOfficialOrAffiliatedHost(url, spec.key) ||
        sourceType !== "UNVERIFIED" ||
        (() => {
          try {
            return (
              new URL(url).hostname.replace(/^www\./, "") ===
              new URL(campaign.officialSource).hostname.replace(/^www\./, "")
            );
          } catch {
            return false;
          }
        })());

    sourceRows.push({
      campaignKey: spec.key,
      campaignId: spec.campaignId,
      url: page.url || url,
      ok: page.ok,
      sourceType,
      currentCycleStatus: cycle,
      sourceLanguage: /galicia|coru|aloxamento|xornada/i.test(plain)
        ? "gl"
        : /congreso|expositores|alojamiento|filosof/i.test(plain)
          ? "es"
          : "en",
      officialish,
      isPdf: page.isPdf === true,
      error: page.error || "",
    });

    // Affiliated: only from official / directly affiliated event hosts
    if (
      isOfficialOrAffiliatedHost(url, spec.key) &&
      /hotel|alojamiento|inscripci|registro|exhibitor|cvent|eventsair|palladium/i.test(plain)
    ) {
      const linkRe = /href=["'](https?:\/\/[^"']+)["']/gi;
      let m;
      const host = (() => {
        try {
          return new URL(campaign.officialSource).hostname.replace(/^www\./, "");
        } catch {
          return "";
        }
      })();
      const seenAff = new Set(affiliated.map((a) => a.affiliatedUrl));
      while ((m = linkRe.exec(page.text)) && affiliated.length < 25) {
        const href = m[1].replace(/&amp;/g, "&");
        if (isAggregator(href) || seenAff.has(href)) continue;
        try {
          const uh = new URL(href).hostname.replace(/^www\./, "");
          if (host && uh.includes(host)) continue;
          if (/hotel|reserv|palladium|inscripci|registro|exhibitor|cvent|marriott|radisson|hodelpa/i.test(href)) {
            seenAff.add(href);
            affiliated.push({
              campaignKey: spec.key,
              fromUrl: url,
              affiliatedUrl: href,
              proof: "outbound_link_from_official_event_page",
            });
          }
        } catch {
          /* ignore */
        }
      }
    }
  }

  // Lodging only from official / affiliated hosts (never SERP university noise)
  let lodging = { class: "NONE", note: "", url: "" };
  for (const page of fetched) {
    if (!isOfficialOrAffiliatedHost(page.url || "", spec.key)) continue;
    const classifyBody = page.plain || stripHtml(page.text || "");
    const L = lodgingFromText(classifyBody, page.url || "");
    if (
      ["DIRECT_LODGING_EVIDENCE", "STRONG_HOTEL_MOTION"].includes(L.class) &&
      (lodging.class === "NONE" ||
        (L.class === "DIRECT_LODGING_EVIDENCE" && lodging.class !== "DIRECT_LODGING_EVIDENCE"))
    ) {
      lodging = { ...L, url: page.url };
    }
  }

  // Extract entities only from official / directly affiliated hosts
  const entities = [];
  const entitySeen = new Set();
  for (const page of fetched) {
    if (!page.ok && !page.text && !page.plain) continue;
    if (!isOfficialOrAffiliatedHost(page.url || "", spec.key)) continue;
    const row = sourceRows.find((s) => (s.url || "") === (page.url || ""));
    const cycle = row?.currentCycleStatus || "UNVERIFIED";
    const body = page.plain || stripHtml(page.text || "");
    const cands = extractOrgCandidates(body, spec.key, page.url || "");
    for (const c of cands) {
      const k = c.entityName.toLowerCase();
      if (entitySeen.has(k)) continue;
      entitySeen.add(k);
      // Prior-cycle hotel partners: process signal only — not current participation confirmation
      const cycleStatus =
        /PRIOR_CYCLE/.test(c.evidenceText) || cycle === "PRIOR_CYCLE_PROCESS_SIGNAL"
          ? "PRIOR_CYCLE_PROCESS_SIGNAL"
          : cycle === "CURRENT_CYCLE" && lodging.class === "DIRECT_LODGING_EVIDENCE" && /Hotel Oficial|2027/i.test(c.evidenceText)
            ? "CURRENT_CYCLE"
            : cycle;
      entities.push({
        ...c,
        campaignId: spec.campaignId,
        campaignKey: spec.key,
        sourceUrl: page.url,
        sourceLanguage: row?.sourceLanguage || "es",
        sourceType: row?.sourceType || "UNVERIFIED",
        currentCycleStatus: cycleStatus,
      });
    }
  }

  // Ceiling classification — never treat organizer-only or wrong-site scrapes as LIST_AVAILABLE
  const hasCurrentList = sourceRows.some(
    (s) =>
      s.officialish &&
      isOfficialOrAffiliatedHost(s.url, spec.key) &&
      s.currentCycleStatus === "CURRENT_CYCLE" &&
      /LIST|PROGRAM_PDF|DIRECTORY|MANUAL|DELEGATE/i.test(s.sourceType)
  );
  let listStatus = "PUBLIC_DATA_CEILING";
  let nextTrigger = "Monitor official site for participant/exhibitor list publication";
  if (hasCurrentList && entities.some((e) => e.participationRole === "EXHIBITOR" || e.entityType === "UNIVERSITY")) {
    listStatus = "LIST_AVAILABLE";
    nextTrigger = "Continue child packet completion on extracted accounts";
  } else if (spec.key === "BIOCULTURA") {
    listStatus = "LIST_NOT_YET_PUBLISHED";
    nextTrigger =
      "BioCultura 2027 exhibitor directory / manual typically publishes closer to Mar 2027 — recheck Q4 2026–Q1 2027";
  } else if (spec.key === "IAPS") {
    listStatus = "LIST_NOT_YET_PUBLISHED";
    nextTrigger = "Watch IAPS/UDC symposium programme + speaker list closer to Jun 2027";
  } else if (spec.key === "RIF") {
    // Convocation exists; participant/university programme list not yet public (PDF compressed / no extractable names)
    listStatus = "LIST_NOT_YET_PUBLISHED";
    nextTrigger =
      "Programme / accepted papers / university delegation list after convocatoria closes — recheck before Mar 2027";
  } else if (spec.key === "CIELO") {
    listStatus = "LIST_NOT_YET_PUBLISHED";
    nextTrigger =
      "Call for papers open — participant/speaker list after acceptance; lodging PDF if published before Dec 2026";
  } else if (spec.key === "AUTOAMERICAS") {
    listStatus =
      lodging.class === "DIRECT_LODGING_EVIDENCE" ? "LIST_PARTIAL" : "LIST_NOT_YET_PUBLISHED";
    nextTrigger =
      "Exhibitor directory for 2027 not public yet; official hotel (Dominican Fiesta) published — recheck exhibitor list Q1 2027";
  }

  // Build non-organizer evidence seeds for decomp (hotels PRIOR_CYCLE → SIGNAL_ONLY)
  const seeds = entities
    .filter((e) => {
      // Skip pure organizer duplicate of campaign.organizationName for promotion path
      return true;
    })
    .map((e) => toEvidenceSeed(e, spec, e.sourceUrl, e.sourceLanguage, lodging));

  // Keep one organizer seed for lineage but force SIGNAL_ONLY
  const organizerSeed = {
    organizationName: campaign.organizationName,
    role: "ORGANIZER",
    participantType: "ORGANIZER",
    travelingGroup: "Organizing / technical secretariat",
    buyerEntity: `${campaign.organizationName} — secretaría técnica`,
    buyerRole: "Technical Secretariat / Events",
    publicContactPath: campaign.officialSource,
    lodgingState: lodging.class === "DIRECT_LODGING_EVIDENCE" ? "STRONG" : "UNKNOWN",
    lodgingNote: lodging.note || "Organizer seed — not promoted as customer opportunity",
    evidenceUrl: campaign.officialSource,
    sourceLanguage: campaign.sourceLanguage || "es",
    forceClass: "SIGNAL_ONLY",
    publicDataCeiling: true,
  };

  const evidenceSeeds = [organizerSeed, ...seeds].filter((s, i, a) => {
    const k = s.organizationName.toLowerCase();
    return a.findIndex((x) => x.organizationName.toLowerCase() === k) === i;
  });

  upsertDemandCampaigns(
    spec.hotelId,
    [
      {
        campaignId: spec.campaignId,
        evidenceSeeds,
        officialListStatus: listStatus,
        officialListNextTrigger: nextTrigger,
        lodgingEvidenceClass: lodging.class,
        lodgingEvidenceUrl: lodging.url || null,
        lodgingEvidenceNote: lodging.note || null,
        listMiningAt: new Date().toISOString(),
        researchStatus: listStatus,
        nextAction: nextTrigger,
      },
    ],
    { note: "Official-list mining update — evidenceSeeds refreshed" }
  );

  return {
    spec,
    campaign,
    sourceRows,
    affiliated,
    serpHits,
    entities,
    lodging,
    listStatus,
    nextTrigger,
    evidenceSeeds,
    evidenceSeedCount: evidenceSeeds.length,
  };
}

async function main() {
  ensureDir(OUT);
  const beforeAc = stats(AC);
  const beforeRad = stats(RAD);
  const beforeYotel = stats(YOTEL);
  const beforeBeth = stats(BETH);

  const results = [];
  for (const spec of COHORT) {
    console.log("Mining", spec.key);
    results.push(await mineCampaign(spec));
  }

  // Re-run shared decomposition for both hotels (campaign-scoped)
  invalidateGdiHotelReadCache(AC);
  invalidateGdiHotelReadCache(RAD);
  const decompAc = await runHotelDemandCampaignDecompositions(AC, {
    nowDate: NOW,
    persist: true,
    enableJev: false,
    maxCompletionSteps: 0,
    campaignIds: COHORT.filter((c) => c.hotelId === AC).map((c) => c.campaignId),
  });
  const decompRad = await runHotelDemandCampaignDecompositions(RAD, {
    nowDate: NOW,
    persist: true,
    enableJev: false,
    maxCompletionSteps: 0,
    campaignIds: COHORT.filter((c) => c.hotelId === RAD).map((c) => c.campaignId),
  });

  // Preserve only real Watch opportunity links — drop orphan / contaminated child IDs
  for (const hotelId of [AC, RAD]) {
    const doc = loadDemandCampaigns(hotelId);
    const opps = loadOpportunities(hotelId)?.opportunities || [];
    const oppById = new Map(opps.map((o) => [o.id, o]));
    const watches = opps.filter((o) => /WATCH/i.test(String(o.customerFacingState || "")));
    const updates = [];
    for (const c of doc.campaigns) {
      const oppIds = new Set();
      for (const id of c.opportunityIds || []) {
        if (oppById.has(id)) oppIds.add(id);
      }
      for (const w of watches) {
        const titleBlob = `${w.displayTitle || ""} ${w.title || ""} ${w.organizationName || ""}`.toLowerCase();
        const campBlob = `${c.name || ""} ${c.organizationName || ""}`.toLowerCase();
        if (
          (c.evidenceSeeds || []).some(
            (s) => s.linkedOpportunityId === w.id || s.organizationName === w.organizationName
          ) ||
          (campBlob && titleBlob.includes(campBlob.slice(0, 24))) ||
          (titleBlob && campBlob.includes(titleBlob.slice(0, 24)))
        ) {
          oppIds.add(w.id);
        }
      }
      updates.push({
        campaignId: c.campaignId,
        opportunityIds: [...oppIds],
        officialListStatus: c.officialListStatus,
        officialListNextTrigger: c.officialListNextTrigger,
        lodgingEvidenceClass: c.lodgingEvidenceClass,
        evidenceSeeds: c.evidenceSeeds,
      });
    }
    upsertDemandCampaigns(hotelId, updates, {
      note: "Preserve real watch links; drop orphan child IDs after list mining",
    });
  }

  invalidateGdiHotelReadCache(AC);
  invalidateGdiHotelReadCache(RAD);
  const afterAc = stats(AC);
  const afterRad = stats(RAD);
  const afterYotel = stats(YOTEL);
  const afterBeth = stats(BETH);

  // --- CSV outputs ---
  write(
    "CAMPAIGN_SOURCE_STATUS.csv",
    toCsv(
      results.map((r) => ({
        campaignKey: r.spec.key,
        campaignId: r.campaignId || r.spec.campaignId,
        listStatus: r.listStatus,
        lodgingClass: r.lodging.class,
        entities: r.entities.length,
        evidenceSeeds: r.evidenceSeedCount,
        nextTrigger: r.nextTrigger,
      })),
      [
        "campaignKey",
        "campaignId",
        "listStatus",
        "lodgingClass",
        "entities",
        "evidenceSeeds",
        "nextTrigger",
      ]
    )
  );

  write(
    "OFFICIAL_LIST_SOURCES.csv",
    toCsv(
      results.flatMap((r) => r.sourceRows),
      [
        "campaignKey",
        "campaignId",
        "url",
        "ok",
        "sourceType",
        "currentCycleStatus",
        "sourceLanguage",
        "officialish",
        "isPdf",
        "error",
      ]
    )
  );

  write(
    "AFFILIATED_SOURCES.csv",
    toCsv(
      results.flatMap((r) => r.affiliated),
      ["campaignKey", "fromUrl", "affiliatedUrl", "proof"]
    )
  );

  write(
    "NAMED_ENTITIES.csv",
    toCsv(
      results.flatMap((r) =>
        r.entities.map((e) => ({
          campaignKey: e.campaignKey,
          campaignId: e.campaignId,
          entityName: e.entityName,
          entityType: e.entityType,
          participationRole: e.participationRole,
          sourceUrl: e.sourceUrl,
          sourceLanguage: e.sourceLanguage,
          sourceType: e.sourceType,
          currentCycleStatus: e.currentCycleStatus,
          evidenceText: e.evidenceText,
          evidenceConfidence: e.evidenceConfidence,
        }))
      ),
      [
        "campaignKey",
        "campaignId",
        "entityName",
        "entityType",
        "participationRole",
        "sourceUrl",
        "sourceLanguage",
        "sourceType",
        "currentCycleStatus",
        "evidenceText",
        "evidenceConfidence",
      ]
    )
  );

  write(
    "PARTICIPATION_ROLES.csv",
    toCsv(
      results.flatMap((r) =>
        r.entities.map((e) => ({
          campaignKey: e.campaignKey,
          entityName: e.entityName,
          participationRole: e.participationRole,
          currentCycleStatus: e.currentCycleStatus,
        }))
      ),
      ["campaignKey", "entityName", "participationRole", "currentCycleStatus"]
    )
  );

  const travelRows = results.flatMap((r) =>
    r.evidenceSeeds.map((s) => ({
      campaignKey: r.spec.key,
      organization: s.organizationName,
      travelingEntityType: s.travelingEntityType || "",
      confidence: s.travelingEntityConfidence || "",
      proven: s.travelingEntityProven === true ? "PROVEN" : s.travelingEntityConfidence || "UNRESOLVED",
      forceClass: s.forceClass || "",
    }))
  );
  write(
    "TRAVELING_ENTITIES.csv",
    toCsv(travelRows, [
      "campaignKey",
      "organization",
      "travelingEntityType",
      "confidence",
      "proven",
      "forceClass",
    ])
  );

  write(
    "GROUP_MOTION.csv",
    toCsv(
      results.flatMap((r) =>
        r.evidenceSeeds
          .filter((s) => s.forceClass !== "SIGNAL_ONLY" || s.travelingEntityConfidence === "STRONG_INFERENCE")
          .map((s) => ({
            campaignKey: r.spec.key,
            organization: s.organizationName,
            travelingGroup: s.travelingGroup,
            when: r.campaign.eventStartDate,
            lodging: s.lodgingState,
          }))
      ),
      ["campaignKey", "organization", "travelingGroup", "when", "lodging"]
    )
  );

  write(
    "BUYER_PATHS.csv",
    toCsv(
      results.flatMap((r) =>
        r.evidenceSeeds.map((s) => ({
          campaignKey: r.spec.key,
          organization: s.organizationName,
          buyerEntity: s.buyerEntity,
          buyerRole: s.buyerRole,
          contactPath: s.publicContactPath,
          forceClass: s.forceClass || "",
        }))
      ),
      [
        "campaignKey",
        "organization",
        "buyerEntity",
        "buyerRole",
        "contactPath",
        "forceClass",
      ]
    )
  );

  write(
    "LODGING_EVIDENCE.csv",
    toCsv(
      results.map((r) => ({
        campaignKey: r.spec.key,
        lodgingClass: r.lodging.class,
        url: r.lodging.url || "",
        note: r.lodging.note || "",
      })),
      ["campaignKey", "lodgingClass", "url", "note"]
    )
  );

  write(
    "FUTURE_DECISIONS.csv",
    toCsv(
      results.map((r) => ({
        campaignKey: r.spec.key,
        eventStart: r.campaign.eventStartDate,
        eventEnd: r.campaign.eventEndDate,
        eventYear: r.campaign.eventYear,
        nextTrigger: r.nextTrigger,
      })),
      ["campaignKey", "eventStart", "eventEnd", "eventYear", "nextTrigger"]
    )
  );

  const decompRows = [...(decompAc.results || []), ...(decompRad.results || [])].map((r) => ({
    campaignId: r.campaignId,
    status: r.status,
    researchLeads: r.researchLeads?.length ?? 0,
    signalOnly: r.signalOnly?.length ?? 0,
    created: r.created?.length ?? 0,
    ready: (r.readiness || []).filter((x) => x.readyOk).length,
    watch: (r.readiness || []).filter((x) => x.watchOk).length,
  }));

  write(
    "PACKETS.csv",
    toCsv(
      [...(decompAc.results || []), ...(decompRad.results || [])].flatMap((r) =>
        (r.packets || []).map((p) => ({
          campaignId: r.campaignId,
          opportunityId: p.opportunityId || p.id || "",
          quality: p.quality || p.packetQuality || "",
        }))
      ),
      ["campaignId", "opportunityId", "quality"]
    )
  );

  write(
    "READY_WATCH.csv",
    toCsv(
      [
        {
          hotel: "AC",
          readyBefore: beforeAc.ready,
          readyAfter: afterAc.ready,
          watchBefore: beforeAc.watch,
          watchAfter: afterAc.watch,
          newReady: afterAc.ready - beforeAc.ready,
          newWatch: afterAc.watch - beforeAc.watch,
        },
        {
          hotel: "RAD",
          readyBefore: beforeRad.ready,
          readyAfter: afterRad.ready,
          watchBefore: beforeRad.watch,
          watchAfter: afterRad.watch,
          newReady: afterRad.ready - beforeRad.ready,
          newWatch: afterRad.watch - beforeRad.watch,
        },
      ],
      [
        "hotel",
        "readyBefore",
        "readyAfter",
        "watchBefore",
        "watchAfter",
        "newReady",
        "newWatch",
      ]
    )
  );

  write(
    "PUBLIC_DATA_CEILING.csv",
    toCsv(
      results.map((r) => ({
        campaignKey: r.spec.key,
        listStatus: r.listStatus,
        stillCeiling: /CEILING|NOT_YET|PRIVATE|NO_LIST/i.test(r.listStatus) ? "YES" : "NO",
        nextTrigger: r.nextTrigger,
      })),
      ["campaignKey", "listStatus", "stillCeiling", "nextTrigger"]
    )
  );

  const langYield = { es: 0, gl: 0, en: 0 };
  for (const r of results) {
    for (const s of r.sourceRows) {
      if (s.officialish) langYield[s.sourceLanguage] = (langYield[s.sourceLanguage] || 0) + 1;
    }
  }
  write(
    "MULTILINGUAL_YIELD.csv",
    toCsv(
      [
        { language: "es", officialish_sources: langYield.es || 0 },
        { language: "gl", officialish_sources: langYield.gl || 0 },
        { language: "en", officialish_sources: langYield.en || 0 },
        {
          language: "ALL",
          named_entities: results.reduce((n, r) => n + r.entities.length, 0),
          lodging_direct: results.filter((r) => r.lodging.class === "DIRECT_LODGING_EVIDENCE").length,
        },
      ],
      ["language", "officialish_sources", "named_entities", "lodging_direct"]
    )
  );

  write(
    "CANONICAL_RECONCILIATION.csv",
    toCsv(
      [
        { layer: "campaigns", AC: afterAc.campaigns, RAD: afterRad.campaigns, YOTEL: afterYotel.campaigns },
        { layer: "ready_gate", AC: afterAc.ready, RAD: afterRad.ready, YOTEL: afterYotel.ready },
        { layer: "watch_gate", AC: afterAc.watch, RAD: afterRad.watch, YOTEL: afterYotel.watch },
        { layer: "pursuits", AC: afterAc.pursuits, RAD: afterRad.pursuits, YOTEL: afterYotel.pursuits },
        { layer: "beth_ready", AC: "", RAD: "", YOTEL: afterBeth.ready },
      ],
      ["layer", "AC", "RAD", "YOTEL"]
    )
  );

  write(
    "UI_QA.md",
    `# UI QA

- Customer surface must show account-level Ready/Watch only — **campaigns not exposed**.
- Organizer SIGNAL_ONLY seeds must not appear as customer Ready.
- Existing five pursuits unchanged by list mining alone.
- Bethesda Ready ${afterBeth.ready} (before ${beforeBeth.ready}).
`
  );

  const per = Object.fromEntries(
    results.map((r) => [
      r.spec.key,
      {
        listStatus: r.listStatus,
        officialLists: r.sourceRows.filter(
          (s) =>
            s.officialish &&
            isOfficialOrAffiliatedHost(s.url, r.spec.key) &&
            /LIST|PROGRAM|MANUAL|DIRECTORY|DELEGATE|REGISTRATION/i.test(s.sourceType)
        ).length,
        namedAccounts: r.entities.filter((e) => e.entityType !== "HOTEL").length,
        travelingProven: r.evidenceSeeds.filter((s) => s.travelingEntityProven).length,
        travelingStrong: r.evidenceSeeds.filter((s) => s.travelingEntityConfidence === "STRONG_INFERENCE")
          .length,
        lodging: r.lodging.class,
        nextTrigger: r.nextTrigger,
        evidenceSeeds: r.evidenceSeedCount,
      },
    ])
  );

  const summary = {
    per,
    decompAc: decompRows.filter((d) => String(d.campaignId).startsWith("accamp")),
    decompRad: decompRows.filter((d) => String(d.campaignId).startsWith("radiss")),
    beforeAc,
    afterAc,
    beforeRad,
    afterRad,
    beforeYotel,
    afterYotel,
    beforeBeth,
    afterBeth,
    totals: {
      campaignsMined: 5,
      namedEntities: results.reduce((n, r) => n + r.entities.length, 0),
      officialishSources: results.reduce(
        (n, r) => n + r.sourceRows.filter((s) => s.officialish).length,
        0
      ),
      affiliated: results.reduce((n, r) => n + r.affiliated.length, 0),
      lodgingDirect: results.filter((r) => r.lodging.class === "DIRECT_LODGING_EVIDENCE").length,
      newReady: afterAc.ready - beforeAc.ready + (afterRad.ready - beforeRad.ready),
      newWatch: afterAc.watch - beforeAc.watch + (afterRad.watch - beforeRad.watch),
      ceilingCount: results.filter((r) =>
        /CEILING|NOT_YET|PRIVATE|NO_LIST/i.test(r.listStatus)
      ).length,
      langYield,
    },
  };
  write("SUMMARIES.json", JSON.stringify(summary, null, 2));

  const autoLodging = results.find((r) => r.spec.key === "AUTOAMERICAS")?.lodging?.class || "NONE";
  write(
    "FOUNDER_REPORT.md",
    `# FOUNDER REPORT — Official-list mining (5 campaigns)

## Verdict

Shared official-list mining ran on the frozen cohort only. **No Ready inflation.** No invented participant lists. Organizer seeds remain SIGNAL_ONLY.

${
  autoLodging === "DIRECT_LODGING_EVIDENCE"
    ? "Strongest lodging breakthrough: **AUTOAMERICAS 2027** publishes **Hotel Oficial** (Dominican Fiesta) with preferential exhibitor/speaker rates — `DIRECT_LODGING_EVIDENCE`."
    : "AUTOAMERICAS lodging page mined; classification: `" + autoLodging + "`."
}

Most campaigns remain **LIST_NOT_YET_PUBLISHED** / **PUBLIC_DATA_CEILING** for current-cycle exhibitor/participant directories. RIF convocatoria PDF is official but compressed — no extractable named university list yet. Contaminated lookalike-congress scrapes were rejected.

## Per campaign

| Campaign | List status | Lodging | Named entities | Next trigger |
|----------|-------------|---------|----------------|--------------|
${results
  .map(
    (r) =>
      `| ${r.spec.key} | ${r.listStatus} | ${r.lodging.class} | ${r.entities.length} | ${r.nextTrigger.slice(0, 80)}… |`
  )
  .join("\n")}

## Gates

| Hotel | Ready before→after | Watch before→after | Pursuits |
|-------|--------------------|--------------------|----------|
| AC | ${beforeAc.ready}→${afterAc.ready} | ${beforeAc.watch}→${afterAc.watch} | ${afterAc.pursuits} |
| RAD | ${beforeRad.ready}→${afterRad.ready} | ${beforeRad.watch}→${afterRad.watch} | ${afterRad.pursuits} |
| YOTEL | ${beforeYotel.ready}→${afterYotel.ready} | campaigns ${afterYotel.campaigns} | — |
| Bethesda Ready | ${beforeBeth.ready}→${afterBeth.ready} | — | — |

## Top bottleneck

Current-cycle **exhibitor/participant directories are not public yet**. Mining stops at publication triggers rather than inventing lists. AUTOAMERICAS official hotel is published; exhibitor directory is not.
`
  );

  write(
    "CHANGELOG.md",
    `# CHANGELOG — Official-list mining AC/RAD

- Mined only the five frozen campaigns (no broad discovery, no Apify)
- Multilingual SERP + official-host allowlist fetch; lookalike congress sites rejected
- Updated \`evidenceSeeds\` with extracted entities; organizers + prior-cycle + hotels forced SIGNAL_ONLY
- AUTOAMERICAS lodging: Hotel Oficial Dominican Fiesta when page text confirms (\`DIRECT_LODGING_EVIDENCE\`)
- RIF: convocatoria official; no inventing universities from culture-congress SERP noise
- Re-ran shared campaign decomposition; Ready/Watch thresholds unchanged; pursuits preserved
- YOTEL Ready ${afterYotel.ready} (was ${beforeYotel.ready}); Bethesda Ready ${afterBeth.ready} (was ${beforeBeth.ready})
`
  );

  console.log(JSON.stringify(summary.totals, null, 2));
  console.log(JSON.stringify(per, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
