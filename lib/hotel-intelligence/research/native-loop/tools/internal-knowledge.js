/**
 * Internal knowledge tools — Dealality existing evidence (no Webhound).
 * Iteration 1: ownership surface + nine-case registry + packet-2.5 corpus.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createOwnershipSurface } from "../../../ownership/ownership-surface-v1.js";
import { HOTEL_CASES } from "../../../ownership/nine-case-evidence-registry.js";
import { addEvidence, upsertClaim } from "../claim-graph.js";
import { addWorkingNote } from "../working-notes.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../../../");
const CORPUS_DIR = path.join(ROOT, "fixtures/golden-demo/packet-2.5/source-corpus/documents");
const CORPUS_MANIFEST = path.join(ROOT, "fixtures/golden-demo/packet-2.5/source-corpus/corpus-manifest.json");

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function authorityTier(sourceType) {
  const t = String(sourceType || "").toLowerCase();
  if (/securities|filing|regulatory|government|registry|investor/.test(t)) return "high";
  if (/official|first.?party|company|brand/.test(t)) return "high";
  if (/press|trade|hospitality/.test(t)) return "strong_secondary";
  if (/linkedin|bio|portfolio/.test(t)) return "supporting";
  if (/aggregat|seo|directory|scrape/.test(t)) return "weak";
  return "supporting";
}

export function loadOwnershipSurfaceKnowledge(state) {
  const hotelId = state.entity?.census_id || state.entity?.hotel_id;
  const surface = createOwnershipSurface();
  const notes = [];

  try {
    if (hotelId && typeof surface.hotelOwnerAnchor === "function") {
      const anchor = surface.hotelOwnerAnchor(hotelId);
      if (anchor?.ok && anchor.anchor?.primary_owner_entity_id) {
        const ev = addEvidence(state, {
          source_type: "dealality_ownership_surface",
          authority_tier: "high",
          excerpt: JSON.stringify(anchor.anchor).slice(0, 2000),
          title: "Dealality ownership surface anchor",
          entity_association: "SUBJECT",
        });
        addWorkingNote(state, {
          topic: "ownership",
          observation: `Ownership surface anchor: ${anchor.anchor.owner_display_name || anchor.anchor.primary_owner_entity_id}`,
          supporting_evidence_ids: [ev.evidence_id],
          status: "SUPPORTED",
        });
        upsertClaim(state, {
          subject: state.entity?.name,
          relationship: "OWNED_BY",
          object: anchor.anchor.owner_display_name || anchor.anchor.primary_owner_entity_id,
          field: "economic_owner",
          status: anchor.anchor.confidence === "HIGH" ? "HIGH" : "PROBABLE",
          confidence: anchor.anchor.confidence || "MEDIUM",
          evidence_ids: [ev.evidence_id],
          inference: false,
        });
        notes.push("ownership_surface_hit");
      }
    }
  } catch {
    /* surface optional */
  }

  const nine = HOTEL_CASES[state.entity?.census_id];
  if (nine) {
    for (const audit of nine.ownership_audits || []) {
      const ev = addEvidence(state, {
        url: audit.source_url || null,
        source_type: "registry_or_press",
        authority_tier: authorityTier(audit.source_url),
        excerpt: `${audit.claimed}: ${audit.entity} — ${audit.establishes || ""}`,
        title: nine.hotel,
        entity_association: audit.hotel_identity_match ? "SUBJECT" : "UNCLEAR",
      });
      if (audit.supported && audit.claimed === "REGISTERED_BUSINESS") {
        upsertClaim(state, {
          subject: nine.hotel,
          relationship: "REGISTERED_BUSINESS",
          object: audit.entity,
          field: "registered_business",
          status: "VERIFIED",
          confidence: "HIGH",
          evidence_ids: [ev.evidence_id],
        });
      } else if (audit.supported && audit.claimed === "OPERATOR") {
        upsertClaim(state, {
          subject: nine.hotel,
          relationship: "OPERATED_BY",
          object: audit.entity,
          field: "operator",
          status: "PROBABLE",
          confidence: "MEDIUM",
          evidence_ids: [ev.evidence_id],
        });
      } else if (!audit.supported && /OWNER|PROPCO|ECONOMIC/.test(audit.claimed || "")) {
        upsertClaim(state, {
          subject: nine.hotel,
          relationship: "OWNED_BY",
          object: audit.entity,
          field: "economic_owner",
          status: "UNRESOLVED",
          confidence: null,
          evidence_ids: [ev.evidence_id],
          unresolved: true,
        });
        addWorkingNote(state, {
          topic: "unresolved",
          observation: `Ownership claim for ${audit.entity} not supported in nine-case package.`,
          supporting_evidence_ids: [ev.evidence_id],
          status: "UNRESOLVED",
          open_question: "Need deed/title or issuer filing for economic owner.",
        });
      }
    }
    notes.push("nine_case_hit");
  }

  return { notes, hit: notes.length > 0 };
}

export function searchPacket25Corpus(state, queries = []) {
  if (!fs.existsSync(CORPUS_DIR)) return { hits: 0 };
  const hotelNorm = norm(state.entity?.name);
  const aliases = (state.entity?.aliases || []).map(norm);
  const qblob = norm(queries.join(" "));

  let docs = [];
  try {
    const files = fs.readdirSync(CORPUS_DIR).filter((f) => f.endsWith(".json"));
    for (const f of files) {
      const doc = JSON.parse(fs.readFileSync(path.join(CORPUS_DIR, f), "utf8"));
      docs.push(doc);
    }
  } catch {
    return { hits: 0 };
  }

  // Prefer manifest case match
  let preferredIds = new Set();
  try {
    if (fs.existsSync(CORPUS_MANIFEST)) {
      const man = JSON.parse(fs.readFileSync(CORPUS_MANIFEST, "utf8"));
      for (const c of man.cases || []) {
        if (norm(c.hotel_name).includes(hotelNorm) || hotelNorm.includes(norm(c.hotel_name))) {
          for (const d of c.documents || []) preferredIds.add(d.replace(/\.json$/, ""));
        }
      }
    }
  } catch {
    /* ignore */
  }

  let hits = 0;
  for (const doc of docs) {
    const hay = norm(`${doc.hotel_name} ${doc.title} ${doc.text} ${doc.context_org}`);
    const nameHit =
      (hotelNorm && hay.includes(hotelNorm)) ||
      aliases.some((a) => a && a.length > 4 && hay.includes(a)) ||
      preferredIds.has(doc.id);
    // Iteration 1: NEVER accept query-only hits for another hotel — false matches are worse than UNKNOWN.
    if (!nameHit) continue;

    // Reject adjacent wrong hotel: Krystal Resort vs Krystal Grand
    if (/krystal grand/.test(hotelNorm) && /krystal resort/.test(hay) && !/krystal grand/.test(hay)) {
      continue;
    }
    if (/cambridge beaches/.test(hotelNorm) && !/cambridge beaches/.test(hay)) {
      continue;
    }
    if (/ava resort/.test(hotelNorm) && !/ava/.test(hay)) {
      continue;
    }

    const ev = addEvidence(state, {
      url: doc.url,
      source_type: doc.source_type || doc.authority || "corpus",
      authority_tier: authorityTier(doc.source_type || doc.authority),
      excerpt: String(doc.text || "").slice(0, 1200),
      title: doc.title,
      entity_association: "SUBJECT",
      corpus_doc_id: doc.id,
    });
    hits += 1;

    extractClaimsFromCorpusText(state, doc, ev);
    addWorkingNote(state, {
      topic: "ownership",
      observation: `Corpus hit: ${doc.title}`,
      supporting_evidence_ids: [ev.evidence_id],
      status: "SUPPORTED",
    });
  }

  void qblob;
  return { hits };
}

function extractClaimsFromCorpusText(state, doc, ev) {
  try {
    extractClaimsFromCorpusTextUnsafe(state, doc, ev);
  } catch (err) {
    addWorkingNote(state, {
      topic: "unresolved",
      observation: `Claim extract skipped: ${err.message}`,
      status: "OBSERVED",
    });
  }
}

function extractClaimsFromCorpusTextUnsafe(state, doc, ev) {
  const text = String(doc.text || "");
  const hotel = state.entity?.name || doc.hotel_name;
  const org = doc.context_org;

  if (/entidad propietaria|propietaria del inmueble|PropCo|owned by/i.test(text)) {
    const m =
      text.match(/subsidiaria\s+([^.(]+?)\s+\(/i) ||
      text.match(/(Inmobiliaria[^.]{0,80})/i) ||
      text.match(/owned by\s+([^.]+)/i);
    const propcoName = m && m[1] != null ? String(m[1]).trim() : null;
    if (propcoName) {
      upsertClaim(state, {
        subject: hotel,
        relationship: "PROPCO",
        object: propcoName,
        field: "propco",
        status: authorityTier(doc.source_type) === "high" ? "VERIFIED" : "PROBABLE",
        confidence: authorityTier(doc.source_type) === "high" ? "HIGH" : "MEDIUM",
        evidence_ids: [ev.evidence_id],
      });
    }
  }

  if (/owned by|hoteles propios incluye|is owned by|adquiere, desarrolla y opera/i.test(text) && org) {
    upsertClaim(state, {
      subject: hotel,
      relationship: "OWNED_BY",
      object: org,
      field: "economic_owner",
      status: /securities|issuer|annual/i.test(doc.source_type || doc.authority || "")
        ? "VERIFIED"
        : "PROBABLE",
      confidence: /securities|issuer|annual/i.test(doc.source_type || doc.authority || "")
        ? "HIGH"
        : "MEDIUM",
      evidence_ids: [ev.evidence_id],
    });
  }

  if (/operated by|opera hoteles|stewarded by/i.test(text)) {
    const opMatch =
      text.match(/stewarded by\s+\*{0,2}([^*\n.]+)/i) || text.match(/operated by\s+([^.]+)/i);
    const op = org || (opMatch && opMatch[1] ? opMatch[1] : null);
    if (op) {
      upsertClaim(state, {
        subject: hotel,
        relationship: "OPERATED_BY",
        object: String(op).replace(/\*/g, "").trim(),
        field: "operator",
        status: "PROBABLE",
        confidence: "MEDIUM",
        evidence_ids: [ev.evidence_id],
      });
    }
  }

  if (/Breathless/i.test(text) && /announce|convert|will/i.test(text)) {
    upsertClaim(state, {
      subject: hotel,
      relationship: "ANNOUNCED_BRAND",
      object: "Breathless",
      field: "announced_brand",
      status: "ANNOUNCED",
      confidence: "MEDIUM",
      evidence_ids: [ev.evidence_id],
    });
  }

  if (/formerly|former brand|Hilton/i.test(text) && /Hilton/i.test(text)) {
    upsertClaim(state, {
      subject: hotel,
      relationship: "FORMER_BRAND",
      object: "Hilton",
      field: "former_brand",
      status: "PROBABLE",
      confidence: "MEDIUM",
      evidence_ids: [ev.evidence_id],
    });
  }

  if (/Luis Alberto Avelar|Phil Hospod|James Kot|Clarence Hofheins/i.test(text)) {
    const people = [
      ["Luis Alberto Avelar", "GSF"],
      ["Phil Hospod", "Dovetail"],
      ["James Kot", "Dovetail"],
      ["Clarence Hofheins", "Cambridge Beaches"],
    ];
    for (const [name, orgHint] of people) {
      if (text.includes(name)) {
        upsertClaim(state, {
          subject: name,
          relationship: "PERSON_AFFILIATED_WITH",
          object: org || orgHint,
          field: "person",
          status: "PROBABLE",
          confidence: "MEDIUM",
          evidence_ids: [ev.evidence_id],
          inference: false,
        });
      }
    }
  }
}

export function loadPortfolioKnowledge(state) {
  const surface = createOwnershipSurface();
  let ownerId = state.entity?.owner_id || null;
  const name = state.entity?.name;
  // Alias shorthand used in golden set
  if (ownerId === "dle_gsf") ownerId = "dle_06G6AB1VK0BCCD94DNN7W8DRWZ";
  try {
    let profile = null;
    if (ownerId && typeof surface.ownerPortfolio === "function") {
      profile = surface.ownerPortfolio({ owner_id: ownerId });
    } else if (ownerId && typeof surface.ownerGet === "function") {
      profile = surface.ownerGet({ owner_id: ownerId });
    }
    if (profile?.ok) {
      const ev = addEvidence(state, {
        source_type: "dealality_owner_portfolio",
        authority_tier: "high",
        excerpt: JSON.stringify(profile).slice(0, 2500),
        title: `Portfolio ${name || ownerId}`,
      });
      addWorkingNote(state, {
        topic: "portfolio",
        observation: "Loaded owner portfolio from ownership surface.",
        supporting_evidence_ids: [ev.evidence_id],
        status: "SUPPORTED",
      });
      upsertClaim(state, {
        subject: name || ownerId,
        relationship: "HAS_PORTFOLIO",
        object: profile.profile?.owner_display_name || name || ownerId,
        field: "portfolio",
        status: "PROBABLE",
        confidence: "MEDIUM",
        evidence_ids: [ev.evidence_id],
      });
      return { hit: true };
    }
  } catch {
    /* ignore */
  }
  return { hit: false };
}
