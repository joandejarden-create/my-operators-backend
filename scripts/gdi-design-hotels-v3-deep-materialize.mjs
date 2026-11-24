#!/usr/bin/env node
/**
 * Merge CohortExecute V3 deep-researched opportunities into local hotel inventory.
 * Preserves prior inventory IDs; appends new deep IDs; no Airtable / no CURRENT.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { GDI_CROSS_SEGMENT_OPPORTUNITY_DEDUPE_V2 } from "../lib/group-demand-intelligence/research/gdi-cross-segment-opportunity-dedupe-v2.js";
import { evaluateVerifiedCommercialRoute } from "../lib/group-demand-intelligence/presentation/gdi-verified-commercial-route-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/gdi-design-hotels-full-e2e-commercial-v3-paid",
);

const HOTELS = [
  { hotelId: "rece0NdV9FCnaFlVS", hotelName: "Condesa DF" },
  { hotelId: "recaokE8psIbuVeuH", hotelName: "LaLuna Boutique Hotels & Villas" },
  { hotelId: "rec1t35e3sPerzxYu", hotelName: "Memmo Príncipe Real" },
];

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function plausible(name) {
  const n = text(name);
  if (!n || n.length < 5 || n.length > 80) return false;
  if (
    /organization contact desk|member\s*(select|log)|website feedback|pennsylvania|place brand|belgium\s*tel|survey software|amsterdam|the netherlands|mexico city|^george grenada$|reduction forum|developers|conference|congress|exhibition|software|committee/i.test(
      n,
    )
  ) {
    return false;
  }
  const parts = n.split(/\s+/);
  if (parts.length < 2 || parts.length > 5) return false;
  return parts.filter((p) => /^[\p{L}][\p{L}'\-]{1,30}$/u.test(p)).length >= 2;
}

function pickRun(hotelId) {
  const base = path.join(REPORT, "hotels", hotelId);
  return fs
    .readdirSync(base)
    .map((n) => path.join(base, n))
    .sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs)[0];
}

async function main() {
  const summary = [];
  for (const hotel of HOTELS) {
    const runDir = pickRun(hotel.hotelId);
    const screening = JSON.parse(
      fs.readFileSync(path.join(runDir, "pre-deep-candidate-screening.json"), "utf8"),
    );
    const deep = JSON.parse(
      fs.readFileSync(
        path.join(runDir, "deep-research-selection-register.json"),
        "utf8",
      ),
    );
    const trace = JSON.parse(
      fs.readFileSync(path.join(runDir, "chain-execution-trace.json"), "utf8"),
    );
    const deepIds = new Set(
      (deep.selected || [])
        .map((s) => text(s.id || s.opportunityId))
        .filter(Boolean),
    );
    const contactById = new Map();
    for (const row of trace.processed || []) {
      const id = text(row.opportunityId || row.id);
      const name = text(row.primaryContact?.name || row.primaryContactName);
      contactById.set(id, {
        primaryContact: plausible(name)
          ? {
              name,
              role: text(row.primaryContact?.role) || null,
              organization: text(row.organizationName) || null,
              customerFacingNamed: true,
              source: "v3_chain",
            }
          : null,
        namedStakeholders: plausible(name)
          ? [{ name, role: text(row.primaryContact?.role) || null }]
          : [],
        disposition: row.disposition || null,
      });
    }

    const byId = new Map();
    for (const c of screening.eligibleCandidates || []) {
      const id = text(c.id || c.opportunityId);
      if (id) byId.set(id, c);
    }
    for (const row of trace.processed || []) {
      const id = text(row.opportunityId || row.id);
      if (!id || byId.has(id)) continue;
      byId.set(id, {
        id,
        opportunityId: id,
        title: row.title || row.programmeName || row.organizationName,
        organizationName: row.organizationName,
        researchSegment: row.segmentId,
        hotelOpportunityThesis: null,
        seedUrls: [],
        investigatedUrls: [],
        evidence: [],
      });
    }

    const selected = [...byId.values()].filter((c) => {
      const id = text(c.id || c.opportunityId);
      return deepIds.has(id) || contactById.has(id);
    });

    const newOpps = selected.map((raw) => {
      const id = text(raw.id || raw.opportunityId);
      const c = contactById.get(id) || {};
      const urls = [
        ...(raw.seedUrls || []),
        ...(raw.investigatedUrls || []),
        ...((raw.evidence || []).map((e) => e?.url).filter(Boolean)),
      ];
      const routeUrl = urls.find((u) =>
        /contact|organiser|organizer|committee|team|about|lodging|accommodation|housing|hotel/i.test(
          String(u),
        ),
      );
      const thesis =
        text(raw.hotelOpportunityThesis) ||
        (raw.title && raw.organizationName
          ? `${raw.organizationName} — ${raw.title}: investigate hotel-relevant group / lodging / meetings demand for ${hotel.hotelName}.`
          : text(raw.title) || text(raw.organizationName));
      const primary = c.primaryContact || null;
      let approvedCommercialRoute = null;
      let commercialContactRoute = null;
      if (!primary && routeUrl && raw.organizationName) {
        const candidateLabel = `Official ${raw.organizationName} organizing / lodging channel`;
        const routeGate = evaluateVerifiedCommercialRoute({
          label: candidateLabel,
          url: routeUrl,
        });
        if (routeGate.ok) {
          approvedCommercialRoute = candidateLabel;
          commercialContactRoute = routeUrl;
        }
      }
      let recommendedNextStep = null;
      if (primary) {
        recommendedNextStep = `Research outreach to ${primary.name}${
          primary.role ? ` (${primary.role})` : ""
        } at ${raw.organizationName || "the organization"} — confirm lodging decision role and propose property-fit options.`;
      } else if (approvedCommercialRoute) {
        recommendedNextStep = `Engage via verified commercial route (${approvedCommercialRoute}) — confirm group lodging needs and request the appropriate organizer contact.`;
      }
      return {
        id,
        opportunityId: id,
        hotelId: hotel.hotelId,
        hotelName: hotel.hotelName,
        title: raw.title || raw.programmeName || raw.organizationName || null,
        organizationName: raw.organizationName || null,
        organization: raw.organizationName || null,
        programmeName: raw.programmeName || null,
        researchSegment: raw.researchSegment || raw.segmentId || null,
        segmentAssociations:
          raw.segmentAssociations ||
          [raw.researchSegment || raw.segmentId].filter(Boolean),
        demandType: raw.demandType || null,
        hotelOpportunityThesis: thesis,
        evidence: raw.evidence || null,
        acceptedClaims: raw.acceptedClaims || null,
        seedUrls: raw.seedUrls || [],
        investigatedUrls: raw.investigatedUrls || [],
        sourceUrls: urls.filter(Boolean),
        namedStakeholders: c.namedStakeholders || [],
        primaryContact: primary,
        approvedCommercialRoute,
        commercialContactRoute,
        marketScopeClass: raw.marketScopeClass || "Hotel",
        commercialDisposition: c.disposition || "WATCHLIST_RESEARCH",
        salesReady: false,
        customerPublicationReady: false,
        publicationOccurred: false,
        deepResearched: true,
        researchLifecycleStatus: "DEEP_RESEARCHED",
        recommendedNextStep,
        materialization: {
          source: "gdi-design-hotels-full-e2e-commercial-v3-paid",
          runDir,
          schema: "gdi-design-hotels-v3-deep-materialize",
          notPublication: true,
          dedupeContract: GDI_CROSS_SEGMENT_OPPORTUNITY_DEDUPE_V2,
        },
      };
    });

    const doc = await loadOpportunitiesCanonical(hotel.hotelId, {
      bypassCache: true,
    });
    const existing = doc.opportunities || [];
    const existingIds = new Set(
      existing.map((o) => text(o.opportunityId || o.id)),
    );
    let added = 0;
    for (const o of newOpps) {
      if (existingIds.has(o.opportunityId)) {
        const ex = existing.find(
          (x) => text(x.opportunityId || x.id) === o.opportunityId,
        );
        if (!ex) continue;
        if (!ex.deepResearched) {
          ex.deepResearched = true;
          ex.researchLifecycleStatus = "DEEP_RESEARCHED";
        }
        if (o.primaryContact?.name && !ex.primaryContact?.name) {
          ex.primaryContact = o.primaryContact;
        }
        if (o.approvedCommercialRoute && !ex.approvedCommercialRoute) {
          ex.approvedCommercialRoute = o.approvedCommercialRoute;
          ex.commercialContactRoute = o.commercialContactRoute;
        }
        if (
          o.recommendedNextStep &&
          text(ex.recommendedNextStep).length < 20
        ) {
          ex.recommendedNextStep = o.recommendedNextStep;
        }
      } else {
        existing.push(o);
        existingIds.add(o.opportunityId);
        added += 1;
      }
    }

    await saveOpportunitiesCanonical(hotel.hotelId, {
      ...doc,
      opportunities: existing,
      updatedAt: new Date().toISOString(),
      note: "v3 paid deep materialize merge — prior inventory preserved; new deep IDs appended",
    });

    summary.push({
      hotelId: hotel.hotelId,
      runDir,
      newOpps: newOpps.length,
      added,
      inventoryAfter: existing.length,
      withPrimary: existing.filter((o) => o.primaryContact?.name).length,
      withRoute: existing.filter((o) => o.approvedCommercialRoute).length,
      deep: existing.filter((o) => o.deepResearched).length,
    });
  }

  fs.writeFileSync(
    path.join(REPORT, "V3_DEEP_MATERIALIZE.json"),
    `${JSON.stringify(summary, null, 2)}\n`,
  );
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
