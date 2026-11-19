/**
 * Hotel Explorer Founder Preview — GET-only.
 * Live Airtable = canonical ownership API.
 * Phase 3 Proposed = live base + staged dry-run overlay (never written by this route).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getGoldenDemoHotelOwnership } from "./golden-demo-ownership.js";
import { projectEvidenceToSourcesCitations } from "../lib/hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DRY_RUN_PATH = path.join(
  ROOT,
  "reports/hotel-explorer/phase3-research/HE-BETH-PHASE3-HI-GRAPH-DRY-RUN-V1.json",
);
const ROOMS_MEETING_PATH = path.join(
  ROOT,
  "reports/hotel-explorer/phase4-quality/HE-BETH-GUEST-ROOMS-MEETING-SPACE-STAGED-V1.json",
);
const BETHESDA_ID = "recLuxvwwxID7U2B8";

function loadRoomsMeetingInventory() {
  if (!fs.existsSync(ROOMS_MEETING_PATH)) return null;
  try {
    return JSON.parse(fs.readFileSync(ROOMS_MEETING_PATH, "utf8"));
  } catch (_err) {
    return null;
  }
}

function sendError(res, status, error, details = {}) {
  return res.status(status).json({ success: false, error, ...details });
}

function loadPhase3DryRun() {
  if (!fs.existsSync(DRY_RUN_PATH)) {
    return { ok: false, error: "PHASE3_DRY_RUN_MISSING", path: DRY_RUN_PATH };
  }
  try {
    const raw = JSON.parse(fs.readFileSync(DRY_RUN_PATH, "utf8"));
    return { ok: true, dryRun: raw };
  } catch (err) {
    return { ok: false, error: "PHASE3_DRY_RUN_PARSE", message: String(err?.message || err) };
  }
}

function nameOfOrg(org) {
  return org?.trading_name || org?.legal_name || null;
}

/**
 * Build proposed ownership overlay from staged dry-run (read-only file).
 * Marks every proposed field so UI can distinguish from live Airtable.
 */
function buildProposedOverlay(dryRun, livePayload) {
  const staged = dryRun.staged || {};
  const orgs = staged.organizations || [];
  const rels = staged.relationships || [];
  const persons = staged.persons || [];
  const channels = staged.channels || [];
  const evidence = staged.evidence || [];

  const byType = (t) => rels.find((r) => r.relationship_type === t) || null;
  const legalRel = byType("Legal Owner");
  const econRel = byType("Economic Owner");
  const marbeth = orgs.find((o) => /marbeth/i.test(String(o.legal_name || "")));
  const mrec = orgs.find((o) => /mullen real estate capital/i.test(String(o.legal_name || "")));
  const mhm = orgs.find((o) => /mullen hospitality/i.test(String(o.legal_name || "")));
  const proposedPeople = persons.map((p) => ({
    display_name: p.display_name,
    name: p.display_name,
    title: p.title,
    role: p.title,
    role_group: p.role_group,
    confidence: p.confidence,
    source: "PHASE3_STAGED_DRY_RUN",
    preview_state: "PROPOSED_STAGED",
    organization: nameOfOrg(mrec),
  }));

  const sources = projectEvidenceToSourcesCitations(
    evidence.map((e) => ({
      sourceUrl: e.source_url,
      sourceName: e.source_name,
      sourceType: e.source_type,
      sourcePublishedDate: e.source_published_date,
      retrievedAt: e.retrieved_at,
      evidenceSnippet: e.evidence_snippet,
      notes: e.notes,
      fieldName: e.field_name,
      valueObserved: e.value_observed,
      current: e.current,
      confidence: e.confidence,
      evidenceId: e.evidence_id || e.dedupe_key,
    })),
  ).map((s) => ({
    ...s,
    preview_state: "PROPOSED_STAGED",
    persisted: false,
  }));

  const liveReport = livePayload?.report || {};
  const liveControl = liveReport.ownership_and_control || {};

  return {
    preview_mode: "PHASE3_PROPOSED",
    preview_banner:
      "Phase 3 Proposed Preview — staged research overlay. Not persisted to Airtable. Not customer-approved.",
    apply_authorized: false,
    persisted: false,
    write_on_get: false,
    staging_artifact: path.relative(ROOT, DRY_RUN_PATH).replace(/\\/g, "/"),
    report: {
      ...liveReport,
      executive_summary: {
        text:
          `PROPOSED: Legal PropCo ${nameOfOrg(marbeth) || "Marbeth Acquisition LLC"}; ` +
          `economic sponsor ${nameOfOrg(mrec) || "Mullen Real Estate Capital"} (MEDIUM confidence). ` +
          `UBO unresolved. Operator/brand Marriott edges remain live Airtable where present.`,
        verification_bucket: "PROPOSED",
        source: "PHASE3_STAGED_DRY_RUN",
        preview_state: "PROPOSED_STAGED",
        what_remains_uncertain: [
          "Ultimate beneficial owner (natural person / controlling shareholding)",
          "Qualified signatories",
          "Personal emails / phones for owner-side contacts",
        ],
      },
      what_remains_uncertain: [
        "Ultimate beneficial owner (natural person / controlling shareholding)",
        "Qualified signatories",
        "Personal emails / phones for owner-side contacts",
      ],
      ownership_and_control: {
        ...liveControl,
        economic_owner_or_group: {
          name: nameOfOrg(mrec),
          known: true,
          status: "PROPOSED",
          verification_bucket: econRel?.confidence || "MEDIUM",
          source: "PHASE3_STAGED_DRY_RUN",
          preview_state: "PROPOSED_STAGED",
          note: "Sponsor association from portfolio + mailing match — not UBO",
        },
        economic_owner: {
          name: nameOfOrg(mrec),
          known: true,
          verification_bucket: econRel?.confidence || "MEDIUM",
          source: "PHASE3_STAGED_DRY_RUN",
          preview_state: "PROPOSED_STAGED",
        },
        property_company: {
          name: nameOfOrg(marbeth),
          known: true,
          status: "PROPOSED",
          source: "PHASE3_STAGED_DRY_RUN",
          preview_state: "PROPOSED_STAGED",
          note: "SDAT legal titleholder (proposed persist)",
        },
        legal_property_owner_propco: {
          name: nameOfOrg(marbeth),
          known: true,
          status: "PROPOSED",
          source: "PHASE3_STAGED_DRY_RUN",
          preview_state: "PROPOSED_STAGED",
          note: "SDAT legal titleholder (proposed persist)",
        },
        ubo_group: {
          name: null,
          known: false,
          status: "UNRESOLVED",
          preview_state: "UNRESOLVED",
          note: "Explicitly unresolved — do not invent UBO",
        },
        operator: liveControl.operator || null,
        brand: liveControl.brand || null,
        control_status: "ECONOMIC_OWNER_PROPOSED_UBO_UNRESOLVED",
      },
      organizations: [
        ...(liveReport.organizations || []),
        ...orgs.map((o) => ({
          display_name: nameOfOrg(o),
          legal_name: o.legal_name,
          organization_type: o.organization_type,
          verification_state: "PROPOSED",
          source: "PHASE3_STAGED_DRY_RUN",
          preview_state: "PROPOSED_STAGED",
          note: o.note || null,
        })),
      ],
      relationships: [
        ...(liveReport.relationships || []).map((r) => ({
          ...r,
          preview_state: r.preview_state || "LIVE_AIRTABLE",
        })),
        ...rels.map((r) => ({
          relationship_type: r.relationship_type,
          confidence: r.confidence,
          verification_state: "PROPOSED",
          source: "PHASE3_STAGED_DRY_RUN",
          preview_state: "PROPOSED_STAGED",
          note: r.note || null,
          organization_name:
            r.relationship_type === "Legal Owner"
              ? nameOfOrg(marbeth)
              : r.relationship_type === "Economic Owner"
                ? nameOfOrg(mrec)
                : null,
        })),
      ],
      decision_authority: {
        people: proposedPeople,
        source: "PHASE3_STAGED_DRY_RUN",
        preview_state: "PROPOSED_STAGED",
        note: "Proposed people from Phase 3 staging — personal emails/phones unresolved",
      },
      decision_makers: proposedPeople,
      contact_channels_proposed: channels.map((c) => ({
        kind: c.kind,
        value: c.value,
        verification_status: c.verification_status,
        source: "PHASE3_STAGED_DRY_RUN",
        preview_state: "PROPOSED_STAGED",
      })),
      evidence: {
        claim: "Phase 3 staged evidence pack for Bethesda Marriott (proposed persist)",
        sources,
        what_it_proves:
          "Documents supporting proposed Legal Owner, economic sponsor association, contacts, and history.",
        what_it_does_not_prove:
          "Does not establish UBO; historical sources remain historical; not yet Airtable-persisted.",
        source: "PHASE3_STAGED_DRY_RUN",
        preview_state: "PROPOSED_STAGED",
        persisted: false,
      },
      why_this_matters: {
        text:
          "Proposed ownership intelligence distinguishes PropCo (Marbeth), economic sponsor (MREC), and operator (Marriott). Review Sources for evidence before authorizing Airtable apply.",
        source: "PHASE3_STAGED_DRY_RUN",
        preview_state: "PROPOSED_STAGED",
      },
      research_status: {
        ...(liveReport.research_status || {}),
        preview_mode: "PHASE3_PROPOSED",
        proposed_evidence_count: sources.length,
        ubo_resolved: false,
        message:
          "Proposed overlay active — staged dry-run only. Live Airtable mode shows persisted graph alone.",
      },
      field_provenance: {
        ...(liveReport.field_provenance || {}),
        economic_owner: {
          class: "PROPOSED_STAGED",
          source: "Phase 3 dry-run (not Airtable)",
        },
        property_company: {
          class: "PROPOSED_STAGED",
          source: "Phase 3 dry-run (not Airtable)",
        },
        sources: {
          class: "PROPOSED_STAGED",
          source: "Phase 3 staged HI Graph Evidence plan",
        },
      },
    },
    organization: mrec
      ? {
          display_name: nameOfOrg(mrec),
          legal_name: mrec.legal_name,
          website: mrec.official_website || "https://mullencap.com/",
          organization_type: mrec.organization_type,
          verification_state: "PROPOSED",
          source: "PHASE3_STAGED_DRY_RUN",
          preview_state: "PROPOSED_STAGED",
          key_people: persons,
          contact_channels: channels,
        }
      : livePayload?.organization || null,
    proposed_entities: {
      property_company: marbeth || null,
      economic_owner: mrec || null,
      asset_manager: mhm || null,
      people: persons,
      channels,
      evidence_documents: evidence.length,
    },
  };
}

/**
 * Capture live ownership without sending a response (reuse handler via mock req/res).
 */
async function loadLiveOwnership(recordId) {
  return new Promise((resolve) => {
    const req = { params: { recordId }, query: {}, method: "GET" };
    const res = {
      statusCode: 200,
      status(code) {
        this.statusCode = code;
        return this;
      },
      json(body) {
        resolve({ statusCode: this.statusCode || 200, body });
        return this;
      },
    };
    Promise.resolve(getGoldenDemoHotelOwnership(req, res)).catch((err) => {
      resolve({
        statusCode: 500,
        body: { success: false, error: "live_ownership_failed", message: String(err?.message || err) },
      });
    });
  });
}

function buildProposedContactsViewModel(dryRun, livePayload) {
  const staged = dryRun.staged || {};
  const orgs = staged.organizations || [];
  const persons = staged.persons || [];
  const channels = staged.channels || [];
  const marbeth = orgs.find((o) => /marbeth/i.test(String(o.legal_name || "")));
  const mrec = orgs.find((o) => /mullen real estate capital/i.test(String(o.legal_name || "")));
  const emailCh = channels.find((c) => c.kind === "email");
  const addressCh = channels.find((c) => c.kind === "address");
  const hotelName =
    (livePayload && livePayload.hotel && livePayload.hotel.name) || "Bethesda Marriott";

  return {
    version: "contact-intelligence-viewmodel-v1",
    staging: true,
    production_writes: false,
    surfe_live_calls: 0,
    newly_researched: false,
    preview_mode: "PHASE3_PROPOSED",
    preview_banner:
      "Phase 3 Proposed contacts — staged research only. Not persisted. Not customer-approved.",
    write_on_get: false,
    apply_authorized: false,
    hotelId: BETHESDA_ID,
    hotelName,
    lastReviewed: "2026-11-19",
    ownershipSummary: {
      hotel: { name: hotelName, role: "HOTEL" },
      propco: {
        name: nameOfOrg(marbeth),
        role: "PROPERTY_OWNER_LEGAL_ENTITY",
        label: "Property Owner / PropCo (PROPOSED)",
        shortName: "Marbeth",
      },
      sponsor: {
        name: nameOfOrg(mrec),
        role: "OWNER_SPONSOR_GROUP",
        label: "Economic Owner / Contactable Owner Org (PROPOSED · MEDIUM)",
      },
      operator: {
        name:
          (livePayload?.report?.ownership_and_control?.operator &&
            livePayload.report.ownership_and_control.operator.name) ||
          "Marriott International",
        role: "OPERATOR",
        label: "Current Operator",
        sameAsSponsor: false,
      },
      brand: {
        name: (livePayload?.hotel && livePayload.hotel.brand_display) || "Marriott",
        role: "CURRENT_BRAND",
        label: "Current Brand",
      },
      brandHistory: [],
    },
    qualitySummary: {
      ownerOrgStatus: "PROPOSED",
      primaryDecisionMakers: persons.length,
      verifiedEmails: emailCh ? 1 : 0,
      verifiedOrPublicPhones: 0,
      fallbackContactPaths: channels.length,
      lastReviewed: "2026-11-19",
      ubo_resolved: false,
    },
    contactableOwnerOrg: {
      organizationId: "proposed:mullen_real_estate_capital",
      name: nameOfOrg(mrec),
      entityType: "Private sponsor (proposed)",
      relationship: "Proposed economic sponsor — UBO unresolved",
      relationshipBadges: ["PROPOSED ECONOMIC OWNER", "NOT UBO"],
      hq: (addressCh && addressCh.value) || null,
      website: (mrec && mrec.official_website) || "https://mullencap.com/",
      phones: [],
      emails: emailCh
        ? [
            {
              type: "ORG_GENERAL_EMAIL",
              value: emailCh.value,
              source: "PHASE3_STAGED_DRY_RUN",
              source_type: "company_website",
              source_url: "https://mullencap.com/contact/",
              verification_status: emailCh.verification_status || "VERIFIED_PUBLIC",
              verification_method: "public_company_contact_page",
              last_verified: "2026-11-19",
              confidence: "HIGH",
              provider: null,
              client_display_allowed: true,
              discovery_method: "phase3_free_first",
              note: "Organization channel — not a personal decision-maker inbox",
              preview_state: "PROPOSED_STAGED",
            },
          ]
        : [],
      preview_state: "PROPOSED_STAGED",
    },
    people: persons.map((p, idx) => {
      const group =
        /ownership|decision|ceo|president/i.test(String(p.role_group || "") + " " + String(p.title || ""))
          ? "PRIMARY_OWNER_SIDE"
          : "SECONDARY_OWNER_CORPORATE";
      return {
        personId: "proposed:" + String(p.display_name || "").toLowerCase().replace(/\s+/g, "_"),
        name: p.display_name,
        title: p.title,
        role: p.title,
        group,
        role_group: p.role_group,
        organization: nameOfOrg(mrec),
        organizationName: nameOfOrg(mrec),
        confidence: p.confidence,
        preview_state: "PROPOSED_STAGED",
        decisionAuthority: idx === 0 ? "Likely owner-side decision contact (proposed)" : "Authority Not Verified",
        signingAuthority: "Authority Not Verified",
        verificationNotes:
          "PROPOSED — named on MREC About/Team. Personal email/phone unresolved. Not UBO.",
        whyRelevant:
          "Named on Mullen Real Estate Capital leadership materials; proposed owner-side entry point for this asset.",
        contactability: "PROFILE_OR_ORG_FALLBACK",
        stakeholderClass: p.role_group || "Owner / Principal",
        lastVerified: "2026-11-19",
        emailContacts: [],
        phoneContacts: [],
        emails: [],
        phones: [],
        channels: [],
        enrichAction: {
          label: "Find Contact Details",
          enabled: true,
          reason: null,
        },
      };
    }),
    organizations: [
      {
        name: nameOfOrg(marbeth),
        legal_name: marbeth && marbeth.legal_name,
        role: "PROPERTY_OWNER_LEGAL_ENTITY",
        preview_state: "PROPOSED_STAGED",
      },
      {
        name: nameOfOrg(mrec),
        legal_name: mrec && mrec.legal_name,
        role: "OWNER_SPONSOR_GROUP",
        preview_state: "PROPOSED_STAGED",
      },
    ],
    unresolvedItems: [
      "Ultimate beneficial owner (natural person) unresolved",
      "Qualified signatories unresolved",
      "Personal emails / phones for owner-side contacts unresolved",
    ],
    bestContactPaths: emailCh
      ? [
          {
            label: "MREC organization email (proposed)",
            value: emailCh.value,
            verification_status: "VERIFIED_PUBLIC",
          },
          {
            label: "MREC website contact page",
            value: "https://mullencap.com/contact/",
            verification_status: "PUBLICLY_PUBLISHED",
          },
        ]
      : [],
  };
}

export async function getHotelExplorerFounderPreviewContacts(req, res) {
  try {
    const recordId = String(req.params.recordId || "").trim();
    if (!recordId.startsWith("rec")) {
      return sendError(res, 400, "invalid_airtable_record_id");
    }
    const modeRaw = String(req.query.mode || "live").trim().toLowerCase();
    const mode = modeRaw === "proposed" || modeRaw === "phase3" ? "proposed" : "live";
    res.setHeader("X-Dealality-Write", "forbidden");
    res.setHeader("X-Dealality-Preview-Mode", mode === "proposed" ? "PHASE3_PROPOSED" : "LIVE_AIRTABLE");
    res.setHeader("Cache-Control", "no-store");

    if (mode !== "proposed" || recordId !== BETHESDA_ID) {
      // Live / non-Bethesda: do not invent contacts — tell client to use canonical CI path.
      return res.status(204).end();
    }

    const staged = loadPhase3DryRun();
    if (!staged.ok) {
      return sendError(res, 404, staged.error, { path: staged.path, message: staged.message });
    }
    const live = await loadLiveOwnership(recordId);
    const vm = buildProposedContactsViewModel(staged.dryRun, live.body || {});
    return res.json(vm);
  } catch (err) {
    console.error("[hotel-explorer-founder-preview-contacts]", err);
    return sendError(res, 500, "founder_preview_contacts_failed", {
      message: String(err?.message || err),
    });
  }
}

export async function getHotelExplorerFounderPreview(req, res) {
  try {
    const recordId = String(req.params.recordId || "").trim();
    if (!recordId.startsWith("rec")) {
      return sendError(res, 400, "invalid_airtable_record_id");
    }
    const modeRaw = String(req.query.mode || "live").trim().toLowerCase();
    const mode = modeRaw === "proposed" || modeRaw === "phase3" ? "proposed" : "live";

    // Hard guarantee: this route never writes.
    res.setHeader("X-Dealality-Write", "forbidden");
    res.setHeader("X-Dealality-Preview-Mode", mode === "proposed" ? "PHASE3_PROPOSED" : "LIVE_AIRTABLE");
    res.setHeader("Cache-Control", "no-store");

    const live = await loadLiveOwnership(recordId);
    if (!live.body?.success && live.statusCode >= 400) {
      return res.status(live.statusCode).json(live.body);
    }
    const livePayload = live.body || {};
    const roomsInv = recordId === BETHESDA_ID ? loadRoomsMeetingInventory() : null;

    function withRooms(payload) {
      if (!roomsInv) return payload;
      const hotel = {
        ...(payload.hotel || {}),
        guestRoomsMeetingSpace: roomsInv,
      };
      const report = { ...(payload.report || {}) };
      const evidence = { ...(report.evidence || {}) };
      const existingSources = Array.isArray(evidence.sources) ? evidence.sources : [];
      const roomSources = (roomsInv.sources || []).map((s) => ({
        title: s.title,
        url: s.url,
        provider: s.verificationStatus || "rooms_meeting_space",
        observed_date: null,
        evidence_excerpt: Array.isArray(s.supports)
          ? `Supports: ${s.supports.join("; ")}`
          : "",
        claim: s.title,
        proves: Array.isArray(s.supports) ? s.supports.join("; ") : s.title,
        does_not: s.does_not || null,
        preview_state:
          s.verificationStatus === "VERIFIED" ? "BRAND_VERIFIED" : "STAGED_REFERENCE",
        persisted: false,
      }));
      const seen = new Set(
        existingSources.map((s) => String(s.url || s.title || "").toLowerCase()),
      );
      const mergedSources = existingSources.slice();
      for (const rs of roomSources) {
        const key = String(rs.url || rs.title || "").toLowerCase();
        if (key && seen.has(key)) continue;
        if (key) seen.add(key);
        mergedSources.push(rs);
      }
      report.evidence = {
        ...evidence,
        sources: mergedSources,
        claim:
          evidence.claim ||
          "Ownership, guestroom, and meeting-space evidence for Bethesda Marriott founder preview",
      };
      report.guestRoomsMeetingSpace = roomsInv;
      return {
        ...payload,
        hotel,
        report,
        guestRoomsMeetingSpace: roomsInv,
      };
    }

    if (mode === "live") {
      return res.json(
        withRooms({
          ...livePayload,
          success: true,
          preview_mode: "LIVE_AIRTABLE",
          preview_banner:
            "Live Airtable — only information already persisted and retrievable from canonical sources. Guest Rooms & Meeting Space brand materials are read-only presentation overlays (not Airtable writes).",
          apply_authorized: false,
          write_on_get: false,
          founder_preview: true,
        }),
      );
    }

    if (recordId !== BETHESDA_ID) {
      return res.json({
        ...livePayload,
        success: true,
        preview_mode: "PHASE3_PROPOSED",
        preview_banner:
          "Phase 3 Proposed Preview is currently staged for Bethesda Marriott only. Showing live Airtable for this hotel.",
        proposed_available: false,
        write_on_get: false,
        founder_preview: true,
      });
    }

    const staged = loadPhase3DryRun();
    if (!staged.ok) {
      return sendError(res, 404, staged.error, { path: staged.path, message: staged.message });
    }

    const overlay = buildProposedOverlay(staged.dryRun, livePayload);
    return res.json(
      withRooms({
        ...livePayload,
        ...overlay,
        success: true,
        hotel: livePayload.hotel || {
          id: recordId,
          name: "Bethesda Marriott",
          airtable_record_id: recordId,
        },
        airtable_graph: livePayload.airtable_graph || null,
        data_status: "FOUNDER_PREVIEW_PROPOSED_OVERLAY",
        founder_preview: true,
        write_on_get: false,
      }),
    );
  } catch (err) {
    console.error("[hotel-explorer-founder-preview]", err);
    return sendError(res, 500, "founder_preview_failed", {
      message: String(err?.message || err),
    });
  }
}

export default {
  getHotelExplorerFounderPreview,
  getHotelExplorerFounderPreviewContacts,
};
