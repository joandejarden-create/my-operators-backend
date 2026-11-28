/**
 * Golden Demo Ownership Intelligence API (controlled Mexico / GSF cohort).
 * Read-only. No CoStar. No Census writes.
 */

import {
  buildHotelOwnershipReport,
  buildOwnershipGroupProfile,
  buildNeighborsPayload,
  listGoldenDemoCohortIndex,
  GSF_SLUG,
} from "../lib/hotel-intelligence/ownership/golden-demo/gsf-cohort.js";
import { buildExecutiveOwnershipNarrative } from "../lib/hotel-intelligence/ownership/he-executive-ownership-narrative-v1.js";

function sendError(res, status, error, details = {}) {
  return res.status(status).json({ success: false, error, ...details });
}

export async function getGoldenDemoOwnershipIndex(req, res) {
  try {
    return res.json({ success: true, ...listGoldenDemoCohortIndex() });
  } catch (err) {
    console.error("[golden-demo-ownership] index", err);
    return sendError(res, 500, "golden_demo_index_failed");
  }
}

export async function getGoldenDemoHotelOwnership(req, res) {
  try {
    const recordId = String(req.params.recordId || "").trim();
    if (!recordId.startsWith("rec")) {
      return sendError(res, 400, "invalid_airtable_record_id");
    }
    let payload = buildHotelOwnershipReport(recordId);

    // Airtable-first overlay from HI Organizations + HI Hotel–Org Relationships.
    // For out-of-cohort hotels (e.g. Bethesda): graph is the canonical HE source.
    // For golden-demo cohort (KGPV): fixtures remain UX reference; graph attached when present
    // but fixtures are not treated as verified Airtable (Gate 1).
    let airtableGraph = null;
    try {
      const { readHiGraphForHotel } = await import(
        "../lib/hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js"
      );
      airtableGraph = await readHiGraphForHotel(recordId, { includePortfolio: true });
    } catch (graphErr) {
      console.warn("[golden-demo-ownership] hi-graph overlay skipped", graphErr?.message || graphErr);
    }

    if (
      airtableGraph?.ok &&
      (airtableGraph.relationshipCount > 0 || (airtableGraph.evidenceCount || 0) > 0)
    ) {
      const proj = airtableGraph.projection || {};
      const report = payload.report || {};
      const control = report.ownership_and_control || {};
      const nameOf = (org) => org?.tradingName || org?.legalName || null;
      const primaryOrg = airtableGraph.primaryOrganization || proj.operator || proj.brand || null;
      const portfolioByHotel = new Map();
      for (const row of airtableGraph.portfolio || []) {
        const hid = row.airtable_record_id;
        if (!hid) continue;
        const existing = portfolioByHotel.get(hid);
        const rel = row.relationship_type;
        if (!existing) {
          portfolioByHotel.set(hid, {
            hotel: hid === recordId ? payload.hotel?.name || hid : hid,
            name: hid === recordId ? payload.hotel?.name || hid : hid,
            airtable_record_id: hid,
            relationship: rel,
            relationship_type: rel,
            confidence: row.confidence || row.verification_state || "MEDIUM",
            economic_owner_verified: row.economic_owner_verified === true,
            focus_hotel: hid === recordId || row.focus_hotel === true,
            source: "HI_AIRTABLE_GRAPH",
            relationships: [rel],
          });
        } else {
          existing.relationships.push(rel);
          if (row.economic_owner_verified) existing.economic_owner_verified = true;
          // Prefer Operator label when both Brand+Operator exist for same hotel.
          if (/Operator|Management/i.test(String(rel))) {
            existing.relationship = rel;
            existing.relationship_type = rel;
          }
        }
      }
      const portfolioHotels = [...portfolioByHotel.values()];

      const graphSources = airtableGraph.sourcesForUi || [];
      payload.airtable_graph = {
        source: "HI_AIRTABLE_GRAPH",
        baseId: airtableGraph.baseId,
        relationshipCount: airtableGraph.relationshipCount,
        evidenceCount: airtableGraph.evidenceCount || 0,
        control_status: proj.control_status,
        relationships: airtableGraph.relationships,
        organizations: airtableGraph.organizations,
        evidence: airtableGraph.evidence || [],
        primaryOrganization: primaryOrg,
        portfolioCount: portfolioHotels.length,
        provenance: {
          class: "LIVE_HI_GRAPH_AIRTABLE",
          verified: true,
          note:
            "HI Organizations + HI Hotel–Org Relationships + HI Graph Evidence on appCCUsuGsE1ifoLk",
        },
      };

      if (!payload.in_cohort) {
        payload.data_status = "AIRTABLE_GRAPH_PARTIAL";
        const portfolioKpis = {
          total_relationships: portfolioHotels.length,
          operated_or_managed: portfolioHotels.filter((h) =>
            /Operator|Management|OPERATED/i.test(
              String(h.relationship_type || h.relationship || ""),
            ),
          ).length,
          owned_or_controlled_verified: portfolioHotels.filter(
            (h) => h.economic_owner_verified === true,
          ).length,
        };
        const ownedHotels = portfolioHotels.filter(
          (h) => h.economic_owner_verified === true,
        );
        const operatedHotels = portfolioHotels.filter((h) =>
          /Operator|Management|OPERATED/i.test(
            String(h.relationship_type || h.relationship || ""),
          ),
        );
        const economicOrg = proj.economic_owner || null;
        const economicName = nameOf(economicOrg);
        const economicRel =
          (airtableGraph.relationships || []).find(
            (r) => String(r.relationshipType) === "Economic Owner" && r.current !== false,
          ) ||
          (airtableGraph.relationships || []).find(
            (r) => String(r.relationshipType) === "Economic Owner",
          ) ||
          null;
        // Sponsor-attribution edges (MEDIUM confidence / explicit sponsor notes / non-HIGH)
        // must not render as certified Validated economic ownership headlines.
        const economicNotes = String(economicRel?.notes || "");
        const economicConfidence = String(
          economicRel?.confidence || economicOrg?.confidence || "MEDIUM",
        ).toUpperCase();
        const economicIsSponsorAttribution =
          /sponsor attribution|ubo unresolved|not (a )?certified|proposed/i.test(
            economicNotes,
          ) || economicConfidence === "MEDIUM" || economicConfidence === "LOW";
        const economicResolved = Boolean(economicName);
        const economicUiStatus = !economicResolved
          ? "NOT_YET_VERIFIED"
          : economicIsSponsorAttribution
            ? "PROPOSED"
            : "VALIDATED";
        const economicVerificationBucket = !economicResolved
          ? "PARTIAL"
          : economicIsSponsorAttribution
            ? "PROPOSED"
            : economicConfidence || "HIGH";
        const propcoName = nameOf(proj.property_company);
        const propcoParty = propcoName
          ? {
              name: propcoName,
              known: true,
              status: "VALIDATED",
              verification_bucket:
                proj.property_company?.confidence || "HIGH",
              source: "HI_AIRTABLE_GRAPH",
              note: "Legal Owner / PropCo from HI Hotel–Org Relationships",
            }
          : {
              name: null,
              known: false,
              status: "UNKNOWN",
              note: "PropCo / legal title unknown.",
            };
        if (primaryOrg) {
          payload.organization = {
            id: primaryOrg.orgId || primaryOrg.airtableId,
            airtable_id: primaryOrg.airtableId,
            display_name: nameOf(primaryOrg),
            legal_name: primaryOrg.legalName || nameOf(primaryOrg),
            website: primaryOrg.officialWebsite || null,
            jurisdiction: primaryOrg.jurisdiction || null,
            verification_state: primaryOrg.verificationState || null,
            organization_type: primaryOrg.organizationType || null,
            ticker: primaryOrg.orgId || null,
            registry_id: primaryOrg.orgId || null,
            portfolio_kpis: portfolioKpis,
            owned_controlled_hotels: ownedHotels,
            operated_managed_hotels: operatedHotels,
            source: "HI_AIRTABLE_GRAPH",
          };
          payload.ownership_group = payload.organization;
        }
        const operatorName = nameOf(proj.operator);
        // Prefer customer brand label over brand-parent organization trading name.
        const brandName =
          payload.hotel?.brand_display ||
          payload.hotel?.brand ||
          payload.hotel?.affiliation ||
          nameOf(proj.brand) ||
          null;
        const economicPartyForNarrative = economicResolved
          ? {
              name: economicName,
              known: true,
              status: economicUiStatus,
              verification_bucket: economicVerificationBucket,
              note: economicIsSponsorAttribution
                ? "Sponsor attribution — not certified verified economic ownership"
                : economicNotes || undefined,
            }
          : { name: null, known: false };
        const execNarrative = buildExecutiveOwnershipNarrative({
          hotelName: payload.hotel?.name || null,
          legalOwner: propcoParty,
          economicOwner: economicPartyForNarrative,
          brand: brandName,
          operator: operatorName,
        });
        const execText = execNarrative.text;
        // Stash parties so a later census name hydrate can rebuild the customer narrative.
        payload._execNarrativeParties = {
          legalOwner: propcoParty,
          economicOwner: economicPartyForNarrative,
          brand: brandName,
          operator: operatorName,
        };
        payload.report = {
          ...report,
          executive_summary: {
            text: execText,
            verification_bucket: economicVerificationBucket,
            source: "CUSTOMER_OWNERSHIP_NARRATIVE_V1",
            what_remains_uncertain: economicResolved && !economicIsSponsorAttribution
              ? []
              : [
                  economicIsSponsorAttribution
                    ? "Ultimate ownership interest for the proposed economic sponsor"
                    : "Economic owner / controlling sponsor",
                  propcoName ? null : "Legal property company",
                  "Named decision-makers and outreach channels",
                ].filter(Boolean),
          },
          what_remains_uncertain: economicResolved && !economicIsSponsorAttribution
            ? []
            : [
                economicIsSponsorAttribution
                  ? "Ultimate ownership interest for the proposed economic sponsor"
                  : "Economic owner / controlling sponsor",
                propcoName ? null : "Legal property company",
                "Named decision-makers and outreach channels",
              ].filter(Boolean),
          why_this_matters: {
            text: operatorName
              ? `${operatorName} is shown as the operating/management relationship for this hotel. Brand and management presence do not certify economic ownership.`
              : "Ownership relationships for this hotel are still being assembled. Brand or operator presence alone does not certify ownership.",
            source: "CUSTOMER_OWNERSHIP_NARRATIVE_V1",
          },
          portfolio_preview: {
            hotels: portfolioHotels,
            kpis: portfolioKpis,
            source: "HI_AIRTABLE_GRAPH",
            coverage_note:
              "Portfolio preview shows hotels currently linked to this ownership organization — not a complete brand-system portfolio.",
          },
          organizations: (airtableGraph.organizations || []).map((org) => ({
            airtable_id: org.airtableId,
            org_id: org.orgId,
            display_name: nameOf(org),
            legal_name: org.legalName || nameOf(org),
            organization_type: org.organizationType || null,
            verification_state: org.verificationState || null,
            source: "HI_AIRTABLE_GRAPH",
          })),
          ownership_and_control: {
            ...control,
            economic_owner_or_group: economicResolved
              ? {
                  name: economicName,
                  known: true,
                  status: economicUiStatus,
                  verification_bucket: economicVerificationBucket,
                  confidence: economicConfidence,
                  source: "HI_AIRTABLE_GRAPH",
                  note: economicIsSponsorAttribution
                    ? "Sponsor attribution — not certified verified economic ownership / UBO"
                    : economicNotes || undefined,
                }
              : {
                  name: null,
                  known: false,
                  status: "NOT_YET_VERIFIED",
                  note: "Owner: Not yet verified",
                },
            economic_owner: economicResolved
              ? {
                  name: economicName,
                  known: true,
                  status: economicUiStatus,
                  verification_bucket: economicVerificationBucket,
                  confidence: economicConfidence,
                  source: "HI_AIRTABLE_GRAPH",
                  note: economicIsSponsorAttribution
                    ? "Sponsor attribution — not certified verified economic ownership / UBO"
                    : economicNotes || undefined,
                }
              : {
                  name: null,
                  known: false,
                  note: "ECONOMIC_OWNER_UNRESOLVED",
                },
            // Keep both keys in sync — UI historically reads legal_property_owner_propco;
            // an empty stub object must not block property_company.
            property_company: propcoParty,
            legal_property_owner_propco: propcoParty,
            operator: operatorName
              ? {
                  name: operatorName,
                  known: true,
                  verification_bucket: "HIGH",
                  relationship_type: "OPERATED_BY",
                  source: "HI_AIRTABLE_GRAPH",
                }
              : control.operator || null,
            brand: nameOf(proj.brand)
              ? {
                  name: nameOf(proj.brand),
                  known: true,
                  source: "HI_AIRTABLE_GRAPH",
                  note: "Brand relationship organization (brand parent/operator entity) — not an economic-owner claim",
                }
              : control.brand || null,
            control_status: proj.control_status,
          },
          relationships: airtableGraph.relationships.map((rel) => ({
            relationship_type: rel.relationshipType,
            organization_id: rel.organizationId,
            airtable_id: rel.airtableId,
            verification_state: rel.verificationState,
            confidence: rel.confidence,
            source: "HI_AIRTABLE_GRAPH",
          })),
          organization_and_portfolio: {
            hotels: portfolioHotels,
            source: "HI_AIRTABLE_GRAPH",
            kpis: portfolioKpis,
          },
          research_status: {
            ...(report.research_status || {}),
            airtable_graph: true,
            economic_owner_resolved: economicResolved && !economicIsSponsorAttribution,
            economic_sponsor_attribution: economicIsSponsorAttribution,
            evidence_count: airtableGraph.evidenceCount || 0,
            message: economicResolved
              ? economicIsSponsorAttribution
                ? "A potential economic sponsor is identified; ultimate ownership interest is not independently confirmed."
                : "Economic ownership is supported by verified ownership relationships."
              : "Operator and brand may be present; economic ownership remains unresolved.",
          },
          // Sources tab SoT: HI Graph Evidence projected into existing citation shape.
          evidence: {
            ...(report.evidence || {}),
            claim:
              (report.evidence && report.evidence.claim) ||
              (economicResolved
                ? `Ownership relationships and supporting documents for ${payload.hotel?.name || recordId}`
                : "Property Research Evidence"),
            sources: graphSources.length
              ? graphSources
              : (report.evidence && report.evidence.sources) || [],
            what_it_proves:
              "Source documents that support ownership, management, brand, or related property claims for this hotel.",
            what_it_does_not_prove:
              "Does not invent ultimate beneficial ownership; historical sources remain historical.",
            source: "HI_GRAPH_EVIDENCE",
          },
          field_provenance: {
            operator: { class: operatorName ? "VERIFIED_HI_REL" : "EMPTY", source: "HI Hotel–Org Relationships" },
            brand: {
              class: nameOf(proj.brand) ? "VERIFIED_HI_REL" : "EMPTY",
              source: "HI Hotel–Org Relationships",
            },
            economic_owner: {
              class: economicResolved
                ? economicIsSponsorAttribution
                  ? "PROPOSED_SPONSOR_ATTRIBUTION"
                  : "VERIFIED_HI_REL"
                : "UNVERIFIED",
              source: "HI Hotel–Org Relationships",
            },
            property_company: {
              class: propcoName ? "VERIFIED_HI_REL" : "UNVERIFIED",
              source: "HI Hotel–Org Relationships (Legal Owner)",
            },
            portfolio_preview: {
              class: "VERIFIED_DERIVED",
              source: "HI Hotel–Org Relationships portfolio query",
            },
            sources: {
              class: graphSources.length ? "VERIFIED_HI_EVIDENCE" : "EMPTY",
              source: "HI Graph Evidence",
            },
          },
        };
        payload.org_network = payload.report.organizations;
      }
    }

    // HPC property contact fallback for Contacts chapter (read-only; no CI write).
    // Also resolve canonical hotel display name for portfolio rows (out-of-cohort).
    try {
      const { resolvePropertyCensusOwnership } = await import(
        "../lib/hotel-intelligence/property-census-identity-v1.js"
      );
      // Reuse brand-presence base via env — ownership route already has Airtable creds in process.
      const Airtable = (await import("airtable")).default;
      const key = process.env.AIRTABLE_API_KEY;
      const baseId = process.env.AIRTABLE_BASE_ID_ALT;
      if (key && baseId && !payload.in_cohort) {
        const atBase = new Airtable({ apiKey: key }).base(baseId);
        const identity = await resolvePropertyCensusOwnership(atBase, recordId);
        const fields = identity.record?.fields || {};
        const hotelName =
          fields.Name ||
          fields["Hotel Name"] ||
          fields["Property Name"] ||
          null;
        if (hotelName) {
          payload.hotel = {
            ...(payload.hotel || {}),
            id: recordId,
            airtable_record_id: recordId,
            name: hotelName,
            source_table: identity.owningTable || null,
            owning_table_kind: identity.owningTableKind || null,
          };
          const portfolioHotels =
            payload.report?.organization_and_portfolio?.hotels || [];
          for (const row of portfolioHotels) {
            if (
              row &&
              (row.airtable_record_id === recordId || row.focus_hotel) &&
              (!row.name || row.name === recordId || row.hotel === recordId)
            ) {
              row.name = hotelName;
              row.hotel = hotelName;
            }
          }
          // Rebuild customer executive narrative once the canonical hotel name is known.
          if (
            payload.report?.executive_summary?.source === "CUSTOMER_OWNERSHIP_NARRATIVE_V1" &&
            payload._execNarrativeParties
          ) {
            const brandForNarrative =
              payload.hotel?.brand_display ||
              payload.hotel?.brand ||
              payload.hotel?.affiliation ||
              payload._execNarrativeParties.brand ||
              null;
            const rebuilt = buildExecutiveOwnershipNarrative({
              hotelName,
              ...payload._execNarrativeParties,
              brand: brandForNarrative,
            });
            payload.report.executive_summary = {
              ...payload.report.executive_summary,
              text: rebuilt.text,
            };
          }
        }
        const phone =
          fields.Phone || fields.Telephone || fields.phone || null;
        const website =
          fields["Official Property URL"] || fields.Website || fields.website || null;
        if (phone || website) {
          const report = payload.report || {};
          payload.report = {
            ...report,
            corporate_contacts: {
              ...(report.corporate_contacts || {}),
              property_phone: phone || report.corporate_contacts?.property_phone || null,
              website: website || report.corporate_contacts?.website || null,
              property_fallback_source: identity.owningTable || "Hotel Property Census",
              source: "HPC_PROPERTY_FALLBACK",
            },
          };
        }

        // Durable rooms / meeting-space summary from HPC (not founder-preview staged JSON).
        try {
          const { buildGuestRoomsMeetingSpaceFromHpc } = await import(
            "../lib/hotel-intelligence/he-hpc-guest-rooms-meeting-space-v1.js"
          );
          const roomsInv = buildGuestRoomsMeetingSpaceFromHpc(fields, {
            hpcId: recordId,
            hotelName: hotelName || payload.hotel?.name || null,
          });
          if (roomsInv) {
            payload.guestRoomsMeetingSpace = roomsInv;
            payload.hotel = {
              ...(payload.hotel || {}),
              guestRoomsMeetingSpace: roomsInv,
              rooms:
                roomsInv.guestRooms?.totalGuestrooms?.value ??
                payload.hotel?.rooms ??
                null,
              totalMeetingSpace:
                roomsInv.meetingSpaceSummary?.totalMeetingSpaceSqFt?.value ??
                payload.hotel?.totalMeetingSpace ??
                null,
              yearBuilt: (() => {
                const od = fields["Opening Date"];
                if (!od) return payload.hotel?.yearBuilt ?? null;
                const y = Number(String(od).slice(0, 4));
                return Number.isFinite(y) ? y : payload.hotel?.yearBuilt ?? null;
              })(),
              lastRenovated: (() => {
                const rd = fields["Renovation / Conversion Date"];
                if (!rd) return payload.hotel?.lastRenovated ?? null;
                const y = Number(String(rd).slice(0, 4));
                return Number.isFinite(y) ? y : payload.hotel?.lastRenovated ?? null;
              })(),
              yearRenovated: (() => {
                const rd = fields["Renovation / Conversion Date"];
                if (!rd) return payload.hotel?.yearRenovated ?? null;
                const y = Number(String(rd).slice(0, 4));
                return Number.isFinite(y) ? y : payload.hotel?.yearRenovated ?? null;
              })(),
            };
            const report = payload.report || {};
            payload.report = {
              ...report,
              guestRoomsMeetingSpace: roomsInv,
            };
          }
        } catch (roomsErr) {
          console.warn(
            "[golden-demo-ownership] HPC rooms/meeting hydrate skipped",
            roomsErr?.message || roomsErr,
          );
        }
      }
    } catch (contactErr) {
      console.warn(
        "[golden-demo-ownership] HPC contact fallback skipped",
        contactErr?.message || contactErr,
      );
    }

    // Guest Rooms & Meeting Space: canonical HPC physical-product adapter (SoT).
    // Staged packets are secondary detail / proposed overlay only — not Live primary.
    try {
      const { loadCanonicalPhysicalProductInventory } = await import(
        "../lib/hotel-intelligence/he-physical-product-adapter-v1.js"
      );
      const inv = await loadCanonicalPhysicalProductInventory(recordId, {
        hotelName: payload?.hotel?.name || payload?.hotel?.propertyName || null,
        preferProposedOverlay: false,
      });
      if (inv) {
        const hotel = { ...(payload.hotel || {}), guestRoomsMeetingSpace: inv };
        const report = { ...(payload.report || {}), guestRoomsMeetingSpace: inv };
        payload = {
          ...payload,
          hotel,
          report,
          guestRoomsMeetingSpace: inv,
          physicalProductSourceOfTruth: inv.sourceOfTruth || "HOTEL_PROPERTY_CENSUS",
        };
      }
    } catch (roomsErr) {
      console.warn(
        "[golden-demo-ownership] physical-product adapter skipped",
        roomsErr?.message || roomsErr,
      );
    }

    // Shared hotel view model — hydrate Org Portfolio focus row from HPC/HI.
    // Missing rooms/brand/market on a focus row with HPC values = mapping defect, not research gap.
    try {
      const { formatHotelPropertyCensusAsBrandPresenceHotel, HOTEL_PROPERTY_CENSUS_TABLE } =
        await import("../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js");
      const {
        buildSharedHotelViewModel,
        hydratePortfolioHotelsFromSharedView,
      } = await import("../lib/hotel-intelligence/he-shared-hotel-view-model-v1.js");

      let hpcHotel = null;
      const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
      const baseId = process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID;
      if (key && baseId) {
        const Airtable = (await import("airtable")).default;
        const atBase = new Airtable({ apiKey: key }).base(baseId);
        try {
          const rec = await atBase(HOTEL_PROPERTY_CENSUS_TABLE).find(recordId);
          hpcHotel = formatHotelPropertyCensusAsBrandPresenceHotel(rec);
        } catch {
          hpcHotel = null;
        }
      }

      const control = payload?.report?.ownership_and_control || {};
      if (control.economic_owner && typeof control.economic_owner === "object") {
        const nm = String(
          control.economic_owner.name || control.economic_owner.tradingName || ""
        ).toLowerCase();
        // Founder directive: Mullen economic sponsorship stays Proposed until certified.
        if (nm.includes("mullen") && !control.economic_owner.founder_classification) {
          control.economic_owner.founder_classification = "Proposed";
          control.economic_owner.classification = "Proposed";
        }
      }
      if (control.economic_owner_or_group && typeof control.economic_owner_or_group === "object") {
        const nm = String(control.economic_owner_or_group.name || "").toLowerCase();
        if (nm.includes("mullen")) {
          control.economic_owner_or_group.founder_classification = "Proposed";
          control.economic_owner_or_group.classification = "Proposed";
          // Do not silently present graph VALIDATED as product Validated.
          if (/^validat/i.test(String(control.economic_owner_or_group.status || ""))) {
            control.economic_owner_or_group.status = "PROPOSED";
            control.economic_owner_or_group.status_note =
              "Founder classification Proposed — graph VALIDATED edge is not product Validated";
          }
        }
      }

      const shared = buildSharedHotelViewModel({
        hpcHotel,
        ownership: payload,
        physicalProduct: payload.guestRoomsMeetingSpace || null,
        hotelId: recordId,
      });
      payload.sharedHotelView = shared;
      if (
        shared.rooms != null ||
        shared.brand ||
        shared.market ||
        shared.operator ||
        shared.website ||
        shared.telephone
      ) {
        payload.hotel = {
          ...(payload.hotel || {}),
          id: recordId,
          airtable_record_id: recordId,
          name: shared.name || payload.hotel?.name,
          rooms: shared.rooms ?? payload.hotel?.rooms,
          brand: shared.brand || payload.hotel?.brand,
          market: shared.market || payload.hotel?.market,
          submarket: shared.submarket || payload.hotel?.submarket,
          managementCompany: shared.operator || payload.hotel?.managementCompany,
          chainScale: shared.chainScale || payload.hotel?.chainScale,
          status: shared.status || payload.hotel?.status,
          propertyCompany: shared.propertyCompany || payload.hotel?.propertyCompany,
          economicOwner: shared.economicOwner || payload.hotel?.economicOwner,
          economic_owner_classification: shared.economicOwnerClassification || "Proposed",
          // Header actions — property Official Property URL / Phone from HPC shared VM.
          website: shared.website || payload.hotel?.website || null,
          hotelWebsite: shared.website || payload.hotel?.hotelWebsite || null,
          telephone: shared.telephone || payload.hotel?.telephone || null,
          phone: shared.telephone || payload.hotel?.phone || null,
        };
      }
      if (payload.organization?.owned_controlled_hotels) {
        payload.organization.owned_controlled_hotels = hydratePortfolioHotelsFromSharedView(
          payload.organization.owned_controlled_hotels,
          shared,
          { focusHotelId: recordId }
        );
      }
      if (payload.report?.organization_and_portfolio?.hotels) {
        payload.report.organization_and_portfolio.hotels = hydratePortfolioHotelsFromSharedView(
          payload.report.organization_and_portfolio.hotels,
          shared,
          { focusHotelId: recordId }
        );
      }
    } catch (sharedErr) {
      console.warn(
        "[golden-demo-ownership] shared hotel view hydrate skipped",
        sharedErr?.message || sharedErr,
      );
    }

    // Final customer narrative rebuild after shared hotel view hydrates brand/name.
    if (
      payload._execNarrativeParties &&
      payload.report?.executive_summary?.source === "CUSTOMER_OWNERSHIP_NARRATIVE_V1"
    ) {
      const finalBrand =
        payload.hotel?.brand_display ||
        payload.hotel?.brand ||
        payload.hotel?.affiliation ||
        payload._execNarrativeParties.brand ||
        null;
      const finalOperator =
        payload.hotel?.managementCompany ||
        payload._execNarrativeParties.operator ||
        null;
      const rebuilt = buildExecutiveOwnershipNarrative({
        hotelName: payload.hotel?.name || null,
        ...payload._execNarrativeParties,
        brand: finalBrand,
        operator: finalOperator,
      });
      payload.report.executive_summary = {
        ...payload.report.executive_summary,
        text: rebuilt.text,
      };
    }
    if (payload._execNarrativeParties) delete payload._execNarrativeParties;

    return res.json({ success: true, ...payload });
  } catch (err) {
    console.error("[golden-demo-ownership] hotel", err);
    return sendError(res, 500, "golden_demo_hotel_failed");
  }
}

export async function getGoldenDemoOwnershipGroup(req, res) {
  try {
    const slug = String(req.params.slug || GSF_SLUG).trim();
    const payload = buildOwnershipGroupProfile(slug);
    if (!payload.ok) return sendError(res, 404, payload.error || "group_not_found");
    return res.json({ success: true, ...payload });
  } catch (err) {
    console.error("[golden-demo-ownership] group", err);
    return sendError(res, 500, "golden_demo_group_failed");
  }
}

/** Focus Graph neighbors — Packet 2 query contract. */
export async function getGoldenDemoOwnershipNeighbors(req, res) {
  try {
    const q = req.query || {};
    const nodeId = String(q.node_id || q.nodeId || q.entity_id || "").trim();
    const nodeType = String(q.node_type || q.nodeType || "ownership_group").trim();
    const hops = Number(q.hops || q.depth || 1);
    if (!nodeId) return sendError(res, 400, "node_id_required");

    const csv = (v) =>
      String(v || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);

    const payload = buildNeighborsPayload({
      nodeId,
      nodeType,
      hops,
      relationshipTypes: csv(q.relationship_types || q.relationshipTypes),
      relationshipCategories: csv(q.relationship_categories || q.relationshipCategories || q.category),
      currentOnly: q.current_only !== "0" && q.currentOnly !== "0" && q.current_only !== "false",
      minimumConfidence: q.minimum_confidence || q.minimumConfidence || "PROBABLE",
      includeProbable: q.include_probable !== "0" && q.includeProbable !== "0" && q.include_probable !== "false",
      nodeTypes: csv(q.node_types || q.nodeTypes),
      limit: q.limit != null ? Number(q.limit) : undefined,
      expandAll: q.expand_all === "1" || q.expandAll === "1" || q.expand_all === "true",
    });
    if (payload.ok === false) return sendError(res, 404, payload.error || "node_not_found");
    return res.json({ success: true, ...payload });
  } catch (err) {
    console.error("[golden-demo-ownership] neighbors", err);
    return sendError(res, 500, "golden_demo_neighbors_failed");
  }
}
