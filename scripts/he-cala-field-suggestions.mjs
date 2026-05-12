/**
 * Verdict + copy-paste-ready suggested values for Hotel Equities (CALA) Operator Setup.
 * Select values MUST match option labels in public/third-party-operator-setup-new-two.html exactly.
 */

const OV_COMMERCIAL = `HE CALA combines in-market commercial leadership with Hotel Equities’ enterprise sales, marketing, and revenue management muscle. We align leisure, group, and business demand strategies to each destination—then execute with disciplined pricing governance, channel mix controls, and clear owner reporting so revenue decisions are visible, explainable, and tied to GOP outcomes.`;

const OV_DISCIPLINE = `We run accountable operating rhythms: brand QA and compliance calendars, standardized SOPs, procurement discipline, and finance controls supported by regional leadership and shared services. Owners see issues early, with corrective plans, timelines, and owners looped in when trade-offs affect guest experience or economics.`;

const OV_COMMUNICATION = `Owners get a predictable cadence—monthly financial and operational readouts plus deeper quarterly business reviews—and a clear “insight → decision → action” path for the top levers (mix, rate, F&B, labor, capex). Decisions are documented with rationale so ownership, asset management, and brand partners stay aligned.`;

const OV_FLEXIBILITY = `CALA assets often require pragmatic trade-offs across brand standards, local regulation, labor markets, and seasonality. We balance owner objectives with brand integrity using data-backed recommendations, staged investments, and transparent options—not rigid one-size-fits-all playbooks.`;

const OV_RISK = `Risk management spans safety and security, business continuity, insurance coordination, and crisis communications, with regional leadership accountable for execution. We coordinate with local counsel and brand programs so responses match the situation and the ownership structure.`;

const CAP_PROFILE_OPS = `HE CALA is built as an in-market third-party operator for the Caribbean and Latin America, with leadership across operations, finance, sales & marketing, HR, IT/openings, and shared services. We execute resort, lifestyle, and select-service assets with the same owner-aligned philosophy that powers Hotel Equities’ broader platform—local contracts, local experts, and enterprise support where it improves outcomes.`;

const CAP_PROFILE_COMM = `Commercially, we emphasize integrated demand generation: sales structure, marketing activation, and revenue management working as one system—not siloed functions. Owners benefit from clear commercial targets, comp-set discipline, and transparent performance dialogue tied to market dynamics in each destination.`;

const CAP_PROFILE_TRANS = `For transitions and openings, we use explicit 30/60/90-style milestones covering cash, payroll, safety, brand systems, staffing, and IT cutover—led by experienced openings and IT leadership. The goal is minimal guest disruption, stable teams, and fast clarity on the operating plan and owner reporting rhythm.`;

const BRAND_COMPLIANCE = `We treat brand standards as a quality floor while protecting owner economics. Brand QA programs are run proactively with corrective action plans when needed; when revenue trade-offs arise, we bring data (mix, channel, RGI where applicable) and recommend joint decisions with ownership and brand partners.`;

const BRAND_REL_MODEL = `We operate across franchised flags and independent / collection / resort product, with playbooks tuned to brand-managed versus owner-led repositioning. Our CALA portfolio mix reflects that flexibility—luxury and lifestyle alongside select-service where the market calls for it.`;

const RISK_PROGRAMS = `Our programs cover life safety, security, business continuity, insurance compliance, and crisis communications, scaled to asset type and location. Regional leadership coordinates with property teams, brands, and local counsel so responses are timely, documented, and appropriate to the ownership and lender context.`;

const DEAL_TERMS_PLACEHOLDER = `Complete only with Legal: economics are deal-specific. Until finalized, use your NDA data room for schedules (fees, central charges, performance tests, renewal, audits, PIP treatment). Do not paste placeholder percentages here.`;

const CRISIS = `HE CALA follows Hotel Equities’ enterprise crisis and business-continuity frameworks while adapting to regional risks (e.g., weather, supply disruption, security). Property-specific plans align to brand requirements and local law; regional leadership coordinates response, owner communication, and recovery priorities.`;

const INSURANCE = `Insurance and risk-transfer programs are maintained per management agreement, lender, and brand requirements, with certificates and coverage summaries coordinated through finance and property leadership.`;

const ESG = `Where brands or owners require sustainability measurement and reporting, we align data collection and reporting cadence to those standards. We focus on practical operational improvements (utilities, waste, responsible sourcing) without overstating claims—specific programs vary by asset and brand.`;

const CARBON = `Carbon and emissions tracking follows brand and owner requirements where applicable; we support credible measurement pathways and improvement initiatives that are operationally realistic for each property.`;

const ENERGY = `Energy programs emphasize utility optimization, preventive maintenance, and engineering standards aligned to brand and owner objectives, with reporting tied to operating reviews when targets exist.`;

const WASTE = `Waste and recycling programs follow brand standards and local regulation, with F&B and housekeeping SOPs tuned to each property’s volume and service level.`;

const SIG1 = `CALA portfolio scale (replace with your approved internal figure): open + pipeline hotels/rooms; anchor markets such as Dominican Republic, Jamaica, Mexico, USVI, Costa Rica, Eastern Caribbean.`;

const SIG2 = `HE group commercial scale—external use only if approved: reference franchise partner brand breadth and F&B/outlet scale from approved HE marketing materials.`;

const SIG3 = `In-market leadership: Miami plus Mexico City and Dominican Republic hubs; regional roles across Operations, Finance, Sales & Marketing, HR, IT/Openings, and Shared Services/Ops Finance.`;

const OV_FALLBACK = `Describe the owner outcomes HE CALA creates in the Caribbean and Latin America (commercial discipline, transparency, flexibility, risk management) in 3–5 sentences. Avoid statistics you cannot verify externally.`;

const BF_DEFAULT = `Choose values that match the CALA deals you pursue (e.g., resort/lifestyle vs urban gateway; franchised vs independent; realistic milestone timing from sourcing through opening). Use “Not Measured / N/A” on signals until you have a defensible benchmark.`;

const INFRA_DEFAULT = `Where systems vary by brand, state “Brand-dependent by asset” and list examples only when accurate. Summarize owner reporting (cadence + secure channels) without inventing vendor names.`;

const LEADERSHIP_FALLBACK = `Summarize CALA leadership bench (President, CDO, VP Ops, VP Finance, S&M, HR, IT/Openings, Shared Services). If using Explorer exec slots, mirror your top three leaders from the Leadership Team Members table.`;

/** @type {Record<string, { verdict: string, suggestedCopyPaste: string } | ((ctx: { raw: string, isEmpty: boolean, tab: string }) => { verdict: string, suggestedCopyPaste: string })>} */
const FIELD_SUGGESTIONS = {
    additionalBrands: { verdict: "Change", suggestedCopyPaste: "None" },
    crisisExperience: { verdict: "Change", suggestedCopyPaste: CRISIS },
    insuranceCoverage: { verdict: "Change", suggestedCopyPaste: INSURANCE },
    esgReporting: { verdict: "Change", suggestedCopyPaste: ESG },
    carbonTracking: { verdict: "Change", suggestedCopyPaste: CARBON },
    energyEfficiency: { verdict: "Change", suggestedCopyPaste: ENERGY },
    wasteReduction: { verdict: "Change", suggestedCopyPaste: WASTE },
    regions: { verdict: "Change", suggestedCopyPaste: "Caribbean, Latin America" },

    overview_signal_1_value: { verdict: "Change", suggestedCopyPaste: SIG1 },
    overview_signal_2_value: { verdict: "Change", suggestedCopyPaste: SIG2 },
    overview_signal_3_value: { verdict: "Change", suggestedCopyPaste: SIG3 },

    brand_narrative_compliance: { verdict: "Change", suggestedCopyPaste: BRAND_COMPLIANCE },
    brand_narrative_relationship: { verdict: "Change", suggestedCopyPaste: BRAND_REL_MODEL },
    brand_signal_audit: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
    brand_signal_reflag: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
    brand_signal_franchise_align: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },
    brand_signal_soft_retention: { verdict: "Change", suggestedCopyPaste: "Not Measured / N/A" },

    cap_kpi_operating_model: { verdict: "Update", suggestedCopyPaste: "Mixed Branded and Independent Portfolio" },
    cap_kpi_execution_strength: { verdict: "Update", suggestedCopyPaste: "Proven" },
    cap_kpi_transition: { verdict: "Update", suggestedCopyPaste: "Strong" },
    cap_kpi_reporting: { verdict: "Update", suggestedCopyPaste: "Advanced" },
    cap_profile_operational: { verdict: "Change", suggestedCopyPaste: CAP_PROFILE_OPS },
    cap_profile_commercial: { verdict: "Change", suggestedCopyPaste: CAP_PROFILE_COMM },
    cap_profile_transition: { verdict: "Change", suggestedCopyPaste: CAP_PROFILE_TRANS },
    cap_card_asset_positioning: {
        verdict: "Change",
        suggestedCopyPaste:
            "We position each CALA asset for its destination economics—resort and leisure anchors, urban gateway and lifestyle, or select-service where the demand story supports it—then align CapEx, brand standards, and commercial plans so the guest promise matches owner returns.",
    },
    cap_card_service_diff: {
        verdict: "Change",
        suggestedCopyPaste:
            "Differentiation comes from integrated execution: F&B and experience concepting where relevant, procurement leverage, training and talent systems, and a commercial engine that connects sales, marketing, and revenue management to on-property operations.",
    },
    cap_card_execution_rel: {
        verdict: "Change",
        suggestedCopyPaste:
            "Owner relationships are built on transparency and pace: clear decision rights, fast issue escalation to regional leaders, and joint action plans when performance deviates from plan—without surprises at month-end.",
    },
    cap_card_governance: {
        verdict: "Change",
        suggestedCopyPaste:
            "Governance includes disciplined reporting, internal controls, brand compliance oversight, and documented approvals for material decisions—scaled to institutional owners and single-asset sponsors alike.",
    },
    cap_deep_revenue_systems: {
        verdict: "Change",
        suggestedCopyPaste:
            "Revenue systems are brand-appropriate (PMS/RMS/channel tooling varies by flag) with governance on discounting, group displacement, and channel mix. Owners participate in commercial business reviews with clear comp-set and pacing context.",
    },
    cap_deep_execution_infra: {
        verdict: "Change",
        suggestedCopyPaste:
            "Execution infrastructure includes openings/IT leadership, HR and talent programs, finance shared services, and procurement—so properties run consistently while retaining local market responsiveness.",
    },
    cap_signal_budget: { verdict: "Update", suggestedCopyPaste: "Not Measured / N/A" },
    cap_signal_lift: { verdict: "Update", suggestedCopyPaste: "Not Measured / N/A" },
    cap_signal_trans: { verdict: "Update", suggestedCopyPaste: "Not Measured / N/A" },

    ov_card_commercial: { verdict: "Change", suggestedCopyPaste: OV_COMMERCIAL },
    ov_card_discipline: { verdict: "Change", suggestedCopyPaste: OV_DISCIPLINE },
    ov_card_communication: { verdict: "Change", suggestedCopyPaste: OV_COMMUNICATION },
    ov_card_flexibility: { verdict: "Change", suggestedCopyPaste: OV_FLEXIBILITY },
    ov_card_risk: { verdict: "Change", suggestedCopyPaste: OV_RISK },
    ov_cluster_interaction: {
        verdict: "Change",
        suggestedCopyPaste:
            "Typical owner interaction includes monthly operating and financial reviews, quarterly strategy sessions, and ad-hoc escalation paths for capex, brand, or commercial decisions—with materials distributed in advance and actions tracked to completion.",
    },
    ov_cluster_deliverables: {
        verdict: "Change",
        suggestedCopyPaste:
            "Deliverables include operating plans, forecasts, labor and productivity analytics, commercial pacing reports, capex trackers, brand QA status, and post-stay guest insight summaries—tailored to lender and owner requirements.",
    },
    ownerEngagementNarrative: {
        verdict: "Change",
        suggestedCopyPaste:
            "HE CALA is built for owners who want an operator that is present in the region: local leadership, shared services where they improve speed and quality, and the backing of a larger platform for procurement, training, and commercial scale—without losing accountability at the property.",
    },

    risk_programs_narrative: { verdict: "Change", suggestedCopyPaste: RISK_PROGRAMS },

    contactName: { verdict: "Change", suggestedCopyPaste: "Michael Register" },
    contactEmail: { verdict: "Change", suggestedCopyPaste: "mregister@hotelequities.com" },
    contactPhone: { verdict: "Change", suggestedCopyPaste: "+1 (305) 608-3522" },
    diligenceDocumentLinks: {
        verdict: "Change",
        suggestedCopyPaste: "https://www.hotelequities.com/cala",
    },

    tr_signal_revpar: ({ raw, isEmpty }) => {
        if (isEmpty || !raw || raw === "Negative or Flat") {
            return {
                verdict: "Change",
                suggestedCopyPaste: "Wide Dispersion Across Portfolio",
            };
        }
        return { verdict: "Keep", suggestedCopyPaste: raw };
    },
};

/**
 * @param {{ fieldName: string, tab: string, isEmpty: boolean, rawValue: unknown }} row
 * @returns {{ verdict: string, suggestedCopyPaste: string }}
 */
export function getSuggestionForRow(row) {
    const { fieldName, tab, isEmpty } = row;
    const raw = row.rawValue == null ? "" : String(row.rawValue).trim();

    const spec = FIELD_SUGGESTIONS[fieldName];
    if (typeof spec === "function") {
        return spec({ raw, isEmpty, tab });
    }
    if (spec && typeof spec === "object") {
        return { verdict: spec.verdict, suggestedCopyPaste: spec.suggestedCopyPaste };
    }

    if (tab === "Deal Terms" && isEmpty) {
        return { verdict: "Review", suggestedCopyPaste: DEAL_TERMS_PLACEHOLDER };
    }
    if (tab === "Infrastructure & Data" && isEmpty) {
        return { verdict: "Review", suggestedCopyPaste: INFRA_DEFAULT };
    }
    if (tab === "Best Fit & Preferences" && isEmpty) {
        return { verdict: "Review", suggestedCopyPaste: BF_DEFAULT };
    }
    if (tab === "Owner Value & Engagement" && isEmpty) {
        if (fieldName.startsWith("ov_")) {
            return { verdict: "Review", suggestedCopyPaste: OV_FALLBACK };
        }
        return {
            verdict: "Review",
            suggestedCopyPaste:
                "Enter only Finance-approved metrics, or leave blank until disclosure is approved. For narrative fields, describe owner outcomes in plain language.",
        };
    }
    if (tab === "Leadership & Team" && isEmpty) {
        return { verdict: "Review", suggestedCopyPaste: LEADERSHIP_FALLBACK };
    }

    if (!isEmpty && raw) {
        return { verdict: "Keep", suggestedCopyPaste: raw };
    }

    return {
        verdict: "Review",
        suggestedCopyPaste:
            "Add HE CALA–specific, owner-ready language. Have Legal/Comms review before external use.",
    };
}
