import Airtable from "airtable";
import {
    fetchAirtableTableFieldNameSet,
    filterFieldsToAirtableSchema,
    remapBasicsFieldsForAirtableSchema,
} from "./lib/third-party-operator-basics-airtable-column-aliases.js";
import { buildFootprintRowPayloadFromIntake } from "./lib/third-party-operator-footprint-intake.js";
import { parseFormattedInt } from "./lib/third-party-operator-value-utils.js";

/**
 * Same Airtable base as Brand Setup / Brand Library / My Brands (`AIRTABLE_BASE_ID` + `AIRTABLE_API_KEY`).
 * The "Third Party Operators" table lives in that base; no separate base env is used.
 */
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

// Table names or table IDs (tbl…) — optional env overrides
const TABLE_NAME = process.env.AIRTABLE_THIRD_PARTY_OPERATORS_TABLE || "3rd Party Operator - Basics";
const CASE_STUDIES_TABLE =
    process.env.AIRTABLE_THIRD_PARTY_OPERATOR_CASE_STUDIES_TABLE || "3rd Party Operator - Case Studies";
const OWNER_DILIGENCE_QA_TABLE =
    process.env.AIRTABLE_THIRD_PARTY_OPERATOR_OWNER_DILIGENCE_QA_TABLE || "3rd Party Operator - Owner Diligence QA";
const FOOTPRINT_TABLE =
    process.env.AIRTABLE_THIRD_PARTY_OPERATOR_FOOTPRINT_TABLE || "3rd Party Operator - Footprint";
const OPERATOR_BASICS_LINK_FIELD = "Operator (Basics Link)";

function parseJsonArrayInput(value) {
    if (value == null || value === "") return [];
    if (Array.isArray(value)) return value;
    if (typeof value === "string") {
        try {
            const parsed = JSON.parse(value);
            return Array.isArray(parsed) ? parsed : [];
        } catch {
            return [];
        }
    }
    return [];
}

function chunkArray(items, chunkSize) {
    const out = [];
    for (let i = 0; i < items.length; i += chunkSize) out.push(items.slice(i, i + chunkSize));
    return out;
}

async function createChildRecords(tableName, rows) {
    if (!Array.isArray(rows) || rows.length === 0) return;
    const chunks = chunkArray(rows, 10); // Airtable create max batch size
    for (const chunk of chunks) {
        await base(tableName).create(
            chunk.map((fields) => ({ fields })),
            { typecast: true }
        );
    }
}

function escapeAirtableFormulaString(value) {
    return String(value == null ? "" : value)
        .replace(/\\/g, "\\\\")
        .replace(/'/g, "\\'");
}

async function deleteRecordsByIds(tableName, ids) {
    if (!Array.isArray(ids) || ids.length === 0) return;
    const chunks = chunkArray(ids, 10);
    for (const chunk of chunks) {
        await base(tableName).destroy(chunk);
    }
}

async function replaceChildRecordsByOperatorId(tableName, operatorRecordId, rows) {
    const safeId = escapeAirtableFormulaString(operatorRecordId);
    const existing = await base(tableName)
        .select({
            filterByFormula: `{Operator Record ID}='${safeId}'`,
            fields: ["Operator Record ID"],
            pageSize: 100,
        })
        .all();
    if (existing.length) {
        await deleteRecordsByIds(
            tableName,
            existing.map((r) => r.id)
        );
    }
    await createChildRecords(tableName, rows);
}

async function upsertFootprintByOperatorId(operatorRecordId, fpPayload) {
    if (!fpPayload || typeof fpPayload !== "object") return;
    const safeId = escapeAirtableFormulaString(operatorRecordId);
    const candidates = await base(FOOTPRINT_TABLE)
        .select({
            filterByFormula: `OR({Operator Record ID}='${safeId}', FIND('${safeId}', ARRAYJOIN({${OPERATOR_BASICS_LINK_FIELD}})))`,
            fields: ["Operator Record ID", OPERATOR_BASICS_LINK_FIELD],
            pageSize: 100,
        })
        .all()
        .catch(() => []);
    if (candidates.length > 0) {
        await base(FOOTPRINT_TABLE).update(candidates[0].id, fpPayload, { typecast: true });
        return;
    }
    await base(FOOTPRINT_TABLE).create(fpPayload, { typecast: true });
}

function compactAirtableFieldPayload(obj) {
    return Object.fromEntries(
        Object.entries(obj).filter(([, value]) => {
            if (value == null) return false;
            if (typeof value === "string") return value.trim() !== "";
            if (Array.isArray(value)) return value.length > 0;
            return true;
        })
    );
}

/**
 * Submit third-party management operator intake form data to Airtable
 */
export default async function submitThirdPartyOperator(req, res) {
    try {
        if (req.method !== 'POST') {
            return res.status(405).json({ error: 'Method not allowed. Use POST to submit operator information.' });
        }

        const {
            recordId: inputRecordId,
            // Company Information
            companyName,
            website,
            headquarters,
            yearEstablished,
            contactEmail,
            contactPhone,
            contactName,
            preferredContactMethod,
            companyDescription,
            companyTagline,
            missionStatement,
            primaryServiceModel,
            companySize,
            yearsInBusiness,
            numberOfMarkets,
            portfolioMetricsAsOf,
            serviceDifferentiators,
            ownerResponseTime,
            concernResolutionTime,
            ownerEducation,
            ownerSatisfactionScore,
            ownerPortalFeatures,
            mgmtFeeMin,
            mgmtFeeMax,
            mgmtFeeBasis,
            mgmtFeeNotes,
            incentiveFeeMin,
            incentiveFeeMax,
            incentiveFeeBasis,
            incentiveFeeNotes,
            incentiveExcessMin,
            incentiveExcessMax,
            incentiveExcessBasis,
            incentiveExcessNotes,
            // Brand Support
            numberOfBrands,
            brands,
            chainScalesSupported,
            additionalBrands,
            brandsPortfolioDetail,
            // Geographic Coverage
            regions,
            specificMarkets,
            // Location Type Distribution (%)
            locationTypeUrban,
            locationTypeSuburban,
            locationTypeResort,
            locationTypeAirport,
            locationTypeSmallMetro,
            locationTypeInterstate,
            locationTypeTotal,
            // Exits/Deflaggings + Figures as of
            exitsDeflaggings,
            figuresAsOf,
            // Geographic Distribution (Existing vs Pipeline)
            geo_na_existing_hotels,
            geo_na_existing_rooms,
            geo_na_pipeline_hotels,
            geo_na_pipeline_rooms,
            geo_na_total_hotels,
            geo_na_total_rooms,
            geo_cala_existing_hotels,
            geo_cala_existing_rooms,
            geo_cala_pipeline_hotels,
            geo_cala_pipeline_rooms,
            geo_cala_total_hotels,
            geo_cala_total_rooms,
            geo_eu_existing_hotels,
            geo_eu_existing_rooms,
            geo_eu_pipeline_hotels,
            geo_eu_pipeline_rooms,
            geo_eu_total_hotels,
            geo_eu_total_rooms,
            geo_mea_existing_hotels,
            geo_mea_existing_rooms,
            geo_mea_pipeline_hotels,
            geo_mea_pipeline_rooms,
            geo_mea_total_hotels,
            geo_mea_total_rooms,
            geo_apac_existing_hotels,
            geo_apac_existing_rooms,
            geo_apac_pipeline_hotels,
            geo_apac_pipeline_rooms,
            geo_apac_total_hotels,
            geo_apac_total_rooms,
            geo_total_existing_hotels,
            geo_total_existing_rooms,
            geo_total_pipeline_hotels,
            geo_total_pipeline_rooms,
            geo_total_total_hotels,
            geo_total_total_rooms,
            // Chain Scale & Property Types
            chainScale,
            totalProperties,
            totalRooms,
            propertyTypes,
            additionalExperience,
            // Company History
            companyHistory,
            differentiators,
            achievements,
            managementPhilosophy,
            // Portfolio & Financial Metrics
            portfolioValue,
            annualRevenueManaged,
            portfolioGrowthRate,
            minPropertySize,
            maxPropertySize,
            avgPropertySize,
            // Performance Metrics
            revparImprovement,
            occupancyImprovement,
            noiImprovement,
            ownerRetention,
            renewalRate,
            turnaroundCount,
            stabilizationTime,
            // Team & Organizational Structure
            totalEmployees,
            avgOnSiteStaff,
            regionalTeams,
            avgExperience,
            keyLeadership,
            certifications,
            // Service Offerings
            revenueManagementServices,
            salesMarketingSupport,
            accountingReporting,
            procurementServices,
            hrTrainingServices,
            technologyServices,
            designRenovationSupport,
            developmentServices,
            // Property Experience Types
            newBuildExperience,
            conversionExperience,
            turnaroundExperience,
            preOpeningExperience,
            preOpeningRampLeadTimeMonths,
            transitionExperience,
            stabilizedExperience,
            renovationExperience,
            // Technology Stack
            primaryPMS,
            revenueManagementSystem,
            accountingSystem,
            guestCommunication,
            analyticsPlatform,
            mobileCheckin,
            ownerPortal,
            apiIntegrations,
            // Reporting & Transparency
            reportingFrequency,
            reportTypes,
            budgetProcess,
            capexPlanning,
            capexTolerance,
            performanceReviews,
            // Fee Structure Details
            baseFeeRange,
            incentiveFeeStructure,
            additionalFees,
            additionalFeeDetails,
            feeTransparency,
            performanceAdjustments,
            // Owner Relationship
            communicationStyle,
            ownerInvolvement,
            operatingCollaborationMode,
            decisionMaking,
            disputeResolution,
            ownerAdvisoryBoard,
            // References & Case Studies
            ownerReferences,
            caseStudiesDetail,
            ownerDiligenceQa,
            diligenceDocumentLinks,
            testimonialLinks,
            industryRecognition,
            lenderReferences,
            majorLenders,
            // Deal Terms
            minInitialTermQty,
            minInitialTermLength,
            minInitialTermDuration,
            renewalOptionQty,
            renewalOptionLength,
            renewalOptionDuration,
            renewalNoticeQty,
            renewalNoticeDuration,
            renewalStructure,
            renewalNoticeResponsibility,
            renewalConditions,
            performanceTestRequirement,
            curePeriodQty,
            curePeriodDuration,
            qaComplianceRequirement,
            pipAtRenewal,
            pipForConversions,
            // Economics, termination & risk norms
            baseFeeEscalation,
            baseFeeEscalationHow,
            feeMinimumFloor,
            feeMinimumFloorMin,
            feeMinimumFloorMax,
            feeMinimumFloorBasis,
            centralServiceAllocations,
            centralServiceAllocationsNotes,
            preOpeningFees,
            preOpeningFeesNotes,
            performanceMetricsUsed,
            performanceLookbackPeriod,
            performanceTerminationRights,
            ownerEarlyTerminationRights,
            ownerEarlyTerminationNotes,
            terminationFeeStructure,
            terminationFeeStructureNotes,
            keyMoneyCoInvestment,
            ownerFundedReserves,
            capReimbursableExpenses,
            auditRightsRequired,
            dealTermsAdditionalNotes,
            // Legacy Contract Terms (kept for backward compatibility)
            typicalContractLength,
            earlyTermination,
            renewalTerms,
            customizationWillingness,
            ownerExitRights,
            performanceGuarantees,
            // Crisis Management
            emergencyResponse,
            businessContinuity,
            crisisExperience,
            support24x7,
            insuranceCoverage,
            // Sustainability & ESG
            sustainabilityPrograms,
            esgReporting,
            energyEfficiency,
            wasteReduction,
            carbonTracking,
            // Additional Information
            avgContractTerm,
            feeStructure,
            // Chain scale per-segment metrics
            luxuryProperties,
            luxuryRooms,
            luxuryAvgStaff,
            luxuryExistingProperties,
            luxuryExistingRooms,
            luxuryPipelineProperties,
            luxuryPipelineRooms,
            upperUpscaleProperties,
            upperUpscaleRooms,
            upperUpscaleAvgStaff,
            upperUpscaleExistingProperties,
            upperUpscaleExistingRooms,
            upperUpscalePipelineProperties,
            upperUpscalePipelineRooms,
            upscaleProperties,
            upscaleRooms,
            upscaleAvgStaff,
            upscaleExistingProperties,
            upscaleExistingRooms,
            upscalePipelineProperties,
            upscalePipelineRooms,
            upperMidscaleProperties,
            upperMidscaleRooms,
            upperMidscaleAvgStaff,
            upperMidscaleExistingProperties,
            upperMidscaleExistingRooms,
            upperMidscalePipelineProperties,
            upperMidscalePipelineRooms,
            midscaleProperties,
            midscaleRooms,
            midscaleAvgStaff,
            midscaleExistingProperties,
            midscaleExistingRooms,
            midscalePipelineProperties,
            midscalePipelineRooms,
            economyProperties,
            economyRooms,
            economyAvgStaff,
            economyExistingProperties,
            economyExistingRooms,
            economyPipelineProperties,
            economyPipelineRooms,
            specializations,
            technology,
            testimonials,
            additionalNotes,
            // Ideal Project / Project Fit
            idealProjectTypes,
            idealBuildingTypes,
            idealAgreementTypes,
            idealRoomCountMin,
            idealRoomCountMax,
            idealProjectSizeMin,
            idealProjectSizeMax,
            minLeadTimeMonths,
            preferredOwnerType,
            coBrandingAllowed,
            brandedResidencesAllowed,
            mixedUseAllowed,
            priorityMarkets,
            marketsToAvoid,
            marketExpansionComfort,
            marketExpansionRampTimeMonths,
            ownerHotelExperience,
            projectStage,
            milestoneOperatorSelectionMinMonths,
            milestoneConstructionStartMinMonths,
            milestoneSoftOpeningMinMonths,
            milestoneGrandOpeningMinMonths,
            dateFlexibility,
            brandStatus,
            pipRepositioningDetails,
            ownerInvolvementLevel,
            ownerNonNegotiableTypes,
            ownerNonNegotiables,
            feeExpectationVsMarket,
            capexSupport,
            exitHorizon,
            capitalStatus,
            knownRedFlags,
            esgExpectations,
            idealProjectsAdditionalNotes,
            submittedAt
        } = req.body;

        // Validate required fields
        if (!companyName || !website || !headquarters || !yearEstablished || !contactEmail) {
            return res.status(400).json({ 
                error: 'Missing required fields',
                required: ['companyName', 'website', 'headquarters', 'yearEstablished', 'contactEmail']
            });
        }

        if (!numberOfBrands || numberOfBrands < 1) {
            return res.status(400).json({ 
                error: 'Number of brands supported must be at least 1'
            });
        }

        if (!brands || (typeof brands === 'string' && brands.trim() === '') || (Array.isArray(brands) && brands.length === 0)) {
            return res.status(400).json({ 
                error: 'At least one brand must be selected'
            });
        }

        if (!regions || (typeof regions === 'string' && regions.trim() === '') || (Array.isArray(regions) && regions.length === 0)) {
            return res.status(400).json({ 
                error: 'At least one region must be selected'
            });
        }

        if (!chainScale || (typeof chainScale === 'string' && chainScale.trim() === '') || (Array.isArray(chainScale) && chainScale.length === 0)) {
            return res.status(400).json({ 
                error: 'At least one chain scale must be selected'
            });
        }

        const totalPropertiesParsed = parseFormattedInt(totalProperties);
        if (totalPropertiesParsed == null || totalPropertiesParsed < 1) {
            return res.status(400).json({ 
                error: 'Total properties managed must be at least 1'
            });
        }

        // Prepare fields for Airtable
        // Convert comma-separated strings to arrays for multiple select fields
        const formatMultiSelect = (value) => {
            if (!value) return [];
            if (Array.isArray(value)) return value.filter(v => v && String(v).trim() !== '');
            if (typeof value === 'string') {
                return value.split(',').map(v => v.trim()).filter(v => v !== '');
            }
            return [];
        };

        /** Store repeater / Q&A payloads in Airtable Long text fields as JSON */
        const stringifyJsonArrayField = (value) => {
            if (value == null || value === '') return '';
            if (Array.isArray(value)) return JSON.stringify(value);
            if (typeof value === 'string') {
                try {
                    const parsed = JSON.parse(value);
                    return JSON.stringify(Array.isArray(parsed) ? parsed : []);
                } catch {
                    return value.trim();
                }
            }
            return '';
        };

        const brandsPortfolioArray = (() => {
            if (brandsPortfolioDetail == null || brandsPortfolioDetail === '') return [];
            if (Array.isArray(brandsPortfolioDetail)) return brandsPortfolioDetail;
            if (typeof brandsPortfolioDetail === 'string') {
                try {
                    const parsed = JSON.parse(brandsPortfolioDetail);
                    return Array.isArray(parsed) ? parsed : [];
                } catch {
                    return [];
                }
            }
            return [];
        })();

        const formatOptionalPercentField = (v) => {
            if (v == null) return '';
            const s = String(v).trim();
            if (s === '') return '';
            if (/%\s*$/.test(s)) return s.replace(/\s+/g, '');
            const n = parseFloat(s.replace(/,/g, ''));
            if (!Number.isNaN(n)) return `${n}%`;
            return s;
        };

        const formatPortfolioGrowthForAirtable = (v) => {
            if (v == null || String(v).trim() === '') return '';
            const s = String(v).trim();
            const n = parseFloat(s);
            if (!Number.isNaN(n)) return `${n} properties per year`;
            return s;
        };

        const formatTurnaroundCountForAirtable = (v) => {
            if (v == null || String(v).trim() === '') return '';
            const s = String(v).trim();
            const n = parseInt(s, 10);
            if (!Number.isNaN(n)) return String(n);
            return s;
        };

        const formatOwnerFundedPercent = (v) => {
            if (v == null || String(v).trim() === '') return '';
            const s = String(v).trim();
            if (/%/.test(s)) return s;
            const n = parseFloat(s.replace(/,/g, ''));
            if (!Number.isNaN(n)) return `${n}%`;
            return s;
        };

        const formatUsdFloorField = (v) => {
            if (v == null || String(v).trim() === '') return '';
            const n = parseFloat(String(v).trim().replace(/[$,]/g, ''));
            if (!Number.isNaN(n)) return `$${Math.round(n).toLocaleString('en-US')}`;
            return String(v).trim();
        };

        const fieldPresent = (v) => v != null && String(v).trim() !== '';

        const mgmtRangeStr = (() => {
            const hasMin = fieldPresent(mgmtFeeMin);
            const hasMax = fieldPresent(mgmtFeeMax);
            if (hasMin && hasMax) return `${formatOptionalPercentField(mgmtFeeMin)}–${formatOptionalPercentField(mgmtFeeMax)}`;
            if (hasMin) return formatOptionalPercentField(mgmtFeeMin);
            if (hasMax) return formatOptionalPercentField(mgmtFeeMax);
            return '';
        })();
        const baseFeeRangeFromGrid = [
            mgmtRangeStr,
            mgmtFeeBasis,
            mgmtFeeNotes
        ].filter((v) => v != null && String(v).trim() !== '').join(' | ');

        const incentiveLine = (() => {
            if (
                !fieldPresent(incentiveFeeMin) &&
                !fieldPresent(incentiveFeeMax) &&
                !fieldPresent(incentiveFeeBasis) &&
                !fieldPresent(incentiveFeeNotes)
            ) {
                return '';
            }
            const hasMin = fieldPresent(incentiveFeeMin);
            const hasMax = fieldPresent(incentiveFeeMax);
            let range = '';
            if (hasMin && hasMax) {
                range = `${formatOptionalPercentField(incentiveFeeMin)}–${formatOptionalPercentField(incentiveFeeMax)}`;
            } else if (hasMin) {
                range = formatOptionalPercentField(incentiveFeeMin);
            } else if (hasMax) {
                range = formatOptionalPercentField(incentiveFeeMax);
            }
            return ['Typical incentive:', range, incentiveFeeBasis, incentiveFeeNotes].filter((v) => v != null && String(v).trim() !== '').join(' ');
        })();
        const excessLine = (() => {
            if (
                !fieldPresent(incentiveExcessMin) &&
                !fieldPresent(incentiveExcessMax) &&
                !fieldPresent(incentiveExcessBasis) &&
                !fieldPresent(incentiveExcessNotes)
            ) {
                return '';
            }
            const hasMin = fieldPresent(incentiveExcessMin);
            const hasMax = fieldPresent(incentiveExcessMax);
            let range = '';
            if (hasMin && hasMax) {
                range = `${formatOptionalPercentField(incentiveExcessMin)}–${formatOptionalPercentField(incentiveExcessMax)}`;
            } else if (hasMin) {
                range = formatOptionalPercentField(incentiveExcessMin);
            } else if (hasMax) {
                range = formatOptionalPercentField(incentiveExcessMax);
            }
            return ['Excess / hurdle:', range, incentiveExcessBasis, incentiveExcessNotes].filter((v) => v != null && String(v).trim() !== '').join(' ');
        })();
        const incentiveFeeStructureFromGrid = [incentiveLine, excessLine].filter(Boolean).join('\n');

        const fields = {
            // Company Information
            'Company Name': String(companyName).trim(),
            'Website': String(website).trim(),
            'Headquarters': String(headquarters).trim(),
            'Year Established': parseInt(yearEstablished, 10),
            'Contact Email': String(contactEmail).trim().toLowerCase(),
            'Contact Phone': contactPhone ? String(contactPhone).trim() : '',
            'Contact Name': contactName ? String(contactName).trim() : '',
            'Preferred Contact Method': preferredContactMethod ? String(preferredContactMethod).trim() : '',
            'Company Description': companyDescription ? String(companyDescription).trim() : '',
            'Company Tagline': companyTagline ? String(companyTagline).trim() : '',
            'Mission Statement': missionStatement ? String(missionStatement).trim() : '',
            'Primary Service Model': primaryServiceModel ? String(primaryServiceModel).trim() : '',
            'Company Size': companySize ? String(companySize).trim() : '',
            'Years in Business': yearsInBusiness !== undefined && yearsInBusiness !== '' ? parseInt(yearsInBusiness, 10) : null,
            'Number of Markets Operated In': numberOfMarkets !== undefined && numberOfMarkets !== '' ? parseInt(numberOfMarkets, 10) : null,
            'Portfolio Metrics As of Date': portfolioMetricsAsOf ? String(portfolioMetricsAsOf).trim() : '',
            'Service Offering Summary': serviceDifferentiators ? String(serviceDifferentiators).trim() : '',
            'Typical Owner Response Time': ownerResponseTime ? String(ownerResponseTime).trim() : '',
            'Typical Concern Resolution Time': concernResolutionTime ? String(concernResolutionTime).trim() : '',
            'Owner Education Programs': ownerEducation ? String(ownerEducation).trim() : '',
            'Owner Satisfaction Score (NPS)': ownerSatisfactionScore !== undefined && ownerSatisfactionScore !== '' ? parseFloat(ownerSatisfactionScore) : null,
            'Owner Portal Features': ownerPortalFeatures ? String(ownerPortalFeatures).trim() : '',
            // Brand Support
            'Number of Brands Supported': parseInt(numberOfBrands, 10),
            'Brands Managed': formatMultiSelect(brands),
            'Chain Scales You Support': formatMultiSelect(chainScalesSupported),
            'Additional Brands': additionalBrands ? String(additionalBrands).trim() : '',
            'Brands Portfolio Detail': stringifyJsonArrayField(brandsPortfolioArray),
            // Geographic Coverage
            'Regions Supported': formatMultiSelect(regions),
            'Specific Markets': specificMarkets ? String(specificMarkets).trim() : '',
            // Location Type Distribution (%)
            'Location Type % Urban': locationTypeUrban ? parseFloat(locationTypeUrban) : null,
            'Location Type % Suburban': locationTypeSuburban ? parseFloat(locationTypeSuburban) : null,
            'Location Type % Resort': locationTypeResort ? parseFloat(locationTypeResort) : null,
            'Location Type % Airport': locationTypeAirport ? parseFloat(locationTypeAirport) : null,
            'Location Type % Small Metro/Town': locationTypeSmallMetro ? parseFloat(locationTypeSmallMetro) : null,
            'Location Type % Interstate': locationTypeInterstate ? parseFloat(locationTypeInterstate) : null,
            'Location Type % Total': locationTypeTotal ? parseFloat(locationTypeTotal) : null,
            'Exits/Deflaggings (Past 24 Months)': exitsDeflaggings ? parseInt(exitsDeflaggings, 10) : null,
            'Figures As Of': figuresAsOf ? String(figuresAsOf).trim() : '',
            // Geographic Distribution (Existing vs Pipeline)
            'Geo NA Existing Hotels': parseFormattedInt(geo_na_existing_hotels),
            'Geo NA Existing Rooms': parseFormattedInt(geo_na_existing_rooms),
            'Geo NA Pipeline Hotels': parseFormattedInt(geo_na_pipeline_hotels),
            'Geo NA Pipeline Rooms': parseFormattedInt(geo_na_pipeline_rooms),
            'Geo NA Total Hotels': parseFormattedInt(geo_na_total_hotels),
            'Geo NA Total Rooms': parseFormattedInt(geo_na_total_rooms),

            'Geo CALA Existing Hotels': parseFormattedInt(geo_cala_existing_hotels),
            'Geo CALA Existing Rooms': parseFormattedInt(geo_cala_existing_rooms),
            'Geo CALA Pipeline Hotels': parseFormattedInt(geo_cala_pipeline_hotels),
            'Geo CALA Pipeline Rooms': parseFormattedInt(geo_cala_pipeline_rooms),
            'Geo CALA Total Hotels': parseFormattedInt(geo_cala_total_hotels),
            'Geo CALA Total Rooms': parseFormattedInt(geo_cala_total_rooms),

            'Geo EU Existing Hotels': parseFormattedInt(geo_eu_existing_hotels),
            'Geo EU Existing Rooms': parseFormattedInt(geo_eu_existing_rooms),
            'Geo EU Pipeline Hotels': parseFormattedInt(geo_eu_pipeline_hotels),
            'Geo EU Pipeline Rooms': parseFormattedInt(geo_eu_pipeline_rooms),
            'Geo EU Total Hotels': parseFormattedInt(geo_eu_total_hotels),
            'Geo EU Total Rooms': parseFormattedInt(geo_eu_total_rooms),

            'Geo MEA Existing Hotels': parseFormattedInt(geo_mea_existing_hotels),
            'Geo MEA Existing Rooms': parseFormattedInt(geo_mea_existing_rooms),
            'Geo MEA Pipeline Hotels': parseFormattedInt(geo_mea_pipeline_hotels),
            'Geo MEA Pipeline Rooms': parseFormattedInt(geo_mea_pipeline_rooms),
            'Geo MEA Total Hotels': parseFormattedInt(geo_mea_total_hotels),
            'Geo MEA Total Rooms': parseFormattedInt(geo_mea_total_rooms),

            'Geo APAC Existing Hotels': parseFormattedInt(geo_apac_existing_hotels),
            'Geo APAC Existing Rooms': parseFormattedInt(geo_apac_existing_rooms),
            'Geo APAC Pipeline Hotels': parseFormattedInt(geo_apac_pipeline_hotels),
            'Geo APAC Pipeline Rooms': parseFormattedInt(geo_apac_pipeline_rooms),
            'Geo APAC Total Hotels': parseFormattedInt(geo_apac_total_hotels),
            'Geo APAC Total Rooms': parseFormattedInt(geo_apac_total_rooms),

            'Geo Total Existing Hotels': parseFormattedInt(geo_total_existing_hotels),
            'Geo Total Existing Rooms': parseFormattedInt(geo_total_existing_rooms),
            'Geo Total Pipeline Hotels': parseFormattedInt(geo_total_pipeline_hotels),
            'Geo Total Pipeline Rooms': parseFormattedInt(geo_total_pipeline_rooms),
            'Geo Total Hotels': parseFormattedInt(geo_total_total_hotels),
            'Geo Total Rooms': parseFormattedInt(geo_total_total_rooms),
            // Chain Scale & Property Types
            'Chain Scale': formatMultiSelect(chainScale),
            'Total Properties Managed': totalPropertiesParsed,
            'Total Rooms Managed': parseFormattedInt(totalRooms),
            'Property Types': formatMultiSelect(propertyTypes),
            // Chain scale per-segment metrics
            'Luxury Properties Managed': parseFormattedInt(luxuryProperties),
            'Luxury Rooms Managed': parseFormattedInt(luxuryRooms),
            'Luxury Avg On-Site Staff Per Property': luxuryAvgStaff ? parseFloat(luxuryAvgStaff) : null,
            'Luxury Existing Properties': parseFormattedInt(luxuryExistingProperties),
            'Luxury Existing Rooms': parseFormattedInt(luxuryExistingRooms),
            'Luxury Pipeline Properties': parseFormattedInt(luxuryPipelineProperties),
            'Luxury Pipeline Rooms': parseFormattedInt(luxuryPipelineRooms),
            'Upper Upscale Properties Managed': parseFormattedInt(upperUpscaleProperties),
            'Upper Upscale Rooms Managed': parseFormattedInt(upperUpscaleRooms),
            'Upper Upscale Avg On-Site Staff Per Property': upperUpscaleAvgStaff ? parseFloat(upperUpscaleAvgStaff) : null,
            'Upper Upscale Existing Properties': parseFormattedInt(upperUpscaleExistingProperties),
            'Upper Upscale Existing Rooms': parseFormattedInt(upperUpscaleExistingRooms),
            'Upper Upscale Pipeline Properties': parseFormattedInt(upperUpscalePipelineProperties),
            'Upper Upscale Pipeline Rooms': parseFormattedInt(upperUpscalePipelineRooms),
            'Upscale Properties Managed': parseFormattedInt(upscaleProperties),
            'Upscale Rooms Managed': parseFormattedInt(upscaleRooms),
            'Upscale Avg On-Site Staff Per Property': upscaleAvgStaff ? parseFloat(upscaleAvgStaff) : null,
            'Upscale Existing Properties': parseFormattedInt(upscaleExistingProperties),
            'Upscale Existing Rooms': parseFormattedInt(upscaleExistingRooms),
            'Upscale Pipeline Properties': parseFormattedInt(upscalePipelineProperties),
            'Upscale Pipeline Rooms': parseFormattedInt(upscalePipelineRooms),
            'Upper Midscale Properties Managed': parseFormattedInt(upperMidscaleProperties),
            'Upper Midscale Rooms Managed': parseFormattedInt(upperMidscaleRooms),
            'Upper Midscale Avg On-Site Staff Per Property': upperMidscaleAvgStaff ? parseFloat(upperMidscaleAvgStaff) : null,
            'Upper Midscale Existing Properties': parseFormattedInt(upperMidscaleExistingProperties),
            'Upper Midscale Existing Rooms': parseFormattedInt(upperMidscaleExistingRooms),
            'Upper Midscale Pipeline Properties': parseFormattedInt(upperMidscalePipelineProperties),
            'Upper Midscale Pipeline Rooms': parseFormattedInt(upperMidscalePipelineRooms),
            'Midscale Properties Managed': parseFormattedInt(midscaleProperties),
            'Midscale Rooms Managed': parseFormattedInt(midscaleRooms),
            'Midscale Avg On-Site Staff Per Property': midscaleAvgStaff ? parseFloat(midscaleAvgStaff) : null,
            'Midscale Existing Properties': parseFormattedInt(midscaleExistingProperties),
            'Midscale Existing Rooms': parseFormattedInt(midscaleExistingRooms),
            'Midscale Pipeline Properties': parseFormattedInt(midscalePipelineProperties),
            'Midscale Pipeline Rooms': parseFormattedInt(midscalePipelineRooms),
            'Economy Properties Managed': parseFormattedInt(economyProperties),
            'Economy Rooms Managed': parseFormattedInt(economyRooms),
            'Economy Avg On-Site Staff Per Property': economyAvgStaff ? parseFloat(economyAvgStaff) : null,
            'Economy Existing Properties': parseFormattedInt(economyExistingProperties),
            'Economy Existing Rooms': parseFormattedInt(economyExistingRooms),
            'Economy Pipeline Properties': parseFormattedInt(economyPipelineProperties),
            'Economy Pipeline Rooms': parseFormattedInt(economyPipelineRooms),
            // Company History
            'Company History': companyHistory ? String(companyHistory).trim() : '',
            'Key Differentiators': differentiators ? String(differentiators).trim() : '',
            'Notable Achievements': achievements ? String(achievements).trim() : '',
            'Management Philosophy': managementPhilosophy ? String(managementPhilosophy).trim() : '',
            // Portfolio & Financial Metrics
            'Portfolio Value': portfolioValue ? String(portfolioValue).trim() : '',
            'Annual Revenue Managed': annualRevenueManaged ? String(annualRevenueManaged).trim() : '',
            'Portfolio Growth Rate': portfolioGrowthRate ? String(portfolioGrowthRate).trim() : '',
            'Min Property Size': minPropertySize ? parseInt(minPropertySize, 10) : null,
            'Max Property Size': maxPropertySize ? parseInt(maxPropertySize, 10) : null,
            'Avg Property Size': avgPropertySize ? parseInt(avgPropertySize, 10) : null,
            // Performance Metrics
            'RevPAR Improvement': revparImprovement ? parseFloat(revparImprovement) : null,
            'Occupancy Improvement': occupancyImprovement ? parseFloat(occupancyImprovement) : null,
            'NOI Improvement': noiImprovement ? parseFloat(noiImprovement) : null,
            'Owner Retention Rate': ownerRetention ? parseFloat(ownerRetention) : null,
            'Renewal Rate': renewalRate ? parseFloat(renewalRate) : null,
            'Properties Turned Around': formatTurnaroundCountForAirtable(turnaroundCount),
            'Time to Stabilization': stabilizationTime ? parseInt(stabilizationTime, 10) : null,
            // Team & Organizational Structure
            'Total Employees': totalEmployees ? parseInt(totalEmployees, 10) : null,
            'Avg On-Site Staff': avgOnSiteStaff ? parseFloat(avgOnSiteStaff) : null,
            'Regional Teams': regionalTeams ? parseInt(regionalTeams, 10) : null,
            'Avg Experience Years': avgExperience ? parseFloat(avgExperience) : null,
            'Key Leadership': keyLeadership ? String(keyLeadership).trim() : '',
            'Certifications': certifications ? String(certifications).trim() : '',
            // Service Offerings
            'Revenue Management Services': formatMultiSelect(revenueManagementServices),
            'Sales Marketing Support': formatMultiSelect(salesMarketingSupport),
            'Accounting Reporting': formatMultiSelect(accountingReporting),
            'Procurement Services': formatMultiSelect(procurementServices),
            'HR Training Services': formatMultiSelect(hrTrainingServices),
            'Technology Services': formatMultiSelect(technologyServices),
            'Design Renovation Support': formatMultiSelect(designRenovationSupport),
            'Development Services': formatMultiSelect(developmentServices),
            // Property Experience Types
            'New Build Experience': newBuildExperience ? String(newBuildExperience).trim() : '',
            'Conversion Experience': conversionExperience ? String(conversionExperience).trim() : '',
            'Turnaround Experience': turnaroundExperience ? String(turnaroundExperience).trim() : '',
            'Pre-Opening Experience': preOpeningExperience ? String(preOpeningExperience).trim() : '',
            'Pre-Opening Ramp Lead Time (Months)': preOpeningRampLeadTimeMonths ? parseInt(preOpeningRampLeadTimeMonths, 10) : null,
            'Transition Experience': transitionExperience ? String(transitionExperience).trim() : '',
            'Stabilized Experience': stabilizedExperience ? String(stabilizedExperience).trim() : '',
            'Renovation Experience': renovationExperience ? String(renovationExperience).trim() : '',
            'Additional Experience Types': formatMultiSelect(additionalExperience),
            // Technology Stack
            'Primary PMS': primaryPMS ? String(primaryPMS).trim() : '',
            'Revenue Management System': revenueManagementSystem ? String(revenueManagementSystem).trim() : '',
            'Accounting System': accountingSystem ? String(accountingSystem).trim() : '',
            'Guest Communication': guestCommunication ? String(guestCommunication).trim() : '',
            'Analytics Platform': analyticsPlatform ? String(analyticsPlatform).trim() : '',
            'Mobile Check-in': mobileCheckin ? String(mobileCheckin).trim() : '',
            'Owner Portal': ownerPortal ? String(ownerPortal).trim() : '',
            'API Integrations': apiIntegrations ? String(apiIntegrations).trim() : '',
            // Reporting & Transparency
            'Reporting Frequency': reportingFrequency ? String(reportingFrequency).trim() : '',
            'Report Types': formatMultiSelect(reportTypes),
            'Budget Process': budgetProcess ? String(budgetProcess).trim() : '',
            'Capex Planning': capexPlanning ? String(capexPlanning).trim() : '',
            'CapEx Tolerance': capexTolerance ? String(capexTolerance).trim() : '',
            'Performance Reviews': performanceReviews ? String(performanceReviews).trim() : '',
            // Fee Structure Details
            'Base Fee Range': (baseFeeRange ? String(baseFeeRange).trim() : '') || baseFeeRangeFromGrid,
            'Incentive Fee Structure': (incentiveFeeStructure ? String(incentiveFeeStructure).trim() : '') || incentiveFeeStructureFromGrid,
            'Additional Fees': formatMultiSelect(additionalFees),
            'Additional Fee Details': additionalFeeDetails ? String(additionalFeeDetails).trim() : '',
            'Fee Transparency': feeTransparency ? String(feeTransparency).trim() : '',
            'Performance Adjustments': performanceAdjustments ? String(performanceAdjustments).trim() : '',
            // Owner Relationship
            'Communication Style': communicationStyle ? String(communicationStyle).trim() : '',
            'Owner Involvement': ownerInvolvement ? String(ownerInvolvement).trim() : '',
            'Operating Collaboration Mode': operatingCollaborationMode ? String(operatingCollaborationMode).trim() : '',
            'Decision Making Process': decisionMaking ? String(decisionMaking).trim() : '',
            'Dispute Resolution': disputeResolution ? String(disputeResolution).trim() : '',
            'Owner Advisory Board': ownerAdvisoryBoard ? String(ownerAdvisoryBoard).trim() : '',
            // References & Case Studies
            'Owner References': ownerReferences ? parseInt(ownerReferences, 10) : null,
            // Add Long text columns in Airtable if missing: Case Studies Detail, Owner Diligence Q&A, Owner Diligence Document Links
            'Case Studies Detail': stringifyJsonArrayField(caseStudiesDetail),
            'Owner Diligence Q&A': stringifyJsonArrayField(ownerDiligenceQa),
            'Owner Diligence Document Links': diligenceDocumentLinks ? String(diligenceDocumentLinks).trim() : '',
            'Testimonial Links': testimonialLinks ? String(testimonialLinks).trim() : '',
            'Industry Recognition': industryRecognition ? String(industryRecognition).trim() : '',
            'Lender References': lenderReferences ? String(lenderReferences).trim() : '',
            'Major Lenders': majorLenders ? String(majorLenders).trim() : '',
            // Deal Terms
            'Min Initial Term Qty': minInitialTermQty ? String(minInitialTermQty).trim() : '',
            'Min Initial Term Length': minInitialTermLength ? String(minInitialTermLength).trim() : '',
            'Min Initial Term Duration': minInitialTermDuration ? String(minInitialTermDuration).trim() : '',
            'Renewal Option Qty': renewalOptionQty ? String(renewalOptionQty).trim() : '',
            'Renewal Option Length': renewalOptionLength ? String(renewalOptionLength).trim() : '',
            'Renewal Option Duration': renewalOptionDuration ? String(renewalOptionDuration).trim() : '',
            'Renewal Notice Qty': renewalNoticeQty ? String(renewalNoticeQty).trim() : '',
            'Renewal Notice Duration': renewalNoticeDuration ? String(renewalNoticeDuration).trim() : '',
            'Renewal Structure': renewalStructure ? String(renewalStructure).trim() : '',
            'Renewal Notice Responsibility': renewalNoticeResponsibility ? String(renewalNoticeResponsibility).trim() : '',
            'Renewal Conditions': renewalConditions ? String(renewalConditions).trim() : '',
            'Performance Test Requirement': performanceTestRequirement ? String(performanceTestRequirement).trim() : '',
            'Cure Period Qty': curePeriodQty ? String(curePeriodQty).trim() : '',
            'Cure Period Duration': curePeriodDuration ? String(curePeriodDuration).trim() : '',
            'QA Compliance Requirement': qaComplianceRequirement ? String(qaComplianceRequirement).trim() : '',
            'PIP at Renewal': pipAtRenewal ? String(pipAtRenewal).trim() : '',
            'PIP for Conversions': pipForConversions ? String(pipForConversions).trim() : '',
            // Economics, termination & risk norms
            'Base Fee Escalation': baseFeeEscalation ? String(baseFeeEscalation).trim() : '',
            'Base Fee Escalation How': baseFeeEscalationHow ? String(baseFeeEscalationHow).trim() : '',
            'Minimum Fee Floor': feeMinimumFloor ? String(feeMinimumFloor).trim() : '',
            'Minimum Fee Floor Min': formatUsdFloorField(feeMinimumFloorMin),
            'Minimum Fee Floor Max': formatUsdFloorField(feeMinimumFloorMax),
            'Minimum Fee Floor Basis': feeMinimumFloorBasis ? String(feeMinimumFloorBasis).trim() : '',
            'Central Service Allocations': centralServiceAllocations ? String(centralServiceAllocations).trim() : '',
            'Central Service Allocations Notes': centralServiceAllocationsNotes ? String(centralServiceAllocationsNotes).trim() : '',
            'Pre-Opening Fees Types': formatMultiSelect(preOpeningFees),
            'Pre-Opening Fees Notes': preOpeningFeesNotes ? String(preOpeningFeesNotes).trim() : '',
            'Performance Metrics Used': formatMultiSelect(performanceMetricsUsed),
            'Performance Lookback Period': performanceLookbackPeriod ? String(performanceLookbackPeriod).trim() : '',
            'Performance Termination Rights': performanceTerminationRights ? String(performanceTerminationRights).trim() : '',
            'Owner Early Termination Rights': ownerEarlyTerminationRights ? String(ownerEarlyTerminationRights).trim() : '',
            'Owner Early Termination Notes': ownerEarlyTerminationNotes ? String(ownerEarlyTerminationNotes).trim() : '',
            'Termination Fee Structure': terminationFeeStructure ? String(terminationFeeStructure).trim() : '',
            'Termination Fee Structure Notes': terminationFeeStructureNotes ? String(terminationFeeStructureNotes).trim() : '',
            'Key Money / Co-Investment': keyMoneyCoInvestment ? String(keyMoneyCoInvestment).trim() : '',
            'Owner-Funded Reserves Expectations': formatOwnerFundedPercent(ownerFundedReserves),
            'Cap Operator Reimbursable Expenses': capReimbursableExpenses ? String(capReimbursableExpenses).trim() : '',
            'Audit Rights Required': auditRightsRequired ? String(auditRightsRequired).trim() : '',
            'Deal Terms Additional Notes': dealTermsAdditionalNotes ? String(dealTermsAdditionalNotes).trim() : '',
            // Legacy Contract Terms (kept for backward compatibility)
            'Typical Contract Length': typicalContractLength ? String(typicalContractLength).trim() : '',
            'Early Termination': earlyTermination ? String(earlyTermination).trim() : '',
            'Renewal Terms': renewalTerms ? String(renewalTerms).trim() : '',
            'Customization Willingness': customizationWillingness ? String(customizationWillingness).trim() : '',
            'Owner Exit Rights': ownerExitRights ? String(ownerExitRights).trim() : '',
            'Performance Guarantees': performanceGuarantees ? String(performanceGuarantees).trim() : '',
            // Crisis Management
            'Emergency Response': emergencyResponse ? String(emergencyResponse).trim() : '',
            'Business Continuity': businessContinuity ? String(businessContinuity).trim() : '',
            'Crisis Experience': crisisExperience ? String(crisisExperience).trim() : '',
            '24/7 Support': support24x7 ? String(support24x7).trim() : '',
            'Insurance Coverage': insuranceCoverage ? String(insuranceCoverage).trim() : '',
            // Sustainability & ESG
            'Sustainability Programs': sustainabilityPrograms ? String(sustainabilityPrograms).trim() : '',
            'ESG Reporting': esgReporting ? String(esgReporting).trim() : '',
            'Energy Efficiency': energyEfficiency ? String(energyEfficiency).trim() : '',
            'Waste Reduction': wasteReduction ? String(wasteReduction).trim() : '',
            'Carbon Tracking': carbonTracking ? String(carbonTracking).trim() : '',
            // Additional Information
            'Average Contract Term': avgContractTerm ? String(avgContractTerm).trim() : '',
            'Fee Structure': feeStructure ? String(feeStructure).trim() : '',
            'Specializations': specializations ? String(specializations).trim() : '',
            'Technology & Systems': technology ? String(technology).trim() : '',
            'Owner Testimonials': testimonials ? String(testimonials).trim() : '',
            'Additional Notes': additionalNotes ? String(additionalNotes).trim() : '',
            // Ideal Project / Project Fit
            'Ideal Project Types': formatMultiSelect(idealProjectTypes),
            'Ideal Building Types': formatMultiSelect(idealBuildingTypes),
            'Ideal Agreement Types': formatMultiSelect(idealAgreementTypes),
            'Ideal Room Count Min': idealRoomCountMin ? parseInt(idealRoomCountMin, 10) : null,
            'Ideal Room Count Max': idealRoomCountMax ? parseInt(idealRoomCountMax, 10) : null,
            'Ideal Project Size Min': idealProjectSizeMin ? parseInt(idealProjectSizeMin, 10) : null,
            'Ideal Project Size Max': idealProjectSizeMax ? parseInt(idealProjectSizeMax, 10) : null,
            'Min Lead Time Months': minLeadTimeMonths ? parseInt(minLeadTimeMonths, 10) : null,
            'Preferred Owner Type': preferredOwnerType ? String(preferredOwnerType).trim() : '',
            'Co-Branding Allowed': coBrandingAllowed ? String(coBrandingAllowed).trim() : '',
            'Branded Residences Allowed': brandedResidencesAllowed ? String(brandedResidencesAllowed).trim() : '',
            'Mixed-Use Allowed': mixedUseAllowed ? String(mixedUseAllowed).trim() : '',
            'Priority Markets': formatMultiSelect(priorityMarkets),
            'Markets to Avoid': formatMultiSelect(marketsToAvoid),
            'Market Expansion Comfort': marketExpansionComfort ? String(marketExpansionComfort).trim() : '',
            'Market Expansion Ramp Lead Time (Months)': marketExpansionRampTimeMonths ? parseInt(marketExpansionRampTimeMonths, 10) : null,
            // Additional Ideal Project / Owner Fit Details
            'Owner Hotel Experience': formatMultiSelect(ownerHotelExperience),
            'Acceptable Project Stages': formatMultiSelect(projectStage),
            'Milestone Min Months - First Discussion to Operator Selection': milestoneOperatorSelectionMinMonths ? parseInt(milestoneOperatorSelectionMinMonths, 10) : null,
            'Milestone Min Months - Operator Selection to Construction Start': milestoneConstructionStartMinMonths ? parseInt(milestoneConstructionStartMinMonths, 10) : null,
            'Milestone Min Months - Pre-Opening Ramp to Soft Opening': milestoneSoftOpeningMinMonths ? parseInt(milestoneSoftOpeningMinMonths, 10) : null,
            'Milestone Min Months - Soft Opening to Grand Opening': milestoneGrandOpeningMinMonths ? parseInt(milestoneGrandOpeningMinMonths, 10) : null,
            'Date Flexibility': dateFlexibility ? String(dateFlexibility).trim() : '',
            'Brand Status Scenarios': formatMultiSelect(brandStatus),
            'PIP / Repositioning Details': pipRepositioningDetails ? String(pipRepositioningDetails).trim() : '',
            'Acceptable Owner Involvement Levels': formatMultiSelect(ownerInvolvementLevel),
            'Owner Non-Negotiable Types': formatMultiSelect(ownerNonNegotiableTypes),
            'Owner Non-Negotiables & Decision Rights': ownerNonNegotiables ? String(ownerNonNegotiables).trim() : '',
            'Acceptable Fee Expectations vs Market': formatMultiSelect(feeExpectationVsMarket),
            'CapEx and FF&E Support': capexSupport ? String(capexSupport).trim() : '',
            'Acceptable Exit Horizon': formatMultiSelect(exitHorizon),
            'Acceptable Capital Status at Engagement': formatMultiSelect(capitalStatus),
            'Known Red Flag Items': knownRedFlags ? String(knownRedFlags).trim() : '',
            'ESG / Sustainability Expectations': esgExpectations ? String(esgExpectations).trim() : '',
            'Ideal Projects Additional Notes': idealProjectsAdditionalNotes ? String(idealProjectsAdditionalNotes).trim() : '',
        };

        if (req.file && req.file.filename) {
            const baseUrl =
                process.env.PUBLIC_URL ||
                (req.protocol && req.get && `${req.protocol}://${req.get('host')}`) ||
                'http://localhost:3000';
            const logoUrl = `${String(baseUrl).replace(/\/$/, '')}/uploads/${req.file.filename}`;
            fields['Company Logo'] = [{ url: logoUrl, filename: req.file.originalname || req.file.filename }];
            if (String(logoUrl).includes('localhost')) {
                console.warn(
                    'Third-party operator intake: logo URL is localhost — Airtable cannot fetch it from the internet. Set PUBLIC_URL for a public base URL.'
                );
            }
        }

        // Add submitted timestamp if provided
        if (submittedAt) {
            fields['Submitted At'] = submittedAt;
        }

        // Airtable rejects create() if any unknown field name is present, even when blank.
        // Only send populated values so optional/uncreated columns do not block submission.
        const compactFields = Object.fromEntries(
            Object.entries(fields).filter(([, value]) => {
                if (value == null) return false;
                if (typeof value === 'string') return value.trim() !== '';
                if (Array.isArray(value)) return value.length > 0;
                return true; // keep numbers/booleans/objects
            })
        );

        const baseId = process.env.AIRTABLE_BASE_ID;
        const apiKey = process.env.AIRTABLE_API_KEY;

        let basicsSchema = null;
        let footprintSchema = null;
        if (baseId && apiKey) {
            try {
                basicsSchema = await fetchAirtableTableFieldNameSet(baseId, apiKey, TABLE_NAME);
            } catch (e) {
                console.warn("third-party-operator-intake: Basics schema fetch skipped:", e.message || e);
            }
            try {
                footprintSchema = await fetchAirtableTableFieldNameSet(baseId, apiKey, FOOTPRINT_TABLE);
            } catch (e) {
                console.warn("third-party-operator-intake: Footprint schema fetch skipped:", e.message || e);
            }
        }

        let fieldsToCreate = compactFields;
        if (basicsSchema && basicsSchema.size > 0) {
            try {
                fieldsToCreate = remapBasicsFieldsForAirtableSchema(compactFields, basicsSchema);
                fieldsToCreate = filterFieldsToAirtableSchema(fieldsToCreate, basicsSchema);
            } catch (remapErr) {
                console.warn("third-party-operator-intake: basics column alias remap skipped:", remapErr.message || remapErr);
            }
        }

        const targetRecordId = String(inputRecordId || "").trim();
        const isUpdate = !!targetRecordId;
        // Create/update the main intake record in Airtable (Footprint-only columns must not be sent here)
        const record = isUpdate
            ? await base(TABLE_NAME).update(targetRecordId, fieldsToCreate, { typecast: true })
            : await base(TABLE_NAME).create(fieldsToCreate, { typecast: true });

        // Also persist normalized child rows (keep legacy JSON writes above for backward compatibility).
        const caseStudiesRows = parseJsonArrayInput(caseStudiesDetail)
            .filter((item) => item && typeof item === "object")
            .map((item) => ({
                "Operator Record ID": record.id,
                "Company Name": fields["Company Name"] || "",
                "Hotel Type": item.hotel_type ? String(item.hotel_type).trim() : "",
                "Region": item.region ? String(item.region).trim() : "",
                "Branded / Independent": item.branded_independent ? String(item.branded_independent).trim() : "",
                "Situation": item.situation ? String(item.situation).trim() : "",
                "Services": item.services ? String(item.services).trim() : "",
                "Outcome": item.outcome ? String(item.outcome).trim() : "",
                "Owner Relevance": item.owner_relevance ? String(item.owner_relevance).trim() : "",
            }))
            .filter((row) =>
                Object.entries(row).some(([key, val]) => key !== "Operator Record ID" && key !== "Company Name" && val)
            );

        const ownerDiligenceRows = parseJsonArrayInput(ownerDiligenceQa)
            .filter((item) => item && typeof item === "object")
            .map((item) => ({
                "Operator Record ID": record.id,
                "Company Name": fields["Company Name"] || "",
                "Category": item.category ? String(item.category).trim() : "",
                "Question": item.question ? String(item.question).trim() : "",
                "Answer": item.answer ? String(item.answer).trim() : "",
            }))
            .filter((row) => row.Answer);

        let childWriteWarning = null;
        try {
            if (isUpdate) {
                await replaceChildRecordsByOperatorId(CASE_STUDIES_TABLE, record.id, caseStudiesRows);
                await replaceChildRecordsByOperatorId(OWNER_DILIGENCE_QA_TABLE, record.id, ownerDiligenceRows);
            } else {
                await createChildRecords(CASE_STUDIES_TABLE, caseStudiesRows);
                await createChildRecords(OWNER_DILIGENCE_QA_TABLE, ownerDiligenceRows);
            }
        } catch (childError) {
            // Don't block intake success while child tables are being rolled out.
            childWriteWarning = childError && childError.message ? childError.message : "Failed to write child tables";
            console.error("Child table write warning:", childError);
        }

        if (baseId && apiKey && footprintSchema && footprintSchema.size > 0) {
            try {
                const fpPayload = buildFootprintRowPayloadFromIntake(
                    compactFields,
                    record.id,
                    footprintSchema,
                    OPERATOR_BASICS_LINK_FIELD
                );
                if (fpPayload) {
                    if (isUpdate) {
                        await upsertFootprintByOperatorId(record.id, fpPayload);
                    } else {
                        await base(FOOTPRINT_TABLE).create(fpPayload, { typecast: true });
                    }
                }
            } catch (fpErr) {
                const msg = fpErr && fpErr.message ? fpErr.message : "Footprint row not created";
                console.error("Third-party operator Footprint write warning:", fpErr);
                childWriteWarning = childWriteWarning ? `${childWriteWarning}; ${msg}` : msg;
            }
        }

        // Return success response with record ID
        return res.status(isUpdate ? 200 : 201).json({
            success: true,
            message: isUpdate ? 'Operator information updated successfully' : 'Operator information submitted successfully',
            recordId: record.id,
            warning: childWriteWarning,
            fields: {
                companyName: fields['Company Name'],
                email: fields['Contact Email']
            }
        });

    } catch (error) {
        console.error('Error submitting third-party operator intake:', error);
        
        // Handle Airtable-specific errors
        if (error.error) {
            const airtableMessage =
                (error.error && error.error.message) ||
                (typeof error.error === 'string' ? error.error : '') ||
                error.message ||
                'Failed to create record';
            return res.status(400).json({
                error: 'Airtable error',
                message: airtableMessage,
                details: error.error
            });
        }

        return res.status(500).json({
            error: 'Internal server error',
            message: error.message || 'Failed to submit operator information'
        });
    }
}
