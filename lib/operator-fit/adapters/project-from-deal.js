/**
 * Adapt existing deal scoring context → Operator Fit project requirements.
 * Consumes existing fields only — no new intake mappings.
 */

import {
  fieldInferred,
  fieldNotApplicable,
  fieldPresent,
  fieldUnknown,
} from "./field-state.js";
import { mapOperatingStructureList } from "../structure-mapping.js";

function toStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && Number.isFinite(v)) return String(v);
  if (Array.isArray(v)) return v.map((x) => toStr(x)).filter(Boolean).join(", ");
  return String(v).trim();
}

function toList(v) {
  if (v == null) return [];
  if (Array.isArray(v)) return v.map((x) => toStr(x)).filter(Boolean);
  const s = toStr(v);
  if (!s) return [];
  return s.split(/\s*,\s*/).map((x) => x.trim()).filter(Boolean);
}

function pickField(obj, keys) {
  for (const k of keys) {
    if (obj && obj[k] != null && toStr(obj[k])) return obj[k];
  }
  return null;
}

function wrapList(raw, source) {
  const list = toList(raw);
  if (list.length) return fieldPresent(list, { source });
  return fieldUnknown({ source });
}

function wrapScalar(raw, source) {
  const s = toStr(raw);
  if (s) return fieldPresent(s, { source });
  return fieldUnknown({ source });
}

/**
 * @param {{ dealFields?: object, locationData?: object, mpData?: object, siData?: object, dealId?: string }} ctx
 */
export function adaptProjectFromDealContext(ctx = {}) {
  const deal = ctx.dealFields || {};
  const loc = ctx.locationData || {};
  const mp = ctx.mpData || {};
  const si = ctx.siData || {};

  const country = pickField(loc, ["Country", "country"]) || pickField(deal, ["Country"]);
  const city = pickField(loc, ["City", "city"]);
  const scale = pickField(loc, ["Hotel Chain Scale", "chainScale"]);
  const building = pickField(loc, ["Building Type"]);
  const stage = pickField(loc, ["Stage of Development"]);
  const rooms = pickField(loc, ["Total Number of Rooms/Keys", "Rooms", "keys"]);
  const projectType = pickField(deal, ["Project Type"]) || pickField(loc, ["Project Type"]);
  const fb = pickField(deal, ["F&B Complexity"]);
  const opening = pickField(deal, ["Opening Timeline", "Opening / Transition Phase"]);

  const mgmt = toList(
    si["Preferred Management Structure"] || si.preferredManagementStructure || []
  );
  const operatingModel = toStr(si["Operating Model"] || si.operatingModel);
  const brandAgreement = toStr(si["Brand Agreement Structure"] || si.brandAgreementStructure);
  const legacyStructure = toStr(mp["Preferred Deal Structure"]);

  const structureRaw = [...mgmt];
  if (operatingModel) structureRaw.push(operatingModel);
  if (brandAgreement && /franchise/i.test(brandAgreement) && !structureRaw.length) {
    structureRaw.push("Franchise Only");
  }
  if (!structureRaw.length && legacyStructure) structureRaw.push(legacyStructure);

  const preferredBrands = toList(si["Preferred Brands"] || si.preferredBrands || deal["Preferred Brands"]);
  const mustHaveServices = toList(
    si["Must-Have Operator Services"] || si["Required Operator Services"] || []
  );
  const dealBreakers = toList(si["Top 3 Deal Breakers"] || []);
  const marketPresenceReq = toStr(si["Market Presence Requirement"]);
  const ownerControl = toStr(si["Owner Control Preference"] || si["Owner Control Priorities"]);
  const reporting = toStr(
    si["Owner Reporting Expectations"] || si["Owner Reporting Package"] || si["Owner Reporting Frequency"]
  );
  const preOpening = toStr(si["Pre-Opening Support Needed"]);
  const commercialPriority = toStr(si["Commercial Priority"]);

  const excludedBrandManaged =
    structureRaw.length > 0 &&
    mapOperatingStructureList(structureRaw).every(
      (k) => k === "owner_operated" || k === "franchise_only"
    ) &&
    !mapOperatingStructureList(structureRaw).includes("brand_managed") &&
    !/brand-managed|brand managed/i.test(operatingModel);

  const mixedUse =
    /mixed/i.test(String(projectType || "")) || /mixed/i.test(String(building || ""));
  const residences = /residence/i.test(String(building || "")) || /residence/i.test(String(projectType || ""));
  const meetingsHint =
    /convention|group|meetings/i.test(String(commercialPriority || "")) ||
    /convention|group/i.test(String(building || ""));

  return {
    projectId: ctx.dealId || toStr(deal.id) || null,
    identity: fieldPresent(
      {
        name: toStr(deal["Deal Name"] || deal.Name || deal.name) || null,
        dealId: ctx.dealId || null,
      },
      { source: "deals" }
    ),
    geography: {
      country: country ? fieldPresent(toStr(country), { source: "location" }) : fieldUnknown({ source: "location" }),
      city: city ? fieldPresent(toStr(city), { source: "location" }) : fieldUnknown({ source: "location" }),
      marketPresenceRequirement: marketPresenceReq
        ? fieldPresent(marketPresenceReq, { source: "si" })
        : fieldUnknown({ source: "si" }),
    },
    hotelSegment: scale ? fieldPresent(toStr(scale), { source: "location" }) : fieldUnknown({ source: "location" }),
    assetType: building ? fieldPresent(toStr(building), { source: "location" }) : fieldUnknown({ source: "location" }),
    developmentType: projectType
      ? fieldPresent(toStr(projectType), { source: "deals" })
      : fieldUnknown({ source: "deals" }),
    stage: stage ? fieldPresent(toStr(stage), { source: "location" }) : fieldUnknown({ source: "location" }),
    keyCount: rooms != null && rooms !== "" ? fieldPresent(Number(rooms) || rooms, { source: "location" }) : fieldUnknown({ source: "location" }),
    resortOrUrban: fieldUnknown({ source: "inferred_optional", note: "Not a dedicated intake field" }),
    fbComplexity: fb ? fieldPresent(toStr(fb), { source: "deals" }) : fieldUnknown({ source: "deals" }),
    meetingGroupComplexity: meetingsHint
      ? fieldInferred(true, { source: "keyword_hint" })
      : fieldUnknown({ source: "deals" }),
    mixedUse: mixedUse ? fieldPresent(true, { source: "project_type" }) : fieldPresent(false, { source: "project_type" }),
    brandedResidences: residences
      ? fieldPresent(true, { source: "project_type" })
      : fieldPresent(false, { source: "project_type" }),
    preOpeningNeeds: preOpening
      ? fieldPresent(preOpening, { source: "si" })
      : opening
        ? fieldPresent(opening, { source: "deals" })
        : fieldUnknown({ source: "si" }),
    operatingStructurePreferences: structureRaw.length
      ? fieldPresent(structureRaw, {
          source: mgmt.length ? "si" : legacyStructure ? "legacy_mp" : "si",
          canonicalKeys: mapOperatingStructureList(structureRaw),
          brandAgreement: brandAgreement || null,
          operatingModel: operatingModel || null,
        })
      : fieldUnknown({ source: "si" }),
    strategicPriorities: commercialPriority
      ? fieldPresent([commercialPriority], { source: "si" })
      : fieldUnknown({ source: "si" }),
    selectedOrEvaluatedBrands: preferredBrands.length
      ? fieldPresent(preferredBrands, { source: "si" })
      : fieldUnknown({ source: "si" }),
    hardRequirements: {
      mustHaveServices: mustHaveServices.length
        ? fieldPresent(mustHaveServices, { source: "si" })
        : fieldUnknown({ source: "si" }),
      ownerControl: ownerControl ? fieldPresent(ownerControl, { source: "si" }) : fieldUnknown({ source: "si" }),
      reporting: reporting ? fieldPresent(reporting, { source: "si" }) : fieldUnknown({ source: "si" }),
    },
    knownExclusions: {
      dealBreakers: dealBreakers.length
        ? fieldPresent(dealBreakers, { source: "si" })
        : fieldUnknown({ source: "si" }),
      excludesBrandManaged: excludedBrandManaged
        ? fieldPresent(true, { source: "structure_inference" })
        : fieldPresent(false, { source: "structure_inference" }),
    },
    economicsHardRequirements: fieldNotApplicable({
      note: "No structured hard fee requirement field consumed in Phase 1–2",
    }),
    _rawSources: { dealKeys: Object.keys(deal), locKeys: Object.keys(loc), siKeys: Object.keys(si) },
  };
}
