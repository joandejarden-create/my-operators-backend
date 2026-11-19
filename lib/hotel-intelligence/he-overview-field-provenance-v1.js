/**
 * Attach explicit provenance for Hotel Explorer Overview fields that may
 * come from HPC aliases, ADP/HI derived profiles, or remain empty.
 * Read-only. No Airtable writes.
 */

function blank(v) {
  const s = String(v == null ? "" : v).trim();
  return !s || s.toLowerCase() === "unknown";
}

function firstPresent(fields, names) {
  for (const name of names) {
    const v = fields?.[name];
    if (v == null) continue;
    if (typeof v === "string" && !String(v).trim()) continue;
    if (Array.isArray(v) && !v.length) continue;
    return { field: name, value: v };
  }
  return null;
}

/**
 * Build fieldProvenance for the five Gate 1 audit fields + sync parentCompany.
 * @param {object} hotel - brand-presence DTO (mutated)
 * @param {object} opts
 * @param {object} [opts.hpcFields] - raw HPC Airtable fields
 * @param {string} [opts.owningTableKind]
 */
export function attachOverviewFieldProvenance(hotel, opts = {}) {
  if (!hotel || typeof hotel !== "object") return hotel;
  const fields = opts.hpcFields || {};
  const kind = opts.owningTableKind || hotel?.propertyIdentity?.owningTableKind || null;
  const fp = { ...(hotel.fieldProvenance || {}) };

  if (kind === "HPC") {
    const parentHit = firstPresent(fields, ["Parent Company"]);
    const brandFamilyHit = firstPresent(fields, ["Brand Family", "Family / Source Family"]);
    if (parentHit) {
      hotel.parentCompany = String(parentHit.value).trim();
      hotel.brandFamily = brandFamilyHit
        ? String(brandFamilyHit.value).trim()
        : hotel.brandFamily || null;
      fp.parentCompany = {
        class: "VERIFIED_HPC",
        hpcField: parentHit.field,
        semantic: "parent_company_field",
        note: "HPC Parent Company field. Not an economic-owner assertion.",
      };
    } else if (brandFamilyHit) {
      hotel.parentCompany = String(brandFamilyHit.value).trim();
      hotel.brandFamily = String(brandFamilyHit.value).trim();
      fp.parentCompany = {
        class: "VERIFIED_HPC",
        hpcField: brandFamilyHit.field,
        semantic: "brand_parent_company",
        note: "Brand parent / Brand Family — NOT economic or real-estate ownership.",
      };
    } else {
      hotel.parentCompany = null;
      fp.parentCompany = {
        class: "EMPTY",
        note: "No HPC Parent Company or Brand Family; ADP brand-parent overlay suppressed for HPC.",
      };
    }

    const scaleHit = firstPresent(fields, ["Chain Scale", "Hotel Class / Segment"]);
    if (scaleHit && !blank(hotel.chainScale)) {
      fp.chainScale = {
        class: "VERIFIED_HPC",
        hpcField: scaleHit.field,
        taxonomy: "Dealality HPC Hotel Class / Segment (class band)",
        value: hotel.chainScale,
        note: "Not an STR Chain Scale import; uses HPC classification field.",
      };
    } else if (!blank(hotel.chainScale)) {
      fp.chainScale = {
        class: "UNVERIFIED_INFERENCE",
        value: hotel.chainScale,
        note: "Displayed chainScale has no matching HPC Chain Scale / Hotel Class field.",
      };
    } else {
      fp.chainScale = { class: "EMPTY" };
    }

    const opHit = firstPresent(fields, ["Operation Type", "Management Model"]);
    const operatorType = firstPresent(fields, ["Operator Type"]);
    if (opHit && !blank(hotel.operationType)) {
      fp.operationType = {
        class: "VERIFIED_HPC",
        hpcField: opHit.field,
        value: hotel.operationType,
        corroboration: operatorType
          ? { hpcField: operatorType.field, value: operatorType.value }
          : null,
        note: "Operating structure from HPC Management Model / Operation Type — not inferred from brand name alone.",
      };
    } else if (!blank(hotel.operationType)) {
      fp.operationType = {
        class: "UNVERIFIED_INFERENCE",
        value: hotel.operationType,
        note: "Displayed operationType has no matching HPC Operation Type / Management Model.",
      };
    } else {
      fp.operationType = { class: "EMPTY" };
    }

    const locHit = firstPresent(fields, ["Location Type", "Location", "Asset Context"]);
    if (locHit && !blank(hotel.locationType)) {
      fp.locationType = {
        class: "VERIFIED_HPC",
        hpcField: locHit.field,
        taxonomy:
          locHit.field === "Asset Context"
            ? "HPC Asset Context (Suburban/Urban/… geographic asset class)"
            : "HPC Location / Location Type",
        value: hotel.locationType,
        note: "Not a heuristic; mapped from HPC geographic classification field.",
      };
    } else if (!blank(hotel.locationType)) {
      fp.locationType = {
        class: "UNVERIFIED_INFERENCE",
        value: hotel.locationType,
        note: "Displayed locationType has no matching HPC Location / Asset Context field.",
      };
    } else {
      fp.locationType = { class: "EMPTY" };
    }

    const meetingHpc = firstPresent(fields, [
      "Total Meeting Space",
      "Total Meeting Space Sq Ft",
      "Meeting Space Sq Ft",
    ]);
    if (meetingHpc && hotel.totalMeetingSpace != null) {
      fp.totalMeetingSpace = {
        class: "VERIFIED_HPC",
        hpcField: meetingHpc.field,
        unit: "sq_ft",
        metric: "total_meeting_space",
        value: hotel.totalMeetingSpace,
      };
    } else if (hotel.meetingSpaceProvenance?.factKind === "VERIFIED_FACT") {
      fp.totalMeetingSpace = {
        class: "VERIFIED_DERIVED",
        source: hotel.meetingSpaceProvenance.provenance || "hotel_intelligence_meeting_space",
        evidenceUrl: hotel.meetingSpaceProvenance.sourceUrl || null,
        unit: "sq_ft",
        metric: "total_meeting_space",
        value: hotel.totalMeetingSpace,
        displayValue: hotel.meetingSpaceDisplay || null,
        hpcMeetingSpaceFlag: fields["Meeting Space Flag"] === true,
        note: "Total meeting space from ADP/HI commercial profile with official events URL; not yet persisted on HPC Total Meeting Space fields.",
      };
    } else if (hotel.totalMeetingSpace != null) {
      fp.totalMeetingSpace = {
        class: "UNVERIFIED_INFERENCE",
        value: hotel.totalMeetingSpace,
        note: "Meeting space shown without HPC field or verified enrichment provenance.",
      };
    } else {
      fp.totalMeetingSpace = {
        class: "EMPTY",
        hpcMeetingSpaceFlag: fields["Meeting Space Flag"] === true,
      };
    }
  }

  hotel.fieldProvenance = fp;
  return hotel;
}

export default { attachOverviewFieldProvenance };
