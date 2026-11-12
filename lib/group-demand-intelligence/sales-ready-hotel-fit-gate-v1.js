/**
 * Sales Ready hotel-fit / destination gate V1
 *
 * Hotel-agnostic. Blocks Sales Ready when the target hotel’s commercial path is
 * destination-unconfirmed, out-of-market, competitor-host-only, or only a
 * conditional / stretch hypothesis (e.g. “only if Beltway inventory accepted”).
 *
 * Does NOT require RFP, confirmed room nights, or a second named contact.
 * Credible named commercial intermediaries remain eligible when hotel fit is
 * defensible and destination/housing path is not unresolved.
 */

export const SALES_READY_HOTEL_FIT_GATE_V1 = "gdi-sales-ready-hotel-fit-gate-v1";

export const HOTEL_FIT_GATE_REASON = Object.freeze({
  OK: "hotel_fit_defensible_for_sales_ready",
  DESTINATION_OUTSIDE: "destination_outside_or_out_of_market",
  HOST_UNCONFIRMED: "destination_or_host_hotel_unconfirmed",
  CONDITIONAL_STRETCH: "conditional_or_stretch_hotel_fit_unconfirmed",
  COMPETITOR_HOST_ONLY: "competitor_host_without_open_target_path",
  THIN_FIT: "hotel_fit_rationale_thin",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function fitBlob(opportunity = {}) {
  return text(
    [
      opportunity.hotelFitHonesty,
      opportunity.hotelFitNote,
      opportunity.hotelFitRationale,
      opportunity.whyThisHotel,
      opportunity.summaryWhyHotel,
      opportunity.hotelOpportunityThesis,
      opportunity.eventLocationSummary,
      opportunity.eventLocationStatus,
      opportunity.title,
      opportunity.opportunityName,
      opportunity.recommendedAction,
      opportunity.recommendedSalesAction,
    ].join(" "),
  );
}

function fitRationaleLength(opportunity = {}) {
  return text(
    opportunity.hotelFitNote ||
      opportunity.hotelFitRationale ||
      opportunity.whyThisHotel ||
      opportunity.summaryWhyHotel ||
      opportunity.hotelOpportunityThesis ||
      "",
  ).length;
}

/**
 * @returns {{ ok: boolean, reason: string, version: string, detail?: object }}
 */
export function assessSalesReadyHotelFitGate(opportunity = {}, opts = {}) {
  const honesty = upper(opportunity.hotelFitHonesty || opportunity.hotelFitQuality);
  const blob = fitBlob(opportunity);
  const rationaleLen = fitRationaleLength(opportunity);

  if (/OUT_OF_MARKET|DESTINATION_OUTSIDE/.test(honesty)) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: HOTEL_FIT_GATE_REASON.DESTINATION_OUTSIDE,
      detail: { hotelFitHonesty: opportunity.hotelFitHonesty || null },
    };
  }

  if (/DESTINATION_UNCONFIRMED/.test(honesty)) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: HOTEL_FIT_GATE_REASON.HOST_UNCONFIRMED,
      detail: { hotelFitHonesty: opportunity.hotelFitHonesty || null },
    };
  }

  if (
    /hotel not yet (named|published)|hotel not (named|specified)|hotel TBA|hotel info(?:rmation)? forthcoming/i.test(
      blob,
    )
  ) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: HOTEL_FIT_GATE_REASON.HOST_UNCONFIRMED,
      detail: { matched: "host_hotel_language" },
    };
  }

  // Conditional stretch: Loudoun / Beltway pattern — housing only if partner accepts
  // inventory outside the natural event geography.
  if (
    /only if .{0,100}accept|otherwise watch|beltway (inventory|acceptance)|not a natural .{0,40}(event|market)|exploratory\s*\/\s*stretch/i.test(
      blob,
    )
  ) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: HOTEL_FIT_GATE_REASON.CONDITIONAL_STRETCH,
      detail: { matched: "conditional_stretch_language" },
    };
  }

  if (
    /OVERFLOW_WATCH_COMPETITOR_HOST|COMPETITOR_HOST/.test(honesty) &&
    !/stay[- ]to[- ]play|housing (list|bureau|partner)|overflow (open|evidenced|available)/i.test(
      blob,
    )
  ) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: HOTEL_FIT_GATE_REASON.COMPETITOR_HOST_ONLY,
      detail: { hotelFitHonesty: opportunity.hotelFitHonesty || null },
    };
  }

  const minLen = Number.isFinite(opts.minRationaleLength)
    ? opts.minRationaleLength
    : 24;
  if (rationaleLen < minLen && !/VERIFIED_CITY|VERIFIED_VENUE|IN_MARKET/i.test(honesty)) {
    // Allow short rationale when location is verified in-market and honesty is clean.
    const locOk = /bethesda|montgomery|rockville|nih|maryland soccerplex|boyds/i.test(
      text(
        [
          opportunity.eventLocationSummary,
          opportunity.city,
          opportunity.eventCity,
          opportunity.destination,
        ].join(" "),
      ),
    );
    if (!locOk) {
      return {
        version: SALES_READY_HOTEL_FIT_GATE_V1,
        ok: false,
        reason: HOTEL_FIT_GATE_REASON.THIN_FIT,
        detail: { rationaleLength: rationaleLen, minLen },
      };
    }
  }

  return {
    version: SALES_READY_HOTEL_FIT_GATE_V1,
    ok: true,
    reason: HOTEL_FIT_GATE_REASON.OK,
    detail: { rationaleLength: rationaleLen },
  };
}

/**
 * Softer gate for Account Development — exclude partnership/overflow watch and
 * unconfirmed hosts that DOS treats as MONITOR / QUALIFY_FIRST.
 */
export function assessAccountDevelopmentHotelFitGate(opportunity = {}) {
  const honesty = upper(opportunity.hotelFitHonesty || "");
  const maturity = upper(
    opportunity.commercialMaturity || opportunity.historicalRecoveryDisposition || "",
  );
  const blob = fitBlob(opportunity);

  if (/OUT_OF_MARKET|DESTINATION_OUTSIDE/.test(honesty)) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: HOTEL_FIT_GATE_REASON.DESTINATION_OUTSIDE,
    };
  }

  if (maturity === "PARTNERSHIP_WATCH" || /PARTNERSHIP_WATCH/.test(maturity)) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: "partnership_watch_not_account_development",
    };
  }

  // Explicit research-continuation product maturity is not Account Development.
  if (maturity === "RESEARCH_CONTINUATION" || /RESEARCH_CONTINUATION/.test(maturity)) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: "research_continuation_not_account_development",
    };
  }

  if (
    maturity === "OVERFLOW_WATCH" ||
    /OVERFLOW_WATCH/.test(maturity) ||
    /OVERFLOW_WATCH_COMPETITOR_HOST/.test(honesty)
  ) {
    if (
      !/stay[- ]to[- ]play|housing (list|bureau|partner)|overflow (open|evidenced|available)|external room blocks?/i.test(
        blob,
      )
    ) {
      return {
        version: SALES_READY_HOTEL_FIT_GATE_V1,
        ok: false,
        reason: "overflow_watch_without_open_housing_path",
      };
    }
  }

  if (/DESTINATION_UNCONFIRMED/.test(honesty)) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: HOTEL_FIT_GATE_REASON.HOST_UNCONFIRMED,
    };
  }

  // Hotel still unnamed for a dated association event → qualify first, not AD.
  // "Hotel information forthcoming" alone may still be AD (housing partner path).
  if (/hotel not yet (named|published)|hotel not (named|specified)|hotel TBA\b/i.test(blob)) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: HOTEL_FIT_GATE_REASON.HOST_UNCONFIRMED,
    };
  }

  if (
    /only if .{0,100}accept|otherwise watch/i.test(blob) &&
    !/bethesda premier|woman'?s club|potomac memorial/i.test(blob)
  ) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: HOTEL_FIT_GATE_REASON.CONDITIONAL_STRETCH,
    };
  }

  // Monitor / research-continuation recommended actions should not promote to AD.
  if (
    /do not aggressively sell|do not pursue|monitor venue\/housing|retain overflow watch|continue partnership watch|watch — qualify further|research continuation/i.test(
      text(opportunity.recommendedAction || opportunity.recommendedSalesAction || ""),
    )
  ) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: "recommended_action_monitor_not_account_development",
    };
  }

  // Explicit watch-lane IDs / unannounced-date watch rows stay MONITOR.
  const title = text(opportunity.title || opportunity.opportunityName);
  const id = text(opportunity.id || opportunity.opportunityId);
  const priority = upper(opportunity.priority || opportunity.priorityFilterKey);
  if (
    (priority === "WATCHLIST" || priority === "WATCH") &&
    !/ACCOUNT_DEVELOPMENT|SALES_READY/.test(maturity) &&
    (/_watch\b/i.test(id) ||
      /\bwatch\b/i.test(title) ||
      /dates? not yet announced/i.test(title))
  ) {
    return {
      version: SALES_READY_HOTEL_FIT_GATE_V1,
      ok: false,
      reason: "watchlist_watch_title_not_account_development",
    };
  }

  return {
    version: SALES_READY_HOTEL_FIT_GATE_V1,
    ok: true,
    reason: HOTEL_FIT_GATE_REASON.OK,
  };
}
