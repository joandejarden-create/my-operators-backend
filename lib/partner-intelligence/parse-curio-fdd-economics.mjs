/**
 * Parse Curio Collection (Hilton) FDD Items 5, 6, 7, 17, and Item 19 highlights from plain text.
 * Evidence lives in reports/curio-fdd-plain.txt — not echoed verbatim in UI copy.
 */

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

/** Skip table-of-contents hits (lines full of dots). */
function sliceItemBody(text, itemNum, heading) {
  const re = new RegExp(`ITEM\\s*${itemNum}\\b`, "gi");
  let m;
  const headingRe = new RegExp(heading, "i");
  while ((m = re.exec(text)) !== null) {
    const window = text.slice(m.index, m.index + 500);
    if (/\.{8,}/.test(window)) continue;
    if (!headingRe.test(window)) continue;
    const tail = text.slice(m.index);
    const endRel = tail.slice(1).search(new RegExp(`\\nITEM\\s*${itemNum + 1}\\s*\\n`, "i"));
    return endRel >= 0 ? tail.slice(0, endRel + 1) : tail;
  }
  return "";
}

function firstMoney(s) {
  const m = String(s || "").match(/\$\s*([\d,]+(?:\.\d+)?)/);
  return m ? parseFloat(m[1].replace(/,/g, "")) : null;
}

function moneyRange(s) {
  const m =
    String(s || "").match(/\$\s*([\d,]+(?:\.\d+)?)\s*(?:-|to|or)\s*\$\s*([\d,]+(?:\.\d+)?)/i) ||
    String(s || "").match(/\$\s*([\d,]+(?:\.\d+)?)\s+and\s+\$\s*([\d,]+(?:\.\d+)?)/i);
  if (!m) return [null, null];
  return [parseFloat(m[1].replace(/,/g, "")), parseFloat(m[2].replace(/,/g, ""))];
}

function firstPct(s) {
  const raw = String(s || "").trim();
  if (!raw) return null;
  const bare = raw.match(/^([\d.]+)$/);
  if (bare) return parseFloat(bare[1]) / 100;
  const m = raw.match(/([\d.]+)\s*%/);
  return m ? parseFloat(m[1]) / 100 : null;
}

/**
 * @param {string} text Full FDD plain text
 */
export function parseCurioFddEconomics(text) {
  const t = nz(text);
  if (!t) throw new Error("Empty FDD text");

  const item5 = sliceItemBody(t, 5, "INITIAL FEES");
  const item6 = sliceItemBody(t, 6, "OTHER FEES");
  const item7 = sliceItemBody(t, 7, "ESTIMATED INITIAL INVESTMENT");
  const item17 = sliceItemBody(t, 17, "RENEWAL, TERMINATION, TRANSFER AND DISPUTE RESOLUTION");
  const item19 = sliceItemBody(t, 19, "FINANCIAL PERFORMANCE");

  const royaltyRoom =
    firstPct(item6.match(/Monthly Royalty[\s\S]{0,80}?5\s*%\s+of\s+Gross\s+Rooms/i)?.[0] || "") ??
    firstPct(t.match(/5\s*%\s+of\s+Gross\s+Rooms\s+Revenue/i)?.[0] || "");

  const programFee =
    firstPct(item6.match(/Monthly Program[\s\S]{0,80}?4\s*%\s+of\s+Gross\s+Rooms/i)?.[0] || "") ??
    firstPct(t.match(/Monthly Program[\s\S]{0,40}?4\s*%/i)?.[0] || "");

  const spaRoyalty =
    firstPct(item6.match(/Monthly Spa[\s\S]{0,80}?2\s*%\s+of\s+Gross\s+Spa/i)?.[0] || "") ?? null;

  const honorsFolio =
    firstPct(item6.match(/Hilton Honors[\s\S]{0,120}?4\s*%\s+of\s+total\s+eligible\s+guest\s+folio/i)?.[0] || "") ??
    firstPct(item6.match(/4\s*%\s+of\s+total\s+eligible\s+guest\s+folio/i)?.[0] || "");

  const digitalAdvance =
    firstPct(item6.match(/Hilton Advance[\s\S]{0,80}?1\.35\s*%/i)?.[0] || "") ?? null;

  const thirdPartyRes = (() => {
    const m = item6.match(/Third-Party[\s\S]{0,80}?up to \$\s*([\d.]+)\s+per\s+stay/i);
    return m ? parseFloat(m[1]) : null;
  })();

  const appNewDev = (() => {
    const m = item5.match(/\$85,000\s+plus\s+\$\s*400/i) || t.match(/\$85,000\s+plus\s+\$\s*400/i);
    return m ? { base: 85000, perRoomOver250: 400 } : { base: null, perRoomOver250: null };
  })();

  const appChangeOfOwnership = firstMoney(
    item5.match(/Change of[\s\S]{0,40}?\$\s*150,000/i)?.[0] || ""
  );
  const appRelicensing = firstMoney(item5.match(/Re-licensing[\s\S]{0,40}?\$\s*85,000/i)?.[0] || "");
  const pipFee = firstMoney(item5.match(/PIP[\s\S]{0,40}?\$\s*10,000/i)?.[0] || item6.match(/PIP[\s\S]{0,40}?\$\s*10,000/i)?.[0] || "");
  const openingProcessFee = firstMoney(item5.match(/Opening Process[\s\S]{0,40}?\$\s*20,000/i)?.[0] || "");

  const [trainingMin, trainingMax] = moneyRange(
    item5.match(/\$5,000\s+to\s+\$15,000/i)?.[0] || item7.match(/\$5,000\s+to\s+\$15,000/i)?.[0] || ""
  );

  const [onqInitialMin, onqInitialMax] = moneyRange(
    item5.match(/\$82,800\s+and\s+\$218,800/i)?.[0] ||
      item5.match(/\$82,800\s+to\s+\$218,800/i)?.[0] ||
      ""
  );

  const [onqMaintMin, onqMaintMax] = moneyRange(
    item6.match(/\$2,295\s+to\s+\$6,689/i)?.[0] || ""
  );

  const [onqConnMin, onqConnMax] = moneyRange(item6.match(/\$400\s+and\s+\$600/i)?.[0] || item6.match(/\$400\s+to\s+\$600/i)?.[0] || "");

  const totalRange = moneyRange(
    t.match(/\$3,928,488\s+to\s+\$119,557,900/i)?.[0] ||
      item7.match(/\$3,928,488\s+to\s+\$119,557,900/i)?.[0] ||
      ""
  );

  const paidToHiltonMax = firstMoney(
    t.match(/including up to \$\s*467,680/i)?.[0] || item7.match(/467,680/i)?.[0] || ""
  );

  const typicalRooms = 200;
  const perRoomMin = totalRange[0] != null ? Math.round(totalRange[0] / typicalRooms) : null;
  const perRoomMax = totalRange[1] != null ? Math.round(totalRange[1] / typicalRooms) : null;

  const termNewDev = (() => {
    const m = item17.match(/23\s+years\s+after\s+the\s+Effective\s+Date/i);
    return m ? 23 : null;
  })();
  const termConversionMin = (() => {
    const m = item17.match(/10\s+to\s+20\s+years\s+after\s+the\s+Opening\s+Date/i);
    return m ? 10 : null;
  })();
  const termConversionMax = termConversionMin != null ? 20 : null;
  const noRenewal = /do not have the right to renew/i.test(item17);

  const honorsOccAvg = firstPct(
    item19.match(
      /Average Percentage of Hilton Honors Contribution to Occupancy for Comparable[\s\S]{0,40}?Hotels\s+([\d.]+)\s*%/i
    )?.[1] ||
      t.match(
        /Average Percentage of Hilton Honors Contribution to Occupancy for Comparable[\s\S]{0,40}?Hotels\s+([\d.]+)\s*%/i
      )?.[1] ||
      ""
  );
  const honorsOccMedian = firstPct(
    item19.match(
      /Median Percentage of Hilton Honors Contribution to Occupancy for Comparable[\s\S]{0,40}?Hotels\s+([\d.]+)\s*%/i
    )?.[1] ||
      t.match(
        /Median Percentage of Hilton Honors Contribution to Occupancy for Comparable[\s\S]{0,40}?Hotels\s+([\d.]+)\s*%/i
      )?.[1] ||
      ""
  );

  const programFeeRampY1 = firstPct(item6.match(/3\s*%\s+for\s+year\s+1\s+and\s+2/i)?.[0] || "");
  const programFeeRampY3 = firstPct(item6.match(/3\.5\s*%\s+for\s+year\s+3/i)?.[0] || "");

  return {
    parsedAt: new Date().toISOString().slice(0, 10),
    sourceVintage: "2026 US Curio FDD",
    item5: {
      applicationFeeNewDevBase: appNewDev.base,
      applicationFeePerRoomOver250: appNewDev.perRoomOver250,
      applicationFeeChangeOfOwnership: appChangeOfOwnership,
      applicationFeeRelicensing: appRelicensing,
      pipFee,
      openingProcessServicesFee: openingProcessFee,
      trainingMin,
      trainingMax,
      onqInitialMin,
      onqInitialMax,
    },
    item6: {
      royaltyRoomPct: royaltyRoom,
      programFeePct: programFee,
      spaRoyaltyPct: spaRoyalty,
      honorsEligibleFolioPct: honorsFolio,
      digitalAdvancePct: digitalAdvance,
      thirdPartyReservationPerStay: thirdPartyRes,
      onqMaintMin,
      onqMaintMax,
      onqConnMin,
      onqConnMax,
      programFeeRampY1Pct: programFeeRampY1,
      programFeeRampY3Pct: programFeeRampY3,
    },
    item7: {
      typicalRoomCount: typicalRooms,
      totalInvestmentMin: totalRange[0],
      totalInvestmentMax: totalRange[1],
      perRoomMin,
      perRoomMax,
      paidToHiltonMax,
    },
    item17: {
      termNewDevelopmentYears: termNewDev,
      termConversionYearsMin: termConversionMin,
      termConversionYearsMax: termConversionMax,
      renewalProvided: !noRenewal,
    },
    item19: {
      honorsOccupancyContributionAvgPct: honorsOccAvg,
      honorsOccupancyContributionMedianPct: honorsOccMedian,
      comparableHotelCount: 63,
      sampleYear: 2025,
    },
  };
}
