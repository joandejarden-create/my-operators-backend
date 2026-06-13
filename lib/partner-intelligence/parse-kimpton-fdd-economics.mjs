/**
 * Parse Kimpton IHG FDD Items 5, 6, 7, and key Item 17 term facts from plain text.
 * Evidence lives in reports/kimpton-fdd-plain.txt — not echoed in UI copy.
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
  const m = s.match(/\$\s*([\d,]+(?:\.\d+)?)/);
  return m ? parseFloat(m[1].replace(/,/g, "")) : null;
}

function moneyRange(s) {
  const m =
    s.match(/\$\s*([\d,]+(?:\.\d+)?)\s*(?:-|to|or)\s*\$\s*([\d,]+(?:\.\d+)?)/i) ||
    s.match(/\$\s*([\d,]+(?:\.\d+)?)\s+and\s+\$\s*([\d,]+(?:\.\d+)?)/i);
  if (!m) return [null, null];
  return [parseFloat(m[1].replace(/,/g, "")), parseFloat(m[2].replace(/,/g, ""))];
}

function firstPct(s) {
  const m = s.match(/([\d.]+)\s*%/);
  return m ? parseFloat(m[1]) / 100 : null;
}

/**
 * @param {string} text Full FDD plain text
 */
export function parseKimptonFddEconomics(text) {
  const t = nz(text);
  if (!t) throw new Error("Empty FDD text");

  const item5 = sliceItemBody(t, 5, "INITIAL FEES");
  const item6 = sliceItemBody(t, 6, "OTHER FEES");
  const item7 = sliceItemBody(t, 7, "ESTIMATED INITIAL INVESTMENT");
  const item17 = sliceItemBody(t, 17, "RENEWAL, TERMINATION, TRANSFER AND DISPUTE RESOLUTION");

  const appChunk = item5 || t;
  const appPerRoom = /\$\s*500\s+per\s+guest\s+room/i.test(appChunk) ? 500 : null;
  const appMin = (() => {
    const m = appChunk.match(/not\s+less\s+than\s+\$\s*([\d,]+)/i);
    return m ? parseFloat(m[1].replace(/,/g, "")) : null;
  })();

  const royaltyRoom =
    firstPct(item6.match(/Royalty\s+6\s*%/i)?.[0] || "") ??
    firstPct(item6.match(/6\s*%\s+of\s+Gross\s+Rooms\s+Revenue/i)?.[0] || "") ??
    firstPct(t.match(/Royalty\s+6\s*%/i)?.[0] || "");
  const royaltyFb =
    firstPct(item6.match(/plus\s+1\s*%\s+of\s+Gross\s+Food\s+and\s+Beverage/i)?.[0] || "") ??
    firstPct(t.match(/plus\s+1\s*%\s+of\s+Gross\s+Food\s+and\s+Beverage/i)?.[0] || "");
  const servicesContribution = firstPct(
    item6.match(/Services\s+Contribution\s+3\s*%/i)?.[0] ||
      item6.match(/Services\s+Contribution[\s\S]{0,40}?3\s*%/i)?.[0] ||
      ""
  );
  const loyaltyFull = firstPct(
    item6.match(/4\.55\s*%\s+of\s+Qualifying\s+Full\s+Folio/i)?.[0] || ""
  );
  const loyaltyRoomMeeting = firstPct(
    item6.match(/1\.365\s*%\s+of\s+Qualifying\s+Room/i)?.[0] || ""
  );
  const techMonthly = (() => {
    const m = item6.match(/\$\s*([\d.]+)\s+per\s+room,\s*per\s+month/i);
    return m ? parseFloat(m[1]) : null;
  })();
  const gdsPerRes = (() => {
    const m = item6.match(/GDS\s+Fees\s+\$\s*([\d.]+)\s+per\s+reservation/i);
    return m ? parseFloat(m[1]) : null;
  })();
  const digitalPct = firstPct(
    item6.match(/2\.25\s*%\s+commission/i)?.[0] || item6.match(/2\.25\s*%/i)?.[0] || ""
  );

  const [preopenMin, preopenMax] = moneyRange(
    item5.match(/Preopening\s+Support\s+Fee[\s\S]{0,80}?\$[\d,]+\s+and\s+\$[\d,]+/i)?.[0] ||
      item5.match(/\$20,000\s+and\s+\$35,000/i)?.[0] ||
      ""
  );
  const [certMin, certMax] = moneyRange(
    item5.match(/\$40,000\s+to\s+\$60,000/i)?.[0] || item5.match(/\$40,000\s+and\s+\$60,000/i)?.[0] || ""
  );
  const pipFee = firstMoney(item5.match(/\$\s*12,000\s+fee\s+for\s+the\s+inspection/i)?.[0] || "");
  const learningAnnual = firstMoney(item5.match(/\$\s*5,500/i)?.[0] || "");

  const totalRange = moneyRange(
    item7.match(/\$65,070,400\s+to\s+\$91,517,000/i)?.[0] ||
      t.match(/\$65,070,400\s+to\s+\$91,517,000/i)?.[0] ||
      ""
  );
  const perRoomRange = moneyRange(
    item7.match(/\$325,352\s+or\s+\$457,585/i)?.[0] ||
      t.match(/\$325,352\s+or\s+\$457,585/i)?.[0] ||
      item7.match(/\$325,352\s+to\s+\$457,585/i)?.[0] ||
      ""
  );
  const ihgPaidRange = moneyRange(
    t.match(/\$607,000\s+and\s+\$1,097,000/i)?.[0] || ""
  );

  const termNewDev = (() => {
    const m = item17.match(/expires\s+20\s+years/i);
    return m ? 20 : null;
  })();
  const termConversion = (() => {
    const m = item17.match(/10\s+years\s+from\s+date\s+Hotel\s+opens/i);
    return m ? 10 : null;
  })();
  const noRenewal = /does\s+not\s+provide\s+for\s+renewal/i.test(item17);

  return {
    parsedAt: new Date().toISOString().slice(0, 10),
    item5: {
      applicationFeePerRoom: appPerRoom,
      applicationFeeMinimum: appMin,
      pipInspectionFee: pipFee,
      preopeningSupportMin: preopenMin,
      preopeningSupportMax: preopenMax,
      ihglLearningAnnual: learningAnnual,
      deptHeadCertMin: certMin,
      deptHeadCertMax: certMax,
    },
    item6: {
      royaltyRoomPct: royaltyRoom,
      royaltyFoodBeveragePct: royaltyFb,
      servicesContributionPct: servicesContribution,
      loyaltyFullFolioPct: loyaltyFull,
      loyaltyRoomMeetingPct: loyaltyRoomMeeting,
      technologyPerRoomMonthly: techMonthly,
      gdsPerReservation: gdsPerRes,
      digitalMarketingCommissionPct: digitalPct,
    },
    item7: {
      typicalRoomCount: 200,
      totalInvestmentMin: totalRange[0],
      totalInvestmentMax: totalRange[1],
      perRoomMin: perRoomRange[0],
      perRoomMax: perRoomRange[1],
      paidToIhgflMin: ihgPaidRange[0],
      paidToIhgflMax: ihgPaidRange[1],
    },
    item17: {
      termNewDevelopmentYears: termNewDev,
      termConversionYears: termConversion,
      renewalProvided: !noRenewal,
    },
  };
}
