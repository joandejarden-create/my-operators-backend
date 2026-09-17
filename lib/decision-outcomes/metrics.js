/**
 * Phase-1 Decision & Outcome metrics (foundation only — no significance claims).
 */

import {
  PRODUCT_MODULE,
  EVENT_KIND,
  GDI_VALIDATION_TYPE,
  GDI_COMMERCIAL_VALUE,
} from "./types.js";
import {
  listDecisionSummaries,
  loadDecision,
  loadEvents,
  listHotelsWithDecisions,
} from "./persistence.js";
import { projectCurrentState } from "./projection.js";

async function hotelMetrics(hotelId, productModule) {
  const summaries = (await listDecisionSummaries(hotelId)).filter(
    (d) => !productModule || d.productModule === productModule
  );
  let validated = 0;
  let acted = 0;
  let outcomeed = 0;
  let worthPursuing = 0;
  let contactRight = 0;
  let contactAssessed = 0;

  for (const s of summaries) {
    const decision = await loadDecision(hotelId, s.decisionId);
    const events = await loadEvents(hotelId, s.decisionId);
    const current = projectCurrentState(decision, events);
    if (current.validationCount) validated += 1;
    if (current.actionCount) acted += 1;
    if (current.outcomeCount) outcomeed += 1;

    const commercial =
      current.latestValidationsByType?.[GDI_VALIDATION_TYPE.COMMERCIAL_VALUE];
    if (
      commercial?.validationValue === GDI_COMMERCIAL_VALUE.WORTH_PURSUING_NOW
    ) {
      worthPursuing += 1;
    }
    const person =
      current.latestValidationsByType?.[
        GDI_VALIDATION_TYPE.CONTACT_PERSON_VALIDATION
      ];
    if (person) {
      contactAssessed += 1;
      if (person.validationValue === "RIGHT_PERSON") contactRight += 1;
    }
  }

  const n = summaries.length || 0;
  return {
    hotelId,
    productModule: productModule || "ALL",
    decisionCount: n,
    validationCompletionRate: n ? validated / n : null,
    actionRate: n ? acted / n : null,
    outcomeCompletionRate: n ? outcomeed / n : null,
    worthPursuingCount: worthPursuing,
    contactPersonAccuracy:
      contactAssessed > 0 ? contactRight / contactAssessed : null,
    note: "Foundation metrics only — not statistical significance.",
  };
}

export async function getHotelDecisionMetrics(
  hotelId,
  { productModule = null } = {}
) {
  return hotelMetrics(hotelId, productModule);
}

export async function getModuleDecisionMetrics(productModule) {
  const hotels = await listHotelsWithDecisions();
  const perHotel = [];
  for (const h of hotels) {
    perHotel.push(await hotelMetrics(h, productModule));
  }
  const decisionCount = perHotel.reduce((s, m) => s + m.decisionCount, 0);
  return {
    productModule,
    hotelCount: hotels.length,
    decisionCount,
    hotels: perHotel,
    note: "Foundation aggregation only.",
  };
}

export async function getGdiPhase1Metrics(hotelId) {
  return getHotelDecisionMetrics(hotelId, {
    productModule: PRODUCT_MODULE.GDI,
  });
}

export async function getAdpPhase1Metrics(hotelId) {
  return getHotelDecisionMetrics(hotelId, {
    productModule: PRODUCT_MODULE.ADP,
  });
}

export { EVENT_KIND };
