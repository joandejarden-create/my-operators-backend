/**

 * Unit tests for deriveDealNextAction (My Deals recommended next step).

 */

import {

  deriveDealNextAction,

  computeDealContactedSignals,

  PHASE,

  QUICK_ACTION_IDS,

} from "../lib/dealality/derive-deal-next-action.js";



let passed = 0;

let failed = 0;



function ok(cond, msg) {

  if (cond) {

    passed += 1;

    console.log("ok:", msg);

  } else {

    failed += 1;

    console.error("FAIL:", msg);

  }

}



function baseDeal(overrides) {

  return {

    id: "recTEST000000001",

    projectName: "Test Hotel",

    hotelLocation: "Austin, US",

    dealStatus: "Active",

    formStatus: "Complete",

    dealBidType: "Franchise only",

    dealReadinessStage: "",

    dealReadinessScore: null,

    dealReadinessMissingCount: null,

    dealReadinessBlockingCount: null,

    dealReadinessLastReviewed: "",

    hasOutreachSetup: false,

    preferredBrandsChosen: "",

    strategicIntentForm: {},

    ...overrides,

  };

}



function hasAction(result, id) {

  return (result.quickActions || []).some((a) => a.id === id);

}



// sparse draft deal

{

  const r = deriveDealNextAction({

    deal: baseDeal({

      formStatus: "Draft",

      projectName: "",

      hotelLocation: "—",

    }),

  });

  ok(r.phase === PHASE.INTAKE_INCOMPLETE, "sparse draft → intake incomplete");

  ok(r.whatNeedsAttention.includes("intake"), "sparse draft → intake attention");

  ok(r.showRecommendedNextStep === true, "sparse draft → show in menu");

  ok(hasAction(r, QUICK_ACTION_IDS.COMPLETE_DEAL_INFO), "sparse draft → Complete Deal Info action");

}



// no DRS

{

  const r = deriveDealNextAction({ deal: baseDeal() });

  ok(r.phase === PHASE.READINESS_NOT_RUN, "no DRS → readiness not run");

  ok(r.whatNeedsAttention.includes("Deal Readiness"), "no DRS → readiness attention");

  ok(r.showRecommendedNextStep === true, "no DRS → show in menu");

}



// Shaping + blocking

{

  const r = deriveDealNextAction({

    deal: baseDeal({

      dealReadinessStage: "Shaping",

      dealReadinessScore: 55,

      dealReadinessBlockingCount: 3,

      dealReadinessMissingCount: 8,

      dealReadinessLastReviewed: "2026-01-01",

    }),

  });

  ok(r.phase === PHASE.READINESS_GAPS, "Shaping/blocking → readiness gaps");

  ok(r.whatNeedsAttention.includes("gaps"), "blocking → gaps attention");

  ok(r.missingInformation.length > 0, "blocking → missing info bullets");

}



// advancing / no outreach setup

{

  const r = deriveDealNextAction({

    deal: baseDeal({

      dealReadinessStage: "Advancing",

      dealReadinessScore: 78,

      dealReadinessLastReviewed: "2026-01-01",

      preferredBrandsChosen: "Marriott",

      hasOutreachSetup: false,

    }),

  });

  ok(

    r.phase === PHASE.OUTREACH_PREP || r.phase === PHASE.BRAND_ALIGNMENT,

    "advancing no outreach → outreach prep or brand alignment"

  );

  if (r.phase === PHASE.OUTREACH_PREP) {

    ok(r.whatNeedsAttention.includes("outreach"), "advancing → outreach attention");

    ok(hasAction(r, QUICK_ACTION_IDS.PREPARE_OUTREACH), "advancing → outreach quick action");

  }

}



// brand path alignment

{

  const r = deriveDealNextAction({

    deal: baseDeal({

      dealReadinessStage: "Advancing",

      dealReadinessScore: 80,

      dealReadinessLastReviewed: "2026-01-01",

      preferredBrandsChosen: "Hyatt, Hilton",

      hasOutreachSetup: true,

    }),

  });

  ok(r.phase === PHASE.BRAND_ALIGNMENT, "brand path ready → brand alignment");

  ok(r.whatNeedsAttention.includes("alignment"), "brand path → alignment attention");

}



// operator path

{

  const r = deriveDealNextAction({

    deal: baseDeal({

      dealBidType: "3rd party only",

      dealReadinessStage: "Advancing",

      dealReadinessScore: 72,

      dealReadinessLastReviewed: "2026-01-01",

      hasOutreachSetup: true,

      preferredBrandsChosen: "",

    }),

    operatorStrategyRowCount: 0,

  });

  ok(r.phase === PHASE.OPERATOR_STRATEGY, "operator path → operator strategy");

  ok(r.whatNeedsAttention.includes("operator"), "operator path → operator attention");

}



// contacted awaiting owner

{

  const r = deriveDealNextAction({

    deal: baseDeal({

      dealReadinessStage: "Ready",

      dealReadinessScore: 93,

      dealReadinessLastReviewed: "2026-01-01",

      hasOutreachSetup: true,

    }),

    contactedRows: [{ status: "More Info Requested", ndaStatus: "", dealRoomAccess: "" }],

  });

  ok(r.phase === PHASE.ACTIVE_OUTREACH_OWNER, "contacted owner action phase");

  ok(r.whatNeedsAttention.toLowerCase().includes("owner"), "owner phase → owner attention");

  ok(r.primaryActionLabel === "Review Contacted Brand Status", "owner → primary label");

}



// contacted awaiting brand — strong readiness (Cancún-style)

{

  const r = deriveDealNextAction({

    deal: baseDeal({

      projectName: "Aeropuerto Cancún Select-Service Hotel",

      dealStatus: "Active / Visible",

      dealReadinessStage: "Ready",

      dealReadinessScore: 93,

      dealReadinessLastReviewed: "2026-01-01",

      hasOutreachSetup: true,

    }),

    contactedRows: [{ status: "Sent / Awaiting Response", ndaStatus: "", dealRoomAccess: "" }],

  });

  ok(r.phase === PHASE.ACTIVE_OUTREACH_BRAND, "awaiting brand → active outreach");

  ok(r.currentDealState.includes("outreach is already underway"), "strong readiness → underway state");

  ok(r.whatNeedsAttention.includes("before adding more brands"), "awaiting brand → specific attention");

  ok(r.primaryActionLabel === "Review Contacted Brand Status", "awaiting brand → primary label");

  ok(r.showRecommendedNextStep === true, "awaiting brand → show in menu");

  ok(Boolean(r.currentDealState), "result includes currentDealState");

  ok(Boolean(r.primaryQuickActionId), "result includes primaryQuickActionId");

}



// deal signals

{

  const signals = computeDealContactedSignals([

    { status: "Sent / Awaiting Response", lastActivity: "2026-06-01" },

    { status: "More Info Requested", lastActivity: "2026-06-10" },

  ]);

  ok(signals.some((s) => s.label === "Contacted Brands" && s.value === "2"), "signals → contacted count");

  ok(signals.some((s) => s.label === "Awaiting Owner Action"), "signals → awaiting owner");

}



// signed / passed muted

{

  const r = deriveDealNextAction({

    deal: baseDeal({ dealStatus: "Signed" }),

    contactedRows: [{ status: "Declined" }, { status: "Archived" }],

  });

  ok(r.phase === PHASE.CLOSED_PASSED, "signed/archived → closed passed");

  ok(r.muted === true, "closed → muted");

  ok(r.showRecommendedNextStep === true, "closed → still meaningful guidance");

}



// invalid deal id hidden

{

  const r = deriveDealNextAction({ deal: { id: "invalid" } });

  ok(r.showRecommendedNextStep === false, "invalid id → hide menu item");

}



console.log(`\ntest-deal-next-action: ${passed} passed, ${failed} failed`);

process.exit(failed > 0 ? 1 : 0);


