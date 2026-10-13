/**
 * test-gdi-maturity-workflow-separation-v1.mjs
 * ACTIONABLE does not auto-set PURSUING.
 */
import assert from "node:assert/strict";
import {
  assignGdiMaturityState,
  applyGdiMaturityStamp,
  resolveSalesWorkflowState,
  SALES_WORKFLOW_STATE,
  GDI_MATURITY_STATE,
} from "../lib/group-demand-intelligence/gdi-maturity-v1.js";

// Explicit workflow preserved
{
  const opp = {
    title: "Test",
    organizationName: "Test Org",
    salesWorkflowState: SALES_WORKFLOW_STATE.RESEARCHING,
    gdiMaturityState: GDI_MATURITY_STATE.ACTIONABLE,
  };
  assert.equal(
    resolveSalesWorkflowState(opp),
    SALES_WORKFLOW_STATE.RESEARCHING
  );
}

// Default UNTOUCHED even when we force-stamp without Ready
{
  const { opportunity, evaluation } = applyGdiMaturityStamp({
    title: "Named Shell",
    organizationName: "Some Company LLC",
    officialSource: "https://example.com/event",
  });
  assert.equal(opportunity.salesWorkflowState, SALES_WORKFLOW_STATE.UNTOUCHED);
  assert.equal(evaluation.salesWorkflowInferredFromMaturity, false);
  assert.notEqual(opportunity.salesWorkflowState, SALES_WORKFLOW_STATE.PURSUING);
}

// Even if maturity were ACTIONABLE in evaluation path, workflow stays UNTOUCHED
// unless explicitly set on the opportunity.
{
  const r = assignGdiMaturityState({
    title: "X",
    organizationName: "Y",
    salesWorkflowState: undefined,
  });
  assert.equal(r.salesWorkflowState, SALES_WORKFLOW_STATE.UNTOUCHED);
  assert.notEqual(r.salesWorkflowState, SALES_WORKFLOW_STATE.PURSUING);
}

console.log("test-gdi-maturity-workflow-separation-v1: PASS");
