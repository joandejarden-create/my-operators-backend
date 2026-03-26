/**
 * Deal Capture™ Dashboard – Role Configuration
 * Single source of truth for KPI labels and Pipeline labels by role.
 */

export const ROLES = ['owner', 'brand', 'operator'];

/** KPI keys in display order (same 6 for all roles) */
export const KPI_KEYS = ['active', 'needs-action', 'new-this-week', 'awaiting', 'in-progress', 'at-risk'];

/** KPI labels by role (6 each) */
export const KPI_LABELS = {
  owner: [
    'Active Opportunities',
    'Needs Your Action',
    'New This Week',
    'Awaiting Counterparty',
    'In Negotiation',
    'At Risk'
  ],
  brand: [
    'Inbound Opportunities',
    'Needs Your Action',
    'New This Week',
    'In Review',
    'Awaiting Owner',
    'At Risk'
  ],
  operator: [
    'Inbound Opportunities',
    'Needs Your Action',
    'New This Week',
    'Feasibility / Bid Prep',
    'Awaiting Owner',
    'At Risk'
  ]
};

/** Canonical pipeline stage keys (always these 6) */
export const PIPELINE_STAGE_KEYS = ['submitted', 'review', 'engaged', 'negotiation', 'won', 'lost'];

/** Pipeline labels by role (stageKey -> label) */
export const PIPELINE_LABELS = {
  owner: {
    submitted: 'Submitted',
    review: 'Under Review',
    engaged: 'Bid Submitted',
    negotiation: 'Negotiation',
    won: 'Signed',
    lost: 'Passed'
  },
  brand: {
    submitted: 'New Inbound',
    review: 'Under Review',
    engaged: 'Bid Submitted',
    negotiation: 'Negotiation',
    won: 'Closed',
    lost: 'Passed'
  },
  operator: {
    submitted: 'New Inbound',
    review: 'Under Review',
    engaged: 'Bid Submitted',
    negotiation: 'Negotiation',
    won: 'Closed',
    lost: 'Passed'
  }
};

/** @param {string} role - owner|brand|operator */
export function getKpiLabels(role) {
  const r = ROLES.includes(role) ? role : 'owner';
  return KPI_KEYS.map((key, i) => ({
    key,
    label: KPI_LABELS[r][i]
  }));
}

/** @param {string} role - owner|brand|operator */
export function getPipelineLabels(role) {
  const r = ROLES.includes(role) ? role : 'owner';
  return PIPELINE_STAGE_KEYS.map(stageKey => ({
    stageKey,
    label: PIPELINE_LABELS[r][stageKey]
  }));
}
