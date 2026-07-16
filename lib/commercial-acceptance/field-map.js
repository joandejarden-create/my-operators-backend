/**
 * Commercial Acceptances — Airtable field map (Deal Capture MVP / AIRTABLE_BASE_ID).
 * Spec: docs/commercial-acceptance-airtable-fields.md
 */
export const COMMERCIAL_ACCEPTANCES_TABLE_NAME = "Commercial Acceptances";

export const MAP_COMMERCIAL_ACCEPTANCE = {
  acceptanceId: "Acceptance ID",
  recordLabel: "Record Label",
  memberLegalName: "Member Legal Name",
  companyProfile: "Company Profile",
  users: "Users",
  memberAccountId: "Member Account ID",
  acceptanceType: "Acceptance Type",
  memberType: "Member Type",
  billingClass: "Billing Class",
  participationLabel: "Participation Label",
  termsVersion: "Terms Version",
  scheduleVersion: "Schedule Version",
  scheduleTemplate: "Schedule Template",
  termsUrl: "Terms URL",
  acceptedByName: "Accepted By Name",
  acceptedByEmail: "Accepted By Email",
  acceptedByTitle: "Accepted By Title",
  acceptedAt: "Accepted At",
  acceptanceMethod: "Acceptance Method",
  acceptanceEvidence: "Acceptance Evidence",
  acceptanceEvidenceNotes: "Acceptance Evidence Notes",
  ipAddress: "IP Address",
  userAgent: "User Agent",
  effectiveDate: "Effective Date",
  initialTermEndDate: "Initial Term End Date",
  foundingEndDate: "Founding End Date",
  paidTransitionReviewDate: "Paid Transition Review Date",
  autoRenewal: "Auto Renewal",
  nonRenewalNoticeDays: "Non-Renewal Notice Days",
  listSubscriptionAnnualUsd: "List Subscription Annual USD",
  subscriptionAnnualUsd: "Subscription Annual USD",
  successFeeWaived: "Success Fee Waived",
  upfrontSubmissionFeeUsd: "Upfront Submission Fee USD",
  listPerKeyRateUsd: "List Per Key Rate USD",
  perKeyRateUsd: "Per Key Rate USD",
  listMinimumSuccessFeeUsd: "List Minimum Success Fee USD",
  minimumSuccessFeeUsd: "Minimum Success Fee USD",
  loiCommitmentFeePct: "LOI Commitment Fee Pct",
  finalSuccessFeePct: "Final Success Fee Pct",
  tailPeriodMonths: "Tail Period Months",
  discountApplied: "Discount Applied",
  discountType: "Discount Type",
  discountPercent: "Discount Percent",
  discountAmountUsd: "Discount Amount USD",
  discountAppliesTo: "Discount Applies To",
  discountDuration: "Discount Duration",
  discountValidThrough: "Discount Valid Through",
  discountCodeLabel: "Discount Code / Label",
  discountReason: "Discount Reason",
  discountApprovedBy: "Discount Approved By",
  feeNotes: "Fee Notes",
  acceptanceStatus: "Acceptance Status",
  platformAccessGranted: "Platform Access Granted",
  accessGrantedAt: "Access Granted At",
  grantedBy: "Granted By",
  internalNotes: "Internal Notes",
  supersededBy: "Superseded By",
  previousAcceptance: "Previous Acceptance",
  dealalityContactEmail: "Dealality Contact Email",
  memberRepresentativeEmail: "Member Representative Email",
};

export const VAL_ACCEPTANCE_TYPE = [
  "Public Terms Only",
  "Founding Schedule",
  "Standard Schedule",
  "Schedule Amendment",
  "Paid Transition",
];

export const VAL_MEMBER_TYPE = [
  "Owner Member",
  "Brand Member",
  "Operator Member",
  "Advisor",
  "Other",
];

export const VAL_BILLING_CLASS = [
  "founding_complimentary",
  "standard_owner",
  "standard_brand",
  "standard_operator",
  "enterprise_custom",
  "non_billing",
];

export const VAL_PARTICIPATION_LABEL = [
  "Founding Participant",
  "Pilot",
  "Standard",
  "Enterprise Custom",
  "Discounted",
];

export const VAL_SCHEDULE_TEMPLATE = [
  "founding_participant_prefilled",
  "standard_template",
  "custom",
];

export const VAL_ACCEPTANCE_METHOD = [
  "In-platform click",
  "Email reply",
  "DocuSign",
  "PDF signature",
  "Other",
];

export const VAL_DISCOUNT_TYPE = [
  "Percent",
  "Fixed USD",
  "Full waiver",
  "Custom mix",
  "None",
];

export const VAL_DISCOUNT_APPLIES_TO = [
  "Subscription",
  "Success Fee",
  "Per-key rate",
  "Minimum fee",
  "LOI Commitment Fee",
  "Final Success Fee",
  "Add-ons",
  "First year only",
];

export const VAL_DISCOUNT_DURATION = [
  "Entire Initial Term",
  "First year only",
  "Through fixed date",
  "Until written notice",
];

export const VAL_ACCEPTANCE_STATUS = [
  "Pending",
  "Accepted",
  "Superseded",
  "Expired",
  "Withdrawn",
];

export const RECORD_LABEL_FORMULA =
  "IF(AND({Member Legal Name}, {Acceptance Type}, {Accepted At}), {Member Legal Name} & \" — \" & {Acceptance Type} & \" (\" & DATETIME_FORMAT({Accepted At}, 'YYYY-MM-DD') & \")\", IF(AND({Member Legal Name}, {Acceptance Type}), {Member Legal Name} & \" — \" & {Acceptance Type}, {Acceptance ID}))";
