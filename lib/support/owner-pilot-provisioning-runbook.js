/**
 * Owner Pilot Provisioning Runbook — server-side content only (not public/).
 * Served via GET /api/support/owner-pilot-provisioning-runbook (admin auth required).
 */

/** @returns {import("./owner-pilot-provisioning-runbook.types.js").OwnerPilotRunbook} */
export function getOwnerPilotProvisioningRunbook() {
  return {
    title: "Owner Pilot Provisioning Runbook",
    subtitle:
      "Repeatable process for provisioning and validating owner/advisor pilot users — login, workspace access, deal visibility, and cross-owner isolation.",
    badges: [
      { label: "Internal Runbook", variant: "internal" },
      { label: "Owner Pilot" },
    ],
    warning:
      "<strong>Admin only.</strong> This page is intended for Dealality internal/admin use only. Do not expose to pilot users. Contains operational details about Memberstack identity, Airtable Users, Company Profile linkage, workspace permissions, and deal access rules.",
    sections: [
      sectionSourceOfTruth(),
      sectionPilotReadinessStandards(),
      sectionRequiredRecords(),
      sectionUsersPilotProvisioningView(),
      sectionOwnerPilotOutreachSetup(),
      sectionMemberstackSetup(),
      sectionOwnerChecklist(),
      sectionAdvisorChecklist(),
      sectionVerificationScripts(),
      sectionBrowserQa(),
      sectionFailureModes(),
      sectionOptionalFutureCleanup(),
      sectionFutureAccessModel(),
      sectionJoanPilotReference(),
    ],
  };
}

function sectionSourceOfTruth() {
  return {
    id: "source-of-truth",
    title: "1. Source of Truth Model",
    defaultOpen: true,
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "Permission flow: Memberstack JWT → <code>memberstackAuth</code> → <code>resolveDealalityUser</code> → <code>requireMyDealsAccess</code> / <code>requireDealRecordAccess</code>.",
      },
      {
        type: "table",
        headers: ["Layer", "Owns", "Does not own"],
        rows: [
          [
            "<strong>Memberstack</strong>",
            "Login identity (<code>mem_…</code> / <code>mem_sb_…</code>), JWT, email, password, plan membership",
            "Workspace access, deal visibility, company type, admin flags",
          ],
          [
            "<strong>Airtable Users</strong>",
            "Identity mirror, Company Profile link, account status, role hints (User Type, etc.)",
            "Workspace permissions — no <strong>Workspace Access</strong> field on Users in the current base",
          ],
          [
            "<strong>Airtable Company Profile</strong>",
            "<strong>Workspace Access</strong>, <strong>Company Type</strong>, operating model, team links",
            "Memberstack password",
          ],
          [
            "<strong>Airtable Deals</strong>",
            "Deal ownership via <strong>Company Profile</strong> + <strong>User_ID</strong> links",
            "Authentication",
          ],
        ],
      },
      {
        type: "paragraph",
        html:
          "<strong>Rule:</strong> Workspace permissions are sourced from <strong>Company Profile → Workspace Access</strong> (<code>fldhZqzi0LskI0MpK</code>). There is no <code>Workspace Access</code> field on Users in the current base. Company Profile also owns <strong>Company Type</strong> and related capability fields; Users role hints are not workspace SSOT.",
      },
    ],
  };
}

function sectionPilotReadinessStandards() {
  return {
    id: "pilot-readiness-standards",
    title: "1a. Pilot Readiness Standards",
    defaultOpen: true,
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "These are the <strong>required standards</strong> for every owner/advisor pilot before an invite is sent.",
      },
      {
        type: "unorderedList",
        items: [
          "Workspace Access must be set on <strong>Company Profile</strong> (<code>fldhZqzi0LskI0MpK</code>). There is no Workspace Access field on Users in the current base — workspace permissions are not sourced from the Users row.",
          "Memberstack Member ID must be synced in both legacy fields: <strong>Unique_Webflow_ID</strong> / <strong>Unique Webflow ID</strong> and <strong>Slug</strong>.",
          "Deals must link to <strong>Company Profile</strong>. <strong>User_ID</strong> may remain as optional secondary scoping, but User_ID-only deal access is legacy and not accepted for new pilots.",
          "Account Status must be <code>Active</code> before invite.",
          "Production Airtable Users rows must never contain <code>mem_sb_</code> test Memberstack IDs.",
          "<code>verify-pilot-user-by-email</code> must exit <strong>0 with zero warnings</strong> before invite.",
        ],
      },
      { type: "heading", text: "How standards are enforced" },
      {
        type: "unorderedList",
        items: [
          "Signup/webhook sync never writes Company Type, Region Access, or Deal Access to Users (<code>lib/airtable-users-protected-patch.js</code>). Protected patch also blocks a legacy <code>Workspace Access</code> key on Users if it ever appears — that field does not exist on Users today.",
          "<code>link-airtable-user-memberstack.mjs</code> writes both legacy ID fields; rejects <code>mem_sb_</code> unless <code>--allow-test-memberstack-id</code>.",
          "<code>backfill-pilot-deal-company-profile.mjs</code> links pilot deals to Company Profile when missing.",
          "<code>verify-pilot-user-by-email.mjs</code> validates MS id pair, Account Status, Company Profile Workspace Access, and deal scoping.",
        ],
      },
      {
        type: "paragraph",
        html:
          "Reference baseline: Joan (<code>joan@aohospitalityadvisors.com</code>) meets all standards — see <strong>§11 Joan Clean Baseline</strong>.",
      },
    ],
  };
}

function sectionRequiredRecords() {
  return {
    id: "required-records",
    title: "2. Required Airtable Records and Fields",
    contentBlocks: [
      { type: "heading", text: "Users table" },
      {
        type: "paragraph",
        html: 'Table: <code>tbl6shiyz2wdUqE5F</code> (env: <code>AIRTABLE_ME_USERS_TABLE</code>)',
      },
      {
        type: "table",
        headers: ["Field", "Pilot required?", "Purpose"],
        rows: [
          ["<strong>Email</strong>", "Yes", "Lookup fallback; display; signup match"],
          [
            "<strong>Unique Webflow ID</strong> / <strong>Unique_Webflow_ID</strong>",
            "Yes (production)",
            "Primary Memberstack member id — both names may appear in base",
          ],
          ["<strong>Slug</strong>", "Yes (mirror)", "Secondary Memberstack id — keep identical to Unique Webflow ID"],
          ["<strong>First Name</strong> / <strong>Last Name</strong>", "Recommended", "Display"],
          ["<strong>Company Profile</strong> (link)", "<strong>Yes</strong>", "Permission source; deal scoping"],
          [
            "<strong>Account Status</strong>",
            "<strong>Yes</strong>",
            "<code>Active</code> required before pilot invite; missing = provisioning issue",
          ],
          [
            "<strong>Platform Role</strong> / <strong>User Type</strong> / <strong>Role</strong>",
            "User Type: yes for provisioning view",
            "<strong>User Type</strong> controls which Webflow/app pages are used — not workspace permissions (CP → Workspace Access). Platform Role/Role are code fallbacks; may not exist on live base.",
          ],
          [
            "<strong>Contact Visibility</strong>",
            "Yes (provisioning view)",
            "Controls whether the user/contact is shown in the platform — not workspace permissions",
          ],
          [
            "<strong>Deal Access</strong> / <strong>Document Access</strong>",
            "No (pilot workspace gates)",
            "Exist on Users but are not workspace permissions — do not substitute for Company Profile Workspace Access",
          ],
          [
            "<strong>Region Access</strong>",
            "No (pilot)",
            "Protected from sync if present; not enforced in deal filter today",
          ],
          [
            "<strong>Deals</strong> (link on Users row)",
            "Not for pilot provisioning view",
            "Reverse link — may be used for dashboards/reporting; <strong>does not</strong> grant My Deals unless deal row links back via Company Profile or User_ID",
          ],
        ],
      },
      {
        type: "alert",
        html:
          "<strong>Account Status:</strong> Field exists on Users in this base. Set <code>Active</code> before pilot invite. <code>Pending</code> blocks workspace access. Missing or empty status is a provisioning defect — do not invite until resolved.",
      },
      {
        type: "paragraph",
        html:
          "Never write from signup/webhook: <code>Company Type</code>, <code>Region Access</code>, <code>Deal Access</code> (<code>lib/airtable-users-protected-patch.js</code>). There is no <code>Workspace Access</code> field on Users to set — workspace permissions live on Company Profile only.",
      },
      { type: "heading", text: "Company Profile table" },
      {
        type: "table",
        headers: ["Field", "Owner pilot?", "Purpose"],
        rows: [
          ["<strong>Company Name</strong>", "Yes", "Display / lookup"],
          ["<strong>Workspace Access</strong>", "<strong>Yes</strong>", "Must include <code>Owner</code> for My Deals — field id <code>fldhZqzi0LskI0MpK</code>"],
          [
            "<strong>Company Type</strong>",
            "Strongly recommended",
            "Fallback if Workspace Access empty — e.g. <code>Hotel Owner</code>",
          ],
          ["<strong>Team Members</strong> (→ Users)", "Recommended", "Partner Directory; not required for My Deals gate"],
          ["<strong>Operating regions</strong>", "Optional", "Not enforced on deal access today"],
        ],
      },
      {
        type: "paragraph",
        html:
          "Set <strong>Workspace Access</strong> on the linked <strong>Company Profile</strong> record. The Users table has no Workspace Access column in the current base.",
      },
      { type: "heading", text: "Deals table" },
      { type: "paragraph", html: "Table: <code>tblbvSxjiIhXzW6XW</code>" },
      {
        type: "table",
        headers: ["Field", "Required?", "Purpose"],
        rows: [
          [
            "<strong>Company Profile</strong> (link)",
            "<strong>Yes (required)</strong>",
            "User sees deal when deal company matches user Company Profile",
          ],
          [
            "<strong>User_ID</strong> (link → Users)",
            "Optional",
            "Secondary scoping only — do not use as sole access path for new pilots",
          ],
          ["<strong>Deal Status</strong>", "Recommended", "UX only for pilot"],
        ],
      },
      {
        type: "alert",
        html:
          "<strong>Deal ownership standard (active):</strong> <strong>Deals → Company Profile</strong> is required for all new pilots. <strong>Deals → User_ID</strong> is optional secondary scoping. User_ID-only access is legacy — do not accept for new pilots. Joan reference account: 8/8 deals via Company Profile.",
      },
      {
        type: "paragraph",
        html:
          "Access rule (<code>lib/dealality/deal-record-access.js</code>): Admin → all deals. Owner → allow if deal Company Profile ∩ user companyIds OR deal User_ID ∩ user record id.",
      },
    ],
  };
}

function sectionUsersPilotProvisioningView() {
  return {
    id: "users-pilot-provisioning-view",
    title: "2b. Users Pilot Provisioning View (Airtable)",
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "This section recommends a <strong>narrow Airtable view</strong> for manual pilot provisioning only. It does <strong>not</strong> mean fields hidden from this view are unused globally — many remain required for User Management, Partner Directory, brand/operator modules, intake flows, dashboards, and reporting.",
      },
      {
        type: "alert",
        html:
          "<strong>Do not delete, rename, or remove fields from other operational views.</strong> Create or update one dedicated view named <strong>Pilot Provisioning</strong>. Full field notes: <code>reports/users-table-cleanup-audit.md</code> (Pilot Provisioning View Audit).",
      },
      { type: "heading", text: "Show in Pilot Provisioning view" },
      {
        type: "paragraph",
        html:
          "<strong>Distinction:</strong> Workspace access → Company Profile → Workspace Access. <strong>User Type</strong> → which Webflow/app pages are used. <strong>Contact Visibility</strong> → whether the user is shown in the platform.",
      },
      {
        type: "orderedList",
        items: [
          "<strong>User_ID</strong>",
          "<strong>Record_ID</strong>",
          "<strong>Unique_Webflow_ID</strong>",
          "<strong>Slug</strong> (Memberstack id mirror — edit via <code>link-airtable-user-memberstack.mjs</code>)",
          "<strong>Email</strong>",
          "<strong>First Name</strong>, <strong>Last Name</strong>",
          "<strong>Account Status</strong> = <code>Active</code> before invite",
          "<strong>User Type</strong> — Webflow/app page routing (not workspace SSOT)",
          "<strong>Contact Visibility</strong> — platform visibility (not workspace SSOT)",
          "<strong>Company Profile</strong> (link)",
          "<strong>Company Name</strong> (signup/display hint)",
          "<strong>Phone Number</strong>",
        ],
      },
      { type: "heading", text: "Hide from Pilot Provisioning view only" },
      {
        type: "paragraph",
        html:
          "Hide these columns <strong>in this view only</strong>. They may still be used elsewhere in Dealality:",
      },
      {
        type: "unorderedList",
        items: [
          "<strong>Deal Access</strong> / <strong>Document Access</strong> — User Management metadata; not My Deals route gates today; may support Deal Room later",
          "<strong>Deals</strong> and other deal link columns — reverse links; dashboards/reporting; access is via Deals → Company Profile (+ optional User_ID)",
          "Region / language fields — User Management, Partner Directory, <code>/api/me</code>, future access logic",
          "<strong>HO - *</strong> / <strong>HB - *</strong> intake fields — onboarding, intake, deal setup flows",
          "Brand / operator / favorites links — brand explorer, operator setup, favorites modules",
          "Partner directory metrics and responsiveness badges",
        ],
      },
      {
        type: "paragraph",
        html:
          "Suggested view description: <em>Pilot invite provisioning only. Workspace access is controlled on Company Profile → Workspace Access. User Type controls which Webflow/app pages are used. Contact Visibility controls whether the user is shown in the platform. Fields hidden here may still be used elsewhere in Dealality.</em>",
      },
    ],
  };
}

function sectionOwnerPilotOutreachSetup() {
  return {
    id: "owner-pilot-outreach-setup",
    title: "2c. Owner Pilot Outreach Setup",
    contentBlocks: [
      {
        type: "alert",
        variant: "info",
        html:
          "<strong>Internal GTM only.</strong> Pilot outreach tracking lives in the GTM base table <strong>Pilot Target List</strong> (<code>tblgsKWuI25MWohAP</code>). This is not product-facing and does not send email automatically.",
      },
      {
        type: "paragraph",
        html:
          "<strong>Airtable is the source of truth</strong> for who is being contacted, message angle, approved copy, status, and follow-ups. First wave should be <strong>manual and founder-led</strong> — draft offline if helpful, paste final copy into Airtable, send manually.",
      },
      {
        type: "unorderedList",
        items: [
          "Prioritize <strong>CALA</strong> for real pilot opportunities (CALA-first, not CALA-only).",
          "Use warm non-CALA contacts for feedback/referral/workflow validation unless explicitly prioritized for pilot opportunity sourcing.",
          "Do not ask brands/operators for confidential owner pipelines. Ask for criteria input, operator perspective, or owner-opt-in introductions only.",
        ],
      },
      {
        type: "table",
        headers: ["Rule", "Detail"],
        rows: [
          [
            "<strong>Final Approved Email</strong>",
            "Only this field is exported as the mail-merge message body.",
          ],
          [
            "<strong>Ready for Mail Merge</strong>",
            "Must be checked before CSV export (plus Outreach Status = Approved).",
          ],
          [
            "<strong>Do Not Contact</strong>",
            "Always excludes a record from export and send views.",
          ],
          [
            "<strong>Outreach Status</strong>",
            "New workflow field — separate from legacy <strong>Status</strong> (Not Contacted / In Progress / …).",
          ],
          [
            "<strong>Outreach Message Angle</strong>",
            "Existing angle picklist — use <strong>Why They Matter</strong> for free-text narrative.",
          ],
        ],
      },
      {
        type: "heading",
        text: "Outreach Status workflow",
      },
      {
        type: "unorderedList",
        items: [
          "<code>Not Started</code> → <code>Draft Needed</code> → <code>Drafted</code> → <code>Needs Review</code> → <code>Approved</code> → <code>Sent</code>",
          "Replies: <code>Replied</code> or <code>Follow-Up Needed</code>",
          "Terminal: <code>Not Interested</code>, <code>Converted to Pilot</code>, <code>Archived</code>",
        ],
      },
      {
        type: "heading",
        text: "Scripts (read-only export unless field setup)",
      },
      {
        type: "unorderedList",
        items: [
          "<code>node scripts/setup-owner-targets-outreach-fields.mjs --dry-run</code> — preview missing fields",
          "<code>node scripts/setup-owner-targets-outreach-fields.mjs --execute</code> — create missing fields only",
          "<code>node scripts/report-owner-targets-outreach-readiness.mjs</code> — pipeline counts",
          "<code>node scripts/export-owner-targets-mail-merge.mjs --dry-run</code> — preview CSV rows",
          "<code>node scripts/export-owner-targets-mail-merge.mjs --batch \"Pilot Wave 1\"</code> — write CSV",
        ],
      },
      {
        type: "paragraph",
        html:
          "See also <code>docs/gtm-owner-target-list.md</code> (Pilot Target List outreach section). Do not connect Gmail/SendGrid/Mailchimp in this phase.",
      },
    ],
  };
}

function sectionMemberstackSetup() {
  return {
    id: "memberstack-setup",
    title: "3. Memberstack Setup Steps",
    contentBlocks: [
      { type: "heading", text: "Create / invite user" },
      {
        type: "orderedList",
        items: [
          "Use <strong>Production</strong> Memberstack for real pilots on dealality.com (not Test Mode).",
          "Create member with pilot email or invite via Memberstack dashboard.",
          "Assign an approved access plan so login is not blocked by Memberstack itself.",
        ],
      },
      { type: "heading", text: "Confirm Memberstack ID" },
      {
        type: "unorderedList",
        items: [
          "Production id: <code>mem_…</code> (not <code>mem_sb_…</code>)",
          "Dashboard → Members → copy member id, or inspect JWT after login",
        ],
      },
      { type: "heading", text: "Link Memberstack ID to Airtable" },
      {
        type: "code",
        text:
          "node scripts/link-airtable-user-memberstack.mjs \\\n  --email pilot@example.com \\\n  --memberstack-id mem_XXXXX \\\n  --dry-run   # then omit --dry-run",
      },
      {
        type: "paragraph",
        html: "Writes <strong>Unique Webflow ID</strong> (Memberstack Member ID) and <strong>Slug</strong> (mirror) — both must match. Rejects <code>mem_sb_</code> unless <code>--allow-test-memberstack-id</code>.",
      },
      { type: "heading", text: "Live vs Test Memberstack" },
      {
        type: "unorderedList",
        items: [
          "Live Memberstack IDs start with <code>mem_</code>",
          "Test Mode IDs start with <code>mem_sb_</code>",
          "Live and Test Mode are <strong>separate member databases</strong> — same email can exist in both with different passwords",
          "<strong>Never write <code>mem_sb_</code> IDs into production Airtable Users rows</strong>",
          "<code>MEMBERSTACK_TEST_SECRET_KEY</code> is server-side only (post-login <code>/api/me</code> resolution) — it does <strong>not</strong> affect browser login",
          "For localhost: reset the Test Mode password in Memberstack dashboard, or log in on production and hand off <code>http://localhost:8080/app?msToken=&lt;jwt&gt;</code>",
        ],
      },
      {
        type: "table",
        headers: ["Environment", "Memberstack id", "Airtable storage"],
        rows: [
          ["<strong>Production</strong>", "<code>mem_…</code>", "Store live id on Users row"],
          [
            "<strong>Localhost</strong>",
            "<code>mem_sb_…</code> (Test Mode)",
            "Do not overwrite live id; resolve by email on server",
          ],
        ],
      },
      { type: "heading", text: "Memberstack custom fields backfill" },
      {
        type: "paragraph",
        html:
          "<code>scripts/link-airtable-user-memberstack.mjs</code> updates <strong>Airtable only</strong> — it does not patch Memberstack custom fields.",
      },
      {
        type: "paragraph",
        html: "After manual linking, run:",
      },
      {
        type: "code",
        text: "node scripts/backfill-memberstack-signup-fields.mjs --email <email>",
      },
      {
        type: "paragraph",
        html: "Verify these Memberstack custom fields are populated:",
      },
      {
        type: "unorderedList",
        items: [
          "<code>first-name</code>",
          "<code>last-name</code>",
          "<code>company-name</code>",
          "<code>airtable-user-id</code>",
          "<code>company-profile-id</code>",
          "<code>unique-webflow-id</code>",
        ],
      },
      { type: "heading", text: "Field ownership" },
      {
        type: "table",
        headers: ["Memberstack owns", "Airtable owns"],
        rows: [
          [
            "Password, JWT, member id, email, plan membership, name/phone/company hints",
            "Workspace Access, Company Type, deal links, approval status, Company Profile link",
          ],
        ],
      },
      {
        type: "paragraph",
        html:
          "<strong>Do not sync from Memberstack:</strong> Workspace Access, Company Type, Deal Access, Region Access, Admin/Demo flags.",
      },
    ],
  };
}

function sectionOwnerChecklist() {
  return {
    id: "owner-checklist",
    title: "4. Pilot Owner Provisioning Checklist",
    contentBlocks: [
      {
        type: "orderedList",
        className: "runbook-checklist",
        items: [
          "Create or confirm <strong>Company Profile</strong> — name, <strong>Company Type</strong> (e.g. <code>Hotel Owner</code>)",
          "Set <strong>Workspace Access</strong> on Company Profile (e.g. <code>Owner</code>; Admin only for internal staff)",
          "Create <strong>Users</strong> row — email, first/last name",
          "Link <strong>Users → Company Profile</strong>",
          "Set <strong>Account Status</strong> = <code>Active</code>",
          "Create Memberstack member (Production) — live <code>mem_…</code> id",
          "Link live Memberstack id to both <strong>Unique_Webflow_ID</strong> / <strong>Unique Webflow ID</strong> and <strong>Slug</strong> (<code>link-airtable-user-memberstack.mjs</code>)",
          "Run <code>backfill-memberstack-signup-fields.mjs --email &lt;email&gt;</code>",
          "Link every pilot deal to <strong>Company Profile</strong> (<code>backfill-pilot-deal-company-profile.mjs</code> or manual)",
          "Optionally keep <strong>User_ID</strong> on deals as secondary scoping — not required as sole path",
          "Run <code>node scripts/verify-pilot-user-by-email.mjs --email &lt;email&gt;</code> — require <strong>exit 0 with zero warnings</strong> before invite",
          "Login → <code>GET /api/me</code> — <code>workspaceAccess</code> includes Owner, <code>accountAccess.state</code> = active",
          "Open <strong>My Deals</strong> — correct deal count",
          "Run <code>npm run test:batch1-cross-owner-access</code>",
          "Browser: open another owner's deal URL — expect 403 / blocked",
        ],
      },
      {
        type: "paragraph",
        html:
          "<strong>Do not rely on:</strong> Users Platform Role alone, Demo workspace alone, User_ID-only deal access, or Deals linked only on the Users row.",
      },
    ],
  };
}

function sectionAdvisorChecklist() {
  return {
    id: "advisor-checklist",
    title: "5. Pilot Advisor Provisioning Checklist",
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "<strong>Current limitation:</strong> No dedicated external advisor access model in code today. Advisor company types exist but do not grant Owner My Deals access unless <strong>Workspace Access</strong> includes <code>Owner</code>.",
      },
      {
        type: "table",
        headers: ["Approach", "When", "Setup"],
        rows: [
          [
            "<strong>A. Owner-representative</strong>",
            "Advisor works for owner org",
            "Same Company Profile as owner; Workspace Access = Owner; separate Users row + Memberstack login",
          ],
          [
            "<strong>B. Internal Dealality admin</strong>",
            "Deal Capture staff only",
            "Workspace Access = Admin — <strong>not for external advisors</strong>",
          ],
          [
            "<strong>C. External collaborator</strong>",
            "Third-party advisor",
            "<strong>Not supported yet</strong> — needs advisor→deal or advisor→company relationship fields",
          ],
        ],
      },
      {
        type: "paragraph",
        html:
          "For owner-representative pilots: use <strong>Option A</strong> only when the advisor legitimately shares the owner company profile. Do <strong>not</strong> use Admin for external advisors.",
      },
      {
        type: "paragraph",
        html: "Fields needed later: Advisor Company Profile, advisor deal assignments, read-only vs edit scope.",
      },
    ],
  };
}

function sectionVerificationScripts() {
  return {
    id: "verification-scripts",
    title: "6. Verification Scripts",
    contentBlocks: [
      {
        type: "alert",
        html:
          "<strong>Pre-invite gate:</strong> <code>verify-pilot-user-by-email.mjs</code> is the final check before sending a pilot invite. Require <strong>exit 0 with zero warnings</strong>. Any warning or problem code means provisioning is incomplete.",
      },
      { type: "heading", text: "Existing scripts (use today)" },
      {
        type: "unorderedList",
        items: [
          "<code>scripts/verify-pilot-user-by-email.mjs --email</code> — <strong>final pre-invite gate</strong> (MS id, Account Status, WS Access, deal scoping)",
          "<code>scripts/link-airtable-user-memberstack.mjs</code> — link Memberstack id to Users (Airtable only)",
          "<code>scripts/backfill-memberstack-signup-fields.mjs --email</code> — patch MS custom fields after manual link",
          "<code>scripts/backfill-pilot-deal-company-profile.mjs --email --dry-run|--execute</code> — link pilot deals to Company Profile",
          "<code>scripts/verify-demo-user-setup.mjs</code> — Users row, Memberstack id, company link",
          "<code>scripts/verify-demo-user-deals.mjs</code> — deal linkage + My Deals filter count",
          "<code>scripts/test-batch1-cross-owner-access.mjs</code> — cross-owner denial",
          "<code>scripts/audit-memberstack-airtable-users.mjs</code> — MS ↔ Airtable audit",
          "<code>scripts/test-account-access-status.mjs</code> — pending vs active logic",
          "<code>scripts/test-company-workspace-access.mjs</code> — Workspace Access resolution",
          "<code>scripts/test-memberstack-airtable-source-of-truth.mjs</code> — protected-field sync guards",
          "<code>npm run test:batch1-route-auth</code> / <code>test:batch2a-route-auth</code> — route auth wiring",
        ],
      },
      { type: "heading", text: "Pre-invite gate" },
      {
        type: "paragraph",
        html:
          "Run <code>verify-pilot-user-by-email.mjs</code> last. Require exit <strong>0 with zero warnings</strong> — see <strong>§1a Pilot Readiness Standards</strong>.",
      },
      { type: "heading", text: "Recommended future scripts (plan only)" },
      {
        type: "unorderedList",
        items: [
          "<code>verify-pilot-user-deals.mjs</code> — expected deal ids per pilot",
          "<code>verify-api-me-resolution.mjs</code> — JWT → /api/me workspace assertions",
          "<code>verify-memberstack-airtable-linkage.mjs</code> — MS Admin API vs Airtable id",
          "<code>verify-company-profile-access.mjs</code> — Workspace Access vs Company Type consistency",
        ],
      },
    ],
  };
}

function sectionBrowserQa() {
  return {
    id: "browser-qa",
    title: "7. Manual Browser QA Checklist",
    contentBlocks: [
      {
        type: "orderedList",
        className: "runbook-checklist",
        items: [
          "<strong>Login</strong> — Memberstack on published site / app shell",
          "<strong>/api/me</strong> — <code>dealality.isOwner: true</code>, <code>workspaceAccess</code> includes Owner, <code>pendingApproval: false</code>",
          "<strong>My Deals loads</strong> — no auth errors",
          "<strong>Deal count</strong> matches Airtable scoping",
          "<strong>Open one deal</strong> — Deal Setup / edit",
          "<strong>DRS</strong> — Deal Readiness (authenticated)",
          "<strong>BAS</strong> — Brand Alignment snapshot",
          "<strong>OAS / OCS</strong> — Operator capability/alignment if operator-relevant deal",
          "<strong>Target List / Shortlist</strong> — load and one action",
          "<strong>Attachments</strong> — upload on Contact &amp; Uploads path",
          "<strong>User Management</strong> — 403 if non-admin",
          "<strong>Cross-owner URL</strong> — <code>GET /api/my-deals/{otherOwnerDealId}</code> → 403",
        ],
      },
    ],
  };
}

function sectionFailureModes() {
  return {
    id: "failure-modes",
    title: "8. Common Failure Modes",
    contentBlocks: [
      {
        type: "table",
        headers: ["Symptom", "Check"],
        rows: [
          ["<code>/api/me</code> 401", "Bearer JWT present; <code>MEMBERSTACK_SECRET_KEY</code> matches mode"],
          ["<code>/api/me</code> 403 user_not_found", "Users row exists; Memberstack id or email match"],
          ["Login OK, no deals", "Deal Company Profile and/or User_ID link back; run verify-demo-user-deals.mjs"],
          ["Wrong workspace", "Company Profile → Workspace Access (<code>fldhZqzi0LskI0MpK</code>) — not User Type / Deal Access on Users"],
          ["Sees another owner's deals", "<strong>Critical bug</strong> — run cross-owner test; fix deal scoping"],
          ["Memberstack id mismatch", "Live <code>mem_</code> in Airtable vs sandbox session"],
          ["Live vs sandbox login 401", "Separate passwords; reset Test Mode password or use msToken handoff"],
          ["MS custom fields empty", "Run backfill-memberstack-signup-fields.mjs after manual link"],
          ["Company Profile missing", "User cannot resolve Owner workspace"],
          ["Workspace Access missing", "Falls back to Company Type inference — may fail"],
          ["Multiple Company Profiles", "Deals must match one linked company"],
          ["Owner-Operator hybrid", "Needs Owner + Operator in Workspace Access for both workspaces"],
          ["Demo/Admin leakage", "Demo alone does not pass My Deals; Admin sees all deals — remove from real pilot CPs"],
        ],
      },
    ],
  };
}

function sectionOptionalFutureCleanup() {
  return {
    id: "optional-future-cleanup",
    title: "9. Optional Future Cleanup",
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "These items are <strong>not</strong> blockers for pilot invites. Active standards are in <strong>§1a Pilot Readiness Standards</strong>; Joan baseline is in <strong>§11</strong>.",
      },
      {
        type: "unorderedList",
        items: [
          "Create or refine the Airtable <strong>Pilot Provisioning</strong> view on Users — see <strong>§2b</strong> and <code>reports/users-table-cleanup-audit.md</code>. Hide permission-like and intake columns <strong>from that view only</strong>; do not delete fields.",
          "Review <strong>Deal Access</strong> / <strong>Document Access</strong> in provisioning views — confirm operators are not treating them as workspace permissions (SSOT remains Company Profile → Workspace Access). User Type and Contact Visibility should remain visible in the Pilot Provisioning view for app page routing and platform visibility.",
          "Rename <strong>Unique_Webflow_ID</strong> / <strong>Unique Webflow ID</strong> to <strong>Memberstack Member ID</strong> in Airtable after code/env migration.",
          "Deprecate <strong>Slug</strong> once a cleaner Memberstack Member ID field exists and all references are migrated.",
          "Build internal provisioning UI later.",
          "Build advisor access model later.",
          "Future product review: whether <strong>Deal Access</strong> / region fields on Users should be enforced or consolidated — not a pilot view or delete action today.",
        ],
      },
    ],
  };
}

function sectionFutureAccessModel() {
  return {
    id: "future-access-model",
    title: "10. Future Access Model",
    contentBlocks: [
      {
        type: "table",
        headers: ["Access type", "Source of truth", "Enforced today?"],
        rows: [
          ["<strong>Workspace Access</strong>", "Company Profile", "Yes — route gates"],
          ["<strong>Region Access</strong>", "TBD (fields exist)", "No"],
          ["<strong>Deal Access</strong>", "Deal Company Profile + User_ID", "Yes"],
          ["<strong>Company Profile linkage</strong>", "Users → CP; Deals → CP", "Yes"],
          ["<strong>Advisor access</strong>", "Not modeled", "No"],
          ["<strong>Owner-Operator hybrid</strong>", "CP: Owner + Operator in Workspace Access", "Yes"],
          ["<strong>Brand access</strong>", "CP: Brand; Users brand allow-list", "Brand explorer routes"],
          ["<strong>Operator access</strong>", "CP: Operator + third-party mgmt fields", "Operator routes"],
          ["<strong>Demo access</strong>", "CP: Demo — UI preview only", "Partial (not My Deals alone)"],
          ["<strong>Admin access</strong>", "CP: Admin or admin role tokens", "Yes — all deals"],
        ],
      },
      {
        type: "paragraph",
        html:
          "<strong>Memberstack</strong> = authentication only. <strong>Airtable Company Profile</strong> = permissions. <strong>Airtable Deals</strong> = deal-level scoping.",
      },
    ],
  };
}

function sectionJoanPilotReference() {
  return {
    id: "joan-pilot-reference",
    title: "11. Joan Clean Baseline",
    contentBlocks: [
      {
        type: "paragraph",
        html:
          "<strong>Joan is the clean internal admin-owner pilot reference account</strong> (AO Hospitality Advisors). She meets every standard in <strong>§1a Pilot Readiness Standards</strong> — use this record as the template for new owner pilots.",
      },
      {
        type: "alert",
        variant: "success",
        html:
          "<strong>Verification:</strong> <code>node scripts/verify-pilot-user-by-email.mjs --email joan@aohospitalityadvisors.com</code> exits <strong>0 with zero warnings</strong>.",
      },
      {
        type: "table",
        headers: ["Item", "Value"],
        rows: [
          ["Email", "<code>joan@aohospitalityadvisors.com</code>"],
          ["Users record", "<code>recNemUemQ98o6NSA</code>"],
          ["Company Profile", "AO Hospitality Advisors — <code>recfkFTlz8UeSbQrD</code>"],
          ["Live Memberstack ID", "<code>mem_cmqdv53pi00bf0suj25u42l46</code>"],
          ["Unique_Webflow_ID / Unique Webflow ID", "<code>mem_cmqdv53pi00bf0suj25u42l46</code> (synced)"],
          ["Slug (mirror)", "<code>mem_cmqdv53pi00bf0suj25u42l46</code> (synced)"],
          ["Account Status", "<code>Active</code>"],
          ["Workspace Access", "Admin + Owner (Company Profile → Workspace Access)"],
          ["Deals visible", "8"],
          ["Deals via Company Profile", "8 / 8"],
          ["User_ID-only deals", "0 (User_ID links retained as optional secondary scoping)"],
          ["Missing Company Profile on deals", "0"],
          ["Test Memberstack ID", "<code>mem_sb_cmnr4lcid042q0sqbav1uffdr</code> — localhost only; never on prod Users row"],
          ["Localhost", "Reset Test Mode password in Memberstack, or production login + <code>msToken</code> handoff"],
        ],
      },
      {
        type: "paragraph",
        html:
          "Provisioning sequence used: link Memberstack id → set Account Status Active → backfill deal Company Profile links → verify with zero warnings.",
      },
    ],
  };
}

/** @returns {string[]} Section title substrings for auth tests */
export function getOwnerPilotRunbookExpectedSectionTitles() {
  return getOwnerPilotProvisioningRunbook().sections.map((s) => s.title);
}
