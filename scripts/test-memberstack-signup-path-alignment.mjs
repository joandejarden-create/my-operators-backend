/**
 * Signup path — identity-only writes (no Airtable API).
 *   node scripts/test-memberstack-signup-path-alignment.mjs
 */
import assert from "node:assert/strict";
import {
  AUTH_ROLE_HINT_FIELD_CANDIDATES,
  buildSignupUsersPatch,
  USERS_PROTECTED_NEVER_WRITE,
  resolveSignupRoleHint,
  writeUsersRecordWithFieldFallback,
} from "../lib/airtable-users-protected-patch.js";

const existingOwner = {
  "User Type": "Owner",
  "Platform Role": "Owner",
  "Workspace Access": ["Owner", "Operator"],
  "Company Profile": ["reccQJUKO2RAY9zhE"],
  Email: "owner@example.com",
};

const onboarding = {
  Email: "owner@example.com",
  "Unique Webflow ID": "mem_test",
  "Company Name": "Acme",
  Title: "CEO",
};

// 1. Existing user re-signup — role Operator must not overwrite User Type / WS / Company Profile
const reSignup = buildSignupUsersPatch(existingOwner, onboarding, {
  isCreate: false,
  roleHint: "Operator",
});
assert.ok(!("User Type" in reSignup.patch));
assert.ok(!("Platform Role" in reSignup.patch));
assert.ok(!("Workspace Access" in reSignup.patch));
assert.ok(!("Company Profile" in reSignup.patch));
assert.equal(reSignup.patch.Email, "owner@example.com");
if (AUTH_ROLE_HINT_FIELD_CANDIDATES.length) {
  const hintKey = AUTH_ROLE_HINT_FIELD_CANDIDATES[0];
  assert.equal(reSignup.patch[hintKey], "Operator");
}

// 2. Demo user re-signup — Demo WS preserved; Owner not granted
const existingDemo = {
  "Workspace Access": ["Demo"],
  "User Type": "Demo",
};
const demoRe = buildSignupUsersPatch(existingDemo, onboarding, {
  isCreate: false,
  roleHint: "Owner",
});
assert.ok(!("Workspace Access" in demoRe.patch));
assert.ok(!("User Type" in demoRe.patch));

// 3. New user signup — identity only; no WS from role
const newUser = buildSignupUsersPatch({}, onboarding, {
  isCreate: true,
  roleHint: "Owner",
});
assert.ok(!("Workspace Access" in newUser.patch));
assert.ok(!("Company Profile" in newUser.patch));
assert.equal(newUser.patch.Email, "owner@example.com");
if (AUTH_ROLE_HINT_FIELD_CANDIDATES.length) {
  assert.equal(newUser.patch[AUTH_ROLE_HINT_FIELD_CANDIDATES[0]], "Owner");
} else {
  assert.equal(newUser.patch["User Type"], "Owner");
}

// resolveSignupRoleHint prefers role over companyType
assert.equal(resolveSignupRoleHint({ role: "Operator", companyType: "Owner" }), "Operator");

// Protected set includes permission-like keys
assert.ok(USERS_PROTECTED_NEVER_WRITE.has("Workspace Access"));
assert.ok(USERS_PROTECTED_NEVER_WRITE.has("Permission Level"));

// 4. Unknown Auth Role Hint field — write fallback strips and succeeds
let createAttempts = 0;
const mockBase = () => ({
  create: async (fields) => {
    createAttempts += 1;
    if (fields["Auth Role Hint"]) {
      const err = new Error('Unknown field name: "Auth Role Hint"');
      err.statusCode = 422;
      throw err;
    }
    return { id: "recNEW123", fields };
  },
  update: async () => {
    throw new Error("unexpected update");
  },
});

const record = await writeUsersRecordWithFieldFallback(mockBase, "tblTest", {
  isCreate: true,
  recordId: null,
  fields: {
    Email: "new@example.com",
    "Auth Role Hint": "Operator",
    "Company Name": "Co",
  },
});
assert.equal(record.id, "recNEW123");
assert.equal(createAttempts, 2);
assert.ok(!record.fields?.["Auth Role Hint"]);

console.log("test-memberstack-signup-path-alignment: ok");
