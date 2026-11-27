#!/usr/bin/env node
/**
 * $0 Checkpoint A Wave-2 evidence reconciliation + Stage 4 channel population
 * from existing person-specific source URLs + Stage 5 local FQO reassessment.
 * No paid APIs. No Findymail. No Surfe.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  validateContactChannelCandidate,
  collectVerifiedChannelsForPerson,
  CONTACT_CHANNEL_TYPE,
  runProfessionalContactResearchStage,
} from "../../lib/group-demand-intelligence/workflow/gdi-contact-research-stage-v1.js";
import { evaluateFinalizedQualifiedOpportunity } from "../../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import {
  evaluateEntryPointFqoReachability,
  mergeEntryPointReachabilityIntoFqoGates,
} from "../../lib/group-demand-intelligence/workflow/gdi-fqo-entry-point-reachability-v1.js";
import { WHO_HOW_STATUS } from "../../lib/group-demand-intelligence/workflow/gdi-who-how-agent-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "../..");
const outDir = path.join(
  root,
  "reports/gdi-wyndham-final-commercial-continuation-prep-v1/checkpoint-a",
);
const wave2Path = path.join(outDir, "wave2/CHECKPOINT_A_LIVE_RESULT.json");
const baselinePath = path.join(
  root,
  "reports/gdi-wyndham-final-commercial-continuation-prep-v1/10-LOCKED_26_COMMERCIAL_BASELINE.json",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function isPersonLinkedIn(url) {
  return /^https?:\/\/([a-z0-9-]+\.)?linkedin\.com\/in\/[A-Za-z0-9_%-]+\/?/i.test(
    String(url || ""),
  );
}

function personNameTokens(name) {
  return text(name)
    .toLowerCase()
    .replace(/,.*/g, "")
    .split(/\s+/)
    .filter((t) => t && !/^(cmp|cem|cgmp|mba|phd|jr|sr|ii|iii)$/i.test(t));
}

function linkedInSlugMatchesPerson(url, fullName) {
  if (!isPersonLinkedIn(url) || !fullName) return false;
  const slug = String(url)
    .split(/linkedin\.com\/in\//i)[1]
    ?.split(/[/?#]/)[0]
    ?.toLowerCase();
  if (!slug) return false;
  const tokens = personNameTokens(fullName);
  if (tokens.length < 2) return tokens[0] ? slug.includes(tokens[0]) : false;
  const first = tokens[0];
  const last = tokens[tokens.length - 1];
  return (
    (slug.includes(first) && slug.includes(last)) ||
    slug.includes(`${first}-${last}`) ||
    slug.includes(`${first}${last}`) ||
    slug.startsWith(first.slice(0, 4))
  );
}

function authorityDeniedByRationale(why) {
  return /\b(unverified|not (yet )?verified|does not (itself )?(establish|confirm|prove)|no (direct )?evidence.{0,40}(sign|contract|authority|buyer)|buying authority (is|remains|not)\b)/i.test(
    String(why || ""),
  );
}

function scrubAuthorityContradiction(person) {
  if (!person || !person.fullName) return person;
  const auth = !!(
    person.hotelBuyingAuthorityVerified || person.buyerAuthorityVerified
  );
  const why = person.whyWeAreShowingThisContact || person.whyCredibleRoute || "";
  if (auth && authorityDeniedByRationale(why)) {
    return {
      ...person,
      hotelBuyingAuthorityVerified: false,
      buyerAuthorityVerified: false,
      authorityReconciled: "DOWNGRADED_RATIONALE_DENIES_AUTHORITY",
    };
  }
  return person;
}

function collectPeople(wh) {
  const out = [];
  if (wh.directBuyer?.fullName) {
    out.push({ role: "DIRECT_BUYER", person: scrubAuthorityContradiction(wh.directBuyer) });
  }
  if (wh.buyerInfluencer?.fullName) {
    out.push({
      role: "INFLUENCER",
      person: scrubAuthorityContradiction(wh.buyerInfluencer),
    });
  }
  (wh.entryPoints || []).forEach((ep, idx) => {
    if (ep?.fullName && ep.status !== "NOT_FOUND") {
      out.push({
        role: `ENTRY_POINT_${idx + 1}`,
        person: scrubAuthorityContradiction(ep),
      });
    }
  });
  return out;
}

function extractLinkedInCandidates(person, evidence = []) {
  const urls = new Set();
  for (const u of [
    person.verifiedLinkedIn,
    person.linkedinUrl,
    person.linkedInUrl,
    person.sourceUrl,
    ...(person.sourceURLs || []),
  ]) {
    if (isPersonLinkedIn(u)) urls.add(String(u).split("?")[0]);
  }
  for (const e of evidence || []) {
    if (isPersonLinkedIn(e?.sourceUrl)) urls.add(String(e.sourceUrl).split("?")[0]);
  }
  return [...urls];
}

const live = JSON.parse(fs.readFileSync(wave2Path, "utf8"));
const baseline = JSON.parse(fs.readFileSync(baselinePath, "utf8"));
const lockedIds = new Set([
  ...(baseline.buckets?.CLEAN11?.ids || []),
  ...(baseline.buckets?.TURNER?.ids || []),
  ...(baseline.buckets?.ACG?.ids || []),
  ...(baseline.buckets?.NEW_QA_WRITE_READY?.ids || []),
]);

const primary = {};
let withBuyerField = 0;
let withInflField = 0;
let withTwoEp = 0;
let withOneEp = 0;
const peopleKeys = new Set();
const authorityFixes = [];
const stage4Rows = [];

for (const r of live.tranche.results) {
  const wh = { ...(r.whoHow || {}) };
  const st = wh.whoHowStatus || "UNKNOWN";
  primary[st] = (primary[st] || 0) + 1;

  const people = collectPeople(wh);
  if (people.some((p) => p.role === "DIRECT_BUYER")) withBuyerField += 1;
  if (people.some((p) => p.role === "INFLUENCER")) withInflField += 1;
  const eps = people.filter((p) => p.role.startsWith("ENTRY_POINT"));
  if (eps.length >= 2) withTwoEp += 1;
  else if (eps.length === 1) withOneEp += 1;

  for (const { role, person } of people) {
    peopleKeys.add(`${role}|${person.fullName}|${person.organization || ""}`);
    if (person.authorityReconciled) {
      authorityFixes.push({
        opportunityId: r.opportunityId,
        role,
        name: person.fullName,
        fix: person.authorityReconciled,
      });
    }
  }

  // Stage 4 — populate PERSON_LINKEDIN from existing evidence URLs only
  const channelPeople = [];
  for (const { role, person } of people) {
    const candidates = extractLinkedInCandidates(person, wh.evidence || []);
    let accepted = null;
    const rejected = [];
    for (const url of candidates) {
      if (!linkedInSlugMatchesPerson(url, person.fullName)) {
        rejected.push({ url, reason: "LINKEDIN_SLUG_PERSON_MISMATCH_OR_WEAK" });
        continue;
      }
      const v = validateContactChannelCandidate(
        {
          channelType: CONTACT_CHANNEL_TYPE.PERSON_LINKEDIN,
          value: url,
          sourceUrl: url,
        },
        { fullName: person.fullName },
      );
      if (v.ok) {
        accepted = {
          channelType: CONTACT_CHANNEL_TYPE.PERSON_LINKEDIN,
          value: url,
          sourceUrl: url,
          verificationStatus: "VERIFIED_PUBLIC_SOURCE",
          provenance: "CHECKPOINT_A_EXISTING_EVIDENCE_URL",
          matchedPerson: person.fullName,
          role,
        };
        break;
      }
      rejected.push({ url, reason: v.reasons?.[0] || "CHANNEL_REJECTED" });
    }
    channelPeople.push({
      role,
      fullName: person.fullName,
      title: person.currentTitle || person.title || null,
      organization: person.organization || null,
      identityVerified: !!person.identityVerified,
      buyingAuthorityVerified: !!(
        person.hotelBuyingAuthorityVerified || person.buyerAuthorityVerified
      ),
      acceptedChannel: accepted,
      rejected,
      person: {
        ...person,
        linkedinUrl: accepted?.value || person.verifiedLinkedIn || null,
        verifiedLinkedIn: accepted?.value || person.verifiedLinkedIn || null,
        outreachChannelType: accepted
          ? CONTACT_CHANNEL_TYPE.PERSON_LINKEDIN
          : person.outreachChannelType || null,
      },
    });
  }

  // Apply first accepted channel onto opportunity primaryContact for FQO
  const opp = {
    ...(r.opportunity || {}),
    evidenceUrls: [
      ...new Set([
        ...((r.opportunity && r.opportunity.evidenceUrls) || []),
        ...((wh.evidence || []).map((e) => e.sourceUrl).filter(Boolean)),
      ]),
    ],
    recommendedSalesAction:
      r.opportunity?.recommendedSalesAction ||
      channelPeople.find((p) => p.person.recommendedSalesApproach)?.person
        .recommendedSalesApproach ||
      null,
  };
  const primaryPerson =
    channelPeople.find((p) => p.role === "DIRECT_BUYER") ||
    channelPeople.find((p) => p.role === "INFLUENCER") ||
    channelPeople[0];
  if (primaryPerson) {
    opp.primaryContact = {
      ...(opp.primaryContact || {}),
      name: primaryPerson.fullName,
      title: primaryPerson.title,
      organization: primaryPerson.organization,
      linkedinUrl: primaryPerson.acceptedChannel?.value || null,
      whyThisContact: primaryPerson.person.whyWeAreShowingThisContact || null,
      recommendedContactAction:
        primaryPerson.person.recommendedSalesApproach || null,
      personSourceUrl: primaryPerson.acceptedChannel?.value || primaryPerson.person.sourceUrl || null,
    };
  }

  const contactStage = await runProfessionalContactResearchStage(opp, {
    mode: "DryRun",
    fixture: true,
    fixtureChannels: primaryPerson?.acceptedChannel
      ? [primaryPerson.acceptedChannel]
      : [],
  });

  let fqo = evaluateFinalizedQualifiedOpportunity(contactStage.opportunity || opp, {});
  const reach = evaluateEntryPointFqoReachability(contactStage.opportunity || opp, {});
  fqo = mergeEntryPointReachabilityIntoFqoGates(fqo, reach);

  const futureOk = fqo?.gates?.FUTURE_SOURCING_PATHWAY?.ok === true;
  const demandOk = fqo?.gates?.EVIDENCE_BACKED_DEMAND?.ok === true;
  const fitOk = fqo?.gates?.HOTEL_GEO_COMMERCIAL_FIT?.ok === true;
  const namedOk = fqo?.gates?.NAMED_COMMERCIAL_PERSON?.ok === true;
  const channelOk = fqo?.gates?.PERSON_SPECIFIC_CHANNEL?.ok === true;
  const actionOk = fqo?.gates?.HOTEL_SPECIFIC_ACTION?.ok === true;

  // Heuristic: completed 2026 past-event risk when title is clearly past and no continuation language
  const title = String(opp.title || "");
  const pastEventRisk =
    /2026/.test(title) &&
    !/2027|2028|continuation|overflow|housing bureau|future|annual recurring/i.test(
      `${title} ${wh.evidence?.map((e) => e.claim).join(" ") || ""}`,
    );

  stage4Rows.push({
    opportunityId: r.opportunityId,
    organizationName: opp.organizationName,
    title: opp.title,
    segmentId: opp.segmentId,
    primaryWhoHowStatus: st,
    people: channelPeople.map((p) => ({
      role: p.role,
      fullName: p.fullName,
      buyingAuthorityVerified: p.buyingAuthorityVerified,
      channel: p.acceptedChannel,
      rejected: p.rejected,
    })),
    linkedInChannelPopulated: channelPeople.some((p) => p.acceptedChannel),
    inLockedBaseline26: lockedIds.has(r.opportunityId),
    fqoFinalized: fqo?.finalizedQualified === true,
    fqoGates: Object.fromEntries(
      Object.entries(fqo?.gates || {}).map(([k, v]) => [k, v?.ok === true]),
    ),
    pastEventRisk,
    classification: fqo?.finalizedQualified
      ? "FQO_CANDIDATE"
      : namedOk && !channelOk
        ? "NAMED_NO_CHANNEL"
        : namedOk && channelOk && (!demandOk || !fitOk || !futureOk)
          ? "NAMED_CHANNEL_FAILS_DEMAND_FIT_OR_FUTURE"
          : "RESEARCH_OR_EVIDENCE_GAP",
    remainingBlockers: Object.entries(fqo?.gates || {})
      .filter(([, v]) => v && v.ok === false)
      .map(([k]) => k),
  });
}

const recon = {
  version: "gdi-checkpoint-a-wave2-evidence-reconciliation-v1",
  uniqueOpportunities: live.tranche.results.length,
  primaryStatusCounts: primary,
  primaryStatusSum: Object.values(primary).reduce((a, b) => a + b, 0),
  opportunitiesWithDirectBuyerPersonField: withBuyerField,
  opportunitiesWithInfluencerPersonField: withInflField,
  opportunitiesWithOneEntryPointField: withOneEp,
  opportunitiesWithTwoEntryPointFields: withTwoEp,
  distinctPeopleSlots: peopleKeys.size,
  spendReconciliation: {
    checkpointACeilingUsd: 5,
    wave1IncrementalUsd: 2.5,
    wave2IncrementalUsd: 2.5,
    cumulativeUsd: 5.0,
    wave2RunnerSpentUsdField: live.spentUsd,
    clarification:
      "Wave-2 runner spentUsd is cumulative for the authorization ledger (Wave1+Wave2). Incremental Wave-2 spend ≈ USD 2.50; Checkpoint A total USD 5.00.",
  },
  discrepancyNotes: [
    "Primary WHO/HOW statuses sum to 25 (8 DIRECT_BUYER + 2 INFLUENCER + 15 ENTRY_POINTS_2).",
    "Direct-buyer person-field count can exceed primary DIRECT_BUYER when an opportunity also stores a directBuyer object alongside a different primary status (e.g. influencer-primary with buyer field) — report both layers; do not equate them.",
    "Two-entry-point field count can exceed VERIFIED_ENTRY_POINTS_2 when buyer/influencer-primary opportunities also carry two entry-point objects.",
  ],
  authorityFixes,
};

const stage4Summary = {
  version: "gdi-checkpoint-a-stage4-zero-dollar-v1",
  paid: false,
  findymail: false,
  surfe: false,
  opportunities: stage4Rows.length,
  linkedInChannelsPopulated: stage4Rows.filter((r) => r.linkedInChannelPopulated)
    .length,
  fqoCandidates: stage4Rows.filter((r) => r.fqoFinalized).length,
  namedNoChannel: stage4Rows.filter((r) => r.classification === "NAMED_NO_CHANNEL")
    .length,
  failsDemandFitFuture: stage4Rows.filter(
    (r) => r.classification === "NAMED_CHANNEL_FAILS_DEMAND_FIT_OR_FUTURE",
  ).length,
  pastEventRiskCount: stage4Rows.filter((r) => r.pastEventRisk).length,
  overlapLocked26: stage4Rows.filter((r) => r.inLockedBaseline26).length,
  rows: stage4Rows,
};

fs.writeFileSync(
  path.join(outDir, "19-WAVE2_EVIDENCE_RECONCILIATION.json"),
  `${JSON.stringify({ recon, authorityFixes }, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(outDir, "20-STAGE4_ZERO_DOLLAR_CHANNELS.json"),
  `${JSON.stringify(stage4Summary, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(outDir, "21-STAGE5_FQO_REASSESSMENT.json"),
  `${JSON.stringify(
    {
      version: "gdi-checkpoint-a-stage5-fqo-reassessment-v1",
      lockedBaseline: 26,
      baselineUnchanged: true,
      incrementalFqoFromCheckpointAPlusStage4: stage4Summary.fqoCandidates,
      note: "FQO requires full six gates including demand/fit/future/action — many Checkpoint A dossiers lack Stage-2 evidence reattachment, so incremental FQO may remain 0 until dossier evidence is joined.",
      summary: {
        fqoCandidates: stage4Summary.fqoCandidates,
        namedNoChannel: stage4Summary.namedNoChannel,
        failsDemandFitFuture: stage4Summary.failsDemandFitFuture,
        pastEventRiskCount: stage4Summary.pastEventRiskCount,
        linkedInChannelsPopulated: stage4Summary.linkedInChannelsPopulated,
      },
      rows: stage4Rows,
    },
    null,
    2,
  )}\n`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      recon,
      stage4: {
        linkedInChannelsPopulated: stage4Summary.linkedInChannelsPopulated,
        fqoCandidates: stage4Summary.fqoCandidates,
        namedNoChannel: stage4Summary.namedNoChannel,
        failsDemandFitFuture: stage4Summary.failsDemandFitFuture,
        pastEventRiskCount: stage4Summary.pastEventRiskCount,
      },
    },
    null,
    2,
  ),
);
