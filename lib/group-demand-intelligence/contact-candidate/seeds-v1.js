/**
 * Evidence-backed candidate seeds for Bethesda GDI discovery (WHO only).
 * $0 Surfe / $0 paid enrichment. Do not invent names — every seed needs sourceUrl.
 * Keys = opportunity id.
 */

const VERIFIED = "2026-09-15";

/**
 * @type {Record<string, Array<object>>}
 */
export const CONTACT_CANDIDATE_SEEDS_V1 = Object.freeze({
  // Association meetings — public role inbox only; no named planner published.
  // Seeds intentionally empty → engine must return UNRESOLVED named contact.
  gdi_opp_amwa_2027_annual: [],
  gdi_opp_amwa_2027_interim: [],
  gdi_opp_amwa_2028_annual: [],

  // Loudoun — same HBC housing path as other MD tournaments (already in pack);
  // add tournament ops backup if known from public sanctions lists later.

  // Georgetown Advancement Events — org channel only (named planner not public).
  gdi_opp_georgetown_homecoming_2027: [],

  // Corporate HQ-adjacent watch — no evidence of a named meetings owner → unresolved.
  gdi_opp_marriott_hq_adjacent_corporate_watch: [],

  // AJAS Rockville HQ watch — no public event contact for hotel sourcing.
  gdi_opp_ajas_hq_rockville_watch: [],

  // AHIMA Advocacy — no published named hotel contact in current corpus.
  gdi_opp_ahima_advocacy_2027: [],

  // CMSS Spring — no named meetings owner in current corpus.
  gdi_opp_cmss_spring_2027: [],

  // ACC Legislative — no named housing contact in current corpus.
  gdi_opp_acc_legislative_2027: [],

  // NDSS Advocacy — unresolved until official staff page names a meetings owner.
  gdi_opp_ndss_advocacy_2027: [],

  // ASAE — service inbox only until a named annual-meeting planner is published.
  gdi_opp_asae_annual_2029: [],

  // AAD / ECS / WBC overflow — housing channel TBD; do not invent housing bureau contacts.
  gdi_opp_aad_2028_overflow: [],
  gdi_opp_ecs_251_2027_overflow: [],
  gdi_opp_world_biomaterials_2028_overflow: [],

  // NIH SBPO — program mailbox only.
  gdi_opp_nih_sbpo_vos_pattern: [],

  /**
   * Example successor / historical patterns used by tests & reactivation logic demos.
   * Not applied to live Bethesda pack unless opportunity id matches.
   */
  __fixture_reactivation_prior_planner: [
    {
      name: "Alex Priorplanner",
      role: "Director of Meetings",
      organization: "Example Association",
      claimKind: "FACT",
      sourceUrl: "https://example.org/meetings/staff",
      relationshipToEvent: "Named meetings director for prior-year annual meeting; still employed",
      priorEventInvolvement: true,
      historicalOnly: true,
      stillEmployed: true,
      reactivationBoost: true,
      targetRoleMatch: "EVENT_MEETINGS_OWNER",
      gdiContactRole: "MEETINGS_OWNER",
      whyThisPerson:
        "Served as Director of Meetings for the prior event cycle and remains listed on the association staff page.",
      firstSeenAt: VERIFIED,
      lastVerifiedAt: VERIFIED,
    },
  ],
});

export const CONTACT_CANDIDATE_DISCOVERY_PASS_ID = "gdi_contact_candidate_discovery_v1";
