/**
 * Brand-specific overview.scenario bodies for CHI stub profiles (no tier1 overview block).
 * Keys: canonical profile name from choice-chi-brand-resolve / buildStubProfile.
 */

/** @type {Record<string, string[]>} */
export const CHI_STUB_SCENARIO_BODIES = {
  "Park Plaza (Choice)": [
    "Independent or full-service conversion seeking upscale Park Plaza retail—stylish rooms, F&B, and meetings with Choice Privileges and enterprise distribution.",
    "Gateway capitals and mixed-use hubs where upscale full-service ADR supports Park Plaza public space and dining investment—confirm CALA authorization and prototype in LOI (heritage is global; Americas pipeline is selective).",
    "Multi-property sponsors aligning upscale full-service with Radisson-family siblings—Park Plaza carries higher F&B and design burden than Park Inn or core Radisson; tier discipline across the portfolio matters.",
  ],
  "Radisson Collection  (Choice)": [
    "Iconic luxury or upper-upscale independent seeking collection positioning—hand-selected assets with distinctive design, not open enrollment on a midscale prototype.",
    "CALA urban or resort settings where collection tier supports premium ADR—benchmark Individuals and Blu comps in the same market; confirm collection agreement scope versus softer collection tiers.",
    "Luxury-collection portfolio owners curating unique properties—each hotel keeps its character while sharing Choice systems; collection QA and PIP scope differ materially from core Radisson flags.",
  ],
  "Radisson Inn & Suites": [
    "New-build or conversion along corporate corridors and airport nodes—upper-midscale Radisson family box for owners outgrowing tired midscale flags without full-service capex.",
    "Andean and Central American gateways where upper-midscale Radisson family supply is thin—study Park Inn and core Radisson CALA comps when Inn & Suites is authorized; validate Item 20 geography before underwriting.",
    "Regional sponsors standardizing upper-midscale across a portfolio—pair Inn & Suites with Park Inn or core Radisson only where asset class and fee stack match; avoid mixing tiers on the same comp set.",
  ],
  "WoodSpring Suites": [
    "Extended-stay new build or conversion on employment, medical, or logistics corridors—weekly mix, in-room kitchen economics, and lean housekeeping versus nightly midscale flags.",
    "U.S. and Canada suburban and highway markets where WoodSpring prototype fits drive-to weekly demand—Item 19 and consumer footprint are Americas-weighted; do not assume CALA listings without LOI confirmation.",
    "Extended-stay portfolio sponsors comparing WoodSpring with MainStay, Suburban, and Everhome—underwrite weekly ADR, kitchen maintenance, and fee stack per brand FDD, not a single CHI extended-stay average.",
  ],
};

/**
 * @param {string} profileName
 * @returns {string[] | null}
 */
export function stubScenarioBodiesForProfile(profileName) {
  return CHI_STUB_SCENARIO_BODIES[profileName] || null;
}
