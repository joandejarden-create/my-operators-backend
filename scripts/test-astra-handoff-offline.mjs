import assert from 'node:assert/strict';
import { researchHotelOwnershipContactPath } from '../lib/hotel-intelligence/contact-intelligence/ownership-contact-research-handoff.js';
globalThis.fetch=()=>{throw Error('NETWORK_FORBIDDEN')};
let queries=[];
globalThis.astraFixture={
 search:async({query})=>{queries.push(query); return {ok:true,data:{results:queries.length===2?[{url:'https://marriott.com/press/sample-transaction',title:'Sample Bay Hotel acquired by Sample Holdings'}]:[]}};},
 scrape:async()=>({ok:true,data:'Navigation\n'.repeat(500)+'Sample Holdings acquired Sample Bay Hotel.\n'}),
};
const result=await researchHotelOwnershipContactPath({hotel:{hotel_id:'synthetic-only',hotel_name:'Sample Bay Hotel',language:'en'},forbidden_org_hosts:['marriott.com']},{serpapi_max:0,context_dev_max:5});
assert.ok(queries.length>=2);
assert.equal(result.ownership.owner_display_name,'Sample Holdings');
assert.equal(result.ownership.classification,'OWNER_CANDIDATE');
assert.equal(result.ownership.return_to_ownership_lane,true);
assert.equal(result.qualifies_for_fullenrich,false);
assert.ok(result.sources.find(s=>s.kind==='context_scrape_ownership').passages[0].start>2000);
assert.ok(result.budgets.context_dev.spent_credits<=5);
assert.equal(result.write_guarantees.canonical_hotel_to_owner_mutations,0);
assert.equal(result.write_guarantees.census_owner_mutations,0);
console.log('PASS synthetic executed handoff: fallback query → brand-host press → late-document candidate; pending ownership, no enrichment, cap <=5.');
// Remote 4xx is not evidence of zero billing; no ledger refund without explicit zero-charge metadata.
globalThis.astraFixture.search=async()=>({ok:false,error:{class:'VALIDATION_OR_CLIENT',status:403}});
const denied=await researchHotelOwnershipContactPath({hotel:{hotel_id:'synthetic-denied',hotel_name:'Other Bay Hotel'}},{serpapi_max:0,context_dev_max:3});
assert.equal(denied.budgets.context_dev.spent_credits,2);
assert.ok(denied.unresolved_reasons.includes('CONTEXT_SEARCH_FAILED'));
console.log('PASS synthetic executed handoff: remote 403 retained as reserved cost and execution failure.');
