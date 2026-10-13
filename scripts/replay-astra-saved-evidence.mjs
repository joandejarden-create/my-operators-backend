/** Saved REPORT passage/row replay only; not raw provider or end-to-end replay. */
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { gateProviderCandidates } from '../lib/hotel-intelligence/contact-intelligence/fullenrich-gated-submit.js';
import { ownershipPassageSupport, classifyContactPurpose } from '../lib/hotel-intelligence/contact-intelligence/research-evidence.js';
const dir=process.argv[2];
if(!dir) throw Error('Usage: node scripts/replay-astra-saved-evidence.mjs <saved-evidence/reports>');
const read=(name)=>JSON.parse(fs.readFileSync(path.join(dir,name),'utf8'));
const completion=read('ci-20hotel-e2e-completion-follow-up.json');
const results=read('ci-20hotel-e2e-results.json');
const original=read('ci-20hotel-e2e-results.original-pass.json');
const chic=completion.ownership_review.find(r=>r.hotel_id==='recqfvmhe4GyqdsiD');
assert.ok(chic.passage.includes('portfolio'));
assert.equal(ownershipPassageSupport(chic.passage,'Royalton CHIC Antigua','Blue Diamond Resorts'),false);
const replay=(results.development_improvement_pass_v3?.surfe?.email_results || []).map(p=>{
 const c={hotel_name:p.hotels?.[0],hotel_to_owner:{supported:true,relationship_class:'ECONOMIC_OWNER_OR_SPONSOR',evidence_refs:['prior_ownership']},person:{display_name:p.full_name,title:p.job_title,identity_supported:true},organization:{name:p.organization},identifiers:{domain:{value:p.domain,status:'CONFIRMED'}},affiliation_corroboration:{status:'SURFE_ONLY',source_class:'SURFE_ONLY',evidence_refs:[]}};
 const g=gateProviderCandidates([c],{provider:'surfe'});
 assert.equal(g.allowed.length,0);
 return {person:p.full_name,rejected:g.rejected[0].gate.violations.map(v=>v.code)};
});
assert.equal(replay.length,5);
// Address itself is explicitly recorded in supplied completion source (report sanitation may replace it).
assert.equal(classifyContactPurpose('DEMedia@dart.ky'),'MEDIA_PURPOSE');
const out={replay_kind:'SAVED_REPORT_ROWS_AND_PASSAGES_NOT_RAW_E2E',reported_baseline_preserved:{original:'0/20 owners',improvement:'5/20 reported owners',completion:'1/20 reported complete chains'},chic:{source:chic.source_url,passage:chic.passage,audit:'PORTFOLIO_ASSOCIATION_ONLY_IN_SAVED_PASSAGE'},prior_surfe_rows:replay,original_trace_observations:{rows:original.ownership_rows.length,one_credit_or_less:original.ownership_rows.filter(r=>r.context_credits_used<=1).length,full_raw_search_responses_available:false},live_accuracy_measured:false};
console.log(JSON.stringify(out,null,2));
