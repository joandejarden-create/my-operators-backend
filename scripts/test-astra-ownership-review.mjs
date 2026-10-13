import assert from 'node:assert/strict';
import { gateProviderCandidates, submitFullEnrichWorkEmailsGated } from '../lib/hotel-intelligence/contact-intelligence/fullenrich-gated-submit.js';
import { validateOwnerPersonEnrichmentSubmission as gate, evaluateOwnerPersonReturnedContact as returned, isRoleRelevantForOwnerContact } from '../lib/hotel-intelligence/contact-intelligence/owner-person-enrichment-gate.js';
import { ownershipQueries, ownershipSearchResults, rankOwnershipSources, ownershipDocumentPassages, ownershipCandidates } from '../lib/hotel-intelligence/contact-intelligence/ownership-research-planning.js';
import { ownershipPassageSupport, classifyContactPurpose, classifyOwnershipLead } from '../lib/hotel-intelligence/contact-intelligence/research-evidence.js';
let count=0;
function test(name,fn) {fn();count++; console.log('PASS synthetic:',name);}
const ref=(url,excerpt)=>({source_url:url,excerpt,source_type:'COMPANY_ANNOUNCEMENT'});
function fixture(){return {hotel_name:'Sample Bay Hotel',person:{display_name:'Alex Rivera',title:'Director of Acquisitions'},organization:{name:'Sample Holdings'},identifiers:{domain:{value:'sample.example',status:'CONFIRMED',evidence_refs:[ref('https://sample.example/about','Sample Holdings is our company name and this is our official website.')]}},hotel_to_owner:{relationship_class:'ECONOMIC_OWNER_OR_SPONSOR',evidence_refs:[ref('https://news.example/deal','Sample Holdings acquired Sample Bay Hotel.')]},affiliation_corroboration:{source_class:'COMPANY_ANNOUNCEMENT',evidence_refs:[ref('https://sample.example/team','Alex Rivera is Director of Acquisitions at Sample Holdings.')]}};}
test('captured ownership/affiliation/domain positive',()=>assert.equal(gate(fixture()).ok,true));
test('URLs and labels alone rejected',()=>{const c=fixture();c.hotel_to_owner={supported:true,relationship_class:'ECONOMIC_OWNER_OR_SPONSOR',evidence_refs:['https://news.example/deal']};assert.equal(gate(c).ok,false);});
test('different hotel rejected',()=>{const c=fixture();c.hotel_name='Other Bay Hotel';assert.equal(gate(c).ok,false);});
test('different employer rejected',()=>{const c=fixture();c.affiliation_corroboration.evidence_refs[0].excerpt='Alex Rivera is Director of Acquisitions at Other Holdings.';assert.equal(gate(c).ok,false);});
test('former affiliation rejected',()=>{const c=fixture();c.affiliation_corroboration.evidence_refs[0].excerpt='Alex Rivera is former Director of Acquisitions at Sample Holdings.';assert.equal(gate(c).ok,false);});
test('domain confirmed flag cannot substitute contents',()=>{const c=fixture();delete c.identifiers.domain.evidence_refs;assert.equal(gate(c).ok,false);});
test('unrelated site cannot confirm domain',()=>{const c=fixture();c.identifiers.domain.evidence_refs[0].source_url='https://directory.example/company';assert.equal(gate(c).ok,false);});
test('affiliation refs cannot pay ownership evidence debt',()=>{const c=fixture();c.hotel_to_owner.evidence_refs=[];c.hotel_to_owner.supported=true;assert.equal(gate(c).ok,false);});
test('training development excluded even with role flag',()=>{const c=fixture();c.person.title='Training and Development Manager';c.affiliation_corroboration.role_relevant=true;assert.equal(gate(c).ok,false);assert.equal(isRoleRelevantForOwnerContact(c.person.title),false);});
test('omitted workflow fails closed',()=>{const c=fixture();delete c.hotel_to_owner;delete c.affiliation_corroboration;assert.equal(gateProviderCandidates([c]).allowed.length,0);});
test('row cannot downgrade explicit owner scope',()=>{const c=fixture();c.workflow='hotel_contact';delete c.affiliation_corroboration;assert.equal(gateProviderCandidates([c],{workflow:'owner_person_enrichment'}).allowed.length,0);});
test('caller cannot downgrade owner row',()=>{const c=fixture();c.workflow='owner_person_enrichment';delete c.affiliation_corroboration;assert.equal(gateProviderCandidates([c],{workflow:'hotel_contact'}).allowed.length,0);});
test('unknown workflow cannot bypass',()=>{const c=fixture();delete c.hotel_to_owner;delete c.affiliation_corroboration;assert.equal(gateProviderCandidates([c],{workflow:'typo'}).allowed.length,0);});
test('gdi explicit scope retains base behavior',()=>{const c=fixture();delete c.hotel_to_owner;delete c.affiliation_corroboration;assert.equal(gateProviderCandidates([c],{workflow:'gdi_contact'}).allowed.length,1);});
const result={personName:'Alex Rivera',providerFullName:'Alex Rivera',targetDomain:'sample.example',providerEmail:'alex@sample.example',providerValidationStatus:'VALID'};
test('VALID/domain alone is not affiliation',()=>assert.equal(returned(result).accept_person_attributed_email,false));
test('qualified and VALID accepted internally',()=>assert.equal(returned({...result,qualificationInput:fixture()}).accept_person_attributed_email,true));
test('INVALID email not accepted',()=>assert.equal(returned({...result,qualificationInput:fixture(),providerValidationStatus:'INVALID'}).accept_person_attributed_email,false));
test('media route rejected even for qualified person',()=>assert.equal(returned({...result,qualificationInput:fixture(),providerEmail:'DEMedia@sample.example'}).contact_class,'MEDIA_PURPOSE'));
test('similar first name is not same person',()=>assert.equal(returned({...result,qualificationInput:fixture(),providerFullName:'Alexis Rivera'}).contact_class,'RETURNED_WRONG_PERSON'));
test('portfolio/opening does not entail ownership',()=>assert.equal(ownershipPassageSupport('Sample Bay Hotel opens as the newest addition to Sample Holdings portfolio.','Sample Bay Hotel','Sample Holdings'),false));
test('proposed acquisition does not entail ownership',()=>assert.equal(ownershipPassageSupport('Sample Holdings plans to acquire Sample Bay Hotel.','Sample Bay Hotel','Sample Holdings'),false));
test('hotel core name binds Cap Juluca acquisition',()=>assert.equal(ownershipPassageSupport('Belmond acquired Cap Juluca in 2017.','Cap Juluca A Belmond Hotel Anguilla','Belmond'),true));
test('acquired the property binds when hotel named in excerpt',()=>assert.equal(ownershipPassageSupport('Cap Juluca reopened. In 2017, Belmond acquired the property and renovated.','Cap Juluca A Belmond Hotel Anguilla','Belmond'),true));
test('operator self-description negative control',()=>{
  const lead=classifyOwnershipLead('Auberge Resorts Collection, internationally renowned owner and operator of boutique luxury hotels, has rebranded Malliouhana.','Malliouhana An Auberge Resort Anguilla');
  assert.ok(lead.reasons.includes('OPERATOR_SELF_DESCRIPTION'));
  assert.equal(lead.allow_owner_candidate,false);
});
test('decor metaphor negative control',()=>{
  const lead=classifyOwnershipLead('Guest rooms have the feel of a stylish residence owned by a glamorous world traveler.','Malliouhana An Auberge Resort Anguilla');
  assert.ok(lead.reasons.includes('DECOR_OR_MARKETING_METAPHOR'));
  const d=ownershipDocumentPassages('Malliouhana suites. Guest rooms have the feel of a stylish residence owned by a glamorous world traveler.',{hotel_name:'Malliouhana An Auberge Resort Anguilla'});
  const c=ownershipCandidates(d,{hotel_name:'Malliouhana An Auberge Resort Anguilla'});
  assert.ok(c.every(x=>x.classification==='REJECTED_NEGATIVE_CONTROL'||!x.name));
});
test('AJ Capital acquisition by pattern',()=>{
  const text='Malliouhana Hotel & Spa reopened following acquisition by AJ Capital Partners and an 18-month redesign.';
  assert.ok(classifyOwnershipLead(text,'Malliouhana An Auberge Resort Anguilla').reasons.includes('TRANSACTION_ACQUISITION_LANGUAGE'));
  const d=ownershipDocumentPassages(text,{hotel_name:'Malliouhana An Auberge Resort Anguilla'});
  const c=ownershipCandidates(d,{hotel_name:'Malliouhana An Auberge Resort Anguilla'});
  assert.ok(c.some(x=>x.name==='AJ Capital Partners'));
  assert.equal(ownershipPassageSupport(text,'Malliouhana An Auberge Resort Anguilla','AJ Capital Partners'),true);
  assert.ok(c.some(x=>x.supported_transaction_claim===true));
});
test('supported_transaction_claim is not current ownership',()=>{
  const d=ownershipDocumentPassages('In 2017, Belmond acquired Cap Juluca.',{hotel_name:'Cap Juluca A Belmond Hotel Anguilla'});
  const c=ownershipCandidates(d,{hotel_name:'Cap Juluca A Belmond Hotel Anguilla'});
  assert.ok(c.some(x=>x.name==='Belmond'&&x.supported_transaction_claim===true&&x.supported_ownership===true));
});
test('guide domains filtered from rank',()=>{
  const ranked=rankOwnershipSources([
    {url:'https://www.wikihow.com/Find-the-Owner-of-an-LLC',title:'How to find owners',snippet:'ownership guide'},
    {url:'https://news.example/deal',title:'Sample Bay Hotel acquired by Sample Holdings',snippet:'acquired'},
  ],{hotel_name:'Sample Bay Hotel'});
  assert.equal(ranked.length,1);
  assert.match(ranked[0].url,/news\.example/);
});
test('whole document reads transaction after character 2000',()=>{const text='Navigation\n'.repeat(500)+'Sample Holdings acquired Sample Bay Hotel.\n';const d=ownershipDocumentPassages(text,{hotel_name:'Sample Bay Hotel'});assert.ok(d.passages.some(p=>p.start>2000));assert.equal(ownershipCandidates(d,{hotel_name:'Sample Bay Hotel'})[0].name,'Sample Holdings');});
test('press on brand host stays evidence candidate',()=>assert.equal(rankOwnershipSources([{url:'https://brand.example/press/deal',title:'Sample Bay Hotel acquired by Sample Holdings',snippet:''}],{hotel_name:'Sample Bay Hotel'}).length,1));
test('malformed response not treated as empty research',()=>{assert.equal(ownershipSearchResults({wrong:[]}),null);assert.deepEqual(ownershipSearchResults({results:[]}),[]);});
test('Portuguese query and suffix normalization',()=>{const q=ownershipQueries({hotel_name:'Sample Bay, an Autograph Collection All-Inclusive Resort',language:'pt',country:'Brazil'});assert.match(q[0],/proprietário/);assert.ok(q[0].startsWith('"Sample Bay"'));});
test('borrowed qualification cannot qualify different person',()=>assert.equal(returned({...result,personName:'Jordan Blake',providerFullName:'Jordan Blake',qualificationInput:fixture()}).accept_person_attributed_email,false));
test('no returned identity is not accepted solely by domain',()=>assert.equal(returned({...result,providerFullName:null,qualificationInput:fixture()}).accept_person_attributed_email,false));
test('single long paragraph retains late ownership statement',()=>{const d=ownershipDocumentPassages('Navigation. '.repeat(600)+'Sample Holdings acquired Sample Bay Hotel.',{hotel_name:'Sample Bay Hotel'});assert.equal(ownershipCandidates(d,{hotel_name:'Sample Bay Hotel'})[0].name,'Sample Holdings');});
const forged=await submitFullEnrichWorkEmailsGated({candidates:[{gate:{ok:true},submit_row:{first_name:'Fake',domain:'wrong.example'}}]});
assert.equal(forged.submitted,false);count++;console.log('PASS synthetic: forged precomputed submit rejected before missing provider import');
console.log(`PASS ${count} new synthetic checks; zero network calls; no live accuracy measured.`);
