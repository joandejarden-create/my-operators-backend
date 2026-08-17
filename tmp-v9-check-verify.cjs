const fs = require('fs');
const t = fs.readFileSync(
  'C:/Users/joand/.cursor/projects/c-Users-joand-OneDrive-Documents-deal-capture-proxy/agent-tools/70aabbe7-2709-48cf-8f8b-977b2a3f35af.txt',
  'utf8'
);
const checks = {
  len: t.length,
  how: t.includes('From First Research'),
  aud: t.includes('Built for Every Stakeholder'),
  faq: t.includes('Frequently Asked Questions'),
  footer: t.includes('Reduce Friction'),
  cta: t.includes('Stop Comparing Pitches'),
  howId: t.includes('id=\\"how\\"') || t.includes('id="how"'),
  audId: t.includes('id=\\"audiences\\"') || t.includes('id="audiences"'),
  faqId: t.includes('id=\\"faq\\"') || t.includes('id="faq"'),
};
console.log(JSON.stringify(checks, null, 2));
