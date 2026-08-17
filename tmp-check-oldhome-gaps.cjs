async function main() {
  const html = await fetch('https://www.dealality.com/old-home?t=' + Date.now()).then((r) => r.text());
  const checks = {
    audTabs: (html.match(/dc-aud-tab/g) || []).length,
    hotelOwnerLabel: /HOTEL OWNER|Hotel Owner/i.test(html),
    privateBeta: /Private Beta Access/i.test(html),
    whyCards: /Before Outreach|During Evaluation|Deal Readiness|At LOI/.test(html),
    wcards: /dc-wcards|dc-wc\b/.test(html),
    faqSide: /Approach Insights/i.test(html),
    createProfile: /Create Your Profile/i.test(html),
    joinBrand: /Join as a Brand or Operator/i.test(html),
  };
  console.log(checks);

  // sample aud tab text
  const m = html.match(/dc-aud-tab[\s\S]{0,400}/);
  console.log('\naud sample', m && m[0].slice(0, 350));
  const w = html.match(/id="why"[\s\S]{0,1200}/);
  console.log('\nwhy sample', w && w[0].slice(0, 800));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
