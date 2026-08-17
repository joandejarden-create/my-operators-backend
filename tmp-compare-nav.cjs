const urls = [
  'https://www.dealality.com/',
  'https://www.dealality.com/insights',
  'https://my-operators-backend-production.up.railway.app/marketing/dealality-landing-v9-standalone.html',
];

(async () => {
  for (const url of urls) {
    try {
      const t = await (await fetch(url)).text();
      console.log('\n===', url, '===');
      console.log('Insights>', />(Insights|FAQs|For Brands & Operators)</.test(t));
      const nav = t.match(/<nav[\s\S]{0,1800}/);
      if (nav) console.log(nav[0].replace(/\s+/g, ' ').slice(0, 900));
      const links = [...t.matchAll(/>(For Owners|For Brands[^<]*|For Partners|How it Works|How It Works|FAQs?|Insights)</gi)].map((m) => m[1]);
      console.log('links', [...new Set(links)]);
    } catch (e) {
      console.log(url, e.message);
    }
  }
})();
