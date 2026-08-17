const url = 'https://www.dealality.com/old-home?cb=' + Date.now();
fetch(url, { headers: { 'Cache-Control': 'no-cache' } })
  .then((r) => r.text())
  .then((t) => {
    const i = t.indexOf('id="modules"');
    console.log('modules idx', i);
    console.log(t.slice(Math.max(0, i), i + 900));
    console.log('---');
    console.log('tabs css', t.includes('6a6905579687cbd43649cf04'));
    console.log('tabs js', t.includes('6a69058f60171c2764fb0e62') || t.includes('oldhomebenefitstabscdn'));
    console.log('Owner Outcomes', t.includes('Owner Outcomes'));
    console.log('Credible Futures', t.includes('Credible Futures'));
    console.log('badge-left', t.includes('modules-badge-left'));
    console.log('Platform Capabilities', t.includes('Platform Capabilities'));
  })
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
