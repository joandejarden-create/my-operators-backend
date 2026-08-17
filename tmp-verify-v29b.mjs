const t = await (await fetch("https://www.dealality.com/old-home?v=" + Date.now())).text();
console.log("v29b", t.includes("old-home-problem-storyboard.v20260729b.js"));
console.log("v29a", t.includes("old-home-problem-storyboard.v20260729a.js"));
console.log("v2e", t.includes("old-home-problem-v2.v20260729e.js"));
