const t = await (await fetch("https://www.dealality.com/old-home")).text();
const navChunk = t.match(/id="nav"[\s\S]{0,800}/);
console.log(navChunk ? navChunk[0] : "no nav");
console.log("---style tag---", /dc-pnav\{/.test(t) || /dc-pnav {/.test(t));
console.log("premium class", /dc-page--premium/.test(t));
console.log("dc-ph1", /dc-ph1/.test(t));
console.log("dc-pbtn", /dc-pbtn/.test(t));
