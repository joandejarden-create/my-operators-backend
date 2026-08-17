const crypto = require("crypto");
const fs = require("fs");

const css = `/* Insights 3-wide override — exactly N cards fill the track */
#insights-grid,.oh-insights-grid{--ins-gap:2rem!important;--ins-visible:3!important;gap:var(--ins-gap)!important;scroll-snap-type:x mandatory!important}
#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{--ins-card-w:calc((100% - (var(--ins-gap) * (var(--ins-visible) - 1))) / var(--ins-visible));flex:0 0 var(--ins-card-w)!important;flex-shrink:0!important;width:var(--ins-card-w)!important;min-width:var(--ins-card-w)!important;max-width:var(--ins-card-w)!important;box-sizing:border-box!important;scroll-snap-align:start!important}
@media(max-width:960px){#insights-grid,.oh-insights-grid{--ins-visible:2!important;--ins-gap:1.25rem!important}}
@media(max-width:640px){#insights-grid,.oh-insights-grid{--ins-visible:1!important;--ins-gap:1rem!important}}
`;

fs.writeFileSync("tmp-insights-3wide.css", css);
const hash = crypto.createHash("md5").update(css).digest("hex");
console.log(JSON.stringify({ hash, len: css.length }));
