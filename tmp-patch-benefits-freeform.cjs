const fs = require('fs');

const headPath = 'tmp-old-home-head-benefits-tabs.txt';
const footPath = 'tmp-old-home-footer-benefits-tabs.txt';
const cssPath = 'tmp-benefits-tabs-css.txt';

// Head was captured from MCP; rebuild from saved freeform + new CSS
const headRaw = fs.readFileSync('tmp-old-home-head-current.txt', 'utf8');
const footRaw = fs.readFileSync('tmp-old-home-footer-current.txt', 'utf8');
const tabsCss = fs.readFileSync(cssPath, 'utf8').trim();

const startMark = '/* Benefits header match FAQ */';
const endMark = '</style>';
const i = headRaw.indexOf(startMark);
const j = headRaw.lastIndexOf(endMark);
if (i < 0 || j < 0) {
  console.error('markers missing', i, j);
  process.exit(1);
}
const newHead =
  headRaw.slice(0, i) +
  '/* Benefits dual tabs */\n' +
  tabsCss +
  '\n' +
  headRaw.slice(j);
fs.writeFileSync(headPath, newHead);

const modulesJs = `<script>(function(){
var out=document.getElementById("modules-tab-outcomes");
var plat=document.getElementById("modules-tab-platform");
var pOut=document.getElementById("modules-panel-outcomes");
var pPlat=document.getElementById("modules-panel-platform");
function show(which){
  var isOut=which==="outcomes";
  if(out)out.setAttribute("aria-selected",isOut?"true":"false");
  if(plat)plat.setAttribute("aria-selected",isOut?"false":"true");
  if(pOut){if(isOut)pOut.removeAttribute("hidden");else pOut.setAttribute("hidden","");}
  if(pPlat){if(isOut)pPlat.setAttribute("hidden","");else pPlat.removeAttribute("hidden");}
}
if(out)out.addEventListener("click",function(e){e.preventDefault();show("outcomes");});
if(plat)plat.addEventListener("click",function(e){e.preventDefault();show("platform");});
})();</script>`;

let newFoot = footRaw;
if (!newFoot.includes('modules-tab-outcomes')) {
  newFoot = newFoot.trimEnd() + '\n' + modulesJs + '\n';
}
fs.writeFileSync(footPath, newFoot);
console.log('head', newHead.length, 'foot', newFoot.length);
