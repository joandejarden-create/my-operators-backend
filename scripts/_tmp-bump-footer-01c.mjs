import fs from "fs";

let c = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-site-footer-nav-width.html",
  "utf8"
);
c = c
  .replace(
    /6a6e4fed590d5f9032193ea3_dealality-marketing-nav-width\.v20260801b\.js/g,
    "6a6e5092c766e9957260e969_dealality-marketing-nav-width.v20260801c.js"
  )
  .replace(
    /6a6e4ecc93bf55bda98a5a1f_dealality-marketing-nav-width\.v20260801a\.js/g,
    "6a6e5092c766e9957260e969_dealality-marketing-nav-width.v20260801c.js"
  )
  .replace(
    /sha256-sKp\+6AYpFnNgztUp0\/FewEp8YdQ9eR8ThVqU59QaLk0=/g,
    "sha256-SD3RUmG3MlJdm2sLE7wLx8qck9L3e4SbQPuZCsc/Li0="
  )
  .replace(
    /sha256-UCpwXYZCx9rIT5g4D0LngJpFk4iqXh\+v7BmPMjxCIzI=/g,
    "sha256-SD3RUmG3MlJdm2sLE7wLx8qck9L3e4SbQPuZCsc/Li0="
  )
  .replace(/dcMktNavWidth01[ab]/g, "dcMktNavWidth01c");

fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-site-footer-nav-width.html",
  c
);
console.log({
  hasC: /v20260801c\.js/.test(c),
  hasOld: /v20260801[ab]\.js/.test(c),
});
