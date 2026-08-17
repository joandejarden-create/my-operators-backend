const pages = [
  "https://media.choicehotels.com/press-releases?l=50",
  "https://media.choicehotels.com/press-releases?l=50&o=50",
  "https://media.choicehotels.com/press-releases?l=50&o=100",
  "https://media.choicehotels.com/press-releases?l=50&o=150",
];
const keys = /panama|paramaribo|suriname|riviera|caracol|potosi|puebla|radisson.*open/i;
for (const page of pages) {
  const t = await fetch(page).then((r) => r.text());
  const links = [...t.matchAll(/href="(\/[^"]+)"[^>]*>([^<]{10,200})</g)];
  for (const [, href, title] of links) {
    if (keys.test(title) || keys.test(href)) {
      console.log(`https://media.choicehotels.com${href}`);
      console.log(`  ${title.trim()}`);
    }
  }
}
