import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const file = path.join(path.dirname(fileURLToPath(import.meta.url)), '../public/js/brand-explorer-atelier-from-api.js');
let s = fs.readFileSync(file, 'utf8');

s = s.replace("'</span></motion></motion>' +", "'</span></motion></motion>' +");
