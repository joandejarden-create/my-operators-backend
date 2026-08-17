import fs from 'fs';

const transcriptPath =
  'C:/Users/joand/.cursor/projects/c-Dev-deal-capture-proxy/agent-transcripts/2db4745d-8227-4ab8-9716-53ffba4f3586/subagents/cd0828f3-12b8-46f0-8a9b-9c3f9996888c.jsonl';

const lines = fs.readFileSync(transcriptPath, 'utf8').split(/\n/);
let found = null;
for (const line of lines) {
  if (!line.includes('CallMcpTool') || !line.includes('set_settings')) continue;
  let j;
  try {
    j = JSON.parse(line);
  } catch {
    continue;
  }
  const blocks = j?.message?.content || [];
  for (const b of blocks) {
    if (b.type !== 'tool_use' || b.name !== 'CallMcpTool') continue;
    const a = b.input?.arguments;
    const code =
      a?.actions?.[0]?.set_settings?.operations?.[0]?.settings?.[0]?.static_text?.value;
    if (code && code.includes('shell.v20260731b.css') && code.length > 10000) {
      found = a;
      break;
    }
  }
  if (found) break;
}

if (!found) {
  console.log(JSON.stringify({ error: 'not_found' }));
  process.exit(1);
}

let code = found.actions[0].set_settings.operations[0].settings[0].static_text.value;
// Upgrade shell b -> c with known CDN asset from our payload
code = code.replace(
  /https:\/\/cdn\.prod\.website-files\.com\/68108c29063eeb5d1bd7ae4a\/[^"']+_old-home-manual-process\.shell\.v20260731b\.css/g,
  'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d6f8824a07ced4109566b_old-home-manual-process.shell.v20260731c.css'
);
code = code.replace(/shell\.v20260731b\.css/g, 'shell.v20260731c.css');

found.actions[0].set_settings.operations[0].settings[0].static_text.value = code;
found.context =
  'Updates Manual Process HtmlEmbed shell CSS so Problem section background is full-bleed.';

// Prefer exact local payload if available and matches length closely
const local = JSON.parse(fs.readFileSync('tmp/dmp-set-embed-shell-c.json', 'utf8'));
const localCode =
  local.actions[0].set_settings.operations[0].settings[0].static_text.value;

fs.writeFileSync('tmp/dmp-upgraded-from-b.json', JSON.stringify(found));
fs.writeFileSync('tmp/dmp-use-local.json', JSON.stringify(local));

console.log(
  JSON.stringify({
    upgradedLen: code.length,
    localLen: localCode.length,
    upgradedHasC: code.includes('shell.v20260731c.css'),
    upgradedHasB: code.includes('shell.v20260731b.css'),
    localHasC: localCode.includes('shell.v20260731c.css'),
    same: code === localCode,
  })
);
