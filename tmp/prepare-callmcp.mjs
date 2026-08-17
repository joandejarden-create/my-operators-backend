import fs from 'fs';

// Load our intended payload
const args = JSON.parse(fs.readFileSync('tmp/dmp-set-embed-shell-c.json', 'utf8'));

// Also try to extract last successful CallMcpTool from transcript for structure check
const transcriptPath =
  'C:/Users/joand/.cursor/projects/c-Dev-deal-capture-proxy/agent-transcripts/2db4745d-8227-4ab8-9716-53ffba4f3586/subagents/cd0828f3-12b8-46f0-8a9b-9c3f9996888c.jsonl';

let priorCodeLen = null;
try {
  const lines = fs.readFileSync(transcriptPath, 'utf8').split(/\n/);
  for (const line of lines) {
    if (!line.includes('data_element_settings_tool') || !line.includes('set_settings')) continue;
    const j = JSON.parse(line);
    const blocks = j?.message?.content || [];
    for (const b of blocks) {
      if (b.type !== 'tool_use' || b.name !== 'CallMcpTool') continue;
      const a = b.input?.arguments;
      const code =
        a?.actions?.[0]?.set_settings?.operations?.[0]?.settings?.[0]?.static_text?.value;
      if (code && code.length > 10000) {
        priorCodeLen = code.length;
        break;
      }
    }
    if (priorCodeLen) break;
  }
} catch {
  // ignore
}

const code = args.actions[0].set_settings.operations[0].settings[0].static_text.value;
fs.writeFileSync(
  'C:/Users/joand/.cursor/projects/c-Dev-deal-capture-proxy/agent-tools/READY_FOR_CALLMCP.json',
  JSON.stringify(args)
);

console.log(
  JSON.stringify({
    readyPath: 'agent-tools/READY_FOR_CALLMCP.json',
    bytes: Buffer.byteLength(JSON.stringify(args)),
    codeLen: code.length,
    priorCodeLen,
    hasShellC: code.includes('shell.v20260731c.css'),
    hasVersion: code.includes('data-dmp-version="1.1.33"'),
  })
);
