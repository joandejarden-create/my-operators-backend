// Test-only imports. No credentials, SDKs, network or fake claim of live API validation.
const modules = {
  '/context-dev/client.js': `export const OWNERSHIP_CONTACT_EXTRACT_SCHEMA={}; export const isContextDevConfigured=()=>true; export const contextDevSearch=(p)=>globalThis.astraFixture.search(p); export const contextDevScrapeMarkdown=(p)=>globalThis.astraFixture.scrape(p); export const contextDevExtract=()=>{throw Error('UNEXPECTED_EXTRACT')};`,
  '/owner-person-discovery.js': `export const discoverOwnerPersonPath=()=>{throw Error('UNEXPECTED_SERP')};export const verifyOwnershipPath=()=>{throw Error('UNEXPECTED_VERIFY')};`,
  '/live-native-discovery.js': `export const serpGoogle=()=>{throw Error('UNEXPECTED_SERP')};export const extractContactsFromHtml=()=>({emails:[],phones:[]});`,
};
export async function resolve(specifier, context, nextResolve) {
  for (const [suffix,code] of Object.entries(modules)) if (specifier.endsWith(suffix)) return {url:'data:text/javascript,'+encodeURIComponent(code),shortCircuit:true};
  return nextResolve(specifier, context);
}
