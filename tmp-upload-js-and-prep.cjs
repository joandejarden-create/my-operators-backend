const fs = require('fs');
const details = {
  uploadUrl: 'https://webflow-prod-assets.s3.amazonaws.com/',
  uploadDetails: {
    acl: 'public-read',
    bucket: 'webflow-prod-assets',
    xAmzAlgorithm: 'AWS4-HMAC-SHA256',
    xAmzCredential: 'AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request',
    xAmzDate: '20260728T193959Z',
    key: '68108c29063eeb5d1bd7ae4a/6a69058f60171c2764fb0e62_dealality-old-home-benefits-tabs.v20260728.js',
    policy:
      'eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQyMDozOTo1OVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI4L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOFQxOTM5NTlaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2OTA1OGY2MDE3MWMyNzY0ZmIwZTYyX2RlYWxhbGl0eS1vbGQtaG9tZS1iZW5lZml0cy10YWJzLnYyMDI2MDcyOC5qcyJ9XX0=',
    xAmzSignature: '7e7e08bd3326085d60234a29a4f72e014e3f5802ed06669adc99a47332e144d2',
    successActionStatus: '201',
    contentType: 'application/javascript',
    cacheControl: 'max-age=31536000',
  },
};

async function main() {
  const fileData = fs.readFileSync('tmp-benefits-tabs.js');
  const form = new FormData();
  const u = details.uploadDetails;
  form.append('acl', u.acl);
  form.append('bucket', u.bucket);
  form.append('X-Amz-Algorithm', u.xAmzAlgorithm);
  form.append('X-Amz-Credential', u.xAmzCredential);
  form.append('X-Amz-Date', u.xAmzDate);
  form.append('key', u.key);
  form.append('Policy', u.policy);
  form.append('X-Amz-Signature', u.xAmzSignature);
  form.append('success_action_status', u.successActionStatus);
  form.append('Content-Type', u.contentType);
  form.append('Cache-Control', u.cacheControl);
  form.append('file', new Blob([fileData], { type: u.contentType }), 'dealality-old-home-benefits-tabs.v20260728.js');
  const res = await fetch(details.uploadUrl, { method: 'POST', body: form });
  console.log('js', res.status, (await res.text()).slice(0, 180));
  if (res.status !== 201) process.exit(1);

  let head = fs.readFileSync('tmp-old-home-head-patched.txt', 'utf8');
  const cssLink =
    '<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6905579687cbd43649cf04_dealality-old-home-benefits-tabs.v20260728.css">';
  const marker = 'dealality-old-home-freeform.v20260728benefits.css">';
  if (!head.includes('benefits-tabs.v20260728.css')) {
    head = head.replace(marker, marker + '\n' + cssLink);
  }
  fs.writeFileSync('tmp-old-home-head-with-tabs-link.txt', head);

  let foot = fs.readFileSync('tmp-old-home-footer-fixed.txt', 'utf8');
  const jsTag =
    '<script src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a69058f60171c2764fb0e62_dealality-old-home-benefits-tabs.v20260728.js"></script>';
  if (!foot.includes('benefits-tabs.v20260728.js')) {
    foot = foot.trimEnd() + '\n' + jsTag + '\n';
  }
  fs.writeFileSync('tmp-old-home-footer-with-tabs-js.txt', foot);
  fs.writeFileSync(
    'tmp-set-freeform-cdn.json',
    JSON.stringify({
      actions: [
        {
          label: 'set-head',
          set_page_freeform_code: {
            page_id: '68108c2a063eeb5d1bd7ae90',
            location: 'head',
            content: head,
          },
        },
        {
          label: 'set-foot',
          set_page_freeform_code: {
            page_id: '68108c2a063eeb5d1bd7ae90',
            location: 'footer',
            content: foot,
          },
        },
      ],
      context: 'Add Benefits dual-tab CDN CSS and JS to old-home freeform code.',
    })
  );
  console.log('prepared', head.length, foot.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
