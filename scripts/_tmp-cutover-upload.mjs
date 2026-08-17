/**
 * One-shot Webflow S3 upload for Old Home cutover assets.
 * Usage: node scripts/_tmp-cutover-upload.mjs
 */
import fs from "fs";
import path from "path";
import crypto from "crypto";

const ROOT = "C:/Dev/deal-capture-proxy/public/marketing";
const SITE = "68108c29063eeb5d1bd7ae4a";

// From create_asset MCP responses (batch 1+2). Skip request-demo (identical to live 01b).
const ASSETS = [
  {
    file: "old-home-fouc-gate.v20260801k.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172435Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2bd3173c62c655ea8cd1_old-home-fouc-gate.v20260801k.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNDozNVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI0MzVaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJiZDMxNzNjNjJjNjU1ZWE4Y2QxX29sZC1ob21lLWZvdWMtZ2F0ZS52MjAyNjA4MDFrLmpzIn1dfQ==",
      xAmzSignature: "f9774a94746b118143a54377136a622b902a41e5ee2b85ade393961d7eac7d2b",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2bd3173c62c655ea8cd1_old-home-fouc-gate.v20260801k.js",
    id: "6a6e2bd3173c62c655ea8cd1",
  },
  {
    file: "old-home-hero-fit-boot.v20260801c.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172435Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2bd3f94fbd17feddba37_old-home-hero-fit-boot.v20260801c.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNDozNVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI0MzVaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJiZDNmOTRmYmQxN2ZlZGRiYTM3X29sZC1ob21lLWhlcm8tZml0LWJvb3QudjIwMjYwODAxYy5qcyJ9XX0=",
      xAmzSignature: "239f633c0fb042859d6a9314b3dc1e87be77c3bc67b7593c57a4f5c2ba9a3fd7",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2bd3f94fbd17feddba37_old-home-hero-fit-boot.v20260801c.js",
    id: "6a6e2bd3f94fbd17feddba37",
  },
  {
    file: "old-home-hero-rotator.v20260801d.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172436Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2bd303751affa38d0e4a_old-home-hero-rotator.v20260801d.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNDozNloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI0MzZaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJiZDMwMzc1MWFmZmEzOGQwZTRhX29sZC1ob21lLWhlcm8tcm90YXRvci52MjAyNjA4MDFkLmpzIn1dfQ==",
      xAmzSignature: "c7ae005de301daa485456019f8becd7802310dde130e53fc75dca8ec53d3d608",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2bd303751affa38d0e4a_old-home-hero-rotator.v20260801d.js",
    id: "6a6e2bd303751affa38d0e4a",
  },
  {
    file: "old-home-how-we-do-it.v20260801d.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172436Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2bd4eb6dc0e334533bc8_old-home-how-we-do-it.v20260801d.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNDozNloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI0MzZaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJiZDRlYjZkYzBlMzM0NTMzYmM4X29sZC1ob21lLWhvdy13ZS1kby1pdC52MjAyNjA4MDFkLmpzIn1dfQ==",
      xAmzSignature: "b854c1db91ad68a97145271b4aa05df66247af82d242c4ffffcbc7cf763cc3df",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2bd4eb6dc0e334533bc8_old-home-how-we-do-it.v20260801d.js",
    id: "6a6e2bd4eb6dc0e334533bc8",
  },
  {
    file: "old-home-explore-cta.v20260801e.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172450Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2be2291ad17ff6f4a6b3_old-home-explore-cta.v20260801e.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNDo1MFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI0NTBaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJiZTIyOTFhZDE3ZmY2ZjRhNmIzX29sZC1ob21lLWV4cGxvcmUtY3RhLnYyMDI2MDgwMWUuanMifV19",
      xAmzSignature: "a6afe1a48e4672071c1bead659a562c439029e126519d69c0c487c920410f250",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2be2291ad17ff6f4a6b3_old-home-explore-cta.v20260801e.js",
    id: "6a6e2be2291ad17ff6f4a6b3",
  },
  {
    file: "old-home-motion.prod.v20260801g.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172450Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2be2eb6dc0e334533fbc_old-home-motion.prod.v20260801g.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNDo1MFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI0NTBaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJiZTJlYjZkYzBlMzM0NTMzZmJjX29sZC1ob21lLW1vdGlvbi5wcm9kLnYyMDI2MDgwMWcuanMifV19",
      xAmzSignature: "57ef1b7e486157731042a18a60253f68f5b7c5df9d4375e3d41f9b3804a6e199",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2be2eb6dc0e334533fbc_old-home-motion.prod.v20260801g.js",
    id: "6a6e2be2eb6dc0e334533fbc",
  },
  {
    file: "dealality-old-home-hero-scroll-cue.v20260801d.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172450Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2be24286560977c4b169_dealality-old-home-hero-scroll-cue.v20260801d.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNDo1MFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI0NTBaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJiZTI0Mjg2NTYwOTc3YzRiMTY5X2RlYWxhbGl0eS1vbGQtaG9tZS1oZXJvLXNjcm9sbC1jdWUudjIwMjYwODAxZC5qcyJ9XX0=",
      xAmzSignature: "090ca44f0991a2d27592bb2184c30c947cd53ba08185dedc0dad0ed643549d2d",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2be24286560977c4b169_dealality-old-home-hero-scroll-cue.v20260801d.js",
    id: "6a6e2be24286560977c4b169",
  },
  {
    file: "dealality-old-home-nav-cleanup.v20260801b.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172451Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2be3291ad17ff6f4a6ef_dealality-old-home-nav-cleanup.v20260801b.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNDo1MVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI0NTFaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJiZTMyOTFhZDE3ZmY2ZjRhNmVmX2RlYWxhbGl0eS1vbGQtaG9tZS1uYXYtY2xlYW51cC52MjAyNjA4MDFiLmpzIn1dfQ==",
      xAmzSignature: "2dbbd47b4df00e55d231dd1f870a4434d6f31a7c8e314f3c9fe3bb0f7f5cb195",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2be3291ad17ff6f4a6ef_dealality-old-home-nav-cleanup.v20260801b.js",
    id: "6a6e2be3291ad17ff6f4a6ef",
  },
  {
    file: "dealality-old-home-faqs.v20260801c.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172451Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2be3590d5f9032117833_dealality-old-home-faqs.v20260801c.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNDo1MVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI0NTFaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJiZTM1OTBkNWY5MDMyMTE3ODMzX2RlYWxhbGl0eS1vbGQtaG9tZS1mYXFzLnYyMDI2MDgwMWMuanMifV19",
      xAmzSignature: "f093441c5fddd3f0815c1182ea58e49393e622b6bf37a7ab308a972b394341a3",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2be3590d5f9032117833_dealality-old-home-faqs.v20260801c.js",
    id: "6a6e2be3590d5f9032117833",
  },
  {
    file: "dealality-old-home-testimonials.v20260801b.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172451Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2be3590d5f9032117860_dealality-old-home-testimonials.v20260801b.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNDo1MVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI0NTFaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJiZTM1OTBkNWY5MDMyMTE3ODYwX2RlYWxhbGl0eS1vbGQtaG9tZS10ZXN0aW1vbmlhbHMudjIwMjYwODAxYi5qcyJ9XX0=",
      xAmzSignature: "14b43c671ec97e9b7ce56366ff183410fd9ac190af4d1d35d95858de47446a0b",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2be3590d5f9032117860_dealality-old-home-testimonials.v20260801b.js",
    id: "6a6e2be3590d5f9032117860",
  },
];

function sri(buf) {
  return "sha256-" + crypto.createHash("sha256").update(buf).digest("base64");
}

async function uploadOne(asset) {
  const filePath = path.join(ROOT, asset.file);
  const buf = fs.readFileSync(filePath);
  const form = new FormData();
  const d = asset.uploadDetails;
  form.append("acl", d.acl);
  form.append("bucket", d.bucket);
  form.append("X-Amz-Algorithm", d.xAmzAlgorithm);
  form.append("X-Amz-Credential", d.xAmzCredential);
  form.append("X-Amz-Date", d.xAmzDate);
  form.append("key", d.key);
  form.append("Policy", d.policy);
  form.append("X-Amz-Signature", d.xAmzSignature);
  form.append("success_action_status", d.successActionStatus);
  form.append("Content-Type", d.contentType);
  form.append("Cache-Control", d.cacheControl);
  form.append("file", new Blob([buf], { type: d.contentType }), asset.file);

  const res = await fetch(asset.uploadUrl, { method: "POST", body: form });
  const text = await res.text();
  return {
    file: asset.file,
    status: res.status,
    ok: res.status === 201,
    id: asset.id,
    hostedUrl: asset.hostedUrl,
    sri: sri(buf),
    body: text.slice(0, 200),
  };
}

const results = [];
for (const asset of ASSETS) {
  try {
    const r = await uploadOne(asset);
    results.push(r);
    console.log(r.ok ? "OK" : "FAIL", r.status, r.file, r.id);
  } catch (e) {
    results.push({ file: asset.file, ok: false, error: String(e) });
    console.error("ERR", asset.file, e);
  }
}

fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/cutover-upload-results.json",
  JSON.stringify({ site: SITE, results }, null, 2)
);
console.log("Wrote data/cutover-upload-results.json");
const failed = results.filter((r) => !r.ok);
process.exit(failed.length ? 1 : 0);
