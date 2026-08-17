import fs from 'fs';
import FormData from 'form-data';
import https from 'https';

const uploadDetails = {
  acl: 'public-read',
  bucket: 'webflow-prod-assets',
  xAmzAlgorithm: 'AWS4-HMAC-SHA256',
  xAmzCredential: 'AKIAQLLHWD6MEJGETLST/20260730/us-east-1/s3/aws4_request',
  xAmzDate: '20260730T220613Z',
  key: '68108c29063eeb5d1bd7ae4a/6a6bcad5d2a0492c49da9bd1_oh-deal-desk-phase-a.css',
  policy:
    'eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0zMFQyMzowNjoxM1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MzAvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzMwVDIyMDYxM1oifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZiY2FkNWQyYTA0OTJjNDlkYTliZDFfb2gtZGVhbC1kZXNrLXBoYXNlLWEuY3NzIn1dfQ==',
  xAmzSignature: '9a78a0dfe76223a768fcc03f835caa0a4e90f7ea95b7464a7e7c098d527f140f',
  successActionStatus: '201',
  contentType: 'text/css',
  cacheControl: 'max-age=31536000',
};

const fileName = 'oh-deal-desk-phase-a.css';
const fileData = fs.readFileSync('docs/_oh_deal_desk_only.css');

const form = new FormData();
form.append('acl', uploadDetails.acl);
form.append('bucket', uploadDetails.bucket);
form.append('X-Amz-Algorithm', uploadDetails.xAmzAlgorithm);
form.append('X-Amz-Credential', uploadDetails.xAmzCredential);
form.append('X-Amz-Date', uploadDetails.xAmzDate);
form.append('key', uploadDetails.key);
form.append('Policy', uploadDetails.policy);
form.append('X-Amz-Signature', uploadDetails.xAmzSignature);
form.append('success_action_status', uploadDetails.successActionStatus);
form.append('Content-Type', uploadDetails.contentType);
form.append('Cache-Control', uploadDetails.cacheControl);
form.append('file', fileData, { filename: fileName, contentType: uploadDetails.contentType });

const length = await new Promise((resolve, reject) => {
  form.getLength((err, len) => (err ? reject(err) : resolve(len)));
});

await new Promise((resolve, reject) => {
  const req = https.request(
    'https://webflow-prod-assets.s3.amazonaws.com/',
    {
      method: 'POST',
      headers: {
        ...form.getHeaders(),
        'Content-Length': length,
      },
    },
    (res) => {
      let body = '';
      res.on('data', (c) => (body += c));
      res.on('end', () => {
        console.log(JSON.stringify({ status: res.statusCode, body: body.slice(0, 500) }));
        if (res.statusCode === 201) resolve();
        else reject(new Error(`upload failed ${res.statusCode}`));
      });
    }
  );
  req.on('error', reject);
  form.pipe(req);
});
