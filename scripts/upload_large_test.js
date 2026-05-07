const fs = require('fs');
const path = require('path');

(async () => {
  try {
    const base = 'http://localhost:3021';
    console.log('🔐 Getting auth token...');
    const tokenResp = await fetch(`${base}/auth/github/callback?code=test_code`);
    if (!tokenResp.ok) throw new Error('Failed to get token');
    const tokenJson = await tokenResp.json();
    const token = tokenJson.access_token;
    if (!token) throw new Error('No token returned');
    console.log('✓ Token obtained\n');

    const csvPath = path.resolve(__dirname, '..', 'docs', 'sample-profiles-500k.csv');
    const stats = fs.statSync(csvPath);
    const fileSizeMB = (stats.size / (1024 * 1024)).toFixed(2);
    
    console.log(`📤 Uploading ${path.basename(csvPath)}`);
    console.log(`   File size: ${fileSizeMB} MB`);
    console.log(`   Path: ${csvPath}\n`);
    
    const stream = fs.createReadStream(csvPath);
    const startTime = Date.now();
    let uploadedBytes = 0;

    stream.on('data', (chunk) => {
      uploadedBytes += chunk.length;
      const progressMB = (uploadedBytes / (1024 * 1024)).toFixed(2);
      process.stdout.write(`\r   Streamed: ${progressMB} MB`);
    });

    const res = await fetch(`${base}/api/profiles/upload`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'X-API-Version': '1',
        'Content-Type': 'text/csv'
      },
      duplex: 'half',
      body: stream
    });

    const elapsedMs = Date.now() - startTime;
    const elapsedSec = (elapsedMs / 1000).toFixed(2);
    process.stdout.write('\r                           \r'); // Clear progress line

    const text = await res.text();
    
    console.log(`✓ HTTP ${res.status} in ${elapsedSec}s\n`);
    
    try {
      const json = JSON.parse(text);
      console.log(`  Status:      ${json.status}`);
      console.log(`  Total rows:  ${json.total_rows.toLocaleString()}`);
      console.log(`  Inserted:    ${json.inserted.toLocaleString()}`);
      console.log(`  Skipped:     ${json.skipped.toLocaleString()}`);
      
      if (Object.keys(json.reasons).length > 0) {
        console.log(`  Skip reasons: ${JSON.stringify(json.reasons)}`);
      }
      
      const throughput = (json.total_rows / (elapsedMs / 1000)).toLocaleString('en-US', { maximumFractionDigits: 0 });
      console.log(`\n  Elapsed time: ${elapsedSec}s`);
      console.log(`  Rows/sec:     ${throughput}`);
      console.log(`\n⚡ Throughput: ${throughput} rows/sec`);
      console.log(`⏱️  Total time: ${elapsedSec} seconds\n`);
    } catch (e) {
      console.log(text);
    }
  } catch (err) {
    console.error('\n❌ Error:', err.message);
    process.exit(1);
  }
})();
