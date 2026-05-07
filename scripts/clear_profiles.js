const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

function getDatabaseUrl() {
  if (process.env.DATABASE_URL) return process.env.DATABASE_URL;
  const envPath = path.resolve(__dirname, '..', '.env');
  if (fs.existsSync(envPath)) {
    const lines = fs.readFileSync(envPath, 'utf8').split(/\r?\n/);
    for (const line of lines) {
      const m = line.match(/^DATABASE_URL=(.+)$/);
      if (m) return m[1].trim();
    }
  }
  return null;
}

const dbUrl = getDatabaseUrl();
if (!dbUrl) {
  console.error('ERROR: DATABASE_URL not set and .env not found or missing.');
  console.error('Set the DATABASE_URL env var or add DATABASE_URL to .env then re-run.');
  process.exit(1);
}

const pool = new Pool({ connectionString: dbUrl, ssl: false });

(async () => {
  try {
    console.log('Connecting to database...');
    const res = await pool.query('DELETE FROM profiles;');
    console.log(`✓ Deleted ${res.rowCount} rows from profiles`);
    await pool.end();
    process.exit(0);
  } catch (err) {
    console.error('ERROR:', err.message);
    await pool.end().catch(() => {});
    process.exit(1);
  }
})();
