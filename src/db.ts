import fs from "node:fs";
import { Pool, PoolClient } from "pg";
import { generateUuidV7 } from "./utils/crypto";
import { SEED_PATH } from "./config";
import { SeedProfile } from "./types";

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) {
  throw new Error("DATABASE_URL is required");
}

export const pool = new Pool({
  connectionString: databaseUrl,
  ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false
});

// Ensure session does not use a server-side statement timeout for long migrations
pool.on("connect", async (client: PoolClient) => {
  try {
    await client.query("SET statement_timeout = 0");
  } catch (err) {
    // ignore failures setting session params
  }
});

export const initializeDatabase = async (): Promise<void> => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS profiles (
      id UUID PRIMARY KEY,
      name TEXT NOT NULL,
      gender TEXT NOT NULL,
      gender_probability DOUBLE PRECISION NOT NULL,
      age INTEGER NOT NULL,
      age_group TEXT NOT NULL,
      country_id TEXT NOT NULL,
      country_name TEXT NOT NULL,
      country_probability DOUBLE PRECISION NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS ux_profiles_name_lower ON profiles (LOWER(name));
    CREATE INDEX IF NOT EXISTS idx_profiles_gender ON profiles(gender);
    CREATE INDEX IF NOT EXISTS idx_profiles_age_group ON profiles(age_group);
    CREATE INDEX IF NOT EXISTS idx_profiles_country_id ON profiles(country_id);
    CREATE INDEX IF NOT EXISTS idx_profiles_age ON profiles(age);
    CREATE INDEX IF NOT EXISTS idx_profiles_created_at ON profiles(created_at);
    CREATE INDEX IF NOT EXISTS idx_profiles_created_at_id ON profiles(created_at DESC, id DESC);
    CREATE INDEX IF NOT EXISTS idx_profiles_gender_created_at_id ON profiles(gender, created_at DESC, id DESC);
    CREATE INDEX IF NOT EXISTS idx_profiles_age_group_created_at_id ON profiles(age_group, created_at DESC, id DESC);
    CREATE INDEX IF NOT EXISTS idx_profiles_country_id_created_at_id ON profiles(country_id, created_at DESC, id DESC);
    CREATE INDEX IF NOT EXISTS idx_profiles_age_created_at_id ON profiles(age, created_at DESC, id DESC);
    CREATE INDEX IF NOT EXISTS idx_profiles_gender_probability_created_at_id ON profiles(gender_probability, created_at DESC, id DESC);
    CREATE INDEX IF NOT EXISTS idx_profiles_gender_probability ON profiles(gender_probability);
    CREATE INDEX IF NOT EXISTS idx_profiles_country_probability ON profiles(country_probability);
    CREATE INDEX IF NOT EXISTS idx_profiles_country_name ON profiles(country_name);
    CREATE INDEX IF NOT EXISTS idx_profiles_country_name_lower ON profiles(LOWER(country_name));
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id UUID PRIMARY KEY,
      github_id VARCHAR(128) NOT NULL UNIQUE,
      username VARCHAR(255) NOT NULL,
      email VARCHAR(255),
      avatar_url TEXT,
      role VARCHAR(20) NOT NULL DEFAULT 'analyst' CHECK (role IN ('admin', 'analyst')),
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      last_login_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS oauth_pkce_states (
      state VARCHAR(255) PRIMARY KEY,
      code_verifier TEXT NOT NULL,
      redirect_uri TEXT,
      expires_at TIMESTAMPTZ NOT NULL
    );
  `);

  // Add redirect_uri column if it doesn't exist (migration for existing databases)
  await pool.query(`
    ALTER TABLE oauth_pkce_states
    ADD COLUMN IF NOT EXISTS redirect_uri TEXT;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS refresh_tokens (
      id UUID PRIMARY KEY,
      user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      token_hash TEXT NOT NULL UNIQUE,
      expires_at TIMESTAMPTZ NOT NULL,
      is_revoked BOOLEAN NOT NULL DEFAULT FALSE,
      replaced_by_token_hash TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user_id ON refresh_tokens(user_id);
    CREATE INDEX IF NOT EXISTS idx_refresh_tokens_token_hash ON refresh_tokens(token_hash);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS access_tokens (
      id UUID PRIMARY KEY,
      user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      token_hash TEXT NOT NULL UNIQUE,
      expires_at TIMESTAMPTZ NOT NULL,
      is_revoked BOOLEAN NOT NULL DEFAULT FALSE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_access_tokens_token_hash ON access_tokens(token_hash);
  `);

  const seedRaw = fs.readFileSync(SEED_PATH, "utf8");
  const seedData = JSON.parse(seedRaw) as { profiles?: SeedProfile[] };
  const rows = Array.isArray(seedData.profiles) ? seedData.profiles : [];

  const countResult = await pool.query("SELECT COUNT(*)::int AS total FROM profiles");
  const existingTotal = Number(countResult.rows[0]?.total ?? 0);
  if (existingTotal >= rows.length) {
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    // Batch inserts in groups of 100 to improve performance
    const batchSize = 100;
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      const values: Array<unknown> = [];
      const placeholders: string[] = [];
      let paramIndex = 1;

      for (const profile of batch) {
        placeholders.push(
          `($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, NOW())`
        );
        values.push(
          generateUuidV7(),
          String(profile.name).trim().toLowerCase(),
          profile.gender,
          Number(profile.gender_probability),
          Number(profile.age),
          profile.age_group,
          String(profile.country_id).toUpperCase(),
          String(profile.country_name).trim(),
          Number(profile.country_probability)
        );
      }

      await client.query(
        `INSERT INTO profiles (
           id, name, gender, gender_probability, age, age_group,
           country_id, country_name, country_probability, created_at
         ) VALUES ${placeholders.join(", ")}
         ON CONFLICT (LOWER(name)) DO NOTHING`,
        values
      );
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};

export const withTransaction = async <T>(work: (client: PoolClient) => Promise<T>): Promise<T> => {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await work(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};
