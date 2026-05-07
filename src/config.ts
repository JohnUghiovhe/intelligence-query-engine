import path from "node:path";

export const PORT = Number(process.env.PORT) || 3021;
export const REQUEST_TIMEOUT_MS = 30000;
export const SEED_PATH = path.resolve(process.cwd(), "seed_profiles.json");
export const ACCESS_TOKEN_TTL_MS = 3 * 60 * 1000;
export const REFRESH_TOKEN_TTL_MS = 5 * 60 * 1000;
export const PKCE_STATE_TTL_MS = 10 * 60 * 1000;
export const RATE_LIMIT_WINDOW_MS = 60 * 1000;
export const AUTH_RATE_LIMIT_MAX_REQUESTS = Number(process.env.AUTH_RATE_LIMIT_MAX_REQUESTS) || 10;
export const USER_RATE_LIMIT_MAX_REQUESTS = Number(process.env.USER_RATE_LIMIT_MAX_REQUESTS) || 60;
export const ALLOWED_AGE_GROUPS = new Set(["child", "teenager", "adult", "senior"]);
export const ALLOWED_SORT_COLUMNS = new Set(["age", "created_at", "gender_probability"]);
export const ALLOWED_ORDER = new Set(["asc", "desc"]);
