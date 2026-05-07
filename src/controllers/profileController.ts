import { Request, Response } from "express";
import { once } from "node:events";
import { PassThrough } from "node:stream";
import { pipeline } from "node:stream/promises";
import { from as copyFrom } from "pg-copy-streams";
import { pool, withTransaction } from "../db";
import { generateUuidV7 } from "../utils/crypto";
import { toError } from "../utils/http";
import {
  ALLOWED_AGE_GROUPS,
  ALLOWED_ORDER,
  ALLOWED_SORT_COLUMNS,
  REQUEST_TIMEOUT_MS
} from "../config";
import { CsvRowParser, ParsedCsvRow, normalizeCsvHeader } from "../utils/csv";
import { buildQueryCacheKey, createInMemoryCache, normalizeParsedFilters } from "../utils/queryCache";
import {
  AgifyResponse,
  GenderizeResponse,
  NationalizeResponse,
  ParsedFilters,
  PagingAndSort,
  ProfileRow,
  Queryable
} from "../types";

type QueryCachePayload = {
  total: number;
  total_pages: number;
  data: ProfileRow[];
  next_cursor?: string | null;
};

type UploadReason =
  | "broken_encoding"
  | "database_error"
  | "duplicate_name"
  | "invalid_age"
  | "invalid_gender"
  | "invalid_probability"
  | "malformed_row"
  | "missing_fields";

type UploadSummary = {
  status: "success";
  total_rows: number;
  inserted: number;
  skipped: number;
  reasons: Partial<Record<UploadReason, number>>;
};

type UploadCandidate = {
  name: string;
  gender: ProfileRow["gender"];
  age: number;
  age_group: ProfileRow["age_group"];
  country_id: string;
  country_name: string;
  gender_probability: number;
  country_probability: number;
};

const QUERY_CACHE_TTL_MS = 30_000;
const QUERY_CACHE_MAX_ENTRIES = 250;
const queryCache = createInMemoryCache<QueryCachePayload | ProfileRow | UploadSummary>(
  QUERY_CACHE_MAX_ENTRIES,
  QUERY_CACHE_TTL_MS
);

const PROFILE_SECONDARY_INDEX_NAMES = [
  "idx_profiles_gender",
  "idx_profiles_age_group",
  "idx_profiles_country_id",
  "idx_profiles_age",
  "idx_profiles_created_at",
  "idx_profiles_created_at_id",
  "idx_profiles_gender_created_at_id",
  "idx_profiles_age_group_created_at_id",
  "idx_profiles_country_id_created_at_id",
  "idx_profiles_age_created_at_id",
  "idx_profiles_gender_probability_created_at_id",
  "idx_profiles_gender_probability",
  "idx_profiles_country_probability",
  "idx_profiles_country_name",
  "idx_profiles_country_name_lower"
] as const;

const PROFILE_UNIQUE_INDEX_NAME = "ux_profiles_name_lower";
const PROFILE_UNIQUE_INDEX_STATEMENT = "CREATE UNIQUE INDEX IF NOT EXISTS ux_profiles_name_lower ON profiles (LOWER(name))";

const PROFILE_SECONDARY_INDEX_STATEMENTS = [
  "CREATE INDEX IF NOT EXISTS idx_profiles_gender ON profiles(gender)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_age_group ON profiles(age_group)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_country_id ON profiles(country_id)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_age ON profiles(age)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_created_at ON profiles(created_at)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_created_at_id ON profiles(created_at DESC, id DESC)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_gender_created_at_id ON profiles(gender, created_at DESC, id DESC)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_age_group_created_at_id ON profiles(age_group, created_at DESC, id DESC)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_country_id_created_at_id ON profiles(country_id, created_at DESC, id DESC)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_age_created_at_id ON profiles(age, created_at DESC, id DESC)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_gender_probability_created_at_id ON profiles(gender_probability, created_at DESC, id DESC)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_gender_probability ON profiles(gender_probability)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_country_probability ON profiles(country_probability)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_country_name ON profiles(country_name)",
  "CREATE INDEX IF NOT EXISTS idx_profiles_country_name_lower ON profiles(LOWER(country_name))"
] as const;

const dropProfileSecondaryIndexes = async (client: { query: (text: string) => Promise<unknown> }): Promise<void> => {
  for (const indexName of PROFILE_SECONDARY_INDEX_NAMES) {
    await client.query(`DROP INDEX IF EXISTS ${indexName}`);
  }
};

const recreateProfileSecondaryIndexes = async (client: { query: (text: string) => Promise<unknown> }): Promise<void> => {
  for (const statement of PROFILE_SECONDARY_INDEX_STATEMENTS) {
    await client.query(statement);
  }
};

const recreateProfileUniqueIndex = async (client: { query: (text: string) => Promise<unknown> }): Promise<void> => {
  await client.query(PROFILE_UNIQUE_INDEX_STATEMENT);
};

const toIso = (value: unknown): string => {
  if (value instanceof Date) return value.toISOString();
  return new Date(String(value)).toISOString();
};

const normalizeProfileRow = (row: Record<string, unknown>): ProfileRow => ({
  id: String(row.id),
  name: String(row.name),
  gender: String(row.gender) as ProfileRow["gender"],
  gender_probability: Number(row.gender_probability),
  age: Number(row.age),
  age_group: String(row.age_group) as ProfileRow["age_group"],
  country_id: String(row.country_id),
  country_name: String(row.country_name),
  country_probability: Number(row.country_probability),
  created_at: toIso(row.created_at)
});

const clearQueryCache = (): void => {
  queryCache.clear();
};

const buildProfileCacheKey = (id: string): string => `profile|id=${id.toLowerCase()}`;

const buildFilterCacheKey = (
  scope: string,
  filters: ParsedFilters,
  paging?: Pick<PagingAndSort, "limit" | "order" | "sortBy" | "page"> & { cursor?: PagingAndSort["cursor"] }
): string => buildQueryCacheKey(scope, normalizeParsedFilters(filters), paging);

const buildUploadReasonMap = (): Record<UploadReason, number> => ({
  broken_encoding: 0,
  database_error: 0,
  duplicate_name: 0,
  invalid_age: 0,
  invalid_gender: 0,
  invalid_probability: 0,
  malformed_row: 0,
  missing_fields: 0
});

const incrementReason = (reasons: Record<UploadReason, number>, reason: UploadReason, amount = 1): void => {
  reasons[reason] += amount;
};

const escapeCsvValue = (value: string): string => {
  if (!/[",\r\n]/.test(value)) return value;
  return `"${value.replace(/"/g, '""')}"`;
};

const serializeUploadCandidate = (candidate: UploadCandidate): string => {
  const row = [
    generateUuidV7(),
    candidate.name,
    candidate.gender,
    candidate.gender_probability,
    candidate.age,
    candidate.age_group,
    candidate.country_id,
    candidate.country_name,
    candidate.country_probability
  ].map((value) => escapeCsvValue(String(value)));

  return `${row.join(",")}\n`;
};

const getAgeFromRow = (value: string): number | null => {
  if (!value.trim()) return null;
  const age = Number(value);
  if (!Number.isInteger(age) || age < 0) return null;
  return age;
};

const getProbabilityFromRow = (value: string | undefined): number | null => {
  if (value === undefined || !value.trim()) return 0;
  const parsed = Number(value);
  if (Number.isNaN(parsed) || parsed < 0 || parsed > 1) return null;
  return parsed;
};

const extractUploadCandidate = (
  headerIndex: Map<string, number>,
  row: ParsedCsvRow
): { candidate?: UploadCandidate; reason?: UploadReason } => {
  if (row.brokenEncoding) return { reason: "broken_encoding" };
  if (row.malformed) return { reason: "malformed_row" };

  const expectedColumns = headerIndex.size;
  if (row.cells.length !== expectedColumns) return { reason: "malformed_row" };

  const readValue = (key: string): string => {
    const index = headerIndex.get(key);
    if (index === undefined) return "";
    return String(row.cells[index] ?? "").trim();
  };

  const name = readValue("name").toLowerCase();
  const genderRaw = readValue("gender").toLowerCase();
  const ageRaw = readValue("age");
  const countryIdRaw = readValue("country_id").toUpperCase();
  const countryName = readValue("country_name");
  const genderProbabilityRaw = readValue("gender_probability");
  const countryProbabilityRaw = readValue("country_probability");

  if (!name || !genderRaw || !ageRaw || !countryIdRaw || !countryName) return { reason: "missing_fields" };
  if (genderRaw !== "male" && genderRaw !== "female") return { reason: "invalid_gender" };

  const age = getAgeFromRow(ageRaw);
  if (age === null) return { reason: "invalid_age" };

  const genderProbability = getProbabilityFromRow(genderProbabilityRaw);
  if (genderProbability === null) return { reason: "invalid_probability" };

  const countryProbability = getProbabilityFromRow(countryProbabilityRaw);
  if (countryProbability === null) return { reason: "invalid_probability" };

  if (!/^[A-Z]{2}$/.test(countryIdRaw)) return { reason: "invalid_probability" };

  return {
    candidate: {
      name,
      gender: genderRaw,
      age,
      age_group: getAgeGroup(age),
      country_id: countryIdRaw,
      country_name: countryName,
      gender_probability: genderProbability,
      country_probability: countryProbability
    }
  };
};

const getAgeGroup = (age: number): ProfileRow["age_group"] => {
  if (age <= 12) return "child";
  if (age <= 19) return "teenager";
  if (age <= 59) return "adult";
  return "senior";
};

const parseName = (name: unknown): { value?: string; code?: number; message?: string } => {
  if (name === undefined || name === null) return { code: 400, message: "Missing or empty parameter" };
  if (typeof name !== "string") return { code: 422, message: "Invalid parameter type" };
  const value = name.trim().toLowerCase();
  if (!value) return { code: 400, message: "Missing or empty parameter" };
  return { value };
};

const fetchJson = async <T>(url: string, headers?: Record<string, string>): Promise<T> => {
  const response = await fetch(url, { signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS), headers });
  if (!response.ok) throw new Error("UPSTREAM_STATUS_ERROR");
  return (await response.json()) as T;
};

const getExternalData = async (
  name: string
): Promise<{
  gender: "male" | "female";
  genderProbability: number;
  age: number;
  ageGroup: ProfileRow["age_group"];
  countryId: string;
  countryProbability: number;
}> => {
  const [genderize, agify, nationalize] = await Promise.all([
    fetchJson<GenderizeResponse>(`https://api.genderize.io?name=${encodeURIComponent(name)}`),
    fetchJson<AgifyResponse>(`https://api.agify.io?name=${encodeURIComponent(name)}`),
    fetchJson<NationalizeResponse>(`https://api.nationalize.io?name=${encodeURIComponent(name)}`)
  ]);

  if (!genderize.gender) throw new Error("Genderize_INVALID");
  if (agify.age === null || Number.isNaN(Number(agify.age))) throw new Error("Agify_INVALID");
  if (!Array.isArray(nationalize.country) || nationalize.country.length === 0) throw new Error("Nationalize_INVALID");

  const topCountry = nationalize.country.reduce((best, current) =>
    current.probability > best.probability ? current : best
  );
  const age = Number(agify.age);

  return {
    gender: genderize.gender,
    genderProbability: Number(genderize.probability ?? 0),
    age,
    ageGroup: getAgeGroup(age),
    countryId: topCountry.country_id.toUpperCase(),
    countryProbability: Number(topCountry.probability ?? 0)
  };
};

const getInvalidUpstreamError = (error: unknown): string | null => {
  if (!(error instanceof Error)) return null;
  if (error.message.startsWith("Genderize_")) return "Genderize returned an invalid response";
  if (error.message.startsWith("Agify_")) return "Agify returned an invalid response";
  if (error.message.startsWith("Nationalize_")) return "Nationalize returned an invalid response";
  return null;
};

const buildWhereClause = (filters: ParsedFilters): { clause: string; values: Array<string | number> } => {
  const where: string[] = [];
  const values: Array<string | number> = [];
  let index = 1;

  if (filters.gender) {
    where.push(`gender = $${index++}`);
    values.push(filters.gender);
  }
  if (filters.age_group) {
    where.push(`age_group = $${index++}`);
    values.push(filters.age_group);
  }
  if (filters.country_id) {
    where.push(`country_id = $${index++}`);
    values.push(filters.country_id);
  }
  if (typeof filters.min_age === "number") {
    where.push(`age >= $${index++}`);
    values.push(filters.min_age);
  }
  if (typeof filters.max_age === "number") {
    where.push(`age <= $${index++}`);
    values.push(filters.max_age);
  }
  if (typeof filters.min_gender_probability === "number") {
    where.push(`gender_probability >= $${index++}`);
    values.push(filters.min_gender_probability);
  }
  if (typeof filters.min_country_probability === "number") {
    where.push(`country_probability >= $${index++}`);
    values.push(filters.min_country_probability);
  }

  return { clause: where.length > 0 ? `WHERE ${where.join(" AND ")}` : "", values };
};

const ensureValidNumber = (value: unknown): number | undefined | "invalid" => {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || value.trim() === "") return "invalid";
  const parsed = Number(value);
  if (Number.isNaN(parsed)) return "invalid";
  return parsed;
};

export const parseFilterQuery = (query: Request["query"]): ParsedFilters | null => {
  const filters: ParsedFilters = {};

  const readSingle = (key: string): string | undefined | "invalid" => {
    const val = query[key];
    if (val === undefined) return undefined;
    if (Array.isArray(val) || typeof val !== "string") return "invalid";
    const trimmed = val.trim();
    if (!trimmed) return "invalid";
    return trimmed;
  };

  const gender = readSingle("gender");
  const ageGroup = readSingle("age_group");
  const countryId = readSingle("country_id");
  const minAge = ensureValidNumber(readSingle("min_age"));
  const maxAge = ensureValidNumber(readSingle("max_age"));
  const minGenderProbability = ensureValidNumber(readSingle("min_gender_probability"));
  const minCountryProbability = ensureValidNumber(readSingle("min_country_probability"));

  if (
    gender === "invalid" ||
    ageGroup === "invalid" ||
    countryId === "invalid" ||
    minAge === "invalid" ||
    maxAge === "invalid" ||
    minGenderProbability === "invalid" ||
    minCountryProbability === "invalid"
  ) {
    return null;
  }

  if (gender) {
    const parsedGender = gender.toLowerCase();
    if (parsedGender !== "male" && parsedGender !== "female") return null;
    filters.gender = parsedGender;
  }

  if (ageGroup) {
    const parsedAgeGroup = ageGroup.toLowerCase();
    if (!ALLOWED_AGE_GROUPS.has(parsedAgeGroup)) return null;
    filters.age_group = parsedAgeGroup as ParsedFilters["age_group"];
  }

  if (countryId) {
    const parsedCountryId = countryId.toUpperCase();
    if (!/^[A-Z]{2}$/.test(parsedCountryId)) return null;
    filters.country_id = parsedCountryId;
  }

  if (typeof minAge === "number") filters.min_age = minAge;
  if (typeof maxAge === "number") filters.max_age = maxAge;
  if (typeof minGenderProbability === "number" && minGenderProbability >= 0 && minGenderProbability <= 1) {
    filters.min_gender_probability = minGenderProbability;
  } else if (minGenderProbability !== undefined) {
    return null;
  }
  if (typeof minCountryProbability === "number" && minCountryProbability >= 0 && minCountryProbability <= 1) {
    filters.min_country_probability = minCountryProbability;
  } else if (minCountryProbability !== undefined) {
    return null;
  }

  if (
    typeof filters.min_age === "number" &&
    typeof filters.max_age === "number" &&
    filters.min_age > filters.max_age
  ) {
    return null;
  }

  return filters;
};

export const parsePagingAndSort = (query: Request["query"]): PagingAndSort | null => {
  const pageRaw = query.page;
  const limitRaw = query.limit;
  const sortByRaw = query.sort_by;
  const orderRaw = query.order;
  const cursorRaw = query.cursor;

  if (
    Array.isArray(pageRaw) ||
    Array.isArray(limitRaw) ||
    Array.isArray(sortByRaw) ||
    Array.isArray(orderRaw) ||
    Array.isArray(cursorRaw)
  ) {
    return null;
  }

  const page = pageRaw === undefined ? 1 : Number(pageRaw);
  const limit = limitRaw === undefined ? 10 : Number(limitRaw);
  if (!Number.isInteger(page) || page < 1) return null;
  if (!Number.isInteger(limit) || limit < 1 || limit > 50) return null;

  const sortBy = (typeof sortByRaw === "string" ? sortByRaw.trim().toLowerCase() : "created_at") as
    | "age"
    | "created_at"
    | "gender_probability";
  const order = (typeof orderRaw === "string" ? orderRaw.trim().toLowerCase() : "desc") as "asc" | "desc";
  if (!ALLOWED_SORT_COLUMNS.has(sortBy) || !ALLOWED_ORDER.has(order)) return null;

  let cursor: { created_at: string; id: string } | undefined;
  if (cursorRaw !== undefined) {
    if (typeof cursorRaw !== "string" || !cursorRaw.trim()) return null;
    if (sortBy !== "created_at") return null;
    if (pageRaw !== undefined) return null;
    try {
      const decoded = Buffer.from(cursorRaw, "base64url").toString("utf8");
      const parsed = JSON.parse(decoded) as { created_at?: string; id?: string };
      if (!parsed.created_at || !parsed.id) return null;
      cursor = { created_at: parsed.created_at, id: parsed.id };
    } catch {
      return null;
    }
  }

  if (cursor) {
    return { limit, sortBy, order, cursor };
  }

  return { page, limit, sortBy, order };
};

export const encodeCursor = (row: Pick<ProfileRow, "created_at" | "id">): string => {
  return Buffer.from(JSON.stringify({ created_at: row.created_at, id: row.id }), "utf8").toString("base64url");
};

export const appendCursorCondition = (
  whereClause: string,
  order: "asc" | "desc",
  cursor: { created_at: string; id: string },
  values: Array<string | number>
): string => {
  values.push(cursor.created_at, cursor.created_at, cursor.id);
  const comparator = order === "asc" ? ">" : "<";
  const baseIndex = values.length - 2;
  const cursorCondition = `(created_at ${comparator} $${baseIndex} OR (created_at = $${baseIndex + 1} AND id ${comparator} $${baseIndex + 2}))`;
  return whereClause ? `${whereClause} AND ${cursorCondition}` : `WHERE ${cursorCondition}`;
};

const buildPaginationLinks = (req: Request, page: number, limit: number, totalPages: number) => {
  const build = (targetPage: number | null): string | null => {
    if (targetPage === null) return null;
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(req.query)) {
      if (typeof value === "string") params.set(key, value);
    }
    params.set("page", String(targetPage));
    params.set("limit", String(limit));
    return `${req.path}?${params.toString()}`;
  };

  return {
    self: build(page),
    next: totalPages > 0 && page < totalPages ? build(page + 1) : null,
    prev: page > 1 && totalPages > 0 ? build(page - 1) : null
  };
};

const buildCursorLinks = (req: Request, limit: number, order: "asc" | "desc", nextCursor: string | null) => ({
  self: `${req.path}?cursor=${encodeURIComponent(String(req.query.cursor || ""))}&limit=${limit}`,
  next: nextCursor
    ? `${req.path}?cursor=${encodeURIComponent(nextCursor)}&limit=${limit}&sort_by=created_at&order=${order}`
    : null,
  prev: null
});

const toCsvValue = (value: string | number): string => {
  const asString = String(value);
  if (asString.includes(",") || asString.includes("\"") || asString.includes("\n")) {
    return `"${asString.replace(/\"/g, "\"\"")}"`;
  }
  return asString;
};

const profilesToCsv = (rows: ProfileRow[]): string => {
  const headers = [
    "id",
    "name",
    "gender",
    "gender_probability",
    "age",
    "age_group",
    "country_id",
    "country_name",
    "country_probability",
    "created_at"
  ];

  const body = rows.map((row) =>
    [
      row.id,
      row.name,
      row.gender,
      row.gender_probability,
      row.age,
      row.age_group,
      row.country_id,
      row.country_name,
      row.country_probability,
      row.created_at
    ]
      .map((cell) => toCsvValue(cell))
      .join(",")
  );

  return [headers.join(","), ...body].join("\n");
};

const buildQueryResponse = (
  req: Request,
  page: number,
  limit: number,
  total: number,
  totalPages: number,
  data: ProfileRow[]
) => ({
  status: "success",
  page,
  limit,
  total,
  total_pages: totalPages,
  links: buildPaginationLinks(req, page, limit, totalPages),
  data
});

export const parseNaturalLanguageQuery = async (db: Queryable, rawQuery: string): Promise<ParsedFilters | null> => {
  // Normalize dashes: en-dash (–) and em-dash (—) to hyphen-minus (-)
  const normalized = rawQuery.replace(/[–—]/g, "-");
  const q = normalized.toLowerCase().trim().replace(/\s+/g, " ");
  if (!q) return null;

  const filters: ParsedFilters = {};
  const hasMale = /\bmale(s)?\b/.test(q) || /\bman\b/.test(q) || /\bmen\b/.test(q);
  const hasFemale = /\bfemale(s)?\b/.test(q) || /\bwoman\b/.test(q) || /\bwomen\b/.test(q);
  if (hasMale && !hasFemale) filters.gender = "male";
  if (hasFemale && !hasMale) filters.gender = "female";

  if (/\byoung\b/.test(q)) {
    filters.min_age = 16;
    filters.max_age = 24;
  }

  if (/\bchild(ren)?\b/.test(q)) filters.age_group = "child";
  if (/\bteen(age|ager|agers)?\b/.test(q)) filters.age_group = "teenager";
  if (/\badult(s)?\b/.test(q)) filters.age_group = "adult";
  if (/\bsenior(s)?\b/.test(q) || /\belderly\b/.test(q)) filters.age_group = "senior";

  // age ranges: "above 30", "below 50", "between 25 and 46", "25-46", "25 to 46"
  // Note: handles both hyphen-minus (-), en-dash (–), and em-dash (—)
  const above = q.match(/\b(?:above|over|older than|greater than)\s+(\d{1,3})\b/);
  const below = q.match(/\b(?:below|under|younger than|less than)\s+(\d{1,3})\b/);
  const between = q.match(/\bbetween\s+(\d{1,3})\s+(?:and|to|-)\s+(\d{1,3})\b/);
  const hyphenRange = q.match(/\b(\d{1,3})\s*-\s*(\d{1,3})\b/);
  const spacedRange = q.match(/\b(\d{1,3})\s+(?:to|and)\s+(\d{1,3})\b/);

  if (above) filters.min_age = Number(above[1]);
  if (below) filters.max_age = Number(below[1]);
  if (between) {
    filters.min_age = Number(between[1]);
    filters.max_age = Number(between[2]);
  } else if (hyphenRange) {
    filters.min_age = Number(hyphenRange[1]);
    filters.max_age = Number(hyphenRange[2]);
  } else if (spacedRange) {
    filters.min_age = Number(spacedRange[1]);
    filters.max_age = Number(spacedRange[2]);
  }

  // Country extraction with multiple patterns:
  // 1. "from <country>" - e.g., "from nigeria", "from burkina-faso", "from mozambique"
  // 2. Country adjectives - e.g., "nigerian", "kenyan", "mozambican"
  // 3. Standalone country names - e.g., "nigeria", "kenya", "mozambique"
  let countryName: string | null = null;

  // Pattern 1: "from <country>" - improved to better capture hyphenated names
  // Matches: "from nigeria", "from burkina-faso", "from mozambique women aged 20-35"
  const from = q.match(/\bfrom\s+([a-z\-' ]+?)(?:\s+(?:aged|with|and|above|below|under|over|older|younger|between|women?|men?|to|years?|males?|females?)|\s*$)/);
  if (from) {
    countryName = from[1].trim().replace(/\s*-\s*/g, " ").replace(/\s+/g, " ");
  }

  // Pattern 2: Country adjectives (if not found via "from")
  if (!countryName) {
    const countryAdjectives: Record<string, string> = {
      "nigerian": "Nigeria",
      "kenyan": "Kenya",
      "ghanaian": "Ghana",
      "ivorian": "Ivory Coast",
      "tanzanian": "Tanzania",
      "ugandan": "Uganda",
      "malian": "Mali",
      "burkinabe": "Burkina Faso",
      "beninese": "Benin",
      "togolese": "Togo",
      "cameroonian": "Cameroon",
      "senegalese": "Senegal",
      "sierra leonean": "Sierra Leone",
      "liberian": "Liberia",
      "guinean": "Guinea",
      "congolese": "Democratic Republic of the Congo",
      "sudanese": "Sudan",
      "egyptian": "Egypt",
      "moroccan": "Morocco",
      "algerian": "Algeria",
      "tunisian": "Tunisia",
      "southafrican": "South Africa",
      "angolan": "Angola",
      "mozambican": "Mozambique",
      "zimbabwean": "Zimbabwe",
      "zambian": "Zambia",
      "rwandan": "Rwanda",
      "burundian": "Burundi",
      "ethiopian": "Ethiopia",
      "somali": "Somalia",
      "indonesian": "Indonesia",
      "malay": "Malaysia",
      "indian": "India",
      "bangladeshi": "Bangladesh",
      "pakistani": "Pakistan",
      "thai": "Thailand",
      "vietnamese": "Vietnam",
      "philippine": "Philippines",
      "mexican": "Mexico",
      "brazilian": "Brazil",
      "american": "United States",
      "canadian": "Canada",
      "australian": "Australia",
      "british": "United Kingdom",
      "french": "France",
      "german": "Germany",
      "spanish": "Spain",
      "italian": "Italy",
      "dutch": "Netherlands",
      "swiss": "Switzerland"
    };

    for (const [adjective, country] of Object.entries(countryAdjectives)) {
      if (q.includes(adjective)) {
        countryName = country;
        break;
      }
    }
  }

  // Pattern 3: Standalone country names (if still not found)
  if (!countryName) {
    // Query database for all country names and match them against the query
    const countriesResult = await db.query("SELECT DISTINCT LOWER(country_name) as country_name FROM profiles ORDER BY country_name");
    const dbCountries = countriesResult.rows.map((r) => r.country_name as string);
    const normalizedQuery = q.replace(/[-\s]+/g, "");

    for (const dbCountry of dbCountries) {
      const normalizedCountry = dbCountry.replace(/[-\s]+/g, "");
      if (normalizedQuery.includes(normalizedCountry)) {
        // Capitalize for lookup
        countryName = dbCountry
          .split(" ")
          .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
          .join(" ");
        break;
      }
    }
  }

  // Lookup country_id from country_name
  if (countryName) {
    const result = await db.query("SELECT country_id FROM profiles WHERE LOWER(country_name) = LOWER($1) LIMIT 1", [countryName]);
    const row = result.rows[0];
    if (row?.country_id) {
      filters.country_id = String(row.country_id);
    } else {
      return null;
    }
  }

  if (Object.keys(filters).length === 0) return null;
  if (typeof filters.min_age === "number" && typeof filters.max_age === "number" && filters.min_age > filters.max_age) {
    return null;
  }

  return filters;
};

const isUniqueNameConflict = (error: unknown): boolean => {
  if (!(error instanceof Error)) return false;
  return (error as { code?: string }).code === "23505";
};

export const createProfileHandlers = () => {
  const createProfile = async (req: Request, res: Response) => {
    const parsedName = parseName(req.body?.name);
    if (!parsedName.value) {
      toError(res, parsedName.code ?? 400, parsedName.message ?? "Missing or empty parameter");
      return;
    }

    try {
      const existingResult = await pool.query("SELECT * FROM profiles WHERE LOWER(name) = LOWER($1) LIMIT 1", [
        parsedName.value
      ]);
      const existing = existingResult.rows[0];
      if (existing) {
        res.status(200).json({ status: "success", message: "Profile already exists", data: normalizeProfileRow(existing) });
        return;
      }

      const external = await getExternalData(parsedName.value);
      const countryResult = await pool.query("SELECT country_name FROM profiles WHERE country_id = $1 LIMIT 1", [
        external.countryId
      ]);

      const profile: ProfileRow = {
        id: generateUuidV7(),
        name: parsedName.value,
        gender: external.gender,
        gender_probability: external.genderProbability,
        age: external.age,
        age_group: external.ageGroup,
        country_id: external.countryId,
        country_name: countryResult.rows[0]?.country_name ? String(countryResult.rows[0].country_name) : external.countryId,
        country_probability: external.countryProbability,
        created_at: new Date().toISOString()
      };

      try {
        await pool.query(
          `INSERT INTO profiles (
            id, name, gender, gender_probability, age, age_group,
            country_id, country_name, country_probability, created_at
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
          [
            profile.id,
            profile.name,
            profile.gender,
            profile.gender_probability,
            profile.age,
            profile.age_group,
            profile.country_id,
            profile.country_name,
            profile.country_probability,
            profile.created_at
          ]
        );
        clearQueryCache();
      } catch (insertError) {
        if (isUniqueNameConflict(insertError)) {
          const existingAfterConflict = await pool.query("SELECT * FROM profiles WHERE LOWER(name) = LOWER($1) LIMIT 1", [
            parsedName.value
          ]);
          if (existingAfterConflict.rows[0]) {
            res.status(200).json({
              status: "success",
              message: "Profile already exists",
              data: normalizeProfileRow(existingAfterConflict.rows[0])
            });
            return;
          }
        }
        throw insertError;
      }

      res.status(201).json({ status: "success", data: profile });
    } catch (error) {
      const upstreamMessage = getInvalidUpstreamError(error);
      if (upstreamMessage) {
        toError(res, 502, upstreamMessage);
        return;
      }
      if (
        error instanceof TypeError ||
        (error instanceof Error &&
          (error.message === "UPSTREAM_STATUS_ERROR" || error.name === "TimeoutError" || error.name === "AbortError"))
      ) {
        toError(res, 502, "Upstream service failure");
        return;
      }
      toError(res, 500, "Server failure");
    }
  };

  const uploadProfiles = async (req: Request, res: Response) => {
    try {
      const contentType = String(req.headers["content-type"] ?? "").toLowerCase();
      if (!contentType.includes("csv") && !contentType.includes("text/plain") && !contentType.includes("application/octet-stream")) {
        toError(res, 422, "Invalid upload format");
        return;
      }
      const parser = new CsvRowParser();
      const summaryReasons = buildUploadReasonMap();
      let headerIndex: Map<string, number> | null = null;
      let totalRows = 0;
      let acceptedRows = 0;
      let inserted = 0;

      // Get dedicated connection for COPY operation (not in transaction)
      const client = await pool.connect();
      let profileIndexesDropped = false;
      let uniqueProfileIndexDropped = false;
      try {
        // Create temp table
        await client.query(`
          CREATE TEMP TABLE upload_profiles_stage (
            id UUID NOT NULL,
            name TEXT NOT NULL,
            gender TEXT NOT NULL,
            gender_probability DOUBLE PRECISION NOT NULL,
            age INTEGER NOT NULL,
            age_group TEXT NOT NULL,
            country_id TEXT NOT NULL,
            country_name TEXT NOT NULL,
            country_probability DOUBLE PRECISION NOT NULL
          );
        `);

        const copyInput = new PassThrough({ highWaterMark: 2 * 1024 * 1024 });
        const copyStream = (client as unknown as { query: (query: unknown) => NodeJS.WritableStream }).query(copyFrom(`
          COPY upload_profiles_stage (
            id, name, gender, gender_probability, age, age_group,
            country_id, country_name, country_probability
          ) FROM STDIN WITH (FORMAT csv)
        `));
        const copyDone = pipeline(copyInput, copyStream);

        req.setEncoding("utf8");

        await new Promise<void>((resolve, reject) => {
          req.on("data", (chunk: string) => {
            const parsedRows = parser.push(chunk);

            for (const row of parsedRows) {
              if (headerIndex === null) {
                const normalizedHeader = row.cells.map(normalizeCsvHeader);
                headerIndex = new Map<string, number>();
                normalizedHeader.forEach((value, index) => {
                  if (!headerIndex!.has(value)) headerIndex!.set(value, index);
                });
                continue;
              }

              totalRows += 1;
              const extracted = extractUploadCandidate(headerIndex, row);
              if (extracted.reason) {
                incrementReason(summaryReasons, extracted.reason);
                continue;
              }

              const candidate = extracted.candidate!;
              acceptedRows += 1;
              if (!copyInput.write(serializeUploadCandidate(candidate))) {
                req.pause();
              }
            }
          });

          copyInput.on("drain", () => {
            req.resume();
          });

          req.once("end", () => {
            const finalRow = parser.finish();
            if (finalRow && headerIndex !== null) {
              totalRows += 1;
              const extracted = extractUploadCandidate(headerIndex, finalRow);
              if (extracted.reason) {
                incrementReason(summaryReasons, extracted.reason);
              } else if (extracted.candidate) {
                acceptedRows += 1;
                copyInput.write(serializeUploadCandidate(extracted.candidate));
              }
            }
            copyInput.end();
          });

          req.once("error", (err) => {
            reject(err);
          });
          copyDone.then(resolve).catch(reject);
        });

        const existingProfilesResult = await client.query("SELECT COUNT(*)::int AS total FROM profiles");
        const existingProfiles = Number(existingProfilesResult.rows[0]?.total ?? 0);

        await dropProfileSecondaryIndexes(client);
        profileIndexesDropped = true;

        if (existingProfiles === 0) {
          await client.query(`DROP INDEX IF EXISTS ${PROFILE_UNIQUE_INDEX_NAME}`);
          uniqueProfileIndexDropped = true;
        }

        // Now insert from temp table
        await client.query("BEGIN");
        try {
          // Reduce durability for the merge transaction to speed up bulk load.
          // This only applies for this transaction and improves throughput.
          await client.query("SET LOCAL synchronous_commit = OFF");

          const mergedResult = await client.query(
            uniqueProfileIndexDropped
              ? `
                INSERT INTO profiles (
                  id, name, gender, gender_probability, age, age_group,
                  country_id, country_name, country_probability, created_at
                )
                SELECT DISTINCT ON (LOWER(name))
                  id, name, gender, gender_probability, age, age_group,
                  country_id, country_name, country_probability, NOW()
                FROM upload_profiles_stage
                ORDER BY LOWER(name), id
              `
              : `
                INSERT INTO profiles (
                  id, name, gender, gender_probability, age, age_group,
                  country_id, country_name, country_probability, created_at
                )
                SELECT
                  id, name, gender, gender_probability, age, age_group,
                  country_id, country_name, country_probability, NOW()
                FROM upload_profiles_stage
                ON CONFLICT (LOWER(name)) DO NOTHING
              `
          );

          inserted = mergedResult.rowCount ?? mergedResult.rows.length;
          await client.query("COMMIT");
        } catch (error) {
          await client.query("ROLLBACK");
          throw error;
        }

        incrementReason(summaryReasons, "duplicate_name", Math.max(0, acceptedRows - inserted));
        clearQueryCache();

        if (profileIndexesDropped || uniqueProfileIndexDropped) {
          const rebuildUnique = uniqueProfileIndexDropped;
          const rebuildSecondary = profileIndexesDropped;
          profileIndexesDropped = false;
          uniqueProfileIndexDropped = false;

          void (async () => {
            const rebuildClient = await pool.connect();
            try {
              if (rebuildUnique) {
                await recreateProfileUniqueIndex(rebuildClient);
              }
              if (rebuildSecondary) {
                await recreateProfileSecondaryIndexes(rebuildClient);
              }
            } catch (rebuildError) {
              console.error("Failed to rebuild profiles indexes after upload:", rebuildError);
            } finally {
              rebuildClient.release();
            }
          })();
        }
      } finally {
        client.release();
      }

      const skipped = totalRows - inserted;
      const compactReasons = Object.fromEntries(
        Object.entries(summaryReasons).filter(([, count]) => count > 0)
      ) as Partial<Record<UploadReason, number>>;

      res.status(200).json({
        status: "success",
        total_rows: totalRows,
        inserted,
        skipped,
        reasons: compactReasons
      } satisfies UploadSummary);
    } catch (error) {
      toError(res, 500, error instanceof Error ? error.message : "Server failure");
    }
  };

  const searchProfiles = async (req: Request, res: Response) => {
    try {
      if (Array.isArray(req.query.q) || typeof req.query.q !== "string" || !req.query.q.trim()) {
        toError(res, 400, "Missing or empty parameter");
        return;
      }

      const pageRaw = req.query.page;
      const limitRaw = req.query.limit;
      if (Array.isArray(pageRaw) || Array.isArray(limitRaw)) {
        toError(res, 422, "Invalid query parameters");
        return;
      }

      const page = pageRaw === undefined ? 1 : Number(pageRaw);
      const limit = limitRaw === undefined ? 10 : Number(limitRaw);
      if (!Number.isInteger(page) || page < 1 || !Number.isInteger(limit) || limit < 1 || limit > 50) {
        toError(res, 422, "Invalid query parameters");
        return;
      }

      const interpreted = await parseNaturalLanguageQuery(pool, req.query.q);
      if (!interpreted) {
        toError(res, 400, "Unable to interpret query");
        return;
      }

      const normalizedFilters = normalizeParsedFilters(interpreted);
      const cacheKey = buildQueryCacheKey("search", normalizedFilters, { page, limit, sortBy: "created_at", order: "desc" });
      const cached = queryCache.get(cacheKey) as QueryCachePayload | undefined;
      if (cached) {
        res.status(200).json({
          status: "success",
          page,
          limit,
          total: cached.total,
          total_pages: cached.total_pages,
          links: buildPaginationLinks(req, page, limit, cached.total_pages),
          data: cached.data
        });
        return;
      }

      const { clause, values } = buildWhereClause(normalizedFilters);
      const offset = (page - 1) * limit;
      const [totalResult, listResult] = await Promise.all([
        pool.query(`SELECT COUNT(*)::int AS total FROM profiles ${clause}`, values),
        pool.query(
          `SELECT * FROM profiles ${clause} ORDER BY created_at DESC, id DESC LIMIT $${values.length + 1} OFFSET $${values.length + 2}`,
          [...values, limit, offset]
        )
      ]);
      const total = Number(totalResult.rows[0]?.total ?? 0);
      const totalPages = total === 0 ? 0 : Math.ceil(total / limit);
      const data = listResult.rows.map((row) => normalizeProfileRow(row));
      queryCache.set(cacheKey, { total, total_pages: totalPages, data });

      res.status(200).json(buildQueryResponse(req, page, limit, total, totalPages, data));
    } catch {
      toError(res, 500, "Server failure");
    }
  };

  const getProfile = async (req: Request, res: Response) => {
    try {
      const profileId = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id;
      const cacheKey = buildProfileCacheKey(profileId);
      const cached = queryCache.get(cacheKey) as ProfileRow | undefined;
      if (cached) {
        res.status(200).json({ status: "success", data: cached });
        return;
      }

      const result = await pool.query("SELECT * FROM profiles WHERE id = $1 LIMIT 1", [profileId]);
      const profile = result.rows[0];
      if (!profile) {
        toError(res, 404, "Profile not found");
        return;
      }
      const normalized = normalizeProfileRow(profile);
      queryCache.set(cacheKey, normalized);
      res.status(200).json({ status: "success", data: normalized });
    } catch {
      toError(res, 500, "Server failure");
    }
  };

  const listProfiles = async (req: Request, res: Response) => {
    try {
      const parsedFilters = parseFilterQuery(req.query);
      const pageSort = parsePagingAndSort(req.query);
      if (!parsedFilters || !pageSort) {
        toError(res, 422, "Invalid query parameters");
        return;
      }

      const normalizedFilters = normalizeParsedFilters(parsedFilters);
      const cacheKey = buildQueryCacheKey("list", normalizedFilters, pageSort);
      const cached = queryCache.get(cacheKey) as QueryCachePayload | undefined;
      if (cached) {
        res.status(200).json({
          status: "success",
          page: pageSort.page ?? 1,
          limit: pageSort.limit,
          total: cached.total,
          total_pages: cached.total_pages,
          links: pageSort.cursor
            ? buildCursorLinks(req, pageSort.limit, pageSort.order, cached.next_cursor || null)
            : buildPaginationLinks(req, pageSort.page ?? 1, pageSort.limit, cached.total_pages),
          next_cursor: cached.next_cursor ?? null,
          data: cached.data
        });
        return;
      }

      const { clause, values } = buildWhereClause(normalizedFilters);
      const orderBy = `${pageSort.sortBy} ${pageSort.order.toUpperCase()}, id ${pageSort.order.toUpperCase()}`;
      const totalResultPromise = pool.query(`SELECT COUNT(*)::int AS total FROM profiles ${clause}`, values);

      if (pageSort.cursor) {
        const cursorValues = [...values];
        const cursorClause = appendCursorCondition(clause, pageSort.order, pageSort.cursor, cursorValues);
        const [totalResult, cursorResult] = await Promise.all([
          totalResultPromise,
          pool.query(
            `SELECT * FROM profiles ${cursorClause} ORDER BY ${orderBy} LIMIT $${cursorValues.length + 1}`,
            [...cursorValues, pageSort.limit + 1]
          )
        ]);
        const total = Number(totalResult.rows[0]?.total ?? 0);
        const totalPages = total === 0 ? 0 : Math.ceil(total / pageSort.limit);
        const hasMore = cursorResult.rows.length > pageSort.limit;
        const dataRows = hasMore ? cursorResult.rows.slice(0, pageSort.limit) : cursorResult.rows;
        const data = dataRows.map((row) => normalizeProfileRow(row));
        const nextCursor = hasMore && data.length > 0 ? encodeCursor(data[data.length - 1]) : null;
        queryCache.set(cacheKey, { total, total_pages: totalPages, data, next_cursor: nextCursor });

        res.status(200).json({
          status: "success",
          page: 1,
          limit: pageSort.limit,
          total,
          total_pages: totalPages,
          links: buildCursorLinks(req, pageSort.limit, pageSort.order, nextCursor),
          next_cursor: nextCursor,
          data
        });
        return;
      }

      const offset = ((pageSort.page ?? 1) - 1) * pageSort.limit;
      const [totalResult, listResult] = await Promise.all([
        totalResultPromise,
        pool.query(
          `SELECT * FROM profiles ${clause} ORDER BY ${orderBy} LIMIT $${values.length + 1} OFFSET $${values.length + 2}`,
          [...values, pageSort.limit, offset]
        )
      ]);
      const total = Number(totalResult.rows[0]?.total ?? 0);
      const totalPages = total === 0 ? 0 : Math.ceil(total / pageSort.limit);
      const data = listResult.rows.map((row) => normalizeProfileRow(row));
      queryCache.set(cacheKey, { total, total_pages: totalPages, data });

      res.status(200).json({
        status: "success",
        page: pageSort.page ?? 1,
        limit: pageSort.limit,
        total,
        total_pages: totalPages,
        links: buildPaginationLinks(req, pageSort.page ?? 1, pageSort.limit, totalPages),
        data
      });
    } catch {
      toError(res, 500, "Server failure");
    }
  };

  const exportProfiles = async (req: Request, res: Response) => {
    try {
      const format = req.query.format;
      if (format !== "csv") {
        toError(res, 422, "Invalid export format");
        return;
      }

      const parsedFilters = parseFilterQuery(req.query);
      const pageSort = parsePagingAndSort(req.query);
      if (!parsedFilters || !pageSort) {
        toError(res, 422, "Invalid query parameters");
        return;
      }

      const { clause, values } = buildWhereClause(parsedFilters);
      const orderBy = `${pageSort.sortBy} ${pageSort.order.toUpperCase()}, id ${pageSort.order.toUpperCase()}`;
      const result = await pool.query(`SELECT * FROM profiles ${clause} ORDER BY ${orderBy}`, values);
      const csv = profilesToCsv(result.rows.map((row) => normalizeProfileRow(row)));
      const timestamp = new Date().toISOString().replace(/[:.]/g, "-");

      res.setHeader("Content-Type", "text/csv");
      res.setHeader("Content-Disposition", `attachment; filename="profiles_${timestamp}.csv"`);
      res.status(200).send(csv);
    } catch {
      toError(res, 500, "Server failure");
    }
  };

  const deleteProfile = async (req: Request, res: Response) => {
    try {
      const result = await pool.query("DELETE FROM profiles WHERE id = $1", [req.params.id]);
      if (result.rowCount === 0) {
        toError(res, 404, "Profile not found");
        return;
      }
      clearQueryCache();
      res.status(204).send();
    } catch {
      toError(res, 500, "Server failure");
    }
  };

  return { createProfile, uploadProfiles, searchProfiles, getProfile, listProfiles, exportProfiles, deleteProfile };
};
