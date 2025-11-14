import { Activity, Contributor } from "@/types";
import { PGlite, types } from "@electric-sql/pglite";

let dbInstance: PGlite | null = null;

/**
 * Initialize and return PGlite database instance
 */
export function getDb(): PGlite {
  const dataPath = process.env.DB_DATA_PATH;

  if (!dataPath) {
    throw Error(
      "'DB_DATA_PATH' environment needs to be set with a path to the database data."
    );
  }

  // Initialize the database if it doesn't exist, otherwise return the existing instance.
  // This is to avoid creating a new database instance for each call to getDb().
  if (!dbInstance) {
    dbInstance = new PGlite(dataPath);
  }

  return dbInstance;
}

/**
 * Upsert contributors to the database
 * @param contributors - The contributors to upsert
 */
export async function upsertContributor(...contributors: Contributor[]) {
  const db = getDb();

  // Helper function to escape single quotes in SQL strings
  const escapeSql = (value: string | null | undefined): string => {
    if (value === null || value === undefined) return "NULL";
    return `'${String(value).replace(/'/g, "''")}'`;
  };

  // Helper function to format JSON for SQL
  const formatJson = (
    value: Record<string, string> | null | undefined
  ): string => {
    if (value === null || value === undefined) return "NULL";
    return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
  };

  // Helper function to format date for SQL
  const formatDate = (value: Date | null | undefined): string => {
    if (value === null || value === undefined) return "NULL";
    return `'${value.toISOString().split("T")[0]}'`;
  };

  await db.query(`
    INSERT INTO contributor (username, name, role, title, avatar_url, bio, social_profiles, joining_date, meta)
    VALUES ${contributors
      .map(
        (c) =>
          `(${escapeSql(c.username)}, ${escapeSql(c.name)}, ${escapeSql(
            c.role
          )}, ${escapeSql(c.title)}, ${escapeSql(c.avatar_url)}, ${escapeSql(
            c.bio
          )}, ${formatJson(c.social_profiles)}, ${formatDate(
            c.joining_date
          )}, ${formatJson(c.meta)})`
      )
      .join(",")}
    ON CONFLICT (username) DO UPDATE SET 
      name = EXCLUDED.name, 
      role = EXCLUDED.role, 
      title = EXCLUDED.title,
      avatar_url = EXCLUDED.avatar_url, 
      bio = EXCLUDED.bio, 
      social_profiles = EXCLUDED.social_profiles,
      joining_date = EXCLUDED.joining_date,
      meta = EXCLUDED.meta;
  `);
}

/**
 * Upsert activity to the database
 * @param activity - The activity to upsert
 */
export async function upsertActivity(...activities: Activity[]) {
  const db = getDb();

  await db.query(
    `
    INSERT INTO activity (slug, contributor, activity_definition, title, occured_at, link, text, points, meta)
    VALUES ${activities
      .map(
        (a) =>
          `('${a.slug}', '${a.contributor}', '${a.activity_definition}', '${
            a.title
          }', '${a.occured_at}', '${a.link}', '${a.text}', ${
            a.points
          }, '${JSON.stringify(a.meta)}')`
      )
      .join(",")}
    ON CONFLICT (slug) DO UPDATE SET contributor = EXCLUDED.contributor, activity_definition = EXCLUDED.activity_definition, title = EXCLUDED.title, occured_at = EXCLUDED.occured_at, link = EXCLUDED.link, text = EXCLUDED.text, points = EXCLUDED.points, meta = EXCLUDED.meta;
  `,
    [],
    {
      serializers: {
        [types.DATE]: (date: Date) => date.toISOString(),
      },
      parsers: {
        [types.DATE]: (date: string) => new Date(date),
      },
    }
  );
}

/**
 * Batch an array into smaller arrays of a given size
 * @param array - The array to batch
 * @param batchSize - The size of each batch
 * @returns An array of arrays
 */
function batchArray<T>(array: T[], batchSize: number): T[][] {
  const result = [];
  for (let i = 0; i < array.length; i += batchSize) {
    result.push(array.slice(i, i + batchSize));
  }
  return result;
}

function getSqlPositionalParamPlaceholders(length: number, cols: number) {
  // $1, $2, $3, $4, $5, $6, $7, $8, $9, ...
  const params = Array.from({ length: length * cols }, (_, i) => `$${i + 1}`);

  // ($1, $2, $3), ($4, $5, $6), ($7, $8, $9), ...
  return batchArray(params, cols)
    .map((p) => `\n        (${p.join(", ")})`)
    .join(", ");
}

/**
 * Add Slack messages to the slack_eod table
 * @param messages - Array of Slack messages
 * @param channel - Slack channel ID
 */
export async function addSlackEodMessages(
  messages: { id: number; user_id: string; timestamp: Date; text: string }[]
) {
  const db = getDb();

  for (const batch of batchArray(messages, 1000)) {
    const result = await db.query(
      `
      INSERT INTO slack_eod_update (id, user_id, timestamp, text)
      VALUES ${getSqlPositionalParamPlaceholders(batch.length, 4)}
      ON CONFLICT DO NOTHING; -- Ignore duplicates
    `,
      batch.flatMap((m) => [m.id, m.user_id, m.timestamp.toISOString(), m.text])
    );

    console.log(
      `Added ${result.affectedRows}/${batch.length} Slack EOD messages`
    );
  }
}
