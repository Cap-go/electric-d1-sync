/// <reference types="@cloudflare/workers-types" />

import { ShapeStream, isChangeMessage, isControlMessage, type Offset } from "@electric-sql/client";
import { 
  TABLE_SCHEMAS_TYPES,
  UUID_COLUMNS,
  TABLES,
  TABLE_SCHEMAS,
  DBSync,
  TableSchema,
  SQLiteType,
} from "./schema.ts";

interface Env {
  DB_ENA: D1Database;  // East North America
  DB_WNA: D1Database;  // West North America
  DB_WEU: D1Database;  // West Europe
  DB_EEU: D1Database;  // East Europe
  DB_ASIA: D1Database; // Asia
  DB_OC: D1Database;   // Oceania
  ELECTRIC_URL: string;
  WEBHOOK_SECRET: string;
  ELECTRIC_SOURCE_ID: string;
  ELECTRIC_SOURCE_SECRET: string;
}

interface NukeRequest {
  type: 'all' | 'db' | 'table';
  db?: string;
  table?: string;
}

interface SyncRequest {
  region: string;
  table: string;
}

// Function to convert values based on their type
function convertValue(value: any, type: SQLiteType): any {
  if (value === null || value === undefined)
    return null;

  switch (type) {
    case 'INTEGER':
      // Handle bigint and timestamp values
      if (typeof value === 'bigint') {
        return Number(value);
      }
      // Convert timestamp to unix timestamp if it's a date string
      if (typeof value === 'string' && value.includes('T')) {
        return Math.floor(new Date(value).getTime() / 1000);
      }
      return typeof value === 'string' ? Number.parseInt(value) : value;
    case 'BOOLEAN':
      return value ? 1 : 0;
    case 'JSON':
      if (typeof value === 'string') {
        // If it's already a JSON string, return it as is
        try {
          JSON.parse(value); // Validate it's valid JSON
          return value;
        } catch (e) {
          // Not valid JSON, stringify it
          return JSON.stringify(value);
        }
      }
      
      if (Array.isArray(value) && value.length > 0 && 's3_path' in value[0]) {
        // Store as [prefix, [file_name, file_hash], [file_name, file_hash], ...]
        const prefix = value[0].s3_path.slice(0, -value[0].file_name.length);
        return JSON.stringify([
          prefix,
          ...value.map((v: any) => [v.file_name, v.file_hash]),
        ]);
      }
      
      try {
        return JSON.stringify(value);
      } catch (e) {
        console.error("Error stringifying JSON:", e, "Value:", value);
        return null;
      }
    default:
      return value;
  }
}

// Clean fields that are not in the D1 table
function cleanFields(record: any, tableName: string): Record<string, any> {
  if (!record)
    return record;

  const schema = TABLE_SCHEMAS_TYPES[tableName];
  if (!schema) {
    console.error(`Unknown table: ${tableName}`);
    return record;
  }

  // Only keep columns that exist in schema
  const cleanRecord: Record<string, any> = {};
  for (const [key, value] of Object.entries(record)) {
    // Skip if column not in schema
    if (!(key in schema)) {
      continue;
    }

    const type = schema[key];
    const convertedValue = convertValue(value, type);
    if (convertedValue !== null && convertedValue !== undefined) {
      // Make UUIDs lowercase
      if (UUID_COLUMNS.has(key) && typeof convertedValue === 'string') {
        cleanRecord[key] = convertedValue.toLowerCase();
      }
      else {
        cleanRecord[key] = convertedValue;
      }
    }
  }

  return cleanRecord;
}

// Update handleMessage function to use the new schema structure
function handleMessage(msg: any, table: TableSchema) {
  const { headers, value } = msg;
  const tableName = table.name;
  const columns = table.columns.filter(col => col !== table.primaryKey);

  try {
    // Validate input
    if (!value || typeof value !== 'object') {
      console.error(`Invalid message value for table ${tableName}:`, value);
      throw new Error(`Invalid message value for table ${tableName}`);
    }

    // Clean and convert the values
    const cleanedValue = cleanFields(value, tableName);
    
    // Values to insert/update
    const pkValue = cleanedValue[table.primaryKey];
    
    // Handle missing primary key
    if (pkValue === undefined || pkValue === null) {
      console.error(`Missing primary key for table ${tableName}:`, value);
      throw new Error(`Missing primary key for table ${tableName}`);
    }
    
    // Map column values, defaulting to null for missing values
    const values = columns.map(col => {
      const val = cleanedValue[col];
      return val === undefined ? null : val;
    });

    switch (headers.operation) {
      case "insert":
        return {
          sql: `INSERT OR REPLACE INTO ${tableName} (${table.columns.join(", ")}) VALUES (${table.columns.map(() => "?").join(", ")})`,
          params: [pkValue, ...values],
        };
      case "update":
        return {
          sql: `UPDATE ${tableName} SET ${columns.map(col => `${col} = ?`).join(", ")} WHERE ${table.primaryKey} = ?`,
          params: [...values, pkValue],
        };
      case "delete":
        return {
          sql: `DELETE FROM ${tableName} WHERE ${table.primaryKey} = ?`,
          params: [pkValue],
        };
      default:
        throw new Error(`Unknown operation: ${headers.operation}`);
    }
  } catch (error) {
    console.error(`Error handling message for table ${tableName}:`, error, "Value:", JSON.stringify(value));
    throw error;
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    const url = new URL(request.url);
    
    if (url.pathname === "/nuke") {
      return handleNuke(request, env);
    }
    
    if (url.pathname === "/sync") {
      return handleSyncRequest(request, env, ctx);
    }
    
    return new Response("Not found", { status: 404 });
  },

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    try {
      // Initialize all databases first
      const dbs: DBSync[] = [
        { db: env.DB_ENA, region: "ENA" },
        { db: env.DB_WNA, region: "WNA" },
        { db: env.DB_WEU, region: "WEU" },
        { db: env.DB_EEU, region: "EEU" },
        { db: env.DB_ASIA, region: "ASIA" },
        { db: env.DB_OC, region: "OC" },
      ];
      
      await Promise.all(dbs.map(({ db, region }) => checkAndCreateTables(db, region)));
      
      // Trigger individual sync requests for each table and database
      const promises = [];
      
      // Get the worker URL from the cron event
      const workerUrl = new URL("https://sync.capgo.app");
      
      for (const { region } of dbs) {
        for (const table of TABLES) {
          // Use fetch to call our own /sync endpoint
          const syncUrl = new URL(workerUrl);
          syncUrl.pathname = "/sync";
          
          const req = new Request(syncUrl.toString(), {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "x-webhook-signature": env.WEBHOOK_SECRET,
            },
            body: JSON.stringify({ region, table: table.name }),
          });
          
          // Execute the fetch but don't wait for it (fire and forget)
          const promise = fetch(req).then(async (resp) => {
            if (!resp.ok) {
              const body = await resp.text();
              console.error(`Error syncing ${table.name} in ${region} status: ${resp.status} ${body}`);
            }
          }).catch(err => {
            console.error(`Error triggering sync for ${table.name} in ${region}:`, err);
          });
          
          promises.push(promise);
        }
      }
      
      // Wait for all triggers to be sent (not for them to complete)
      await Promise.all(promises);
      console.log(`Triggered sync for all tables and databases`);
    } catch (error) {
      console.error("Error during scheduled sync:", error);
    }
  }
};

async function checkAndCreateTables(db: D1Database, region: string) {
  const start = Date.now();
  try {
    // Check each table with a simple SELECT
    for (const table of Object.keys(TABLE_SCHEMAS)) {
      try {
        // Try to select from the table
        await db.prepare(`SELECT 1 FROM ${table} LIMIT 1`).first();
      } catch (error) {
        // If table doesn't exist, create it
        if (error instanceof Error && error.message.includes('no such table')) {
          const schema = TABLE_SCHEMAS[table as keyof typeof TABLE_SCHEMAS];
          console.log(`[${region}] Creating table ${table} ${schema}`);
          await db.exec(schema);
        } else {
          console.error(`[${region}] Error checking/creating table ${table}:`, error);
          throw error;
        }
      }
    }
    console.log(`[${region}] Tables checked/created in ${Date.now() - start}ms`);
  } catch (error) {
    console.error(`[${region}] Error initializing database:`, error);
    throw error;
  }
}

async function syncTable(db: D1Database, table: TableSchema, region: string, env: Env) {
  const start = Date.now();
  const tableName = table.name;
  
  // Try to acquire lock for this table
  const hasLock = await acquireLock(db, tableName);
  if (!hasLock) {
    console.log(`[${region}] Another sync is already running for table ${tableName}, skipping`);
    return;
  }
  
  try {
    const lastKnownOffset = await getLastOffset(db, tableName);
    
    console.log(`[${region}] Starting sync for table ${tableName} from offset ${lastKnownOffset || 'beginning'}`);
    
    const stream = new ShapeStream({
      url: env.ELECTRIC_URL,
      params: {
        table: tableName,
        replica: "full",
        source_id: env.ELECTRIC_SOURCE_ID,
        source_secret: env.ELECTRIC_SOURCE_SECRET,
      },
      subscribe: false,
      offset: lastKnownOffset,
    });

    let currentBatch: any[] = [];
    let finalOffset: Offset | undefined;

    await new Promise<void>((resolve, reject) => {
      stream.subscribe(
        async (messages) => {
          for (const msg of messages) {
            if (isChangeMessage(msg)) {
              try {
                const sqlOperation = handleMessage(msg, table);
                if (sqlOperation) {
                  currentBatch.push(sqlOperation);
                }
              } catch (error) {
                console.error(`[${region}] Error handling message for ${tableName}:`, error);
                // Continue processing other messages
              }
            }
            else if (isControlMessage(msg) && msg.headers.control === "up-to-date") {
              finalOffset = stream.lastOffset;
              resolve();
            }
          }
        },
        (error: Error) => {
          reject(error);
        }
      );
    });
      
    // Apply changes and save offset only if we have changes
    if (currentBatch.length > 0) {
      console.log(`[${region}] Applying ${currentBatch.length} changes to table ${tableName}`);
      try {
        await db.batch(currentBatch);
        if (finalOffset) {
          await db.prepare("INSERT OR REPLACE INTO sync_state (table_name, last_offset) VALUES (?, ?)")
            .bind(tableName, finalOffset)
            .run();
        }
        console.log(`[${region}] Table ${tableName} synced in ${Date.now() - start}ms`);
      } catch (error) {
        console.error(`[${region}] Error executing batch for ${tableName}:`, error);
        // Log a sample of the batch that caused the error
        const sampleSize = Math.min(currentBatch.length, 3);
        console.error(`[${region}] Sample of ${sampleSize}/${currentBatch.length} batch items that caused the error:`, 
          JSON.stringify(currentBatch.slice(0, sampleSize), null, 2));
        throw error;
      }
    } else {
      console.log(`[${region}] No changes for table ${tableName}`);
    }
  } catch (error) {
    console.error(`[${region}] Error syncing table ${tableName}:`, error);
    throw error;
  } finally {
    // Always release the lock
    try {
      await releaseLock(db, tableName);
      console.log(`[${region}] Released lock for ${tableName}`);
    } catch (error) {
      console.error(`[${region}] Error releasing lock for ${tableName}:`, error);
    }
  }
}

async function syncDatabase(db: D1Database, region: string, env: Env) {
  const start = Date.now();
  console.log(`[${region}] Starting database sync`);
  
  for (const table of TABLES) {
    // Try to acquire lock for this table
    const hasLock = await acquireLock(db, table.name);
    if (!hasLock) {
      console.log(`[${region}] Another sync is already running for table ${table.name}, skipping`);
      continue;
    }

    try {
      await syncTable(db, table, region, env);
    } catch (error) {
      console.error(`[${region}] Error syncing table ${table.name}:`, error);
    } finally {
      // Always release the lock
      await releaseLock(db, table.name);
    }
  }
  
  console.log(`[${region}] Database sync completed in ${Date.now() - start}ms`);
}

async function acquireLock(db: D1Database, tableName: string): Promise<boolean> {
  const now = Math.floor(Date.now() / 1000); // Unix timestamp
  try {
    // Try to insert a lock record
    await db.prepare(`
      INSERT INTO sync_lock (table_name, locked_at)
      VALUES (?, ?)
    `).bind(tableName, now).run();
    return true;
  } catch (error) {
    // If insert fails, check if lock is stale (older than 5 minutes)
    const lock = await db.prepare(`
      SELECT locked_at FROM sync_lock WHERE table_name = ?
    `).bind(tableName).first() as { locked_at: number } | undefined;
    
    if (lock) {
      const fiveMinutesAgo = Math.floor((Date.now() - 5 * 60 * 1000) / 1000);
      
      if (lock.locked_at < fiveMinutesAgo) {
        // Lock is stale, try to update it
        try {
          await db.prepare(`
            UPDATE sync_lock 
            SET locked_at = ? 
            WHERE table_name = ?
          `).bind(now, tableName).run();
          return true;
        } catch (error) {
          console.error(`Error updating stale lock for ${tableName}:`, error);
          return false;
        }
      }
    }
    return false;
  }
}

async function releaseLock(db: D1Database, tableName: string): Promise<void> {
  await db.prepare(`DELETE FROM sync_lock WHERE table_name = ?`).bind(tableName).run();
}

async function handleSync(env: Env) {
  const start = Date.now();
  console.log('Starting global sync');
  
  const dbs: DBSync[] = [
    { db: env.DB_ENA, region: "ENA" },
    { db: env.DB_WNA, region: "WNA" },
    { db: env.DB_WEU, region: "WEU" },
    { db: env.DB_EEU, region: "EEU" },
    { db: env.DB_ASIA, region: "ASIA" },
    { db: env.DB_OC, region: "OC" },
  ];

  try {
    // Initialize all databases
    await Promise.all(dbs.map(({ db, region }) => checkAndCreateTables(db, region)));
    
    // Sync all databases in parallel
    await Promise.all(dbs.map(({ db, region }) => syncDatabase(db, region, env)));
    
    console.log(`Global sync completed in ${Date.now() - start}ms`);
  } catch (error) {
    console.error('Error during sync:', error);
    throw error;
  }
}

async function getLastOffset(db: D1Database, tableName: string): Promise<Offset | undefined> {
  const result = await db.prepare("SELECT last_offset FROM sync_state WHERE table_name = ?").bind(tableName).first();
  return result?.last_offset as Offset | undefined;
}

async function handleNuke(request: Request, env: Env) {
  if (request.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  const signature = request.headers.get("x-webhook-signature");
  if (!signature || signature !== env.WEBHOOK_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }

  const body = await request.json() as NukeRequest;
  const dbs: DBSync[] = [
    { db: env.DB_ENA, region: "ENA" },
    { db: env.DB_WNA, region: "WNA" },
    { db: env.DB_WEU, region: "WEU" },
    { db: env.DB_EEU, region: "EEU" },
    { db: env.DB_ASIA, region: "ASIA" },
    { db: env.DB_OC, region: "OC" },
  ];

  try {
    // First, try to acquire locks on all tables in all databases
    const lockPromises = dbs.flatMap(({ db, region }) => 
      TABLES.map(async (table) => {
        const hasLock = await acquireLock(db, table.name);
        if (!hasLock) {
          throw new Error(`Cannot acquire lock for table ${table.name} in ${region}`);
        }
        return { db, region, table };
      })
    );

    // Wait for all locks to be acquired
    await Promise.all(lockPromises);

    // Now proceed with nuking
    switch (body.type) {
      case 'all':
        await Promise.all(dbs.map(({ db, region }) => nukeDatabase(db, region)));
        return new Response("All databases nuked", { status: 200 });

      case 'db':
        if (!body.db) {
          return new Response("Database name required", { status: 400 });
        }
        const db = dbs.find(d => d.region.toLowerCase() === body.db?.toLowerCase());
        if (!db) {
          return new Response("Database not found", { status: 404 });
        }
        await nukeDatabase(db.db, db.region);
        return new Response(`Database ${db.region} nuked`, { status: 200 });

      case 'table':
        if (!body.db || !body.table) {
          return new Response("Database and table names required", { status: 400 });
        }
        const targetDb = dbs.find(d => d.region.toLowerCase() === body.db?.toLowerCase());
        if (!targetDb) {
          return new Response("Database not found", { status: 404 });
        }
        if (!TABLES.find(t => t.name === body.table)) {
          return new Response("Table not found", { status: 404 });
        }
        await nukeTable(targetDb.db, body.table, targetDb.region);
        return new Response(`Table ${body.table} nuked from ${targetDb.region}`, { status: 200 });

      default:
        return new Response("Invalid nuke type", { status: 400 });
    }
  } catch (error) {
    console.error("Error during nuke operation:", error);
    return new Response(error instanceof Error ? error.message : "Internal server error", { status: 500 });
  } finally {
    // Release all locks
    await Promise.all(dbs.flatMap(({ db, region }) => 
      TABLES.map(table => releaseLock(db, table.name))
    ));
  }
}

async function nukeDatabase(db: D1Database, region: string) {
  console.log(`[${region}] Nuking database`);
  for (const table of TABLES) {
    await nukeTable(db, table.name, region);
  }
  // Clear sync state
  await db.prepare("DELETE FROM sync_state").run();
  await db.prepare("DELETE FROM sync_lock").run();
  console.log(`[${region}] Database nuked`);
}

async function nukeTable(db: D1Database, tableName: string, region: string) {
  console.log(`[${region}] Nuking table ${tableName}`);
  // Drop the table and recreate it
  await db.prepare(`DROP TABLE IF EXISTS ${tableName}`).run();
  await db.exec(TABLE_SCHEMAS[tableName as keyof typeof TABLE_SCHEMAS]);
  await db.prepare("DELETE FROM sync_state WHERE table_name = ?").bind(tableName).run();
  console.log(`[${region}] Table ${tableName} nuked`);
}

async function handleSyncRequest(request: Request, env: Env, ctx: ExecutionContext) {
  // Validate request method
  if (request.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }
  
  // Validate signature
  const signature = request.headers.get("x-webhook-signature");
  if (!signature || signature !== env.WEBHOOK_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }
  
  try {
    // Parse request body
    const body = await request.json() as SyncRequest;
    const { region, table: tableName } = body;
    
    // Validate region
    const dbMap: Record<string, D1Database> = {
      "ENA": env.DB_ENA,
      "WNA": env.DB_WNA,
      "WEU": env.DB_WEU,
      "EEU": env.DB_EEU,
      "ASIA": env.DB_ASIA,
      "OC": env.DB_OC,
    };
    
    const db = dbMap[region];
    if (!db) {
      return new Response(`Invalid region: ${region}`, { status: 400 });
    }
    
    // Validate table
    const table = TABLES.find(t => t.name === tableName);
    if (!table) {
      return new Response(`Invalid table: ${tableName}`, { status: 400 });
    }
    
    // Run the sync in the background
    ctx.waitUntil(
      syncTable(db, table, region, env)
        .catch(error => {
          console.error(`Error syncing ${tableName} in ${region} Error: ${error}`);
        })
    );
    
    // Return success immediately while the sync continues in the background
    return new Response(`Sync started for ${tableName} in ${region}`, { status: 202 });
  } catch (error) {
    console.error("Error handling sync request:", error);
    return new Response(error instanceof Error ? error.message : "Internal server error", { status: 500 });
  }
}
