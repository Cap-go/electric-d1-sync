/// <reference types="@cloudflare/workers-types" />

import { ShapeStream, isChangeMessage, isControlMessage, type Offset, type Row } from "@electric-sql/client";
import { 
  TABLE_SCHEMAS_TYPES,
  UUID_COLUMNS,
  TABLES,
  TABLE_SCHEMAS,
  TableSchema,
  SQLiteType,
} from "./schema.ts";

interface Env {
  DB: D1Database;
  ELECTRIC_URL: string;
  WEBHOOK_SECRET: string;
  ELECTRIC_SOURCE_ID: string;
  ELECTRIC_SOURCE_SECRET: string;
  WORKER_BASE_URL?: string; // Add optional base URL for self-fetch
}

interface NukeRequest {
  type: 'all' | 'table';
  table?: string;
}

interface SyncRequest {
  table: string;
}

interface SyncState {
  offset: Offset | undefined;
  shapeHandle: string | undefined;
}

// Helper function for JSON stringify to handle BigInt
function jsonReplacer(key: string, value: any): any {
    if (typeof value === 'bigint') {
        return value.toString();
    }
    return value;
}

// Function to convert values based on their type
function convertValue(value: any, type: SQLiteType): any {
  if (value === null || value === undefined)
    return null;

  // Always convert BigInt to Number or String if too large
  // D1 driver might handle Numbers, but stringify and others might not.
  // Let's convert to Number for smaller BigInts, string for larger ones.
  if (typeof value === 'bigint') {
      try {
        return Number(value); // Try converting to Number first
      } catch (e) {
          // If Number conversion fails (too large), convert to string
          console.warn(`[convertValue] BigInt too large for Number, converting to string: ${value.toString()}`);
          return value.toString();
      }
  }

  switch (type) {
    case 'INTEGER':
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
          return JSON.stringify(value, jsonReplacer);
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
        return JSON.stringify(value, jsonReplacer);
      } catch (e) {
        // Use the replacer for safe logging here too
        console.error("Error stringifying JSON:", e, "Value:", JSON.stringify(value, jsonReplacer));
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

  const schema = TABLE_SCHEMAS_TYPES[tableName as keyof typeof TABLE_SCHEMAS_TYPES];
  if (!schema) {
    console.error(`[cleanFields ${tableName}] Unknown table schema`);
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
      console.error(`[${tableName}] Invalid message value:`, value);
      throw new Error(`Invalid message value for table ${tableName}`);
    }

    // Clean and convert the values
    const cleanedValue = cleanFields(value, tableName);
    
    // Values to insert/update
    const pkValue = cleanedValue[table.primaryKey];
    
    // Handle missing primary key
    if (pkValue === undefined || pkValue === null) {
      console.error(`[${tableName}] Missing primary key:`, value);
      throw new Error(`Missing primary key for table ${tableName}`);
    }
    
    // Map column values, defaulting to null for missing values
    const values = columns.map(col => {
      const val = cleanedValue[col];
      return val === undefined ? null : val;
    });

    let operation: { sql: string; params: any[] } | null = null;
    switch (headers.operation) {
      case "insert":
        operation = {
          sql: `INSERT OR REPLACE INTO ${tableName} (${table.columns.join(", ")}) VALUES (${table.columns.map(() => "?").join(", ")})`,
          params: [pkValue, ...values],
        };
        break;
      case "update":
        operation = {
          sql: `UPDATE ${tableName} SET ${columns.map(col => `${col} = ?`).join(", ")} WHERE ${table.primaryKey} = ?`,
          params: [...values, pkValue],
        };
        break;
      case "delete":
        operation = {
          sql: `DELETE FROM ${tableName} WHERE ${table.primaryKey} = ?`,
          params: [pkValue],
        };
        break;
      default:
        console.error(`[${tableName}] Unknown operation: ${headers.operation}`);
        throw new Error(`Unknown operation: ${headers.operation}`);
    }

    if (operation) {
      // console.log(`[${tableName}] Generated SQL operation:`, JSON.stringify(operation, jsonReplacer, 2)); // Removed: Too verbose
    } else {
       console.log(`[${tableName}] No SQL operation generated for message.`);
    }
    return operation;

  } catch (error) {
    // Log only essential parts on error to avoid large logs
    console.error(`[${tableName}] Error handling message:`, error, "PK:", value?.[table.primaryKey], "Operation:", headers?.operation);
    throw error; // Re-throw to be caught by the caller if necessary
  }
}

async function checkAndCreateTables(db: D1Database) {
  const start = Date.now();
  console.log(`[DB Init] Starting database table check/creation...`);
  try {
    // Check each table with a simple SELECT
    for (const table of Object.keys(TABLE_SCHEMAS)) {
      console.log(`[DB Init] Checking/Creating table: ${table}`);
      await ensureTableExists(db, table);
      console.log(`[DB Init] Table ${table} ensured.`);
    }
    console.log(`[DB Init] All tables checked/created in ${Date.now() - start}ms`);
  } catch (error) {
    console.error(`[DB Init] Error initializing database:`, error);
    throw error;
  }
}

async function ensureTableExists(db: D1Database, table: string) {
  console.log(`[Ensure Table] Checking existence of table: ${table}`);
  try {
    // Try to select from the table
    await db.prepare(`SELECT 1 FROM ${table} LIMIT 1`).first();
    console.log(`[Ensure Table] Table ${table} exists.`);
  } catch (error) {
    // If table doesn't exist, create it
    if (error instanceof Error && error.message.includes('no such table')) {
      console.log(`[Ensure Table] Table ${table} does not exist. Creating...`);
      const schema = TABLE_SCHEMAS[table as keyof typeof TABLE_SCHEMAS];
      if (!schema) {
        console.error(`[Ensure Table] Schema not found for table: ${table}`);
        throw new Error(`Schema not found for table: ${table}`);
      }
      // Format the SQL query to remove newlines and extra spaces
      const formattedSchema = schema.replace(/\s+/g, ' ').trim();
      console.log(`[Ensure Table] Creating table ${table} with query: "${formattedSchema}"`);
      try {
        await db.exec(formattedSchema);
        console.log(`[Ensure Table] Table ${table} created successfully.`);
      } catch (creationError) {
        console.error(`[Ensure Table] Error executing CREATE TABLE for ${table}:`, creationError);
        throw creationError; // Re-throw creation error
      }
    } else {
      // Log other types of errors encountered during the check
      console.error(`[Ensure Table] Error checking table ${table}:`, error);
      throw error; // Re-throw unexpected error
    }
  }
}

async function syncTable(db: D1Database, table: TableSchema, env: Env) {
  const start = Date.now();
  const tableName = table.name;
  console.log(`[Sync ${tableName}] Starting sync process.`);
  
  // Try to acquire lock for this table
  console.log(`[Sync ${tableName}] Attempting to acquire lock.`);
  const hasLock = await acquireLock(db, tableName);
  if (!hasLock) {
    console.log(`[Sync ${tableName}] Could not acquire lock (already held or error). Skipping sync.`);
    return;
  }
  console.log(`[Sync ${tableName}] Lock acquired.`);
  
  try {
    // Get last known offset AND shape handle
    const { offset: lastKnownOffset, shapeHandle: lastKnownShapeHandle } = await getLastSyncState(db, tableName);
    console.log(`[Sync ${tableName}] Last known state: offset=${lastKnownOffset}, handle=${lastKnownShapeHandle}`);
    
    console.log(`[Sync ${tableName}] Creating ShapeStream to ${env.ELECTRIC_URL} for table ${tableName}`);
    
    // Prepare params, including shapeHandle if not an initial sync
    const customParams: Record<string, any> = {
        table: tableName,
        replica: "full",
        columns: table.columns,
        source_id: env.ELECTRIC_SOURCE_ID,
        source_secret: env.ELECTRIC_SOURCE_SECRET,
    };

    // Prepare constructor options, including offset and shapeHandle if applicable
    const streamOptions: Record<string, any> = {
        url: env.ELECTRIC_URL,
        params: customParams, // Pass only custom params here
        subscribe: false, // We want a snapshot, not continuous sync
    };

    if (lastKnownOffset) {
        streamOptions.offset = lastKnownOffset;
        console.log(`[Sync ${tableName}] Setting stream offset: ${lastKnownOffset}`);

        if (lastKnownShapeHandle) {
             streamOptions.shapeHandle = lastKnownShapeHandle; // Assuming shapeHandle is also a top-level option
             console.log(`[Sync ${tableName}] Setting stream shape handle: ${lastKnownShapeHandle}`);
        } else {
             console.warn(`[Sync ${tableName}] Performing incremental sync (offset=${lastKnownOffset}) but no shape handle found.`);
             // Electric client should handle this if shapeHandle is required
        }
    } else {
         console.log(`[Sync ${tableName}] Performing initial sync (no offset/handle).`);
    }

    // Ensure options match the expected ShapeStreamOptions type
    const typedStreamOptions: import("@electric-sql/client").ShapeStreamOptions = {
      url: env.ELECTRIC_URL,
      params: customParams,
      subscribe: false,
      ...(streamOptions.offset && { offset: streamOptions.offset }),
      ...(streamOptions.shapeHandle && { shapeHandle: streamOptions.shapeHandle }),
    };

    console.log(`[Sync ${tableName}] Final ShapeStream options:`, JSON.stringify(typedStreamOptions, jsonReplacer, 2));
    const stream = new ShapeStream(typedStreamOptions);

    let currentBatch: D1PreparedStatement[] = []; // Use D1PreparedStatement type
    const INTERNAL_BATCH_SIZE = 1000; // Process in chunks of 100
    let finalOffset: Offset | undefined;
    let messageCount = 0;
    let changeMessageCount = 0;

    console.log(`[Sync ${tableName}] Subscribing to stream...`);
    await new Promise<void>((resolve, reject) => {
      stream.subscribe(
        async (messages) => {
          messageCount += messages.length;
          console.log(`[Sync ${tableName}] Received ${messages.length} messages (total: ${messageCount}).`);
          for (const msg of messages) {
            const currentMessageIndex = changeMessageCount + 1; // For progress logging

            if (isChangeMessage(msg)) {
              changeMessageCount++;
              try {
                const sqlOperation = handleMessage(msg, table);
                if (sqlOperation) {
                  // Prepare the statement immediately
                  currentBatch.push(db.prepare(sqlOperation.sql).bind(...sqlOperation.params));

                  // Check if internal batch size is reached
                  if (currentBatch.length >= INTERNAL_BATCH_SIZE) {
                      console.log(`[Sync ${tableName}] Internal batch size (${INTERNAL_BATCH_SIZE}) reached. Executing batch...`);
                      try {
                          const internalBatchResult = await db.batch(currentBatch);
                          console.log(`[Sync ${tableName}] Internal batch executed. Results count: ${internalBatchResult.length}`);
                          // Optional: Log detailed internal batch results if needed for debugging, but keep it concise
                          // internalBatchResult.forEach((res, idx) => console.log(`  Item ${idx}: Success=${res.success}, Error=${res.error?.message}`));
                      } catch (internalBatchError) {
                          console.error(`[Sync ${tableName}] Error executing internal batch:`, internalBatchError);
                          // Depending on desired behavior, you might want to stop processing, 
                          // skip remaining messages, or try to continue.
                          // For now, log the error and clear the batch to potentially continue.
                      }
                      currentBatch = []; // Reset for the next internal batch
                      console.log(`[Sync ${tableName}] Internal batch cleared.`);
                  }
                } else {
                  // Log if handleMessage unexpectedly returns null
                  console.warn(`[Sync ${tableName}] handleMessage returned null for a change message.`);
                }
              } catch (error) {
                // Error is logged within handleMessage
                console.error(`[Sync ${tableName}] Skipping message due to error in handleMessage.`, error);
                // Continue processing other messages
              }
            } else if (isControlMessage(msg) && msg.headers.control === "up-to-date") {
              finalOffset = stream.lastOffset;
              console.log(`[Sync ${tableName}] Received 'up-to-date' control message. Final offset: ${finalOffset}`);
              // Log handle value when 'up-to-date' is received
              console.log(`[Sync ${tableName}] stream.shapeHandle at up-to-date: ${stream.shapeHandle}`);
              resolve(); // Stop processing messages for this stream
            } else {
              // Only log non-change/non-up-to-date messages if necessary
              console.log(`[Sync ${tableName}] Received non-change/non-up-to-date message type:`, msg?.headers?.control || 'Unknown type');
            }

            // Log progress every 100 messages
            if (currentMessageIndex % 100 === 0) {
                console.log(`[Sync ${tableName}] Processed message ${currentMessageIndex}/${messageCount}... Batch size: ${currentBatch.length}`);
            }
          }
          console.log(`[Sync ${tableName}] Finished processing batch of ${messages.length} messages. Change messages processed: ${changeMessageCount}. Batch size: ${currentBatch.length}.`);
        },
        (error: Error) => {
          console.error(`[Sync ${tableName}] Stream subscription error:`, error);
          reject(error); // Reject the promise on stream error
        }
      );

    }); // End of Promise
      
    console.log(`[Sync ${tableName}] Stream promise resolved. Proceeding to batch processing.`);
    console.log(`[Sync ${tableName}] Stream processing complete. Total messages received: ${messageCount}. Change messages processed: ${changeMessageCount}. Operations in batch: ${currentBatch.length}.`);
    
    // Get the shape handle AFTER the stream has potentially established it.
    // Accessing it directly - adjust if library exposes it differently.
    const currentShapeHandle = stream.shapeHandle ?? lastKnownShapeHandle;
    console.log(`[Sync ${tableName}] Shape handle after stream processing: ${currentShapeHandle}`);

    // Process any remaining items in the batch after the loop finishes
    if (currentBatch.length > 0) {
        console.log(`[Sync ${tableName}] Executing final batch of ${currentBatch.length} remaining items...`);
        try {
            const finalBatchResult = await db.batch(currentBatch);
            console.log(`[Sync ${tableName}] Final batch executed. Results count: ${finalBatchResult.length}`);
        } catch (finalBatchError) {
            console.error(`[Sync ${tableName}] Error executing final batch:`, finalBatchError);
            // If the final batch fails, we might not want to save the offset.
            // Consider how to handle this - for now, log and continue to state saving.
        }
        currentBatch = []; // Clear final batch
    }

    // Save the final state (offset/handle) regardless of whether the last batch had items,
    // as long as an offset was received from the stream.
    if (finalOffset) {
        console.log(`[Sync ${tableName}] Proceeding to save final sync state.`);
        try {
            if (!currentShapeHandle) {
                 console.warn(`[Sync ${tableName}] Final offset received, but could not determine shape handle after sync. Saving state without handle.`);
            }
            console.log(`[Sync ${tableName}] Saving final state: offset=${finalOffset}, handle=${currentShapeHandle ?? null}`); // Save null if undefined
            const stateSaveResult = await db.prepare("INSERT OR REPLACE INTO sync_state (table_name, last_offset, shape_handle) VALUES (?, ?, ?)")
                .bind(tableName, finalOffset, currentShapeHandle ?? null) // Bind null if undefined
                .run();
            console.log(`[Sync ${tableName}] Final sync state save result:`, JSON.stringify(stateSaveResult, jsonReplacer, 2));
        } catch (error) {
            console.error(`[Sync ${tableName}] Error saving final sync state:`, error);
        }
    } else {
        console.log(`[Sync ${tableName}] No final offset received from stream, cannot update sync_state.`);
    }
  } catch (error) {
    // Catch errors from stream setup or promise handling
    console.error(`[Sync ${tableName}] Critical error during sync stream setup or processing:`, error);
    // Do not re-throw, allow finally to release lock
  } finally {
    // Always release the lock
    console.log(`[Sync ${tableName}] Releasing lock.`);
    try {
      await releaseLock(db, tableName);
      console.log(`[Sync ${tableName}] Lock released.`);
    } catch (error) {
      console.error(`[Sync ${tableName}] Error releasing lock:`, error);
    }
    console.log(`[Sync ${tableName}] Sync process finished (including finally block) in ${Date.now() - start}ms.`);
  }
}

async function acquireLock(db: D1Database, tableName: string): Promise<boolean> {
  const now = Math.floor(Date.now() / 1000); // Unix timestamp
  const lockId = crypto.randomUUID(); // Unique ID for this attempt
  console.log(`[Lock ${tableName}] Attempting acquire with lockId: ${lockId}, time: ${now}`);
  try {
    // Try to insert a lock record
    await db.prepare(`
      INSERT INTO sync_lock (table_name, locked_at, lock_id)
      VALUES (?, ?, ?)
    `).bind(tableName, now, lockId).run();
    console.log(`[Lock ${tableName}] Lock acquired successfully with lockId: ${lockId}`);
    return true;
  } catch (error: any) {
    // Check if it's a UNIQUE constraint failure (expected if lock exists)
    if (error.message?.includes('UNIQUE constraint failed')) {
       console.log(`[Lock ${tableName}] Lock already exists. Checking if stale.`);
       // If insert fails due to unique constraint, check if lock is stale (older than 5 minutes)
       const lock = await db.prepare(`
         SELECT locked_at, lock_id FROM sync_lock WHERE table_name = ?
       `).bind(tableName).first() as { locked_at: number, lock_id: string } | undefined;
       
       if (lock) {
         const fiveMinutesAgo = now - (5 * 60);
         console.log(`[Lock ${tableName}] Found existing lock: ID=${lock.lock_id}, LockedAt=${lock.locked_at}, FiveMinAgo=${fiveMinutesAgo}`);
         
         if (lock.locked_at < fiveMinutesAgo) {
           console.log(`[Lock ${tableName}] Existing lock is stale (older than 5 minutes). Attempting to steal lock.`);
           // Lock is stale, try to update it (atomic compare-and-swap)
           try {
             const updateResult = await db.prepare(`
               UPDATE sync_lock 
               SET locked_at = ?, lock_id = ? 
               WHERE table_name = ? AND lock_id = ? 
             `).bind(now, lockId, tableName, lock.lock_id).run(); // Use previous lock_id to ensure atomicity

             if (updateResult.meta.changes > 0) {
                console.log(`[Lock ${tableName}] Stale lock updated successfully with new lockId: ${lockId}`);
                return true; // Successfully stole the lock
             } else {
                console.log(`[Lock ${tableName}] Failed to update stale lock (likely updated by another process between check and update).`);
                return false; // Another process updated it first
             }

           } catch (updateError) {
             console.error(`[Lock ${tableName}] Error updating stale lock:`, updateError);
             return false; // Error during update
           }
         } else {
            console.log(`[Lock ${tableName}] Existing lock is not stale. Cannot acquire.`);
            return false; // Lock exists and is not stale
         }
       } else {
          console.warn(`[Lock ${tableName}] Insert failed but could not find existing lock record. Race condition?`);
          return false; // Should not happen, but handle defensively
       }
    } else {
       // Log unexpected errors during insert
       console.error(`[Lock ${tableName}] Unexpected error acquiring lock:`, error);
       return false;
    }
  }
}

async function releaseLock(db: D1Database, tableName: string): Promise<void> {
  console.log(`[Lock ${tableName}] Attempting to release lock.`);
  try {
    // We don't strictly need lock_id for release, but deleting ensures it's gone
    const result = await db.prepare(`DELETE FROM sync_lock WHERE table_name = ?`).bind(tableName).run();
    if (result.meta.changes > 0) {
        console.log(`[Lock ${tableName}] Lock released successfully.`);
    } else {
        console.warn(`[Lock ${tableName}] Attempted to release lock, but no lock found for table.`);
    }
  } catch (error) {
     console.error(`[Lock ${tableName}] Error releasing lock:`, error);
     // Consider if this error should be propagated
  }
}

async function getLastSyncState(db: D1Database, tableName: string): Promise<SyncState> {
  console.log(`[Sync State ${tableName}] Fetching last sync state (offset and handle).`);
  try {
    const result = await db.prepare("SELECT last_offset, shape_handle FROM sync_state WHERE table_name = ?")
                         .bind(tableName)
                         .first<{ last_offset: Offset | undefined, shape_handle: string | null }>();
                         
    const offset = result?.last_offset;
    const shapeHandle = result?.shape_handle ?? undefined; // Convert null to undefined
    
    console.log(`[Sync State ${tableName}] Found state: offset=${offset}, handle=${shapeHandle}`);
    return { offset, shapeHandle };
    
  } catch (error) {
     console.error(`[Sync State ${tableName}] Error fetching last sync state:`, error);
     // Return default state if fetch fails
     return { offset: undefined, shapeHandle: undefined };
  }
}

async function handleNuke(request: Request, env: Env) {
  console.log(`[Nuke] Received nuke request to ${request.url}`);
  if (request.method !== "POST") {
    console.log(`[Nuke] Invalid method: ${request.method}`);
    return new Response("Method not allowed", { status: 405 });
  }

  const signature = request.headers.get("x-webhook-signature");
  // Avoid logging the actual signature unless necessary for debugging
  if (!signature || signature !== env.WEBHOOK_SECRET) {
    console.log(`[Nuke] Unauthorized access attempt.`);
    return new Response("Unauthorized", { status: 401 });
  }
  console.log(`[Nuke] Signature validated.`);

  let body: NukeRequest;
  try {
    body = await request.json() as NukeRequest;
    console.log(`[Nuke] Parsed request body:`, JSON.stringify(body, jsonReplacer));
  } catch (e) {
     console.error(`[Nuke] Error parsing request body:`, e);
     return new Response("Invalid request body", { status: 400 });
  }


  try {
    console.log(`[Nuke] Initializing database for nuke operation...`);
    // Initialize database to ensure the sync tables exist (needed for table-specific nuke locks)
    await checkAndCreateTables(env.DB);
    console.log(`[Nuke] Database initialized.`);
    
    let tableName: string | undefined = undefined;
    if (body.type === 'table') {
      tableName = body.table;
      if (!tableName || !TABLES.some(t => t.name === tableName)) {
        console.log(`[Nuke] Invalid table specified: ${tableName}`);
        return new Response(`Invalid table: ${tableName}`, { status: 400 });
      }
      
      console.log(`[Nuke Table ${tableName}] Attempting lock acquisition.`);
      const hasLock = await acquireLock(env.DB, tableName);
      if (!hasLock) {
        console.log(`[Nuke Table ${tableName}] Could not acquire lock.`);
        return new Response(`Cannot acquire lock for table ${tableName}`, { status: 409 });
      }
       console.log(`[Nuke Table ${tableName}] Lock acquired.`);
    }

    // Now proceed with nuking
    switch (body.type) {
      case 'all':
        console.log(`[Nuke All] Starting database nuke.`);
        await nukeDatabase(env.DB);
        console.log(`[Nuke All] Database nuke complete.`);
        return new Response("Database nuked", { status: 200 });
      
      case 'table':
        // tableName is already validated and locked
        console.log(`[Nuke Table ${tableName}] Starting table nuke.`);
        await nukeTable(env.DB, tableName!);
        console.log(`[Nuke Table ${tableName}] Table nuke complete.`);
        // Release the lock for this table
        console.log(`[Nuke Table ${tableName}] Releasing lock.`);
        await releaseLock(env.DB, tableName!);
        console.log(`[Nuke Table ${tableName}] Lock released.`);
        return new Response(`Table ${tableName} nuked`, { status: 200 });
      
      default:
        // This case should ideally not be reached if using TypeScript types properly
        console.error(`[Nuke] Invalid nuke type received: ${body.type}`);
        // Release lock if it was acquired for an invalid type somehow
        if (tableName) await releaseLock(env.DB, tableName);
        return new Response(`Invalid nuke type: ${body.type}`, { status: 400 });
    }
  } catch (error) {
    console.error("[Nuke] Error during nuke operation:", error);
    // Attempt to release lock if held during an error in the main try block
    if (body.type === 'table' && body.table) {
        try {
            console.log(`[Nuke Table ${body.table}] Attempting lock release after error.`);
            await releaseLock(env.DB, body.table);
        } catch (releaseError) {
            console.error(`[Nuke Table ${body.table}] Error releasing lock after nuke error:`, releaseError);
        }
    }
    return new Response(error instanceof Error ? error.message : "Internal server error during nuke", { status: 500 });
  }
}

async function nukeDatabase(db: D1Database) {
  console.log(`[Nuke DB] Nuking database`);
  const start = Date.now();
  
  // First clear the sync tables
  console.log(`[Nuke DB] Deleting from sync_state`);
  await db.prepare("DELETE FROM sync_state").run();
  console.log(`[Nuke DB] Deleting from sync_lock`);
  await db.prepare("DELETE FROM sync_lock").run();
  
  // Then nuke all actual data tables
  const tableNames = TABLES.map(t => t.name);
  console.log(`[Nuke DB] Nuking tables: ${tableNames.join(', ')}`);
  for (const tableName of tableNames) {
    await nukeTable(db, tableName, false); // Don't touch sync_state during individual table nuke here
  }
  
  console.log(`[Nuke DB] Database nuke completed in ${Date.now() - start}ms`);
}

async function nukeTable(db: D1Database, tableName: string, updateSyncState = true) {
  const start = Date.now();
  console.log(`[Nuke Table ${tableName}] Starting nuke process (updateSyncState=${updateSyncState})`);
  
  // Drop the table
  console.log(`[Nuke Table ${tableName}] Dropping table...`);
  try {
    await db.prepare(`DROP TABLE IF EXISTS ${tableName}`).run();
    console.log(`[Nuke Table ${tableName}] Table dropped.`);
  } catch (dropError) {
     console.error(`[Nuke Table ${tableName}] Error dropping table:`, dropError);
     // Continue to recreate if drop failed (might not exist)
  }
  
  // Recreate the table
  const schema = TABLE_SCHEMAS[tableName as keyof typeof TABLE_SCHEMAS];
  if (!schema) {
     console.error(`[Nuke Table ${tableName}] Schema not found! Cannot recreate table.`);
     throw new Error(`Schema not found for table: ${tableName}`);
  }
  console.log(`[Nuke Table ${tableName}] Recreating table...`);
  try {
    await db.exec(schema);
    console.log(`[Nuke Table ${tableName}] Table recreated.`);
  } catch (createError) {
     console.error(`[Nuke Table ${tableName}] Error recreating table:`, createError);
     throw createError; // Propagate if recreation fails
  }
  
  // Only update sync_state if this is a single table nuke (not part of a database nuke)
  if (updateSyncState) {
    console.log(`[Nuke Table ${tableName}] Deleting sync state for this table.`);
    await db.prepare("DELETE FROM sync_state WHERE table_name = ?").bind(tableName).run();
    console.log(`[Nuke Table ${tableName}] Sync state deleted.`);
  }
  
  console.log(`[Nuke Table ${tableName}] Nuke process completed in ${Date.now() - start}ms`);
}

// This function checks and creates a specific table and sync tables
async function checkAndCreateSpecificTable(db: D1Database, tableName: string) {
  const start = Date.now();
  console.log(`[Check Specific ${tableName}] Ensuring sync tables and table ${tableName} exist.`);
  // Define sync tables explicitly for clarity
  const tablesToCheck = ['sync_state', 'sync_lock', tableName];
  
  try {
    // Check and create sync_state and sync_lock tables, plus the specific table
    for (const table of tablesToCheck) {
      console.log(`[Check Specific ${tableName}] Ensuring ${table} exists...`);
      await ensureTableExists(db, table);
       console.log(`[Check Specific ${tableName}] Ensured ${table} exists.`);
    }
    console.log(`[Check Specific ${tableName}] Table ${tableName} and sync tables checked/created in ${Date.now() - start}ms`);
  } catch (error) {
    console.error(`[Check Specific ${tableName}] Error initializing table ${tableName} or sync tables:`, error);
    throw error;
  }
}

async function handleSyncRequest(request: Request, env: Env, ctx: ExecutionContext) {
  const handlerStart = Date.now();
  console.log(`[Sync Request] Received sync request to ${request.url}`);
  // Validate request method
  if (request.method !== "POST") {
    console.log(`[Sync Request] Invalid method: ${request.method}`);
    return new Response("Method not allowed", { status: 405 });
  }
  
  // Validate signature
  const signature = request.headers.get("x-webhook-signature");
  if (!signature || signature !== env.WEBHOOK_SECRET) {
    console.log(`[Sync Request] Unauthorized access attempt.`);
    return new Response("Unauthorized", { status: 401 });
  }
  console.log(`[Sync Request] Signature validated.`);
  
  let body: SyncRequest;
  try {
    body = await request.json() as SyncRequest;
    console.log(`[Sync Request] Parsed request body:`, JSON.stringify(body, jsonReplacer));
  } catch (e) {
    console.error(`[Sync Request] Error parsing request body:`, e);
    return new Response("Invalid request body", { status: 400 });
  }

  try {
    const { table: tableName } = body;
    
    // Validate table
    const table = TABLES.find(t => t.name === tableName);
    if (!table) {
      console.log(`[Sync Request] Invalid table specified: ${tableName}`);
      return new Response(`Invalid table: ${tableName}`, { status: 400 });
    }
    console.log(`[Sync Request] Valid table specified: ${tableName}`);
    
    // Removed checkAndCreateSpecificTable - assuming scheduled handler ensures tables exist first.
    console.log(`[Sync Request] Scheduling background sync (table existence checked by scheduled handler). Time: ${Date.now() - handlerStart}ms`);
    
    // Run the sync in the background using waitUntil
    console.log(`[Sync Request] Calling ctx.waitUntil for ${tableName}. Time: ${Date.now() - handlerStart}ms`);
    ctx.waitUntil(
      (async () => {
        console.log(`[Background Sync ${tableName}] Starting background execution.`);
        try {
          await syncTable(env.DB, table, env);
          console.log(`[Background Sync ${tableName}] Background execution finished successfully.`);
        } catch (error) {
          // Log error from the background task
          console.error(`[Background Sync ${tableName}] Error during background execution: ${error}`);
          // Decide if further action is needed (e.g., retry, notification)
        }
      })()
    );
    
    // Return success immediately while the sync continues in the background
    console.log(`[Sync Request] Responding 202 Accepted for ${tableName}. Time: ${Date.now() - handlerStart}ms`);
    return new Response(`Sync scheduled for ${tableName}`, { status: 202 });
  } catch (error) {
    // Catch errors from validation, table checking, or scheduling
    console.error("[Sync Request] Error handling sync request:", error);
    return new Response(error instanceof Error ? error.message : "Internal server error during sync request", { status: 500 });
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;
    console.log(`[Fetch] Received request: ${request.method} ${path}`);
    
    // Log essential env vars (avoid secrets)
    console.log(`[Fetch] Env ELECTRIC_URL: ${env.ELECTRIC_URL ? 'Set' : 'Not Set'}`);
    console.log(`[Fetch] Env ELECTRIC_SOURCE_ID: ${env.ELECTRIC_SOURCE_ID ? 'Set' : 'Not Set'}`);
    
    try {
        if (path === "/nuke") {
          return await handleNuke(request, env);
        }
        
        if (path === "/sync") {
          return await handleSyncRequest(request, env, ctx);
        }

        if (path === "/health") {
          console.log(`[Fetch] Responding to /health check`);
          return new Response("OK", { status: 200 });
        }
        
        console.log(`[Fetch] Path not found: ${path}`);
        return new Response("Not found", { status: 404 });

    } catch (error) {
       console.error(`[Fetch] Unhandled error in fetch handler for path ${path}:`, error);
       return new Response("Internal Server Error", { status: 500 });
    }
  },

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    const startTime = Date.now();
    console.log(`[Scheduled ${startTime}] Cron triggered at: ${new Date(event.scheduledTime).toISOString()}`);
    
    // Log essential env vars (avoid secrets)
    console.log(`[Scheduled ${startTime}] Env ELECTRIC_URL: ${env.ELECTRIC_URL ? 'Set' : 'Not Set'}`);
    console.log(`[Scheduled ${startTime}] Env ELECTRIC_SOURCE_ID: ${env.ELECTRIC_SOURCE_ID ? 'Set' : 'Not Set'}`);
    console.log(`[Scheduled ${startTime}] Env WEBHOOK_SECRET: ${env.WEBHOOK_SECRET ? 'Set' : 'Not Set'}`);


    try {
      // Initialize database first - ensures sync tables exist before triggering syncs
      console.log(`[Scheduled ${startTime}] Initializing database tables... Time: ${Date.now() - startTime}ms`);
      await checkAndCreateTables(env.DB);
      console.log(`[Scheduled ${startTime}] Database tables initialized. Time: ${Date.now() - startTime}ms`);
      
      // Helper function for delay
      const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
      
      // Determine the worker URL dynamically - THIS IS CRUCIAL
      // Assuming the worker is accessible via a default route or custom domain.
      // Option 1: Hardcode (less flexible, needs update if URL changes)
      // const workerUrl = new URL("https://your-worker-name.your-account.workers.dev/");
      // Option 2: Construct from request headers (NOT AVAILABLE IN SCHEDULED)
      // Option 3: Use an environment variable (Recommended)
      const workerBaseUrl = env.WORKER_BASE_URL || "https://sync.capgo.app"; // Default, but override with env var
      if (!workerBaseUrl) {
          console.error(`[Scheduled ${startTime}] WORKER_BASE_URL environment variable is not set. Cannot trigger sync requests.`);
          return; // Stop if we don't know where to send requests
      }
      console.log(`[Scheduled ${startTime}] Using worker base URL: ${workerBaseUrl}`);
      
      console.log(`[Scheduled ${startTime}] Starting staggered sync triggers... Time: ${Date.now() - startTime}ms`);
      for (const table of TABLES) {
        const syncUrl = new URL("/sync", workerBaseUrl); // Use WORKER_BASE_URL
        
        // Stagger the requests
        await delay(200); // Wait 200ms before triggering the next one
        console.log(`[Scheduled ${startTime}] Triggering fetch for ${table.name}... Time: ${Date.now() - startTime}ms`);

        // Using waitUntil: Fire-and-forget the trigger. Response/errors handled in background.
        ctx.waitUntil((async () => {
           const triggerStart = Date.now();
           try {
              const response = await fetch(syncUrl.toString(), {
                method: "POST",
                headers: {
                  "Content-Type": "application/json",
                  "x-webhook-signature": env.WEBHOOK_SECRET // Use the secret to authenticate the self-request
                },
                body: JSON.stringify({
                   table: table.name
                })
              });
              // Log the response from the /sync endpoint *within* waitUntil
              const status = response.status;
              const text = await response.text();
              console.log(`[Scheduled ${startTime} -> WaitUntil ${table.name}] Trigger Response: Status ${status}, Body: ${text}. Trigger Duration: ${Date.now() - triggerStart}ms`);
              if (!response.ok) {
                   console.error(`[Scheduled ${startTime} -> WaitUntil ${table.name}] Trigger failed: Status ${status}, Body: ${text}`);
               }
           } catch(error) {
              // Log fetch errors *within* waitUntil
              console.error(`[Scheduled ${startTime} -> WaitUntil ${table.name}] Trigger fetch error:`, error);
           }
        })());
      }
      
      // Removed Promise.all as we are now awaiting each fetch sequentially with delays
      console.log(`[Scheduled ${startTime}] All sync triggers sent sequentially. Cron execution finished in ${Date.now() - startTime}ms.`);
    } catch (error) {
      // Catch errors from checkAndCreateTables or fetch triggering logic
      console.error(`[Scheduled ${startTime}] Error during scheduled execution:`, error);
      // Consider adding monitoring/alerting here
    }
  }
};
