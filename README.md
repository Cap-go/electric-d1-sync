# supabase-d1-sync
Sync Supabase/Postgres tables to a Cloudflare D1 database using PGMQ and Hyperdrive.

# Sync from Postgres to D1 using PGMQ

## Overview

This project provides a Cloudflare Worker setup to replicate data changes from a Postgres database (like Supabase) to a Cloudflare D1 database using PGMQ (Postgres Message Queue) and Cloudflare Hyperdrive.

A trigger function in your source Postgres database writes change events (INSERT, UPDATE, DELETE) to a PGMQ queue (`replicate_data`). The Cloudflare Worker periodically reads messages from this queue via Hyperdrive, batches the corresponding SQL operations, and applies them to the target D1 database.

## Prerequisites

1.  **Source Postgres Database:** A Postgres database (e.g., Supabase) with the `pgmq` extension enabled.
2.  **Cloudflare Account:** Access to Cloudflare Workers, D1, and Hyperdrive.
3.  **Supabase Setup (using `supabase_migration.sql`):**
    *   This project includes a `supabase_migration.sql` script containing the necessary SQL to set up your Supabase database.
    *   **Before running:**
        *   Replace the placeholder `'<your-worker-url>/sync'` within the `process_d1_replication_batch` function definition with the actual URL of your deployed Cloudflare worker (e.g., `'https://your-worker.your-account.workers.dev/sync'`).
        *   **Set the Webhook Secret:**
            1.  **Generate a Secret:** Create a strong, random secret. You can use a password manager or generate one and append it directly to your `.env` file (create the file if it doesn't exist):
                ```bash
                echo "WEBHOOK_SECRET=$(openssl rand -base64 32)" >> .env
                ```
                Copy the generated secret value from the `.env` file for the next steps.
            2.  **Store in Supabase Vault:** Go to your Supabase Project Dashboard -> Project Settings -> Vault. Add a new secret named `d1_webhook_signature` and paste the generated secret value (copied from `.env`) there.
            3.  **Configure Worker:** Ensure this **same secret** is also set as the `WEBHOOK_SECRET` variable in your `wrangler.toml` file (for deployment) and optionally in your `.env` file (for local development with `wrangler dev`).
    *   **Run this script** in your Supabase SQL Editor.
    *   It will:
        *   Enable the `pg_cron` and `pgmq` extensions.
        *   Create the `replicate_data` PGMQ queue.
        *   Create helper functions to securely retrieve the webhook secret (`get_d1_webhook_signature`) from Supabase Vault and process the queue (`process_d1_replication_batch`). **Note:** You need to store your worker's `WEBHOOK_SECRET` in Supabase Vault under the name `d1_webhook_signature`.
        *   Create the trigger function `trigger_http_queue_post_to_function_d1` which sends changes to the PGMQ queue.
        *   Schedule a `pg_cron` job to run `process_d1_replication_batch` every 5 seconds, which calls the worker's `/sync` endpoint if there are messages in the queue.
    *   **Attach Triggers:** You **must** uncomment and customize the example `CREATE TRIGGER` statement at the end of `supabase_migration.sql` for **each table** you want to replicate. For example:
        ```sql
        CREATE TRIGGER trigger_apps_to_d1
            AFTER INSERT OR UPDATE OR DELETE ON public.apps
            FOR EACH ROW EXECUTE FUNCTION trigger_http_queue_post_to_function_d1();
        ```

## Schema Configuration (`schema.json`)

**IMPORTANT:** The file `schema.json` is the **single source of truth** for defining which tables and columns are replicated to D1, how their types are mapped, and which indexes are created.

*   **Structure:** The `schema.json` file is a JSON object where:
    *   Each top-level key represents the name of a table in your **source Postgres database** that you want to replicate.
    *   The value for each table key is an object with the following properties:
        *   `primaryKey` (string, required): The name of the column that acts as the primary key for this table.
        *   `columns` (object, required): An object where keys are the source column names you want to replicate, and values are the target **SQLite data type** (`"INTEGER"`, `"TEXT"`, `"BOOLEAN"`, or `"JSON"`) for the corresponding column in the D1 database. The worker uses this for data conversion and table creation.
            *   **Important:** Only columns listed here will be created in the D1 table and populated during replication. Columns present in the source Postgres table but **omitted** from this object will be **ignored**.
        *   `indexes` (array, optional): An array defining additional indexes to create on the D1 table (the primary key is always indexed). Each element in the array can be:
            *   A string: Represents a single column to index (e.g., `"user_id"`).
            *   An array of strings: Represents a composite index across multiple columns (e.g., `["status", "is_good_plan"]`).
*   **Customization:** You **must** modify `schema.json` to accurately reflect the tables, columns, primary keys, desired D1 types, and indexes relevant to your specific replication needs. Only the tables and columns defined here will be processed by the worker and replicated to D1.
*   **Example (`stripe_info` table):**
    ```json
    "stripe_info": {
      "primaryKey": "id",
      "columns": {
        "id": "INTEGER",
        "customer_id": "TEXT",
        "status": "TEXT",
        "trial_at": "TEXT",
        "is_good_plan": "BOOLEAN",
        // ... other columns
      },
      "indexes": [
        "customer_id", // Single index
        "trial_at",    // Single index
        ["status", "is_good_plan"] // Composite index
      ]
    }
    ```

## Setup

1.  Clone the repository:
    ```bash
    git clone <your-repo-url>
    cd supabase-d1-sync
    ```
2.  Install dependencies:
    ```bash
    npm install 
    # or bun install, yarn install
    ```
3.  Copy `.env.example` to `.env`. While the worker primarily uses `wrangler.toml` for secrets/bindings, `.env` might be used for local development (`wrangler dev`). Fill in:
    *   `DATABASE_ID`: Your Cloudflare D1 Database ID (used for local dev).
    *   `HYPERDRIVE_CONN_STRING`: Your Hyperdrive connection string (used for local dev).
    *   `WEBHOOK_SECRET`: A secure secret for the `/nuke` and `/sync` webhooks (used for local dev).
4.  Create the target D1 database if it doesn't exist:
    ```bash
    wrangler d1 create your_d1_database_name
    ```
5.  Configure Cloudflare Hyperdrive to connect to your source Postgres database. Note the connection string.
6.  Update `wrangler.toml`:
    *   Add the D1 database binding under `[[d1_databases]]` using the correct `binding` name (e.g., `DB`) and `database_id`.
    *   Add the Hyperdrive binding under `[[hyperdrive]]` using the correct `binding` name (e.g., `HYPERDRIVE_DB`) and Hyperdrive ID.
    *   Define the `WEBHOOK_SECRET` under `[vars]`.
    *   **(Optional)** Configure a cron trigger under `[triggers]` if you want the worker to poll the PGMQ queue automatically. Alternatively, trigger the `/sync` endpoint externally.
        ```toml
        # Example: Trigger sync every minute
        [triggers]
        crons = ["* * * * *"]
        ```
7.  Deploy the worker:
    ```bash
    wrangler deploy
    ```

## How it Works

1.  **Data Change:** An INSERT, UPDATE, or DELETE occurs on a tracked table in your source Postgres database.
2.  **PGMQ Trigger:** Your Postgres trigger function captures this change and writes a message detailing the operation and data to the `replicate_data` PGMQ queue.
3.  **Worker Trigger:** The Cloudflare Worker is triggered, either by:
    *   A `cron` schedule defined in `wrangler.toml`.
    *   A `POST` request to the `/sync` endpoint (requires the `WEBHOOK_SECRET`).
4.  **Queue Reading:** The worker connects to your Postgres database via the configured Hyperdrive binding.
5.  It reads a batch of messages from the `replicate_data` PGMQ queue.
6.  **Message Processing:** For each message:
    *   The worker parses the operation type (INSERT, UPDATE, DELETE) and data.
    *   It cleans the data fields and converts types according to the schema defined in `src/schema.ts` to match the D1 table structure.
    *   It generates the corresponding D1 SQL statement (`INSERT OR REPLACE`, `UPDATE`, `DELETE`).
7.  **Batch Execution:** The generated D1 SQL statements are collected into a batch (up to `BATCH_SIZE`, default 998).
8.  The worker executes the batch against the bound D1 database.
9.  **Queue Deletion:** After successfully processing the D1 batch, the worker deletes the corresponding messages from the `replicate_data` PGMQ queue using their IDs.
10. **Repeat:** The worker continues reading and processing batches until the queue is empty for this run or timeouts occur.

## Nuke Webhook (`/nuke`)

To clear data from the D1 replica, you can use the `/nuke` endpoint. This is useful for resetting the replica state during development or testing.

**Important:** Secure this endpoint by setting a strong `WEBHOOK_SECRET` in your environment variables and `wrangler.toml`. Requests must include a matching `x-webhook-signature` header.

### Nuke Entire D1 Database Contents (Tables defined in `schema.ts`)
```bash
curl -X POST https://your-worker.<your-account>.workers.dev/nuke \
  -H "x-webhook-signature: your-secret-here" \
  -H "Content-Type: application/json" \
  -d '{"type": "all"}'
```
This drops and recreates all tables defined in `src/schema.ts`.

### Nuke Specific Table in D1
```bash
curl -X POST https://your-worker.<your-account>.workers.dev/nuke \
  -H "x-webhook-signature: your-secret-here" \
  -H "Content-Type: application/json" \
  -d '{"type": "table", "table": "your_table_name"}'
```
This drops and recreates the specified table, provided it's defined in `src/schema.ts`.

## Sync Webhook (`/sync`)

Trigger the PGMQ queue processing manually by sending a POST request:

```bash
curl -X POST https://your-worker.<your-account>.workers.dev/sync \
  -H "x-webhook-signature: your-secret-here"
```
This is an alternative to using a cron trigger.

## Stopping the Sync

To stop the automatic synchronization:

1.  Remove or comment out the `[triggers]` section (specifically the `crons`) in `wrangler.toml`.
    ```toml
    # [triggers]
    # crons = ["* * * * *"]
    ```
2.  Redeploy the worker:
    ```bash
    wrangler deploy
    ```
This stops the worker from being triggered automatically. Syncing will only occur if the `/sync` endpoint is called manually.

## Resetting the Sync

To completely reset the D1 replica:

1.  **Clear D1:** Use the `/nuke` webhook with `{"type": "all"}` to drop and recreate the tables in D1.
2.  **(Optional) Clear PGMQ:** If needed, you might want to archive or clear any pending messages in the `replicate_data` PGMQ queue in your source Postgres database to avoid reprocessing old changes. Consult PGMQ documentation for queue management commands.
3.  **Restart:** Ensure the worker is running (deploy with the cron trigger if desired, or trigger `/sync` manually). The worker will start processing any *new* messages added to the PGMQ queue after the reset.
