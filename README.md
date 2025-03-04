# electric-d1-sync
Sync postgres table to d1 db multiple region

# Basic Sync from Postgres to D1 using ElectricSQL

## Overview

This is a basic example of how to sync data from Postgres to Cloudflare D1 read replicas across multiple regions using ElectricSQL. The sync worker runs every minute as a scheduled task and can also be triggered via webhook when changes occur in the database.

## Setup

1. Clone the repository
2. Run `npm install` to install the dependencies
3. Run `docker compose up` to start the Postgres and ElectricSQL servers
4. Create D1 databases for each region:
   ```bash
   wrangler d1 create electric-sync-na  # North America
   wrangler d1 create electric-sync-eu  # Europe
   wrangler d1 create electric-sync-asia # Asia
   wrangler d1 create electric-sync-sa  # South America
   wrangler d1 create electric-sync-oc  # Oceania
   wrangler d1 create electric-sync-af  # Africa
   ```
5. Update `wrangler.toml` with your D1 database IDs and set a secure `WEBHOOK_SECRET`
6. Deploy the worker:
   ```bash
   wrangler deploy
   ```

## Database Schema

The worker expects the following schema in each D1 database:

```sql
-- App Versions
CREATE TABLE IF NOT EXISTS app_versions (
    id INTEGER PRIMARY KEY,
    owner_org TEXT,
    app_id TEXT,
    name TEXT,
    r2_path TEXT,
    user_id TEXT,
    deleted BOOLEAN,
    external_url TEXT,
    checksum TEXT,
    session_key TEXT,
    storage_provider TEXT,
    min_update_version TEXT,
    manifest JSON
);

-- Channels
CREATE TABLE IF NOT EXISTS channels (
    id INTEGER PRIMARY KEY,
    name TEXT,
    app_id TEXT,
    version INTEGER,
    created_by TEXT,
    owner_org TEXT,
    public BOOLEAN,
    disable_auto_update_under_native BOOLEAN,
    disable_auto_update TEXT,
    ios BOOLEAN,
    android BOOLEAN,
    allow_device_self_set BOOLEAN,
    allow_emulator BOOLEAN,
    allow_dev BOOLEAN
);

-- Channel Devices
CREATE TABLE IF NOT EXISTS channel_devices (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER,
    app_id TEXT,
    device_id TEXT,
    owner_org TEXT
);

-- Apps
CREATE TABLE IF NOT EXISTS apps (
    id TEXT PRIMARY KEY,
    app_id TEXT,
    icon_url TEXT,
    user_id TEXT,
    name TEXT,
    last_version TEXT,
    retention INTEGER,
    owner_org TEXT,
    default_upload_channel TEXT,
    transfer_history TEXT
);

-- Organizations
CREATE TABLE IF NOT EXISTS orgs (
    id TEXT PRIMARY KEY,
    created_by TEXT,
    logo TEXT,
    name TEXT,
    management_email TEXT,
    customer_id TEXT
);

-- Stripe Info
CREATE TABLE IF NOT EXISTS stripe_info (
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    status TEXT,
    trial_at TEXT,
    is_good_plan BOOLEAN,
    mau_exceeded BOOLEAN,
    storage_exceeded BOOLEAN,
    bandwidth_exceeded BOOLEAN
);

-- Sync State
CREATE TABLE IF NOT EXISTS sync_state (
    table_name TEXT PRIMARY KEY,
    last_offset TEXT NOT NULL
);
```

## Adding data to Postgres

Run `docker compose exec postgres psql -U postgres -d electric` to start a `psql` terminal to the Postgres server. You can use this to add data to the Postgres tables.

## How it Works

The sync process runs in two ways:

1. **Scheduled Sync**: Every minute, the worker automatically checks for changes
2. **Webhook Sync**: When changes occur in the database, it can trigger an immediate sync

Each sync:
1. Retrieves the last known offset from the `sync_state` table
2. Connects to ElectricSQL and streams changes since the last offset
3. Batches the changes and applies them to all D1 databases in parallel
4. Saves the new offset for the next sync

The sync will automatically stop after 25 seconds (before the worker timeout) or when it receives an "up-to-date" message from ElectricSQL.

## Webhook Integration

To trigger an immediate sync when changes occur, send a POST request to the `/webhook` endpoint with the webhook secret in the header:

```bash
curl -X POST https://your-worker.workers.dev/webhook \
  -H "x-webhook-signature: your-secret-here"
```

You can set up database triggers or application hooks to call this endpoint whenever relevant tables change.

## Stopping the Sync

To stop the sync:

1. Remove the cron trigger from `wrangler.toml`
2. Redeploy the worker
3. Stop the Postgres and ElectricSQL servers with `docker compose down`

## Resetting the Sync

To reset the sync:

1. Delete the data from all D1 databases
2. Restart the sync process

To clear the Postgres data, you can run `docker compose down -v` to stop the Postgres and ElectricSQL servers and remove the volumes.
