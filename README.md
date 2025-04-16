# electric-d1-sync
Sync postgres table to a single D1 database

# Basic Sync from Postgres to D1 using ElectricSQL

## Overview

This is a basic example of how to sync data from Postgres to a single Cloudflare D1 read replica using ElectricSQL. The sync worker runs every minute as a scheduled task, and each table is synced independently for better parallelization.

## Setup

1. Clone the repository
2. Run `npm install` to install the dependencies
3. Copy `.env.example` to `.env` and fill in your environment variables
4. Create a D1 database:
   ```bash
   wrangler d1 create electric-sync
   ```
5. Update `wrangler.toml` with your D1 database ID and set a secure `WEBHOOK_SECRET`
6. Deploy the worker:
   ```bash
   wrangler deploy
   ```

## How it Works

The sync process runs as a scheduled task every minute. For each table in the database:

1. A separate sync request is made to the `/sync` endpoint
2. The worker handles each request independently, spreading the load
3. Each sync retrieves the last known offset from the `sync_state` table
4. Connects to ElectricSQL and streams changes since the last offset
5. Applies the changes to the D1 database
6. Saves the new offset for the next sync

The sync will automatically stop after 25 seconds (before the worker timeout) or when it receives an "up-to-date" message from ElectricSQL.

## Nuke Webhook

To clear data from the replica, use the `/nuke` endpoint with different options:

### Nuke Database
```bash
curl -X POST https://your-worker.workers.dev/nuke \
  -H "x-webhook-signature: your-secret-here" \
  -H "Content-Type: application/json" \
  -d '{"type": "all"}'
```

### Nuke Specific Table
```bash
curl -X POST https://your-worker.workers.dev/nuke \
  -H "x-webhook-signature: your-secret-here" \
  -H "Content-Type: application/json" \
  -d '{"type": "table", "table": "apps"}'
```

## Stopping the Sync

To stop the sync:

1. Remove the cron trigger from `wrangler.toml`
2. Redeploy the worker

## Resetting the Sync

To reset the sync:

1. Delete the data from the D1 database
2. Restart the sync process
