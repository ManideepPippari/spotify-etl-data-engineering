AWS EventBridge Rule – Spotify ETL Trigger

• Schedule: Every 1 hour (cron expression)
• Target: spotify_ingest_lambda
• Purpose: Pull latest Spotify tracks → store into S3/raw
• Retry policy: 2 attempts
• Dead-letter queue: disableds