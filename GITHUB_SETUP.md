# GitHub Automation Setup

1. Create a GitHub repository and push this folder.
2. In GitHub, open `Settings -> Secrets and variables -> Actions -> New repository secret`.
3. Add these secrets:
   - `VONAGE_API_KEY`
   - `VONAGE_API_SECRET`
   - `SLACK_WEBHOOK_URL`
4. In `Actions`, enable workflows if prompted.
5. Run `Vonage Weekly Report` once with `Run workflow` to test.

Notes:
- The schedule is set to every Wednesday at `09:00 UTC`.
- `vonage_numbers_snapshot.csv` is committed back by the workflow to keep historical continuity across runs.
