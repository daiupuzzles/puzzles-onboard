# Puzzles Client Onboarding

Automated client onboarding for Puzzles Consulting. Creates scaffolding across 6 platforms in 2-5 minutes:

1. **Clockify** — Client + Project (billable)
2. **Asana** — Project with service-specific task sections
3. **Jira Product Discovery** — JPD project + template issues
4. **Google Drive** — Recursive copy of 25-folder template tree
5. **Google Sheets** — Copy client mastersheet + append to BRAND DISTRIBUTION
6. **Zoom** — Team chat channel (optional)

## Live URL

https://onboard.puzzles.consulting

## Auth

Asana OAuth via Supabase (same pattern as web-qa-auditor). Tasks are attributed to the logged-in user.

## Deploy

Push to `main` triggers auto-deploy via Coolify on `5.161.250.36`.

## Secrets

All secrets via Doppler `puzzles/prd`. The container runs `doppler run --` which injects env vars.

## Local Development

```bash
cd scripts
doppler run --project puzzles --config prd -- python app.py
# Open http://localhost:5050
```

## Source

Primary development happens in `skills-os/workspaces/puzzles/projects/client-onboarding/`. This repo is the deployment artifact.
