FROM python:3.11-slim

# Install Doppler CLI
RUN apt-get update && apt-get install -y curl gnupg && \
    curl -sLf --retry 3 --tlsv1.2 --proto "=https" \
      "https://packages.doppler.com/public/cli/gpg.DE2A7741A397C129.key" | \
      gpg --dearmor -o /usr/share/keyrings/doppler-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/doppler-archive-keyring.gpg] \
      https://packages.doppler.com/public/cli/deb/debian any-version main" | \
      tee /etc/apt/sources.list.d/doppler-cli.list && \
    apt-get update && apt-get install -y doppler && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy and install wrapper packages
COPY wrappers/ /wrappers/
RUN pip install --no-cache-dir \
    /wrappers/clockify \
    /wrappers/asana \
    /wrappers/jira \
    /wrappers/google \
    /wrappers/supabase \
    /wrappers/telegram

# Copy app code
COPY scripts/ /app/

# State directory (mount as volume for persistence)
RUN mkdir -p /app/state

EXPOSE 5050

# Single worker: app uses in-process dicts for background thread state
# 600s timeout: onboarding runs take 2-5 minutes
CMD ["doppler", "run", "--project", "puzzles", "--config", "prd", "--", \
     "gunicorn", "app:app", "-b", "0.0.0.0:5050", "-w", "1", "--timeout", "600"]
