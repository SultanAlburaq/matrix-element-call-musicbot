# Matrix Element Call MusicBot

Discord-style music bot UX for Matrix: chat commands (`!play`, `!queue`, `!skip`, etc.) plus real playback in Element Call.

## Docker (Prebuilt Image, Recommended)

Use this when you publish an image (for example on GHCR or Docker Hub). Users only need a config file and one command.

### Linux

```bash
mkdir -p matrix-musicbot/config
cd matrix-musicbot

# Prebuilt image on GHCR
cat > docker-compose.yml <<'YAML'
services:
  musicbot:
    image: ghcr.io/sultanalburaq/matrix-element-call-musicbot:latest
    container_name: musicbot
    restart: unless-stopped
    environment:
      - CONFIG_FILE=/app/config/config.toml
    volumes:
      - ./config:/app/config:ro
      - musicbot_logs:/app/logs
      - musicbot_data:/app/data
      - musicbot_cache:/tmp/musicbot_audio

volumes:
  musicbot_logs:
  musicbot_data:
  musicbot_cache:
YAML

# Create config file
cp /path/to/config/config.example.toml config/config.toml
# Edit config/config.toml and set matrix.homeserver, matrix.user_id, matrix.access_token

docker compose up -d
```

### Windows (PowerShell)

```powershell
New-Item -ItemType Directory -Path matrix-musicbot\config -Force | Out-Null
Set-Location matrix-musicbot

@"
services:
  musicbot:
    image: ghcr.io/sultanalburaq/matrix-element-call-musicbot:latest
    container_name: musicbot
    restart: unless-stopped
    environment:
      - CONFIG_FILE=/app/config/config.toml
    volumes:
      - ./config:/app/config:ro
      - musicbot_logs:/app/logs
      - musicbot_data:/app/data
      - musicbot_cache:/tmp/musicbot_audio

volumes:
  musicbot_logs:
  musicbot_data:
  musicbot_cache:
"@ | Set-Content docker-compose.yml

# Copy /path/to/config/config.example.toml to .\config\config.toml
# Edit .\config\config.toml and set matrix.homeserver, matrix.user_id, matrix.access_token

docker compose up -d
```

## Docker (Build It Yourself From Release)

Use this when users download source release archives and build locally.

### Linux

```bash
# 1) Download and extract release source
# 2) Enter extracted folder
cd musicbot

mkdir -p config
cp config/config.example.toml config/config.toml
# Edit config/config.toml

docker compose up -d --build
```

### Windows (PowerShell)

```powershell
# 1) Download and extract release source
# 2) Enter extracted folder
Set-Location .\musicbot

New-Item -ItemType Directory -Path .\config -Force | Out-Null
Copy-Item .\config\config.example.toml .\config\config.toml
# Edit .\config\config.toml

docker compose up -d --build
```

## Run Without Docker (Raw)

Use this only if you want to run directly on the host.

### Linux

```bash
python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
npm ci --prefix call_worker

cp config/config.example.toml config.toml
# Edit config.toml

python3 main.py
```

Requires: Python 3.11+, Node.js 22+, ffmpeg, yt-dlp.

### Windows (PowerShell)

```powershell
py -3 -m venv venv
.\venv\Scripts\Activate.ps1

pip install -r requirements.txt
npm ci --prefix call_worker

Copy-Item .\config\config.example.toml .\config.toml
# Edit .\config.toml

python .\main.py
```

Requires: Python 3.11+, Node.js 22+, ffmpeg, yt-dlp (available in PATH).

## Basic Verification

After start, check:

```bash
docker compose ps
docker logs --tail=100 musicbot
```

Then in Matrix room:

- `!help`
- `!join`
- `!play never gonna give you up`
