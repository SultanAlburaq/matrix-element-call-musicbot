FROM node:22-bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    python3 \
    python3-venv \
    procps \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN python3 -m venv /opt/venv

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt \
    && pip install --no-cache-dir -U yt-dlp

COPY call_worker/package*.json /app/call_worker/
RUN npm ci --prefix /app/call_worker --omit=dev

COPY . /app

RUN useradd -m -u 10001 musicbot \
    && mkdir -p /app/logs /app/data /tmp/musicbot_audio \
    && chown -R musicbot:musicbot /app /tmp/musicbot_audio

USER musicbot

ENV PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD pgrep -f "python3 main.py" > /dev/null || exit 1

CMD ["python3", "main.py"]
