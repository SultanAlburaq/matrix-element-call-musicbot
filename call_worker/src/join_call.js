const path = require("path");
const readline = require("readline");
const { spawn } = require("child_process");
const fs = require("fs");

const dotenv = require("dotenv");
const { createClient, ClientEvent, MemoryStore } = require("matrix-js-sdk");
const { logger: rootLogger } = require("matrix-js-sdk/lib/logger");
const { MatrixRTCSessionManager, MatrixRTCSessionEvent } = require("matrix-js-sdk/lib/matrixrtc");
const {
    AudioFrame,
    AudioSource,
    LocalAudioTrack,
    Room,
    TrackPublishOptions,
    TrackSource,
    dispose,
} = require("@livekit/rtc-node");

rootLogger.setLevel("WARN");

dotenv.config({ path: path.resolve(process.cwd(), "../.env"), quiet: true });
dotenv.config({ path: path.resolve(process.cwd(), ".env"), override: false, quiet: true });

const SAMPLE_RATE = 48_000;
const CHANNELS = 1;
const FRAME_MS = 20;
const SAMPLES_PER_FRAME = (SAMPLE_RATE * FRAME_MS) / 1000;
const FRAME_BYTES = SAMPLES_PER_FRAME * CHANNELS * 2;

const REQUIRED_ENV = ["MATRIX_HOMESERVER", "MATRIX_USER_ID", "MATRIX_ACCESS_TOKEN"];
const MEMBERSHIP_MODES = new Set(["matrix2_auto", "matrix2", "legacy"]);
const STALL_TIMEOUT_MS = 10_000;
const STOP_GRACE_MS = 300;
const STOP_HARD_TIMEOUT_MS = 1200;
const VOLUME_RAMP_STEP_PER_SAMPLE = 0.0005;

function clampInt16(value) {
    if (value > 32767) return 32767;
    if (value < -32768) return -32768;
    return value;
}

function normalizeVolumePercent(value) {
    if (!Number.isFinite(value)) return 100;
    const bounded = Math.max(0, Math.min(200, Math.trunc(value)));
    return bounded;
}

function parseBoolEnv(name, defaultValue) {
    const raw = process.env[name];
    if (!raw) return defaultValue;
    const value = String(raw).trim().toLowerCase();
    if (["1", "true", "yes", "on"].includes(value)) return true;
    if (["0", "false", "no", "off"].includes(value)) return false;
    return defaultValue;
}

function parseNonNegativeIntEnv(name, defaultValue) {
    const raw = process.env[name];
    if (!raw) return defaultValue;
    const parsed = Number.parseInt(raw, 10);
    if (!Number.isFinite(parsed) || parsed < 0) return defaultValue;
    return parsed;
}

function parseMembershipModeEnv() {
    const raw = process.env.WORKER_MEMBERSHIP_MODE;
    if (!raw) return "legacy";

    const normalized = String(raw).trim().toLowerCase();
    if (normalized === "auto") {
        return "matrix2_auto";
    }
    if (MEMBERSHIP_MODES.has(normalized)) {
        return normalized;
    }
    return "legacy";
}

function useStickyMembershipEvents(membershipMode) {
    return membershipMode !== "legacy";
}

function shouldFallbackToLegacyAuth(error) {
    const status = error && typeof error.status === "number" ? error.status : null;
    return status === 404 || status === 405 || status === 501;
}

function shouldFallbackStickyJoin(mode, error) {
    if (mode === "legacy") return false;
    const message = error instanceof Error ? error.message : String(error || "");
    const normalized = message.toLowerCase();
    return normalized.includes("unsupportedstickyeventsendpointerror") || normalized.includes("sticky events");
}

function joinRtcSessionWithMode(session, { userId, deviceId, memberId }, livekitTransport, membershipMode) {
    session.joinRTCSession(
        { userId, deviceId, memberId },
        [livekitTransport],
        livekitTransport,
        {
            callIntent: "audio",
            unstableSendStickyEvents: useStickyMembershipEvents(membershipMode),
        },
    );
}

const audioSettings = {
    normalizeAudio: parseBoolEnv("NORMALIZE_AUDIO", false),
    fadeInMs: parseNonNegativeIntEnv("FADE_IN_MS", 120),
    volumePercent: parseNonNegativeIntEnv("VOLUME_PERCENT", 100),
};

const WORKER_LOG_MAX_BYTES = parseNonNegativeIntEnv("WORKER_LOG_MAX_BYTES", 2_000_000);
const WORKER_LOG_BACKUPS = parseNonNegativeIntEnv("WORKER_LOG_BACKUPS", 5);

const logFilePath = path.resolve(process.cwd(), "call_worker.log");

function rotateWorkerLogIfNeeded() {
    try {
        if (!fs.existsSync(logFilePath)) {
            return;
        }

        const stat = fs.statSync(logFilePath);
        if (!stat.isFile() || stat.size < WORKER_LOG_MAX_BYTES) {
            return;
        }

        for (let i = WORKER_LOG_BACKUPS - 1; i >= 1; i -= 1) {
            const src = `${logFilePath}.${i}`;
            const dst = `${logFilePath}.${i + 1}`;
            if (fs.existsSync(src)) {
                fs.renameSync(src, dst);
            }
        }
        fs.renameSync(logFilePath, `${logFilePath}.1`);
    } catch {
        // best effort rotation
    }
}

rotateWorkerLogIfNeeded();
const logStream = fs.createWriteStream(logFilePath, { flags: "a" });

function logLine(message) {
    const ts = new Date().toISOString();
    logStream.write(`[${ts}] ${message}\n`);
}

function emit(event) {
    logLine(`event ${JSON.stringify(event)}`);
    process.stdout.write(`${JSON.stringify(event)}\n`);
}

function decodeJwtPayload(jwt) {
    try {
        const parts = String(jwt).split(".");
        if (parts.length < 2) return null;
        const payload = Buffer.from(parts[1], "base64url").toString("utf-8");
        return JSON.parse(payload);
    } catch {
        return null;
    }
}

function parseArgs(argv) {
    const out = {};
    for (let i = 2; i < argv.length; i += 1) {
        const token = argv[i];
        if (token === "--room" && i + 1 < argv.length) {
            out.roomId = argv[i + 1];
            i += 1;
            continue;
        }
        if (token === "--help" || token === "-h") {
            out.help = true;
        }
    }
    return out;
}

function assertRequiredEnv() {
    const missing = REQUIRED_ENV.filter((name) => !process.env[name]);
    if (missing.length > 0) {
        throw new Error(`Missing required environment variables: ${missing.join(", ")}`);
    }
}

function waitForPrepared(client, timeoutMs) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            client.off(ClientEvent.Sync, onSync);
            reject(new Error(`Timed out waiting for sync PREPARED after ${timeoutMs}ms`));
        }, timeoutMs);

        const onSync = (state) => {
            if (state === "PREPARED") {
                clearTimeout(timer);
                client.off(ClientEvent.Sync, onSync);
                resolve();
            }
        };

        client.on(ClientEvent.Sync, onSync);
    });
}

function waitForJoinState(session, timeoutMs) {
    if (session.isJoined()) {
        return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            session.off(MatrixRTCSessionEvent.JoinStateChanged, onState);
            reject(new Error(`Timed out waiting for MatrixRTC join state after ${timeoutMs}ms`));
        }, timeoutMs);

        const onState = (isJoined) => {
            if (isJoined) {
                clearTimeout(timer);
                session.off(MatrixRTCSessionEvent.JoinStateChanged, onState);
                resolve();
            }
        };

        session.on(MatrixRTCSessionEvent.JoinStateChanged, onState);
    });
}

function waitForJoinOutcome(session, userId, deviceId, timeoutMs) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            cleanup();
            reject(new Error(`Timed out waiting for own RTC membership after ${timeoutMs}ms`));
        }, timeoutMs);

        const hasOwn = () => session.memberships.some((m) => m.userId === userId && m.deviceId === deviceId);

        const onMemberships = () => {
            if (hasOwn()) {
                cleanup();
                resolve();
            }
        };

        const onMembershipError = (error) => {
            const message = error instanceof Error ? error.message : String(error);
            cleanup();
            reject(new Error(message));
        };

        const cleanup = () => {
            clearTimeout(timer);
            session.off(MatrixRTCSessionEvent.MembershipsChanged, onMemberships);
            session.off(MatrixRTCSessionEvent.MembershipManagerError, onMembershipError);
        };

        session.on(MatrixRTCSessionEvent.MembershipsChanged, onMemberships);
        session.on(MatrixRTCSessionEvent.MembershipManagerError, onMembershipError);
        onMemberships();
    });
}

async function fetchWhoAmI(baseUrl, accessToken) {
    const response = await fetch(`${baseUrl}/_matrix/client/v3/account/whoami`, {
        method: "GET",
        headers: { Authorization: `Bearer ${accessToken}` },
    });
    if (!response.ok) {
        throw new Error(`whoami failed: ${response.status} ${response.statusText}`);
    }
    return response.json();
}

async function fetchJson(url) {
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`${response.status} ${response.statusText}`);
    }
    return response.json();
}

async function resolveLivekitServiceUrl({ homeserver, userId }) {
    const fromMxid = userId.split(":")[1];
    const candidates = [];
    if (fromMxid) {
        candidates.push(`https://${fromMxid}/.well-known/matrix/client`);
    }
    try {
        const hsHost = new URL(homeserver).hostname;
        candidates.push(`https://${hsHost}/.well-known/matrix/client`);
    } catch {
        // ignore malformed URL candidate
    }

    const seen = new Set();
    for (const url of candidates) {
        if (seen.has(url)) continue;
        seen.add(url);
        try {
            const wk = await fetchJson(url);
            const foci = wk["org.matrix.msc4143.rtc_foci"];
            if (Array.isArray(foci)) {
                const livekit = foci.find((entry) => entry && entry.type === "livekit" && entry.livekit_service_url);
                if (livekit) {
                    return livekit.livekit_service_url;
                }
            }
        } catch {
            // try next candidate
        }
    }

    throw new Error("Could not discover livekit_service_url from .well-known (org.matrix.msc4143.rtc_foci)");
}

async function postJson(url, body) {
    const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
    });
    const text = await response.text();
    if (!response.ok) {
        const error = new Error(`HTTP ${response.status} from ${url}`);
        error.status = response.status;
        error.body = text;
        throw error;
    }
    try {
        return JSON.parse(text);
    } catch {
        throw new Error(`Invalid JSON from ${url}`);
    }
}

async function getLivekitConfig(client, livekitServiceUrl, roomId, membership, membershipMode) {
    const openid = await client.getOpenIdToken();

    if (membershipMode === "legacy") {
        logLine("livekit auth using legacy endpoint /sfu/get");
        const cfg = await postJson(`${livekitServiceUrl}/sfu/get`, {
            room: roomId,
            openid_token: openid,
            device_id: membership.deviceId,
        });
        cfg._auth_mode = "legacy_sfu_get";
        return cfg;
    }

    try {
        logLine("livekit auth using matrix2 endpoint /get_token");
        const cfg = await postJson(`${livekitServiceUrl}/get_token`, {
            room_id: roomId,
            slot_id: "m.call#ROOM",
            openid_token: openid,
            member: {
                id: membership.memberId,
                claimed_user_id: membership.userId,
                claimed_device_id: membership.deviceId,
            },
        });
        cfg._auth_mode = "matrix2_get_token";
        return cfg;
    } catch (error) {
        if (membershipMode === "matrix2") {
            throw error;
        }

        if (shouldFallbackToLegacyAuth(error)) {
            logLine("matrix2 endpoint unavailable, fallback to /sfu/get");
            const cfg = await postJson(`${livekitServiceUrl}/sfu/get`, {
                room: roomId,
                openid_token: openid,
                device_id: membership.deviceId,
            });
            cfg._auth_mode = "legacy_sfu_get_fallback";
            return cfg;
        }
        throw error;
    }
}

class CallWorker {
    constructor({ matrixClient, rtcSession, livekitServiceUrl, roomId, userId, deviceId, membershipMode }) {
        this.matrixClient = matrixClient;
        this.rtcSession = rtcSession;
        this.livekitServiceUrl = livekitServiceUrl;
        this.roomId = roomId;
        this.userId = userId;
        this.deviceId = deviceId;
        this.membershipMode = membershipMode;

        this.livekitRoom = null;
        this.audioSource = null;
        this.audioTrack = null;
        this.currentPlaybackToken = 0;
        this.currentFfmpeg = null;
        this.playbackQueue = Promise.resolve();
        this.currentVolumeGain = normalizeVolumePercent(audioSettings.volumePercent) / 100;
        this.targetVolumeGain = this.currentVolumeGain;
    }

    setVolumePercent(value) {
        const normalizedPercent = normalizeVolumePercent(value);
        this.targetVolumeGain = normalizedPercent / 100;
        audioSettings.volumePercent = normalizedPercent;
        logLine(`volume target updated percent=${normalizedPercent} gain=${this.targetVolumeGain.toFixed(3)}`);
    }

    enqueuePlay(inputSource, title = null, sourceType = "file") {
        const run = async () => {
            await this.playSource(inputSource, title, sourceType);
        };
        this.playbackQueue = this.playbackQueue.then(run, run);
        return this.playbackQueue;
    }

    async waitForFfmpegExit(timeoutMs = 6000) {
        const ffmpeg = this.currentFfmpeg;
        if (!ffmpeg) {
            return;
        }
        if (ffmpeg.exitCode !== null || ffmpeg.killed === true) {
            return;
        }

        await Promise.race([
            new Promise((resolve) => ffmpeg.once("close", resolve)),
            new Promise((_, reject) => setTimeout(() => reject(new Error("ffmpeg exit timeout")), timeoutMs)),
        ]);
    }

    async waitForProcessExit(ffmpeg, timeoutMs) {
        if (!ffmpeg) return;
        if (ffmpeg.exitCode !== null || ffmpeg.killed === true) return;
        await Promise.race([
            new Promise((resolve) => ffmpeg.once("close", resolve)),
            new Promise((_, reject) => setTimeout(() => reject(new Error("ffmpeg exit timeout")), timeoutMs)),
        ]);
    }

    async waitForCloseCode(ffmpeg) {
        if (!ffmpeg) {
            return -1;
        }
        if (ffmpeg.exitCode !== null) {
            return ffmpeg.exitCode;
        }
        const code = await new Promise((resolve) => ffmpeg.once("close", resolve));
        if (typeof code === "number") {
            return code;
        }
        return ffmpeg.exitCode ?? -1;
    }

    async connectLivekit() {
        if (this.livekitRoom) {
            return;
        }

        const membership = {
            userId: this.userId,
            deviceId: this.deviceId,
            memberId: `${this.userId}:${this.deviceId}`,
        };
        const config = await getLivekitConfig(
            this.matrixClient,
            this.livekitServiceUrl,
            this.roomId,
            membership,
            this.membershipMode,
        );
        if (!config || !config.url || !config.jwt) {
            throw new Error("Invalid LiveKit config returned by authorization service");
        }
        const jwtPayload = decodeJwtPayload(config.jwt);
        if (jwtPayload) {
            logLine(
                `livekit jwt identity=${jwtPayload.sub || "?"} room=${jwtPayload.video?.room || "?"} ` +
                    `canPublish=${jwtPayload.video?.canPublish}`,
            );
        }

        const room = new Room();
        await room.connect(config.url, config.jwt, { autoSubscribe: true, dynacast: true });
        logLine(`livekit connected auth_mode=${config._auth_mode || "unknown"}`);

        this.audioSource = new AudioSource(SAMPLE_RATE, CHANNELS);
        this.audioTrack = LocalAudioTrack.createAudioTrack("musicbot-audio", this.audioSource);
        const options = new TrackPublishOptions();
        options.source = TrackSource.SOURCE_MICROPHONE;
        const publication = await room.localParticipant.publishTrack(this.audioTrack, options);
        logLine(`published local audio track sid=${publication?.trackSid || "unknown"}`);

        try {
            await Promise.race([
                publication.waitForSubscription(),
                new Promise((_, reject) => setTimeout(() => reject(new Error("subscription timeout")), 12000)),
            ]);
            logLine("local audio track has at least one subscriber");
        } catch {
            logLine("local audio track subscription not observed within timeout");
        }

        this.livekitRoom = room;
        emit({ event: "livekit_connected", auth_mode: config._auth_mode || "unknown" });
    }

    async playSource(inputSource, title = null, sourceType = "file") {
        await this.connectLivekit();
        await this.stopPlayback();
        try {
            await this.waitForFfmpegExit(6000);
        } catch {
            logLine("ffmpeg did not exit in time before new playback; continuing");
        }

        const playbackToken = this.currentPlaybackToken + 1;
        this.currentPlaybackToken = playbackToken;

        const sourceLabel = String(inputSource || "");
        const isStream = sourceType === "url";

        emit({
            event: "play_started",
            source: sourceLabel,
            file: isStream ? undefined : sourceLabel,
            url: isStream ? sourceLabel : undefined,
            title,
        });
        logLine(
            `play begin ${isStream ? "url" : "file"}=${sourceLabel} title=${title || ""} ` +
                `volume_target=${(this.targetVolumeGain * 100).toFixed(0)}%`,
        );

        const audioFilters = [];
        if (audioSettings.normalizeAudio) {
            audioFilters.push("loudnorm=I=-16:TP=-1.5:LRA=11");
        }
        if (audioSettings.fadeInMs > 0) {
            audioFilters.push(`afade=t=in:st=0:d=${(audioSettings.fadeInMs / 1000).toFixed(3)}`);
        }

        const ffmpegArgs = [
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
        ];
        if (isStream) {
            ffmpegArgs.push(
                "-reconnect",
                "1",
                "-reconnect_streamed",
                "1",
                "-reconnect_delay_max",
                "5",
            );
        }
        ffmpegArgs.push("-i", sourceLabel);
        if (audioFilters.length > 0) {
            ffmpegArgs.push("-af", audioFilters.join(","));
            logLine(`ffmpeg audio filters=${audioFilters.join(",")}`);
        }
        ffmpegArgs.push("-f", "s16le", "-ac", "1", "-ar", "48000", "pipe:1");

        const ffmpeg = spawn(
            "ffmpeg",
            ffmpegArgs,
            { stdio: ["ignore", "pipe", "pipe"] },
        );
        this.currentFfmpeg = ffmpeg;
        logLine(`ffmpeg spawned pid=${ffmpeg.pid || "unknown"}`);

        let ffmpegErr = "";
        ffmpeg.on("error", (err) => {
            logLine(`ffmpeg process error: ${err.message}`);
        });
        ffmpeg.stderr.on("data", (chunk) => {
            ffmpegErr += chunk.toString("utf-8");
        });

        let pending = Buffer.alloc(0);
        let sentFrames = 0;
        let maxAbs = 0;
        let firstNonSilentLogged = false;
        let firstChunkSeen = false;
        let lastFrameAt = Date.now();

        const stallInterval = setInterval(() => {
            if (this.currentPlaybackToken !== playbackToken) {
                return;
            }
            if (!firstChunkSeen) {
                return;
            }
            const msSinceFrame = Date.now() - lastFrameAt;
            if (msSinceFrame > STALL_TIMEOUT_MS && this.currentFfmpeg && this.currentFfmpeg.exitCode === null) {
                logLine(`stall detected after ${msSinceFrame}ms without frame; terminating ffmpeg`);
                this.currentFfmpeg.kill("SIGTERM");
            }
        }, 2_000);

        try {
            for await (const chunk of ffmpeg.stdout) {
                if (this.currentPlaybackToken !== playbackToken) {
                    break;
                }
                if (!firstChunkSeen) {
                    firstChunkSeen = true;
                    logLine(`ffmpeg first stdout chunk bytes=${chunk.length}`);
                }
                pending = pending.length ? Buffer.concat([pending, chunk]) : Buffer.from(chunk);

                while (pending.length >= FRAME_BYTES) {
                    const frameBuffer = pending.subarray(0, FRAME_BYTES);
                    pending = pending.subarray(FRAME_BYTES);

                    const sourceSamples = new Int16Array(
                        frameBuffer.buffer,
                        frameBuffer.byteOffset,
                        SAMPLES_PER_FRAME * CHANNELS,
                    );
                    const frameData = new Int16Array(SAMPLES_PER_FRAME * CHANNELS);
                    frameData.set(sourceSamples);

                    let gain = this.currentVolumeGain;
                    const targetGain = this.targetVolumeGain;
                    for (let i = 0; i < frameData.length; i += 1) {
                        if (gain < targetGain) {
                            gain = Math.min(targetGain, gain + VOLUME_RAMP_STEP_PER_SAMPLE);
                        } else if (gain > targetGain) {
                            gain = Math.max(targetGain, gain - VOLUME_RAMP_STEP_PER_SAMPLE);
                        }
                        frameData[i] = clampInt16(Math.round(frameData[i] * gain));
                    }
                    this.currentVolumeGain = gain;

                    for (let i = 0; i < frameData.length; i += 1) {
                        const v = Math.abs(frameData[i]);
                        if (v > maxAbs) {
                            maxAbs = v;
                        }
                    }
                    if (!firstNonSilentLogged && maxAbs > 0) {
                        firstNonSilentLogged = true;
                        logLine(`first non-silent samples observed max_abs=${maxAbs}`);
                    }

                    const frame = new AudioFrame(frameData, SAMPLE_RATE, CHANNELS, SAMPLES_PER_FRAME);
                    await this.audioSource.captureFrame(frame);
                    sentFrames += 1;
                    lastFrameAt = Date.now();
                    if (sentFrames === 1) {
                        logLine("first audio frame captured");
                    }
                    if (sentFrames % 250 === 0) {
                        logLine(`audio progress frames=${sentFrames}`);
                    }
                }
            }

            if (this.currentPlaybackToken === playbackToken && pending.length > 0) {
                const padded = Buffer.alloc(FRAME_BYTES);
                pending.copy(padded, 0, 0, pending.length);
                const sourceSamples = new Int16Array(
                    padded.buffer,
                    padded.byteOffset,
                    SAMPLES_PER_FRAME * CHANNELS,
                );
                const frameData = new Int16Array(SAMPLES_PER_FRAME * CHANNELS);
                frameData.set(sourceSamples);

                let gain = this.currentVolumeGain;
                const targetGain = this.targetVolumeGain;
                for (let i = 0; i < frameData.length; i += 1) {
                    if (gain < targetGain) {
                        gain = Math.min(targetGain, gain + VOLUME_RAMP_STEP_PER_SAMPLE);
                    } else if (gain > targetGain) {
                        gain = Math.max(targetGain, gain - VOLUME_RAMP_STEP_PER_SAMPLE);
                    }
                    frameData[i] = clampInt16(Math.round(frameData[i] * gain));
                }
                this.currentVolumeGain = gain;

                const frame = new AudioFrame(frameData, SAMPLE_RATE, CHANNELS, SAMPLES_PER_FRAME);
                await this.audioSource.captureFrame(frame);
                sentFrames += 1;
                lastFrameAt = Date.now();
            }

            const exitCode = await this.waitForCloseCode(ffmpeg);
            this.currentFfmpeg = null;
            logLine(`ffmpeg closed code=${exitCode}`);

            if (this.currentPlaybackToken !== playbackToken) {
                emit({
                    event: "play_stopped",
                    source: sourceLabel,
                    file: isStream ? undefined : sourceLabel,
                    url: isStream ? sourceLabel : undefined,
                    title,
                });
                return;
            }
            if (exitCode !== 0) {
                throw new Error(`ffmpeg exited with code ${exitCode}: ${ffmpegErr.trim()}`);
            }
            if (sentFrames === 0) {
                throw new Error("No audio frames were captured from ffmpeg output");
            }

            await this.audioSource.waitForPlayout();
            logLine(
                `playback complete title=${title || ""} frames=${sentFrames} seconds=${(sentFrames * FRAME_MS) / 1000} max_abs=${maxAbs}`,
            );
            emit({
                event: "play_ended",
                source: sourceLabel,
                file: isStream ? undefined : sourceLabel,
                url: isStream ? sourceLabel : undefined,
                title,
            });
        } catch (error) {
            if (this.currentPlaybackToken !== playbackToken) {
                emit({
                    event: "play_stopped",
                    source: sourceLabel,
                    file: isStream ? undefined : sourceLabel,
                    url: isStream ? sourceLabel : undefined,
                    title,
                });
                return;
            }
            throw error;
        } finally {
            clearInterval(stallInterval);
        }
    }

    async stopPlayback() {
        this.currentPlaybackToken += 1;
        const ffmpeg = this.currentFfmpeg;
        if (!ffmpeg || ffmpeg.exitCode !== null || ffmpeg.killed === true) {
            return;
        }

        ffmpeg.kill("SIGTERM");
        try {
            await this.waitForProcessExit(ffmpeg, STOP_GRACE_MS);
            return;
        } catch {
            logLine(`ffmpeg did not exit within ${STOP_GRACE_MS}ms; escalating to SIGKILL`);
        }

        try {
            ffmpeg.kill("SIGKILL");
        } catch {
            // best effort
        }

        try {
            await this.waitForProcessExit(ffmpeg, STOP_HARD_TIMEOUT_MS);
        } catch {
            logLine(`ffmpeg did not exit after SIGKILL within ${STOP_HARD_TIMEOUT_MS}ms`);
        }
    }

    async shutdown() {
        await this.stopPlayback();

        if (this.audioTrack) {
            await this.audioTrack.close();
            this.audioTrack = null;
        }
        this.audioSource = null;

        if (this.livekitRoom) {
            await this.livekitRoom.disconnect();
            this.livekitRoom = null;
        }

        await this.rtcSession.leaveRoomSession(5_000);
    }
}

async function main() {
    const args = parseArgs(process.argv);
    if (args.help) {
        console.log("Usage: node src/join_call.js --room <room-id>");
        console.log("You can also provide MATRIX_ROOM_ID in environment.");
        return;
    }

    assertRequiredEnv();

    const roomId = args.roomId || process.env.MATRIX_ROOM_ID;
    if (!roomId) {
        throw new Error("Missing room id. Pass --room <room-id> or set MATRIX_ROOM_ID");
    }

    const homeserver = process.env.MATRIX_HOMESERVER;
    const userId = process.env.MATRIX_USER_ID;
    const accessToken = process.env.MATRIX_ACCESS_TOKEN;
    const membershipMode = parseMembershipModeEnv();

    const whoami = await fetchWhoAmI(homeserver, accessToken);
    const deviceId = process.env.MATRIX_DEVICE_ID || whoami.device_id;
    if (!deviceId) {
        throw new Error("Could not determine MATRIX_DEVICE_ID (whoami returned no device_id)");
    }

    const livekitServiceUrl = await resolveLivekitServiceUrl({ homeserver, userId });
    const nodeMajor = Number(process.versions.node.split(".")[0] || 0);
    if (nodeMajor > 22) {
        logLine(`warning running on Node ${process.versions.node}; LiveKit is best-tested on Node 22 LTS`);
    }
    logLine(
        `audio settings normalize=${audioSettings.normalizeAudio} fade_in_ms=${audioSettings.fadeInMs} volume_percent=${audioSettings.volumePercent}`,
    );
    logLine(`worker start room=${roomId} membership_mode=${membershipMode}`);

    const client = createClient({
        baseUrl: homeserver,
        accessToken,
        userId,
        deviceId,
        store: new MemoryStore(),
    });
    client.startClient({ initialSyncLimit: 1, lazyLoadMembers: true });
    await waitForPrepared(client, 45_000);

    const room = await client.joinRoom(roomId);
    const sessionManager = new MatrixRTCSessionManager(rootLogger, client, {
        application: "m.call",
        id: "ROOM",
    });
    sessionManager.start();

    const session = sessionManager.getRoomSession(room);

    let effectiveMembershipMode = membershipMode;
    session.on(MatrixRTCSessionEvent.MembershipManagerError, (error) => {
        const message = error instanceof Error ? error.message : String(error);
        if (shouldFallbackStickyJoin(effectiveMembershipMode, message)) {
            logLine(`membership manager reported sticky-events incompatibility: ${message}`);
            return;
        }
        emit({ event: "error", message: `Membership manager error: ${message}` });
    });

    const memberId = `${userId}:${deviceId}`;
    const livekitTransport = {
        type: "livekit",
        livekit_service_url: livekitServiceUrl,
    };

    joinRtcSessionWithMode(session, { userId, deviceId, memberId }, livekitTransport, effectiveMembershipMode);

    try {
        await waitForJoinState(session, 20_000);
        await waitForJoinOutcome(session, userId, deviceId, 20_000);
    } catch (error) {
        if (!shouldFallbackStickyJoin(effectiveMembershipMode, error)) {
            throw error;
        }

        const notice = "Server lacks sticky events; fell back to legacy compatibility mode. Require PL50 (Moderator).";
        emit({ event: "compatibility_notice", roomId, message: notice });
        logLine(notice);

        try {
            await session.leaveRoomSession(5_000);
        } catch {
            // best effort cleanup before retry
        }

        effectiveMembershipMode = "legacy";
        joinRtcSessionWithMode(session, { userId, deviceId, memberId }, livekitTransport, effectiveMembershipMode);
        await waitForJoinState(session, 20_000);
        await waitForJoinOutcome(session, userId, deviceId, 20_000);
    }

    const worker = new CallWorker({
        matrixClient: client,
        rtcSession: session,
        livekitServiceUrl,
        roomId,
        userId,
        deviceId,
        membershipMode: effectiveMembershipMode,
    });

    await worker.connectLivekit();
    emit({ event: "joined", roomId, mode: effectiveMembershipMode });

    const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });

    let shuttingDown = false;
    const shutdown = async () => {
        if (shuttingDown) return;
        shuttingDown = true;

        rl.close();
        await worker.shutdown();
        sessionManager.stop();
        client.stopClient();
        await dispose();
        emit({ event: "left", roomId });
        logStream.end();
    };

    process.once("SIGTERM", () => {
        void shutdown().finally(() => process.exit(0));
    });
    process.once("SIGINT", () => {
        void shutdown().finally(() => process.exit(0));
    });

    for await (const line of rl) {
        const text = line.trim();
        if (!text) continue;

        let command;
        try {
            command = JSON.parse(text);
        } catch {
            emit({ event: "error", message: "Invalid JSON command" });
            continue;
        }

        try {
            if (command.type === "play") {
                const filePath = typeof command.file === "string" ? command.file : null;
                const streamUrl = typeof command.url === "string" ? command.url : null;
                if (!filePath && !streamUrl) {
                    throw new Error("Missing 'file' or 'url' in play command");
                }
                const sourceValue = filePath || streamUrl;
                const sourceType = filePath ? "file" : "url";
                void worker.enqueuePlay(sourceValue, command.title ?? null, sourceType).catch((error) => {
                    emit({ event: "error", message: error instanceof Error ? error.message : String(error) });
                });
                continue;
            }

            if (command.type === "stop") {
                await worker.stopPlayback();
                continue;
            }

            if (command.type === "set_audio") {
                if (typeof command.normalize_audio === "boolean") {
                    audioSettings.normalizeAudio = command.normalize_audio;
                }
                if (Number.isFinite(command.fade_in_ms) && command.fade_in_ms >= 0) {
                    audioSettings.fadeInMs = Math.trunc(command.fade_in_ms);
                }
                if (Number.isFinite(command.volume_percent) && command.volume_percent >= 0) {
                    worker.setVolumePercent(command.volume_percent);
                }
                emit({
                    event: "audio_settings_updated",
                    normalize_audio: audioSettings.normalizeAudio,
                    fade_in_ms: audioSettings.fadeInMs,
                    volume_percent: audioSettings.volumePercent,
                });
                continue;
            }

            if (command.type === "ping") {
                emit({ event: "pong", ts: Date.now() });
                continue;
            }

            if (command.type === "leave" || command.type === "shutdown") {
                await shutdown();
                process.exit(0);
            }

            emit({ event: "error", message: `Unknown command type: ${command.type}` });
        } catch (error) {
            emit({ event: "error", message: error instanceof Error ? error.message : String(error) });
        }
    }

    await shutdown();
}

main().catch((error) => {
    emit({ event: "error", message: error instanceof Error ? error.message : String(error) });
    logLine(`fatal ${error instanceof Error ? error.stack || error.message : String(error)}`);
    logStream.end();
    process.exitCode = 1;
});
