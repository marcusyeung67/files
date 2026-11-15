// ScratchTube HTTP Bridge v7
// Paste into the inspect console at https://scratch.mit.edu/projects/...
(() => {
    'use strict';

    let HOST = window.__scratchTubeHost || "https://marcus-laptop.tail2ea4f6.ts.net";

    // Clean up a previously installed bridge without leaving its audio player running.
    if (window.__scratchTube) {
        try {
            window.__scratchTube.audio.pause();
            window.__scratchTube.audio.removeAttribute('src');
            window.__scratchTube.audio.load();
        } catch (_) {}
        try { window.fetch = window.__scratchTube.origFetch; } catch (_) {}
        console.log('[ScratchTube] Replaced previous bridge instance.');
    }

    const origFetch = window.fetch.bind(window);
    const audio = new Audio();
    audio.preload = 'auto';
    audio.crossOrigin = 'anonymous';

    let activeId = null;
    let cmdGen = 0;
    let isReady = false;
    let lastError = null;

    // Scratch's synthesis decoder was receiving MP3 bytes as if they were a WAV response.
    // Return a small, valid PCM WAV silence response for intercepted TTS requests instead.
    function makeSilentWav(durationSeconds = 0.10, sampleRate = 22050) {
        const channels = 1;
        const bitsPerSample = 16;
        const frames = Math.max(1, Math.floor(durationSeconds * sampleRate));
        const dataBytes = frames * channels * (bitsPerSample / 8);
        const buffer = new ArrayBuffer(44 + dataBytes);
        const view = new DataView(buffer);
        const write = (offset, value) => {
            for (let i = 0; i < value.length; i++) view.setUint8(offset + i, value.charCodeAt(i));
        };
        write(0, 'RIFF');
        view.setUint32(4, 36 + dataBytes, true);
        write(8, 'WAVE');
        write(12, 'fmt ');
        view.setUint32(16, 16, true);
        view.setUint16(20, 1, true);
        view.setUint16(22, channels, true);
        view.setUint32(24, sampleRate, true);
        view.setUint32(28, sampleRate * channels * bitsPerSample / 8, true);
        view.setUint16(32, channels * bitsPerSample / 8, true);
        view.setUint16(34, bitsPerSample, true);
        write(36, 'data');
        view.setUint32(40, dataBytes, true);
        return new Response(buffer, {
            status: 200,
            headers: { 'Content-Type': 'audio/wav', 'Content-Length': String(buffer.byteLength) }
        });
    }

    const silenceResponse = () => makeSilentWav();

    function audioUrl(id) {
        return `${HOST}/synth?text=${encodeURIComponent(`audio_${id}`)}`;
    }

    // The video reads this media clock; keep playback at normal speed to avoid drift.
    audio.playbackRate = 1;
    audio.defaultPlaybackRate = 1;

    function resetAudio() {
        ++cmdGen;
        isReady = false;
        activeId = null;
        lastError = null;
        audio.pause();
        try { audio.currentTime = 0; } catch (_) {}
        audio.removeAttribute('src');
        audio.load();
    }

    function waitForReady(gen, timeoutMs = 7000) {
        if (gen !== cmdGen) return Promise.resolve(false);
        if (audio.readyState >= HTMLMediaElement.HAVE_FUTURE_DATA && Number.isFinite(audio.duration)) {
            isReady = true;
            return Promise.resolve(true);
        }
        return new Promise(resolve => {
            let finished = false;
            const cleanup = () => {
                clearTimeout(timer);
                audio.removeEventListener('loadedmetadata', onReady);
                audio.removeEventListener('canplay', onReady);
                audio.removeEventListener('error', onError);
            };
            const finish = value => {
                if (finished) return;
                finished = true;
                cleanup();
                resolve(value && gen === cmdGen);
            };
            const onReady = () => {
                if (gen === cmdGen && audio.readyState >= HTMLMediaElement.HAVE_FUTURE_DATA) {
                    isReady = true;
                    finish(true);
                }
            };
            const onError = () => {
                lastError = audio.error ? audio.error.message || String(audio.error.code) : 'audio load failed';
                isReady = false;
                finish(false);
            };
            const timer = setTimeout(() => finish(false), timeoutMs);
            audio.addEventListener('loadedmetadata', onReady);
            audio.addEventListener('canplay', onReady);
            audio.addEventListener('error', onError);
            onReady();
        });
    }

    async function waitForSeek(gen, timeoutMs = 1800) {
        if (gen !== cmdGen || !audio.seeking) return gen === cmdGen;
        return new Promise(resolve => {
            let finished = false;
            const cleanup = () => {
                clearTimeout(timer);
                audio.removeEventListener('seeked', onSeeked);
                audio.removeEventListener('error', onError);
            };
            const finish = value => {
                if (finished) return;
                finished = true;
                cleanup();
                resolve(value && gen === cmdGen);
            };
            const onSeeked = () => finish(true);
            const onError = () => finish(false);
            const timer = setTimeout(() => finish(!audio.seeking), timeoutMs);
            audio.addEventListener('seeked', onSeeked, { once: true });
            audio.addEventListener('error', onError, { once: true });
        });
    }

    async function startAt(id, sec) {
        const gen = ++cmdGen;
        const needsReload = activeId !== id || !audio.src || audio.error || audio.networkState === HTMLMediaElement.NETWORK_NO_SOURCE;
        if (needsReload) {
            activeId = id;
            isReady = false;
            lastError = null;
            audio.pause();
            audio.src = audioUrl(id);
            audio.load();
        }

        const ready = await waitForReady(gen);
        if (!ready || gen !== cmdGen) return;

        const duration = Number.isFinite(audio.duration) ? audio.duration : 0;
        const target = Math.max(0, duration > 0 ? Math.min(Number(sec) || 0, Math.max(0, duration - 0.06)) : (Number(sec) || 0));

        if (!Number.isFinite(audio.currentTime) || Math.abs(audio.currentTime - target) > 0.04) {
            try {
                audio.currentTime = target;
                if (!(await waitForSeek(gen))) return;
            } catch (_) {
                // Some streams reject a seek until a later metadata event; playback can still start.
            }
        }
        if (gen !== cmdGen) return;

        for (let attempt = 0; attempt < 3 && gen === cmdGen; attempt++) {
            try {
                await audio.play();
                return;
            } catch (err) {
                lastError = err && err.message ? err.message : String(err);
                if (attempt < 2) await new Promise(resolve => setTimeout(resolve, 120));
            }
        }
        if (gen === cmdGen) console.warn('[ScratchTube Bridge] Could not start audio:', lastError);
    }

    audio.addEventListener('loadedmetadata', () => {
        if (audio.readyState >= HTMLMediaElement.HAVE_METADATA) isReady = true;
    });
    audio.addEventListener('canplay', () => { isReady = true; });
    audio.addEventListener('error', () => {
        isReady = false;
        lastError = audio.error ? audio.error.message || String(audio.error.code) : 'unknown media error';
        console.warn('[ScratchTube Bridge] Audio error:', lastError);
    });

    const clockVal = () => (!audio.paused && !audio.ended && !audio.seeking && audio.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA)
        ? audio.currentTime.toFixed(3) : '-1';

    window.fetch = async (input, init) => {
        let url;
        try {
            url = typeof input === 'string' ? input : (input && input.url) || String(input);
        } catch (_) {
            return origFetch(input, init);
        }

        const isTranslate = url.startsWith('https://translate-service.scratch.mit.edu/translate');
        const isSynthesis = url.startsWith('https://synthesis-service.scratch.mit.edu/synth');
        if (!isTranslate && !isSynthesis) return origFetch(input, init);

        let parsed;
        try { parsed = new URL(url, location.href); } catch (_) { return origFetch(input, init); }
        const text = parsed.searchParams.get('text') || '';
        if (!text.startsWith('HTTP ')) return origFetch(input, init);
        const payload = text.slice(5).split('~')[0];

        if (payload === 'audio_clock' || payload.startsWith('audio_clock_')) {
            return new Response(JSON.stringify({ result: clockVal() }), {
                status: 200, headers: { 'Content-Type': 'application/json' }
            });
        }
        if (payload.startsWith('audio_ready_')) {
            const requestedId = payload.slice('audio_ready_'.length);
            return new Response(JSON.stringify({ result: activeId === requestedId && isReady ? '1' : '0' }), {
                status: 200, headers: { 'Content-Type': 'application/json' }
            });
        }

        if (isSynthesis) {
            if (payload === 'audio_stop') {
                resetAudio();
                return silenceResponse();
            }
            let match = /^audio_start_([\d.]+)_([A-Za-z0-9_-]{11})$/.exec(payload);
            if (match) {
                void startAt(match[2], Number(match[1]));
                return silenceResponse();
            }
            match = /^audio_([A-Za-z0-9_-]{11})$/.exec(payload);
            if (match) {
                void startAt(match[1], 0);
                return silenceResponse();
            }
        }

        const endpoint = isTranslate ? `${HOST}/translate` : `${HOST}/synth`;
        const target = new URL(endpoint);
        target.searchParams.set('text', payload);
        return origFetch(target.toString(), { ...(init || {}), credentials: 'omit' });
    };

    window.__scratchTube = {
        origFetch,
        audio,
        get host() { return HOST; },
        setHost(host) {
            if (!host || typeof host !== 'string') throw new TypeError('Host must be a non-empty URL string');
            HOST = host.replace(/\/+$/, '');
            window.__scratchTubeHost = HOST;
        },
        stop: resetAudio
    };

    console.log('%c[ScratchTube Bridge v7] Ready! Connected to: ' + HOST, 'color:#00ff88;font-weight:bold;');
})();
