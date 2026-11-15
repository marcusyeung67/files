// ScratchTube HTTP Bridge v7
// Paste into inspect console at https://scratch.mit.edu/projects/...
(() => {
    'use strict';
    const HOST = window.__scratchTubeHost || "https://marcus-laptop.tail2ea4f6.ts.net";
    const SILENCE = `${HOST}/synth?text=audio_stop`;

    // Clean up previous instance if re-pasting into console
    if (window.__scratchTube) {
        try { window.__scratchTube.audio.pause(); } catch (_) {}
        window.fetch = window.__scratchTube.origFetch;
        console.log('[ScratchTube] Replaced previous bridge instance.');
    }

    const origFetch = window.fetch.bind(window);
    const audio = new Audio();
    audio.preload = 'auto';

    let activeId = null, cmdGen = 0, isReady = false, waiters = [];

    const settle = ok => { isReady = ok; const q = waiters; waiters = []; q.forEach(fn => fn(ok)); };
    audio.onloadedmetadata = audio.oncanplay = () => settle(true);
    audio.onerror = () => { console.warn('[ScratchTube Bridge] audio error', audio.error); settle(false); };

    const audioUrl = id => `${HOST}/synth?text=${encodeURIComponent(`audio_${id}`)}`;

    async function startAt(id, sec) {
        const gen = ++cmdGen;
        if (activeId !== id || !audio.src) {
            activeId = id;
            settle(false);
            audio.src = audioUrl(id);
            audio.load();
        }
        if (gen !== cmdGen) return;
        if (!isReady && !Number.isFinite(audio.duration)) {
            await new Promise(r => {
                waiters.push(ok => r(ok && gen === cmdGen));
                setTimeout(() => r(false), 5000);
            });
        }
        if (gen !== cmdGen) return;
        const target = Math.max(0, Math.min(sec, (audio.duration || 1) - 0.05));
        
        // Fast sync: only seek if position differs significantly (>0.25s) to avoid decoder stalls
        if (Math.abs(audio.currentTime - target) > 0.25) {
            try { audio.currentTime = target; } catch (_) {}
        }
        
        for (let i = 0; i < 3 && gen === cmdGen; i++) {
            try { await audio.play(); return; } catch (_) { await new Promise(r => setTimeout(r, 100)); }
        }
    }

    const clockVal = () => (!audio.paused && !audio.ended && !audio.seeking && audio.readyState >= 3)
        ? audio.currentTime.toFixed(3) : '-1';

    window.fetch = async (input, init) => {
        const url = (typeof input === 'string') ? input : (input?.url || '');
        const isTr = url.startsWith('https://translate-service.scratch.mit.edu/translate');
        const isSy = url.startsWith('https://synthesis-service.scratch.mit.edu/synth');
        if (!isTr && !isSy) return origFetch(input, init);

        const txt = new URL(url).searchParams.get('text');
        if (!txt || !txt.startsWith('HTTP ')) return origFetch(input, init);
        const payload = txt.slice(5).split('~')[0];

        if (payload.startsWith('audio_clock')) {
            return new Response(JSON.stringify({ result: clockVal() }), { status: 200, headers: { 'Content-Type': 'application/json' } });
        }
        if (payload.startsWith('audio_ready_')) {
            return new Response(JSON.stringify({ result: (activeId === payload.slice(12) && isReady) ? '1' : '0' }), { status: 200, headers: { 'Content-Type': 'application/json' } });
        }
        if (isSy) {
            if (payload === 'audio_stop') { ++cmdGen; audio.pause(); return origFetch(SILENCE, { credentials: 'omit' }); }
            let m = /^audio_start_([\d.]+)_([A-Za-z0-9_-]{11})$/.exec(payload);
            if (m) { void startAt(m[2], +m[1]); return origFetch(SILENCE, { credentials: 'omit' }); }
            m = /^audio_([A-Za-z0-9_-]{11})$/.exec(payload);
            if (m) { void startAt(m[1], 0); return origFetch(SILENCE, { credentials: 'omit' }); }
        }
        const endpoint = isTr ? `${HOST}/translate?text=` : `${HOST}/synth?text=`;
        return origFetch(endpoint + encodeURIComponent(payload), init);
    };

    window.__scratchTube = { origFetch, audio, setHost: h => { window.__scratchTubeHost = h; } };
    console.log('%c[ScratchTube Bridge v6] Ready! Connected to: ' + HOST, 'color:#00ff88;font-weight:bold;');
})();
