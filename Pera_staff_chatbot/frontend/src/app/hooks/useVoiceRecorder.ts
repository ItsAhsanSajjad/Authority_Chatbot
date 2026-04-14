"use client";

import { useCallback, useRef, useState, useEffect } from "react";
import type { VoiceState } from "../lib/types";
import { transcribeAudio } from "../lib/api";

// ── Silence detection config ──────────────────────────────────
// RMS volume below this threshold = silence
const SILENCE_THRESHOLD = 0.012;
// How long silence must persist before auto-stopping (ms)
const SILENCE_MS = 2000;
// Maximum recording duration (ms)
const MAX_MS = 120_000;

// ── Hook interface ────────────────────────────────────────────
interface VoiceResult {
  voiceState: VoiceState;
  elapsed: number;
  interimText: string;
  errorMessage: string | null;
  toggleRecording: () => void;
  clearError: () => void;
}

export function useVoiceRecorder(
  onTranscribed: (text: string) => void,
): VoiceResult {
  const [voiceState, setVoiceState] = useState<VoiceState>("idle");
  const [elapsed, setElapsed]       = useState(0);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  // Keep callback ref fresh without re-creating dependent callbacks
  const onTranscribedRef = useRef(onTranscribed);
  useEffect(() => { onTranscribedRef.current = onTranscribed; }, [onTranscribed]);

  // ── All mutable state in refs (stable across renders) ────────
  const mrRef        = useRef<MediaRecorder | null>(null);
  const streamRef    = useRef<MediaStream | null>(null);
  const ctxRef       = useRef<AudioContext | null>(null);
  const rafRef       = useRef<number | null>(null);
  const tickRef      = useRef<ReturnType<typeof setInterval> | null>(null);
  const maxRef       = useRef<ReturnType<typeof setTimeout> | null>(null);
  const chunksRef    = useRef<Blob[]>([]);
  const mimeRef      = useRef("");
  const stoppingRef  = useRef(false); // prevents double-stop
  const unmountedRef = useRef(false); // prevents setState after unmount

  // ── Kill all timers and animation frames ─────────────────────
  const killTimers = useCallback(() => {
    if (tickRef.current) { clearInterval(tickRef.current);      tickRef.current = null; }
    if (maxRef.current)  { clearTimeout(maxRef.current);        maxRef.current = null; }
    if (rafRef.current)  { cancelAnimationFrame(rafRef.current); rafRef.current = null; }
  }, []);

  // ── Release microphone hardware ──────────────────────────────
  const releaseHardware = useCallback(() => {
    if (ctxRef.current) {
      ctxRef.current.close().catch(() => {});
      ctxRef.current = null;
    }
    if (streamRef.current) {
      streamRef.current.getTracks().forEach(t => t.stop());
      streamRef.current = null;
    }
  }, []);

  // ── Cleanup on unmount ───────────────────────────────────────
  useEffect(() => {
    unmountedRef.current = false;
    return () => {
      unmountedRef.current = true;
      killTimers();
      releaseHardware();
      // Detach onstop so handleStop won't fire after unmount
      if (mrRef.current) {
        mrRef.current.onstop = null;
        if (mrRef.current.state !== "inactive") {
          try { mrRef.current.stop(); } catch { /* ignore */ }
        }
        mrRef.current = null;
      }
    };
  }, [killTimers, releaseHardware]);

  const clearError = useCallback(() => {
    setErrorMessage(null);
    setVoiceState("idle");
  }, []);

  // ── Called by MediaRecorder.onstop — send to Whisper ─────────
  const handleStop = useCallback(async () => {
    killTimers();
    releaseHardware();

    const blob = new Blob(chunksRef.current, {
      type: mimeRef.current || "audio/webm",
    });
    chunksRef.current = [];

    if (unmountedRef.current) return;

    // Reject clips that are too short (user just tapped and released)
    if (blob.size < 800) {
      setVoiceState("idle");
      return;
    }

    setVoiceState("transcribing");

    const result = await transcribeAudio(blob);
    if (unmountedRef.current) return;

    if (result.ok) {
      const text = result.text.trim();
      if (text && !text.startsWith("⚠️")) {
        onTranscribedRef.current(text);
        setVoiceState("idle");
      } else {
        setErrorMessage(text || "Could not transcribe. Please try again.");
        setVoiceState("error");
      }
    } else {
      setErrorMessage(result.error.message);
      setVoiceState("error");
    }
  }, [killTimers, releaseHardware]);

  // ── Stop recording (safe to call multiple times) ─────────────
  const doStop = useCallback(() => {
    if (stoppingRef.current) return;
    stoppingRef.current = true;
    killTimers();
    const mr = mrRef.current;
    if (mr && mr.state !== "inactive") {
      mr.stop(); // triggers ondataavailable → onstop → handleStop
    } else {
      // MediaRecorder never started — just reset state
      releaseHardware();
      if (!unmountedRef.current) setVoiceState("idle");
    }
  }, [killTimers, releaseHardware, handleStop]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Start recording ──────────────────────────────────────────
  const start = useCallback(async () => {
    stoppingRef.current = false;
    chunksRef.current   = [];
    setErrorMessage(null);
    setElapsed(0);

    // ── Request microphone ────────────────────────────────────
    let stream: MediaStream;
    try {
      stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          sampleRate: 16000,
        },
      });
    } catch {
      setErrorMessage("Microphone blocked. Please tap Allow when prompted.");
      setVoiceState("error");
      return;
    }
    streamRef.current = stream;
    setVoiceState("recording");

    // ── Pick best supported MIME type ─────────────────────────
    const candidates = [
      "audio/webm;codecs=opus",
      "audio/webm",
      "audio/ogg;codecs=opus",
      "audio/ogg",
      "audio/mp4",
    ];
    mimeRef.current = candidates.find(t => MediaRecorder.isTypeSupported(t)) ?? "";

    // ── Create MediaRecorder ──────────────────────────────────
    let mr: MediaRecorder;
    try {
      mr = new MediaRecorder(
        stream,
        mimeRef.current ? { mimeType: mimeRef.current } : undefined,
      );
    } catch {
      mr = new MediaRecorder(stream);
      mimeRef.current = "";
    }
    mrRef.current = mr;

    mr.ondataavailable = (e) => {
      if (e.data?.size > 0) chunksRef.current.push(e.data);
    };
    mr.onstop = handleStop;
    mr.start(250); // collect audio in 250ms chunks

    // ── Elapsed counter ───────────────────────────────────────
    tickRef.current = setInterval(
      () => setElapsed(s => s + 1),
      1000,
    );

    // ── Hard maximum ──────────────────────────────────────────
    maxRef.current = setTimeout(() => doStop(), MAX_MS);

    // ── Silence detection via AudioContext ────────────────────
    try {
      const ctx = new AudioContext();
      ctxRef.current = ctx;
      const analyser = ctx.createAnalyser();
      analyser.fftSize = 512;
      ctx.createMediaStreamSource(stream).connect(analyser);

      const buf = new Float32Array(analyser.fftSize);
      let silStart = 0;

      const checkSilence = () => {
        // Stop the RAF loop if we're stopping or context is gone
        if (stoppingRef.current || !ctxRef.current) return;

        analyser.getFloatTimeDomainData(buf);

        // Compute RMS (root-mean-square volume)
        let sum = 0;
        for (let i = 0; i < buf.length; i++) sum += buf[i] * buf[i];
        const rms = Math.sqrt(sum / buf.length);

        if (rms < SILENCE_THRESHOLD) {
          // Silence — start/extend silence timer
          if (silStart === 0) silStart = Date.now();
          else if (Date.now() - silStart >= SILENCE_MS) {
            doStop(); // auto-stop after sustained silence
            return;
          }
        } else {
          silStart = 0; // reset on any speech activity
        }

        rafRef.current = requestAnimationFrame(checkSilence);
      };

      rafRef.current = requestAnimationFrame(checkSilence);
    } catch {
      // AudioContext unavailable (old browser) — user taps to stop manually
    }
  }, [handleStop, doStop]);

  // ── Toggle (tap mic to start, tap again to stop) ─────────────
  const toggleRecording = useCallback(() => {
    if (voiceState === "recording") {
      doStop();
    } else if (voiceState === "idle") {
      start();
    }
    // ignore taps during "transcribing" state
  }, [voiceState, start, doStop]);

  return {
    voiceState,
    elapsed,
    interimText: "", // no interim text with Whisper (result comes all at once)
    errorMessage,
    toggleRecording,
    clearError,
  };
}
