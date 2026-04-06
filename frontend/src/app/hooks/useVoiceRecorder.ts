"use client";

import { useCallback, useRef, useState, useEffect } from "react";
import { transcribeAudio } from "../lib/api";
import type { VoiceState } from "../lib/types";

interface VoiceRecorderResult {
  voiceState: VoiceState;
  elapsed: number;           // seconds since recording started
  errorMessage: string | null;
  toggleRecording: () => void;
  clearError: () => void;
}

export function useVoiceRecorder(
  onTranscribed: (text: string) => void,
): VoiceRecorderResult {
  const [voiceState, setVoiceState] = useState<VoiceState>("idle");
  const [elapsed, setElapsed] = useState(0);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const mediaRecorderRef = useRef<MediaRecorder | null>(null);
  const chunksRef = useRef<Blob[]>([]);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Cleanup timer on unmount
  useEffect(() => {
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, []);

  const startTimer = () => {
    setElapsed(0);
    timerRef.current = setInterval(() => setElapsed((s) => s + 1), 1000);
  };

  const stopTimer = () => {
    if (timerRef.current) {
      clearInterval(timerRef.current);
      timerRef.current = null;
    }
  };

  const clearError = useCallback(() => {
    setErrorMessage(null);
    if (voiceState === "error") setVoiceState("idle");
  }, [voiceState]);

  const toggleRecording = useCallback(async () => {
    // Check browser support
    if (!navigator.mediaDevices?.getUserMedia) {
      setVoiceState("error");
      setErrorMessage(
        "Microphone access is blocked. Use HTTPS or localhost, or enable insecure origins in browser flags.",
      );
      return;
    }

    // Stop recording
    if (voiceState === "recording") {
      mediaRecorderRef.current?.stop();
      setVoiceState("transcribing");
      stopTimer();
      return;
    }

    // Start recording
    try {
      setErrorMessage(null);
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const mr = new MediaRecorder(stream);
      mediaRecorderRef.current = mr;
      chunksRef.current = [];

      mr.ondataavailable = (e) => {
        if (e.data.size > 0) chunksRef.current.push(e.data);
      };

      mr.onstop = async () => {
        stream.getTracks().forEach((t) => t.stop());
        const blob = new Blob(chunksRef.current, { type: "audio/webm" });
        setVoiceState("transcribing");

        const result = await transcribeAudio(blob);
        if (result.ok) {
          onTranscribed(result.text);
          setVoiceState("idle");
        } else {
          setVoiceState("error");
          setErrorMessage(result.error.message);
        }
      };

      mr.start();
      setVoiceState("recording");
      startTimer();
    } catch (err: unknown) {
      stopTimer();
      setVoiceState("error");
      const msg = err instanceof Error ? err.message : "Could not access microphone.";
      setErrorMessage(msg);
    }
  }, [voiceState, onTranscribed]);

  return { voiceState, elapsed, errorMessage, toggleRecording, clearError };
}
