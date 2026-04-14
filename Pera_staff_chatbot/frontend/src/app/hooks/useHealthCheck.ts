"use client";

import { useState, useCallback, useRef } from "react";
import type { ConnectionStatus } from "../lib/types";

/**
 * Tracks backend connectivity based on real API call outcomes.
 * - starts as "online" (optimistic default — avoids "Connecting" limbo)
 * - stays "online" after successful calls
 * - moves to "offline" after failures
 * - recovers to "online" after subsequent success
 */
export function useHealthCheck() {
  const [status, setStatus] = useState<ConnectionStatus>("online");

  const reportSuccess = useCallback(() => {
    setStatus("online");
  }, []);

  const reportFailure = useCallback(() => {
    setStatus("offline");
  }, []);

  return { status, reportSuccess, reportFailure } as const;
}
