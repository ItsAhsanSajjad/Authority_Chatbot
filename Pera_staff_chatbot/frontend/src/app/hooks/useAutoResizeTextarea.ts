"use client";

import { useCallback, useEffect, useRef } from "react";

/**
 * Auto-resize a textarea to fit its content, up to a max height.
 * Returns a ref to attach and an `onInput` handler.
 */
export function useAutoResizeTextarea(maxHeight = 150) {
  const ref = useRef<HTMLTextAreaElement>(null);

  const resize = useCallback(() => {
    const el = ref.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${Math.min(el.scrollHeight, maxHeight)}px`;
  }, [maxHeight]);

  // Also resize when the ref mounts
  useEffect(() => {
    resize();
  }, [resize]);

  return { ref, resize } as const;
}
