"use client";

import { useState, useEffect } from "react";
import { loadTheme, saveTheme } from "../lib/storage";

export function useThemePreference() {
  const [theme, setTheme] = useState<"light" | "dark">("dark");

  // Load on mount
  useEffect(() => {
    setTheme(loadTheme());
  }, []);

  // Sync to DOM + localStorage
  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    saveTheme(theme);
  }, [theme]);

  const toggleTheme = () => setTheme((p) => (p === "dark" ? "light" : "dark"));

  return { theme, toggleTheme } as const;
}
