"use client";

import { Component, type ReactNode } from "react";

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  handleRetry = () => {
    this.setState({ hasError: false, error: null });
  };

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex flex-col items-center justify-center h-screen gap-4 p-8" style={{ background: "var(--bg-page)" }}>
          <div className="w-16 h-16 rounded-2xl flex items-center justify-center text-3xl" style={{ background: "var(--accent-soft)" }}>
            ⚠️
          </div>
          <h2 className="text-lg font-bold" style={{ color: "var(--text-primary)" }}>
            Something went wrong
          </h2>
          <p className="text-sm text-center max-w-md" style={{ color: "var(--text-secondary)" }}>
            The application encountered an unexpected error. Your chat history is preserved.
          </p>
          <button
            onClick={this.handleRetry}
            className="px-5 py-2.5 rounded-xl text-sm font-semibold text-white transition-all"
            style={{ background: "linear-gradient(135deg, var(--accent), var(--accent-hover))" }}
          >
            Try Again
          </button>
          {this.state.error && (
            <details className="mt-2 text-xs max-w-md" style={{ color: "var(--text-faint)" }}>
              <summary className="cursor-pointer">Error details</summary>
              <pre className="mt-1 whitespace-pre-wrap break-all">{this.state.error.message}</pre>
            </details>
          )}
        </div>
      );
    }
    return this.props.children;
  }
}
