import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./lib/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ["var(--font-share-tech-mono)", "monospace"],
        mono: ["var(--font-ibm-plex-mono)"],
      },
      colors: {
        terminal: {
          bg: "#010101",
          panel: "#050505",
          border: "#232323",
          text: "#c9d7e8",
          accent: "#f5a623",
          cyan: "#5cd1ff",
          green: "#7ddc80",
          red: "#ff6c6c",
        },
      },
      boxShadow: {
        panel: "0 0 0 1px rgba(45,45,45,0.85), 0 14px 34px rgba(0,0,0,0.75)",
      },
    },
  },
  plugins: [],
};

export default config;
