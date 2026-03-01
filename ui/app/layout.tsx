import type { Metadata } from "next";
import { IBM_Plex_Mono, Share_Tech_Mono } from "next/font/google";

import "./globals.css";

const shareTechMono = Share_Tech_Mono({
  variable: "--font-share-tech-mono",
  subsets: ["latin"],
  weight: ["400"],
});

const ibmPlexMono = IBM_Plex_Mono({
  variable: "--font-ibm-plex-mono",
  subsets: ["latin"],
  weight: ["400", "500", "600"],
});

export const metadata: Metadata = {
  title: "Stock Terminal",
  description: "Bloomberg-inspired historic dashboard for Indian markets",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className={`${shareTechMono.variable} ${ibmPlexMono.variable}`}>{children}</body>
    </html>
  );
}
