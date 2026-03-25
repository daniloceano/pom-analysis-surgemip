import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Turbopack is the default in Next.js 16; webpack config is not needed
  // because Leaflet and Plotly are loaded client-side only via dynamic({ ssr: false })
  turbopack: {},
};

export default nextConfig;
