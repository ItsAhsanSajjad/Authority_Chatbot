/** @type {import('next').NextConfig} */
const os = require("os");

// Auto-detect LAN IP so mobile always connects correctly
function getLanIp() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    // Skip loopback and virtual adapters
    if (name.toLowerCase().includes("loopback") || name.toLowerCase().includes("virtual")) continue;
    for (const iface of interfaces[name]) {
      if (iface.family === "IPv4" && !iface.internal) {
        return iface.address;
      }
    }
  }
  return "localhost";
}

const lanIp = process.env.NEXT_PUBLIC_API_URL
  ? new URL(process.env.NEXT_PUBLIC_API_URL).hostname
  : getLanIp();

const apiUrl = process.env.NEXT_PUBLIC_API_URL || `http://${lanIp}:8000`;

console.log(`[PERA] Auto-detected API URL: ${apiUrl} (LAN IP: ${lanIp})`);

const nextConfig = {
  env: {
    NEXT_PUBLIC_API_URL: apiUrl,
    NEXT_PUBLIC_LAN_IP: lanIp,
  },
};

module.exports = nextConfig;
