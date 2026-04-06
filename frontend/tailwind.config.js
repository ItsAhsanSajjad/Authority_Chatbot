/** @type {import('tailwindcss').Config} */
module.exports = {
    content: [
        "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
        "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
        "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
    ],
    theme: {
        extend: {
            colors: {
                pera: {
                    primary: '#1e3a5f',
                    secondary: '#2d5a87',
                    accent: '#f59e0b',
                    gold: '#d4af37',
                },
            },
        },
    },
    plugins: [],
};
