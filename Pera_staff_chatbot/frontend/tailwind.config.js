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
                    gold: '#d4a017',
                    'gold-dark': '#b8860b',
                    'gold-light': '#e6b422',
                },
            },
            borderRadius: {
                '2xl': '16px',
                '3xl': '20px',
            },
            animation: {
                'bounce-dot': 'bounceDot 1.4s ease-in-out infinite',
                'slide-up': 'slideUp 0.35s cubic-bezier(0.34,1.56,0.64,1)',
                'fade-in': 'fadeIn 0.4s ease-out',
                'modal-scale': 'modalScale 0.25s cubic-bezier(0.34,1.56,0.64,1)',
                'toast-in': 'toastSlide 0.3s ease-out',
            },
            keyframes: {
                bounceDot: {
                    '0%, 80%, 100%': { transform: 'scale(0.6)', opacity: '0.4' },
                    '40%': { transform: 'scale(1)', opacity: '1' },
                },
                slideUp: {
                    '0%': { opacity: '0', transform: 'translateY(16px)' },
                    '100%': { opacity: '1', transform: 'translateY(0)' },
                },
                fadeIn: {
                    '0%': { opacity: '0', transform: 'translateY(10px)' },
                    '100%': { opacity: '1', transform: 'translateY(0)' },
                },
                modalScale: {
                    '0%': { opacity: '0', transform: 'scale(0.92)' },
                    '100%': { opacity: '1', transform: 'scale(1)' },
                },
                toastSlide: {
                    '0%': { opacity: '0', transform: 'translateX(20px)' },
                    '100%': { opacity: '1', transform: 'translateX(0)' },
                },
            },
        },
    },
    plugins: [],
};
