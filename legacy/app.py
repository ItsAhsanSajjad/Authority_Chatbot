"""
PERA AI Assistant - Simple Single-Page PDF Viewer
Shows one page at a time, starts at reference page
"""
from __future__ import annotations

import os
import base64
import streamlit as st
from typing import List, Dict, Any

from retriever import retrieve
from answerer import answer_question

# -----------------------------------------------------------------------------
# Page Config
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="PERA AI Assistant",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
DATA_DIR = os.path.join(os.path.dirname(__file__), "assets", "data")

# -----------------------------------------------------------------------------
# Helper: Load PDF as base64
# -----------------------------------------------------------------------------
def get_pdf_base64(filename: str) -> str:
    """Read PDF file and return base64 encoded string."""
    filepath = os.path.join(DATA_DIR, filename)
    
    if not os.path.exists(filepath):
        if not filename.endswith(".pdf"):
            filepath = os.path.join(DATA_DIR, filename + ".pdf")
    
    if not os.path.exists(filepath):
        for f in os.listdir(DATA_DIR):
            if filename.lower() in f.lower():
                filepath = os.path.join(DATA_DIR, f)
                break
    
    if os.path.exists(filepath):
        with open(filepath, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    return ""

# -----------------------------------------------------------------------------
# SIMPLE PDF Viewer - One Page at a Time
# -----------------------------------------------------------------------------
def render_pdf_viewer(pdf_base64: str, start_page: int = 1, height: int = 600):
    """Simple single-page PDF viewer. Shows one page, with prev/next buttons."""
    
    html = f'''
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js"></script>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            body {{ background: #525659; font-family: Arial, sans-serif; }}
            
            #toolbar {{
                background: #323639;
                padding: 10px 15px;
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 15px;
            }}
            #toolbar button {{
                background: #0078d4;
                color: white;
                border: none;
                padding: 8px 20px;
                border-radius: 4px;
                cursor: pointer;
                font-size: 14px;
                font-weight: bold;
            }}
            #toolbar button:hover {{ background: #106ebe; }}
            #toolbar button:disabled {{ background: #555; cursor: not-allowed; }}
            #toolbar span {{ color: #fff; font-size: 14px; font-weight: bold; }}
            
            #ref-badge {{
                background: #f59e0b;
                color: #000;
                padding: 4px 10px;
                border-radius: 4px;
                font-size: 12px;
                font-weight: bold;
            }}
            
            #canvas-container {{
                display: flex;
                justify-content: center;
                align-items: flex-start;
                padding: 20px;
                height: {height - 60}px;
                overflow: auto;
            }}
            
            canvas {{
                box-shadow: 0 4px 20px rgba(0,0,0,0.4);
                background: white;
            }}
            
            #loading {{ color: #ccc; text-align: center; padding: 50px; font-size: 16px; }}
        </style>
    </head>
    <body>
        <div id="toolbar">
            <button id="prev-btn" onclick="prevPage()">◀ Previous</button>
            <span>Page <span id="page-num">{start_page}</span> of <span id="page-count">?</span></span>
            <button id="next-btn" onclick="nextPage()">Next ▶</button>
            <span id="ref-badge">📍 Ref: Page {start_page}</span>
            <button onclick="goToRef()">Go to Reference</button>
        </div>
        <div id="canvas-container">
            <div id="loading">Loading PDF...</div>
            <canvas id="pdf-canvas" style="display:none;"></canvas>
        </div>
        
        <script>
            pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js';
            
            let pdfDoc = null;
            let currentPage = {start_page};
            let refPage = {start_page};
            let scale = 1.2;
            
            const canvas = document.getElementById('pdf-canvas');
            const ctx = canvas.getContext('2d');
            
            // Decode base64
            const pdfData = atob("{pdf_base64}");
            const pdfArray = new Uint8Array(pdfData.length);
            for (let i = 0; i < pdfData.length; i++) {{
                pdfArray[i] = pdfData.charCodeAt(i);
            }}
            
            // Load PDF
            pdfjsLib.getDocument({{data: pdfArray}}).promise.then(function(pdf) {{
                pdfDoc = pdf;
                document.getElementById('page-count').textContent = pdf.numPages;
                document.getElementById('loading').style.display = 'none';
                canvas.style.display = 'block';
                
                // Clamp start page to valid range
                if (currentPage > pdf.numPages) currentPage = pdf.numPages;
                if (currentPage < 1) currentPage = 1;
                
                renderPage(currentPage);
            }}).catch(function(error) {{
                document.getElementById('loading').textContent = 'Error: ' + error.message;
            }});
            
            function renderPage(num) {{
                pdfDoc.getPage(num).then(function(page) {{
                    const viewport = page.getViewport({{scale: scale}});
                    canvas.height = viewport.height;
                    canvas.width = viewport.width;
                    
                    page.render({{
                        canvasContext: ctx,
                        viewport: viewport
                    }});
                    
                    document.getElementById('page-num').textContent = num;
                    currentPage = num;
                    
                    // Update button states
                    document.getElementById('prev-btn').disabled = (num <= 1);
                    document.getElementById('next-btn').disabled = (num >= pdfDoc.numPages);
                }});
            }}
            
            function prevPage() {{
                if (currentPage > 1) renderPage(currentPage - 1);
            }}
            
            function nextPage() {{
                if (currentPage < pdfDoc.numPages) renderPage(currentPage + 1);
            }}
            
            function goToRef() {{
                renderPage(refPage);
            }}
        </script>
    </body>
    </html>
    '''
    
    st.components.v1.html(html, height=height, scrolling=False)

# -----------------------------------------------------------------------------
# Custom CSS
# -----------------------------------------------------------------------------
st.markdown("""
<style>
    .block-container { max-width: 1300px; padding-top: 1rem; }
    
    .user-msg {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 12px 16px; border-radius: 12px; margin: 10px 0;
        color: white; font-weight: 500;
    }
    .bot-msg {
        background: #f8fafc; padding: 12px 16px; border-radius: 12px; margin: 10px 0;
        color: #1e293b; border: 1px solid #e2e8f0;
    }
    .snippet-box {
        background: #fef3c7; border-left: 4px solid #f59e0b;
        padding: 10px 14px; margin: 8px 0; font-size: 0.9em;
        color: #92400e; border-radius: 0 8px 8px 0;
    }
    
    footer, #MainMenu { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Session State
# -----------------------------------------------------------------------------
if "messages" not in st.session_state:
    st.session_state.messages = []
if "selected_pdf" not in st.session_state:
    st.session_state.selected_pdf = None

# -----------------------------------------------------------------------------
# Layout
# -----------------------------------------------------------------------------
col_chat, col_pdf = st.columns([1, 1], gap="medium")

with col_chat:
    st.markdown("## 🤖 PERA AI")
    st.markdown("---")
    
    if not st.session_state.messages:
        st.markdown("""
        <div class="bot-msg">
            <b>Assalam-o-Alaikum!</b> PERA Act ke baare mein poochein.
        </div>
        """, unsafe_allow_html=True)
    
    for i, msg in enumerate(st.session_state.messages):
        if msg["role"] == "user":
            st.markdown(f'<div class="user-msg">{msg["content"]}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="bot-msg">{msg["content"]}</div>', unsafe_allow_html=True)
            
            refs = msg.get("references", [])
            if refs:
                for idx, ref in enumerate(refs):
                    doc_name = ref.get("doc_name", "Doc")
                    page = ref.get("page_start", 1)
                    snippet = ref.get("snippet", "")
                    
                    if snippet:
                        st.markdown(f'<div class="snippet-box">📌 <b>{doc_name}</b> (Page {page}):<br/><i>"{snippet[:250]}..."</i></div>', unsafe_allow_html=True)
                    
                    if st.button(f"📄 View: {doc_name} (Page {page})", key=f"ref_{i}_{idx}"):
                        st.session_state.selected_pdf = {
                            "filename": doc_name,
                            "page": page,
                            "title": f"{doc_name} - Page {page}",
                            "snippet": snippet
                        }
                        st.rerun()
    
    user_input = st.chat_input("Sawal likhein...")
    
    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})
        
        with st.spinner("Soch raha hun..."):
            history = [{"role": m["role"], "content": m["content"]} 
                       for m in st.session_state.messages[:-1]]
            
            retrieval = retrieve(user_input)
            result = answer_question(user_input, retrieval, conversation_history=history)
            
            answer = result.get("answer", "Jawab nahi mila.")
            
            refs_out = []
            for ref in result.get("references", []):
                doc = ref.get("document", "Document")
                doc_short = doc.split("/")[-1].replace(".pdf", "").replace(".docx", "")
                refs_out.append({
                    "doc_name": doc_short, 
                    "page_start": ref.get("page_start", 1),
                    "snippet": ref.get("snippet", "")
                })
            
            if refs_out:
                st.session_state.selected_pdf = {
                    "filename": refs_out[0]["doc_name"],
                    "page": refs_out[0]["page_start"],
                    "title": f"{refs_out[0]['doc_name']} - Page {refs_out[0]['page_start']}",
                    "snippet": refs_out[0].get("snippet", "")
                }
            
            st.session_state.messages.append({
                "role": "assistant",
                "content": answer,
                "references": refs_out
            })
        
        st.rerun()

with col_pdf:
    st.markdown("## 📄 Document Viewer")
    st.markdown("---")
    
    if st.session_state.selected_pdf:
        pdf_info = st.session_state.selected_pdf
        st.markdown(f"**{pdf_info['title']}**")
        
        snippet = pdf_info.get("snippet", "")
        if snippet:
            st.info(f"📌 **Reference Text:**\n\n*\"{snippet[:400]}...\"*")
        
        pdf_b64 = get_pdf_base64(pdf_info["filename"])
        
        if pdf_b64:
            render_pdf_viewer(pdf_b64, start_page=pdf_info.get("page", 1), height=550)
        else:
            st.error(f"PDF not found: {pdf_info['filename']}")
    else:
        st.info("👈 Ask a question to view documents")

with st.sidebar:
    st.image("assets/pera_logo.png", width=100)
    if st.button("🗑️ Clear", use_container_width=True):
        st.session_state.messages = []
        st.session_state.selected_pdf = None
        st.rerun()
