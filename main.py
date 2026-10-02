import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="Sistema Baklizi - Migração de Servidor",
    page_icon="🚀",
    layout="centered"
)

# Injeta CSS e Script de Redirecionamento Automático
html_redirect = """
<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="2; url=http://192.168.50.211:8000">
    <script>
        // Redirecionamento automático imediato
        setTimeout(function() {
            window.location.href = "http://192.168.50.211:8000";
        }, 1500);
    </script>
    <style>
        .container {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            text-align: center;
            padding: 40px 20px;
            background: linear-gradient(135deg, #1e293b, #0f172a);
            border-radius: 16px;
            color: #ffffff;
            box-shadow: 0 10px 30px rgba(0,0,0,0.5);
            border: 1px solid #334155;
        }
        .icon {
            font-size: 56px;
            margin-bottom: 15px;
            animation: pulse 1.5s infinite;
        }
        @keyframes pulse {
            0% { transform: scale(1); }
            50% { transform: scale(1.1); }
            100% { transform: scale(1); }
        }
        h1 {
            color: #38bdf8;
            font-size: 26px;
            margin-bottom: 10px;
            font-weight: 700;
        }
        p {
            color: #cbd5e1;
            font-size: 16px;
            line-height: 1.6;
            margin-bottom: 25px;
        }
        .btn-access {
            display: inline-block;
            background: linear-gradient(135deg, #0284c7, #0369a1);
            color: #ffffff !important;
            text-decoration: none;
            font-size: 18px;
            font-weight: bold;
            padding: 14px 32px;
            border-radius: 10px;
            box-shadow: 0 4px 15px rgba(2, 132, 199, 0.4);
            transition: all 0.2s ease;
            border: 1px solid #38bdf8;
        }
        .btn-access:hover {
            background: linear-gradient(135deg, #0369a1, #075985);
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(2, 132, 199, 0.6);
        }
        .note {
            margin-top: 30px;
            font-size: 13px;
            color: #94a3b8;
            border-top: 1px solid #334155;
            padding-top: 15px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="icon">🚀</div>
        <h1>O Sistema Baklizi Mudou de Servidor!</h1>
        <p>
            O aplicativo agora roda <strong>100% no servidor interno de alta velocidade da empresa</strong>.<br>
            Você está sendo redirecionado automaticamente...
        </p>
        <a href="http://192.168.50.211:8000" class="btn-access" target="_top">
            👉 Clique aqui para Acessar o Novo Sistema
        </a>
        <div class="note">
            ℹ️ <strong>Endereço interno:</strong> <code>http://192.168.50.211:8000</code><br>
            Lembre-se de estar conectado à rede interna da empresa (Wi-Fi ou cabo).
        </div>
    </div>
</body>
</html>
"""

components.html(html_redirect, height=450)
st.markdown("---")
st.info("💡 **Dica:** Salve o novo endereço nos seus favoritos: **http://192.168.50.211:8000**")
