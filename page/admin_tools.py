import streamlit as st
import subprocess
import shutil
import os
from datetime import datetime

REQ_FILE = "requirements.txt"


def show_admin_tools():
    st.title("🧰 Administração do Sistema")
    st.markdown("### Atualização de Dependências (requirements.txt)")

    st.info("Use esta ferramenta para atualizar automaticamente o arquivo `requirements.txt` "
            "de acordo com os pacotes instalados no ambiente virtual.")

    if st.button("🔄 Atualizar requirements.txt", type="primary"):
        with st.spinner("Gerando novo arquivo..."):
            backup_file = f"requirements_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

            if os.path.exists(REQ_FILE):
                shutil.copy(REQ_FILE, backup_file)
                st.caption(f"🧾 Backup criado: `{backup_file}`")

            try:
                result = subprocess.run(
                    ["pip", "freeze"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                with open(REQ_FILE, "w", encoding="utf-8") as f:
                    f.write(result.stdout)

                st.success(
                    "✅ Arquivo `requirements.txt` atualizado com sucesso!")
                st.code(result.stdout, language="text")

            except subprocess.CalledProcessError as e:
                st.error("❌ Erro ao gerar requirements.txt:")
                st.text(e.stderr)
            except Exception as e:
                st.error(f"⚠️ Erro inesperado: {e}")

    st.markdown("---")
    st.subheader("🧹 Opções Extras")
    if st.button("Ver conteúdo atual do requirements.txt"):
        if os.path.exists(REQ_FILE):
            with open(REQ_FILE, "r", encoding="utf-8") as f:
                st.code(f.read(), language="text")
        else:
            st.warning("Arquivo `requirements.txt` não encontrado.")
