import re
import streamlit as st
from sqlalchemy import text
from utils.cargos import cargo_exists, list_cargos, normalize_cargo_name
from utils.timezone import now_brazil

LISTA_LOJAS = [
    "001", "002", "003", "004", "005", "006", "007", "008",
    "011", "012", "013", "014", "016", "017", "018",
    "F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09",
    "F10", "F11", "M12", "M13", "ADM", "RH"
]


def _normalize_username_from_nome(nome: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]", "", nome.lower())
    return normalized[:30]


def _solicitacao_ja_existe(engine, username_sugerido: str) -> bool:
    query = text(
        """
        SELECT EXISTS (
            SELECT 1 FROM solicitacoes_acesso
            WHERE username_sugerido = :username
              AND status = 'Pendente'
        )
        """
    )
    with engine.connect() as conn:
        return bool(conn.execute(query, {"username": username_sugerido}).scalar())


def _usuario_ja_existe(engine, username: str) -> bool:
    query = text("SELECT EXISTS (SELECT 1 FROM users WHERE username = :username)")
    with engine.connect() as conn:
        return bool(conn.execute(query, {"username": username}).scalar())


def criar_solicitacao_acesso(engine, nome: str, cargo: str, loja: str, senha_sugerida: str, empresa: str = "Baklizi"):
    username_sugerido = _normalize_username_from_nome(nome)

    if not username_sugerido:
        return False, "Não foi possível gerar usuário a partir do nome informado."

    if _usuario_ja_existe(engine, username_sugerido):
        return False, f"Já existe usuário cadastrado com o login '{username_sugerido}'."

    if _solicitacao_ja_existe(engine, username_sugerido):
        return False, "Já existe solicitação pendente para este usuário sugerido."

    if not cargo_exists(engine, cargo):
        return False, "Selecione um cargo valido na lista disponibilizada."

    query = text(
        """
        INSERT INTO solicitacoes_acesso (
            nome,
            cargo,
            loja,
            empresa,
            senha_sugerida,
            username_sugerido,
            status,
            data_solicitacao
        ) VALUES (
            :nome,
            :cargo,
            :loja,
            :empresa,
            :senha_sugerida,
            :username_sugerido,
            'Pendente',
            :data_solicitacao
        )
        """
    )

    with engine.begin() as conn:
        conn.execute(
            query,
            {
                "nome": nome.strip(),
                "cargo": normalize_cargo_name(cargo) if cargo else None,
                "loja": loja,
                "empresa": empresa.strip() if empresa else "Baklizi",
                "senha_sugerida": senha_sugerida,
                "username_sugerido": username_sugerido,
                "data_solicitacao": now_brazil(),
            },
        )

    return True, username_sugerido


def show_solicitacao_acesso_page(engine, base_data_path):
    _ = base_data_path
    cargos_disponiveis = list_cargos(engine)

    st.subheader("📝 Solicitação de Acesso")
    st.markdown(
        "Preencha os dados abaixo para sugerir o cadastro de um novo usuário. "
        "A conta só será criada após aprovação de um administrador."
    )

    if not cargos_disponiveis:
        st.warning(
            "Nao ha cargos cadastrados no momento. Solicite a um administrador que cadastre ao menos um cargo."
        )
        return

    EMPRESAS_DISPONIVEIS = ["Baklizi", "Free Shop", "Monaco"]

    with st.form("form_solicitacao_acesso", clear_on_submit=True):
        nome = st.text_input("1º - Nome")
        cargo = st.selectbox("2º - Cargo", cargos_disponiveis, index=None, placeholder="Selecione um cargo")
        loja = st.selectbox("3º - Loja", LISTA_LOJAS, index=None)
        empresa = st.selectbox("4º - Empresa", EMPRESAS_DISPONIVEIS, index=0)
        senha_sugerida = st.text_input("5º - Senha sugerida", type="password")

        enviar = st.form_submit_button("Enviar para aprovação")

        if enviar:
            if not nome.strip():
                st.warning("Informe o nome.")
                return
            if not cargo:
                st.warning("Selecione o cargo.")
                return
            if not loja:
                st.warning("Selecione a loja.")
                return
            if not empresa:
                st.warning("Selecione a empresa.")
                return
            if len(senha_sugerida) < 4:
                st.warning("A senha sugerida deve ter pelo menos 4 caracteres.")
                return

            sucesso, retorno = criar_solicitacao_acesso(
                engine,
                nome,
                cargo,
                loja,
                senha_sugerida,
                empresa,
            )

            if sucesso:
                st.success(
                    "Solicitação enviada com sucesso! "
                    f"Usuário sugerido: {retorno}. "
                    "Acesso liberado somente após aprovação do administrador."
                )
            else:
                st.error(retorno)
