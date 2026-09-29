import streamlit as st
from sqlalchemy import text
import pandas as pd
import hashlib
import json
from datetime import datetime
from utils.fornecedores_loader import (
    get_all_fornecedores_catalog,
    format_fornecedores_summary,
    parse_fornecedores_acesso,
    load_produtos_para_fornecedor,
)

# --- Configurações Globais ---
LISTA_LOJAS_FORNECEDOR = [
    "001", "002", "003", "004", "005", "006", "007", "008",
    "011", "012", "013", "014", "016", "017", "018",
    "F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09",
    "F10", "F11", "M12", "M13", "ADM", "RH"
]
ROLES_FORNECEDOR = ["fornecedor", "admin_fornecedor"]


# --- Funções Auxiliares de Hashing ---
def make_hashes_fornecedor(password: str) -> str:
    return hashlib.sha256(str(password).encode("utf-8")).hexdigest()


# --- Funções de Manutenção do DB (CRUD de Fornecedores & Representantes) ---
def create_fornecedores_table(engine):
    """Cria/atualiza a tabela de fornecedores e adiciona admins iniciais se não existirem."""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fornecedores_users (
                    username TEXT PRIMARY KEY,
                    password TEXT NOT NULL,
                    empresa TEXT,
                    ultimo_acesso TIMESTAMP,
                    status_logado TEXT,
                    role TEXT DEFAULT 'fornecedor',
                    lojas_acesso TEXT,
                    fornecedores_acesso TEXT
                )
            """))

            # Garante coluna fornecedores_acesso para bancos existentes
            conn.execute(text("""
                ALTER TABLE fornecedores_users
                ADD COLUMN IF NOT EXISTS fornecedores_acesso TEXT;
            """))

            # Adicionar admins iniciais se não existirem
            admins = {
                "ale": make_hashes_fornecedor("7890"),
                "rafael": make_hashes_fornecedor("302010")
            }
            for admin_user, admin_pass in admins.items():
                result = conn.execute(
                    text("SELECT 1 FROM fornecedores_users WHERE username = :user"),
                    {"user": admin_user}
                ).scalar()
                if not result:
                    conn.execute(text("""
                        INSERT INTO fornecedores_users (
                            username, password, role, empresa, status_logado, fornecedores_acesso
                        )
                        VALUES (:user, :pass, 'admin_fornecedor', 'Administração', 'DESLOGADO', '[]')
                    """), {"user": admin_user, "pass": admin_pass})
    except Exception as e:
        st.error(f"Erro ao inicializar banco de dados de fornecedores: {e}")


def get_all_fornecedores_details(engine, base_data_path: str = None) -> pd.DataFrame:
    """Busca todos os fornecedores cadastrados com suas permissões e indústrias liberadas."""
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(
                text("SELECT username, empresa, role, lojas_acesso, fornecedores_acesso, status_logado FROM fornecedores_users ORDER BY username"),
                con=conn
            )
        
        if df.empty:
            return pd.DataFrame(columns=['Usuário', 'Empresa / Representação', 'Função', 'Lojas Permitidas', 'Fornecedores Autorizados', 'Status'])

        catalog_df = get_all_fornecedores_catalog(base_data_path)

        def format_lojas(lojas_raw):
            if lojas_raw is None or pd.isna(lojas_raw):
                return "Todas (Padrão)"
            lojas_str = str(lojas_raw).strip()
            if not lojas_str or lojas_str.lower() in ["none", "nan", "null", "[]"]:
                return "Todas (Padrão)"
            try:
                lojas_list = json.loads(lojas_str)
                return ", ".join(lojas_list) if lojas_list else "Nenhuma"
            except (json.JSONDecodeError, Exception):
                return lojas_str

        def format_fornecedores_col(forn_raw, role_val, emp_val):
            codigos = parse_fornecedores_acesso(forn_raw)
            if codigos:
                return format_fornecedores_summary(codigos, catalog_df, max_display=3)
            emp_str = str(emp_val or "").strip()
            role_str = str(role_val or "").strip()
            if role_str == "admin_fornecedor" or emp_str.lower() in ["administração", "administracao"]:
                return "👑 Acesso Total (Admin)"
            return "Nenhum fornecedor vinculado"

        df['lojas_fmt'] = df['lojas_acesso'].apply(format_lojas)
        df['forn_fmt'] = df.apply(lambda r: format_fornecedores_col(r['fornecedores_acesso'], r['role'], r['empresa']), axis=1)
        
        df_out = pd.DataFrame({
            'Usuário': df['username'].fillna("").astype(str),
            'Empresa / Representação': df['empresa'].fillna("").astype(str),
            'Função': df['role'].fillna("fornecedor").astype(str),
            'Lojas Permitidas': df['lojas_fmt'],
            'Fornecedores Autorizados': df['forn_fmt'],
            'Status': df['status_logado'].fillna("DESLOGADO").astype(str)
        })
        return df_out
        
    except Exception as e:
        st.error(f"Erro ao carregar fornecedores: {e}")
        return pd.DataFrame(columns=['Usuário', 'Empresa / Representação', 'Função', 'Lojas Permitidas', 'Fornecedores Autorizados', 'Status'])


def add_new_fornecedor(engine, username, password, role, empresa, lojas_acesso_list, fornecedores_codigos):
    """Adiciona um novo fornecedor / representante ao DB."""
    try:
        hashed_password = make_hashes_fornecedor(password)
        lojas_acesso_json = json.dumps(lojas_acesso_list)
        fornecedores_acesso_json = json.dumps(parse_fornecedores_acesso(fornecedores_codigos))
        
        query = text("""
            INSERT INTO fornecedores_users (
                username, password, role, empresa, lojas_acesso, fornecedores_acesso, status_logado
            ) 
            VALUES (:username, :password, :role, :empresa, :lojas, :fornecedores, :status)
        """)
        params = {
            "username": username.lower().strip(),
            "password": hashed_password,
            "role": role, 
            "empresa": empresa.strip(),
            "lojas": lojas_acesso_json,
            "fornecedores": fornecedores_acesso_json,
            "status": 'DESLOGADO'
        }
        
        with engine.begin() as conn:
            conn.execute(query, params)
        return True
    
    except Exception as e:
        st.error(f"Erro ao adicionar fornecedor: {'Usuário já existe.' if 'unique' in str(e).lower() else e}")
        return False


def update_fornecedor_permissions(engine, username, role, empresa, lojas_acesso_list, fornecedores_codigos):
    """Atualiza o role, empresa, lojas e fornecedores autorizados de um usuário."""
    try:
        lojas_acesso_json = json.dumps(lojas_acesso_list)
        fornecedores_acesso_json = json.dumps(parse_fornecedores_acesso(fornecedores_codigos))
        
        query = text("""
            UPDATE fornecedores_users 
            SET role = :role, 
                empresa = :empresa, 
                lojas_acesso = :lojas,
                fornecedores_acesso = :fornecedores
            WHERE username = :username
        """)
        params = {
            "role": role,
            "empresa": empresa.strip(),
            "lojas": lojas_acesso_json,
            "fornecedores": fornecedores_acesso_json,
            "username": username.lower().strip()
        }
        
        with engine.begin() as conn:
            result = conn.execute(query, params)
        return result.rowcount > 0
    except Exception as e:
        st.error(f"Erro ao alterar permissões: {e}")
        return False


def update_fornecedor_password(engine, username, new_password):
    """Altera a senha de um fornecedor."""
    try:
        hashed_password = make_hashes_fornecedor(new_password)
        query = text("UPDATE fornecedores_users SET password = :password WHERE username = :username")
        params = {"password": hashed_password, "username": username.lower().strip()}
        
        with engine.begin() as conn:
            result = conn.execute(query, params)
        return result.rowcount > 0
    except Exception as e:
        st.error(f"Erro ao alterar senha: {e}")
        return False


def delete_fornecedor(engine, username):
    """Remove um fornecedor do DB."""
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM fornecedores_users WHERE username = :username"),
                {"username": username.lower().strip()}
            )
        return result.rowcount > 0
    except Exception as e:
        st.error(f"Erro ao deletar fornecedor: {e}")
        return False


# --- Lógica de Exibição da Página ---
def show_admin_fornecedor_page(engine, base_data_path: str = None):
    """Interface do painel de administração de fornecedores e representantes."""
    st.title("🛡️ Gestão de Fornecedores & Representantes")
    st.markdown(
        "Gerencie acessos de fornecedores e **representantes multi-marcas**, vinculando as indústrias autorizadas do `query.parquet`."
    )
    
    # Garante que a tabela e os admins existam
    create_fornecedores_table(engine)
    
    # Carrega catálogo de fornecedores do query.parquet
    catalog_df = get_all_fornecedores_catalog(base_data_path)
    total_fornecedores_catalogo = len(catalog_df)
    
    # Dicionários de mapeamento
    cod_to_label = dict(zip(catalog_df["cod_fornecedor"], catalog_df["label"]))
    label_to_cod = dict(zip(catalog_df["label"], catalog_df["cod_fornecedor"]))
    opcoes_labels = list(catalog_df["label"])
    
    # Resumo rápido em cards
    df_fornecedores = get_all_fornecedores_details(engine, base_data_path)
    
    col_k1, col_k2, col_k3 = st.columns(3)
    col_k1.metric("Representantes / Usuários", len(df_fornecedores))
    col_k2.metric("Fornecedores no ERP (query.parquet)", total_fornecedores_catalogo)
    total_skus = catalog_df["total_skus"].sum() if not catalog_df.empty else 0
    col_k3.metric("Total de SKUs Analisados", f"{total_skus:,.0f}")
    
    st.subheader("Usuários Cadastrados")
    st.dataframe(df_fornecedores, hide_index=True, use_container_width=True)
    st.markdown("---")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "➕ Adicionar Representante / Fornecedor",
        "⚙️ Gerenciar Acesso & Lojas",
        "🎯 Direcionar Fornecedores a Representante",
        "🔑 Alterar Senha",
        "🗑️ Excluir Usuário"
    ])

    # =========================================================
    # TAB 1: Adicionar Novo Fornecedor / Representante
    # =========================================================
    with tab1:
        st.subheader("Cadastrar Novo Fornecedor ou Representante")
        st.caption("Digite o código ou nome da indústria no seletor abaixo para vincular os produtos ao representante.")
        
        with st.form("add_fornecedor_form", clear_on_submit=True):
            col_a1, col_a2 = st.columns(2)
            with col_a1:
                new_username = st.text_input("Login / Usuário (Username):", key="add_forn_user").lower().strip()
                new_password = st.text_input("Senha Inicial:", type="password", key="add_forn_pass")
                new_role = st.selectbox("Função (Role):", ROLES_FORNECEDOR, index=0, key="add_forn_role")
            
            with col_a2:
                new_empresa = st.text_input("Empresa / Nome do Representante:", placeholder="Ex: Representações Sul / Nestlé & Oderich", key="add_forn_empresa")
                new_lojas = st.multiselect("Lojas Permitidas para Digitação:", LISTA_LOJAS_FORNECEDOR, default=LISTA_LOJAS_FORNECEDOR, key="add_forn_lojas")
            
            st.markdown("#### 🏢 Fornecedores / Indústrias Autorizadas")
            new_fornecedores_sel = st.multiselect(
                "Selecione os fornecedores (digite o código ou nome para buscar):",
                options=opcoes_labels,
                key="add_forn_multisel"
            )
            
            todos_cods = [label_to_cod[lbl] for lbl in new_fornecedores_sel if lbl in label_to_cod]
            
            if todos_cods:
                preview_skus = catalog_df[catalog_df["cod_fornecedor"].isin(todos_cods)]["total_skus"].sum()
                st.success(f"📦 Total de **{len(todos_cods)}** fornecedor(es) selecionado(s), somando **{preview_skus} produtos** ativos.")
            
            submit_add = st.form_submit_button("Criar Representante / Fornecedor", type="primary")
            if submit_add:
                if not (new_username and new_password and new_empresa):
                    st.warning("Preencha Login, Senha e Nome da Empresa / Representante.")
                else:
                    if add_new_fornecedor(engine, new_username, new_password, new_role, new_empresa, new_lojas, todos_cods):
                        st.success(f"Fornecedor/Representante '{new_username}' criado com sucesso!")
                        st.rerun()

    # =========================================================
    # TAB 2: Gerenciar Acesso & Lojas
    # =========================================================
    with tab2:
        st.subheader("Editar Cadastro e Permissões")
        if not df_fornecedores.empty:
            user_list = df_fornecedores['Usuário'].tolist()
            user_to_manage = st.selectbox("Selecione o Fornecedor/Representante:", user_list, key="manage_forn_select", index=None)
            
            if user_to_manage:
                # Busca dados brutos no DB
                try:
                    with engine.connect() as conn:
                        q = text("SELECT empresa, role, lojas_acesso, fornecedores_acesso FROM fornecedores_users WHERE username = :user")
                        row = conn.execute(q, {"user": user_to_manage.lower()}).fetchone()
                    
                    current_empresa = row[0] if row and row[0] else ""
                    current_role = row[1] if row and row[1] else "fornecedor"
                    current_lojas_raw = row[2] if row and row[2] else "[]"
                    current_forn_raw = row[3] if row and row[3] else "[]"
                    
                    current_lojas = json.loads(current_lojas_raw) if current_lojas_raw else []
                    current_cods = parse_fornecedores_acesso(current_forn_raw)
                except Exception:
                    current_empresa, current_role, current_lojas, current_cods = "", "fornecedor", [], []
                
                with st.form("manage_fornecedor_form"):
                    st.markdown(f"### Editando: **{user_to_manage}**")
                    col_m1, col_m2 = st.columns(2)
                    with col_m1:
                        m_role = st.selectbox("Função (Role):", ROLES_FORNECEDOR, index=ROLES_FORNECEDOR.index(current_role) if current_role in ROLES_FORNECEDOR else 0)
                        m_empresa = st.text_input("Empresa / Representação:", value=current_empresa)
                    with col_m2:
                        m_lojas = st.multiselect("Lojas Permitidas:", LISTA_LOJAS_FORNECEDOR, default=[l for l in current_lojas if l in LISTA_LOJAS_FORNECEDOR])
                    
                    st.markdown("#### 🏢 Fornecedores Vinculados")
                    defaults_labels = [cod_to_label[c] for c in current_cods if c in cod_to_label]
                    
                    m_fornecedores_sel = st.multiselect(
                        "Fornecedores no Catálogo Consinco (digite o código ou nome):",
                        options=opcoes_labels,
                        default=defaults_labels,
                        key="manage_forn_multisel"
                    )
                    
                    novos_cods_m = [label_to_cod[lbl] for lbl in m_fornecedores_sel if lbl in label_to_cod]
                    
                    if novos_cods_m:
                        preview_skus = catalog_df[catalog_df["cod_fornecedor"].isin(novos_cods_m)]["total_skus"].sum()
                        st.info(f"📦 Total de **{len(novos_cods_m)}** indústrias vinculadas (**{preview_skus} produtos** no catálogo).")
                    else:
                        if m_role == "admin_fornecedor":
                            st.caption("ℹ️ Sem fornecedores fixos configurados: Como Admin, terá visão de todos os itens.")
                        else:
                            st.warning("⚠️ Nenhum fornecedor selecionado. O usuário não verá produtos até que seja vinculado.")
                    
                    if st.form_submit_button("Salvar Alterações de Acesso", type="primary"):
                        if update_fornecedor_permissions(engine, user_to_manage, m_role, m_empresa, m_lojas, novos_cods_m):
                            st.success(f"Permissões de '{user_to_manage}' atualizadas com sucesso!")
                            st.rerun()

    # =========================================================
    # TAB 3: Direcionar Seleção de Itens (Representantes)
    # =========================================================
    with tab3:
        st.subheader("🎯 Direcionamento Rápido de Itens para Representante")
        st.markdown(
            "Selecione um representante e use o seletor para adicionar ou remover indústrias digitando o código ou nome."
        )
        
        if df_fornecedores.empty:
            st.info("Nenhum usuário cadastrado.")
        else:
            rep_user = st.selectbox(
                "Selecione o Representante que receberá a carteira:",
                df_fornecedores['Usuário'].tolist(),
                key="rep_target_select",
                index=None
            )
            
            if rep_user:
                # Carrega fornecedores atuais
                try:
                    with engine.connect() as conn:
                        q = text("SELECT empresa, role, lojas_acesso, fornecedores_acesso FROM fornecedores_users WHERE username = :user")
                        row = conn.execute(q, {"user": rep_user.lower()}).fetchone()
                    rep_empresa = row[0] if row and row[0] else ""
                    rep_role = row[1] if row and row[1] else "fornecedor"
                    rep_lojas = json.loads(row[2]) if row and row[2] else []
                    rep_cods_atuais = parse_fornecedores_acesso(row[3] if row and row[3] else "[]")
                except Exception:
                    rep_empresa, rep_role, rep_lojas, rep_cods_atuais = "", "fornecedor", [], []

                st.markdown(f"Configurando fornecedores para: **{rep_user}** ({rep_empresa})")
                
                selecionados_atuais_labels = [cod_to_label[c] for c in rep_cods_atuais if c in cod_to_label]
                
                novos_labels_escolhidos = st.multiselect(
                    "Fornecedores Atribuídos ao Representante (digite o código ou nome para buscar e adicionar):",
                    options=opcoes_labels,
                    default=selecionados_atuais_labels,
                    key="rep_direcionamento_multisel"
                )
                
                if st.button("🗑️ Limpar Todos os Fornecedores Deste Representante"):
                    st.session_state["rep_direcionamento_multisel"] = []
                    st.rerun()
                
                codigos_finais_rep = [label_to_cod[lbl] for lbl in novos_labels_escolhidos if lbl in label_to_cod]
                
                # Preview dos produtos que o representante verá
                if codigos_finais_rep:
                    st.markdown("---")
                    st.markdown("### 👁️ Prévia dos Itens que o Representante verá:")
                    df_preview_prods = load_produtos_para_fornecedor(fornecedor_codes=codigos_finais_rep, base_data_path=base_data_path)
                    
                    st.success(f"✅ O representante terá acesso a **{len(df_preview_prods)} SKUs** de **{len(codigos_finais_rep)} fornecedores**.")
                    
                    if not df_preview_prods.empty:
                        colunas_preview = ["codigo_interno", "descricao", "fornecedor_label", "codigo_ean", "embalagem", "estoque_cd"]
                        cols_ok = [c for c in colunas_preview if c in df_preview_prods.columns]
                        st.dataframe(
                            df_preview_prods[cols_ok].head(50),
                            hide_index=True,
                            use_container_width=True
                        )
                        if len(df_preview_prods) > 50:
                            st.caption(f"Mostrando primeiros 50 itens de {len(df_preview_prods)}.")
                else:
                    st.warning("Nenhum fornecedor atribuído.")
                    
                st.markdown("---")
                if st.button("💾 Gravar e Atualizar Carteira do Representante", type="primary", use_container_width=True):
                    if update_fornecedor_permissions(engine, rep_user, rep_role, rep_empresa, rep_lojas, codigos_finais_rep):
                        st.success(f"Carteira de itens do representante '{rep_user}' gravada com sucesso!")
                        st.rerun()

    # =========================================================
    # TAB 4: Alterar Senha
    # =========================================================
    with tab4:
        st.subheader("Alterar Senha de Fornecedor / Representante")
        if not df_fornecedores.empty:
            user_list_pass = df_fornecedores['Usuário'].tolist()
            user_for_pass = st.selectbox("Selecione o Usuário:", user_list_pass, key="pass_forn_select", index=None)
            if user_for_pass:
                with st.form("change_pass_fornecedor_form", clear_on_submit=True):
                    new_pass = st.text_input(f"Nova senha para {user_for_pass}:", type="password")
                    if st.form_submit_button("Atualizar Senha", type="primary"):
                        if not new_pass:
                            st.warning("Digite a nova senha.")
                        else:
                            if update_fornecedor_password(engine, user_for_pass, new_pass):
                                st.success(f"Senha de '{user_for_pass}' alterada com sucesso!")

    # =========================================================
    # TAB 5: Excluir Fornecedor
    # =========================================================
    with tab5:
        st.subheader("Excluir Usuário")
        if not df_fornecedores.empty:
            user_list_del = df_fornecedores['Usuário'].tolist()
            current_admin = st.session_state.get('username', '').lower()
            if current_admin in user_list_del:
                user_list_del.remove(current_admin)
            
            user_to_delete = st.selectbox("Selecione o Usuário para EXCLUIR:", user_list_del, key="del_forn_select", index=None)
            if user_to_delete:
                st.warning(f"⚠️ **Atenção:** Esta ação é irreversível e removerá o acesso de **{user_to_delete}**.")
                if st.button(f"Confirmar Exclusão de {user_to_delete}", type="primary"):
                    if delete_fornecedor(engine, user_to_delete):
                        st.success(f"Usuário '{user_to_delete}' excluído com sucesso.")
                        st.rerun()
