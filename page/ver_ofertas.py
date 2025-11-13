import streamlit as st
import pandas as pd
from sqlalchemy import text
from datetime import datetime

# =========================================================
# FUNÇÕES DE BANCO DE DADOS
# =========================================================

@st.cache_data(ttl=300) # Cache de 5 minutos
def get_ofertas_atuais(_engine):
    """Busca ofertas onde a data final é hoje ou no futuro."""
    today = datetime.now().date()
    query = text("""
        SELECT 
            id, 
            codigo, 
            produto, 
            oferta, 
            data_inicio, 
            data_final
        FROM ofertas
        WHERE data_final >= :today
        ORDER BY data_inicio ASC
    """)
    
    with _engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"today": today})
    return df

def update_oferta_no_banco(engine, id_oferta, campo, novo_valor):
    """Atualiza um único campo de uma oferta."""
    try:
        with engine.begin() as conn:
            # Proteção simples contra SQL Injection (garante que 'campo' seja seguro)
            campos_permitidos = ['oferta', 'produto', 'codigo', 'data_inicio', 'data_final']
            if campo not in campos_permitidos:
                st.error(f"Erro: Tentativa de atualizar campo inválido '{campo}'.")
                return
            
            # Formata a data corretamente se for o caso
            if "data" in campo:
                novo_valor = pd.to_datetime(novo_valor).date()
            
            query = text(f"""
                UPDATE ofertas
                SET {campo} = :valor
                WHERE id = :id_oferta
            """)
            conn.execute(query, {"valor": novo_valor, "id_oferta": id_oferta})
        
        # Limpa o cache após a edição
        get_ofertas_atuais.clear()
        
    except Exception as e:
        st.error(f"Erro ao atualizar a oferta: {e}")

def deletar_oferta_do_banco(engine, id_oferta):
    """Deleta uma oferta do banco de dados."""
    try:
        with engine.begin() as conn:
            query = text("DELETE FROM ofertas WHERE id = :id_oferta")
            conn.execute(query, {"id_oferta": id_oferta})
        
        # Limpa o cache após a deleção
        get_ofertas_atuais.clear()
        
    except Exception as e:
        st.error(f"Erro ao deletar a oferta: {e}")

# =========================================================
# INTERFACE DA PÁGINA
# =========================================================

def show_ver_ofertas_page(engine, base_data_path):
    st.title("🛒 Ofertas Atuais")
    
    role = st.session_state.get("role", "user")
    
    # Define se o usuário pode editar
    pode_editar = (role == 'admin') or (role == 'mkt')

    df_ofertas = get_ofertas_atuais(engine)
    
    if df_ofertas.empty:
        st.info("Nenhuma oferta ativa encontrada no sistema.")
        st.stop()
        
    if pode_editar:
        st.info("Como Admin/Mkt, você pode editar ou deletar ofertas diretamente na tabela abaixo.")
        st.markdown("Para **deletar**, marque a caixa 'Deletar' e clique fora da tabela.")

        # --- Visão de Edição (Admin / Mkt) ---
        
        # Adiciona a coluna de deleção
        df_ofertas["Deletar"] = False
        
        # Reordena colunas para a edição
        colunas = [
            'Deletar', 'id', 'codigo', 'produto', 'oferta', 
            'data_inicio', 'data_final'
        ]
        
        # Configuração das colunas
        config = {
            "id": st.column_config.NumberColumn("ID", disabled=True, format="%d"),
            "codigo": st.column_config.NumberColumn("Código", format="%d"),
            "produto": st.column_config.TextColumn("Produto"),
            "oferta": st.column_config.NumberColumn("Oferta (R$)", format="%.2f"),
            "data_inicio": st.column_config.DateColumn("Início", format="DD/MM/YYYY"),
            "data_final": st.column_config.DateColumn("Final", format="DD/MM/YYYY"),
            "Deletar": st.column_config.CheckboxColumn("Deletar?")
        }

        # Salva o estado atual para comparar mudanças
        if 'df_ofertas_original' not in st.session_state:
            st.session_state.df_ofertas_original = df_ofertas.copy()

        df_editado = st.data_editor(
            df_ofertas,
            column_order=colunas,
            column_config=config,
            hide_index=True,
            use_container_width=True,
            key="editor_ofertas"
        )
        
        # --- Lógica para Salvar Mudanças ---
        if df_editado is not None:
            # 1. Processar Deleções
            # (Precisamos processar deleções primeiro)
            ids_para_deletar = df_editado[df_editado["Deletar"] == True]["id"]
            if not ids_para_deletar.empty:
                for id_oferta in ids_para_deletar:
                    deletar_oferta_do_banco(engine, id_oferta)
                st.session_state.df_ofertas_original = None # Força recarregar
                st.success(f"{len(ids_para_deletar)} oferta(s) deletada(s).")
                st.rerun()

            # 2. Processar Edições
            # Compara o DataFrame editado com o original
            try:
                # 'ne' faz a comparação elemento a elemento
                mudancas = (df_editado != st.session_state.df_ofertas_original).any(axis=1)
                linhas_mudadas = df_editado[mudancas]
                
                if not linhas_mudadas.empty:
                    for index, linha in linhas_mudadas.iterrows():
                        id_mudado = linha['id']
                        # Compara cada coluna da linha mudada com a original
                        original_linha = st.session_state.df_ofertas_original.loc[index]
                        
                        for col_nome in df_editado.columns:
                            if col_nome == 'Deletar' or col_nome == 'id':
                                continue # Ignora colunas de controle
                                
                            if linha[col_nome] != original_linha[col_nome]:
                                # Achamos a célula que mudou!
                                update_oferta_no_banco(engine, id_mudado, col_nome, linha[col_nome])
                                st.success(f"Oferta ID {id_mudado} atualizada (Campo: {col_nome}).")
                    
                    st.session_state.df_ofertas_original = None # Força recarregar
                    st.rerun()

            except Exception as e:
                # Isso pode falhar se as colunas mudarem (ex: após deleção)
                pass # Ignora erros de comparação de dataframe

    else:
        # --- Visão Somente Leitura (Usuário Padrão) ---
        st.info("Você pode visualizar as ofertas atuais e usar os filtros nas colunas.")
        st.dataframe(
            df_ofertas,
            column_config={
                "id": None, # Esconde o ID
                "codigo": "Código",
                "produto": "Produto",
                "oferta": st.column_config.NumberColumn("Oferta (R$)", format="%.2f"),
                "data_inicio": st.column_config.DateColumn("Início", format="DD/MM/YYYY"),
                "data_final": st.column_config.DateColumn("Final", format="DD/MM/YYYY"),
            },
            hide_index=True,
            use_container_width=True
        )
