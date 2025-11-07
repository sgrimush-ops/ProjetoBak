import sqlite3
import os

PEDIDOS_DB_PATH = 'data/pedidos.db'

def show_current_orders(c):
    """Mostra as linhas atuais para o usuário saber o ID."""
    print("\n--- PEDIDOS ATUAIS NO BANCO DE DADOS ---")
    
    # Seleciona as colunas chave para identificar o pedido
    try:
        c.execute("SELECT id, codigo, produto, data_pedido FROM pedidos_consolidados ORDER BY id")
    except sqlite3.OperationalError as e:
        print(f"Erro ao ler a tabela (pode estar faltando colunas): {e}")
        print("Tentando ler apenas 'id' e 'codigo'...")
        try:
             c.execute("SELECT id, codigo FROM pedidos_consolidados ORDER BY id")
        except Exception as e_inner:
             print(f"Falha total ao ler o banco: {e_inner}")
             return False
             
    rows = c.fetchall()
    
    if not rows:
        print("O banco de dados de pedidos está limpo. Nenhuma linha encontrada.")
        return False # Retorna False se estiver vazio
    
    # Detecta o número de colunas lidas
    num_cols = len(rows[0])

    if num_cols == 4:
        print(f"{'ID':<5} | {'Código (Corrompido)':<20} | {'Produto':<30} | {'Data':<20}")
        print("-" * 80)
        for row in rows:
            print(f"{str(row[0]):<5} | {str(row[1]):<20} | {str(row[2]):<30} | {str(row[3]):<20}")
    else: # Fallback se 'produto' ou 'data_pedido' estiverem faltando
        print(f"{'ID':<5} | {'Código (Corrompido)':<20}")
        print("-" * 30)
        for row in rows:
            print(f"{str(row[0]):<5} | {str(row[1]):<20}")

    print("-" * 80)
    return True # Retorna True se houver linhas

def delete_order_manually():
    """Loop interativo para deletar linhas manualmente."""
    if not os.path.exists(PEDIDOS_DB_PATH):
        print(f"Erro: Banco de dados não encontrado em {PEDIDOS_DB_PATH}")
        return

    conn = None
    try:
        conn = sqlite3.connect(PEDIDOS_DB_PATH, timeout=10)
        c = conn.cursor()

        while True:
            if not show_current_orders(c):
                # Se não houver linhas, sai do loop
                break
            
            user_input = input("Digite o ID da linha que você quer excluir (ou 'sair' para terminar): ")
            
            if user_input.lower() in ('sair', 'exit', 'q', 's'):
                print("Saindo do script de limpeza.")
                break
            
            try:
                id_to_delete = int(user_input)
                
                # Confirmação
                confirm = input(f"Tem certeza que quer deletar o ID {id_to_delete}? (s/n): ")
                if confirm.lower() != 's':
                    print("Exclusão cancelada.")
                    continue
                    
                # Deleta a linha
                c.execute("DELETE FROM pedidos_consolidados WHERE id = ?", (id_to_delete,))
                conn.commit()
                
                if c.rowcount > 0:
                    print(f"\n*** SUCESSO: Linha ID {id_to_delete} foi excluída. ***\n")
                else:
                    print(f"\n*** AVISO: Nenhuma linha encontrada com o ID {id_to_delete}. ***\n")
                    
            except ValueError:
                print(f"\n*** ERRO: '{user_input}' não é um ID válido. Por favor, digite um número. ***\n")
            except sqlite3.Error as e:
                print(f"\n*** ERRO SQL: {e} ***\n")
                conn.rollback()

        print("Limpeza manual concluída.")

    except sqlite3.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    delete_order_manually()