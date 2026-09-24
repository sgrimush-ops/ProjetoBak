import subprocess
import sys
import os

# Script Python para commit e push automático do ProjetoBak_Sincronizador
# Uso: python sub_commit.py "mensagem do commit"

def run(cmd, cwd=None):
    print(f"Executando: {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd)
    if result.returncode != 0:
        print(f"Erro ao executar: {cmd}")
        sys.exit(result.returncode)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python sub_commit.py 'mensagem do commit'")
        sys.exit(1)

    commit_msg = sys.argv[1]
    pasta_modulo = os.path.dirname(os.path.abspath(__file__))
    pasta_raiz = os.path.abspath(os.path.join(pasta_modulo, '..', '..'))

    # Commit e push no subdiretório
    run("git add .", cwd=pasta_modulo)
    run(f"git commit -m \"{commit_msg}\"", cwd=pasta_modulo)
    run("git push", cwd=pasta_modulo)

    # Atualiza submódulo na raiz
    run("git add Aplicativos/ProjetoBak_Sincronizador", cwd=pasta_raiz)
    run(f'git commit -m "chore(submodule): atualiza ProjetoBak_Sincronizador apos ajuste"', cwd=pasta_raiz)
    run("git push", cwd=pasta_raiz)
