"""
services/exportacao_campanha.py
Módulo de Geração de Arquivos Excel e PDF para Compras, Supply e Lojas.
"""

from __future__ import annotations
import io
from datetime import datetime
from typing import Optional
import pandas as pd
from sqlalchemy import text
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors


def gerar_excel_transferencia_operacional(engine, campanha_id: str) -> bytes:
    """
    Gera a planilha oficial de transferência operacional (Supply -> CD).
    Colunas:
    1. Código Consinco
    2. Descrição do Produto
    3. Loja Destino
    4. Quantidade em Caixas
    5. Embalagem Transferência
    6. Volume Total em Unidades
    7. Tipo de Exposição
    8. Campanha
    """
    sql = text("""
        SELECT 
            ci.produto_codigo AS "Código Consinco",
            ci.descricao_snapshot AS "Descrição do Produto",
            COALESCE(ci.fornecedor, ci.comprador, 'GERAL') AS "Fornecedor",
            cl.loja_codigo AS "Loja Destino",
            cl.caixas_transferencia AS "Quantidade em Caixas",
            ci.embalagem_transferencia AS "Embalagem Transferência",
            cl.volume_transferencia AS "Volume Total em Unidades",
            COALESCE(te.nome, 'NÃO DEFINIDO') AS "Tipo de Exposição",
            c.codigo_campanha AS "Campanha"
        FROM campanha_lojas cl
        JOIN campanha_itens ci ON ci.id = cl.campanha_item_id
        JOIN campanhas c ON c.id = ci.campanha_id
        LEFT JOIN tipos_exposicao te ON te.id = cl.tipo_exposicao_id
        WHERE ci.campanha_id = :cid 
          AND UPPER(COALESCE(te.nome, '')) != 'INATIVA'
          AND COALESCE(cl.caixas_transferencia, 0) > 0
        ORDER BY ci.produto_codigo, cl.loja_codigo
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"cid": campanha_id})

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Transferencias_CD", index=False)
        worksheet = writer.sheets["Transferencias_CD"]
        for col in worksheet.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            col_letter = col[0].column_letter
            worksheet.column_dimensions[col_letter].width = max(max_len + 3, 12)

    return output.getvalue()


def gerar_excel_devolutiva_compras(engine, campanha_id: str) -> bytes:
    """
    Gera a planilha de devolutivas de faltas no CD15 para compras.
    """
    sql = text("""
        SELECT 
            cd.produto_codigo AS "Código Consinco",
            cd.descricao_snapshot AS "Descrição do Produto",
            cd.caixas_necessarias AS "Caixas Totais Necessárias",
            cd.caixas_cd_disponivel AS "Caixas Disponíveis CD15",
            cd.caixas_falta AS "Caixas Faltantes (Comprar)",
            ci.embalagem_compra AS "Embalagem Compra",
            cd.caixas_falta AS "Sugestão Pedido Fornecedor (Caixas)",
            cd.situacao AS "Situação",
            cd.observacao AS "Observação"
        FROM campanha_devolutivas cd
        JOIN campanha_itens ci ON ci.campanha_id = cd.campanha_id AND ci.produto_codigo = cd.produto_codigo
        WHERE cd.campanha_id = :cid
        ORDER BY cd.produto_codigo
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"cid": campanha_id})

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Pendencias_Compra", index=False)
        worksheet = writer.sheets["Pendencias_Compra"]
        for col in worksheet.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            col_letter = col[0].column_letter
            worksheet.column_dimensions[col_letter].width = max(max_len + 3, 12)

    return output.getvalue()


def gerar_excel_consulta_loja(engine, campanha_id: str, loja_codigo: Optional[str] = None) -> bytes:
    """
    Gera o Excel de consulta para a loja física ou consolidado de todas as lojas.
    """
    params = {"cid": campanha_id}
    where_loja = ""
    col_loja_sql = ""
    
    if loja_codigo and str(loja_codigo).upper() != "TODAS":
        lj = str(loja_codigo).zfill(3)
        where_loja = "AND cl.loja_codigo = :loja"
        params["loja"] = lj
    else:
        col_loja_sql = "cl.loja_codigo AS \"Loja\", l.nome AS \"Nome da Loja\","

    sql = text(f"""
        SELECT 
            c.codigo_campanha AS "Campanha",
            c.data_inicio AS "Vigência Início",
            c.data_fim AS "Vigência Fim",
            {col_loja_sql}
            ci.produto_codigo AS "Código Produto",
            ci.descricao_snapshot AS "Descrição",
            COALESCE(te.nome, 'NÃO DEFINIDO') AS "Tipo de Exposição",
            cl.volume_final_supply AS "Volume Final (Unidades)",
            cl.caixas_transferencia AS "Caixas a Receber"
        FROM campanha_lojas cl
        JOIN campanha_itens ci ON ci.id = cl.campanha_item_id
        JOIN campanhas c ON c.id = ci.campanha_id
        JOIN lojas l ON l.codigo = cl.loja_codigo
        LEFT JOIN tipos_exposicao te ON te.id = cl.tipo_exposicao_id
        WHERE ci.campanha_id = :cid 
          AND UPPER(COALESCE(te.nome, '')) != 'INATIVA' {where_loja}
        ORDER BY cl.loja_codigo, ci.produto_codigo
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Itens_Campanha", index=False)
        worksheet = writer.sheets["Itens_Campanha"]
        for col in worksheet.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            col_letter = col[0].column_letter
            worksheet.column_dimensions[col_letter].width = max(max_len + 3, 12)

    return output.getvalue()


def gerar_pdf_campanha_loja(engine, campanha_id: str, loja_codigo: str) -> bytes:
    """
    Gera documento PDF formatado pronto para impressão da loja física.
    """
    lj = str(loja_codigo).zfill(3)
    
    # Busca dados da capa e loja
    with engine.connect() as conn:
        camp = conn.execute(text("""
            SELECT codigo_campanha, nome, data_inicio, data_fim 
            FROM campanhas 
            WHERE id = :cid
        """), {"cid": campanha_id}).fetchone()

        loja = conn.execute(text("""
            SELECT codigo, nome 
            FROM lojas 
            WHERE codigo = :loja
        """), {"loja": lj}).fetchone()

        itens = conn.execute(text("""
            SELECT 
                ci.produto_codigo,
                ci.descricao_snapshot,
                COALESCE(te.nome, 'N/D') as tipo_exposicao,
                cl.volume_final_supply,
                cl.caixas_transferencia
            FROM campanha_lojas cl
            JOIN campanha_itens ci ON ci.id = cl.campanha_item_id
            LEFT JOIN tipos_exposicao te ON te.id = cl.tipo_exposicao_id
            WHERE ci.campanha_id = :cid 
              AND cl.loja_codigo = :loja
              AND UPPER(COALESCE(te.nome, '')) != 'INATIVA'
            ORDER BY ci.produto_codigo
        """), {"cid": campanha_id, "loja": lj}).fetchall()

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=30,
        rightMargin=30,
        topMargin=30,
        bottomMargin=30
    )

    elements = []
    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "TitleStyle",
        parent=styles["Heading1"],
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#1e293b"),
        alignment=1
    )
    subtitle_style = ParagraphStyle(
        "SubTitleStyle",
        parent=styles["Normal"],
        fontSize=11,
        leading=15,
        textColor=colors.HexColor("#475569"),
        alignment=1
    )
    cell_style = ParagraphStyle(
        "CellStyle",
        parent=styles["Normal"],
        fontSize=9,
        leading=12
    )
    header_style = ParagraphStyle(
        "HeaderStyle",
        parent=styles["Normal"],
        fontSize=9,
        leading=12,
        textColor=colors.white,
        fontName="Helvetica-Bold"
    )

    # Cabeçalho
    camp_nome = camp.nome if camp else "Campanha"
    camp_cod = camp.codigo_campanha if camp else ""
    d_ini = camp.data_inicio.strftime("%d/%m/%Y") if camp and camp.data_inicio else ""
    d_fim = camp.data_fim.strftime("%d/%m/%Y") if camp and camp.data_fim else ""
    loja_nome = f"{loja.codigo} - {loja.nome}" if loja else lj

    elements.append(Paragraph("<b>BAKLIZI SUPERMERCADOS</b>", title_style))
    elements.append(Paragraph(f"<b>Plano de Exposição e Abastecimento: {camp_nome} ({camp_cod})</b>", subtitle_style))
    elements.append(Paragraph(f"<b>Unidade:</b> {loja_nome} | <b>Vigência:</b> {d_ini} a {d_fim}", subtitle_style))
    elements.append(Spacer(1, 15))

    # Tabela de Itens
    data_table = [
        [
            Paragraph("Cód", header_style),
            Paragraph("Descrição do Produto", header_style),
            Paragraph("Exposição", header_style),
            Paragraph("Vol (Un)", header_style),
            Paragraph("Caixas", header_style),
            Paragraph("Conf. [ ]", header_style)
        ]
    ]

    for it in itens:
        data_table.append([
            Paragraph(str(it.produto_codigo), cell_style),
            Paragraph(str(it.descricao_snapshot), cell_style),
            Paragraph(str(it.tipo_exposicao), cell_style),
            Paragraph(str(it.volume_final_supply or 0), cell_style),
            Paragraph(str(it.caixas_transferencia or 0), cell_style),
            Paragraph("[   ]", cell_style)
        ])

    table = Table(data_table, colWidths=[45, 230, 110, 50, 45, 55])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("ALIGN", (3, 1), (4, -1), "CENTER"),
        ("ALIGN", (5, 1), (5, -1), "CENTER"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")])
    ]))

    elements.append(table)
    elements.append(Spacer(1, 25))

    # Rodapé com assinatura
    footer_text = f"Documento emitido em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
    elements.append(Paragraph(footer_text, ParagraphStyle("Footer", parent=styles["Normal"], fontSize=8, textColor=colors.gray)))
    elements.append(Spacer(1, 15))
    elements.append(Paragraph("____________________________________________________", subtitle_style))
    elements.append(Paragraph("Assinatura do Encarregado / Gerente de Loja", subtitle_style))

    doc.build(elements)
    return buffer.getvalue()
