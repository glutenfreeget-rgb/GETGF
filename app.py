
import os
import unicodedata
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import psycopg, psycopg.rows
import streamlit as st

# ===================== CONFIG =====================
st.set_page_config(
    page_title="Restô ERP Lite",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===================== CSS =====================
st.markdown("""
<style>
    .main .block-container { padding-top: 0.75rem; padding-bottom: 1rem; }
    .modern-header {
        background: linear-gradient(135deg, #06b6d4, #3b82f6);
        padding: 1.0rem 1.25rem; border-radius: 14px; color: white;
        margin-bottom: 0.75rem; box-shadow: 0 10px 30px rgba(0,0,0,.08);
    }
    .modern-card {
        background: white; padding: 1rem 1.25rem; border-radius: 14px;
        box-shadow: 0 4px 18px rgba(0,0,0,.06); margin-bottom: 0.75rem;
    }
    .muted { color: #64748b; }
    .pill { padding: 2px 8px; border-radius: 999px; background:#f1f5f9; font-size:12px; }
    .ok { color: #059669; }
    .warn { color: #b45309; }
    .bad { color: #dc2626; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
    .kpi { font-variant-numeric: tabular-nums; }
</style>
"""", unsafe_allow_html=True)

# ===================== DB Helpers =====================
def _get_conn():
    # Prefer st.secrets if available
    host = st.secrets.get("DB_HOST", os.getenv("DB_HOST", ""))
    port = st.secrets.get("DB_PORT", os.getenv("DB_PORT", "5432"))
    user = st.secrets.get("DB_USER", os.getenv("DB_USER", ""))
    pwd  = st.secrets.get("DB_PASSWORD", os.getenv("DB_PASSWORD", ""))
    db   = st.secrets.get("DB_NAME", os.getenv("DB_NAME", ""))
    ssl  = st.secrets.get("DB_SSLMODE", os.getenv("DB_SSLMODE", "require"))
    if not host or not user or not pwd or not db:
        st.error("Configure as variáveis de conexão do banco (DB_HOST, DB_USER, DB_PASSWORD, DB_NAME).")
    conn = psycopg.connect(
        host=host, port=port, user=user, password=pwd, dbname=db,
        options="", autocommit=True, row_factory=psycopg.rows.dict_row, sslmode=ssl
    )
    return conn

def qall(sql: str, params: Optional[Tuple]=None) -> List[Dict[str, Any]]:
    with _get_conn() as con, con.cursor() as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
    return rows

def qone(sql: str, params: Optional[Tuple]=None) -> Optional[Dict[str, Any]]:
    with _get_conn() as con, con.cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
    return row

def qexec(sql: str, params: Optional[Tuple]=None) -> int:
    with _get_conn() as con, con.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.rowcount or 0

# ===================== Ensure Schema (ping) =====================
def ensure_ping():
    try:
        qone("select 1;")
        return True
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return False

# ===================== UI Helpers =====================
def header(title: str, subtitle: Optional[str] = None):
    st.markdown(f"<div class='modern-header'><h2 style='margin:0'>{title}</h2>" +
                (f"<div class='muted'>{subtitle}</div>" if subtitle else "") +
                "</div>", unsafe_allow_html=True)

def card_start(): st.markdown("<div class='modern-card'>", unsafe_allow_html=True)
def card_end():   st.markdown("</div>", unsafe_allow_html=True)

# ===================== Pages =====================

def page_dashboard():
    header("📊 Painel", "Visão geral do financeiro e estoque.")
    col1, col2, col3 = st.columns(3)

    stock = qone("select coalesce(sum(stock_qty * avg_cost),0) as val, coalesce(sum(stock_qty),0) as qty from resto.product;" ) or {}
    cmv = qall("select month, cmv_value from resto.v_cmv order by month desc limit 6;")

    with col1:
        card_start()
        st.markdown("**Valor do Estoque (CMP)**\n\n<h3 class='kpi'>R$ {:,.2f}</h3>".format(stock.get('val',0)).replace(',', 'X').replace('.', ',').replace('X','.'), unsafe_allow_html=True)
        st.caption("Quantidade total em estoque: {:.2f}".format(stock.get('qty',0)))
        card_end()

    with col2:
        card_start()
        st.markdown("**Últimos CMVs (mensal)**")
        df = pd.DataFrame(cmv)
        if not df.empty:
            df['month'] = df['month'].dt.strftime('%Y-%m')
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.caption("Sem dados de CMV ainda.")
        card_end()

    with col3:
        card_start()
        today_sales = qone("select coalesce(sum(total),0) as tot from resto.sale where date::date = current_date and status='FECHADA';") or {}
        st.markdown("**Vendas Hoje (fechadas)**\n\n<h3 class='kpi'>R$ {:,.2f}</h3>".format(today_sales.get('tot',0)).replace(',', 'X').replace('.', ',').replace('X','.'), unsafe_allow_html=True)
        card_end()

def page_cadastros():
    header("🗂️ Cadastros", "Unidades, Categorias, Produtos e Fornecedores.")
    tabs = st.tabs(["Unidades", "Categorias", "Fornecedores", "Produtos"])

    # ---------- Unidades ----------
    with tabs[0]:
        card_start()
        st.subheader("Unidades de Medida");
        with st.form("form_unit"):
            name = st.text_input("Nome", value="Unidade")
            abbr = st.text_input("Abreviação", value="un")
            base_hint = st.text_input("Observação/Conversão", value="ex: 1 un = 25 g (dica)")
            ok = st.form_submit_button("Salvar unidade")
        if ok and name and abbr:
            qexec("insert into resto.unit(name, abbr, base_hint) values (%s,%s,%s) on conflict (abbr) do update set name=excluded.name, base_hint=excluded.base_hint;", (name, abbr, base_hint))
            st.success("Unidade salva!")
        units = qall("select id, name, abbr, base_hint from resto.unit order by abbr;");
        st.dataframe(pd.DataFrame(units), use_container_width=True, hide_index=True)
        card_end()

    # ---------- Categorias ----------
    with tabs[1]:
        card_start()
        st.subheader("Categorias");
        with st.form("form_cat"):
            name = st.text_input("Nome da categoria")
            ok = st.form_submit_button("Salvar categoria")
        if ok and name:
            qexec("insert into resto.category(name) values (%s) on conflict(name) do nothing;", (name,))
            st.success("Categoria salva!")
        cats = qall("select id, name from resto.category order by name;");
        st.dataframe(pd.DataFrame(cats), use_container_width=True, hide_index=True)
        card_end()

    # ---------- Fornecedores ----------
    with tabs[2]:
        card_start()
        st.subheader("Fornecedores");
        with st.form("form_sup"):
            name = st.text_input("Nome *")
            cnpj = st.text_input("CNPJ")
            ie   = st.text_input("Inscrição Estadual")
            email= st.text_input("Email")
            phone= st.text_input("Telefone")
            ok = st.form_submit_button("Salvar fornecedor")
        if ok and name:
            qexec("""
                insert into resto.supplier(name, cnpj, ie, email, phone)
                values (%s,%s,%s,%s,%s);
            """, (name, cnpj, ie, email, phone))
            st.success("Fornecedor salvo!")
        rows = qall("select id, name, cnpj, ie, email, phone from resto.supplier order by name;");
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        card_end()

    # ---------- Produtos ----------
    with tabs[3]:
        card_start()
        st.subheader("Produtos (Catálogo Fiscal)");
        units = qall("select id, abbr from resto.unit order by abbr;")
        cats  = qall("select id, name from resto.category order by name;")

        with st.form("form_prod"):
            code = st.text_input("Código interno")
            name = st.text_input("Nome *")
            category_id = st.selectbox("Categoria", options=[(c['id'], c['name']) for c in cats], format_func=lambda x: x[1] if isinstance(x, tuple) else x, index=0 if cats else None)
            unit_id = st.selectbox("Unidade *", options=[(u['id'], u['abbr']) for u in units], format_func=lambda x: x[1] if isinstance(x, tuple) else x, index=0 if units else None)

            st.markdown("**Campos fiscais (opcional agora; útil para emissão de documentos)**")
            colf1, colf2, colf3, colf4 = st.columns(4)
            with colf1:
                ncm = st.text_input("NCM") ; cest = st.text_input("CEST")
            with colf2:
                cfop = st.text_input("CFOP Venda"); csosn = st.text_input("CSOSN")
            with colf3:
                cst_icms = st.text_input("CST ICMS"); ali_icms = st.number_input("Alíquota ICMS %", 0.0, 100.0, 0.0, 0.01)
            with colf4:
                cst_pis = st.text_input("CST PIS"); ali_pis = st.number_input("Alíquota PIS %", 0.0, 100.0, 0.0, 0.01)
            colf5, colf6 = st.columns(2)
            with colf5:
                cst_cof = st.text_input("CST COFINS"); ali_cof = st.number_input("Alíquota COFINS %", 0.0, 100.0, 0.0, 0.01)
            with colf6:
                iss = st.number_input("ISS % (se serviço)", 0.0, 100.0, 0.0, 0.01)

            colb1, colb2, colb3 = st.columns(3)
            with colb1:
                is_sale = st.checkbox("Item de venda", value=True)
            with colb2:
                is_ing  = st.checkbox("Ingrediente", value=False)
            with colb3:
                markup = st.number_input("Markup padrão %", 0.0, 1000.0, 0.0, 0.1)

            ok = st.form_submit_button("Salvar produto")

        if ok and name and unit_id:
            cat_id = category_id[0] if isinstance(category_id, tuple) else None
            uni_id = unit_id[0] if isinstance(unit_id, tuple) else None
            qexec("""
                insert into resto.product(code, name, category_id, unit_id, ncm, cest, cfop_venda, csosn, cst_icms, aliquota_icms, cst_pis, aliquota_pis, cst_cofins, aliquota_cofins, iss_aliquota, is_sale_item, is_ingredient, default_markup)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict (name) do update set
                  code=excluded.code, category_id=excluded.category_id, unit_id=excluded.unit_id,
                  ncm=excluded.ncm, cest=excluded.cest, cfop_venda=excluded.cfop_venda, csosn=excluded.csosn,
                  cst_icms=excluded.cst_icms, aliquota_icms=excluded.aliquota_icms,
                  cst_pis=excluded.cst_pis, aliquota_pis=excluded.aliquota_pis,
                  cst_cofins=excluded.cst_cofins, aliquota_cofins=excluded.aliquota_cofins,
                  iss_aliquota=excluded.iss_aliquota, is_sale_item=excluded.is_sale_item, is_ingredient=excluded.is_ingredient, default_markup=excluded.default_markup;
            """, (code, name, cat_id, uni_id, ncm, cest, cfop, csosn, cst_icms, ali_icms, cst_pis, ali_pis, cst_cof, ali_cof, iss, is_sale, is_ing, markup))
            st.success("Produto salvo!");

        prods = qall("select id, code, name, stock_qty, avg_cost, last_cost from resto.product order by name;");
        st.dataframe(pd.DataFrame(prods), use_container_width=True, hide_index=True)
        card_end()

def page_compras():
    header("📥 Compras", "Lançar notas de entrada e atualizar estoque (CMP)." )
    suppliers = qall("select id, name from resto.supplier order by name;")
    prods = qall("select id, name from resto.product order by name;" )
    units = qall("select id, abbr from resto.unit order by abbr;" )

    card_start()
    with st.form("form_compra"):
        supplier = st.selectbox("Fornecedor *", options=[(s['id'], s['name']) for s in suppliers], format_func=lambda x: x[1])
        doc_number = st.text_input("Número do documento")
        cfop_ent = st.text_input("CFOP Entrada", value="1102")
        doc_date = st.date_input("Data", value=date.today())
        freight = st.number_input("Frete", 0.00, 9999999.99, 0.00, 0.01)
        other = st.number_input("Outros custos", 0.00, 9999999.99, 0.00, 0.01)

        st.markdown("**Itens da compra**")
        if "compra_itens" not in st.session_state:
            st.session_state["compra_itens"] = []  # list of dicts

        with st.expander("Adicionar item"):
            prod = st.selectbox("Produto", options=[(p['id'], p['name']) for p in prods], format_func=lambda x: x[1])
            unit = st.selectbox("Unidade", options=[(u['id'], u['abbr']) for u in units], format_func=lambda x: x[1])
            qty = st.number_input("Quantidade", 0.0, 1_000_000.0, 1.0, 0.1)
            unit_price = st.number_input("Preço unitário", 0.0, 1_000_000.0, 0.0, 0.01)
            discount = st.number_input("Desconto", 0.0, 1_000_000.0, 0.0, 0.01)
            lote = st.text_input("Lote")
            expiry = st.date_input("Validade", value=None)
            add = st.button("Adicionar à lista")
            if add and prod and unit and qty>0:
                total = (qty * unit_price) - discount
                st.session_state["compra_itens"].append({
                    "product_id": prod[0], "product_name": prod[1],
                    "unit_id": unit[0], "unit_abbr": unit[1],
                    "qty": qty, "unit_price": unit_price, "discount": discount,
                    "total": total, "lot_number": lote, "expiry_date": str(expiry) if expiry else None
                })
                st.success("Item adicionado!" );

        df = pd.DataFrame(st.session_state["compra_itens"]) if st.session_state["compra_itens"] else pd.DataFrame(columns=["product_name","qty","unit_abbr","unit_price","discount","total"])
        st.dataframe(df, use_container_width=True, hide_index=True)

        total_doc = float(df["total"].sum()) if not df.empty else 0.0
        st.markdown(f"**Total itens:** R$ {total_doc:,.2f}".replace(',', 'X').replace('.', ',').replace('X','.'))

        submit = st.form_submit_button("Lançar compra e atualizar estoque" )

    card_end()

    if submit and supplier and doc_date and df is not None:
        # Cria header da compra
        pid = supplier[0]
        row = qone("""
            insert into resto.purchase(supplier_id, doc_number, cfop_entrada, doc_date, freight_value, other_costs, total, status)
            values (%s,%s,%s,%s,%s,%s,%s,'LANÇADA')
            returning id;
        """, (pid, doc_number, cfop_ent, doc_date, freight, other, total_doc))
        purchase_id = row["id"]
        # Insere itens + movimenta estoque via sp
        for it in st.session_state["compra_itens"]:
            qexec("""
                insert into resto.purchase_item(
                  purchase_id, product_id, qty, unit_id, unit_price, discount, total, lot_number, expiry_date
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, (purchase_id, it["product_id"], it["qty"], it["unit_id"], it["unit_price"], it["discount"], it["total"], it["lot_number"], it["expiry_date"]))

            # registra movimento IN e atualiza CMP
            qexec("select resto.sp_register_movement(%s,'IN',%s,%s,'purchase',%s,%s);", (it["product_id"], it["qty"], it["unit_price"], purchase_id, f"lote {it['lot_number'] or ''}"))

        st.session_state["compra_itens"] = []
        st.success(f"Compra #{purchase_id} lançada e estoque atualizado!" )

def page_vendas():
    header("🧾 Vendas (simples)", "Registre saídas e gere CMV.")
    prods = qall("select id, name from resto.product where is_sale_item order by name;" )

    card_start()
    with st.form("form_sale"):
        sale_date = st.date_input("Data", value=date.today())
        if "sale_itens" not in st.session_state:
            st.session_state["sale_itens"] = []

        with st.expander("Adicionar item"):
            prod = st.selectbox("Produto", options=[(p['id'], p['name']) for p in prods], format_func=lambda x: x[1])
            qty = st.number_input("Quantidade", 0.0, 1_000_000.0, 1.0, 0.1)
            price = st.number_input("Preço unitário", 0.0, 1_000_000.0, 0.0, 0.01)
            add = st.button("Adicionar")
            if add and prod and qty>0:
                st.session_state["sale_itens"].append({"product_id": prod[0], "product_name": prod[1], "qty": qty, "unit_price": price, "total": qty*price})
                st.success("Item adicionado!")

        df = pd.DataFrame(st.session_state["sale_itens"]) if st.session_state["sale_itens"] else pd.DataFrame(columns=["product_name","qty","unit_price","total"])
        st.dataframe(df, use_container_width=True, hide_index=True)
        total = float(df["total"].sum()) if not df.empty else 0.0
        st.markdown(f"**Total da venda:** R$ {total:,.2f}".replace(',', 'X').replace('.', ',').replace('X','.'))

        submit = st.form_submit_button("Fechar venda e dar baixa no estoque" )
    card_end()

    if submit and sale_date and df is not None:
        row = qone("insert into resto.sale(date, total, status) values (%s,%s,'FECHADA') returning id;", (sale_date, total))
        sale_id = row["id"]
        for it in st.session_state["sale_itens"]:
            qexec("insert into resto.sale_item(sale_id, product_id, qty, unit_price, total) values (%s,%s,%s,%s,%s);", (sale_id, it["product_id"], it["qty"], it["unit_price"], it["total"]) )
            # Saída usa CMP atual
            qexec("select resto.sp_register_movement(%s,'OUT',%s,null,'sale',%s,%s);", (it["product_id"], it["qty"], sale_id, ''))
        st.session_state["sale_itens"] = []
        st.success(f"Venda #{sale_id} fechada e estoque baixado!" )

def page_receitas_precos():
    header("🥣 Fichas Técnicas & Precificação", "Monte receitas e calcule preço sugerido.")
    prods = qall("select id, name from resto.product order by name;")
    units = qall("select id, abbr from resto.unit order by abbr;")

    tab = st.tabs(["Receitas", "Precificação"])  # 0: receita, 1: precificação

    # ---------- Receitas ----------
    with tab[0]:
        card_start()
        st.subheader("Ficha técnica do produto")
        prod = st.selectbox("Produto final *", options=[(p['id'], p['name']) for p in prods], format_func=lambda x: x[1])
        if prod:
            recipe = qone("select * from resto.recipe where product_id=%s;", (prod[0],))
            if not recipe:
                st.info("Sem receita ainda. Preencha para criar.")
                with st.form("form_new_recipe"):
                    yield_qty = st.number_input("Rendimento (quantidade)", 0.0, 1_000_000.0, 10.0, 0.1)
                    yield_unit = st.selectbox("Unidade do rendimento", options=[(u['id'], u['abbr']) for u in units], format_func=lambda x: x[1])
                    overhead = st.number_input("Custo indireto % (gás/energia/mão de obra)", 0.0, 999.0, 0.0, 0.1)
                    loss = st.number_input("Perdas %", 0.0, 100.0, 0.0, 0.1)
                    ok = st.form_submit_button("Criar ficha técnica")
                if ok:
                    qexec("insert into resto.recipe(product_id, yield_qty, yield_unit_id, overhead_pct, loss_pct) values (%s,%s,%s,%s,%s);", (prod[0], yield_qty, yield_unit[0], overhead, loss))
                    st.success("Ficha criada!")
                    recipe = qone("select * from resto.recipe where product_id=%s;", (prod[0],))

            if recipe:
                st.caption(f"Rende {recipe['yield_qty']} {qone('select abbr from resto.unit where id=%s;', (recipe['yield_unit_id'],))['abbr']} | Indiretos: {recipe['overhead_pct']}% | Perdas: {recipe['loss_pct']}% ")
                with st.expander("Adicionar ingrediente"):
                    ing = st.selectbox("Ingrediente", options=[(p['id'], p['name']) for p in prods], format_func=lambda x: x[1], key="ing_sel")
                    qty = st.number_input("Quantidade", 0.0, 1_000_000.0, 1.0, 0.1, key="ing_qty")
                    unit = st.selectbox("Unidade", options=[(u['id'], u['abbr']) for u in units], format_func=lambda x: x[1], key="ing_unit")
                    conv = st.number_input("Fator de conversão (opcional)", 0.0, 1_000_000.0, 1.0, 0.01, help="Ex: 1 colher = 15 ml → use 15 se seu estoque estiver em ml.")
                    add = st.button("Adicionar ingrediente", key="ing_add")
                    if add and ing and qty>0:
                        qexec("insert into resto.recipe_item(recipe_id, ingredient_id, qty, unit_id, conversion_factor) values (%s,%s,%s,%s,%s);", (recipe['id'], ing[0], qty, unit[0], conv))
                        st.success("Ingrediente incluído!" )

                items = qall("""
                    select ri.id, p.name as ingrediente, ri.qty, u.abbr
                      from resto.recipe_item ri
                      join resto.product p on p.id = ri.ingredient_id
                      join resto.unit u on u.id = ri.unit_id
                     where ri.recipe_id=%s
                     order by p.name;
                """, (recipe['id'],))
                st.dataframe(pd.DataFrame(items), use_container_width=True, hide_index=True)

                # custo estimado (view)
                cost = qone("select * from resto.v_recipe_cost where product_id=%s;", (prod[0],))
                if cost:
                    st.markdown(f"**Custo do lote:** R$ {cost['batch_cost']:,.2f} • **Custo unitário estimado:** R$ {cost['unit_cost_estimated']:,.2f}".replace(',', 'X').replace('.', ',').replace('X','.'))
                else:
                    st.caption("Adicione ingredientes para ver o custo.")
        card_end()

    # ---------- Precificação ----------
    with tab[1]:
        card_start()
        st.subheader("Simulador de Preço de Venda" )
        prod = st.selectbox("Produto", options=[(p['id'], p['name']) for p in prods], format_func=lambda x: x[1], key="prec_prod")
        if prod:
            rec = qone("select unit_cost_estimated from resto.v_recipe_cost where product_id=%s;", (prod[0],))
            avg = qone("select avg_cost from resto.product where id=%s;", (prod[0],))
            base_cost = rec['unit_cost_estimated'] if rec and rec['unit_cost_estimated'] else (avg['avg_cost'] if avg else 0.0)
            st.markdown(f"**Custo base (estimado ou CMP):** R$ {base_cost:,.2f}".replace(',', 'X').replace('.', ',').replace('X','.'))

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                markup = st.number_input("Markup %", 0.0, 1000.0, 200.0, 0.1)
            with col2:
                taxa_cartao = st.number_input("Taxas/Cartão %", 0.0, 30.0, 0.0, 0.1)
            with col3:
                impostos = st.number_input("Impostos s/ venda %", 0.0, 50.0, 8.0, 0.1, help="Estimativa Simples Nacional/ISS/PIS/COFINS.")
            with col4:
                desconto = st.number_input("Desconto médio %", 0.0, 100.0, 0.0, 0.1)

            preco_sugerido = base_cost * (1 + (markup/100.0))
            preco_liquido = preco_sugerido * (1 - desconto/100.0) * (1 - taxa_cartao/100.0) * (1 - impostos/100.0)

            st.markdown(f"**Preço sugerido:** R$ {preco_sugerido:,.2f} • **Receita líquida estimada:** R$ {preco_liquido:,.2f}".replace(',', 'X').replace('.', ',').replace('X','.'))
        card_end()

def page_estoque():
    header("📦 Estoque", "Saldos, valorização (CMP) e movimentos.")
    tabs = st.tabs(["Saldos", "Movimentos"]) 

    with tabs[0]:
        card_start()
        rows = qall("select * from resto.v_stock order by name;" )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        card_end()

    with tabs[1]:
        card_start()
        st.subheader("Movimentações recentes" )
        mv = qall("select move_date, kind, product_id, qty, unit_cost, total_cost, reason, reference_id, note from resto.inventory_movement order by move_date desc limit 500;" )
        st.dataframe(pd.DataFrame(mv), use_container_width=True, hide_index=True)
        card_end()

def page_financeiro():
    header("💰 Financeiro", "Livro caixa simples e DRE." )
    tabs = st.tabs(["Livro caixa", "DRE (simples)"])

    with tabs[0]:
        card_start()
        st.subheader("Lançar entrada/saída" )
        cats = qall("select id, name, kind from resto.cash_category order by name;" )
        if not cats:
            qexec("insert into resto.cash_category(name, kind) values ('Vendas', 'IN'), ('Compras', 'OUT'), ('Despesas Fixas', 'OUT'), ('Outros Recebimentos', 'IN'), ('Outros Pagamentos', 'OUT') on conflict do nothing;" )
            cats = qall("select id, name, kind from resto.cash_category order by name;" )

        with st.form("form_caixa"):
            dt = st.date_input("Data", value=date.today())
            cat = st.selectbox("Categoria", options=[(c['id'], f"{c['name']} ({c['kind']})") for c in cats], format_func=lambda x: x[1])
            kind = 'IN' if '(IN)' in cat[1] else 'OUT'
            desc = st.text_input("Descrição")
            val  = st.number_input("Valor", 0.00, 1_000_000.00, 0.00, 0.01)
            method = st.selectbox("Forma de pagamento", ['dinheiro','pix','cartão débito','cartão crédito','boleto','outro'])
            ok = st.form_submit_button("Lançar")
        if ok and val>0:
            qexec("insert into resto.cashbook(entry_date, kind, category_id, description, amount, method) values (%s,%s,%s,%s,%s,%s);", (dt, kind, cat[0], desc, val, method))
            st.success("Lançamento registrado!" )

        df = pd.DataFrame(qall("select entry_date, kind, description, amount, method from resto.cashbook order by entry_date desc, id desc limit 500;" ))
        st.dataframe(df, use_container_width=True, hide_index=True)
        card_end()

    with tabs[1]:
        card_start()
        st.subheader("DRE resumida (mês atual)")
        dre = qone("""
            with
            vendas as (select coalesce(sum(total),0) v from resto.sale where status='FECHADA' and date_trunc('month', date)=date_trunc('month', now())),
            cmv as (select coalesce(sum(case when kind='OUT' then total_cost else 0 end),0) c from resto.inventory_movement where move_date >= date_trunc('month', now()) and move_date < (date_trunc('month', now()) + interval '1 month')),
            caixa_desp as (select coalesce(sum(case when kind='OUT' then amount else 0 end),0) d from resto.cashbook where date_trunc('month', entry_date)=date_trunc('month', now())),
            caixa_outros as (select coalesce(sum(case when kind='IN' then amount else 0 end),0) o from resto.cashbook where date_trunc('month', entry_date)=date_trunc('month', now()))
            select v, c, d, o, (v + o - c - d) as resultado from vendas, cmv, caixa_desp, caixa_outros;
        """)
        if dre:
            st.markdown(
                f"Receita: R$ {dre['v']:,.2f}  \\n"
                f"CMV: R$ {dre['c']:,.2f}  \\n"
                f"Despesas: R$ {dre['d']:,.2f}  \\n"
                f"Outros: R$ {dre['o']:,.2f}  \\n"
                f"**Resultado:** R$ {dre['resultado']:,.2f}"
                .replace(',', 'X').replace('.', ',').replace('X','.'),
                unsafe_allow_html=False
            )
        else:
            st.caption("Sem dados para o mês.")
        card_end()

# ===================== Router =====================
def main():
    if not ensure_ping():
        st.stop()

    header("🍝 Restô ERP Lite", "Financeiro • Fiscal-ready • Estoque • Ficha técnica • Preços")    
    page = st.sidebar.radio("Menu", ["Painel", "Cadastros", "Compras", "Vendas", "Receitas & Preços", "Estoque", "Financeiro"], index=0)

    if page == "Painel": page_dashboard()
    elif page == "Cadastros": page_cadastros()
    elif page == "Compras": page_compras()
    elif page == "Vendas": page_vendas()
    elif page == "Receitas & Preços": page_receitas_precos()
    elif page == "Estoque": page_estoque()
    elif page == "Financeiro": page_financeiro()

if __name__ == "__main__":
    main()
