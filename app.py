
import os
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
""", unsafe_allow_html=True)

# ===================== DB Helpers =====================
def _get_conn():
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
        return cur.fetchall()

def qone(sql: str, params: Optional[Tuple]=None) -> Optional[Dict[str, Any]]:
    with _get_conn() as con, con.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()

def qexec(sql: str, params: Optional[Tuple]=None) -> int:
    with _get_conn() as con, con.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.rowcount or 0

# ===================== Ensure & Migrations =====================
def ensure_ping():
    try:
        qone("select 1;")
        return True
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return False

def ensure_migrations():
    # Produção (ordem de produção) e índices úteis
    qexec("""
    create table if not exists resto.production (
      id           bigserial primary key,
      date         timestamptz not null default now(),
      product_id   bigint not null references resto.product(id),
      qty          numeric(18,6) not null,
      unit_cost    numeric(18,6) not null default 0,
      total_cost   numeric(18,6) not null default 0,
      lot_number   text,
      expiry_date  date,
      note         text
    );
    """)
    qexec("""
    create table if not exists resto.production_item (
      id              bigserial primary key,
      production_id   bigint not null references resto.production(id) on delete cascade,
      ingredient_id   bigint not null references resto.product(id),
      lot_id          bigint references resto.purchase_item(id),
      qty             numeric(18,6) not null,
      unit_cost       numeric(18,6) not null default 0,
      total_cost      numeric(18,6) not null default 0
    );
    """)
    qexec("""create index if not exists invmov_ref_idx on resto.inventory_movement(reference_id);""")

# ===================== UI Helpers =====================

qexec("""
create unique index if not exists cashbook_uq_fingerprint_expr
  on resto.cashbook (
    entry_date,
    round(amount::numeric, 2),
    left(lower(coalesce(description, '')), 50),
    coalesce(method, '')
  );
""")


def header(title: str, subtitle: Optional[str] = None):
    st.markdown(
        f"<div class='modern-header'><h2 style='margin:0'>{title}</h2>" +
        (f"<div class='muted'>{subtitle}</div>" if subtitle else "") +
        "</div>", unsafe_allow_html=True
    )

def card_start(): st.markdown("<div class='modern-card'>", unsafe_allow_html=True)
def card_end():   st.markdown("</div>", unsafe_allow_html=True)

def money(v: float) -> str:
    return ("R$ {:,.2f}".format(v)).replace(",", "X").replace(".", ",").replace("X", ".")

# ===================== Domain Helpers =====================
def lot_balances_for_product(product_id: int) -> pd.DataFrame:
    """Retorna saldos por lote (purchase_item) para um produto.
       saldo = qty_lote - sum(OUT movements com reference_id=lot_id)"""
    rows = qall("""
      with cons as (
        select reference_id as lot_id, coalesce(sum(qty),0) as qty_out
          from resto.inventory_movement
         where kind='OUT' and reference_id is not null
         group by reference_id
      )
      select pi.id as lot_id, pi.product_id, p.name as product_name,
             pi.qty as lot_qty, pi.unit_id,
             pi.unit_price, pi.expiry_date, pi.lot_number,
             coalesce(c.qty_out,0) as consumed,
             greatest(pi.qty - coalesce(c.qty_out,0), 0) as saldo
        from resto.purchase_item pi
        join resto.product p on p.id = pi.product_id
        left join cons c on c.lot_id = pi.id
       where pi.product_id = %s
       order by pi.expiry_date nulls last, pi.id asc;
    """, (product_id,))
    return pd.DataFrame(rows)

def fifo_allocate(product_id: int, required_qty: float) -> List[Dict[str, Any]]:
    """Aloca quantidade necessária por lotes (FIFO por validade, depois id)."""
    df = lot_balances_for_product(product_id)
    alloc = []
    remaining = float(required_qty or 0.0)
    for _, r in df.iterrows():
        if remaining <= 0:
            break
        avail = float(r["saldo"] or 0.0)
        if avail <= 0:
            continue
        take = min(avail, remaining)
        alloc.append({
            "lot_id": int(r["lot_id"]),
            "qty": take,
            "unit_cost": float(r["unit_price"] or 0.0),
            "expiry_date": r.get("expiry_date"),
            "lot_number": r.get("lot_number"),
        })
        remaining -= take
    return alloc

# ===================== Pages =====================

def page_dashboard():
    header("📊 Painel", "Visão geral do financeiro, estoque e validade.")
    col1, col2, col3 = st.columns(3)

    stock = qone("select coalesce(sum(stock_qty * avg_cost),0) as val, coalesce(sum(stock_qty),0) as qty from resto.product;") or {}
    cmv = qall("select month, cmv_value from resto.v_cmv order by month desc limit 6;")
    soon = qall("""
        with cons as (
          select reference_id as lot_id, coalesce(sum(qty),0) as qty_out
            from resto.inventory_movement
           where kind='OUT' and reference_id is not null
           group by reference_id
        )
        select pi.id, p.name, pi.expiry_date,
               greatest(pi.qty - coalesce(c.qty_out,0),0) as saldo,
               (pi.expiry_date - current_date) as dias
          from resto.purchase_item pi
          join resto.product p on p.id = pi.product_id
          left join cons c on c.lot_id = pi.id
         where pi.expiry_date is not null
           and (pi.expiry_date - current_date) <= 30
           and greatest(pi.qty - coalesce(c.qty_out,0),0) > 0
         order by pi.expiry_date asc
         limit 10;
    """)

    with col1:
        card_start()
        st.markdown("**Valor do Estoque (CMP)**\n\n<h3 class='kpi'>{}</h3>".format(money(stock.get('val',0))), unsafe_allow_html=True)
        st.caption("Quantidade total em estoque: {:.2f}".format(stock.get('qty',0)))
        card_end()

    with col2:
        card_start()
        st.markdown("**Últimos CMVs (mensal)**")
        df = pd.DataFrame(cmv)
        if not df.empty:
            df["month"] = pd.to_datetime(df["month"]).dt.strftime("%Y-%m")
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.caption("Sem dados de CMV ainda.")
        card_end()

    with col3:
        card_start()
        st.markdown("**Vencimentos em 30 dias**")
        df = pd.DataFrame(soon)
        if not df.empty:
            df["dias"] = df["dias"].astype(int)
            st.dataframe(df[["name","expiry_date","saldo","dias"]], use_container_width=True, hide_index=True)
        else:
            st.caption("Nenhum lote a vencer nos próximos 30 dias.")
        card_end()

def page_cadastros():
    header("🗂️ Cadastros", "Unidades, Categorias, Produtos e Fornecedores.")
    tabs = st.tabs(["Unidades", "Categorias", "Fornecedores", "Produtos"])

    # ---------- Unidades ----------
    with tabs[0]:
        card_start()
        st.subheader("Unidades de Medida")
        with st.form("form_unit"):
            name = st.text_input("Nome", value="Unidade")
            abbr = st.text_input("Abreviação", value="un")
            base_hint = st.text_input("Observação/Conversão", value="ex: 1 un = 25 g (dica)")
            ok = st.form_submit_button("Salvar unidade")
        if ok and name and abbr:
            qexec("insert into resto.unit(name, abbr, base_hint) values (%s,%s,%s) on conflict (abbr) do update set name=excluded.name, base_hint=excluded.base_hint;", (name, abbr, base_hint))
            st.success("Unidade salva!")
        units = qall("select id, name, abbr, base_hint from resto.unit order by abbr;")
        st.dataframe(pd.DataFrame(units), use_container_width=True, hide_index=True)
        card_end()

    # ---------- Categorias ----------
    with tabs[1]:
        card_start()
        st.subheader("Categorias")
        with st.form("form_cat"):
            name = st.text_input("Nome da categoria")
            ok = st.form_submit_button("Salvar categoria")
        if ok and name:
            qexec("insert into resto.category(name) values (%s) on conflict(name) do nothing;", (name,))
            st.success("Categoria salva!")
        cats = qall("select id, name from resto.category order by name;")
        st.dataframe(pd.DataFrame(cats), use_container_width=True, hide_index=True)
        card_end()

    # ---------- Fornecedores ----------
    with tabs[2]:
        card_start()
        st.subheader("Fornecedores")
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
        rows = qall("select id, name, cnpj, ie, email, phone from resto.supplier order by name;")
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        card_end()

    # ---------- Produtos ----------
    with tabs[3]:
        card_start()
        st.subheader("Produtos (Catálogo Fiscal)")
        units = qall("select id, abbr from resto.unit order by abbr;")
        cats  = qall("select id, name from resto.category order by name;")

        with st.form("form_prod"):
            code = st.text_input("Código interno")
            name = st.text_input("Nome *")
            category_id = st.selectbox("Categoria", options=[(c['id'], c['name']) for c in cats], format_func=lambda x: x[1] if isinstance(x, tuple) else x, index=0 if cats else None)
            unit_id = st.selectbox("Unidade *", options=[(u['id'], u['abbr']) for u in units], format_func=lambda x: x[1] if isinstance(x, tuple) else x, index=0 if units else None)

            st.markdown("**Campos fiscais (opcional)**")
            colf1, colf2, colf3, colf4 = st.columns(4)
            with colf1:
                ncm = st.text_input("NCM") ; cest = st.text_input("CEST")
            with colf2:
                cfop = st.text_input("CFOP Venda") ; csosn = st.text_input("CSOSN")
            with colf3:
                cst_icms = st.text_input("CST ICMS") ; ali_icms = st.number_input("Alíquota ICMS %", 0.0, 100.0, 0.0, 0.01)
            with colf4:
                cst_pis = st.text_input("CST PIS") ; ali_pis = st.number_input("Alíquota PIS %", 0.0, 100.0, 0.0, 0.01)
            colf5, colf6 = st.columns(2)
            with colf5:
                cst_cof = st.text_input("CST COFINS") ; ali_cof = st.number_input("Alíquota COFINS %", 0.0, 100.0, 0.0, 0.01)
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
            st.success("Produto salvo!")

        prods = qall("select id, code, name, stock_qty, avg_cost, last_cost from resto.product order by name;")
        st.dataframe(pd.DataFrame(prods), use_container_width=True, hide_index=True)
        card_end()

def page_compras():
    header("📥 Compras", "Lançar notas e lotes com validade.")
    suppliers = qall("select id, name from resto.supplier order by name;")
    prods = qall("select id, name, unit_id from resto.product order by name;")
    units = qall("select id, abbr from resto.unit order by abbr;")

    card_start()
    with st.form("form_compra"):
        supplier = st.selectbox("Fornecedor *", options=[(s['id'], s['name']) for s in suppliers], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
        doc_number = st.text_input("Número do documento")
        cfop_ent = st.text_input("CFOP Entrada", value="1102")
        doc_date = st.date_input("Data", value=date.today())
        freight = st.number_input("Frete", 0.00, 9999999.99, 0.00, 0.01)
        other = st.number_input("Outros custos", 0.00, 9999999.99, 0.00, 0.01)

        st.markdown("**Itens da compra (cada item é um LOTE)**")
        if "compra_itens" not in st.session_state:
            st.session_state["compra_itens"] = []

        with st.expander("Adicionar item"):
            prod = st.selectbox("Produto", options=[(p['id'], p['name']) for p in prods], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
            unit = st.selectbox("Unidade", options=[(u['id'], u['abbr']) for u in units], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
            qty = st.number_input("Quantidade", 0.0, 1_000_000.0, 1.0, 0.1)
            unit_price = st.number_input("Preço unitário", 0.0, 1_000_000.0, 0.0, 0.01)
            discount = st.number_input("Desconto", 0.0, 1_000_000.0, 0.0, 0.01)
            lote = st.text_input("Lote (opcional)")
            expiry = st.date_input("Validade", value=None)
            add = st.button("Adicionar à lista")
            if add and prod and unit and qty>0:
                total = (qty * unit_price) - discount
                st.session_state["compra_itens"].append({
                    "product_id": prod[0], "product_name": prod[1],
                    "unit_id": unit[0], "unit_abbr": unit[1],
                    "qty": qty, "unit_price": unit_price, "discount": discount,
                    "total": total, "lot_number": lote or None, "expiry_date": str(expiry) if expiry else None
                })
                st.success("Item adicionado!")

        df = pd.DataFrame(st.session_state["compra_itens"]) if st.session_state["compra_itens"] else pd.DataFrame(columns=["product_name","qty","unit_abbr","unit_price","discount","total","expiry_date"])
        st.dataframe(df, use_container_width=True, hide_index=True)

        total_doc = float(df["total"].sum()) if not df.empty else 0.0
        st.markdown(f"**Total itens:** {money(total_doc)}")

        submit = st.form_submit_button("Lançar compra e atualizar estoque")
    card_end()

    if submit and supplier and doc_date and df is not None:
        pid = supplier[0]
        row = qone("""
            insert into resto.purchase(supplier_id, doc_number, cfop_entrada, doc_date, freight_value, other_costs, total, status)
            values (%s,%s,%s,%s,%s,%s,%s,'LANÇADA')
            returning id;
        """, (pid, doc_number, cfop_ent, doc_date, freight, other, total_doc))
        purchase_id = row["id"]
        for it in st.session_state["compra_itens"]:
            rowi = qone("""
                insert into resto.purchase_item(
                  purchase_id, product_id, qty, unit_id, unit_price, discount, total, lot_number, expiry_date
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                returning id;
            """, (purchase_id, it["product_id"], it["qty"], it["unit_id"], it["unit_price"], it["discount"], it["total"], it["lot_number"], it["expiry_date"]))
            lot_id = rowi["id"]
            # registra movimento IN por lote
            note = f"lote:{lot_id}" + (f";exp:{it['expiry_date']}" if it["expiry_date"] else "")
            qexec("select resto.sp_register_movement(%s,'IN',%s,%s,'purchase',%s,%s);", (it["product_id"], it["qty"], it["unit_price"], lot_id, note))

        st.session_state["compra_itens"] = []
        st.success(f"Compra #{purchase_id} lançada e estoque atualizado!")

def page_vendas():
    header("🧾 Vendas (simples)", "Registre saídas e gere CMV.")
    prods = qall("select id, name from resto.product where is_sale_item order by name;")

    card_start()
    # Um ÚNICO formulário com DOIS botões de submit:
    #  - "Adicionar item" (atualiza a lista)
    #  - "Fechar venda..." (grava a venda e baixa estoque)
    with st.form("form_sale", clear_on_submit=False):
        sale_date = st.date_input("Data", value=date.today())

        if "sale_itens" not in st.session_state:
            st.session_state["sale_itens"] = []

        with st.expander("Adicionar item", expanded=True):
            prod = st.selectbox("Produto", options=[(p['id'], p['name']) for p in prods],
                                format_func=lambda x: x[1] if isinstance(x, tuple) else x, key="sale_prod")
            qty = st.number_input("Quantidade", 0.0, 1_000_000.0, 1.0, 0.1, key="sale_qty")
            price = st.number_input("Preço unitário", 0.0, 1_000_000.0, 0.0, 0.01, key="sale_price")

            # >>> IMPORTANTE: dentro do form use st.form_submit_button (NÃO st.button)
            add = st.form_submit_button("➕ Adicionar item", type="secondary")
            if add and prod and qty > 0:
                st.session_state["sale_itens"].append({
                    "product_id": prod[0],
                    "product_name": prod[1],
                    "qty": float(qty),
                    "unit_price": float(price),
                    "total": float(qty) * float(price),
                })
                st.success("Item adicionado!")

        # Mostra carrinho atual (a partir do session_state)
        df_tmp = (pd.DataFrame(st.session_state["sale_itens"])
                  if st.session_state["sale_itens"] else
                  pd.DataFrame(columns=["product_name","qty","unit_price","total"]))
        st.dataframe(df_tmp, use_container_width=True, hide_index=True)
        total_tmp = float(df_tmp["total"].sum()) if not df_tmp.empty else 0.0
        st.markdown(f"**Total da venda (parcial):** {money(total_tmp)}")

        # Botão principal do mesmo form
        submit = st.form_submit_button("✅ Fechar venda e dar baixa no estoque")

    card_end()

    # Fora do form: ao clicar "Fechar venda", grava tudo
    if submit:
        df = pd.DataFrame(st.session_state["sale_itens"])
        if df.empty:
            st.warning("Nenhum item no carrinho.")
            return
        total = float(df["total"].sum())

        row = qone("insert into resto.sale(date, total, status) values (%s,%s,'FECHADA') returning id;",
                   (sale_date, total))
        sale_id = row["id"]

        for it in st.session_state["sale_itens"]:
            qexec("insert into resto.sale_item(sale_id, product_id, qty, unit_price, total) values (%s,%s,%s,%s,%s);",
                  (sale_id, it["product_id"], it["qty"], it["unit_price"], it["total"]))
            # Saída usa CMP atual (sp cuidará); sem amarrar lote neste MVP de venda
            qexec("select resto.sp_register_movement(%s,'OUT',%s,null,'sale',%s,%s);",
                  (it["product_id"], it["qty"], sale_id, ''))

        st.session_state["sale_itens"] = []  # limpa carrinho
        st.success(f"Venda #{sale_id} fechada e estoque baixado!")

def page_receitas_precos():
    header("🥣 Fichas Técnicas & Precificação", "Monte receitas e calcule preço sugerido.")
    prods = qall("select id, name from resto.product order by name;")
    units = qall("select id, abbr from resto.unit order by abbr;")

    tab = st.tabs(["Receitas", "Precificação"])  # 0: receita, 1: precificação

    # ---------- Receitas ----------
    with tab[0]:
        card_start()
        st.subheader("Ficha técnica do produto")
        prod = st.selectbox("Produto final *", options=[(p['id'], p['name']) for p in prods], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
        if prod:
            recipe = qone("select * from resto.recipe where product_id=%s;", (prod[0],))
            if not recipe:
                st.info("Sem receita ainda. Preencha para criar.")
                with st.form("form_new_recipe"):
                    yield_qty = st.number_input("Rendimento (quantidade)", 0.0, 1_000_000.0, 10.0, 0.1)
                    yield_unit = st.selectbox("Unidade do rendimento", options=[(u['id'], u['abbr']) for u in units], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
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
                    ing = st.selectbox("Ingrediente", options=[(p['id'], p['name']) for p in prods], format_func=lambda x: x[1] if isinstance(x, tuple) else x, key="ing_sel")
                    qty = st.number_input("Quantidade", 0.0, 1_000_000.0, 1.0, 0.1, key="ing_qty")
                    unit = st.selectbox("Unidade", options=[(u['id'], u['abbr']) for u in units], format_func=lambda x: x[1] if isinstance(x, tuple) else x, key="ing_unit")
                    conv = st.number_input("Fator de conversão (opcional)", 0.0, 1_000_000.0, 1.0, 0.01, help="Ex: 1 colher = 15 ml → use 15 se seu estoque estiver em ml.")
                    add = st.button("Adicionar ingrediente", key="ing_add")
                    if add and ing and qty>0:
                        qexec("insert into resto.recipe_item(recipe_id, ingredient_id, qty, unit_id, conversion_factor) values (%s,%s,%s,%s,%s);", (recipe['id'], ing[0], qty, unit[0], conv))
                        st.success("Ingrediente incluído!")

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
                    st.markdown(f"**Custo do lote:** {money(cost['batch_cost'])} • **Custo unitário estimado:** {money(cost['unit_cost_estimated'])}")
                else:
                    st.caption("Adicione ingredientes para ver o custo.")
        card_end()

    # ---------- Precificação ----------
    with tab[1]:
        card_start()
        st.subheader("Simulador de Preço de Venda")
        prod = st.selectbox("Produto", options=[(p['id'], p['name']) for p in prods], format_func=lambda x: x[1] if isinstance(x, tuple) else x, key="prec_prod")
        if prod:
            rec = qone("select unit_cost_estimated from resto.v_recipe_cost where product_id=%s;", (prod[0],))
            avg = qone("select avg_cost from resto.product where id=%s;", (prod[0],))
            base_cost = rec['unit_cost_estimated'] if rec and rec['unit_cost_estimated'] else (avg['avg_cost'] if avg else 0.0)
            st.markdown(f"**Custo base (estimado ou CMP):** {money(base_cost)}")

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

            st.markdown(f"**Preço sugerido:** {money(preco_sugerido)} • **Receita líquida estimada:** {money(preco_liquido)}")
        card_end()

def page_producao():
    header("🍳 Produção", "Ordem de produção: consome ingredientes (por lote) e gera produto final.")
    prods = qall("select id, name from resto.product order by name;")
    units = {r["id"]: r["abbr"] for r in qall("select id, abbr from resto.unit;")}

    card_start()
    st.subheader("Nova produção")
    prod = st.selectbox("Produto final *", options=[(p['id'], p['name']) for p in prods], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
    if not prod:
        card_end()
        return

    recipe = qone("select * from resto.recipe where product_id=%s;", (prod[0],))
    if not recipe:
        st.warning("Este produto não possui ficha técnica (receita). Cadastre em 'Receitas & Preços'.")
        card_end()
        return

    yield_qty = float(recipe["yield_qty"] or 0.0)
    if yield_qty <= 0:
        st.error("Ficha técnica inválida: rendimento deve ser > 0.")
        card_end()
        return

    with st.form("form_producao"):
        qty_out = st.number_input("Quantidade a produzir (na unidade do produto)", 0.0, 1_000_000.0, 10.0, 0.1)
        lot_final = st.text_input("Lote do produto final (opcional)")
        expiry_final = st.date_input("Validade do produto final", value=None)
        ok = st.form_submit_button("Produzir")

    if not ok or qty_out <= 0:
        card_end()
        return

    # Carrega ingredientes da receita
    ingredients = qall("""
        select ri.ingredient_id, p.name as ingrediente, ri.qty, ri.conversion_factor, ri.unit_id
          from resto.recipe_item ri
          join resto.product p on p.id = ri.ingredient_id
         where ri.recipe_id=%s
         order by p.name;
    """, (recipe["id"],))

    scale = qty_out / yield_qty
    # Aloca por lotes e calcula custo
    consumos = []  # list of dicts: ingredient_id, lot_id, qty, unit_cost, total
    total_ing_cost = 0.0
    falta = []

    for it in ingredients:
        need = float(it["qty"] or 0.0) * float(it["conversion_factor"] or 1.0) * scale
        allocs = fifo_allocate(it["ingredient_id"], need)
        allocated = sum(a["qty"] for a in allocs)
        if allocated + 1e-9 < need:
            falta.append((it["ingrediente"], need, allocated))
        for a in allocs:
            total = a["qty"] * float(a["unit_cost"] or 0.0)
            consumos.append({
                "ingredient_id": it["ingredient_id"],
                "ingrediente": it["ingrediente"],
                "lot_id": a["lot_id"],
                "qty": a["qty"],
                "unit_cost": a["unit_cost"],
                "total": total
            })
            total_ing_cost += total

    if falta:
        st.error("Estoque insuficiente para os ingredientes:\n" + "\n".join([f"- {n}: precisa {q:.3f}, alocado {a:.3f}" for n,q,a in falta]))
        card_end()
        return

    # Calcula custo do lote final
    overhead = float(recipe["overhead_pct"] or 0.0) / 100.0
    loss = float(recipe["loss_pct"] or 0.0) / 100.0
    batch_cost = total_ing_cost * (1 + overhead) * (1 + loss)
    unit_cost_est = batch_cost / qty_out if qty_out > 0 else 0.0

    # Persiste produção
    prow = qone("""
        insert into resto.production(date, product_id, qty, unit_cost, total_cost, lot_number, expiry_date, note)
        values (now(), %s, %s, %s, %s, %s, %s, %s)
        returning id;
    """, (prod[0], qty_out, unit_cost_est, batch_cost, (lot_final or None), (str(expiry_final) if expiry_final else None), ""))
    production_id = prow["id"]

    # Registra consumo de ingredientes (OUT) por lote
    for c in consumos:
        pi = qone("""
            insert into resto.production_item(production_id, ingredient_id, lot_id, qty, unit_cost, total_cost)
            values (%s,%s,%s,%s,%s,%s)
            returning id;
        """, (production_id, c["ingredient_id"], c["lot_id"], c["qty"], c["unit_cost"], c["total"]))
        note = f"production:{production_id};lot:{c['lot_id']}"
        qexec("select resto.sp_register_movement(%s,'OUT',%s,%s,'production',%s,%s);", (c["ingredient_id"], c["qty"], c["unit_cost"], pi["id"], note))

    # Registra entrada do produto final (IN)
    note_final = f"production:{production_id}" + (f";lot:{lot_final}" if lot_final else "")
    qexec("select resto.sp_register_movement(%s,'IN',%s,%s,'production',%s,%s);", (prod[0], qty_out, unit_cost_est, production_id, note_final))

    st.success(f"Produção #{production_id} registrada. CMP do produto final atualizado.")
    st.markdown(f"**Custo do lote:** {money(batch_cost)} • **Custo unitário aplicado no CMP:** {money(unit_cost_est)}")
    card_end()

def page_estoque():
    header("📦 Estoque", "Saldos, movimentos e lotes/validade.")
    tabs = st.tabs(["Saldos", "Movimentos", "Lotes & Validade"]) 

    with tabs[0]:
        card_start()
        rows = qall("select * from resto.v_stock order by name;")
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        card_end()

    with tabs[1]:
        card_start()
        st.subheader("Movimentações recentes")
        mv = qall("select move_date, kind, product_id, qty, unit_cost, total_cost, reason, reference_id, note from resto.inventory_movement order by move_date desc limit 500;")
        st.dataframe(pd.DataFrame(mv), use_container_width=True, hide_index=True)
        card_end()

    with tabs[2]:
        card_start()
        st.subheader("Alertas de validade e saldos por lote")
        dias = st.slider("Dias até o vencimento", 7, 120, 30, 1)
        rows = qall("""
            with cons as (
              select reference_id as lot_id, coalesce(sum(qty),0) as qty_out
                from resto.inventory_movement
               where kind='OUT' and reference_id is not null
               group by reference_id
            )
            select pi.id as lot_id, p.name, pi.qty as lote, coalesce(c.qty_out,0) as consumido,
                   greatest(pi.qty - coalesce(c.qty_out,0),0) as saldo,
                   pi.unit_price, pi.expiry_date, (pi.expiry_date - current_date) as dias_restantes
              from resto.purchase_item pi
              join resto.product p on p.id = pi.product_id
              left join cons c on c.lot_id = pi.id
             where pi.expiry_date is not null
               and (pi.expiry_date - current_date) <= %s
             order by pi.expiry_date asc;
        """, (dias,))
        df = pd.DataFrame(rows)
        if not df.empty:
            df["dias_restantes"] = df["dias_restantes"].astype(int)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.caption("Nenhum lote dentro do período selecionado.")
        card_end()

def page_financeiro():
    header("💰 Financeiro", "Livro caixa simples e DRE.")
    tabs = st.tabs(["Livro caixa", "DRE (simples)"])

    with tabs[0]:
        card_start()
        st.subheader("Lançar entrada/saída")
        cats = qall("select id, name, kind from resto.cash_category order by name;")
        if not cats:
            qexec("insert into resto.cash_category(name, kind) values ('Vendas', 'IN'), ('Compras', 'OUT'), ('Despesas Fixas', 'OUT'), ('Outros Recebimentos', 'IN'), ('Outros Pagamentos', 'OUT') on conflict do nothing;")
            cats = qall("select id, name, kind from resto.cash_category order by name;")

        with st.form("form_caixa"):
            dt = st.date_input("Data", value=date.today())
            cat = st.selectbox("Categoria", options=[(c['id'], f"{c['name']} ({c['kind']})") for c in cats], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
            kind = 'IN' if '(IN)' in cat[1] else 'OUT'
            desc = st.text_input("Descrição")
            val  = st.number_input("Valor", 0.00, 1_000_000.00, 0.00, 0.01)
            method = st.selectbox("Forma de pagamento", ['dinheiro','pix','cartão débito','cartão crédito','boleto','outro'])
            ok = st.form_submit_button("Lançar")
        if ok and val>0:
            qexec("insert into resto.cashbook(entry_date, kind, category_id, description, amount, method) values (%s,%s,%s,%s,%s,%s);", (dt, kind, cat[0], desc, val, method))
            st.success("Lançamento registrado!")

        df = pd.DataFrame(qall("select id, entry_date, kind, category_id, description, amount, method from resto.cashbook order by entry_date desc, id desc limit 500;"))
        st.dataframe(df, use_container_width=True, hide_index=True)
        # ----- Gerenciar lançamentos (editar / excluir) -----
        with st.expander("Gerenciar lançamentos (editar / excluir)"):
            if df is None or df.empty:
                st.caption("Nenhum lançamento para gerenciar.")
            else:
                rows_cb = df.to_dict('records')
                options = [(int(r['id']), f"#{r['id']} • {r['entry_date']} • {r.get('method') or ''} • {str(r.get('description') or '')[:40]} • {('+' if r['kind']=='IN' else '-')}{r['amount']}") for r in rows_cb]
                sel = st.selectbox("Selecione um lançamento", options=options, format_func=lambda x: x[1] if isinstance(x, tuple) else x)
                if sel:
                    sel_id = sel[0]
                    cur = next((r for r in rows_cb if int(r['id'])==sel_id), None)
                    if cur:
                        cats_all = qall("select id, name, kind from resto.cash_category order by name;")
                        colA, colB, colC = st.columns(3)
                        with colA:
                            new_date = st.date_input("Data", value=pd.to_datetime(cur['entry_date']).date())
                        with colB:
                            cat_opts = [(c['id'], f"{c['name']} ({c['kind']})") for c in cats_all]
                            def _fmt(x): return x[1] if isinstance(x, tuple) else x
                            try:
                                idx = [c[0] for c in cat_opts].index(cur.get('category_id'))
                            except Exception:
                                idx = 0
                            new_cat = st.selectbox("Categoria", options=cat_opts, index=idx, format_func=_fmt)
                        with colC:
                            methods = ['dinheiro','pix','cartão débito','cartão crédito','boleto','transferência','outro']
                            curm = cur.get('method') or 'outro'
                            new_method = st.selectbox("Forma de pagamento", methods, index=(methods.index(curm) if curm in methods else len(methods)-1))
                        new_desc = st.text_input("Descrição", value=cur.get('description') or '')
                        new_amount = st.number_input("Valor", -1_000_000.0, 1_000_000.0, float(cur.get('amount') or 0.0), 0.01)
                        new_kind = 'IN' if '(IN)' in new_cat[1] else 'OUT'
                        col1b, col2b = st.columns(2)
                        with col1b:
                            salvar = st.button("💾 Salvar alterações", type="primary", key=f"save_{sel_id}")
                        with col2b:
                            excluir = st.button("🗑️ Excluir lançamento", type="secondary", key=f"del_{sel_id}")
                        if salvar:
                            try:
                                qexec("""
                                    update resto.cashbook
                                       set entry_date=%s, kind=%s, category_id=%s, description=%s, amount=%s, method=%s
                                     where id=%s;
                                """, (new_date, new_kind, int(new_cat[0]), new_desc[:300], float(new_amount), new_method, sel_id))
                                st.success("Lançamento atualizado.")
                                st.experimental_rerun()
                            except Exception as e:
                                st.error(f"Erro ao atualizar: {e}")
                        if excluir:
                            try:
                                qexec("delete from resto.cashbook where id=%s;", (sel_id,))
                                st.success("Lançamento excluído.")
                                st.experimental_rerun()
                            except Exception as e:
                                st.error(f"Erro ao excluir: {e}")
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
        """ )
        if dre:
            st.markdown(
                f"Receita: {money(dre['v'])}  \n"
                f"CMV: {money(dre['c'])}  \n"
                f"Despesas: {money(dre['d'])}  \n"
                f"Outros: {money(dre['o'])}  \n"
                f"**Resultado:** {money(dre['resultado'])}",
                unsafe_allow_html=False
            )
        else:
            st.caption("Sem dados para o mês.")
        card_end()


# ===================== Importar Extrato (CSV C6 / genérico) =====================
def _read_text_guess(raw: bytes) -> str:
    for enc in ("utf-8-sig", "latin1"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")

def _parse_brl_amount(x) -> float:
    s = str(x or "").strip().replace("R$", "").replace(" ", "")
    if s == "" or s.lower() in {"nan", "none"}:
        return 0.0
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def _guess_sep(line: str) -> str:
    best_sep, best_cols = ",", 1
    for sep in [",", ";", "\t", "|"]:
        cols = len(line.split(sep))
        if cols > best_cols:
            best_sep, best_cols = sep, cols
    return best_sep

def _load_c6_csv(raw: bytes) -> pd.DataFrame:
    from io import StringIO
    txt = _read_text_guess(raw)
    lines = txt.splitlines()
    marker = "Data Lançamento,Data Contábil,Título,Descrição,Entrada(R$),Saída(R$),Saldo do Dia(R$)"
    start = next((i for i, ln in enumerate(lines) if marker in ln), None)
    if start is None:
        return pd.DataFrame()
    csv_body = "\n".join(lines[start:])
    df = pd.read_csv(StringIO(csv_body), sep=",", dtype=str, keep_default_na=False)
    out = pd.DataFrame({
        "entry_date": pd.to_datetime(df["Data Lançamento"], dayfirst=True, errors="coerce").dt.date,
        "description": df["Descrição"].astype(str),
    })
    out["amount"] = df["Entrada(R$)"].apply(_parse_brl_amount) - df["Saída(R$)"].apply(_parse_brl_amount)
    return out.dropna(subset=["entry_date"])

def _load_csv_generic(raw: bytes) -> pd.DataFrame:
    from io import StringIO
    txt = _read_text_guess(raw)
    if not txt:
        return pd.DataFrame()
    first = txt.splitlines()[0]
    sep = _guess_sep(first)
    df = pd.read_csv(StringIO(txt), sep=sep, dtype=str, keep_default_na=False)
    cols = {c.lower(): c for c in df.columns}
    entry_col = cols.get("data") or cols.get("date") or cols.get("data lançamento") or cols.get("entry_date")
    desc_col = cols.get("descrição") or cols.get("descricao") or cols.get("description") or cols.get("histórico") or cols.get("historico")
    # tenta amount direto
    val_col  = cols.get("valor") or cols.get("amount")
    # ou entrada-saida
    ent_col  = cols.get("entrada(r$)") or cols.get("credito") or cols.get("crédito")
    sai_col  = cols.get("saída(r$)") or cols.get("debito") or cols.get("débito")

    if entry_col and desc_col and val_col:
        out = pd.DataFrame({
            "entry_date": pd.to_datetime(df[entry_col], dayfirst=True, errors="coerce").dt.date,
            "description": df[desc_col].astype(str),
            "amount": df[val_col].apply(_parse_brl_amount)
        })
        return out.dropna(subset=["entry_date"])

    if entry_col and desc_col and ent_col and sai_col:
        out = pd.DataFrame({
            "entry_date": pd.to_datetime(df[entry_col], dayfirst=True, errors="coerce").dt.date,
            "description": df[desc_col].astype(str),
        })
        out["amount"] = df[ent_col].apply(_parse_brl_amount) - df[sai_col].apply(_parse_brl_amount)
        return out.dropna(subset=["entry_date"])

    return pd.DataFrame()

def _load_bank_file(up) -> pd.DataFrame:
    raw = up.read()
    txt = _read_text_guess(raw)
    if "Data Lançamento,Data Contábil,Título,Descrição,Entrada(R$),Saída(R$),Saldo do Dia(R$)" in txt:
        df = _load_c6_csv(raw)
        if not df.empty:
            return df
    return _load_csv_generic(raw)

def _find_duplicates(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=bool)
    rows = qall("""
        select entry_date, amount, description
          from resto.cashbook
         where entry_date >= (current_date - interval '365 days')
    """)
    base = pd.DataFrame(rows)
    if base.empty:
        return pd.Series([False]*len(df))

    base["key"] = (
        pd.to_datetime(base["entry_date"]).astype(str)
        + "|" + base["amount"].astype(float).round(2).astype(str)
        + "|" + base["description"].astype(str).str.lower().str.slice(0, 50)
    )
    df2 = df.copy()
    df2["key"] = (
        pd.to_datetime(df2["entry_date"]).astype(str)
        + "|" + df2["amount"].astype(float).round(2).astype(str)
        + "|" + df2["description"].astype(str).str.lower().str.slice(0, 50)
    )
    return df2["key"].isin(set(base["key"]))



def page_importar_extrato():
    header("🏦 Importar Extrato (CSV)", "Modelo C6 ou CSV genérico com data/descrição/valor.")
    card_start()
    st.session_state.setdefault("import_lock", False)
    if st.session_state["import_lock"]:
        st.warning("Importação em andamento... aguarde finalizar.")
        card_end()
        return

    up = st.file_uploader("CSV do banco", type=["csv"], key="bank_file")
    if not up:
        st.info("Dica: o CSV do C6 com cabeçalho 'Data Lançamento,Data Contábil, ...' é detectado automaticamente.")
        card_end()
        return

    df = _load_bank_file(up)
    if df.empty or not set(df.columns) >= {"entry_date","description","amount"}:
        st.error("Não consegui reconhecer o layout (preciso de: data, descrição, valor).")
        card_end()
        return

    st.success(f"Arquivo reconhecido. {len(df)} linhas.")
    st.dataframe(df.head(100), use_container_width=True, hide_index=True)

    st.subheader("2) Ajustes e classificação")
    cats = qall("select id, name, kind from resto.cash_category order by name;")
    if not cats:
        st.warning("Nenhuma categoria encontrada. Crie categorias em **Financeiro → Livro caixa** antes de importar.")
        card_end()
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        default_cat_in = st.selectbox("Categoria padrão para ENTRADAS", options=[(c['id'], f"{c['name']} ({c['kind']})") for c in cats if c['kind']=='IN'],
                                      format_func=lambda x: x[1] if isinstance(x, tuple) else x)
    with col2:
        default_cat_out = st.selectbox("Categoria padrão para SAÍDAS", options=[(c['id'], f"{c['name']} ({c['kind']})") for c in cats if c['kind']=='OUT'],
                                       format_func=lambda x: x[1] if isinstance(x, tuple) else x)
    with col3:
        method_override = st.selectbox(
            "Método (opcional — deixe em auto p/ detectar por descrição)",
            options=["— auto por descrição —","dinheiro","pix","cartão débito","cartão crédito","boleto","transferência","outro"],
            index=0
        )

    # Define coluna kind e category_id por sinal do valor
    out = df.copy()
    out["kind"] = out["amount"].apply(lambda v: "IN" if float(v) >= 0 else "OUT")
    out["category_id"] = out["kind"].apply(lambda k: default_cat_in[0] if k=="IN" else default_cat_out[0])
    # método por linha (auto por descrição)
    try:
        out["method"] = out["description"].apply(_guess_method_from_desc)
    except NameError:
        # fallback seguro: marca como "outro"
        out["method"] = "outro"
    # override global opcional
    if method_override != "— auto por descrição —":
        map_override = {"dinheiro":"dinheiro","pix":"pix","cartão débito":"cartão débito","cartão crédito":"cartão crédito","boleto":"boleto","transferência":"transferência","outro":"outro"}
        out["method"] = map_override.get(method_override, "outro")

    # Duplicados (últimos 12 meses)
    out["duplicado?"] = _find_duplicates(out)

    st.subheader("3) Conferência")
    st.dataframe(out.head(100), use_container_width=True, hide_index=True)
    st.info(f"Detectados **{int(out['duplicado?'].sum())}** possíveis duplicados (mesma data, valor e início da descrição). Eles não serão importados.")

    st.subheader("4) Importar")
    prontos = out[~out["duplicado?"]].copy()
    st.markdown(f"Prontos para importar: **{len(prontos)}**")

    with st.form("form_import"):
        confirma = st.checkbox("Confirmo que revisei os dados e desejo importar os lançamentos.")
        go = st.form_submit_button(f"🚀 Importar {len(prontos)} lançamentos")

    if go and confirma and not st.session_state["import_lock"]:
        st.session_state["import_lock"] = True
        ok = 0
        for _, r in prontos.iterrows():
            qexec("""
                insert into resto.cashbook(entry_date, kind, category_id, description, amount, method)
                values (%s, %s, %s, %s, %s, %s)
                on conflict do nothing;
            """, (str(r["entry_date"]), r["kind"], int(r["category_id"]), str(r["description"])[:300], float(r["amount"]), r.get("method") or "outro"))
            ok += 1
        st.session_state["import_lock"] = False
        st.session_state["bank_file"] = None
        st.success(f"Importados {ok} lançamentos no livro-caixa!")
    card_end()


    df = _load_bank_file(up)
    if df.empty or not set(df.columns) >= {"entry_date","description","amount"}:
        st.error("Não consegui reconhecer o layout (preciso de: data, descrição, valor).")
        card_end()
        return

    st.success(f"Arquivo reconhecido. {len(df)} linhas.")
    st.dataframe(df.head(100), use_container_width=True, hide_index=True)

    st.subheader("2) Ajustes e classificação")
    cats = qall("select id, name, kind from resto.cash_category order by name;")
    if not cats:
        st.warning("Nenhuma categoria encontrada. Crie categorias em **Financeiro → Livro caixa** antes de importar.")
        card_end()
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        default_cat_in = st.selectbox("Categoria padrão para ENTRADAS", options=[(c['id'], f"{c['name']} ({c['kind']})") for c in cats if c['kind']=='IN'],
                                      format_func=lambda x: x[1] if isinstance(x, tuple) else x)
    with col2:
        default_cat_out = st.selectbox("Categoria padrão para SAÍDAS", options=[(c['id'], f"{c['name']} ({c['kind']})") for c in cats if c['kind']=='OUT'],
                                       format_func=lambda x: x[1] if isinstance(x, tuple) else x)
    with col3:
        method = st.selectbox("Método (aplicar em todos)", ['dinheiro','pix','cartão débito','cartão crédito','boleto','outro'])

    # Define coluna kind e category_id por sinal do valor
    out = df.copy()
    out["kind"] = out["amount"].apply(lambda v: "IN" if float(v) >= 0 else "OUT")
    out["category_id"] = out["kind"].apply(lambda k: default_cat_in[0] if k=="IN" else default_cat_out[0])

    # Duplicados (últimos 12 meses)
    out["duplicado?"] = _find_duplicates(out)

    st.subheader("3) Conferência")
    st.dataframe(out.head(100), use_container_width=True, hide_index=True)
    st.info(f"Detectados **{int(out['duplicado?'].sum())}** possíveis duplicados (mesma data, valor e início da descrição). Eles não serão importados.")

    st.subheader("4) Importar")
    prontos = out[~out["duplicado?"]].copy()
    st.markdown(f"Prontos para importar: **{len(prontos)}**")

    with st.form("form_import"):
        confirma = st.checkbox("Confirmo que revisei os dados e desejo importar os lançamentos.")
        go = st.form_submit_button(f"🚀 Importar {len(prontos)} lançamentos")

    if go and confirma:
        ok = 0
        for _, r in prontos.iterrows():
            qexec("""
                insert into resto.cashbook(entry_date, kind, category_id, description, amount, method)
                values (%s, %s, %s, %s, %s, %s);
            """, (str(r["entry_date"]), r["kind"], int(r["category_id"]), str(r["description"])[:300], float(r["amount"]), method))
            ok += 1
        st.success(f"Importados {ok} lançamentos no livro-caixa!")
    card_end()


# ===================== Router =====================
def main():
    if not ensure_ping():
        st.stop()
    ensure_migrations()

    header("🍝 Restô ERP Lite", "Financeiro • Fiscal-ready • Estoque • Ficha técnica • Preços • Produção")
    page = st.sidebar.radio("Menu", ["Painel", "Cadastros", "Compras", "Vendas", "Receitas & Preços", "Produção", "Estoque", "Financeiro", "Importar Extrato"], index=0)

    if page == "Painel": page_dashboard()
    elif page == "Cadastros": page_cadastros()
    elif page == "Compras": page_compras()
    elif page == "Vendas": page_vendas()
    elif page == "Receitas & Preços": page_receitas_precos()
    elif page == "Produção": page_producao()
    elif page == "Estoque": page_estoque()
    elif page == "Financeiro": page_financeiro()
    elif page == "Importar Extrato": page_importar_extrato()

if __name__ == "__main__":
    main()


def _guess_method_from_desc(desc: str) -> str:
    """Heurística para detectar forma de pagamento a partir da descrição do extrato."""
    d = (desc or "").upper()
    # PIX / QR / chave
    if "PIX" in d or "QRCODE" in d or "QR CODE" in d or "CHAVE" in d:
        return "pix"
    # Transferências
    if "TED" in d or "TEF" in d or "DOC" in d or "TRANSFER" in d or "TRANSFERÊNCIA" in d:
        return "transferência"
    # Adquirentes / cartões
    if any(k in d for k in ["PAYGO", "PAGSEGURO", "STONE", "CIELO", "REDE", "GETNET", "MERCADO PAGO", "VISA", "MASTERCARD", "ELO"]):
        return "cartão crédito"
    # Marketplaces / apps
    if any(k in d for k in ["IFOOD", "RAPPI", "UBER", "99FOOD"]):
        return "pix"
    # Boletos
    if "BOLETO" in d:
        return "boleto"
    # Saque / ATM
    if "SAQUE" in d or "ATM" in d:
        return "dinheiro"
    return "outro"
