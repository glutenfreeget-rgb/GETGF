

import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# ===== DB DRIVER ADAPTER (psycopg3 OR psycopg2) =====
try:
    import psycopg
    from psycopg.rows import dict_row
    DRIVER = "pg3"
except Exception:
    import psycopg2
    import psycopg2.extras
    DRIVER = "pg2"

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
def _env(name: str, default: str = "") -> str:
    val = st.secrets.get(name, os.getenv(name, default))
    if isinstance(val, str):
        return val.strip()
    return val or ""

def _get_conn():
    host = _env("DB_HOST")
    port = _env("DB_PORT", "5432")
    user = _env("DB_USER")
    pwd  = _env("DB_PASSWORD")
    db   = _env("DB_NAME")
    ssl  = _env("DB_SSLMODE", "require")

    st.sidebar.caption(f"DB host: {host} • user: {user} • db: {db} • ssl: {ssl}")
    if not all([host, port, user, pwd, db]):
        st.error("Faltam variáveis de conexão (DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME).")
        st.stop()

    if DRIVER == "pg3":
        return psycopg.connect(
            host=host, port=port, user=user, password=pwd, dbname=db,
            sslmode=ssl, autocommit=True, row_factory=dict_row
        )
    else:
        conn = psycopg2.connect(host=host, port=port, user=user, password=pwd, dbname=db, sslmode=ssl)
        conn.autocommit = True
        return conn

def qall(sql: str, params: Optional[Tuple]=None) -> List[Dict[str, Any]]:
    with _get_conn() as con:
        if DRIVER == "pg3":
            with con.cursor() as cur:
                if params is None:
                    cur.execute(sql)
                else:
                    cur.execute(sql, params)
                return cur.fetchall()
        else:
            with con.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if params is None:
                    cur.execute(sql)
                else:
                    cur.execute(sql, params)
                return cur.fetchall()

def qone(sql: str, params: Optional[Tuple]=None) -> Optional[Dict[str, Any]]:
    rows = qall(sql, params)
    return rows[0] if rows else None

def qexec(sql: str, params: Optional[Tuple]=None) -> int:
    with _get_conn() as con:
        if DRIVER == "pg3":
            with con.cursor() as cur:
                if params is None:
                    cur.execute(sql)
                else:
                    cur.execute(sql, params)
                return cur.rowcount or 0
        else:
            with con.cursor() as cur:
                if params is None:
                    cur.execute(sql)
                else:
                    cur.execute(sql, params)
                try:
                    return cur.rowcount or 0
                except Exception:
                    return 0

def safe_qall(sql: str, params: Optional[Tuple]=None) -> List[Dict[str, Any]]:
    try:
        return qall(sql, params)
    except Exception as e:
        st.caption(f"⚠️ {e}")
        return []

def safe_qone(sql: str, params: Optional[Tuple]=None) -> Optional[Dict[str, Any]]:
    try:
        return qone(sql, params)
    except Exception:
        return None

def ensure_ping():
    try:
        qone("select 1;")
        return True
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return False

# ===================== MIGRAÇÕES =====================

def ensure_migrations_core():
    # --- Schema base ---
    qexec("create schema if not exists resto;")

    # --- Tabelas básicas ---
    qexec("""
    create table if not exists resto.unit (
      id bigserial primary key,
      name text not null,
      abbr text not null unique,
      base_hint text
    );
    """)

    qexec("""
    create table if not exists resto.category (
      id bigserial primary key,
      name text not null unique
    );
    """)

    qexec("""
    create table if not exists resto.supplier (
      id bigserial primary key,
      name text not null unique,
      cnpj text,
      ie   text,
      email text,
      phone text
    );
    """)

    qexec("""
    create table if not exists resto.product (
      id bigserial primary key,
      code text,
      name text not null unique,
      category_id bigint references resto.category(id),
      unit_id bigint references resto.unit(id),
      stock_qty numeric(18,6) not null default 0,
      avg_cost  numeric(18,6) not null default 0,
      last_cost numeric(18,6) not null default 0,
      ncm text, cest text, cfop_venda text, csosn text,
      cst_icms text, aliquota_icms numeric(8,4),
      cst_pis text, aliquota_pis numeric(8,4),
      cst_cofins text, aliquota_cofins numeric(8,4),
      iss_aliquota numeric(8,4),
      is_sale_item boolean not null default false,
      is_ingredient boolean not null default false,
      default_markup numeric(10,2) not null default 0
    );
    """)

    qexec("""
    create table if not exists resto.purchase (
      id bigserial primary key,
      supplier_id bigint references resto.supplier(id),
      doc_number text,
      cfop_entrada text,
      doc_date date not null,
      freight_value numeric(18,6) not null default 0,
      other_costs  numeric(18,6) not null default 0,
      total        numeric(18,6) not null default 0,
      status text not null default 'LANÇADA'
    );
    """)

    qexec("""
    create table if not exists resto.purchase_item (
      id bigserial primary key,
      purchase_id bigint not null references resto.purchase(id) on delete cascade,
      product_id  bigint not null references resto.product(id),
      qty numeric(18,6) not null,
      unit_id bigint references resto.unit(id),
      unit_price numeric(18,6) not null default 0,
      discount   numeric(18,6) not null default 0,
      total      numeric(18,6) not null default 0,
      lot_number text,
      expiry_date date
    );
    """)

    qexec("""
    create table if not exists resto.inventory_movement (
      id bigserial primary key,
      move_date timestamptz not null default now(),
      kind text not null check (kind in ('IN','OUT')),
      product_id bigint not null references resto.product(id),
      qty numeric(18,6) not null,
      unit_cost numeric(18,6),
      total_cost numeric(18,6),
      reason text,
      reference_id bigint,
      note text
    );
    """)

    qexec("""
    create table if not exists resto.sale (
      id bigserial primary key,
      date date not null,
      total numeric(18,6) not null default 0,
      status text not null
    );
    """)

    qexec("""
    create table if not exists resto.sale_item (
      id bigserial primary key,
      sale_id bigint not null references resto.sale(id) on delete cascade,
      product_id bigint not null references resto.product(id),
      qty numeric(18,6) not null,
      unit_price numeric(18,6) not null default 0,
      total numeric(18,6) not null default 0
    );
    """)

    qexec("""
    create table if not exists resto.recipe (
      id bigserial primary key,
      product_id bigint not null unique references resto.product(id),
      yield_qty numeric(18,6) not null,
      yield_unit_id bigint references resto.unit(id),
      overhead_pct numeric(8,4) not null default 0,
      loss_pct     numeric(8,4) not null default 0
    );
    """)

    qexec("""
    create table if not exists resto.recipe_item (
      id bigserial primary key,
      recipe_id bigint not null references resto.recipe(id) on delete cascade,
      ingredient_id bigint not null references resto.product(id),
      qty numeric(18,6) not null,
      unit_id bigint references resto.unit(id),
      conversion_factor numeric(18,6) default 1
    );
    """)

    qexec("""
    create table if not exists resto.cash_category (
      id bigserial primary key,
      name text not null unique,
      kind text not null check (kind in ('IN','OUT'))
    );
    """)

    qexec("""
    create table if not exists resto.cashbook (
      id bigserial primary key,
      entry_date date not null,
      kind text not null check (kind in ('IN','OUT')),
      category_id bigint references resto.cash_category(id),
      description text,
      amount numeric(18,6) not null,
      method text
    );
    """)

    # --- Views úteis ---
    qexec("""
    create or replace view resto.v_stock as
    select p.id as product_id, p.name,
           coalesce(p.stock_qty,0) as stock_qty,
           coalesce(p.avg_cost,0)  as avg_cost,
           coalesce(p.stock_qty,0)*coalesce(p.avg_cost,0) as stock_value
      from resto.product p;
    """)

    qexec("""
    create or replace view resto.v_cmv as
    select date_trunc('month', move_date)::date as month,
           coalesce(sum(case when kind='OUT' then total_cost end),0) as cmv_value
      from resto.inventory_movement
     group by 1
     order by 1 desc;
    """)

    qexec("""
    create or replace view resto.v_recipe_cost as
    select r.product_id,
           (sum(coalesce(ri.qty,0)*coalesce(ri.conversion_factor,1)*coalesce(p.avg_cost,0))
             * (1 + coalesce(r.overhead_pct,0)/100.0)
             * (1 + coalesce(r.loss_pct,0)/100.0)
           ) as batch_cost,
           case when coalesce(r.yield_qty,0) > 0
                then (
                  sum(coalesce(ri.qty,0)*coalesce(ri.conversion_factor,1)*coalesce(p.avg_cost,0))
                  * (1 + coalesce(r.overhead_pct,0)/100.0)
                  * (1 + coalesce(r.loss_pct,0)/100.0)
                ) / r.yield_qty
                else null end as unit_cost_estimated
      from resto.recipe r
      left join resto.recipe_item ri on ri.recipe_id = r.id
      left join resto.product p on p.id = ri.ingredient_id
     group by r.product_id, r.yield_qty, r.overhead_pct, r.loss_pct;
    """)

    # --- Índices úteis ---
    qexec("create index if not exists invmov_ref_idx on resto.inventory_movement(reference_id);")

    # --- Função de movimentos (cria/atualiza sempre) ---
    qexec(r"""
    create or replace function resto.sp_register_movement(
      p_product_id bigint,
      p_kind text,
      p_qty numeric,
      p_unit_cost numeric,
      p_reason text,
      p_reference_id bigint,
      p_note text
    ) returns void
    language plpgsql
    as $$
    declare
      v_prev_qty numeric := 0;
      v_prev_avg numeric := 0;
      v_unit_cost numeric := 0;
    begin
      select coalesce(stock_qty,0), coalesce(avg_cost,0)
        into v_prev_qty, v_prev_avg
        from resto.product where id = p_product_id
        for update;

      if p_unit_cost is null then
        v_unit_cost := v_prev_avg;
      else
        v_unit_cost := p_unit_cost;
      end if;

      if p_kind = 'IN' then
        insert into resto.inventory_movement(move_date, kind, product_id, qty, unit_cost, total_cost, reason, reference_id, note)
        values (now(), 'IN', p_product_id, p_qty, v_unit_cost, p_qty * v_unit_cost, p_reason, p_reference_id, p_note);

        update resto.product
           set stock_qty = coalesce(stock_qty,0) + p_qty,
               last_cost = v_unit_cost,
               avg_cost = case when coalesce(stock_qty,0)+p_qty > 0
                               then ((coalesce(stock_qty,0)*coalesce(avg_cost,0)) + (p_qty*v_unit_cost)) / (coalesce(stock_qty,0)+p_qty)
                               else coalesce(avg_cost,0) end
         where id = p_product_id;

      elsif p_kind = 'OUT' then
        insert into resto.inventory_movement(move_date, kind, product_id, qty, unit_cost, total_cost, reason, reference_id, note)
        values (now(), 'OUT', p_product_id, p_qty, v_unit_cost, p_qty * v_unit_cost, p_reason, p_reference_id, p_note);

        update resto.product
           set stock_qty = coalesce(stock_qty,0) - p_qty
         where id = p_product_id;
      else
        raise exception 'Invalid kind %', p_kind;
      end if;
    end $$;
    """)



def ensure_migrations_production():
    # Produção (ordem de produção) e índice útil
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

def ensure_migrations_cash_import():
    # regras p/ classificação do extrato
    qexec("""
    create table if not exists resto.cash_rule (
      id              bigserial primary key,
      pattern         text not null,
      category_id     bigint references resto.cash_category(id),
      kind            text not null check (kind in ('IN','OUT')),
      method          text,
      priority        integer not null default 0,
      created_at      timestamptz not null default now()
    );
    """)
    qexec("""
    insert into resto.cash_category(name, kind) values
      ('Vendas', 'IN'),
      ('Outros Recebimentos', 'IN'),
      ('Compras', 'OUT'),
      ('Taxas de Cartão', 'OUT'),
      ('Taxas/Serviços', 'OUT'),
      ('Despesas Fixas', 'OUT'),
      ('Outros Pagamentos', 'OUT')
    on conflict (name) do nothing;
    """)
    qexec("""
    insert into resto.cash_rule(pattern, category_id, kind, method, priority)
    values
      ('IFOOD',        (select id from resto.cash_category where name='Vendas'), 'IN',  'pix',        50),
      ('IFD',          (select id from resto.cash_category where name='Vendas'), 'IN',  'pix',        45),
      ('MERCADO PAGO', (select id from resto.cash_category where name='Vendas'), 'IN',  'pix',        45),
      ('PAGSEGURO',    (select id from resto.cash_category where name='Taxas de Cartão'), 'OUT', 'cartão', 60),
      ('STONE',        (select id from resto.cash_category where name='Taxas de Cartão'), 'OUT', 'cartão', 60),
      ('TAXA',         (select id from resto.cash_category where name='Taxas/Serviços'), 'OUT', 'outro', 40),
      ('TARIFA',       (select id from resto.cash_category where name='Taxas/Serviços'), 'OUT', 'outro', 40),
      ('PIX',          (select id from resto.cash_category where name='Vendas'), 'IN',  'pix',        10)
    on conflict do nothing;
    """)

def ensure_migrations():
    ensure_migrations_core()
    ensure_migrations_production()
    ensure_migrations_cash_import()

# ===================== UI Helpers =====================

# Safe selectbox helper: never breaks when options list is empty
def S(label, options, **kwargs):
    if not options:
        # show a disabled placeholder so Streamlit doesn't raise "options cannot be empty"
        kwargs = dict(kwargs)
        kwargs["disabled"] = True
        fmt = kwargs.get("format_func")
        # Default format for tuple options -> show second element
        if fmt is None:
            kwargs["format_func"] = lambda x: (x[1] if isinstance(x, tuple) and len(x) > 1 else (x if x not in (None, "") else "—"))
        return S(label, options=[("", "—")], **kwargs)
    return S(label, options=options, **kwargs)

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
    rows = safe_qall("""
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
    df = lot_balances_for_product(product_id)
    alloc = []
    remaining = float(required_qty or 0.0)
    for _, r in df.iterrows():
        if remaining <= 0:
            break
        avail = float(r.get("saldo") or 0.0)
        if avail <= 0:
            continue
        take = min(avail, remaining)
        alloc.append({
            "lot_id": int(r["lot_id"]),
            "qty": take,
            "unit_cost": float(r.get("unit_price") or 0.0),
            "expiry_date": r.get("expiry_date"),
            "lot_number": r.get("lot_number"),
        })
        remaining -= take
    return alloc

# ===================== PAGES =====================

def page_dashboard():
    header("📊 Painel", "Visão geral do financeiro, estoque e validade.")
    col1, col2, col3 = st.columns(3)

    stock = safe_qone("select coalesce(sum(stock_qty * avg_cost),0) as val, coalesce(sum(stock_qty),0) as qty from resto.product;") or {}
    cmv = safe_qall("select month, cmv_value from resto.v_cmv order by month desc limit 6;")
    soon = safe_qall("""
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
            st.caption("Sem dados de CMV ainda (crie a view v_cmv ou registre vendas).")
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
    header("🗂️ Cadastros", "Unidades, Categorias, Fornecedores e Produtos.")
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
        units = safe_qall("select id, name, abbr, base_hint from resto.unit order by abbr;")
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
        cats = safe_qall("select id, name from resto.category order by name;")
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
        rows = safe_qall("select id, name, cnpj, ie, email, phone from resto.supplier order by name;")
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        card_end()

    # ---------- Produtos ----------
    with tabs[3]:
        card_start()
        st.subheader("Produtos (Catálogo Fiscal)")
        units = safe_qall("select id, abbr from resto.unit order by abbr;")
        cats  = safe_qall("select id, name from resto.category order by name;")

        with st.form("form_prod"):
            code = st.text_input("Código interno")
            name = st.text_input("Nome *")
            category_id = S("Categoria", options=[(c['id'], c['name']) for c in cats] if cats else [], format_func=lambda x: x[1] if isinstance(x, tuple) else x, index=0 if cats else None)
            unit_id = S("Unidade *", options=[(u['id'], u['abbr']) for u in units] if units else [], format_func=lambda x: x[1] if isinstance(x, tuple) else x, index=0 if units else None)

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

        prods = safe_qall("select id, code, name, stock_qty, avg_cost, last_cost from resto.product order by name;")
        st.dataframe(pd.DataFrame(prods), use_container_width=True, hide_index=True)
        card_end()

def page_compras():
    header("📥 Compras", "Lançar notas e lotes com validade.")
    suppliers = safe_qall("select id, name from resto.supplier order by name;")
    prods = safe_qall("select id, name, unit_id from resto.product order by name;")
    units = safe_qall("select id, abbr from resto.unit order by abbr;")

    card_start()
    with st.form("form_compra"):
        supplier = S("Fornecedor *", options=[(s['id'], s['name']) for s in suppliers] if suppliers else [], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
        doc_number = st.text_input("Número do documento")
        cfop_ent = st.text_input("CFOP Entrada", value="1102")
        doc_date = st.date_input("Data", value=date.today())
        freight = st.number_input("Frete", 0.00, 9999999.99, 0.00, 0.01)
        other = st.number_input("Outros custos", 0.00, 9999999.99, 0.00, 0.01)

        st.markdown("**Itens da compra (cada item é um LOTE)**")
        if "compra_itens" not in st.session_state:
            st.session_state["compra_itens"] = []

        with st.expander("Adicionar item", expanded=True):
            prod = S("Produto", options=[(p['id'], p['name']) for p in prods] if prods else [], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
            unit = S("Unidade", options=[(u['id'], u['abbr']) for u in units] if units else [], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
            qty = st.number_input("Quantidade", 0.0, 1_000_000.0, 1.0, 0.1)
            unit_price = st.number_input("Preço unitário", 0.0, 1_000_000.0, 0.0, 0.01)
            discount = st.number_input("Desconto", 0.0, 1_000_000.0, 0.0, 0.01)
            lote = st.text_input("Lote (opcional)")
            has_expiry = st.checkbox("Tem validade?", value=False)
            expiry = st.date_input("Validade", value=date.today()) if has_expiry else None

            add = st.form_submit_button("Adicionar à lista", type="secondary")
            if add and prod and unit and qty>0:
                total = (qty * unit_price) - discount
                st.session_state["compra_itens"].append({
                    "product_id": prod[0], "product_name": prod[1],
                    "unit_id": unit[0], "unit_abbr": unit[1],
                    "qty": float(qty), "unit_price": float(unit_price), "discount": float(discount),
                    "total": float(total), "lot_number": (lote or None), "expiry_date": str(expiry) if expiry else None
                })
                st.success("Item adicionado!")

        df = pd.DataFrame(st.session_state["compra_itens"]) if st.session_state["compra_itens"] else pd.DataFrame(columns=["product_name","qty","unit_abbr","unit_price","discount","total","expiry_date"])
        st.dataframe(df, use_container_width=True, hide_index=True)

        total_doc = float(df["total"].sum()) if not df.empty else 0.0
        st.markdown(f"**Total itens:** {money(total_doc)}")

        submit = st.form_submit_button("Lançar compra e atualizar estoque")
    card_end()

    if submit and supplier and doc_date is not None and df is not None:
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
    prods = safe_qall("select id, name from resto.product where is_sale_item order by name;")

    card_start()
    # Um ÚNICO formulário com dois submits
    with st.form("form_sale", clear_on_submit=False):
        sale_date = st.date_input("Data", value=date.today())

        if "sale_itens" not in st.session_state:
            st.session_state["sale_itens"] = []

        with st.expander("Adicionar item", expanded=True):
            prod = S("Produto", options=[(p['id'], p['name']) for p in prods] if prods else [],
                                format_func=lambda x: x[1] if isinstance(x, tuple) else x, key="sale_prod")
            qty = st.number_input("Quantidade", 0.0, 1_000_000.0, 1.0, 0.1, key="sale_qty")
            price = st.number_input("Preço unitário", 0.0, 1_000_000.0, 0.0, 0.01, key="sale_price")

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

        df_tmp = (pd.DataFrame(st.session_state["sale_itens"])
                  if st.session_state["sale_itens"] else
                  pd.DataFrame(columns=["product_name","qty","unit_price","total"]))
        st.dataframe(df_tmp, use_container_width=True, hide_index=True)
        total_tmp = float(df_tmp["total"].sum()) if not df_tmp.empty else 0.0
        st.markdown(f"**Total da venda (parcial):** {money(total_tmp)}")

        submit = st.form_submit_button("✅ Fechar venda e dar baixa no estoque")

    card_end()

    if submit:
        df = pd.DataFrame(st.session_state["sale_itens"])
        if df.empty:
            st.warning("Nenhum item no carrinho.")
            return
        total = float(df["total"].sum())

        row = qone("insert into resto.sale(date, total, status) values (%s,%s,'FECHADA') returning id;", (sale_date, total))
        sale_id = row["id"]

        for it in st.session_state["sale_itens"]:
            qexec("insert into resto.sale_item(sale_id, product_id, qty, unit_price, total) values (%s,%s,%s,%s,%s);",
                  (sale_id, it["product_id"], it["qty"], it["unit_price"], it["total"]))
            qexec("select resto.sp_register_movement(%s,'OUT',%s,null,'sale',%s,%s);", (it["product_id"], it["qty"], sale_id, ''))

        st.session_state["sale_itens"] = []
        st.success(f"Venda #{sale_id} fechada e estoque baixado!")

def page_receitas_precos():
    header("🥣 Fichas Técnicas & Precificação", "Monte receitas e calcule preço sugerido.")
    prods = safe_qall("select id, name from resto.product order by name;")
    units = safe_qall("select id, abbr from resto.unit order by abbr;")

    tab = st.tabs(["Receitas", "Precificação"])

    # ---------- Receitas ----------
    with tab[0]:
        card_start()
        st.subheader("Ficha técnica do produto")
        prod = S("Produto final *", options=[(p['id'], p['name']) for p in prods] if prods else [], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
        if prod:
            recipe = safe_qone("select * from resto.recipe where product_id=%s;", (prod[0],))
            if not recipe:
                st.info("Sem receita ainda. Preencha para criar.")
                with st.form("form_new_recipe"):
                    yield_qty = st.number_input("Rendimento (quantidade)", 0.0, 1_000_000.0, 10.0, 0.1)
                    yield_unit = S("Unidade do rendimento", options=[(u['id'], u['abbr']) for u in units] if units else [], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
                    overhead = st.number_input("Custo indireto % (gás/energia/mão de obra)", 0.0, 999.0, 0.0, 0.1)
                    loss = st.number_input("Perdas %", 0.0, 100.0, 0.0, 0.1)
                    ok = st.form_submit_button("Criar ficha técnica")
                if ok and yield_unit:
                    safe_qall("insert into resto.recipe(product_id, yield_qty, yield_unit_id, overhead_pct, loss_pct) values (%s,%s,%s,%s,%s);",
                              (prod[0], yield_qty, yield_unit[0], overhead, loss))
                    st.success("Ficha criada!")
                    recipe = safe_qone("select * from resto.recipe where product_id=%s;", (prod[0],))

            if recipe:
                unit_abbr = safe_qone('select abbr from resto.unit where id=%s;', (recipe['yield_unit_id'],))
                unit_abbr = unit_abbr['abbr'] if unit_abbr else '?'
                st.caption(f"Rende {recipe['yield_qty']} {unit_abbr} | Indiretos: {recipe['overhead_pct']}% | Perdas: {recipe['loss_pct']}% ")
                with st.expander("Adicionar ingrediente"):
                    ing = S("Ingrediente", options=[(p['id'], p['name']) for p in prods] if prods else [], format_func=lambda x: x[1] if isinstance(x, tuple) else x, key="ing_sel")
                    qty = st.number_input("Quantidade", 0.0, 1_000_000.0, 1.0, 0.1, key="ing_qty")
                    unit = S("Unidade", options=[(u['id'], u['abbr']) for u in units] if units else [], format_func=lambda x: x[1] if isinstance(x, tuple) else x, key="ing_unit")
                    conv = st.number_input("Fator de conversão (opcional)", 0.0, 1_000_000.0, 1.0, 0.01, help="Ex: 1 colher = 15 ml → use 15 se seu estoque estiver em ml.")
                    add = st.button("Adicionar ingrediente", key="ing_add")
                    if add and ing and qty>0 and unit:
                        safe_qall("insert into resto.recipe_item(recipe_id, ingredient_id, qty, unit_id, conversion_factor) values (%s,%s,%s,%s,%s);",
                                  (recipe['id'], ing[0], qty, unit[0], conv))
                        st.success("Ingrediente incluído!")

                items = safe_qall("""
                    select ri.id, p.name as ingrediente, ri.qty, u.abbr
                      from resto.recipe_item ri
                      join resto.product p on p.id = ri.ingredient_id
                      join resto.unit u on u.id = ri.unit_id
                     where ri.recipe_id=%s
                     order by p.name;
                """, (recipe['id'],))
                st.dataframe(pd.DataFrame(items), use_container_width=True, hide_index=True)

                # custo estimado (view)
                cost = safe_qone("select * from resto.v_recipe_cost where product_id=%s;", (prod[0],))
                if cost:
                    st.markdown(f"**Custo do lote:** {money(cost['batch_cost'])} • **Custo unitário estimado:** {money(cost['unit_cost_estimated'])}")
                else:
                    st.caption("Adicione ingredientes para ver o custo (ou crie a view v_recipe_cost).")
        card_end()

    # ---------- Precificação ----------
    with tab[1]:
        card_start()
        st.subheader("Simulador de Preço de Venda")
        prod = S("Produto", options=[(p['id'], p['name']) for p in prods] if prods else [], format_func=lambda x: x[1] if isinstance(x, tuple) else x, key="prec_prod")
        if prod:
            rec = safe_qone("select unit_cost_estimated from resto.v_recipe_cost where product_id=%s;", (prod[0],))
            avg = safe_qone("select avg_cost from resto.product where id=%s;", (prod[0],))
            base_cost = (rec['unit_cost_estimated'] if rec and rec.get('unit_cost_estimated') is not None else (avg['avg_cost'] if avg else 0.0))
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
    prods = safe_qall("select id, name from resto.product order by name;")
    units = {r["id"]: r["abbr"] for r in safe_qall("select id, abbr from resto.unit;")}

    card_start()
    st.subheader("Nova produção")
    prod = S("Produto final *", options=[(p['id'], p['name']) for p in prods] if prods else [], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
    if not prod:
        card_end()
        return

    recipe = safe_qone("select * from resto.recipe where product_id=%s;", (prod[0],))
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
        has_expiry = st.checkbox("Produto final tem validade?", value=False)
        expiry_final = st.date_input("Validade do produto final", value=date.today()) if has_expiry else None
        ok = st.form_submit_button("Produzir")

    if not ok or qty_out <= 0:
        card_end()
        return

    # Carrega ingredientes da receita
    ingredients = safe_qall("""
        select ri.ingredient_id, p.name as ingrediente, ri.qty, ri.conversion_factor, ri.unit_id
          from resto.recipe_item ri
          join resto.product p on p.id = ri.ingredient_id
         where ri.recipe_id=%s
         order by p.name;
    """, (recipe["id"],))

    scale = qty_out / yield_qty
    consumos = []
    total_ing_cost = 0.0
    falta = []

    for it in ingredients:
        need = float(it["qty"] or 0.0) * float(it.get("conversion_factor") or 1.0) * scale
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

    overhead = float(recipe.get("overhead_pct") or 0.0) / 100.0
    loss = float(recipe.get("loss_pct") or 0.0) / 100.0
    batch_cost = total_ing_cost * (1 + overhead) * (1 + loss)
    unit_cost_est = batch_cost / qty_out if qty_out > 0 else 0.0

    prow = qone("""
        insert into resto.production(date, product_id, qty, unit_cost, total_cost, lot_number, expiry_date, note)
        values (now(), %s, %s, %s, %s, %s, %s, %s)
        returning id;
    """, (prod[0], qty_out, unit_cost_est, batch_cost, (lot_final or None), (str(expiry_final) if expiry_final else None), ""))
    production_id = prow["id"]

    for c in consumos:
        pi = qone("""
            insert into resto.production_item(production_id, ingredient_id, lot_id, qty, unit_cost, total_cost)
            values (%s,%s,%s,%s,%s,%s)
            returning id;
        """, (production_id, c["ingredient_id"], c["lot_id"], c["qty"], c["unit_cost"], c["total"]))
        note = f"production:{production_id};lot:{c['lot_id']}"
        qexec("select resto.sp_register_movement(%s,'OUT',%s,%s,'production',%s,%s);", (c["ingredient_id"], c["qty"], c["unit_cost"], pi["id"], note))

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
        rows = safe_qall("select * from resto.v_stock order by name;")
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.caption("Crie a view v_stock ou use os relatórios de compras/vendas.")
        card_end()

    with tabs[1]:
        card_start()
        st.subheader("Movimentações recentes")
        mv = safe_qall("select move_date, kind, product_id, qty, unit_cost, total_cost, reason, reference_id, note from resto.inventory_movement order by move_date desc limit 500;")
        st.dataframe(pd.DataFrame(mv), use_container_width=True, hide_index=True)
        card_end()

    with tabs[2]:
        card_start()
        st.subheader("Alertas de validade e saldos por lote")
        dias = st.slider("Dias até o vencimento", 7, 120, 30, 1)
        rows = safe_qall("""
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
        cats = safe_qall("select id, name, kind from resto.cash_category order by name;")
        if not cats:
            qexec("insert into resto.cash_category(name, kind) values ('Vendas', 'IN'), ('Compras', 'OUT'), ('Despesas Fixas', 'OUT'), ('Outros Recebimentos', 'IN'), ('Outros Pagamentos', 'OUT') on conflict do nothing;")
            cats = safe_qall("select id, name, kind from resto.cash_category order by name;")

        with st.form("form_caixa"):
            dt = st.date_input("Data", value=date.today())
            cat = S("Categoria", options=[(c['id'], f"{c['name']} ({c['kind']})") for c in cats] if cats else [], format_func=lambda x: x[1] if isinstance(x, tuple) else x)
            kind = 'IN' if (cat and '(IN)' in cat[1]) else 'OUT'
            desc = st.text_input("Descrição")
            val  = st.number_input("Valor", 0.00, 1_000_000.00, 0.00, 0.01)
            method = S("Forma de pagamento", ['dinheiro','pix','cartão débito','cartão crédito','boleto','outro'])
            ok = st.form_submit_button("Lançar")
        if ok and val>0 and cat:
            qexec("insert into resto.cashbook(entry_date, kind, category_id, description, amount, method) values (%s,%s,%s,%s,%s,%s);", (dt, kind, cat[0], desc, val, method))
            st.success("Lançamento registrado!")

        df = pd.DataFrame(safe_qall("select entry_date, kind, description, amount, method from resto.cashbook order by entry_date desc, id desc limit 500;"))
        st.dataframe(df, use_container_width=True, hide_index=True)
        card_end()

    with tabs[1]:
        card_start()
        st.subheader("DRE resumida (mês atual)")
        dre = safe_qone("""
            with
            vendas as (select coalesce(sum(total),0) v from resto.sale where status='FECHADA' and date_trunc('month', date)=date_trunc('month', now())),
            cmv as (select coalesce(sum(case when kind='OUT' then total_cost else 0 end),0) c from resto.inventory_movement where move_date >= date_trunc('month', now()) and move_date < (date_trunc('month', now()) + interval '1 month')),
            caixa_desp as (select coalesce(sum(case when kind='OUT' then amount else 0 end),0) d from resto.cashbook where date_trunc('month', entry_date)=date_trunc('month', now())),
            caixa_outros as (select coalesce(sum(case when kind='IN' then amount else 0 end),0) o from resto.cashbook where date_trunc('month', entry_date)=date_trunc('month', now()))
            select v, c, d, o, (v + o - c - d) as resultado from vendas, cmv, caixa_desp, caixa_outros;
        """)
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

# ===================== IMPORTADOR DE EXTRATO =====================
def _parse_brl_amount(x: str) -> float:
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

def _guess_sep(buf: str) -> str:
    candidates = [';', ',', '\t', '|']
    best = ','; best_cols = 1
    line = buf.splitlines()[0] if buf else ''
    for sep in candidates:
        cols = len(line.split(sep))
        if cols > best_cols:
            best_cols, best = cols, sep
    return best

def _load_csv(file) -> pd.DataFrame:
    raw = file.read() if hasattr(file, "read") else file
    try:
        txt = raw.decode('utf-8-sig')
    except Exception:
        txt = raw.decode('latin1', errors='ignore')
    sep = _guess_sep(txt)
    df = pd.read_csv(pd.io.common.StringIO(txt), sep=sep, dtype=str, keep_default_na=False, na_values=[''])
    # normaliza prováveis colunas de valor
    for c in df.columns:
        if any(k in c.lower() for k in ['valor','amount','credito','crédito','debito','débito']):
            df[c] = (df[c].str.replace('.', '', regex=False)
                           .str.replace(',', '.', regex=False)
                           .str.replace('R$', '', regex=False)
                           .str.replace(' ', '', regex=False)
                           .str.replace('+', '', regex=False)
                           .str.replace('\u00A0', '', regex=False))
    return df

def _load_ofx(file) -> pd.DataFrame:
    try:
        from ofxparse import OfxParser
    except Exception:
        st.error("Para importar OFX, adicione `ofxparse` ao requirements.txt")
        return pd.DataFrame()
    ofx = OfxParser.parse(file)
    rows = []
    for acct in ofx.accounts:
        for tx in acct.statement.transactions:
            rows.append({
                'entry_date': (tx.date.date() if hasattr(tx.date, 'date') else tx.date),
                'description': (tx.memo or tx.payee or ''),
                'amount': float(tx.amount)
            })
    return pd.DataFrame(rows)

def _load_c6bank_csv_bytes(raw_bytes) -> pd.DataFrame:
    from io import StringIO
    try:
        txt = raw_bytes.decode("utf-8-sig")
    except Exception:
        txt = raw_bytes.decode("latin1", errors="ignore")
    lines = txt.splitlines()
    header_marker = "Data Lançamento,Data Contábil,Título,Descrição,Entrada(R$),Saída(R$),Saldo do Dia(R$)"
    start = next((i for i, ln in enumerate(lines) if header_marker in ln), None)
    if start is None:
        return pd.DataFrame()
    csv_body = "\n".join(lines[start:])
    df_raw = pd.read_csv(StringIO(csv_body), sep=",", dtype=str, keep_default_na=False)
    df_std = pd.DataFrame({
        "entry_date": pd.to_datetime(df_raw["Data Lançamento"], dayfirst=True, errors="coerce").dt.date,
        "description": df_raw["Descrição"].astype(str),
    })
    entrada = df_raw["Entrada(R$)"].apply(_parse_brl_amount)
    saida   = df_raw["Saída(R$)"].apply(_parse_brl_amount)
    df_std["amount"] = entrada - saida
    df_std = df_std.dropna(subset=["entry_date"])
    return df_std

def _load_bank_file(upfile) -> pd.DataFrame:
    # Detecta automaticamente C6/OFX/CSV genérico evitando bytes com não-ASCII
    name = (upfile.name or "").lower()
    raw = upfile.read()

    header_marker_text = "Data Lançamento,Data Contábil,Título,Descrição,Entrada(R$),Saída(R$),Saldo do Dia(R$)"
    try:
        txt = raw.decode("utf-8-sig")
    except Exception:
        txt = raw.decode("latin1", errors="ignore")

    # C6 Bank: cabeçalho textual + linha de títulos
    if "EXTRATO DE CONTA CORRENTE C6 BANK" in txt or header_marker_text in txt:
        return _load_c6bank_csv_bytes(raw)

    # OFX
    if name.endswith(".ofx"):
        from io import BytesIO
        return _load_ofx(BytesIO(raw))

    # CSV genérico
    from io import BytesIO
    return _load_csv(BytesIO(raw))

@st.cache_data(show_spinner=False)
def _load_rules() -> List[Dict[str, Any]]:
    return safe_qall("""
        select r.id, r.pattern, r.kind, r.method, r.priority,
               r.category_id, c.name as category_name
          from resto.cash_rule r
          left join resto.cash_category c on c.id = r.category_id
         order by r.priority desc, r.id asc;
    """)

def _classify_tx(desc: str, amount: float) -> Dict[str, Any]:
    desc_norm = (desc or '').upper()
    rules = _load_rules()
    for r in rules:
        if r['pattern'] and r['pattern'] in desc_norm:
            kind = r['kind'] or ('IN' if amount >= 0 else 'OUT')
            return {'category_id': r['category_id'], 'category_name': r['category_name'], 'kind': kind, 'method': r['method'] or 'outro'}
    kind = 'IN' if amount >= 0 else 'OUT'
    if kind == 'IN':
        cat = safe_qone("select id, name from resto.cash_category where name='Outros Recebimentos';")
    else:
        cat = safe_qone("select id, name from resto.cash_category where name='Outros Pagamentos';")
    return {'category_id': cat['id'] if cat else None, 'category_name': cat['name'] if cat else None, 'kind': kind, 'method': 'outro'}

def _find_duplicates(df: pd.DataFrame) -> pd.Series:
    if df.empty or 'entry_date' not in df.columns:
        return pd.Series([False]*len(df))
    rows = safe_qall("""
        select entry_date, amount, description
          from resto.cashbook
         where entry_date >= (select coalesce(min(%s::date), current_date - interval '180 days'))
    """, (str(df['entry_date'].min()),))
    base = pd.DataFrame(rows)
    if base.empty:
        return pd.Series([False]*len(df))
    base['key'] = base['entry_date'].astype(str) + '|' + base['amount'].round(2).astype(str) + '|' + base['description'].str.lower().str.slice(0, 40)
    df = df.copy()
    df['key'] = df['entry_date'].astype(str) + '|' + df['amount'].round(2).astype(str) + '|' + df['description'].str.lower().str.slice(0, 40)
    dup = df['key'].isin(set(base['key']))
    return dup

def page_importar_extrato():
    header("🏦 Importar Extrato Bancário", "CSV ou OFX → classifica e lança no Livro Caixa.")
    card_start()
    up = st.file_uploader("Selecione o arquivo do banco (CSV ou OFX)", type=["csv","ofx"])
    if not up:
        st.caption("Dica: o C6 exporta um CSV com cabeçalho textual; este importador já detecta automaticamente.")
        card_end(); return

    df = _load_bank_file(up)
    if set(df.columns) >= {"entry_date","description","amount"}:
        st.success(f"Arquivo reconhecido automaticamente: {len(df)} lançamentos.")
        out = df.copy()
    else:
        st.error("Não foi possível reconhecer o layout do arquivo. Tente exportar em CSV padrão (C6) ou use OFX.")
        card_end(); return

    # classificar por regras
    rows = []
    for _, r in out.iterrows():
        clas = _classify_tx(str(r['description']), float(r['amount']))
        rows.append({
            'entry_date': r['entry_date'],
            'description': str(r['description'])[:250],
            'amount': round(float(r['amount']),2),
            'kind': clas['kind'],
            'category_id': clas['category_id'],
            'category_name': clas['category_name'],
            'method': clas['method']
        })
    out = pd.DataFrame(rows)
    out['duplicado?'] = _find_duplicates(out)

    st.markdown("### Pré-visualização")
    st.dataframe(out, use_container_width=True, hide_index=True)

    to_import = out[~out['duplicado?']].copy()
    st.info(f"Pronto para importar: **{len(to_import)}** lançamentos (ignorando **{int(out['duplicado?'].sum())}** possíveis duplicados).")

    cats = safe_qall("select id, name, kind from resto.cash_category order by name;")
    col1, col2, col3 = st.columns(3)
    with col1:
        sel_cat = S("Atribuir categoria (opcional)", options=[(None,"— manter classificação —")] + ([(c['id'], c['name']) for c in cats] if cats else []),
                               format_func=lambda x: x[1] if isinstance(x, tuple) else x)
    with col2:
        sel_kind = S("Atribuir tipo (opcional)", options=["— manter —","IN","OUT"], index=0)
    with col3:
        sel_method = S("Atribuir método (opcional)", options=["","pix","dinheiro","cartão débito","cartão crédito","boleto","transferência","outro"], index=0)

    if st.checkbox("Aplicar ajustes acima a todos os itens prontos para importação"):
        if isinstance(sel_cat, tuple) and sel_cat[0]:
            to_import['category_id'] = sel_cat[0]
            to_import['category_name'] = sel_cat[1]
        if sel_kind in ("IN","OUT"):
            to_import['kind'] = sel_kind
        if sel_method:
            to_import['method'] = sel_method

    ok = st.button(f"🚀 Importar {len(to_import)} lançamentos no Livro Caixa")
    if ok and not to_import.empty:
        for _, r in to_import.iterrows():
            qexec("""
              insert into resto.cashbook(entry_date, kind, category_id, description, amount, method)
              values (%s,%s,%s,%s,%s,%s);
            """, (str(r['entry_date']), r['kind'], int(r['category_id']) if r['category_id'] else None, r['description'], float(r['amount']), r['method'] or 'outro'))
        st.success(f"Importados {len(to_import)} lançamentos!")
    card_end()

# ===================== ROUTER =====================
def main():
    if not ensure_ping():
        st.stop()
    ensure_migrations()

    header("🍝 Restô ERP Lite", "Financeiro • Fiscal-ready • Estoque • Ficha técnica • Preços • Produção • Extrato bancário")
    page = st.sidebar.radio(
        "Menu",
        ["Painel", "Cadastros", "Compras", "Vendas", "Receitas & Preços", "Produção", "Estoque", "Financeiro", "Importar Extrato"],
        index=0
    )

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
