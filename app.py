
import os
from datetime import date, time
from typing import Any, Dict, List, Optional, Tuple
import re


# ===================== HELPER: inferir método pela descrição =====================
def _guess_method_from_desc(desc: str) -> str:
    d = (str(desc) if desc is not None else "").upper()
    if "PIX" in d or "QRCODE" in d or "QR CODE" in d or "CHAVE" in d:
        return "pix"
    if "TED" in d or "TEF" in d or "DOC" in d or "TRANSFER" in d or "TRANSFERÊNCIA" in d or "TRANSFERENCIA" in d:
        return "transferência"
    if any(k in d for k in ["PAYGO","PAGSEGURO","STONE","CIELO","REDE","GETNET","MERCADO PAGO","VISA","MASTERCARD","ELO"]):
        return "cartão crédito"
    if "DÉBITO" in d or "DEBITO" in d:
        return "cartão débito"
    if "BOLETO" in d:
        return "boleto"
    if "SAQUE" in d or "ATM" in d:
        return "dinheiro"
    return "outro"


import pandas as pd
import psycopg, psycopg.rows
import streamlit as st

# ===================== CONFIG =====================
st.set_page_config(
    page_title="SISGET",
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


def qexec(sql: str, params=None):
    with _get_conn() as con:
        with con.cursor() as cur:
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)
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

def _ensure_product_schema_unificado():
    qexec("""
    do $$ begin
      -- básicos de estoque
      alter table resto.product  add column if not exists code           text;
      alter table resto.product  add column if not exists unit           text default 'un';
      alter table resto.product  add column if not exists category       text;
      alter table resto.product  add column if not exists supplier_id    bigint references resto.supplier(id);
      alter table resto.product  add column if not exists barcode        text;
      alter table resto.product  add column if not exists min_stock      numeric(14,3) default 0;
      alter table resto.product  add column if not exists last_cost      numeric(14,2) default 0;
      alter table resto.product  add column if not exists avg_cost       numeric(14,2);
      alter table resto.product  add column if not exists stock_qty      numeric(14,3);
      alter table resto.product  add column if not exists active         boolean default true;

      -- comercial/fiscal
      alter table resto.product  add column if not exists sale_price       numeric(14,2) default 0;
      alter table resto.product  add column if not exists is_sale_item     boolean default true;
      alter table resto.product  add column if not exists is_ingredient    boolean default false;
      alter table resto.product  add column if not exists default_markup   numeric(10,2) default 0;

      alter table resto.product  add column if not exists ncm              text;
      alter table resto.product  add column if not exists cest             text;
      alter table resto.product  add column if not exists cfop_venda       text;
      alter table resto.product  add column if not exists csosn            text;
      alter table resto.product  add column if not exists cst_icms         text;
      alter table resto.product  add column if not exists aliquota_icms    numeric(6,2);
      alter table resto.product  add column if not exists cst_pis          text;
      alter table resto.product  add column if not exists aliquota_pis     numeric(6,2);
      alter table resto.product  add column if not exists cst_cofins       text;
      alter table resto.product  add column if not exists aliquota_cofins  numeric(6,2);
      alter table resto.product  add column if not exists iss_aliquota     numeric(6,2);
    end $$;
    """)

def _money_br(v):
    try:
        v = float(v or 0)
    except Exception:
        v = 0.0
    return ("R$ {:,.2f}".format(v)).replace(",", "X").replace(".", ",").replace("X", ".")

# --- Helpers (colocar no topo da page_compras) ---
def _ensure_cash_category_compras() -> int:
    r = qone("""
        with upsert as (
          insert into resto.cash_category(kind, name)
          values ('OUT','Compras/Estoque')
          on conflict (kind, name) do update set name=excluded.name
          returning id
        )
        select id from upsert
        union all
        select id from resto.cash_category where kind='OUT' and name='Compras/Estoque' limit 1;
    """)
    return int(r["id"])

def _ensure_cash_category(kind: str, name: str) -> int:
    """Garante/retorna o id da categoria (kind, name) sem exigir índice único."""
    r = qone("""
        with upsert as (
          insert into resto.cash_category(kind, name)
          values (%s,%s)
          on conflict (kind, name) do update set name=excluded.name
          returning id
        )
        select id from upsert
        union all
        select id from resto.cash_category where kind=%s and name=%s limit 1;
    """, (kind, name, kind, name))
    return int(r["id"])

def _sales_import_category_id() -> int:
    """Categoria padrão para entradas importadas tratadas como VENDAS."""
    return _ensure_cash_category('IN', 'Vendas (Importadas)')

def _record_cashbook_out_from_purchase(purchase_id: int, method: str, entry_date):
    head = qone("""
        select p.id, p.doc_date, coalesce(p.freight_value,0) frete, coalesce(p.other_costs,0) outros, s.name fornecedor
          from resto.purchase p
          join resto.supplier s on s.id = p.supplier_id
         where p.id=%s;
    """, (int(purchase_id),))
    tot_itens = qone("select coalesce(sum(total),0) s from resto.purchase_item where purchase_id=%s;", (int(purchase_id),))["s"]
    total = float(tot_itens) + float(head["frete"]) + float(head["outros"])
    cat_id = _ensure_cash_category_compras()
    desc = f"Compra #{purchase_id} – {head['fornecedor']}"
    qexec("""
        insert into resto.cashbook(entry_date, kind, category_id, description, amount, method)
        values (%s,'OUT',%s,%s,%s,%s);
    """, (entry_date, cat_id, desc, total, method))

# ===================== UI Helpers =====================
def header(title: str, subtitle: str = "", logo: str | None = None, logo_height: int = 56):
    import base64, mimetypes, os
    # injeta o CSS SEM usar session_state (injeta a cada rerun)
    st.markdown("""
    <style>
      .hdr-band{
        background: linear-gradient(135deg,#654321 0%,#654321 45%,#654321 100%);
        color:#fff; border-radius:14px; padding:14px 18px; margin:8px 0 18px 0;
        display:flex; align-items:center; gap:14px;
        box-shadow: 0 6px 24px rgba(0,0,0,.15);
      }
      .hdr-band .hdr-logo{ display:block; border-radius:10px; }
      .hdr-band .hdr-txt h1{
        margin:0; font-weight:700; letter-spacing:.2px; line-height:1.2;
        font-size:clamp(18px,2.2vw,24px);
      }
      .hdr-band .hdr-txt p{
        margin:2px 0 0 0; opacity:.9; font-size:clamp(12px,1.6vw,14px);
      }
    </style>
    """, unsafe_allow_html=True)

    def _as_src(img: str) -> str:
        if not img: return ""
        if img.startswith(("http://","https://","data:")):
            return img
        if os.path.exists(img):
            mime, _ = mimetypes.guess_type(img)
            with open(img, "rb") as f:
                b64 = base64.b64encode(f.read()).decode("ascii")
            return f"data:{mime or 'image/png'};base64,{b64}"
        return img

    logo_src = _as_src(logo) if logo else ""
    html = ['<div class="hdr-band">']
    if logo_src:
        html.append(f'<img class="hdr-logo" src="{logo_src}" alt="logo" height="{logo_height}">')
    html.append('<div class="hdr-txt">')
    html.append(f'<h1>{title}</h1>')
    if subtitle:
        html.append(f'<p>{subtitle}</p>')
    html.append('</div></div>')
    st.markdown("".join(html), unsafe_allow_html=True)

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

# helper universal (coloque perto das outras funções utilitárias)
def _rerun():
    try:
        st.rerun()
    except Exception:
        # fallback p/ versões antigas
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()

# ===================== Pages =====================

def page_dashboard():
    header("📊 Painel", "Visão geral do SISGET.")
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

#======================================CADSTROS=================================================================================
def page_cadastros():
    import pandas as pd

    # ——— defensivo: garante colunas/índices necessários ———
    def _ensure_schema():
        qexec("""
        do $$
        begin
          -- preço fiscal no produto
          if not exists (
            select 1 from information_schema.columns
             where table_schema='resto' and table_name='product' and column_name='sale_price'
          ) then
            alter table resto.product add column sale_price numeric(14,2) default 0;
          end if;

          -- dica/observação na unidade (usada no form)
          if not exists (
            select 1 from information_schema.columns
             where table_schema='resto' and table_name='unit' and column_name='base_hint'
          ) then
            alter table resto.unit add column base_hint text;
          end if;

          -- índice único para permitir ON CONFLICT (abbr)
          if not exists (
            select 1
              from pg_indexes
             where schemaname='resto' and indexname='unit_abbr_uq'
          ) then
            create unique index unit_abbr_uq on resto.unit(abbr);
          end if;

          -- opcional: garantir unicidade de categoria por nome (para ON CONFLICT(name))
          if not exists (
            select 1
              from pg_indexes
             where schemaname='resto' and indexname='category_name_uq'
          ) then
            create unique index category_name_uq on resto.category(name);
          end if;
        end $$;
        """)

    _ensure_schema()

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
            qexec("""
                insert into resto.unit(name, abbr, base_hint)
                values (%s,%s,%s)
                on conflict (abbr) do update
                  set name=excluded.name,
                      base_hint=excluded.base_hint;
            """, (name, abbr, base_hint))
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
                # normalizações leves para evitar sujeira
                _name  = (name or "").strip()
                _cnpj  = re.sub(r"\D", "", cnpj or "") or None   # só dígitos
                _ie    = (ie or "").strip() or None
                _email = (email or "").strip() or None
                _phone = re.sub(r"\D", "", phone or "") or None  # só dígitos
            
                try:
                    qexec("""
                        insert into resto.supplier(name, cnpj, ie, email, phone)
                        values (%s,%s,%s,%s,%s);
                    """, (_name, _cnpj, _ie, _email, _phone))
                    st.success("Fornecedor salvo!")
                except Exception as e:
                    st.error("Não foi possível salvar o fornecedor. Verifique os dados (nome obrigatório).")

        rows = qall("select id, name, cnpj, ie, email, phone from resto.supplier order by name;")
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        card_end()

    # ---------- Produtos ----------
    with tabs[3]:
        _ensure_product_schema_unificado()
        card_start()
        st.subheader("Produtos (Catálogo Fiscal) – UNIFICADO")

        units = qall("select abbr from resto.unit order by abbr;") or []
        cats  = qall("select name from resto.category order by name;") or []
        sups  = qall("select id, name from resto.supplier where coalesce(active,true) is true order by name;") or []
        sup_opts = [(None, "— sem fornecedor —")] + [(r["id"], r["name"]) for r in sups]

        with st.form("form_prod_unificado"):
            c1, c2, c3 = st.columns([2,1,1])
            with c1:
                code = st.text_input("Código interno")
                name = st.text_input("Nome *")
            with c2:
                unit = st.selectbox("Unidade *", options=[u["abbr"] for u in units] or ["un"])
            with c3:
                category = st.selectbox("Categoria", options=[c["name"] for c in cats] or [""])

            c4, c5, c6 = st.columns([2,1,1])
            with c4:
                supplier = st.selectbox("Fornecedor", options=sup_opts, format_func=lambda x: x[1] if isinstance(x, tuple) else x)
            with c5:
                min_stock = st.number_input("Estoque mínimo", 0.0, 1_000_000.0, 0.0, 0.001, format="%.3f")
            with c6:
                last_cost = st.number_input("Último custo (R$)", 0.0, 1_000_000.0, 0.0, 0.01, format="%.2f")

            c7, c8, c9 = st.columns([2,1,1])
            with c7:
                barcode = st.text_input("Código de barras")
            with c8:
                sale_price = st.number_input("Preço de venda (R$)", 0.0, 1_000_000.0, 0.0, 0.01, format="%.2f")
            with c9:
                markup = st.number_input("Markup padrão %", 0.0, 1000.0, 0.0, 0.1, format="%.2f")

            c10, c11, c12 = st.columns(3)
            with c10:
                is_sale = st.checkbox("Item de venda", value=True)
            with c11:
                is_ing  = st.checkbox("Ingrediente", value=False)
            with c12:
                active  = st.checkbox("Ativo", value=True)

            st.markdown("**Campos fiscais (opcional)**")
            f1, f2, f3, f4 = st.columns(4)
            with f1:
                ncm = st.text_input("NCM");        cest = st.text_input("CEST")
            with f2:
                cfop = st.text_input("CFOP Venda"); csosn = st.text_input("CSOSN")
            with f3:
                cst_icms = st.text_input("CST ICMS"); ali_icms = st.number_input("Alíquota ICMS %", 0.0, 100.0, 0.0, 0.01)
            with f4:
                cst_pis = st.text_input("CST PIS");   ali_pis = st.number_input("Alíquota PIS %", 0.0, 100.0, 0.0, 0.01)
            f5, f6 = st.columns(2)
            with f5:
                cst_cof = st.text_input("CST COFINS"); ali_cof = st.number_input("Alíquota COFINS %", 0.0, 100.0, 0.0, 0.01)
            with f6:
                iss = st.number_input("ISS % (se serviço)", 0.0, 100.0, 0.0, 0.01)

            ok = st.form_submit_button("Salvar produto")

        if ok and name.strip():
            supplier_id = supplier[0] if isinstance(supplier, tuple) else None
            qexec("""
                insert into resto.product(
                    code, name, unit, category, supplier_id, barcode,
                    min_stock, last_cost, active,
                    sale_price, is_sale_item, is_ingredient, default_markup,
                    ncm, cest, cfop_venda, csosn, cst_icms, aliquota_icms,
                    cst_pis, aliquota_pis, cst_cofins, aliquota_cofins, iss_aliquota
                )
                values (%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s)
                on conflict (name) do update set
                    code=excluded.code,
                    unit=excluded.unit,
                    category=excluded.category,
                    supplier_id=excluded.supplier_id,
                    barcode=excluded.barcode,
                    min_stock=excluded.min_stock,
                    last_cost=excluded.last_cost,
                    active=excluded.active,
                    sale_price=excluded.sale_price,
                    is_sale_item=excluded.is_sale_item,
                    is_ingredient=excluded.is_ingredient,
                    default_markup=excluded.default_markup,
                    ncm=excluded.ncm,
                    cest=excluded.cest,
                    cfop_venda=excluded.cfop_venda,
                    csosn=excluded.csosn,
                    cst_icms=excluded.cst_icms,
                    aliquota_icms=excluded.aliquota_icms,
                    cst_pis=excluded.cst_pis,
                    aliquota_pis=excluded.aliquota_pis,
                    cst_cofins=excluded.cst_cofins,
                    aliquota_cofins=excluded.aliquota_cofins,
                    iss_aliquota=excluded.iss_aliquota;
            """, (code or None, name.strip(), unit, category or None, supplier_id, barcode or None,
                  float(min_stock), float(last_cost), bool(active),
                  float(sale_price), bool(is_sale), bool(is_ing), float(markup),
                  ncm or None, cest or None, cfop or None, csosn or None, cst_icms or None, ali_icms or None,
                  cst_pis or None, ali_pis or None, cst_cof or None, ali_cof or None, iss or None))
            st.success("Produto salvo!")

        # Listagem UNIFICADA
        rows = qall("""
            select p.id, p.code, p.name, p.unit, p.category,
                   s.name as supplier, p.barcode,
                   p.min_stock, p.last_cost, p.sale_price,
                   p.is_sale_item, p.is_ingredient, p.default_markup, p.active
              from resto.product p
              left join resto.supplier s on s.id = p.supplier_id
             order by lower(p.name);
        """) or []
        import pandas as pd
        df = pd.DataFrame(rows)
        if not df.empty:
            df["Excluir?"] = False
            cfg = {
                "id":              st.column_config.NumberColumn("ID", disabled=True),
                "code":            st.column_config.TextColumn("Código"),
                "name":            st.column_config.TextColumn("Nome"),
                "unit":            st.column_config.SelectboxColumn("Un", options=[u["abbr"] for u in units] or ["un"]),
                "category":        st.column_config.SelectboxColumn("Categoria", options=[c["name"] for c in cats] or [""]),
                "supplier":        st.column_config.TextColumn("Fornecedor", disabled=True),
                "barcode":         st.column_config.TextColumn("Código de barras"),
                "min_stock":       st.column_config.NumberColumn("Est. mín.", step=0.001, format="%.3f"),
                "last_cost":       st.column_config.NumberColumn("Últ. custo", step=0.01, format="%.2f"),
                "sale_price":      st.column_config.NumberColumn("Preço venda", step=0.01, format="%.2f"),
                "is_sale_item":    st.column_config.CheckboxColumn("Venda"),
                "is_ingredient":   st.column_config.CheckboxColumn("Ingred."),
                "default_markup":  st.column_config.NumberColumn("Markup %", step=0.1, format="%.2f"),
                "active":          st.column_config.CheckboxColumn("Ativo"),
                "Excluir?":        st.column_config.CheckboxColumn("Excluir?"),
            }
            edited = st.data_editor(
                df[["id","code","name","unit","category","supplier","barcode","min_stock","last_cost","sale_price",
                    "is_sale_item","is_ingredient","default_markup","active","Excluir?"]],
                column_config=cfg, hide_index=True, num_rows="fixed", key="prod_edit_cad", use_container_width=True
            )

            cpa, cpb = st.columns(2)
            with cpa:
                apply = st.button("💾 Salvar alterações (produtos)")
            with cpb:
                refresh = st.button("🔄 Atualizar")
            if refresh:
                st.rerun()

            if apply:
                orig = df.set_index("id")
                new  = edited.set_index("id")
                upd = delc = err = 0

                # exclusões
                to_del = new.index[new["Excluir?"] == True].tolist()
                for pid in to_del:
                    try:
                        qexec("delete from resto.product where id=%s;", (int(pid),))
                        delc += 1
                    except Exception:
                        err += 1

                # updates
                keep = [i for i in new.index if i not in to_del]
                for pid in keep:
                    a = orig.loc[pid]; b = new.loc[pid]
                    changed = any(str(a.get(f,"")) != str(b.get(f,"")) for f in
                                  ["code","name","unit","category","barcode","min_stock","last_cost",
                                   "sale_price","is_sale_item","is_ingredient","default_markup","active"])
                    if not changed:
                        continue
                    try:
                        qexec("""
                            update resto.product
                               set code=%s, name=%s, unit=%s, category=%s, barcode=%s,
                                   min_stock=%s, last_cost=%s, sale_price=%s,
                                   is_sale_item=%s, is_ingredient=%s, default_markup=%s, active=%s
                             where id=%s;
                        """, (b.get("code"), b.get("name"), b.get("unit"), b.get("category"), b.get("barcode"),
                              float(b.get("min_stock") or 0), float(b.get("last_cost") or 0), float(b.get("sale_price") or 0),
                              bool(b.get("is_sale_item")), bool(b.get("is_ingredient")), float(b.get("default_markup") or 0),
                              bool(b.get("active")), int(pid)))
                        upd += 1
                    except Exception:
                        err += 1

                st.success(f"Produtos: ✅ {upd} atualizado(s) • 🗑️ {delc} excluído(s) • ⚠️ {err} erro(s).")
                st.rerun()
        else:
            st.caption("Nenhum produto cadastrado.")
        card_end()

#==========================================================COMPRAS===========================================================================
def page_compras():
    import pandas as pd
    from datetime import date, timedelta

    # ---------------- helpers ----------------
    def _rerun():
        try:
            st.rerun()
        except Exception:
            if hasattr(st, "experimental_rerun"):
                st.experimental_rerun()

    def _ensure_purchase_schema():
        qexec("""
        do $$
        begin
          -- Cabeçalho
          create table if not exists resto.purchase(
            id            bigserial primary key,
            supplier_id   bigint not null references resto.supplier(id),
            doc_number    text,
            cfop_entrada  text,
            doc_date      date not null default current_date,
            freight_value numeric(14,2) default 0,
            other_costs   numeric(14,2) default 0,
            total         numeric(14,2) default 0,
            status        text not null default 'RASCUNHO',
            posted_at     timestamptz,
            estornado_em  timestamptz,
            created_at    timestamptz default now()
          );

          -- Se já existe, garante colunas
          alter table resto.purchase
            add column if not exists status        text not null default 'RASCUNHO';
          alter table resto.purchase
            add column if not exists posted_at     timestamptz;
          alter table resto.purchase
            add column if not exists estornado_em  timestamptz;

          begin
            alter table resto.purchase
              add constraint purchase_status_chk
              check (status in ('RASCUNHO','LANÇADA','POSTADA','ESTORNADA','CANCELADA'));
          exception when duplicate_object then null;
          end;

          -- Itens
          create table if not exists resto.purchase_item(
            id           bigserial primary key,
            purchase_id  bigint not null references resto.purchase(id) on delete cascade,
            product_id   bigint not null references resto.product(id),
            qty          numeric(14,3) not null,
            unit_id      bigint references resto.unit(id),
            unit_price   numeric(14,4) not null default 0,
            discount     numeric(14,2) not null default 0,
            total        numeric(14,2) not null default 0,
            lot_number   text,
            expiry_date  date
          );
        end $$;
        """)

    def _post_purchase(purchase_id: int):
        """Gera movimentos de estoque (IN) de todos os itens dessa compra."""
        items = qall("""
            select id, product_id, qty, unit_price, coalesce(expiry_date::text,'') as exp
              from resto.purchase_item
             where purchase_id=%s;
        """, (int(purchase_id),)) or []
        for it in items:
            note = f"lote:{it['id']}" + (f";exp:{it['exp']}" if it["exp"] else "")
            # registra IN por lote (reference_id = id do purchase_item)
            qexec("select resto.sp_register_movement(%s,'IN',%s,%s,'purchase',%s,%s);",
                  (int(it["product_id"]), float(it["qty"]), float(it["unit_price"]), int(it["id"]), note))
        qexec("update resto.purchase set status='POSTADA', posted_at=now() where id=%s;", (int(purchase_id),))

    def _unpost_purchase(purchase_id: int):
        """Estorna (gera OUT) em todos os itens. Marca ESTORNADA."""
        items = qall("""
            select id, product_id, qty, unit_price
              from resto.purchase_item
             where purchase_id=%s;
        """, (int(purchase_id),)) or []
        for it in items:
            note = f"revert:purchase:{purchase_id};lot:{it['id']}"
            qexec("select resto.sp_register_movement(%s,'OUT',%s,%s,'purchase_revert',%s,%s);",
                  (int(it["product_id"]), float(it["qty"]), float(it["unit_price"]), int(it["id"]), note))
        qexec("update resto.purchase set status='ESTORNADA', estornado_em=now() where id=%s;", (int(purchase_id),))

    _ensure_purchase_schema()

    header("📥 Compras", "Lançar notas, editar/excluir, e postar/estornar no estoque.")
    tabs = st.tabs(["🧾 Nova compra", "🗂️ Gerenciar compras"])

    # ============================== Aba: Nova compra ==============================
    with tabs[0]:
        suppliers = qall("select id, name from resto.supplier order by name;") or []
        prods     = qall("select id, name, unit_id from resto.product order by name;") or []
        units     = qall("select id, abbr from resto.unit order by abbr;") or []

        sup_opts  = [(s['id'], s['name']) for s in suppliers]
        prod_opts = [(p['id'], p['name']) for p in prods]
        unit_opts = [(u['id'], u['abbr']) for u in units]
        unit_idx_by_id = {u['id']: i for i, u in enumerate(units)}  # p/ default do select

        # Estado temporário (lista de itens)
        st.session_state.setdefault("compra_itens", [])

        card_start()
        st.subheader("Cabeçalho")
        colh1, colh2 = st.columns([2,1])
        with colh1:
            supplier = st.selectbox("Fornecedor *", options=sup_opts, format_func=lambda x: x[1] if isinstance(x, tuple) else x, key="comp_sup")
            doc_number = st.text_input("Número do documento", key="comp_docnum")
            cfop_ent   = st.text_input("CFOP Entrada", value="1102", key="comp_cfop")
        with colh2:
            doc_date   = st.date_input("Data do documento", value=date.today(), key="comp_date")
            freight    = st.number_input("Frete", 0.00, 9999999.99, 0.00, 0.01, key="comp_frete")
            other      = st.number_input("Outros custos", 0.00, 9999999.99, 0.00, 0.01, key="comp_outros")

        st.markdown("### Itens da compra (cada item = **lote**)")
        with st.expander("➕ Adicionar item", expanded=False):
            with st.form("form_add_item"):
                pcol1, pcol2 = st.columns([2,1])
                with pcol1:
                    prod = st.selectbox("Produto", options=prod_opts, key="add_prod",
                                        format_func=lambda x: x[1] if isinstance(x, tuple) else x)
                with pcol2:
                    # unidade default do produto (se tiver)
                    default_unit_index = 0
                    try:
                        prow = next((r for r in prods if r["id"] == (prod[0] if isinstance(prod, tuple) else None)), None)
                        if prow and prow.get("unit_id") in unit_idx_by_id:
                            default_unit_index = unit_idx_by_id[prow["unit_id"]]
                    except Exception:
                        pass
                    unit = st.selectbox("Unidade", options=unit_opts, index=default_unit_index,
                                        key="add_unit",
                                        format_func=lambda x: x[1] if isinstance(x, tuple) else x)

                pcol3, pcol4, pcol5 = st.columns([1,1,1])
                with pcol3:
                    qty = st.number_input("Quantidade", 0.0, 1_000_000.0, 1.0, 0.1, key="add_qty")
                with pcol4:
                    unit_price = st.number_input("Preço unitário", 0.0, 1_000_000.0, 0.0, 0.01, key="add_up")
                with pcol5:
                    discount = st.number_input("Desconto (valor)", 0.0, 1_000_000.0, 0.0, 0.01, key="add_desc")

                pcol6, pcol7 = st.columns([2,1])
                with pcol6:
                    lote = st.text_input("Lote (opcional)", key="add_lote")
                with pcol7:
                    exp_flag = st.checkbox("Tem validade?", value=False, key="add_has_exp")
                expiry = None
                if exp_flag:
                    expiry = st.date_input("Validade", value=None, key="add_exp")

                add_ok = st.form_submit_button("Adicionar item")
            if add_ok and prod and unit and qty > 0:
                total = (qty * unit_price) - discount
                st.session_state["compra_itens"].append({
                    "product_id": prod[0], "product_name": prod[1],
                    "unit_id": unit[0], "unit_abbr": unit[1],
                    "qty": float(qty), "unit_price": float(unit_price), "discount": float(discount),
                    "total": float(total), "lot_number": (lote or None),
                    "expiry_date": (str(expiry) if expiry else None)
                })
                st.success("Item adicionado.")

        df = pd.DataFrame(st.session_state["compra_itens"]) if st.session_state["compra_itens"] else pd.DataFrame(
            columns=["product_name","qty","unit_abbr","unit_price","discount","total","expiry_date"]
        )
        st.dataframe(df, use_container_width=True, hide_index=True)
        total_doc = float(df["total"].sum()) if not df.empty else 0.0
        st.markdown(f"**Total de itens:** {money(total_doc)}")

        cbot1, cbot2, cbot3 = st.columns(3)
        with cbot1:
            limpar = st.button("🧹 Limpar itens")
        with cbot2:
            salvar = st.button("💾 Salvar (status: LANÇADA)")
        with cbot3:
            salvar_postar = st.button("🚀 Salvar e POSTAR no estoque")

        if limpar:
            st.session_state["compra_itens"] = []
            st.info("Lista de itens limpa.")
            card_end()
            return

        if (salvar or salvar_postar):
            if not supplier:
                st.error("Selecione o fornecedor.")
                card_end(); return
            if df.empty:
                st.error("Inclua ao menos 1 item.")
                card_end(); return

            pid = supplier[0]
            row = qone("""
                insert into resto.purchase(supplier_id, doc_number, cfop_entrada, doc_date, freight_value, other_costs, total, status)
                values (%s,%s,%s,%s,%s,%s,%s,'LANÇADA')
                returning id;
            """, (int(pid), doc_number, cfop_ent, doc_date, float(freight), float(other), float(total_doc)))
            purchase_id = row["id"]

            # itens
            for it in st.session_state["compra_itens"]:
                qone("""
                    insert into resto.purchase_item(
                      purchase_id, product_id, qty, unit_id, unit_price, discount, total, lot_number, expiry_date
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    returning id;
                """, (int(purchase_id), int(it["product_id"]), float(it["qty"]), int(it["unit_id"]),
                      float(it["unit_price"]), float(it["discount"]), float(it["total"]),
                      (it["lot_number"] or None), (it["expiry_date"] or None)))

            if salvar_postar:
                try:
                    _post_purchase(purchase_id)
                except Exception:
                    st.error("Falha ao postar no estoque (verifique a função resto.sp_register_movement).")
                    card_end(); return

            st.session_state["compra_itens"] = []
            st.success(f"Compra #{purchase_id} salva" + (" e **POSTADA** no estoque." if salvar_postar else "."))
            _rerun()

        card_end()

    # ============================== Aba: Gerenciar compras ==============================
    with tabs[1]:
        card_start()
        st.subheader("Pesquisar")

        g1, g2, g3, g4 = st.columns([1.2,1,1,1])
        with g1:
            dt_ini = st.date_input("De", value=date.today().replace(day=1))
        with g2:
            dt_fim = st.date_input("Até", value=date.today())
        with g3:
            status_sel = st.multiselect("Status", ["RASCUNHO","LANÇADA","POSTADA","ESTORNADA","CANCELADA"],
                                        default=["LANÇADA","POSTADA","RASCUNHO"])
        with g4:
            sup_f = st.selectbox("Fornecedor (filtro)",
                                 options=[(0,"— todos —")] + [(s['id'], s['name']) for s in suppliers],
                                 format_func=lambda x: x[1])

        where = ["p.doc_date between %s and %s"]
        params = [dt_ini, dt_fim]
        if status_sel:
            where.append("p.status = any(%s)")
            params.append(status_sel)
        if sup_f and isinstance(sup_f, tuple) and sup_f[0] != 0:
            where.append("p.supplier_id = %s")
            params.append(int(sup_f[0]))

        sql_list = f"""
            select p.id, p.doc_date, p.doc_number, p.status, p.total, s.name as supplier
              from resto.purchase p
              join resto.supplier s on s.id = p.supplier_id
             where {' and '.join(where)}
             order by p.doc_date desc, p.id desc
             limit 500;
        """
        rows = qall(sql_list, tuple(params)) or []
        df_list = pd.DataFrame(rows)
        if df_list.empty:
            st.caption("Nenhuma compra encontrada para os filtros.")
            card_end(); return

        # seleção
        sel = st.selectbox("Selecione a compra", options=[(r["id"], f"#{r['id']} • {r['doc_date']} • {r['supplier']} • {r['status']} • {money(r['total'])}") for r in rows],
                           format_func=lambda x: x[1] if isinstance(x, tuple) else x)
        if not sel:
            card_end(); return

        sel_id = int(sel[0])
        head = qone("""
            select p.*, s.name as supplier_name
              from resto.purchase p
              join resto.supplier s on s.id = p.supplier_id
             where p.id=%s;
        """, (sel_id,))
        itens = qall("""
            select pi.id, pi.product_id, pr.name as produto, pi.qty, pi.unit_price, pi.discount, pi.total,
                   pi.lot_number, pi.expiry_date, u.abbr as un
              from resto.purchase_item pi
              join resto.product pr on pr.id = pi.product_id
              left join resto.unit u on u.id = pi.unit_id
             where pi.purchase_id=%s
             order by pi.id;
        """, (sel_id,)) or []

        st.markdown(f"### Compra #{sel_id} – {head['supplier_name']} • **{head['status']}**")

        # --------- Header editável (se não POSTADA nem ESTORNADA/CANCELADA)
        can_edit = head["status"] in ("RASCUNHO","LANÇADA")
        with st.form(f"form_head_{sel_id}"):
            hc1, hc2, hc3 = st.columns([2,1,1])
            with hc1:
                sup2 = st.selectbox("Fornecedor *", options=sup_opts,
                                    index=next((i for i, t in enumerate(sup_opts) if t[0]==head["supplier_id"]), 0),
                                    disabled=not can_edit, key=f"edit_sup_{sel_id}")
                doc2 = st.text_input("Número do documento", value=head.get("doc_number") or "", disabled=not can_edit, key=f"edit_doc_{sel_id}")
            with hc2:
                cf2 = st.text_input("CFOP Entrada", value=head.get("cfop_entrada") or "", disabled=not can_edit, key=f"edit_cfop_{sel_id}")
                dt2 = st.date_input("Data", value=head.get("doc_date"), disabled=not can_edit, key=f"edit_date_{sel_id}")
            with hc3:
                fr2 = st.number_input("Frete", 0.0, 9999999.99, float(head.get("freight_value") or 0.0), 0.01, disabled=not can_edit, key=f"edit_frete_{sel_id}")
                ot2 = st.number_input("Outros custos", 0.0, 9999999.99, float(head.get("other_costs") or 0.0), 0.01, disabled=not can_edit, key=f"edit_outros_{sel_id}")
            btn_upd_head = st.form_submit_button("💾 Salvar cabeçalho", disabled=not can_edit)

        if btn_upd_head and can_edit:
            qexec("""
                update resto.purchase
                   set supplier_id=%s, doc_number=%s, cfop_entrada=%s, doc_date=%s,
                       freight_value=%s, other_costs=%s
                 where id=%s;
            """, (int(sup2[0]), doc2, cf2, dt2, float(fr2), float(ot2), sel_id))
            st.success("Cabeçalho atualizado.")
            _rerun()

        # --------- Itens (data_editor) – somente se pode editar
        df_it = pd.DataFrame(itens)
        if not df_it.empty:
            df_it["Excluir?"] = False
            colcfg = {
                "id":          st.column_config.NumberColumn("ID", disabled=True),
                "produto":     st.column_config.TextColumn("Produto", disabled=True),
                "un":          st.column_config.TextColumn("Un", disabled=True),
                "qty":         st.column_config.NumberColumn("Qtd", step=0.001, format="%.3f", disabled=not can_edit),
                "unit_price":  st.column_config.NumberColumn("Preço", step=0.01, format="%.4f", disabled=not can_edit),
                "discount":    st.column_config.NumberColumn("Desc", step=0.01, format="%.2f", disabled=not can_edit),
                "total":       st.column_config.NumberColumn("Total", step=0.01, format="%.2f", disabled=True),
                "lot_number":  st.column_config.TextColumn("Lote", disabled=not can_edit),
                "expiry_date": st.column_config.DateColumn("Validade", disabled=not can_edit),
                "Excluir?":    st.column_config.CheckboxColumn("Excluir?", help="Marque para remover", disabled=not can_edit),
            }
            edited = st.data_editor(
                df_it[["id","produto","un","qty","unit_price","discount","total","lot_number","expiry_date","Excluir?"]],
                column_config=colcfg,
                hide_index=True,
                num_rows="fixed",
                key=f"it_editor_{sel_id}",
                use_container_width=True
            )

            ia, ib, ic, id_ = st.columns(4)
            with ia:
                apply_items = st.button("💾 Salvar alterações (itens)", disabled=not can_edit, key=f"btn_apply_items_{sel_id}")
            with ib:
                with st.popover("➕ Adicionar item", disabled=not can_edit):
                    with st.form(f"form_add_item_exist_{sel_id}"):
                        nprod = st.selectbox("Produto", options=prod_opts,
                                             key=f"add_prod_exist_{sel_id}",
                                             format_func=lambda x: x[1] if isinstance(x, tuple) else x)
                        nunit = st.selectbox("Unidade", options=unit_opts,
                                             key=f"add_unit_exist_{sel_id}",
                                             format_func=lambda x: x[1] if isinstance(x, tuple) else x)
                        nqty  = st.number_input("Quantidade", 0.0, 1_000_000.0, 1.0, 0.1, key=f"add_qty_exist_{sel_id}")
                        nup   = st.number_input("Preço unitário", 0.0, 1_000_000.0, 0.0, 0.01, key=f"add_up_exist_{sel_id}")
                        ndesc = st.number_input("Desconto (valor)", 0.0, 1_000_000.0, 0.0, 0.01, key=f"add_desc_exist_{sel_id}")
                        nlote = st.text_input("Lote (opcional)", key=f"add_lote_exist_{sel_id}")
                        nexpF = st.checkbox("Tem validade?", value=False, key=f"add_hasexp_exist_{sel_id}")
                        nexp  = st.date_input("Validade", value=None, key=f"add_exp_exist_{sel_id}") if nexpF else None
                        nadd  = st.form_submit_button("Incluir")
                    if nadd and nprod and nunit and nqty > 0:
                        ntotal = (nqty * nup) - ndesc
                        qexec("""
                            insert into resto.purchase_item
                                (purchase_id, product_id, qty, unit_id, unit_price, discount, total, lot_number, expiry_date)
                            values (%s,%s,%s,%s,%s,%s,%s,%s,%s);
                        """, (sel_id, int(nprod[0]), float(nqty), int(nunit[0]),
                              float(nup), float(ndesc), float(ntotal), (nlote or None), (str(nexp) if nexpF and nexp else None)))
                        st.success("Item incluído.")
                        _rerun()
            with ic:
                post_btn = st.button("📦 Postar no estoque", disabled=(head["status"]=="POSTADA"), key=f"btn_post_{sel_id}")
            with id_:
                est_btn = st.button("↩️ Estornar", disabled=(head["status"]!="POSTADA"), key=f"btn_est_{sel_id}")

            if apply_items and can_edit:
                # comparar e aplicar
                orig = df_it.set_index("id")
                new  = edited.set_index("id")

                upd = 0; delc = 0; err = 0
                # deletar
                to_del = new.index[new["Excluir?"] == True].tolist()
                for iid in to_del:
                    try:
                        qexec("delete from resto.purchase_item where id=%s;", (int(iid),))
                        delc += 1
                    except Exception:
                        err += 1

                # atualizar (recalcula total)
                keep_ids = [i for i in new.index if i not in to_del]
                for iid in keep_ids:
                    a = orig.loc[iid]; b = new.loc[iid]
                    changed = any(str(a.get(f,"")) != str(b.get(f,"")) for f in
                                  ["qty","unit_price","discount","lot_number","expiry_date"])
                    if not changed:
                        continue
                    try:
                        new_total = float(b.get("qty") or 0)*float(b.get("unit_price") or 0) - float(b.get("discount") or 0)
                        qexec("""
                            update resto.purchase_item
                               set qty=%s, unit_price=%s, discount=%s, total=%s, lot_number=%s, expiry_date=%s
                             where id=%s;
                        """, (float(b.get("qty") or 0), float(b.get("unit_price") or 0),
                              float(b.get("discount") or 0), float(new_total),
                              (b.get("lot_number") or None), (str(b.get("expiry_date")) if b.get("expiry_date") else None),
                              int(iid)))
                        upd += 1
                    except Exception:
                        err += 1

                # atualiza total do cabeçalho
                new_total_doc = qone("select coalesce(sum(total),0) s from resto.purchase_item where purchase_id=%s;", (sel_id,))["s"]
                qexec("update resto.purchase set total=%s where id=%s;", (float(new_total_doc or 0), sel_id))

                st.success(f"Itens: ✅ {upd} atualizado(s) • 🗑️ {delc} removido(s) • ⚠️ {err} erro(s).")
                _rerun()

            if post_btn and head["status"] != "POSTADA":
                try:
                    _post_purchase(sel_id)
                    st.success("Compra postada no estoque.")
                    _rerun()
                except Exception:
                    st.error("Não foi possível postar. Verifique a função `resto.sp_register_movement`.")

            if est_btn and head["status"] == "POSTADA":
                _unpost_purchase(sel_id)
                st.success("Compra estornada (estoque revertido).")
                _rerun()

        # --------- Excluir / Cancelar
        st.divider()
        d1, d2 = st.columns(2)
        with d1:
            can_delete = head["status"] in ("RASCUNHO","LANÇADA")
            del_btn = st.button("🗑️ Excluir compra (não postada)", disabled=not can_delete, type="secondary", key=f"btn_del_{sel_id}")
        with d2:
            can_cancel = head["status"] in ("RASCUNHO","LANÇADA")
            cancel_btn = st.button("🚫 Cancelar (mantém registro, sem afetar estoque)", disabled=not can_cancel, key=f"btn_cancel_{sel_id}")

        if del_btn and can_delete:
            qexec("delete from resto.purchase where id=%s;", (sel_id,))
            st.success("Compra excluída.")
            _rerun()

        if cancel_btn and can_cancel:
            qexec("update resto.purchase set status='CANCELADA' where id=%s;", (sel_id,))
            st.info("Compra cancelada.")
            _rerun()

        card_end()


#=====================================VENDAS =================================================================================
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

# ===================== PRECIFICAÇÃO =====================
def page_receitas_precos():
    import pandas as pd

    # Helpers
    def _rerun():
        try:
            st.rerun()
        except Exception:
            if hasattr(st, "experimental_rerun"):
                st.experimental_rerun()

    def _money(x):
        try:
            return money(x)
        except Exception:
            try:
                v = float(x or 0)
            except Exception:
                v = 0.0
            s = f"R$ {v:,.2f}"
            # formata pt-BR
            return s.replace(",", "X").replace(".", ",").replace("X", ".")

    def _has_yield_unit_col() -> bool:
        r = qone("""
            select exists (
                select 1 from information_schema.columns
                 where table_schema='resto' and table_name='recipe' and column_name='yield_unit_id'
            ) as ok;
        """)
        return bool(r and r.get("ok"))

    def _recipe_cost(product_id: int):
        """Calcula custo estimado pelo last_cost dos ingredientes + overhead + perdas.
           Retorna dict {'unit_cost': float, 'batch_cost': float, 'yield_qty': float, 'yield_unit': 'abbr' ou None}
           ou None se não houver ficha técnica/ingredientes."""
        recipe = qone("select * from resto.recipe where product_id=%s;", (product_id,))
        if not recipe:
            return None

        items = qall("""
            select ri.qty, coalesce(ri.conversion_factor,1) as conv, coalesce(p.last_cost,0) as last_cost
              from resto.recipe_item ri
              join resto.product p on p.id = ri.ingredient_id
             where ri.recipe_id=%s;
        """, (recipe["id"],)) or []

        if not items:
            return None

        tot_ing = 0.0
        for it in items:
            try:
                tot_ing += float(it["qty"]) * float(it["conv"]) * float(it["last_cost"])
            except Exception:
                pass

        overhead = float(recipe.get("overhead_pct") or 0.0) / 100.0
        loss     = float(recipe.get("loss_pct") or 0.0) / 100.0
        yq       = float(recipe.get("yield_qty") or 0.0)
        if yq <= 0:
            return None

        batch = tot_ing * (1 + overhead) * (1 + loss)
        unit  = batch / yq if yq > 0 else 0.0

        yabbr = None
        if _has_yield_unit_col():
            yu = recipe.get("yield_unit_id")
            if yu:
                r = qone("select abbr from resto.unit where id=%s;", (yu,))
                yabbr = r["abbr"] if r else None

        return {"unit_cost": unit, "batch_cost": batch, "yield_qty": yq, "yield_unit": yabbr}

    header("💲 Precificação", "Simule preços e margens a partir da ficha técnica (ou último custo).")
    tabs = st.tabs(["🧮 Simulador", "📊 Tabela de preços"])

    # Carrega produtos base
    prods = qall("select id, name, unit, category, last_cost, active from resto.product order by name;") or []
    if not prods:
        st.warning("Cadastre produtos em **Estoque → Cadastro** antes.")
        return

    # ==================== Aba: Simulador ====================
    with tabs[0]:
        card_start()
        st.subheader("Simulador de preço por produto")

        prod = st.selectbox(
            "Produto",
            options=[(p["id"], p["name"]) for p in prods],
            format_func=lambda x: x[1] if isinstance(x, tuple) else x,
            key="prec_prod"
        )
        if not prod:
            card_end()
            return

        pid = int(prod[0])
        prow = next((r for r in prods if r["id"] == pid), None) or {}
        est = _recipe_cost(pid)

        base_cost = None
        base_label = ""
        if est:
            base_cost = float(est["unit_cost"])
            ytxt = f"{est['yield_qty']:.3f} {est['yield_unit']}" if est.get("yield_unit") else f"{est['yield_qty']:.3f}"
            st.caption(f"Cálculo por ficha técnica • Rendimento: {ytxt} • Custo do lote: {_money(est['batch_cost'])}")
            base_label = "Custo unitário estimado (ficha técnica)"
        else:
            base_cost = float(prow.get("last_cost") or 0.0)
            base_label = "Custo base (last_cost do produto)"
            st.caption("Sem ficha técnica com ingredientes → usando last_cost do produto.")

        st.markdown(f"**{base_label}:** {_money(base_cost)}")

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            markup = st.number_input("Markup %", 0.0, 1000.0, 200.0, 0.1, format="%.2f")
        with c2:
            taxa = st.number_input("Taxas/Cartão %", 0.0, 30.0, 0.0, 0.1, format="%.2f")
        with c3:
            imp = st.number_input("Impostos %", 0.0, 50.0, 8.0, 0.1, format="%.2f",
                                  help="Estimativa de tributos sobre venda (ex.: Simples/ISS/PIS/COFINS).")
        with c4:
            desc = st.number_input("Desconto médio %", 0.0, 100.0, 0.0, 0.1, format="%.2f")

        preco_sugerido = base_cost * (1 + markup/100.0)
        receita_liq = preco_sugerido * (1 - desc/100.0) * (1 - taxa/100.0) * (1 - imp/100.0)
        margem_bruta = (receita_liq - base_cost) / preco_sugerido * 100 if preco_sugerido else 0.0

        st.markdown(
            f"**Preço sugerido:** {_money(preco_sugerido)}  \n"
            f"**Receita líquida estimada:** {_money(receita_liq)}  \n"
            f"**Margem bruta sobre preço sugerido:** {margem_bruta:.2f}%"
        )
        card_end()

    # ==================== Aba: Tabela de preços ====================
    with tabs[1]:
        card_start()
        st.subheader("Tabela de preços (em massa)")

        f1, f2, f3 = st.columns([2,1,1])
        with f1:
            f_cat = st.text_input("Filtrar por categoria (texto exato opcional)", key="prec_tab_cat")
        with f2:
            f_only_active = st.checkbox("Somente ativos", value=True, key="prec_tab_active")
        with f3:
            base_markup = st.number_input("Markup base %", 0.0, 1000.0, 200.0, 0.1, key="prec_tab_markup", format="%.2f")

        f4, f5 = st.columns(2)
        with f4:
            base_taxa = st.number_input("Taxas/Cartão %", 0.0, 30.0, 0.0, 0.1, key="prec_tab_taxa", format="%.2f")
        with f5:
            base_imp = st.number_input("Impostos %", 0.0, 50.0, 8.0, 0.1, key="prec_tab_imp", format="%.2f")

        # Monta dataframe
        rows = []
        for p in prods:
            if f_only_active and not p.get("active"):
                continue
            if f_cat.strip() and (str(p.get("category") or "") != f_cat.strip()):
                continue

            est = _recipe_cost(p["id"])
            if est:
                base_cost = float(est["unit_cost"])
            else:
                base_cost = float(p.get("last_cost") or 0.0)

            ps = base_cost * (1 + base_markup/100.0)
            rl = ps * (1 - base_taxa/100.0) * (1 - base_imp/100.0)
            mg = (rl - base_cost) / ps * 100 if ps else 0.0

            rows.append({
                "Produto": p["name"],
                "Categoria": p.get("category") or "",
                "Un": p.get("unit") or "",
                "Custo base": base_cost,
                "Preço sugerido": ps,
                "Margem bruta %": mg
            })

        df = pd.DataFrame(rows)
        if df.empty:
            st.caption("Nenhum produto para os filtros.")
            card_end()
            return

        # Exibição amigável
        df_show = df.copy()
        df_show["Custo base"] = df_show["Custo base"].map(_money)
        df_show["Preço sugerido"] = df_show["Preço sugerido"].map(_money)
        df_show["Margem bruta %"] = df_show["Margem bruta %"].map(lambda x: f"{x:.2f}%")

        st.dataframe(df_show, use_container_width=True, hide_index=True)
        card_end()

# ===================== PRODUÇÃO =====================
def page_producao():
    import pandas as pd
    from datetime import date

    # ---------------- helpers ----------------
    def _rerun():
        try:
            st.rerun()
        except Exception:
            if hasattr(st, "experimental_rerun"):
                st.experimental_rerun()

    def _has_yield_unit_col() -> bool:
        r = qone("""
            select exists (
                select 1
                  from information_schema.columns
                 where table_schema='resto'
                   and table_name='recipe'
                   and column_name='yield_unit_id'
            ) as x;
        """)
        return bool(r and r.get("x"))

    def _ensure_production_schema():
        # Cria tudo de forma defensiva (sem quebrar o que já existe)
        qexec("""
        do $$
        begin
          -- Unidades
          create table if not exists resto.unit (
              id    bigserial primary key,
              abbr  text not null unique,
              name  text
          );

          -- Receita (1:1 com produto)
          create table if not exists resto.recipe (
              id           bigserial primary key,
              product_id   bigint not null references resto.product(id) on delete cascade,
              yield_qty    numeric(18,6) not null default 1,
              overhead_pct numeric(9,4)  not null default 0,
              loss_pct     numeric(9,4)  not null default 0,
              note         text,
              created_at   timestamptz not null default now(),
              updated_at   timestamptz not null default now()
          );
          create unique index if not exists recipe_uq_product on resto.recipe(product_id);

          -- Se não existir yield_unit_id, adiciona (sem NOT NULL)
          if not exists (
              select 1 from information_schema.columns
               where table_schema='resto' and table_name='recipe' and column_name='yield_unit_id'
          ) then
              alter table resto.recipe add column yield_unit_id bigint references resto.unit(id);
          end if;

          -- Itens da receita (ingredientes)
          create table if not exists resto.recipe_item (
              id                bigserial primary key,
              recipe_id         bigint not null references resto.recipe(id) on delete cascade,
              ingredient_id     bigint not null references resto.product(id),
              qty               numeric(14,3) not null,
              unit_id           bigint,
              conversion_factor numeric(14,6) default 1.0,
              note              text
          );
          create index if not exists recipe_item_recipe_idx on resto.recipe_item(recipe_id);

          -- Produção (ordem de produção)
          create table if not exists resto.production (
              id          bigserial primary key,
              date        timestamptz default now(),
              product_id  bigint not null references resto.product(id),
              qty         numeric(14,3) not null,
              unit_cost   numeric(14,4) not null,
              total_cost  numeric(14,2) not null,
              lot_number  text,
              expiry_date date,
              note        text
          );

          -- Consumo por produção (rastreamento)
          create table if not exists resto.production_item (
              id             bigserial primary key,
              production_id  bigint not null references resto.production(id) on delete cascade,
              ingredient_id  bigint not null references resto.product(id),
              lot_id         bigint,
              qty            numeric(14,3) not null,
              unit_cost      numeric(14,4) not null,
              total_cost     numeric(14,2) not null
          );
          create index if not exists prod_item_prod_idx on resto.production_item(production_id);

          -- Campos auxiliares nos produtos
          alter table resto.product add column if not exists unit      text default 'un';
          alter table resto.product add column if not exists last_cost numeric(14,2) default 0;
          alter table resto.product add column if not exists active    boolean default true;
        end $$;
        """)

        # Semeia unidades básicas sem estourar UniqueViolation
        for abbr, name in [("un","Unidade"),("kg","Quilo"),("g","Grama"),
                           ("L","Litro"),("ml","Mililitro"),("cx","Caixa"),("pct","Pacote")]:
            qexec("insert into resto.unit(abbr,name) values (%s,%s) on conflict do nothing;", (abbr, name))

    _ensure_production_schema()

    header("🍳 Produção", "Ordem de produção: consome ingredientes (por lote) e gera produto final.")

    # Produtos ativos p/ seleção
    prods = qall("select id, name, unit, last_cost from resto.product where active is true order by name;") or []
    if not prods:
        st.warning("Cadastre produtos em **Estoque → Cadastro** antes.")
        return
    prod_opts = [(p["id"], p["name"]) for p in prods]

    # Seleção do produto final (compartilhada entre as abas)
    prod = st.selectbox(
        "Produto final *",
        options=prod_opts,
        key="producao_prod_sel",
        format_func=lambda x: x[1] if isinstance(x, tuple) else x
    )
    if not prod:
        return
    prod_id = prod[0]
    prod_row = next((r for r in prods if r["id"] == prod_id), {"unit": "un", "last_cost": 0})

    tabs = st.tabs(["🛠️ Nova produção", "📜 Ficha técnica (receita)"])

    # ==================== Aba Ficha Técnica (FORMULÁRIO ÚNICO) ====================
    with tabs[1]:
        card_start()
        st.subheader("📄 Ficha técnica do produto — cadastro simplificado")

        has_yield_unit = _has_yield_unit_col()

        # dados base
        units_rows = qall("select id, abbr from resto.unit order by abbr;") or []
        abbr_by_id = {u["id"]: u["abbr"] for u in units_rows}
        id_by_abbr = {u["abbr"]: u["id"] for u in units_rows}

        # ingredientes possíveis (exceto o próprio produto final)
        ing_all = [(p["id"], p["name"]) for p in prods if p["id"] != prod_id]
        name_by_id = {i: n for i, n in ing_all}
        id_by_name = {n: i for i, n in ing_all}

        # receita atual (se houver)
        recipe = qone("select * from resto.recipe where product_id=%s;", (prod_id,))
        init_yield = float(recipe.get("yield_qty") if recipe else 1.0)
        init_over  = float(recipe.get("overhead_pct") if recipe else 0.0)
        init_loss  = float(recipe.get("loss_pct") if recipe else 0.0)
        init_note  = (recipe.get("note") if recipe else "") or ""
        init_yunit_id = recipe.get("yield_unit_id") if (recipe and has_yield_unit) else None

        # ingredientes atuais
        ing_rows = []
        if recipe:
            ing_rows = qall("""
                select ri.id, p.name as ingrediente, ri.qty, ri.unit_id,
                       coalesce(ri.conversion_factor, 1.0) as conversion_factor
                  from resto.recipe_item ri
                  join resto.product p on p.id = ri.ingredient_id
                 where ri.recipe_id=%s
                 order by p.name;
            """, (recipe["id"],)) or []

        def _df_base():
            if ing_rows:
                df = pd.DataFrame(ing_rows)
                df["Un"] = df["unit_id"].map(abbr_by_id).fillna("")
                df["Excluir?"] = False
                return df[["id", "ingrediente", "qty", "Un", "conversion_factor", "Excluir?"]]
            return pd.DataFrame([{
                "id": None, "ingrediente": "", "qty": 0.0, "Un": "", "conversion_factor": 1.0, "Excluir?": False
            }], columns=["id","ingrediente","qty","Un","conversion_factor","Excluir?"])

        df_grid = _df_base()

        # ---------- FORMULÁRIO ÚNICO ----------
        with st.form(f"form_recipe_full_{prod_id}"):
            col1, col2, col3, col4 = st.columns([1.2, 1, 1, 2])
            with col1:
                r_yield = st.number_input("Rendimento (quantidade)", min_value=0.001, step=0.001,
                                          value=float(init_yield), format="%.3f")
            with col2:
                r_over  = st.number_input("Overhead (%)", min_value=0.0, step=0.5,
                                          value=float(init_over),  format="%.2f")
            with col3:
                r_loss  = st.number_input("Perdas (%)",   min_value=0.0, step=0.5,
                                          value=float(init_loss),  format="%.2f")
            with col4:
                r_unit_sel = None
                if has_yield_unit:
                    opts_units = [(u["id"], u["abbr"]) for u in units_rows]
                    try:
                        idx_u = [i for i, (uid, _) in enumerate(opts_units) if uid == init_yunit_id][0] if init_yunit_id else 0
                    except Exception:
                        idx_u = 0
                    r_unit_sel = st.selectbox("Unidade do rendimento", options=opts_units, index=idx_u,
                                              format_func=lambda x: x[1] if isinstance(x, tuple) else x)

            note = st.text_area("Observações (opcional)", value=(init_note or ""), height=70)

            st.markdown("#### Ingredientes (adicione/edite/remova no grid)")
            cfg_ing = {
                "id":  st.column_config.TextColumn("ID", disabled=True),
                "ingrediente": st.column_config.SelectboxColumn(
                    "Ingrediente", options=[n for _, n in ing_all]
                ),
                "qty": st.column_config.NumberColumn("Qtd", step=0.001, format="%.3f"),
                "Un":  st.column_config.SelectboxColumn("Un.", options=[""] + [u["abbr"] for u in units_rows]),
                "conversion_factor": st.column_config.NumberColumn("Fator conv.", step=0.01, format="%.2f"),
                "Excluir?": st.column_config.CheckboxColumn("Excluir?"),
            }
            edited = st.data_editor(
                df_grid,
                column_config=cfg_ing,
                hide_index=True,
                num_rows="dynamic",
                use_container_width=True,
                key=f"ft_editor_{prod_id}"
            )

            salvar = st.form_submit_button("💾 Salvar ficha técnica")

        # ---------- SALVAMENTO (sem return/st.stop) ----------
        if salvar:
            if has_yield_unit and not r_unit_sel:
                st.error("Selecione a **unidade do rendimento**.")
            else:
                # cria/atualiza recipe
                if not recipe:
                    if has_yield_unit:
                        recipe = qone("""
                            insert into resto.recipe(product_id, yield_qty, yield_unit_id, overhead_pct, loss_pct, note)
                            values (%s,%s,%s,%s,%s,%s)
                            returning *;
                        """, (prod_id, float(r_yield), int(r_unit_sel[0]), float(r_over), float(r_loss), (note or None)))
                    else:
                        recipe = qone("""
                            insert into resto.recipe(product_id, yield_qty, overhead_pct, loss_pct, note)
                            values (%s,%s,%s,%s,%s)
                            returning *;
                        """, (prod_id, float(r_yield), float(r_over), float(r_loss), (note or None)))
                else:
                    if has_yield_unit:
                        qexec("""
                            update resto.recipe
                               set yield_qty=%s, yield_unit_id=%s, overhead_pct=%s, loss_pct=%s, note=%s, updated_at=now()
                             where id=%s;
                        """, (float(r_yield), int(r_unit_sel[0]) if r_unit_sel else recipe.get("yield_unit_id"),
                              float(r_over), float(r_loss), (note or None), int(recipe["id"])))
                    else:
                        qexec("""
                            update resto.recipe
                               set yield_qty=%s, overhead_pct=%s, loss_pct=%s, note=%s, updated_at=now()
                             where id=%s;
                        """, (float(r_yield), float(r_over), float(r_loss), (note or None), int(recipe["id"])))

                rid = int(recipe["id"])

                # normaliza grid
                if edited is None or edited.empty:
                    edited = pd.DataFrame(columns=["id","ingrediente","qty","Un","conversion_factor","Excluir?"])
                edited = edited.fillna({"id":"", "ingrediente":"", "qty":0.0, "Un":"", "conversion_factor":1.0, "Excluir?":False})

                # itens originais (p/ detectar removidos por “sumir do grid”)
                df_orig = _df_base()
                orig_ids = set([int(i) for i in df_orig["id"].dropna().tolist() if str(i).strip() != ""])
                new_ids  = set([int(i) for i in edited["id"].tolist() if str(i).strip().isdigit()])
                missing_ids = orig_ids - new_ids
                for _id in missing_ids:
                    try:
                        qexec("delete from resto.recipe_item where id=%s;", (int(_id),))
                    except Exception:
                        pass

                # insere/atualiza/deleta marcados
                upd = ins = dele = err = 0
                for _, row in edited.iterrows():
                    _id   = str(row.get("id") or "").strip()
                    nome  = (row.get("ingrediente") or "").strip()
                    qty   = float(row.get("qty") or 0.0)
                    unabr = (row.get("Un") or "").strip()
                    conv  = float(row.get("conversion_factor") or 1.0)
                    ex    = bool(row.get("Excluir?") or False)

                    if not _id and not nome:
                        continue

                    ing_id = id_by_name.get(nome)
                    unit_id = id_by_abbr.get(unabr) if unabr else None

                    if _id and ex:
                        try:
                            qexec("delete from resto.recipe_item where id=%s;", (int(_id),))
                            dele += 1
                        except Exception:
                            err += 1
                        continue

                    if not nome or qty <= 0:
                        continue

                    try:
                        if _id:
                            qexec("""
                                update resto.recipe_item
                                   set ingredient_id=%s, qty=%s, unit_id=%s, conversion_factor=%s
                                 where id=%s;
                            """, (int(ing_id), float(qty), unit_id, float(conv), int(_id)))
                            upd += 1
                        else:
                            qexec("""
                                insert into resto.recipe_item(recipe_id, ingredient_id, qty, unit_id, conversion_factor)
                                values (%s,%s,%s,%s,%s);
                            """, (rid, int(ing_id), float(qty), unit_id, float(conv)))
                            ins += 1
                    except Exception:
                        err += 1

                st.success(f"Ficha técnica salva. ✅ {upd} atualizado(s) • ➕ {ins} incluído(s) • 🗑️ {dele} removido(s) • ⚠️ {err} erro(s).")
                _rerun()

me mande a aba ja alterada por favor
        # ---------- Custos detalhados por ingrediente (explicativo, com conversão de unidade) ----------
        if recipe:
            rows = qall("""
                select ri.qty,
                       coalesce(ri.conversion_factor,1) as conv,
                       ri.unit_id,
                       p.name as ingrediente,
                       coalesce(p.last_cost,0) as last_cost,
                       p.unit as prod_unit
                  from resto.recipe_item ri
                  join resto.product p on p.id = ri.ingredient_id
                 where ri.recipe_id=%s
                 order by p.name;
            """, (recipe["id"],)) or []

            import pandas as pd
            df_det = pd.DataFrame(rows)

            # util: converte quantidades entre abreviações conhecidas
            def _convert_qty(q, from_abbr, to_abbr):
                fa = (from_abbr or "").strip()
                ta = (to_abbr or "").strip()
                q = float(q or 0)
                if not fa or not ta or fa == ta:
                    return q, False
                pairs = {
                    ("g","kg"): 1/1000, ("kg","g"): 1000,
                    ("ml","L"): 1/1000, ("L","ml"): 1000,
                }
                factor = pairs.get((fa, ta))
                if factor is None:
                    # não conversível conhecido -> assume 1:1
                    return q, False
                return q * factor, True

            if not df_det.empty:
                # unidade cadastrada no item da receita
                df_det["Un"] = df_det["unit_id"].map(lambda x: abbr_by_id.get(x, "") if x is not None else "")
                # quantidade efetiva (com fator)
                df_det["qty_eff"] = df_det["qty"].astype(float) * df_det["conv"].astype(float)

                subtot = []
                calc_txt = []

                def _fmt_qty(val):
                    s = f"{float(val):,.3f}"
                    return s.replace(",", "X").replace(".", ",").replace("X", ".")

                for _, r in df_det.iterrows():
                    un_item = (r.get("Un") or "").strip()           # ex.: g, kg, ml, L, un
                    un_cost = (r.get("prod_unit") or "").strip()    # unidade-base do custo do produto (p.unit)
                    qeff = float(r["qty_eff"])
                    last_cost = float(r["last_cost"])

                    q_in_cost, changed = _convert_qty(qeff, un_item, un_cost)
                    subtotal = q_in_cost * last_cost
                    subtot.append(subtotal)

                    if changed:
                        calc = f"{_fmt_qty(qeff)} {un_item or un_cost} → {_fmt_qty(q_in_cost)} {un_cost} × {money(last_cost)} = {money(subtotal)}"
                    else:
                        # se não mudou, mostra direto na unidade de custo (ou na do item, se não houver)
                        base_un = un_cost or un_item
                        calc = f"{_fmt_qty(q_in_cost)} {base_un} × {money(last_cost)} = {money(subtotal)}"
                    calc_txt.append(calc)

                df_det["subtotal"] = subtot
                df_det["Cálculo"] = calc_txt

                # Tabela amigável
                df_view = pd.DataFrame({
                    "Ingrediente": df_det["ingrediente"],
                    "Qtd": df_det["qty"].astype(float),
                    "Un": df_det["Un"],
                    "Fator": df_det["conv"].astype(float),
                    "Custo último (R$)": df_det["last_cost"].astype(float),
                    "Qtd efetiva": df_det["qty_eff"].astype(float),
                    "Subtotal (R$)": df_det["subtotal"].astype(float),
                    "Cálculo": df_det["Cálculo"],
                })

                st.markdown("### 📊 Custos por ingrediente (explicado)")
                st.dataframe(
                    df_view,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Qtd": st.column_config.NumberColumn("Qtd", format="%.3f"),
                        "Fator": st.column_config.NumberColumn("Fator", format="%.2f"),
                        "Custo último (R$)": st.column_config.NumberColumn("Custo último (R$)", format="%.2f"),
                        "Qtd efetiva": st.column_config.NumberColumn("Qtd efetiva", format="%.3f"),
                        "Subtotal (R$)": st.column_config.NumberColumn("Subtotal (R$)", format="%.2f"),
                    }
                )

                # Totais e explicação do lote (mesma lógica, agora com subtotais corretos)
                tot_ing = float(df_det["subtotal"].sum())
                over_pct = float(recipe.get("overhead_pct") or 0.0) / 100.0
                loss_pct = float(recipe.get("loss_pct") or 0.0) / 100.0

                over_val = tot_ing * over_pct
                loss_val = (tot_ing + over_val) * loss_pct
                batch_cost = tot_ing + over_val + loss_val

                yq = float(recipe.get("yield_qty") or 1.0)
                unit_cost = batch_cost / (yq if yq > 0 else 1.0)

                ytxt = f"{yq:.3f}"
                if has_yield_unit:
                    try:
                        r_u = qone("select abbr from resto.unit where id=%s;", (recipe.get("yield_unit_id"),))
                        yabbr = (r_u or {}).get("abbr") or ""
                        if yabbr:
                            ytxt = f"{yq:.3f} {yabbr}"
                    except Exception:
                        pass

                st.markdown("#### 🧮 Resumo do lote")
                c1, c2, c3, c4, c5 = st.columns(5)
                with c1: st.metric("Σ Ingredientes", money(tot_ing))
                with c2: st.metric(f"Overhead ({over_pct*100:.2f}%)", money(over_val))
                with c3: st.metric(f"Perdas ({loss_pct*100:.2f}%)", money(loss_val))
                with c4: st.metric("Custo do lote", money(batch_cost))
                with c5: st.metric("Custo unitário", money(unit_cost))

                with st.expander("Como calculamos? (passo a passo)", expanded=False):
                    st.markdown(
                        "- **Subtotal por item** = converte a `Qtd × Fator` para a unidade do **custo do produto** (p.unit) e multiplica por `Custo último`  \n"
                        f"- **Total ingredientes** = soma dos subtotais = **{money(tot_ing)}**  \n"
                        f"- **Overhead** = Total ingredientes × {over_pct*100:.2f}% = **{money(over_val)}**  \n"
                        f"- **Perdas** = (Total ingredientes + Overhead) × {loss_pct*100:.2f}% = **{money(loss_val)}**  \n"
                        f"- **Custo do lote** = Total ingredientes + Overhead + Perdas = **{money(batch_cost)}**  \n"
                        f"- **Custo unitário** = Custo do lote ÷ Rendimento ({ytxt}) = **{money(unit_cost)}**"
                    )


    # ==================== Aba Nova Produção ====================
    with tabs[0]:
        card_start()
        st.subheader("Nova produção")

        recipe = qone("select * from resto.recipe where product_id=%s;", (prod_id,))
        if not recipe:
            st.warning("Este produto não possui ficha técnica (receita). Cadastre na aba **Ficha técnica**.")
            card_end()
            return

        yield_qty = float(recipe.get("yield_qty") or 0.0)
        if yield_qty <= 0:
            st.error("Ficha técnica inválida: rendimento deve ser > 0.")
            card_end()
            return

        # Entrada de dados da OP
        with st.form(f"form_producao_{prod_id}"):
            qty_out = st.number_input(f"Quantidade a produzir ({prod_row.get('unit') or 'un'})",
                                      min_value=0.001, step=0.001, value=10.000, format="%.3f")
            c1, c2 = st.columns([1,1])
            with c1:
                lot_final = st.text_input("Lote do produto final (opcional)", value="")
            with c2:
                use_exp = st.checkbox("Definir validade", value=False, key=f"use_exp_{prod_id}")
            expiry_final = None
            if use_exp:
                expiry_final = st.date_input("Validade do produto final", value=date.today())
            ok = st.form_submit_button("✅ Produzir")

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
        """, (recipe["id"],)) or []

        if not ingredients:
            st.error("Ficha técnica sem ingredientes.")
            card_end()
            return

        # Escala e aloca por FIFO (usa sua função fifo_allocate no app)
        scale = float(qty_out) / yield_qty
        consumos = []   # [{ingredient_id, ingrediente, lot_id, qty, unit_cost, total}]
        total_ing_cost = 0.0
        faltantes = []

        for it in ingredients:
            need = float(it["qty"] or 0.0) * float(it.get("conversion_factor") or 1.0) * scale
            allocs = fifo_allocate(it["ingredient_id"], need)  # -> lista de {lot_id, qty, unit_cost}
            got = sum(a["qty"] for a in allocs)
            if got + 1e-9 < need:
                faltantes.append((it["ingrediente"], need, got))
            for a in allocs:
                total = float(a["qty"]) * float(a.get("unit_cost") or 0.0)
                consumos.append({
                    "ingredient_id": it["ingredient_id"],
                    "ingrediente": it["ingrediente"],
                    "lot_id": a["lot_id"],
                    "qty": float(a["qty"]),
                    "unit_cost": float(a.get("unit_cost") or 0.0),
                    "total": total
                })
                total_ing_cost += total

        if faltantes:
            msg = "Estoque insuficiente:\n" + "\n".join(
                f"- {n}: precisa {q:.3f}, alocado {al:.3f}" for n, q, al in faltantes
            )
            st.error(msg)
            card_end()
            return

        # Custo do lote
        overhead = float(recipe.get("overhead_pct") or 0.0) / 100.0
        loss = float(recipe.get("loss_pct") or 0.0) / 100.0
        batch_cost = total_ing_cost * (1 + overhead) * (1 + loss)
        unit_cost_est = batch_cost / float(qty_out)

        # Persiste produção
        prow = qone("""
            insert into resto.production(date, product_id, qty, unit_cost, total_cost, lot_number, expiry_date, note)
            values (now(), %s, %s, %s, %s, %s, %s, %s)
            returning id;
        """, (prod_id, float(qty_out), float(unit_cost_est), float(batch_cost),
              (lot_final or None), (str(expiry_final) if expiry_final else None), ""))
        production_id = prow["id"]

        # Saída dos ingredientes (OUT) por lote + rastreabilidade
        for c in consumos:
            pi = qone("""
                insert into resto.production_item(production_id, ingredient_id, lot_id, qty, unit_cost, total_cost)
                values (%s,%s,%s,%s,%s,%s)
                returning id;
            """, (production_id, c["ingredient_id"], c["lot_id"], c["qty"], c["unit_cost"], c["total"]))
            note = f"production:{production_id};lot:{c['lot_id']}"
            qexec("select resto.sp_register_movement(%s,'OUT',%s,%s,'production',%s,%s);",
                  (c["ingredient_id"], c["qty"], c["unit_cost"], pi["id"], note))

        # Entrada do produto final (IN)
        note_final = f"production:{production_id}" + (f";lot:{lot_final}" if lot_final else "")
        qexec("select resto.sp_register_movement(%s,'IN',%s,%s,'production',%s,%s);",
              (prod_id, float(qty_out), float(unit_cost_est), production_id, note_final))

        st.success(f"Produção #{production_id} registrada.")
        st.markdown(f"**Custo do lote:** {money(batch_cost)} • **Custo unitário aplicado (CMP):** {money(unit_cost_est)}")

        # Preview dos consumos
        if consumos:
            dfc = pd.DataFrame(consumos)[["ingrediente","lot_id","qty","unit_cost","total"]]
            st.dataframe(dfc, use_container_width=True, hide_index=True)

        card_end()



    
# ===================== ESTOQUE =====================
def page_estoque():
    import pandas as pd

    # --- helper de rerun (compatível)
    def _rerun():
        try:
            st.rerun()
        except Exception:
            if hasattr(st, "experimental_rerun"):
                st.experimental_rerun()

    # --- debug: mostra colunas da resto.product (ajuda quando der erro)
    def _debug_product_columns():
        cols = qall("""
            select column_name,
                   is_nullable,
                   column_default,
                   data_type
              from information_schema.columns
             where table_schema='resto' and table_name='product'
             order by ordinal_position;
        """) or []
        if cols:
            st.info("Diagnóstico da tabela resto.product (colunas / null / default / tipo):")
            st.dataframe(pd.DataFrame(cols), use_container_width=True, hide_index=True)

    # --- garante tabelas/colunas necessárias sem quebrar o que já existe
    def _ensure_inventory_schema():
        qexec("""
        do $$
        declare r record;
        begin
          -- tabela de fornecedores
          create table if not exists resto.supplier (
              id          bigserial primary key,
              name        text not null,
              created_at  timestamptz default now()
          );

          -- garante colunas necessárias
          alter table resto.supplier add column if not exists doc        varchar(32);
          alter table resto.supplier add column if not exists phone      text;
          alter table resto.supplier add column if not exists email      text;
          alter table resto.supplier add column if not exists note       text;
          alter table resto.supplier add column if not exists active     boolean default true;

          -- índices úteis
          create index if not exists supplier_name_idx on resto.supplier (lower(name));

          -- produto: garante campos mínimos
          alter table resto.product  add column if not exists unit        text default 'un';
          alter table resto.product  add column if not exists category    text;
          alter table resto.product  add column if not exists supplier_id bigint references resto.supplier(id);
          alter table resto.product  add column if not exists barcode     text;
          alter table resto.product  add column if not exists min_stock   numeric(14,3) default 0;
          alter table resto.product  add column if not exists last_cost   numeric(14,2) default 0;
          alter table resto.product  add column if not exists active      boolean default true;
          alter table resto.product  add column if not exists created_at  timestamptz default now();

          create index if not exists product_name_idx  on resto.product (lower(name));

          -- relaxa NOT NULL das colunas problemáticas conhecidas
          begin
            alter table resto.product alter column supplier_id drop not null;
          exception when others then null; end;

          begin
            alter table resto.product alter column category drop not null;
          exception when others then null; end;

          begin
            alter table resto.product alter column barcode drop not null;
          exception when others then null; end;

          -- relaxa NOT NULL de QUALQUER coluna extra que seja NOT NULL sem default (exceto as padrões e id)
          for r in
            select column_name
              from information_schema.columns
             where table_schema='resto'
               and table_name='product'
               and is_nullable='NO'
               and column_default is null
               and column_name not in ('id','name','unit','category','supplier_id',
                                       'barcode','min_stock','last_cost','active','created_at')
          loop
            begin
              execute format('alter table resto.product alter column %I drop not null', r.column_name);
            exception when others then null;
            end;
          end loop;
        end $$;
        """)

    # --- fornecedor padrão para evitar NOT NULL
    def _default_supplier_id():
        row = qone("select id from resto.supplier where lower(name)=lower(%s);", ("Fornecedor Padrão",))
        if row:
            return row["id"]
        row = qone("""
            insert into resto.supplier(name, active)
            values (%s, true)
            returning id;
        """, ("Fornecedor Padrão",))
        return row["id"]

    # garante esquema antes de qualquer consulta
    _ensure_inventory_schema()

    header("📦 Estoque", "Saldos, movimentos e lotes/validade.")
    tabs = st.tabs(["Saldos", "Movimentos", "Lotes & Validade", "Cadastro"])

    # ============ Aba: Saldos ============
    with tabs[0]:
        card_start()
        try:
            rows = qall("select * from resto.v_stock order by name;")
        except Exception:
            rows = []
        st.dataframe(pd.DataFrame(rows or []), use_container_width=True, hide_index=True)
        card_end()

    # ============ Aba: Movimentos ============
    with tabs[1]:
        card_start()
        st.subheader("Movimentações recentes")
        mv = qall("""
            select move_date, kind, product_id, qty, unit_cost, total_cost, reason, reference_id, note
              from resto.inventory_movement
          order by move_date desc
             limit 500;
        """)
        st.dataframe(pd.DataFrame(mv or []), use_container_width=True, hide_index=True)
        card_end()

    # ============ Aba: Lotes & Validade ============
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
        df = pd.DataFrame(rows or [])
        if not df.empty:
            df["dias_restantes"] = df["dias_restantes"].astype(int)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.caption("Nenhum lote dentro do período selecionado.")
        card_end()

    # ============ Aba: Cadastro (Insumos & Fornecedores) ============
    with tabs[3]:
        card_start()
        st.subheader("👤 Fornecedores")

        # ---- Formulário: novo fornecedor
        with st.form("form_new_supplier"):
            c1, c2 = st.columns([2,1])
            with c1:
                sp_name  = st.text_input("Nome do fornecedor *")
                sp_doc   = st.text_input("Documento (CNPJ/CPF)", value="")
                sp_email = st.text_input("E-mail", value="")
            with c2:
                sp_phone = st.text_input("Telefone", value="")
                sp_active = st.checkbox("Ativo", value=True)
            sp_note = st.text_area("Observações", value="", height=80)
            sp_submit = st.form_submit_button("➕ Adicionar fornecedor")
        if sp_submit:
            if not sp_name.strip():
                st.warning("Informe o nome do fornecedor.")
            else:
                qexec("""
                    insert into resto.supplier (name, doc, phone, email, note, active)
                    values (%s, %s, %s, %s, %s, %s);
                """, (sp_name.strip(), sp_doc or None, sp_phone or None, sp_email or None, sp_note or None, sp_active))
                st.success("Fornecedor adicionado.")
                _rerun()

        # ---- Lista/edição de fornecedores
        try:
            sup = qall("select id, name, doc, phone, email, active from resto.supplier order by name;") or []
        except Exception:
            _ensure_inventory_schema()
            sup = qall("select id, name, doc, phone, email, active from resto.supplier order by name;") or []
        df_sup = pd.DataFrame(sup)
        if not df_sup.empty:
            df_sup["Excluir?"] = False
            cfg_sup = {
                "id":     st.column_config.NumberColumn("ID", disabled=True),
                "name":   st.column_config.TextColumn("Nome"),
                "doc":    st.column_config.TextColumn("Documento"),
                "phone":  st.column_config.TextColumn("Telefone"),
                "email":  st.column_config.TextColumn("E-mail"),
                "active": st.column_config.CheckboxColumn("Ativo"),
                "Excluir?": st.column_config.CheckboxColumn("Excluir?", help="Marca para excluir"),
            }
            edited_sup = st.data_editor(
                df_sup,
                column_config=cfg_sup,
                hide_index=True,
                num_rows="fixed",
                key="sup_editor",
                use_container_width=True
            )

            colA, colB = st.columns(2)
            with colA:
                apply_sup = st.button("💾 Salvar alterações (fornecedores)")
            with colB:
                refresh_sup = st.button("🔄 Atualizar lista")

            if refresh_sup:
                _rerun()

            if apply_sup:
                orig = df_sup.set_index("id")
                new  = edited_sup.set_index("id")
                upd = 0; delc = 0; err = 0

                # exclusões
                to_del = new.index[new["Excluir?"] == True].tolist()
                for sid in to_del:
                    try:
                        qexec("delete from resto.supplier where id=%s;", (sid,))
                        delc += 1
                    except Exception:
                        err += 1

                # updates
                keep_ids = [i for i in new.index if i not in to_del]
                for sid in keep_ids:
                    a = orig.loc[sid]; b = new.loc[sid]
                    if any(str(a.get(f,"")) != str(b.get(f,"")) for f in ["name","doc","phone","email","active"]):
                        try:
                            qexec("""
                                update resto.supplier
                                   set name=%s, doc=%s, phone=%s, email=%s, active=%s
                                 where id=%s;
                            """, (b["name"], b.get("doc"), b.get("phone"), b.get("email"),
                                  bool(b.get("active")), int(sid)))
                            upd += 1
                        except Exception:
                            err += 1
                st.success(f"Fornecedor: ✅ {upd} atualizado(s) • 🗑️ {delc} excluído(s) • ⚠️ {err} erro(s).")
                _rerun()
        else:
            st.caption("Nenhum fornecedor cadastrado.")

        st.divider()
        _ensure_product_schema_unificado()
        st.subheader("🧂 Produtos / Insumos – UNIFICADO")

        units = qall("select abbr from resto.unit order by abbr;") or []
        cats  = qall("select name from resto.category order by name;") or []
        sups  = qall("select id, name from resto.supplier where coalesce(active,true) is true order by name;") or []
        sup_opts = [(None, "— sem fornecedor —")] + [(r["id"], r["name"]) for r in sups]

        # ---------- Formulário (igual ao de Cadastros) ----------
        with st.form("form_prod_unificado_estoque"):
            c1, c2, c3 = st.columns([2,1,1])
            with c1:
                pr_code = st.text_input("Código interno")
                pr_name = st.text_input("Nome *")
            with c2:
                pr_unit = st.selectbox("Unidade *", options=[u["abbr"] for u in units] or ["un"], key="prd_unit")
            with c3:
                pr_cat  = st.selectbox("Categoria", options=[c["name"] for c in cats] or [""], key="prd_cat")

            c4, c5, c6 = st.columns([2,1,1])
            with c4:
                pr_sup = st.selectbox("Fornecedor", options=sup_opts, format_func=lambda x: x[1] if isinstance(x, tuple) else x, key="prd_sup")
            with c5:
                pr_min = st.number_input("Estoque mínimo", 0.0, 1_000_000.0, 0.0, 0.001, format="%.3f", key="prd_min")
            with c6:
                pr_cost = st.number_input("Último custo (R$)", 0.0, 1_000_000.0, 0.0, 0.01, format="%.2f", key="prd_cost")

            c7, c8, c9 = st.columns([2,1,1])
            with c7:
                pr_bar = st.text_input("Código de barras", key="prd_bar")
            with c8:
                pr_price = st.number_input("Preço de venda (R$)", 0.0, 1_000_000.0, 0.0, 0.01, format="%.2f", key="prd_price")
            with c9:
                pr_markup = st.number_input("Markup padrão %", 0.0, 1000.0, 0.0, 0.1, format="%.2f", key="prd_markup")

            c10, c11, c12 = st.columns(3)
            with c10:
                pr_is_sale = st.checkbox("Item de venda", value=True, key="prd_is_sale")
            with c11:
                pr_is_ing  = st.checkbox("Ingrediente", value=False, key="prd_is_ing")
            with c12:
                pr_active  = st.checkbox("Ativo", value=True, key="prd_active")

            st.markdown("**Campos fiscais (opcional)**")
            f1, f2, f3, f4 = st.columns(4)
            with f1:
                pr_ncm = st.text_input("NCM", key="prd_ncm");        pr_cest = st.text_input("CEST", key="prd_cest")
            with f2:
                pr_cfop = st.text_input("CFOP Venda", key="prd_cfop"); pr_csosn = st.text_input("CSOSN", key="prd_csosn")
            with f3:
                pr_cst_icms = st.text_input("CST ICMS", key="prd_cst_icms"); pr_ali_icms = st.number_input("Alíquota ICMS %", 0.0, 100.0, 0.0, 0.01, key="prd_ali_icms")
            with f4:
                pr_cst_pis = st.text_input("CST PIS", key="prd_cst_pis");   pr_ali_pis = st.number_input("Alíquota PIS %", 0.0, 100.0, 0.0, 0.01, key="prd_ali_pis")
            f5, f6 = st.columns(2)
            with f5:
                pr_cst_cof = st.text_input("CST COFINS", key="prd_cst_cof"); pr_ali_cof = st.number_input("Alíquota COFINS %", 0.0, 100.0, 0.0, 0.01, key="prd_ali_cof")
            with f6:
                pr_iss = st.number_input("ISS % (se serviço)", 0.0, 100.0, 0.0, 0.01, key="prd_iss")

            pr_submit = st.form_submit_button("➕ Adicionar/Atualizar produto")

        if pr_submit and pr_name.strip():
            sup_id = pr_sup[0] if isinstance(pr_sup, tuple) else None
            qexec("""
                insert into resto.product(
                    code, name, unit, category, supplier_id, barcode,
                    min_stock, last_cost, active,
                    sale_price, is_sale_item, is_ingredient, default_markup,
                    ncm, cest, cfop_venda, csosn, cst_icms, aliquota_icms,
                    cst_pis, aliquota_pis, cst_cofins, aliquota_cofins, iss_aliquota
                )
                values (%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s)
                on conflict (name) do update set
                    code=excluded.code,
                    unit=excluded.unit,
                    category=excluded.category,
                    supplier_id=excluded.supplier_id,
                    barcode=excluded.barcode,
                    min_stock=excluded.min_stock,
                    last_cost=excluded.last_cost,
                    active=excluded.active,
                    sale_price=excluded.sale_price,
                    is_sale_item=excluded.is_sale_item,
                    is_ingredient=excluded.is_ingredient,
                    default_markup=excluded.default_markup,
                    ncm=excluded.ncm,
                    cest=excluded.cest,
                    cfop_venda=excluded.cfop_venda,
                    csosn=excluded.csosn,
                    cst_icms=excluded.cst_icms,
                    aliquota_icms=excluded.aliquota_icms,
                    cst_pis=excluded.cst_pis,
                    aliquota_pis=excluded.aliquota_pis,
                    cst_cofins=excluded.cst_cofins,
                    aliquota_cofins=excluded.aliquota_cofins,
                    iss_aliquota=excluded.iss_aliquota;
            """, (pr_code or None, pr_name.strip(), pr_unit, pr_cat or None, sup_id, pr_bar or None,
                  float(pr_min), float(pr_cost), bool(pr_active),
                  float(pr_price), bool(pr_is_sale), bool(pr_is_ing), float(pr_markup),
                  pr_ncm or None, pr_cest or None, pr_cfop or None, pr_csosn or None, pr_cst_icms or None, pr_ali_icms or None,
                  pr_cst_pis or None, pr_ali_pis or None, pr_cst_cof or None, pr_ali_cof or None, pr_iss or None))
            st.success("Produto salvo/atualizado.")
            st.rerun()

        # ---------- Lista/edição UNIFICADA ----------
        rows_p = qall("""
            select p.id, p.code, p.name, p.unit, p.category,
                   s.name as supplier, p.barcode,
                   p.min_stock, p.last_cost, p.sale_price,
                   p.is_sale_item, p.is_ingredient, p.default_markup, p.active
              from resto.product p
              left join resto.supplier s on s.id = p.supplier_id
             order by lower(p.name);
        """) or []
        import pandas as pd
        dfp = pd.DataFrame(rows_p)
        if not dfp.empty:
            dfp["Excluir?"] = False
            cfgp = {
                "id":              st.column_config.NumberColumn("ID", disabled=True),
                "code":            st.column_config.TextColumn("Código"),
                "name":            st.column_config.TextColumn("Nome"),
                "unit":            st.column_config.SelectboxColumn("Un", options=[u["abbr"] for u in units] or ["un"]),
                "category":        st.column_config.SelectboxColumn("Categoria", options=[c["name"] for c in cats] or [""]),
                "supplier":        st.column_config.TextColumn("Fornecedor", disabled=True),
                "barcode":         st.column_config.TextColumn("Código de barras"),
                "min_stock":       st.column_config.NumberColumn("Est. mín.", step=0.001, format="%.3f"),
                "last_cost":       st.column_config.NumberColumn("Últ. custo", step=0.01, format="%.2f"),
                "sale_price":      st.column_config.NumberColumn("Preço venda", step=0.01, format="%.2f"),
                "is_sale_item":    st.column_config.CheckboxColumn("Venda"),
                "is_ingredient":   st.column_config.CheckboxColumn("Ingred."),
                "default_markup":  st.column_config.NumberColumn("Markup %", step=0.1, format="%.2f"),
                "active":          st.column_config.CheckboxColumn("Ativo"),
                "Excluir?":        st.column_config.CheckboxColumn("Excluir?"),
            }
            edited_p = st.data_editor(
                dfp[["id","code","name","unit","category","supplier","barcode","min_stock","last_cost","sale_price",
                     "is_sale_item","is_ingredient","default_markup","active","Excluir?"]],
                column_config=cfgp, hide_index=True, num_rows="fixed", key="prod_edit_est", use_container_width=True
            )

            cpa, cpb = st.columns(2)
            with cpa:
                apply_p = st.button("💾 Salvar alterações (produtos)")
            with cpb:
                refresh_p = st.button("🔄 Atualizar")
            if refresh_p:
                st.rerun()

            if apply_p:
                orig = dfp.set_index("id")
                new  = edited_p.set_index("id")
                upd = delc = err = 0

                to_del = new.index[new["Excluir?"] == True].tolist()
                for pid in to_del:
                    try:
                        qexec("delete from resto.product where id=%s;", (int(pid),))
                        delc += 1
                    except Exception:
                        err += 1

                keep_ids = [i for i in new.index if i not in to_del]
                for pid in keep_ids:
                    a = orig.loc[pid]; b = new.loc[pid]
                    changed = any(str(a.get(f,"")) != str(b.get(f,"")) for f in
                                  ["code","name","unit","category","barcode","min_stock","last_cost",
                                   "sale_price","is_sale_item","is_ingredient","default_markup","active"])
                    if not changed:
                        continue
                    try:
                        qexec("""
                            update resto.product
                               set code=%s, name=%s, unit=%s, category=%s, barcode=%s,
                                   min_stock=%s, last_cost=%s, sale_price=%s,
                                   is_sale_item=%s, is_ingredient=%s, default_markup=%s, active=%s
                             where id=%s;
                        """, (b.get("code"), b.get("name"), b.get("unit"), b.get("category"), b.get("barcode"),
                              float(b.get("min_stock") or 0), float(b.get("last_cost") or 0), float(b.get("sale_price") or 0),
                              bool(b.get("is_sale_item")), bool(b.get("is_ingredient")), float(b.get("default_markup") or 0),
                              bool(b.get("active")), int(pid)))
                        upd += 1
                    except Exception:
                        err += 1

                st.success(f"Produtos: ✅ {upd} atualizado(s) • 🗑️ {delc} excluído(s) • ⚠️ {err} erro(s).")
                st.rerun()
        else:
            st.caption("Nenhum produto cadastrado.")





# ===================== FINANCEIRO =====================
def page_financeiro():

    import pandas as pd  # <- FIX: usado em _run_grid, DRE, Painel, Comparativo
    
    # helper p/ recarregar sem depender do nome antigo
    def _rerun():
        try:
            st.experimental_rerun()
        except Exception:
            pass

    # ---------- helpers estruturais novos (não quebram nada) ----------
    def _ensure_payable_schema():
        qexec("""
        do $$
        begin
          create table if not exists resto.payable (
            id          bigserial primary key,
            purchase_id bigint references resto.purchase(id) on delete cascade,
            supplier_id bigint not null references resto.supplier(id),
            due_date    date not null,
            amount      numeric(14,2) not null,
            status      text not null default 'ABERTO',  -- ABERTO | PAGO | CANCELADO
            paid_at     date,
            method      text,
            category_id bigint references resto.cash_category(id),
            note        text
          );
          create index if not exists payable_status_idx on resto.payable(status);
        end $$;
        """)

    def _ensure_cash_category(kind: str, name: str) -> int:
        """Garante e retorna id de uma categoria de caixa."""
        r = qone("""
            with upsert as (
              insert into resto.cash_category(kind, name)
              values (%s,%s)
              on conflict (kind, name) do update set name=excluded.name
              returning id
            )
            select id from upsert
            union all
            select id from resto.cash_category where kind=%s and name=%s limit 1;
        """, (kind, name, kind, name))
        return int(r["id"])


    def _record_cashbook(kind: str, category_id: int, entry_date, description: str, amount: float, method: str):
        qexec("""
            insert into resto.cashbook(entry_date, kind, category_id, description, amount, method)
            values (%s, %s, %s, %s, %s, %s);
        """, (entry_date, kind, int(category_id), description, float(amount), method))

    # garante estruturas de "a pagar"
    _ensure_payable_schema()

    header("💰 Financeiro", "Entradas, Saídas e DRE.")
    # ADIÇÃO: mantive as 6 abas existentes e acrescentei a nova '🧾 A Pagar' no final
    tabs = st.tabs([
        "💸 Entradas", "💳 Saídas", "📈 DRE", "⚙️ Gestão", "📊 Painel", "📆 Comparativo", "🧾 A Pagar"
    ])

    METHODS = ['— todas —', 'dinheiro', 'pix', 'cartão débito', 'cartão crédito', 'boleto', 'transferência', 'outro']

    # ---------- helpers ----------
    def _cat_options(kind_code: str):
        rows = qall("select id, name, kind from resto.cash_category where kind=%s order by name;", (kind_code,))
        return [(0, "— todas —")] + [(r["id"], r["name"]) for r in rows or []], {r["id"]: r["name"] for r in (rows or [])}

    def _filters_ui(prefix: str, kind_code: str):
        st.subheader("Filtros")
        c1, c2 = st.columns(2)
        with c1:
            dt_ini = st.date_input("De", value=date.today().replace(day=1), key=f"{prefix}_dtini")
        with c2:
            dt_fim = st.date_input("Até", value=date.today(), key=f"{prefix}_dtfim")

        opts, cat_map = _cat_options(kind_code)
        c3, c4, c5 = st.columns([2, 1, 1])
        with c3:
            cat_sel = st.selectbox("Categoria", options=opts, format_func=lambda x: x[1], key=f"{prefix}_cat")
        with c4:
            method = st.selectbox("Método", METHODS, key=f"{prefix}_method")
        with c5:
            lim = st.number_input("Limite", 100, 5000, 1000, 100, key=f"{prefix}_limit")

        desc = st.text_input("Buscar texto na descrição", key=f"{prefix}_desc")

        return dt_ini, dt_fim, cat_sel, method, desc, lim, cat_map

    def _run_grid(kind_code: str, prefix: str, title: str):
        card_start()
        st.subheader(title)

        dt_ini, dt_fim, cat_sel, method, desc, lim, cat_map = _filters_ui(prefix, kind_code)

        wh = ["kind=%s", "entry_date >= %s", "entry_date <= %s"]
        pr = [kind_code, dt_ini, dt_fim]

        if cat_sel and cat_sel[0]:
            wh.append("category_id = %s")
            pr.append(int(cat_sel[0]))
        if method and method != '— todas —':
            wh.append("method = %s")
            pr.append(method)
        if desc and desc.strip():
            wh.append("description ILIKE %s")
            pr.append(f"%{desc.strip()}%")

        sql = f"""
            select id, entry_date, kind, category_id, description, amount, method
              from resto.cashbook
             where {' and '.join(wh)}
             order by entry_date desc, id desc
             limit %s;
        """
        pr2 = pr + [int(lim)]
        rows = qall(sql, tuple(pr2))
        df = pd.DataFrame(rows or [])
        if not df.empty:
            df["categoria"] = df["category_id"].map(cat_map).fillna("")
            df = df[["entry_date", "categoria", "method", "description", "amount", "id", "category_id", "kind"]]
            st.dataframe(df.drop(columns=["id", "category_id", "kind"]), use_container_width=True, hide_index=True)
            total = float(df["amount"].sum())
            st.markdown(f"**Total filtrado ({'Entradas' if kind_code=='IN' else 'Saídas'}):** {money(total)}")
        else:
            st.caption("Sem lançamentos para os filtros selecionados.")
        card_end()

    # ---------- Aba: Entradas ----------
    with tabs[0]:
        _run_grid("IN", "in", "📥 Entradas")

    # ---------- Aba: Saídas ----------
    with tabs[1]:
        _run_grid("OUT", "out", "📤 Saídas")

    # ---------- Aba: DRE (grid bonito) ----------
    with tabs[2]:
        card_start()
        st.subheader("DRE (período)")

        d1, d2 = st.columns(2)
        with d1:
            dre_ini = st.date_input("De", value=date.today().replace(day=1), key="dre_dtini")
        with d2:
            dre_fim = st.date_input("Até", value=date.today(), key="dre_dtfim")

        # Calcula DRE (tenta completo; se não, fallback p/ livro-caixa)
        v = c = d = o = 0.0  # receita, cmv, despesas, outras receitas
        detalhamento = "Completo (vendas + CMV + livro-caixa)"
        try:
            dt_end_next = dre_fim + timedelta(days=1)
            dre = qone("""
                with
                vendas as (
                    select coalesce(sum(total),0) v
                      from resto.sale
                     where status='FECHADA'
                       and date >= %s
                       and date <  %s
                ),
                cmv as (
                    select coalesce(sum(case when kind='OUT' then total_cost else 0 end),0) c
                      from resto.inventory_movement
                     where move_date >= %s
                       and move_date <  %s
                ),
                caixa_desp as (
                    select coalesce(sum(case when kind='OUT' then amount else 0 end),0) d
                      from resto.cashbook
                     where entry_date >= %s
                       and entry_date <  %s
                ),
                caixa_outros as (
                    select coalesce(sum(case when kind='IN' then amount else 0 end),0) o
                      from resto.cashbook
                     where entry_date >= %s
                       and entry_date <  %s
                )
                select v, c, d, o, (v + o - c - d) as resultado
                  from vendas, cmv, caixa_desp, caixa_outros;
            """, (dre_ini, dt_end_next, dre_ini, dt_end_next, dre_ini, dt_end_next, dre_ini, dt_end_next))
            if dre:
                v = float(dre["v"] or 0)
                c = float(dre["c"] or 0)
                d = float(dre["d"] or 0)
                o = float(dre["o"] or 0)
            else:
                raise RuntimeError("sem dados")
        except Exception:
            # fallback simplificado (apenas livro-caixa)
            detalhamento = "Simplificado (somente livro-caixa)"
            v = float(qone("""
                select coalesce(sum(amount),0) s
                  from resto.cashbook
                 where kind='IN' and entry_date between %s and %s
            """, (dre_ini, dre_fim))["s"] or 0)
            d = float(qone("""
                select coalesce(sum(amount),0) s
                  from resto.cashbook
                 where kind='OUT' and entry_date between %s and %s
            """, (dre_ini, dre_fim))["s"] or 0)
            c = 0.0
            o = 0.0

        resultado = v + o - c - d

        # Métricas no topo
        k1, k2, k3, k4 = st.columns(4)
        with k1: st.metric("Receita (vendas)",  money(v))
        with k2: st.metric("CMV",                money(c))
        with k3: st.metric("Despesas (caixa)",  money(d))
        with k4: st.metric("Resultado",         money(resultado))

        # Monta GRID (valores negativos para itens que subtraem)
        import pandas as pd
        linhas = [
            {"Conta": "Receita de Vendas",                   "Valor (R$)": v,        "Observação": ""},
            {"Conta": "(-) CMV",                             "Valor (R$)": -c,       "Observação": "Custo dos produtos vendidos"},
            {"Conta": "(-) Despesas (Livro-Caixa)",          "Valor (R$)": -d,       "Observação": ""},
            {"Conta": "(+) Outras Receitas (Livro-Caixa)",   "Valor (R$)": o,        "Observação": ""},
            {"Conta": "Resultado do Período",                "Valor (R$)": resultado,"Observação": detalhamento},
        ]

        df_dre = pd.DataFrame(linhas)
        # Participação % sobre Receita (quando houver receita > 0)
        part = []
        for val in df_dre["Valor (R$)"]:
            if v != 0:
                part.append((val / v) * 100.0)
            else:
                part.append(None)
        df_dre["Participação % s/ Receita"] = part

        # Exibição com column_config (formatação amigável)
        colcfg = {
            "Conta": st.column_config.TextColumn("Conta"),
            "Valor (R$)": st.column_config.NumberColumn("Valor (R$)", format="%.2f"),
            "Participação % s/ Receita": st.column_config.NumberColumn("Part. %", format="%.2f"),
            "Observação": st.column_config.TextColumn("Observação"),
        }
        st.dataframe(df_dre, use_container_width=True, hide_index=True, column_config=colcfg)

        # Download
        csv = df_dre.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Exportar CSV (DRE)", data=csv, file_name="dre_periodo.csv", mime="text/csv")

        card_end()





    # ---------- Aba: Gestão (Filtros + Grid editável) ----------
    with tabs[3]:
        card_start()
        st.subheader("Filtros")

        # Filtros (Gestão lida com ambos os tipos)
        colf1, colf2 = st.columns(2)
        with colf1:
            g_dtini = st.date_input("De", value=date.today().replace(day=1), key="gest_dtini")
        with colf2:
            g_dtfim = st.date_input("Até", value=date.today(), key="gest_dtfim")

        kind_label = st.selectbox("Tipo", ["— ambos —", "Entradas (IN)", "Saídas (OUT)"], key="gest_kind")
        if kind_label == "Entradas (IN)":
            kind_filter = "IN"
        elif kind_label == "Saídas (OUT)":
            kind_filter = "OUT"
        else:
            kind_filter = None

        cats_all = qall("select id, name, kind from resto.cash_category order by name;") or []
        cat_labels = ["— todas —"] + [f"{c['name']} ({c['kind']})" for c in cats_all]
        cat_label_to_id = {"— todas —": 0}
        for c in cats_all:
            cat_label_to_id[f"{c['name']} ({c['kind']})"] = c["id"]

        colf3, colf4, colf5 = st.columns([2, 1, 1])
        with colf3:
            g_cat_label = st.selectbox("Categoria", cat_labels, key="gest_cat")
        with colf4:
            g_method = st.selectbox("Método", METHODS, key="gest_method")
        with colf5:
            g_limit = st.number_input("Limite", 100, 5000, 1000, 100, key="gest_limit")

        g_desc = st.text_input("Buscar texto na descrição", key="gest_desc")

        wh = ["entry_date >= %s", "entry_date <= %s"]
        pr = [g_dtini, g_dtfim]
        if kind_filter:
            wh.append("kind = %s"); pr.append(kind_filter)
        if g_cat_label and cat_label_to_id[g_cat_label] != 0:
            wh.append("category_id = %s"); pr.append(cat_label_to_id[g_cat_label])
        if g_method and g_method != '— todas —':
            wh.append("method = %s"); pr.append(g_method)
        if g_desc and g_desc.strip():
            wh.append("description ILIKE %s"); pr.append(f"%{g_desc.strip()}%")

        sqlg = f"""
            select id, entry_date, kind, category_id, description, amount, method
              from resto.cashbook
             where {' and '.join(wh)}
             order by entry_date desc, id desc
             limit %s;
        """
        prg = tuple(pr + [int(g_limit)])
        rows_g = qall(sqlg, prg) or []
        df_g = pd.DataFrame(rows_g)

        st.subheader("Grid editável")
        if not df_g.empty:
            id_to_label = {c["id"]: f"{c['name']} ({c['kind']})" for c in cats_all}
            label_to_id = {v: k for k, v in id_to_label.items()}

            df_g["categoria"] = df_g["category_id"].map(id_to_label).fillna("")
            df_g["Excluir?"] = False

            colcfg = {
                "id": st.column_config.NumberColumn("ID", disabled=True),
                "entry_date": st.column_config.DateColumn("Data"),
                "kind": st.column_config.SelectboxColumn("Tipo", options=["IN", "OUT"]),
                "categoria": st.column_config.SelectboxColumn("Categoria", options=list(label_to_id.keys())),
                "method": st.column_config.SelectboxColumn("Método", options=METHODS[1:]),
                "description": st.column_config.TextColumn("Descrição"),
                "amount": st.column_config.NumberColumn("Valor", step=0.01, format="%.2f"),
                "Excluir?": st.column_config.CheckboxColumn("Excluir?", help="Marque para excluir este lançamento"),
            }

            edited = st.data_editor(
                df_g[["id","entry_date","kind","categoria","method","description","amount","Excluir?"]],
                column_config=colcfg,
                num_rows="fixed",
                hide_index=True,
                key="gest_editor",
                use_container_width=True
            )

            colb1, colb2 = st.columns(2)
            with colb1:
                aplicar = st.button("💾 Aplicar alterações", key="gest_apply")
            with colb2:
                refresh = st.button("🔄 Atualizar", key="gest_refresh")

            if refresh:
                _rerun()

            if aplicar:
                orig = df_g.set_index("id")
                new = edited.set_index("id")

                upd, del_ids, err = 0, 0, 0

                # exclusões
                to_delete = new.index[new["Excluir?"] == True].tolist()
                for _id in to_delete:
                    try:
                        qexec("delete from resto.cashbook where id=%s;", (_id,))
                        del_ids += 1
                    except Exception:
                        err += 1

                # updates (somente linhas não marcadas para excluir)
                keep_ids = [i for i in new.index if i not in to_delete]
                for _id in keep_ids:
                    a = orig.loc[_id]; b = new.loc[_id]

                    changed = any(str(a.get(f, "")) != str(b.get(f, "")) for f in
                                  ["entry_date","kind","categoria","method","description","amount"])
                    if not changed:
                        continue

                    new_cat_id = label_to_id.get(b["categoria"]) or int(a["category_id"])
                    new_kind = b["kind"] if b["kind"] in ("IN","OUT") else a["kind"]

                    try:
                        qexec("""
                            update resto.cashbook
                               set entry_date=%s, kind=%s, category_id=%s, description=%s, amount=%s, method=%s
                             where id=%s;
                        """, (b["entry_date"], new_kind, int(new_cat_id),
                              (b["description"] or "")[:300], float(b["amount"]), b["method"], int(_id)))
                        upd += 1
                    except Exception:
                        err += 1

                st.success(f"✅ {upd} atualizado(s) • 🗑️ {del_ids} excluído(s) • ⚠️ {err} com erro.")
                _rerun()
        else:
            st.caption("Sem lançamentos para os filtros.")

        card_end()

    # ---------- Aba: 📊 Painel ----------
    with tabs[4]:
        card_start()
        st.subheader("📊 Painel por Categoria")

        colp1, colp2 = st.columns(2)
        with colp1:
            p_dtini = st.date_input("De", value=date.today().replace(day=1), key="painel_dtini")
        with colp2:
            p_dtfim = st.date_input("Até", value=date.today(), key="painel_dtfim")

        cats_all = qall("select id, name from resto.cash_category order by name;") or []
        cat_opts = [(c["id"], c["name"]) for c in cats_all]
        p_cats = st.multiselect(
            "Categorias (opcional)",
            options=cat_opts,
            format_func=lambda x: x[1],
            key="painel_cats"
        )
        sel_ids = [c[0] for c in p_cats] if p_cats else []

        wh = ["cb.entry_date >= %s", "cb.entry_date <= %s"]
        pr = [p_dtini, p_dtfim]
        if sel_ids:
            wh.append("cb.category_id = ANY(%s)")
            pr.append(sel_ids)

        sqlp = f"""
            select
                coalesce(c.name, '(sem categoria)') as categoria,
                sum(case when cb.kind='IN'  then cb.amount else 0 end)  as entradas_raw,
                sum(case when cb.kind='OUT' then cb.amount else 0 end)  as saidas_raw
            from resto.cashbook cb
            left join resto.cash_category c on c.id = cb.category_id
            where {' and '.join(wh)}
            group by categoria
            order by categoria;
        """
        rows_p = qall(sqlp, tuple(pr))
        dfp = pd.DataFrame(rows_p or [])

        if not dfp.empty:
            dfp["entradas"] = dfp["entradas_raw"].astype(float)
            dfp["saidas"]   = dfp["saidas_raw"].astype(float).apply(lambda x: -x if x < 0 else x)
            dfp["saldo"]    = dfp["entradas"] - dfp["saidas"]

            k1, k2, k3 = st.columns(3)
            with k1: st.metric("Entradas (período)",  money(float(dfp["entradas"].sum())))
            with k2: st.metric("Saídas (período)",    money(float(dfp["saidas"].sum())))
            with k3: st.metric("Resultado (E − S)",   money(float(dfp["saldo"].sum())))

            st.markdown("#### Por categoria")
            df_show = dfp[["categoria","entradas","saidas","saldo"]].sort_values("saldo", ascending=False)
            st.dataframe(df_show, use_container_width=True, hide_index=True)

            chart_df = df_show.set_index("categoria")[["entradas","saidas"]]
            st.bar_chart(chart_df, use_container_width=True)

            csv = df_show.to_csv(index=False).encode("utf-8")
            st.download_button("⬇️ Exportar CSV (Painel)", data=csv, file_name="painel_financeiro.csv", mime="text/csv")
        else:
            st.caption("Sem lançamentos no período/filtro.")

        card_end()

    # ---------- Aba: 📆 Comparativo ----------
    with tabs[5]:
        card_start()
        st.subheader("📆 Comparativo (Mensal / Semestral / Anual)")

        colc1, colc2, colc3 = st.columns([1, 1, 2])
        with colc1:
            modo = st.selectbox("Período", ["Mensal (12m)", "Semestral (6m)", "Anual (5a)"], key="cmp_modo")
        with colc2:
            method = st.selectbox("Método", METHODS, key="cmp_method")
        with colc3:
            cats_all = qall("select id, name from resto.cash_category order by name;") or []
            cat_opts = [(c["id"], c["name"]) for c in cats_all]
            cmp_cats = st.multiselect("Categorias (opcional)", options=cat_opts, format_func=lambda x: x[1], key="cmp_cats")
            cat_ids = [c[0] for c in cmp_cats] if cmp_cats else []

        if modo in ("Mensal (12m)", "Semestral (6m)"):
            nmeses = 12 if "12" in modo else 6

            wh_extra = []
            params = []
            if cat_ids:
                wh_extra.append("cb.category_id = ANY(%s)")
                params.append(cat_ids)
            if method and method != '— todas —':
                wh_extra.append("cb.method = %s")
                params.append(method)

            sql_cmp = f"""
                with series as (
                    select generate_series(
                        date_trunc('month', current_date) - interval '{nmeses-1} months',
                        date_trunc('month', current_date),
                        interval '1 month'
                    )::date as m
                ),
                agg as (
                    select date_trunc('month', cb.entry_date)::date as m,
                           sum(case when cb.kind='IN'  then cb.amount else 0 end)  as vin,
                           sum(case when cb.kind='OUT' then cb.amount else 0 end)  as vout
                      from resto.cashbook cb
                     where cb.entry_date >= (select min(m) from series)
                       and cb.entry_date <  (date_trunc('month', current_date) + interval '1 month')
                       {(' and ' + ' and '.join(wh_extra)) if wh_extra else ''}
                  group by 1
                )
                select to_char(s.m, 'YYYY-MM') as periodo,
                       coalesce(a.vin,0)  as entradas_raw,
                       coalesce(a.vout,0) as saidas_raw
                  from series s
                  left join agg a on a.m = s.m
              order by s.m;
            """
            rows_cmp = qall(sql_cmp, tuple(params)) or []
            dfc = pd.DataFrame(rows_cmp)

            if not dfc.empty:
                dfc["entradas"] = dfc["entradas_raw"].astype(float)
                dfc["saidas"]   = dfc["saidas_raw"].astype(float).apply(lambda x: -x if x < 0 else x)
                dfc["saldo"]    = dfc["entradas"] - dfc["saidas"]

                k1, k2, k3 = st.columns(3)
                with k1: st.metric("Entradas", money(float(dfc["entradas"].sum())))
                with k2: st.metric("Saídas",   money(float(dfc["saidas"].sum())))
                with k3: st.metric("Saldo",    money(float(dfc["saldo"].sum())))

                st.markdown("#### Evolução mensal")
                show = dfc[["periodo","entradas","saidas","saldo"]]
                st.dataframe(show, use_container_width=True, hide_index=True)
                st.bar_chart(show.set_index("periodo")[["entradas","saidas"]], use_container_width=True)

                csv = show.to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Exportar CSV (Comparativo Mensal/Semestral)", data=csv,
                                   file_name="comparativo_mensal.csv", mime="text/csv")
            else:
                st.caption("Sem lançamentos no período/critério selecionado.")

        else:
            wh_extra = []
            params = []
            if cat_ids:
                wh_extra.append("cb.category_id = ANY(%s)")
                params.append(cat_ids)
            if method and method != '— todas —':
                wh_extra.append("cb.method = %s")
                params.append(method)

            sql_cmp_y = f"""
                with series as (
                    select generate_series(
                        date_trunc('year', current_date) - interval '4 years',
                        date_trunc('year', current_date),
                        interval '1 year'
                    )::date as y
                ),
                agg as (
                    select date_trunc('year', cb.entry_date)::date as y,
                           sum(case when cb.kind='IN'  then cb.amount else 0 end)  as vin,
                           sum(case when cb.kind='OUT' then cb.amount else 0 end)  as vout
                      from resto.cashbook cb
                     where cb.entry_date >= (date_trunc('year', current_date) - interval '4 years')
                       and cb.entry_date <  (date_trunc('year', current_date) + interval '1 year')
                       {(' and ' + ' and '.join(wh_extra)) if wh_extra else ''}
                  group by 1
                )
                select to_char(s.y, 'YYYY') as ano,
                       coalesce(a.vin,0)  as entradas_raw,
                       coalesce(a.vout,0) as saidas_raw
                  from series s
                  left join agg a on a.y = s.y
              order by s.y;
            """
            rows_cmp_y = qall(sql_cmp_y, tuple(params)) or []
            dfy = pd.DataFrame(rows_cmp_y)

            if not dfy.empty:
                dfy["entradas"] = dfy["entradas_raw"].astype(float)
                dfy["saidas"]   = dfy["saidas_raw"].astype(float).apply(lambda x: -x if x < 0 else x)
                dfy["saldo"]    = dfy["entradas"] - dfy["saidas"]

                k1, k2, k3 = st.columns(3)
                with k1: st.metric("Entradas (5a)", money(float(dfy["entradas"].sum())))
                with k2: st.metric("Saídas (5a)",   money(float(dfy["saidas"].sum())))
                with k3: st.metric("Saldo (5a)",    money(float(dfy["saldo"].sum())))

                st.markdown("#### Evolução anual (últimos 5 anos)")
                show_y = dfy[["ano","entradas","saidas","saldo"]]
                st.dataframe(show_y, use_container_width=True, hide_index=True)
                st.bar_chart(show_y.set_index("ano")[["entradas","saidas"]], use_container_width=True)

                csv = show_y.to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Exportar CSV (Comparativo Anual)", data=csv,
                                   file_name="comparativo_anual.csv", mime="text/csv")
            else:
                st.caption("Sem lançamentos no período/critério selecionado.")

        card_end()

    # ---------- Aba: 🧾 A Pagar (NOVO, sem mexer nas outras abas) ----------
    with tabs[6]:
        card_start()
        st.subheader("🧾 Contas a Pagar (compras → caixa)")

        # seção: gerar títulos a pagar a partir de compras POSTADAS sem título
        st.markdown("#### Gerar título a partir de compra postada")
        compras_sem_titulo = qall("""
            select p.id, s.name as fornecedor, p.doc_date,
                   coalesce((select sum(total) from resto.purchase_item where purchase_id=p.id),0)
                   + coalesce(p.freight_value,0) + coalesce(p.other_costs,0) as total
              from resto.purchase p
              join resto.supplier s on s.id = p.supplier_id
             where p.status = 'POSTADA'
               and not exists (select 1 from resto.payable x where x.purchase_id = p.id and x.status <> 'CANCELADO')
             order by p.doc_date desc, p.id desc
             limit 200;
        """) or []

        if compras_sem_titulo:
            opt = st.selectbox(
                "Compra postada (sem título)",
                options=[(r["id"], f"#{r['id']} • {r['fornecedor']} • {r['doc_date']} • {money(float(r['total']))}") for r in compras_sem_titulo],
                format_func=lambda x: x[1] if isinstance(x, tuple) else x,
                key="apagar_comp_sel"
            )
            if opt:
                comp_row = next((r for r in compras_sem_titulo if r["id"] == opt[0]), None)
                with st.form("form_gerar_payable"):
                    col1, col2, col3 = st.columns([1,1,2])
                    with col1:
                        due = st.date_input("Vencimento", value=date.today())
                    with col2:
                        valor = st.number_input("Valor do título", min_value=0.01, step=0.01, value=float(comp_row["total"]), format="%.2f")
                    with col3:
                        note = st.text_input("Observação", value=f"Compra #{comp_row['id']} - {comp_row['fornecedor']}")
                    ok = st.form_submit_button("➕ Gerar título (A Pagar)")
                if ok:
                    # resolve supplier_id
                    sid = qone("select supplier_id from resto.purchase where id=%s;", (int(comp_row["id"]),))["supplier_id"]
                    qexec("""
                        insert into resto.payable(purchase_id, supplier_id, due_date, amount, status, note)
                        values (%s,%s,%s,%s,'ABERTO',%s);
                    """, (int(comp_row["id"]), int(sid), due, float(valor), note or None))
                    st.success("Título gerado.")
                    _rerun()
        else:
            st.caption("Não há compras POSTADAS sem título de pagamento.")

        st.divider()
        st.markdown("#### Títulos em aberto")

        # filtros simples
        f1, f2 = st.columns(2)
        with f1:
            so_vencidos = st.checkbox("Somente vencidos", value=False)
        with f2:
            dt_ref = st.date_input("Considerar vencidos até", value=date.today())

        wh = ["status='ABERTO'"]
        pr = []
        if so_vencidos:
            wh.append("due_date <= %s")
            pr.append(dt_ref)

        rows_open = qall(f"""
            select p.id, p.purchase_id, p.supplier_id, s.name as fornecedor,
                   p.due_date, p.amount, coalesce(p.note,'') as note
              from resto.payable p
              join resto.supplier s on s.id = p.supplier_id
             where {' and '.join(wh)}
             order by p.due_date asc, p.id asc;
        """, tuple(pr)) or []

        df_open = pd.DataFrame(rows_open)
        if df_open.empty:
            st.caption("Nenhum título em aberto para os filtros.")
            card_end()
            return

        # edição leve + marcar ações
        df_open["Pagar?"] = False
        df_open["Excluir?"] = False
        cfg = {
            "id":          st.column_config.NumberColumn("ID", disabled=True),
            "purchase_id": st.column_config.NumberColumn("Compra", disabled=True),
            "fornecedor":  st.column_config.TextColumn("Fornecedor", disabled=True),
            "due_date":    st.column_config.DateColumn("Vencimento"),
            "amount":      st.column_config.NumberColumn("Valor", step=0.01, format="%.2f"),
            "note":        st.column_config.TextColumn("Observação"),
            "Pagar?":      st.column_config.CheckboxColumn("Pagar agora?"),
            "Excluir?":    st.column_config.CheckboxColumn("Excluir?"),
        }

        edited_open = st.data_editor(
            df_open[["id","purchase_id","fornecedor","due_date","amount","note","Pagar?","Excluir?"]],
            column_config=cfg,
            hide_index=True,
            num_rows="fixed",
            use_container_width=True,
            key="payable_editor"
        )

        st.markdown("##### Baixa em lote / Ações")
        colx1, colx2, colx3, colx4 = st.columns([1,1,1,2])
        with colx1:
            dt_pag = st.date_input("Data de pagamento", value=date.today(), key="pay_dt")
        with colx2:
            metodo_pag = st.selectbox("Método", METHODS[1:], key="pay_method")  # sem '— todas —'
        with colx3:
            # categoria padrão Compras/Estoque
            cat_out_default = _ensure_cash_category('OUT', 'Compras/Estoque')
            # permitir trocar (se quiser usar outra)
            cats_out = qall("select id, name from resto.cash_category where kind='OUT' order by name;") or []
            # manter default como primeira opção visível
            cats_ordered = sorted(cats_out, key=lambda r: (0 if r["id"] == cat_out_default else 1, r["name"]))
            cat_sel = st.selectbox("Categoria (saída)", options=[(r["id"], r["name"]) for r in cats_ordered],
                                   format_func=lambda x: x[1] if isinstance(x, tuple) else x,
                                   key="pay_cat")
        with colx4:
            do_pay = st.button("💸 Pagar selecionados")
        coly1, coly2 = st.columns([1,1])
        with coly1:
            do_save = st.button("💾 Salvar alterações (datas/valores/observação)")
        with coly2:
            do_del = st.button("🗑️ Excluir selecionados")

        # ações
        if do_save:
            orig = df_open.set_index("id")
            new  = edited_open.set_index("id")
            upd = 0; err = 0
            for pid in new.index.tolist():
                a = orig.loc[pid]; b = new.loc[pid]
                if any(str(a.get(f,"")) != str(b.get(f,"")) for f in ["due_date","amount","note"]):
                    try:
                        qexec("""
                            update resto.payable
                               set due_date=%s, amount=%s, note=%s
                             where id=%s and status='ABERTO';
                        """, (b["due_date"], float(b["amount"]), (b.get("note") or None), int(pid)))
                        upd += 1
                    except Exception:
                        err += 1
            st.success(f"✅ {upd} título(s) atualizado(s) • ⚠️ {err} erro(s)")
            _rerun()

        if do_del:
            new = edited_open.set_index("id")
            ids = new.index[new["Excluir?"] == True].tolist()
            ok, bad = 0, 0
            for pid in ids:
                try:
                    qexec("update resto.payable set status='CANCELADO' where id=%s and status='ABERTO';", (int(pid),))
                    ok += 1
                except Exception:
                    bad += 1
            st.success(f"🗑️ {ok} título(s) cancelado(s) • ⚠️ {bad} erro(s)")
            _rerun()

        if do_pay:
            new = edited_open.set_index("id")
            ids = new.index[new["Pagar?"] == True].tolist()
            if not ids:
                st.warning("Selecione pelo menos um título para pagar.")
            else:
                cat_id = int(cat_sel[0]) if isinstance(cat_sel, tuple) else cat_out_default
                ok, bad = 0, 0
                for pid in ids:
                    try:
                        row = qone("""
                            select p.id, p.amount, p.note, s.name fornecedor
                              from resto.payable p
                              join resto.supplier s on s.id = p.supplier_id
                             where p.id=%s and p.status='ABERTO';
                        """, (int(pid),))
                        if not row:
                            bad += 1
                            continue
                        desc = f"Pagamento título #{pid} – {row['fornecedor']}"
                        _record_cashbook('OUT', cat_id, dt_pag, desc, float(row["amount"]), metodo_pag)
                        qexec("""
                            update resto.payable
                               set status='PAGO', paid_at=%s, method=%s, category_id=%s
                             where id=%s;
                        """, (dt_pag, metodo_pag, cat_id, int(pid)))
                        ok += 1
                    except Exception:
                        bad += 1
                st.success(f"💸 {ok} pago(s) • ⚠️ {bad} com erro")
                _rerun()

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

    def _col(*names):
        for n in names:
            if n in df.columns:
                return n
        return None

    col_date = _col("Data Lançamento", "Data Lancamento")
    col_tit  = _col("Título", "Titulo", "Title")
    col_desc = _col("Descrição", "Descricao", "Description")
    col_ent  = _col("Entrada(R$)", "Credito", "Crédito", "Credit")
    col_sai  = _col("Saída(R$)", "Saida(R$)", "Debito", "Débito", "Debit")

    if not (col_date and col_ent and col_sai and (col_tit or col_desc)):
        return pd.DataFrame()

    out = pd.DataFrame({
        "entry_date": pd.to_datetime(df[col_date], dayfirst=True, errors="coerce").dt.date,
        "description_main": (df[col_desc].astype(str) if col_desc else ""),
        "description_alt":  (df[col_tit].astype(str)  if col_tit  else ""),
    })
    out["description"] = (
        out["description_alt"].str.strip() + " | " + out["description_main"].str.strip()
    ).str.strip(" |")
    out["amount"] = df[col_ent].apply(_parse_brl_amount) - df[col_sai].apply(_parse_brl_amount)
    return out.dropna(subset=["entry_date"])

def _load_csv_generic(raw: bytes) -> pd.DataFrame:
    from io import StringIO
    txt = _read_text_guess(raw)
    if not txt:
        return pd.DataFrame()
    first = txt.splitlines()[0] if txt.splitlines() else ""
    sep = _guess_sep(first)
    df = pd.read_csv(StringIO(txt), sep=sep, dtype=str, keep_default_na=False)

    cols = {c.lower(): c for c in df.columns}
    entry_col = (cols.get("data") or cols.get("date") or cols.get("data lançamento")
                 or cols.get("data lancamento") or cols.get("entry_date"))
    if not entry_col:
        return pd.DataFrame()

    cand_main = ["descrição","descricao","description","histórico","historico","memo","narrativa"]
    cand_alt  = ["título","titulo","title","detalhe","detalhes","complemento","observação","observacao"]
    desc_col = next((cols[c] for c in cand_main if c in cols), None)
    alt_col  = next((cols[c] for c in cand_alt  if c in cols and cols[c] != desc_col), None)
    if not (desc_col or alt_col):
        desc_col = alt_col or next((cols[c] for c in ("titulo","título","title","description") if c in cols), None)

    base = pd.DataFrame({
        "entry_date": pd.to_datetime(df[entry_col], dayfirst=True, errors="coerce").dt.date,
        "description_main": df[desc_col].astype(str) if desc_col else "",
        "description_alt":  df[alt_col].astype(str)  if alt_col  else "",
    })
    base["description"] = (
        base["description_alt"].str.strip() + " | " + base["description_main"].str.strip()
    ).str.strip(" |")

    val_col = cols.get("valor") or cols.get("amount")
    ent_col = (cols.get("entrada(r$)") or cols.get("credito") or cols.get("crédito") or cols.get("credit"))
    sai_col = (cols.get("saída(r$)") or cols.get("saida(r$)") or cols.get("debito") or cols.get("débito") or cols.get("debit"))

    if val_col:
        base["amount"] = df[val_col].apply(_parse_brl_amount)
    elif ent_col and sai_col:
        base["amount"] = df[ent_col].apply(_parse_brl_amount) - df[sai_col].apply(_parse_brl_amount)
    else:
        return pd.DataFrame()

    return base.dropna(subset=["entry_date"])

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

# ---------- NOVOS HELPERS (categoria fixa para ENTRADAS = VENDAS) ----------
def _ensure_cash_category(kind: str, name: str) -> int:
    """Garante e retorna id da categoria (sem depender de índices específicos)."""
    r = qone("""
        with upsert as (
          insert into resto.cash_category(kind, name)
          values (%s,%s)
          on conflict (kind, name) do update set name=excluded.name
          returning id
        )
        select id from upsert
        union all
        select id from resto.cash_category where kind=%s and name=%s limit 1;
    """, (kind, name, kind, name))
    return int(r["id"])

def _sales_import_category_id() -> int:
    """Categoria fixa para entradas importadas tratadas como VENDAS."""
    return _ensure_cash_category('IN', 'Vendas (Importadas)')

def _guess_method_from_desc(desc: str) -> str:
    s = (desc or "").lower()
    if "pix" in s: return "pix"
    if "débito" in s or "debito" in s: return "cartão débito"
    if "crédito" in s or "credito" in s: return "cartão crédito"
    if "boleto" in s: return "boleto"
    if "ted" in s or "doc" in s or "transfer" in s: return "transferência"
    if "dinheiro" in s or "cash" in s: return "dinheiro"
    return "outro"

def page_importar_extrato():
    header("🏦 Importar Extrato (CSV)", "Modelo C6 ou CSV genérico com data/descrição/valor.")
    card_start()
    st.subheader("1) Selecione o arquivo")
    up = st.file_uploader("CSV do banco", type=["csv"])
    if not up:
        st.info("Dica: o CSV do C6 com cabeçalho 'Data Lançamento, Data Contábil, ...' é detectado automaticamente.")
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

    # Buscamos categorias atuais (mas vamos FIXAR ENTRADAS como 'Vendas (Importadas)')
    cats = qall("select id, name, kind from resto.cash_category order by name;") or []
    out_cats = [(c['id'], f"{c['name']} ({c['kind']})") for c in cats if c['kind']=='OUT']

    # fallback defensivo caso não haja nenhuma OUT cadastrada
    if not out_cats:
        def_out_id = _ensure_cash_category('OUT', 'Despesas (Importadas)')
        out_cats = [(def_out_id, "Despesas (Importadas) (OUT)")]

    col1, col2 = st.columns(2)
    with col1:
        st.info("Entradas importadas serão categorizadas automaticamente como **Vendas (Importadas)**.")
    with col2:
        default_cat_out = st.selectbox(
            "Categoria padrão para SAÍDAS",
            options=out_cats,
            format_func=lambda x: x[1] if isinstance(x, tuple) else x
        )

    method = st.selectbox(
        "Método (opcional — deixe em auto p/ detectar por descrição)",
        ['— auto por descrição —','dinheiro','pix','cartão débito','cartão crédito','boleto','transferência','outro']
    )

    # Define coluna kind
    out = df.copy()
    out["kind"] = out["amount"].apply(lambda v: "IN" if float(v) >= 0 else "OUT")

    # Categoria por linha:
    sales_cat_id = _sales_import_category_id()
    out["category_id"] = out["kind"].apply(lambda k: sales_cat_id if k == "IN" else default_cat_out[0])

    # Método por linha (auto por descrição) + override opcional
    try:
        out["method"] = out["description"].apply(_guess_method_from_desc)
    except Exception:
        out["method"] = "outro"
    if method != "— auto por descrição —":
        out["method"] = method

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
        inserted, skipped_dup, skipped_err = 0, 0, 0

        for _, r in prontos.iterrows():
            entry_date = str(r["entry_date"])
            kind       = str(r["kind"])
            category   = int(r["category_id"])
            desc       = (str(r["description"]) if r.get("description") is not None else "")[:300]
            amount     = round(float(r["amount"]), 2)
            method_row = r.get("method") or 'outro'

            sql = """
            insert into resto.cashbook(entry_date, kind, category_id, description, amount, method)
            select %s, %s, %s, %s, %s, %s
            where not exists (
                select 1
                  from resto.cashbook c
                 where c.entry_date = %s
                   and c.kind        = %s
                   and c.description = %s
                   and c.amount      = %s
            );
            """
            params = (
                entry_date, kind, category, desc, amount, method_row,
                entry_date, kind, desc, amount
            )

            try:
                rc = qexec(sql, params)  # se seu qexec não retorna rowcount, tudo bem: cairá no else e contará como duplicado
                if rc and rc > 0:
                    inserted += 1
                else:
                    skipped_dup += 1
            except Exception:
                skipped_err += 1

        st.success(f"Importação finalizada: {inserted} inseridos • {skipped_dup} ignorados (duplicados) • {skipped_err} com erro.")
    card_end()

    
# ===================== AGENDA DE CONTAS (A PAGAR) =====================
def page_agenda_contas():
    import pandas as pd
    from datetime import date, timedelta

    # ---------- helpers defensivos ----------
    def _rerun():
        try:
            st.rerun()
        except Exception:
            try:
                st.experimental_rerun()
            except Exception:
                pass

    def _ensure_payable_schema():
        # ALTERADO: cria/ajusta tabela de forma defensiva e relaxa NOT NULL dos campos opcionais
        qexec("""
        do $$
        begin
          if to_regclass('resto.payable') is null then
            create table resto.payable (
              id          bigserial primary key,
              purchase_id bigint references resto.purchase(id) on delete cascade,
              supplier_id bigint not null references resto.supplier(id),
              due_date    date not null,
              amount      numeric(14,2) not null,
              status      text not null default 'ABERTO',  -- ABERTO | PAGO | CANCELADO
              paid_at     date,
              method      text,
              category_id bigint references resto.cash_category(id),
              note        text
            );
          else
            -- garante colunas (sem quebrar se já existirem)
            begin
              alter table resto.payable add column if not exists purchase_id bigint references resto.purchase(id) on delete cascade;
              alter table resto.payable add column if not exists supplier_id bigint;
              alter table resto.payable add column if not exists due_date date;
              alter table resto.payable add column if not exists amount numeric(14,2);
              alter table resto.payable add column if not exists status text;
              alter table resto.payable add column if not exists paid_at date;
              alter table resto.payable add column if not exists method text;
              alter table resto.payable add column if not exists category_id bigint references resto.cash_category(id);
              alter table resto.payable add column if not exists note text;
            exception when others then null; end;

            -- relaxa NOT NULL de campos opcionais (se existirem)
            begin alter table resto.payable alter column purchase_id drop not null; exception when others then null; end;
            begin alter table resto.payable alter column method      drop not null; exception when others then null; end;
            begin alter table resto.payable alter column category_id drop not null; exception when others then null; end;
            begin alter table resto.payable alter column note        drop not null; exception when others then null; end;

            -- garante NOT NULL nos essenciais e default de status
            begin alter table resto.payable alter column supplier_id set not null; exception when others then null; end;
            begin alter table resto.payable alter column due_date    set not null; exception when others then null; end;
            begin alter table resto.payable alter column amount      set not null; exception when others then null; end;
            begin alter table resto.payable alter column status      set not null; exception when others then null; end;
            begin alter table resto.payable alter column status      set default 'ABERTO'; exception when others then null; end;
          end if;

          -- índices úteis
          create index if not exists payable_status_idx  on resto.payable(status);
          create index if not exists payable_duedate_idx on resto.payable(due_date);
        end $$;
        """)

    def _ensure_cash_category(kind: str, name: str) -> int:
        # upsert seguro mesmo sem unique criado previamente
        r = qone("""
            with ins as (
              insert into resto.cash_category(kind, name)
              values (%s,%s)
              on conflict (kind, name) do update set name=excluded.name
              returning id
            )
            select id from ins
            union all
            select id from resto.cash_category where kind=%s and name=%s limit 1;
        """, (kind, name, kind, name))
        return int(r["id"])

    def _record_cashbook(kind: str, category_id: int, entry_date, description: str, amount: float, method: str):
        qexec("""
            insert into resto.cashbook(entry_date, kind, category_id, description, amount, method)
            values (%s,%s,%s,%s,%s,%s);
        """, (entry_date, kind, int(category_id), (description or "")[:300], float(amount), method))

    METHODS = ['dinheiro', 'pix', 'cartão débito', 'cartão crédito', 'boleto', 'transferência', 'outro']

    _ensure_payable_schema()

    header("🗓️ Agenda de Contas", "Veja e gerencie os próximos pagamentos (a pagar).")

    # ---------- indicadores (cards) ----------
    hoje = date.today()
    ate7  = hoje + timedelta(days=7)
    ate30 = hoje + timedelta(days=30)

    sum_over = qone("select coalesce(sum(amount),0) s from resto.payable where status='ABERTO' and due_date < %s;", (hoje,))["s"]
    sum_hoje = qone("select coalesce(sum(amount),0) s from resto.payable where status='ABERTO' and due_date = %s;", (hoje,))["s"]
    sum_7    = qone("select coalesce(sum(amount),0) s from resto.payable where status='ABERTO' and due_date > %s and due_date <= %s;", (hoje, ate7))["s"]
    sum_30   = qone("select coalesce(sum(amount),0) s from resto.payable where status='ABERTO' and due_date > %s and due_date <= %s;", (ate7, ate30))["s"]
    sum_tot  = qone("select coalesce(sum(amount),0) s from resto.payable where status='ABERTO';")["s"]

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1: st.metric("🔴 Vencidos",        money(float(sum_over)))
    with k2: st.metric("🟠 Hoje",            money(float(sum_hoje)))
    with k3: st.metric("🟡 Próx. 7 dias",    money(float(sum_7)))
    with k4: st.metric("🟢 Próx. 30 dias",   money(float(sum_30)))
    with k5: st.metric("🧮 Total em aberto", money(float(sum_tot)))

    st.divider()

    # ---------- adicionar título manual ----------
    with st.expander("➕ Adicionar título manual (opcional)", expanded=False):
        sups = qall("select id, name from resto.supplier where coalesce(active,true) is true order by name;") or []
        sup_opt = st.selectbox("Fornecedor *", options=[(s["id"], s["name"]) for s in sups],
                               format_func=lambda x: x[1] if isinstance(x, tuple) else x, key="ap_sup")
        colf1, colf2, colf3 = st.columns([1,1,2])
        with colf1:
            ap_due = st.date_input("Vencimento *", value=hoje, key="ap_due")
        with colf2:
            ap_val = st.number_input("Valor *", min_value=0.01, value=100.00, step=0.01, format="%.2f", key="ap_val")
        with colf3:
            ap_note = st.text_input("Observação", key="ap_note")

        if st.button("Salvar título", key="btn_add_pay"):
            if not sup_opt or not sup_opt[0]:
                st.error("Selecione um fornecedor válido.")
            else:
                # categoria padrão para saídas de compras/estoque
                cat_out_default = _ensure_cash_category('OUT', 'Compras/Estoque')
                # ALTERADO: insere já com method e category_id; purchase_id = NULL
                qexec("""
                    insert into resto.payable
                      (supplier_id, due_date, amount, status, method, category_id, note, purchase_id)
                    values
                      (%s,         %s,       %s,     'ABERTO', %s,     %s,          %s,  %s);
                """, (int(sup_opt[0]), ap_due, float(ap_val), 'outro', int(cat_out_default), ap_note or None, None))
                st.success("Título incluído.")
                _rerun()

    # ---------- filtros ----------
    card_start()
    st.subheader("Filtros")
    c1, c2, c3, c4 = st.columns([1,1,2,1])
    with c1:
        f_de = st.date_input("De", value=hoje, key="ag_f_de")
    with c2:
        f_ate = st.date_input("Até", value=ate30, key="ag_f_ate")
    with c3:
        sups_all = qall("select id, name from resto.supplier order by name;") or []
        sup_opts = [(0, "— todos —")] + [(s["id"], s["name"]) for s in sups_all]
        f_sup = st.selectbox("Fornecedor", options=sup_opts, format_func=lambda x: x[1], key="ag_f_sup")
    with c4:
        f_lim = st.number_input("Limite", 50, 2000, 500, 50, key="ag_f_lim")
    f_text = st.text_input("Buscar em observação", key="ag_f_txt")

    wh = ["p.status='ABERTO'", "p.due_date >= %s", "p.due_date <= %s"]
    pr = [f_de, f_ate]
    if f_sup and f_sup[0] != 0:
        wh.append("p.supplier_id = %s")
        pr.append(int(f_sup[0]))
    if f_text.strip():
        wh.append("p.note ILIKE %s")
        pr.append(f"%{f_text.strip()}%")

    sql = f"""
        select p.id, p.purchase_id, p.supplier_id, s.name as fornecedor,
               p.due_date, p.amount, coalesce(p.note,'') as note
          from resto.payable p
          join resto.supplier s on s.id = p.supplier_id
         where {' and '.join(wh)}
         order by p.due_date asc, p.id asc
         limit %s;
    """
    pr2 = tuple(pr + [int(f_lim)])
    rows = qall(sql, pr2) or []
    df = pd.DataFrame(rows)
    card_end()

    # ---------- grid + ações ----------
    card_start()
    st.subheader("Agenda de pagamentos")

    if df.empty:
        st.caption("Nenhum título para os filtros.")
        card_end()
        return

    # colunas auxiliares
    today = date.today()

    def _tag(d):
        if d < today: return "🔴 vencido"
        if d == today: return "🟠 hoje"
        if d <= today + timedelta(days=7): return "🟡 7 dias"
        return "🟢 futuro"

    df["situação"] = df["due_date"].apply(_tag)
    df["dias"] = (df["due_date"] - today).apply(lambda x: x.days)
    df["Pagar?"] = False
    df["Cancelar?"] = False

    cfg = {
        "id":           st.column_config.NumberColumn("ID", disabled=True),
        "fornecedor":   st.column_config.TextColumn("Fornecedor", disabled=True),
        "due_date":     st.column_config.DateColumn("Vencimento"),
        "dias":         st.column_config.NumberColumn("Dias", disabled=True),
        "situação":     st.column_config.TextColumn("Situação", disabled=True),
        "amount":       st.column_config.NumberColumn("Valor", step=0.01, format="%.2f"),
        "note":         st.column_config.TextColumn("Observação"),
        "Pagar?":       st.column_config.CheckboxColumn("Pagar agora?"),
        "Cancelar?":    st.column_config.CheckboxColumn("Cancelar?"),
    }

    edited = st.data_editor(
        df[["id","fornecedor","due_date","dias","situação","amount","note","Pagar?","Cancelar?"]],
        column_config=cfg,
        hide_index=True,
        num_rows="fixed",
        key="agenda_grid",
        use_container_width=True
    )

    st.markdown("##### Ações em lote")
    a1, a2, a3, a4 = st.columns([1.2, 1.2, 1.2, 2])
    with a1:
        do_save = st.button("💾 Salvar edições")
    with a2:
        do_pay = st.button("💸 Pagar selecionados")
    with a3:
        do_cancel = st.button("🗑️ Cancelar selecionados")
    with a4:
        csv = edited.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Exportar CSV", data=csv, file_name="agenda_contas.csv", mime="text/csv")

    # parâmetros de pagamento
    st.markdown("###### Parâmetros para pagamento")
    b1, b2, b3 = st.columns([1,1,2])
    with b1:
        pay_dt = st.date_input("Data do pagamento", value=today, key="ag_pay_dt")
    with b2:
        pay_method = st.selectbox("Método", METHODS, key="ag_pay_method")
    with b3:
        cat_out_default = _ensure_cash_category('OUT', 'Compras/Estoque')
        cats_out = qall("select id, name from resto.cash_category where kind='OUT' order by name;") or []
        cats_ordered = sorted(cats_out, key=lambda r: (0 if r["id"] == cat_out_default else 1, r["name"]))
        pay_cat = st.selectbox("Categoria (saída)", options=[(r["id"], r["name"]) for r in cats_ordered],
                               format_func=lambda x: x[1] if isinstance(x, tuple) else x,
                               key="ag_pay_cat")

    # aplicar edições básicas (due_date, amount, note)
    if do_save:
        orig = df.set_index("id")
        new  = edited.set_index("id")
        upd = err = 0
        for pid in new.index.tolist():
            a, b = orig.loc[pid], new.loc[pid]
            if any(str(a.get(f,"")) != str(b.get(f,"")) for f in ["due_date","amount","note"]):
                try:
                    qexec("""
                        update resto.payable
                           set due_date=%s, amount=%s, note=%s
                         where id=%s and status='ABERTO';
                    """, (b["due_date"], float(b["amount"]), (b.get("note") or None), int(pid)))
                    upd += 1
                except Exception:
                    err += 1
        st.success(f"✅ {upd} atualizado(s) • ⚠️ {err} erro(s)")
        _rerun()

    # cancelar selecionados
    if do_cancel:
        new = edited.set_index("id")
        ids = [int(i) for i in new.index.tolist() if bool(new.loc[i, "Cancelar?"])]
        ok = bad = 0
        for pid in ids:
            try:
                qexec("update resto.payable set status='CANCELADO' where id=%s and status='ABERTO';", (pid,))
                ok += 1
            except Exception:
                bad += 1
        st.success(f"🗑️ {ok} cancelado(s) • ⚠️ {bad} erro(s)")
        _rerun()

    # pagar selecionados -> gera saída no cashbook e baixa o título
    if do_pay:
        new = edited.set_index("id")
        ids = [int(i) for i in new.index.tolist() if bool(new.loc[i, "Pagar?"])]
        if not ids:
            st.warning("Selecione pelo menos um título em 'Pagar?'.")
        else:
            cat_id = int(pay_cat[0]) if isinstance(pay_cat, tuple) else cat_out_default
            ok = bad = 0
            for pid in ids:
                try:
                    row = qone("""
                        select p.id, p.amount, s.name as fornecedor
                          from resto.payable p
                          join resto.supplier s on s.id = p.supplier_id
                         where p.id=%s and p.status='ABERTO';
                    """, (pid,))
                    if not row:
                        bad += 1
                        continue
                    desc = f"Pagamento título #{pid} – {row['fornecedor']}"
                    _record_cashbook('OUT', cat_id, pay_dt, desc, float(row["amount"]), pay_method)
                    qexec("""
                        update resto.payable
                           set status='PAGO', paid_at=%s, method=%s, category_id=%s
                         where id=%s;
                    """, (pay_dt, pay_method, cat_id, pid))
                    ok += 1
                except Exception:
                    bad += 1
            st.success(f"💸 {ok} pago(s) • ⚠️ {bad} com erro")
            _rerun()

    card_end()


# ===================== RELATÓRIOS =====================
def page_relatorios():
    import pandas as pd
    from datetime import date
    from io import BytesIO

    def _brl(v: float) -> str:
        try:
            n = float(v or 0)
        except Exception:
            n = 0.0
        s = f"R$ {n:,.2f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")

    def _ensure_reportlab_runtime() -> bool:
        try:
            import reportlab  # noqa
            return True
        except Exception:
            st.info("📄 Para exportar PDF, instale o pacote `reportlab`.")
            if st.button("Instalar reportlab agora"):
                import sys, subprocess
                with st.spinner("Instalando reportlab..."):
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "reportlab>=4.0,<5"])
                st.success("Reportlab instalado. Recarregando…")
                try:
                    st.rerun()
                except Exception:
                    try:
                        st.experimental_rerun()
                    except Exception:
                        pass
            return False

    def _build_pdf_bytes(dt_ini, dt_fim, df_show, tot_e, tot_s, tot_res) -> bytes:
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.pagesizes import A4, landscape

        buf = BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=landscape(A4), leftMargin=24, rightMargin=24, topMargin=18, bottomMargin=18)
        styles = getSampleStyleSheet()
        story = []

        titulo = Paragraph(f"<b>Relatório Financeiro</b> — {dt_ini:%d/%m/%Y} a {dt_fim:%d/%m/%Y}", styles['Title'])
        story.append(titulo)
        story.append(Spacer(1, 8))

        # Totais (tabela pequena)
        tot_data = [
            ["Entradas (período)", "Saídas (período)", "Resultado (E − S)"],
            [_brl(tot_e), _brl(tot_s), _brl(tot_res)]
        ]
        tot_tbl = Table(tot_data, colWidths=[220, 220, 220])
        tot_tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#F0F0F0")),
            ('TEXTCOLOR',  (0,0), (-1,0), colors.black),
            ('ALIGN',      (0,0), (-1,-1), 'CENTER'),
            ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0,0), (-1,0), 10),
            ('BOTTOMPADDING',(0,0),(-1,0), 8),
            ('GRID',       (0,0), (-1,-1), 0.25, colors.grey),
        ]))
        story.append(tot_tbl)
        story.append(Spacer(1, 12))

        # Grid por categoria
        data = [["Categoria", "Entradas", "Saídas", "Saldo"]] + [
            [r["categoria"], _brl(r["entradas"]), _brl(r["saidas"]), _brl(r["saldo"])]
            for _, r in df_show.iterrows()
        ]
        tbl = Table(data, colWidths=[300, 150, 150, 150])
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#E8EEF7")),
            ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
            ('ALIGN',      (1,1), (-1,-1), 'RIGHT'),
            ('ALIGN',      (0,0), (0,-1), 'LEFT'),
            ('GRID',       (0,0), (-1,-1), 0.25, colors.grey),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor("#FAFAFA")]),
        ]))
        story.append(tbl)

        doc.build(story)
        return buf.getvalue()

    header("📑 Relatórios", "Exportações oficiais (PDF/CSV).")
    tabs = st.tabs(["💰 Financeiro"])

    with tabs[0]:
        card_start()
        st.subheader("Financeiro — Exportação")

        # Filtros mínimos
        c1, c2 = st.columns(2)
        with c1:
            dt_ini = st.date_input("De", value=date.today().replace(day=1), key="rel_fin_dtini")
        with c2:
            dt_fim = st.date_input("Até", value=date.today(), key="rel_fin_dtfim")

        cats = qall("select id, name from resto.cash_category order by name;") or []
        cat_opts = [(c["id"], c["name"]) for c in cats]
        sel_cats = st.multiselect(
            "Categorias (opcional)",
            options=cat_opts,
            format_func=lambda x: x[1],
            key="rel_fin_cats"
        )
        sel_ids = [c[0] for c in sel_cats] if sel_cats else []

        # Consulta (sem exibir na tela)
        wh = ["cb.entry_date >= %s", "cb.entry_date <= %s"]
        pr = [dt_ini, dt_fim]
        if sel_ids:
            wh.append("cb.category_id = ANY(%s)")
            pr.append(sel_ids)

        sql = f"""
            select
                coalesce(c.name, '(sem categoria)') as categoria,
                sum(case when cb.kind='IN'  then cb.amount else 0 end)  as entradas_raw,
                sum(case when cb.kind='OUT' then cb.amount else 0 end)  as saidas_raw
            from resto.cashbook cb
            left join resto.cash_category c on c.id = cb.category_id
            where {' and '.join(wh)}
            group by categoria
            order by categoria;
        """
        rows = qall(sql, tuple(pr)) or []
        df = pd.DataFrame(rows)

        # Se houver dados, prepara os arquivos; senão, avisa
        if not df.empty:
            df["entradas"] = df["entradas_raw"].astype(float)
            # garante saídas positivas (independe de como foi gravado)
            df["saidas"] = df["saidas_raw"].astype(float).abs()
            df["saldo"] = df["entradas"] - df["saidas"]
            df_show = df[["categoria", "entradas", "saidas", "saldo"]].sort_values("categoria")

            tot_e = float(df_show["entradas"].sum())
            tot_s = float(df_show["saidas"].sum())
            tot_res = float(df_show["saldo"].sum())

            # CSV
            csv_bytes = df_show.rename(columns={
                "categoria": "Categoria",
                "entradas": "Entradas",
                "saidas": "Saídas",
                "saldo": "Saldo"
            }).to_csv(index=False).encode("utf-8")

            # PDF
            has_pdf = _ensure_reportlab_runtime()
            if has_pdf:
                pdf_bytes = _build_pdf_bytes(dt_ini, dt_fim, df_show, tot_e, tot_s, tot_res)

            # Botões de download (sem mostrar grid ou métricas)
            colb1, colb2 = st.columns(2)
            with colb1:
                st.download_button(
                    "⬇️ Baixar CSV (Financeiro)",
                    data=csv_bytes,
                    file_name=f"relatorio_financeiro_{dt_ini:%Y%m%d}_{dt_fim:%Y%m%d}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            with colb2:
                if has_pdf:
                    st.download_button(
                        "📄 Baixar PDF (paisagem)",
                        data=pdf_bytes,
                        file_name=f"relatorio_financeiro_{dt_ini:%Y%m%d}_{dt_fim:%Y%m%d}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
                else:
                    st.caption("PDF indisponível (instale o reportlab para habilitar).")
        else:
            st.info("Sem lançamentos para o período/filtro escolhido. Ajuste os filtros para habilitar os downloads.")

        card_end()




# ===================== Router =====================
def main():
    if not ensure_ping():
        st.stop()
    ensure_migrations()

    #header("🍝 Restô ERP Lite", "Financeiro • Fiscal-ready • Estoque • Ficha técnica • Preços • Produção")
    header(
            " SISTEMA DE GESTÃO GET GLUTEN FREE",
            "Financeiro • Fiscal • Estoque • Ficha técnica • Preços • Produção • DRE • Livro Caixa",
            logo="img/logoget8.png",       # caminho local no repo (ou)
            # logo="https://seu-dominio.com/logo.png",  # URL externa
            logo_height=92
        )
    page = st.sidebar.radio("Menu", ["Painel", "Cadastros", "Compras", "Vendas", "Receitas & Preços", "Produção", "Estoque", "Financeiro","Agenda de Contas","Relatórios","Importar Extrato"], index=0)

    if page == "Painel": page_dashboard()
    elif page == "Cadastros": page_cadastros()
    elif page == "Compras": page_compras()
    elif page == "Vendas": page_vendas()
    elif page == "Receitas & Preços": page_receitas_precos()
    elif page == "Produção": page_producao()
    elif page == "Estoque": page_estoque()
    elif page == "Financeiro": page_financeiro()
    elif page == "Agenda de Contas": page_agenda_contas()   # <— NOVO
    elif page == "Relatórios": page_relatorios()
    elif page == "Importar Extrato": page_importar_extrato()

if __name__ == "__main__":
    main()
