
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

# ===================== IMPORTAÇÕES IFOOD – SCHEMA =====================
def _ensure_ifood_schema():
    """
    Garante as tabelas de importação de arquivos do iFood.
    - resto.ifood_import_batch: 1 linha por arquivo importado
    - resto.ifood_import_row:   1 linha por registro do arquivo (dados em JSON)
    """
    qexec("""
    do $$
    begin
      -- Tabela de lote de importação
      if not exists (
        select 1
          from information_schema.tables
         where table_schema = 'resto'
           and table_name   = 'ifood_import_batch'
      ) then
        create table resto.ifood_import_batch (
          id          bigserial primary key,
          file_name   text        not null,
          file_type   text        not null, -- ex.: 'conciliacao', 'pedidos'
          imported_at timestamptz not null default now()
        );
      end if;

      -- Tabela de linhas importadas
      if not exists (
        select 1
          from information_schema.tables
         where table_schema = 'resto'
           and table_name   = 'ifood_import_row'
      ) then
        create table resto.ifood_import_row (
          id         bigserial primary key,
          batch_id   bigint      not null,
          row_number int         not null,
          order_id   text,
          data       jsonb       not null
        );
      end if;

      -- FK + cascade delete
      begin
        alter table resto.ifood_import_row
          add constraint ifood_import_row_batch_fk
          foreign key (batch_id)
          references resto.ifood_import_batch(id)
          on delete cascade;
      exception
        when duplicate_object then null;
      end;

      -- Unique por lote + número da linha (evita duplicar dentro do mesmo lote)
      begin
        if not exists (
          select 1
            from pg_constraint
           where conname = 'ifood_import_row_batch_row_uq'
             and conrelid = 'resto.ifood_import_row'::regclass
        ) then
          alter table resto.ifood_import_row
            add constraint ifood_import_row_batch_row_uq
            unique (batch_id, row_number);
        end if;
      exception
        when undefined_table then null;
      end;

      -- Índice pra buscar por order_id rapidamente
      begin
        create index if not exists ifood_import_row_order_id_idx
          on resto.ifood_import_row(order_id);
      exception
        when undefined_table then null;
      end;
    end $$;
    """)


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

            # --- normalizador de unidades + conversão robusta ---
            def _norm_unit(u: str) -> str:
                u = (u or "").strip().lower()
                mapa = {
                    "g": "g", "grama": "g", "gramas": "g",
                    "kg": "kg", "quilo": "kg", "kilograma": "kg", "kilogramas": "kg",
                    "ml": "ml", "mililitro": "ml", "mililitros": "ml",
                    "l": "L", "lt": "L", "lts": "L", "litro": "L", "litros": "L",
                    "un": "un", "unid": "un", "unidade": "un", "unidades": "un",
                }
                return mapa.get(u, u or "")
        
            def _convert_qty(q, from_abbr, to_abbr):
                fa = _norm_unit(from_abbr)
                ta = _norm_unit(to_abbr)
                q = float(q or 0)
                if not fa or not ta or fa == ta:
                    return q, False
                pairs = {
                    ("g", "kg"): 1/1000, ("kg", "g"): 1000,
                    ("ml", "L"): 1/1000, ("L", "ml"): 1000,
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
#===================================================CANCELAR PRODUÇÃO=====================================================================
def page_producao_cancelar():
    import pandas as pd
    from datetime import date

    # ---------- helpers ----------
    def _rerun():
        try:
            st.rerun()
        except Exception:
            if hasattr(st, "experimental_rerun"):
                st.experimental_rerun()

    # garante colunas novas (compatível com bases antigas)
    qexec("""
    do $$
    begin
      begin
        alter table resto.production add column if not exists status text;
      exception when duplicate_column then null; end;
      begin
        alter table resto.production add column if not exists canceled_at timestamptz;
      exception when duplicate_column then null; end;
      begin
        alter table resto.production add column if not exists cancel_note text;
      exception when duplicate_column then null; end;
      update resto.production set status='FECHADA' where status is null;
    end $$;
    """)

    # estorno de produção (feito em Python, chamando a SP de movimento)
    def _cancel_production(pid: int, note: str = ""):
        try:
            p = qone("""
                select p.*, pr.name as product_name
                  from resto.production p
                  join resto.product pr on pr.id = p.product_id
                 where p.id=%s;
            """, (int(pid),))
            if not p:
                return False, "Produção não encontrada."

            if (p.get("status") or "").upper() == "CANCELADA":
                return False, "Esta produção já está CANCELADA."

            qty_final = float(p.get("qty") or 0.0)
            prod_id   = int(p["product_id"])
            out_note  = f"revert:production:{int(pid)}"

            # 1) Saída do produto final (remove o que entrou na produção)
            uc = p.get("unit_cost")
            uc_param = float(uc) if uc is not None else None
            try:
                qexec(
                    "select resto.sp_register_movement(%s,'OUT',%s,%s,'production_revert',%s,%s);",
                    (prod_id, qty_final, uc_param, int(pid), out_note),
                )
            except Exception:
                return False, "Falha ao registrar saída do produto acabado."

            # 2) Devolução dos insumos (IN) – usa a lista atual de itens do lote
            items = qall("""
                select id, ingredient_id, qty, unit_cost, lot_id
                  from resto.production_item
                 where production_id=%s
                 order by id;
            """, (int(pid),)) or []

            for it in items:
                ing_id = int(it["ingredient_id"])
                qty    = float(it.get("qty") or 0.0)
                uc_i   = it.get("unit_cost")
                uc_i   = float(uc_i) if uc_i is not None else None
                lot_id = it.get("lot_id")
                note_item = f"revert:production:{int(pid)}" + (f";lot:{int(lot_id)}" if lot_id else "")
                qexec(
                    "select resto.sp_register_movement(%s,'IN',%s,%s,'production_revert',%s,%s);",
                    (ing_id, qty, uc_i, int(it["id"]), note_item),
                )

            # 3) Marca o cabeçalho como cancelado
            qexec(
                "update resto.production set status='CANCELADA', canceled_at=now(), cancel_note=%s where id=%s;",
                (note or None, int(pid)),
            )

            return True, f"Produção #{int(pid)} cancelada e estoques estornados."
        except Exception:
            return False, "Erro inesperado ao cancelar a produção."

    # ---------- UI ----------
    header("⛔ Cancelar produção", "Ajuste os itens do lote (se necessário) e cancele com estorno de estoque.")

    # lista recentes
    rows = qall("""
        select p.id,
               p.date::date                 as data,
               pr.name                      as produto,
               coalesce(p.qty,0)            as qtd_final,
               coalesce(p.unit_cost,0)      as custo_unit,
               coalesce(p.total_cost,0)     as custo_total,
               coalesce(p.status,'FECHADA') as status
          from resto.production p
          join resto.product pr on pr.id = p.product_id
         order by p.id desc
         limit 100;
    """) or []

    card_start()
    st.subheader("Produções recentes")
    df_list = pd.DataFrame(rows)
    if df_list.empty:
        st.info("Nenhuma produção encontrada.")
        card_end()
        return

    st.dataframe(
        df_list,
        use_container_width=True,
        hide_index=True,
        column_config={
            "qtd_final":   st.column_config.NumberColumn("qtd_final",   format="%.3f"),
            "custo_unit":  st.column_config.NumberColumn("custo_unit",  format="%.2f"),
            "custo_total": st.column_config.NumberColumn("custo_total", format="%.2f"),
        }
    )

    c1, c2 = st.columns([1, 2])
    with c1:
        only_open = st.checkbox("Mostrar apenas NÃO canceladas", value=True)
    with c2:
        pid_opt = [
            (int(r["id"]), f"#{int(r['id'])} • {r['data']} • {r['produto']} • {r['status']}")
            for _, r in df_list.iterrows()
            if not (only_open and str(r.get("status","")).upper() == "CANCELADA")
        ]
        sel = st.selectbox("Escolha a produção", options=pid_opt,
                           format_func=lambda x: x[1] if isinstance(x, tuple) else x)

    if not sel:
        card_end()
        return

    sel_id = int(sel[0])

    # ---------- BLOCO DE EDIÇÃO DOS ITENS DO LOTE ----------
    st.markdown("### ✏️ Editar itens do lote")
    st.caption("As alterações **não mexem no estoque agora**; o estoque só muda quando você clicar **Cancelar produção**.")

    # soma original p/ fator de cabeçalho
    orig_sum_row = qone("select coalesce(sum(total_cost),0) s from resto.production_item where production_id=%s;", (sel_id,))
    orig_sum = float((orig_sum_row or {}).get("s") or 0.0)

    itens = qall("""
        select
            pi.id,
            pr.name        as ingrediente,
            pr.unit        as unidade,         -- unidade do insumo (do cadastro)
            pi.qty,
            pi.unit_cost,
            pi.total_cost,
            pi.lot_id,
            pi.ingredient_id
          from resto.production_item pi
          join resto.product pr on pr.id = pi.ingredient_id
         where pi.production_id=%s
         order by pi.id;
    """, (sel_id,)) or []
    df = pd.DataFrame(itens)

    if df.empty:
        st.info("Este lote não possui itens.")
    else:
        df["Excluir?"] = False
        cfg = {
            "id":            st.column_config.NumberColumn("ID", disabled=True),
            "ingrediente":   st.column_config.TextColumn("Ingrediente", disabled=True),
            "unidade":       st.column_config.TextColumn("Unidade", disabled=True),   # coluna unidade no grid
            "qty":           st.column_config.NumberColumn("Quantidade", step=0.001, format="%.3f"),
            # <<< MOSTRAR COMO MOEDA (2 casas) >>>
            "unit_cost":     st.column_config.NumberColumn("Custo unitário", step=0.01, format="%.2f"),
            "total_cost":    st.column_config.NumberColumn("Subtotal", disabled=True, format="%.2f"),
            "lot_id":        st.column_config.NumberColumn("Lote (id)", disabled=True),
            "ingredient_id": st.column_config.NumberColumn("ingredient_id", disabled=True),
            "Excluir?":      st.column_config.CheckboxColumn("Excluir?"),
        }
        edited = st.data_editor(
            df[["id","ingrediente","unidade","qty","unit_cost","total_cost","lot_id","ingredient_id","Excluir?"]],
            column_config=cfg, hide_index=True, num_rows="fixed",
            key=f"edit_items_{sel_id}", use_container_width=True
        )

        cA, cB, cC = st.columns([1.2,1.2,1.6])
        with cA:
            do_save_items = st.button("💾 Salvar alterações nos itens", key=f"save_items_{sel_id}")
        with cB:
            do_reload = st.button("🔄 Recarregar", key=f"reload_items_{sel_id}")
        with cC:
            st.caption("Edite **Quantidade** e **Custo unitário**; **Unidade** é do cadastro do insumo.")

        if do_reload:
            _rerun()

        if do_save_items:
            orig = df.set_index("id")
            new  = edited.set_index("id")
            upd = delc = err = 0

            # deletar marcados
            to_del = new.index[new["Excluir?"] == True].tolist()
            for iid in to_del:
                try:
                    qexec("delete from resto.production_item where id=%s;", (int(iid),))
                    delc += 1
                except Exception:
                    err += 1

            # atualizar alterados
            keep = [i for i in new.index if i not in to_del]
            for iid in keep:
                a = orig.loc[iid]; b = new.loc[iid]
                changed = (float(a.get("qty") or 0) != float(b.get("qty") or 0)) or \
                          (float(a.get("unit_cost") or 0) != float(b.get("unit_cost") or 0))
                if not changed:
                    continue
                try:
                    qty   = float(b.get("qty") or 0.0)
                    ucost = float(b.get("unit_cost") or 0.0)
                    tcost = round(qty * ucost, 2)  # total sempre em 2 casas
                    qexec("""
                        update resto.production_item
                           set qty=%s, unit_cost=%s, total_cost=%s
                         where id=%s;
                    """, (qty, ucost, tcost, int(iid)))
                    upd += 1
                except Exception:
                    err += 1

            # recalcula cabeçalho preservando fator de overhead/perdas
            hdr = qone("select coalesce(total_cost,0) as tot, coalesce(qty,0) as qty from resto.production where id=%s;", (sel_id,))
            prev_total   = float((hdr or {}).get("tot") or 0.0)
            prev_ing_sum = float(orig_sum or 0.0)
            factor = (prev_total / prev_ing_sum) if prev_ing_sum > 0 else 1.0

            new_sum_row = qone("select coalesce(sum(total_cost),0) s from resto.production_item where production_id=%s;", (sel_id,))
            new_sum   = float((new_sum_row or {}).get("s") or 0.0)
            new_total = new_sum * factor
            qty_final = float((hdr or {}).get("qty") or 0.0)
            new_unit  = (new_total / qty_final) if qty_final > 0 else 0.0

            try:
                qexec("update resto.production set total_cost=%s, unit_cost=%s where id=%s;", (new_total, new_unit, sel_id))
            except Exception:
                pass

            st.success(f"Itens: ✅ {upd} atualizado(s) • 🗑️ {delc} excluído(s) • ⚠️ {err} erro(s).")
            _rerun()

    # ---------- adicionar novo item ao lote ----------
    st.markdown("### ➕ Adicionar item ao lote")
    prods_ing = qall("""
        select id, name, unit
          from resto.product
         where coalesce(is_ingredient,true) is true and coalesce(active,true) is true
         order by name;
    """) or qall("select id, name, unit from resto.product order by name;") or []
    opts_ing = [(r["id"], f"{r['name']} [{r.get('unit') or 'un'}]") for r in prods_ing]

    c1, c2, c3 = st.columns([2,1,1])
    with c1:
        novo_ing = st.selectbox("Ingrediente", options=opts_ing,
                                format_func=lambda x: x[1] if isinstance(x, tuple) else x,
                                key=f"new_ing_{sel_id}")
    with c2:
        novo_qty = st.number_input("Quantidade", min_value=0.001, step=0.001, value=1.000, format="%.3f",
                                   key=f"new_qty_{sel_id}")
    with c3:
        # <<< 2 casas decimais no input >>>
        novo_uc  = st.number_input("Custo unitário", min_value=0.0, step=0.01, value=0.00, format="%.2f",
                                   key=f"new_uc_{sel_id}")

    if st.button("Adicionar ao lote", key=f"btn_add_{sel_id}") and novo_ing:
        try:
            tcost = round(float(novo_qty) * float(novo_uc), 2)
            qexec("""
                insert into resto.production_item(production_id, ingredient_id, qty, unit_cost, total_cost)
                values (%s,%s,%s,%s,%s);
            """, (sel_id, int(novo_ing[0]), float(novo_qty), float(novo_uc), tcost))
            # atualiza cabeçalho mantendo fator
            hdr = qone("select coalesce(total_cost,0) as tot, coalesce(qty,0) as qty from resto.production where id=%s;", (sel_id,))
            prev_total  = float((hdr or {}).get("tot") or 0.0)
            new_sum_all = float(qone("select coalesce(sum(total_cost),0) s from resto.production_item where production_id=%s;", (sel_id,))["s"] or 0.0)
            old_sum     = max(new_sum_all - tcost, 0.0)
            factor      = (prev_total / old_sum) if old_sum > 0 else 1.0
            new_total   = new_sum_all * factor
            qty_final   = float((hdr or {}).get("qty") or 0.0)
            new_unit    = (new_total / qty_final) if qty_final > 0 else 0.0
            try:
                qexec("update resto.production set total_cost=%s, unit_cost=%s where id=%s;", (new_total, new_unit, sel_id))
            except Exception:
                pass

            st.success("Item incluído no lote.")
            _rerun()
        except Exception:
            st.error("Falha ao incluir item.")

    st.divider()
    # ---------- CANCELAR A PRODUÇÃO ----------
    st.warning("⚠️ Ao cancelar: o produto final sai do estoque e **todos os itens do lote (atuais)** retornam ao estoque.")
    motivo = st.text_input("Motivo do cancelamento (opcional)")
    if st.button("⛔ Cancelar esta produção agora", type="primary"):
        ok, msg = _cancel_production(sel_id, motivo)
        if ok:
            st.success(msg)
            _rerun()
        else:
            st.error(msg)

    card_end()


# =================================== LISTA DE COMPRAS ===================================
def page_lista_compras():
    import pandas as pd
    from io import BytesIO
    from datetime import date

    # ---------- helpers ----------
    def _rerun():
        try:
            st.rerun()
        except Exception:
            if hasattr(st, "experimental_rerun"):
                st.experimental_rerun()

    def _ensure_schema():
        # Tabela única e simples p/ lista de compras
        qexec("""
        do $$
        begin
          create table if not exists resto.shopping_item(
            id          bigserial primary key,
            product_id  bigint references resto.product(id),
            name        text not null,
            qty         numeric(14,3) not null default 1,
            unit        text,
            checked     boolean not null default false,
            note        text,
            created_at  timestamptz not null default now()
          );
          create index if not exists shop_checked_idx on resto.shopping_item(checked);
          create index if not exists shop_created_idx on resto.shopping_item(created_at);
        end $$;
        """)

    def _build_pdf(rows, titulo="Lista de Compras", subtitulo="", landscape_flag=False, show_checkboxes=True):
        # Gera um PDF simples com tabela
        try:
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
            from reportlab.lib.pagesizes import A4, landscape, portrait
            from reportlab.lib.styles import getSampleStyleSheet
            from reportlab.lib import colors
        except Exception:
            return None, "ReportLab não está disponível neste ambiente. Instale o pacote para gerar PDF."

        buffer = BytesIO()
        pagesize = landscape(A4) if landscape_flag else portrait(A4)

        doc = SimpleDocTemplate(buffer, pagesize=pagesize,
                                leftMargin=24, rightMargin=24, topMargin=28, bottomMargin=24)
        styles = getSampleStyleSheet()
        story = []

        title = Paragraph(f"<b>{titulo}</b>", styles["Title"])
        story.append(title)
        if subtitulo:
            story.append(Paragraph(subtitulo, styles["Normal"]))
        story.append(Spacer(1, 10))

        # Monta dados da tabela
        th = ["", "Item", "Qtd", "Un", "Obs"] if show_checkboxes else ["Item", "Qtd", "Un", "Obs"]
        data = [th]

        for r in rows:
            nome = r.get("name") or ""
            qtd  = float(r.get("qty") or 0)
            un   = r.get("unit") or ""
            obs  = r.get("note") or ""
            box  = "☐" if show_checkboxes else None
            row = ([box, nome, f"{qtd:.3f}", un, obs] if show_checkboxes
                   else [nome, f"{qtd:.3f}", un, obs])
            data.append(row)

        tbl = Table(data, repeatRows=1)
        base_style = [
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 10),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("FONTSIZE", (0, 1), (-1, -1), 9),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]
        tbl.setStyle(TableStyle(base_style))
        story.append(tbl)

        story.append(Spacer(1, 8))
        story.append(Paragraph(f"Gerado em {date.today().strftime('%d/%m/%Y')}", styles["Normal"]))

        try:
            doc.build(story)
            pdf = buffer.getvalue()
            buffer.close()
            return pdf, None
        except Exception:
            return None, "Falha ao montar o PDF."

    _ensure_schema()

    header("🛒 Lista de Compras", "Marque, edite e gere um PDF para impressão.")

    # -------- Adicionar itens --------
    card_start()
    st.subheader("➕ Adicionar item")

    # Carrega produtos (opcional, para autopreencher nome/unidade)
    prods = qall("select id, name, unit from resto.product where coalesce(active,true) is true order by name;") or []
    prod_opts = [(p["id"], f"{p['name']} [{p.get('unit') or 'un'}]") for p in prods]

    c1, c2 = st.columns([2, 1])
    with c1:
        prod_sel = st.selectbox("Produto (opcional)", options=[(0, "— usar nome livre —")] + prod_opts,
                                format_func=lambda x: x[1] if isinstance(x, tuple) else x)
    with c2:
        nome_livre = st.text_input("Nome livre (se não usar produto)")

    c3, c4, c5 = st.columns([1, 1, 2])
    with c3:
        qty_new = st.number_input("Quantidade", min_value=0.001, step=0.001, value=1.000, format="%.3f")
    with c4:
        # unidade padrão do produto, se houver
        default_unit = ""
        if isinstance(prod_sel, tuple) and prod_sel[0] != 0:
            try:
                prow = next((r for r in prods if r["id"] == prod_sel[0]), None)
                default_unit = (prow or {}).get("unit") or ""
            except Exception:
                pass
        unit_new = st.text_input("Unidade", value=default_unit)
    with c5:
        note_new = st.text_input("Observação", value="")

    add_btn = st.button("Adicionar à lista")
    if add_btn:
        if (isinstance(prod_sel, tuple) and prod_sel[0] != 0):
            # via produto
            pid = int(prod_sel[0])
            prow = next((r for r in prods if r["id"] == pid), {})
            name = prow.get("name") or nome_livre or ""
            if not name:
                st.warning("Informe um nome para o item.")
            else:
                qexec("""
                    insert into resto.shopping_item(product_id, name, qty, unit, note)
                    values (%s,%s,%s,%s,%s);
                """, (pid, name, float(qty_new), (unit_new or None), (note_new or None)))
                st.success("Item adicionado.")
                _rerun()
        else:
            # nome livre
            if not (nome_livre or "").strip():
                st.warning("Informe um nome para o item (ou escolha um produto).")
            else:
                qexec("""
                    insert into resto.shopping_item(product_id, name, qty, unit, note)
                    values (null,%s,%s,%s,%s);
                """, (nome_livre.strip(), float(qty_new), (unit_new or None), (note_new or None)))
                st.success("Item adicionado.")
                _rerun()

    card_end()

    # -------- Grid de edição / checklist --------
    card_start()
    st.subheader("Lista atual")

    # filtros simples
    fcol1, fcol2, fcol3 = st.columns([1,1,2])
    with fcol1:
        f_show_only_open = st.checkbox("Mostrar somente não ticados", value=False)
    with fcol2:
        f_order = st.selectbox("Ordenar por", ["status/nome", "nome", "criação decresc."], index=0)
    with fcol3:
        f_text = st.text_input("Buscar por nome/obs", value="")

    wh = []
    pr = []
    if f_show_only_open:
        wh.append("checked is false")
    if f_text.strip():
        wh.append("(name ilike %s or coalesce(note,'') ilike %s)")
        pr += [f"%{f_text.strip()}%", f"%{f_text.strip()}%"]

    sql = "select id, product_id, name, qty, coalesce(unit,'') as unit, checked, coalesce(note,'') as note from resto.shopping_item"
    if wh:
        sql += " where " + " and ".join(wh)

    if f_order == "status/nome":
        sql += " order by checked asc, name asc"
    elif f_order == "nome":
        sql += " order by name asc"
    else:
        sql += " order by created_at desc"

    rows = qall(sql + ";", tuple(pr)) or []
    df = pd.DataFrame(rows)

    if df.empty:
        st.caption("Nenhum item na lista.")
        card_end()
    else:
        df["Excluir?"] = False
        colcfg = {
            "checked":   st.column_config.CheckboxColumn("✓"),
            "name":      st.column_config.TextColumn("Item"),
            "qty":       st.column_config.NumberColumn("Qtd", step=0.001, format="%.3f"),
            "unit":      st.column_config.TextColumn("Un"),
            "note":      st.column_config.TextColumn("Obs"),
            "Excluir?":  st.column_config.CheckboxColumn("Excluir?"),
            "id":        st.column_config.NumberColumn("ID", disabled=True),
            "product_id":st.column_config.NumberColumn("product_id", disabled=True),
        }
        edited = st.data_editor(
            df[["id","checked","name","qty","unit","note","Excluir?","product_id"]],
            hide_index=True, num_rows="fixed", use_container_width=True,
            column_config=colcfg, key="shop_grid"
        )

        cA, cB, cC, cD = st.columns([1.4,1.4,1.4,2])
        with cA:
            do_save = st.button("💾 Salvar alterações")
        with cB:
            do_del  = st.button("🗑️ Excluir selecionados")
        with cC:
            do_clear = st.button("🧹 Limpar TUDO")
        with cD:
            st.caption("Dica: marque ✓ para itens comprados (fica registrado).")

        if do_save:
            orig = df.set_index("id")
            new  = edited.set_index("id")
            upd = 0; err = 0
            for iid in new.index.tolist():
                a = orig.loc[iid]; b = new.loc[iid]
                changed = any([
                    bool(a.get("checked")) != bool(b.get("checked")),
                    str(a.get("name","")) != str(b.get("name","")),
                    float(a.get("qty") or 0) != float(b.get("qty") or 0),
                    str(a.get("unit","")) != str(b.get("unit","")),
                    str(a.get("note","")) != str(b.get("note",""))
                ])
                if not changed:
                    continue
                try:
                    qexec("""
                        update resto.shopping_item
                           set checked=%s, name=%s, qty=%s, unit=%s, note=%s
                         where id=%s;
                    """, (bool(b.get("checked")), str(b.get("name") or ""),
                          float(b.get("qty") or 0.0), (str(b.get("unit") or "") or None),
                          (str(b.get("note") or "") or None), int(iid)))
                    upd += 1
                except Exception:
                    err += 1
            st.success(f"✅ {upd} atualizado(s) • ⚠️ {err} erro(s).")
            _rerun()

        if do_del:
            new = edited.set_index("id")
            to_del = [int(i) for i in new.index.tolist() if bool(new.loc[i, "Excluir?"])]
            ok = bad = 0
            for iid in to_del:
                try:
                    qexec("delete from resto.shopping_item where id=%s;", (iid,))
                    ok += 1
                except Exception:
                    bad += 1
            st.success(f"🗑️ {ok} removido(s) • ⚠️ {bad} erro(s).")
            _rerun()

        if do_clear:
            qexec("delete from resto.shopping_item;", ())
            st.info("Lista esvaziada.")
            _rerun()

        card_end()

        # -------- PDF --------
        card_start()
        st.subheader("📄 Gerar PDF")
        g1, g2, g3 = st.columns([1.2,1.2,2])
        with g1:
            pdf_only_open = st.checkbox("Somente não ticados", value=True)
        with g2:
            pdf_land = st.checkbox("Paisagem (horizontal)", value=False)
        with g3:
            pdf_boxes = st.checkbox("Mostrar caixas de seleção no PDF", value=True)

        # prepara base
        base = edited if pdf_only_open else edited.copy()
        if pdf_only_open:
            base = base[base["checked"] == False]
        base = base[["name","qty","unit","note"]].sort_values("name")

        if st.button("⬇️ Baixar PDF da lista"):
            rows_pdf = base.to_dict(orient="records")
            subtit = "Apenas itens não ticados" if pdf_only_open else "Todos os itens (ticados e não ticados)"
            pdf_bytes, err_pdf = _build_pdf(rows_pdf, titulo="Lista de Compras", subtitulo=subtit,
                                            landscape_flag=pdf_land, show_checkboxes=pdf_boxes)
            if err_pdf:
                st.error(err_pdf)
            else:
                st.download_button("⬇️ Download PDF", data=pdf_bytes, file_name="lista_compras.pdf", mime="application/pdf")
        card_end()

# ===================== RH / FOLHA DE PAGAMENTO =====================
def page_folha():
    import pandas as pd
    import re
    import math
    from datetime import date, timedelta

    def _rerun():
        try:
            st.rerun()
        except Exception:
            if hasattr(st, "experimental_rerun"):
                st.experimental_rerun()

    def _ensure_folha_schema():
        qexec("""
        do $$
        begin
          -- ===================== FUNCIONÁRIOS =====================
          -- garante tabela (caso não exista)
          if not exists (
            select 1
              from information_schema.tables
             where table_schema = 'resto'
               and table_name   = 'employee'
          ) then
            create table resto.employee (
              id             bigserial primary key,
              name           text,
              cpf            text,
              role           text,
              admission_date date,
              dismissal_date date,
              weekly_salary  numeric(14,2) not null default 0,
              active         boolean not null default true,
              payment_method text,
              note           text
            );
          end if;

          -- garante colunas (caso tabela já exista de versões antigas)
          begin
            alter table resto.employee add column if not exists name           text;
          exception when duplicate_column then null; end;

          begin
            alter table resto.employee add column if not exists cpf            text;
          exception when duplicate_column then null; end;

          begin
            alter table resto.employee add column if not exists role           text;
          exception when duplicate_column then null; end;

          begin
            alter table resto.employee add column if not exists admission_date date;
          exception when duplicate_column then null; end;

          begin
            alter table resto.employee add column if not exists dismissal_date date;
          exception when duplicate_column then null; end;

          begin
            alter table resto.employee add column if not exists weekly_salary  numeric(14,2);
          exception when duplicate_column then null; end;

          begin
            alter table resto.employee alter column weekly_salary set default 0;
          exception when undefined_column then null; end;

          begin
            alter table resto.employee add column if not exists active         boolean;
          exception when duplicate_column then null; end;

          begin
            alter table resto.employee alter column active set default true;
          exception when undefined_column then null; end;

          begin
            alter table resto.employee add column if not exists payment_method text;
          exception when duplicate_column then null; end;

          begin
            alter table resto.employee add column if not exists note           text;
          exception when duplicate_column then null; end;

          -- unique opcional por CPF (permite vários NULL)
          begin
            if not exists (
              select 1
                from pg_constraint
               where conname = 'employee_cpf_uq'
                 and conrelid = 'resto.employee'::regclass
            ) then
              alter table resto.employee
                add constraint employee_cpf_uq unique (cpf);
            end if;
          exception
            when undefined_column then
              -- se ainda assim der problema com a coluna cpf, ignora a constraint
              null;
          end;

          -- ===================== FOLHA SEMANAL =====================
          if not exists (
            select 1
              from information_schema.tables
             where table_schema = 'resto'
               and table_name   = 'payroll_week'
          ) then
            create table resto.payroll_week (
              id              bigserial primary key,
              employee_id     bigint not null,
              ref_date        date not null,
              week_start      date not null,
              week_end        date not null,
              week_label      text not null,
              gross           numeric(14,2) not null,
              inss            numeric(14,2) not null default 0,
              other_discounts numeric(14,2) not null default 0,
              extras          numeric(14,2) not null default 0,
              net             numeric(14,2) not null,
              paid            boolean not null default false,
              paid_at         date,
              method          text,
              note            text
            );
          end if;

          -- garante colunas (caso tabela antiga exista com menos campos)
          begin
            alter table resto.payroll_week add column if not exists employee_id     bigint;
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists ref_date        date;
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists week_start      date;
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists week_end        date;
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists week_label      text;
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists gross           numeric(14,2);
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists inss            numeric(14,2);
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week alter column inss set default 0;
          exception when undefined_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists other_discounts numeric(14,2);
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week alter column other_discounts set default 0;
          exception when undefined_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists extras          numeric(14,2);
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week alter column extras set default 0;
          exception when undefined_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists net             numeric(14,2);
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists paid            boolean;
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week alter column paid set default false;
          exception when undefined_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists paid_at         date;
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists method          text;
          exception when duplicate_column then null; end;

          begin
            alter table resto.payroll_week add column if not exists note            text;
          exception when duplicate_column then null; end;

          -- FK para employee (se ainda não existir)
          begin
            alter table resto.payroll_week
              add constraint payroll_week_employee_fk
              foreign key (employee_id)
              references resto.employee(id)
              on delete cascade;
          exception when duplicate_object then null; end;

          -- unique por funcionário + data de referência (segunda da semana)
          begin
            if not exists (
              select 1
                from pg_constraint
               where conname = 'payroll_week_emp_ref_uq'
                 and conrelid = 'resto.payroll_week'::regclass
            ) then
              alter table resto.payroll_week
                add constraint payroll_week_emp_ref_uq unique (employee_id, ref_date);
            end if;
          exception when undefined_column then null; end;
        end $$;
        """)

    def _week_range(d: date):
        """Retorna (inicio, fim) da semana (segunda a domingo) contendo a data d."""
        start = d - timedelta(days=d.weekday())  # segunda-feira
        end = start + timedelta(days=6)
        return start, end

    def _week_label(dini: date, dfim: date) -> str:
        return f"{dini.strftime('%d/%m/%Y')} a {dfim.strftime('%d/%m/%Y')}"

    def _num(v):
        """Converte valores da grid em número (tratando None, '', NaN, etc.)."""
        if v is None:
            return 0
        if isinstance(v, str):
            v = v.replace("R$", "").replace(".", "").replace(",", ".").strip()
            if not v:
                return 0
        try:
            if isinstance(v, float) and math.isnan(v):
                return 0
        except Exception:
            pass
        try:
            return float(v)
        except Exception:
            return 0

    def _salvar_folha_semana(ref_date: date, week_start: date, week_end: date,
                             week_label: str, df_editado: pd.DataFrame):
        """
        Salva/atualiza a folha da semana.
        NÃO usa mais o id da folha; apenas (employee_id, ref_date).
        """
        n_ok = 0
        n_err = 0
        erros = []

        for _, row in df_editado.iterrows():
            try:
                emp_id = int(row["employee_id"])
            except Exception:
                # se não tiver employee_id válido, ignora a linha
                continue

            bruto = _num(row.get("Bruto"))
            inss = _num(row.get("INSS"))
            outros = _num(row.get("Outros descontos"))
            extras = _num(row.get("Extras"))
            # recalcula líquido para garantir consistência
            liquido = bruto - inss - outros + extras

            pago = bool(row.get("Pago?"))
            metodo = (row.get("Forma pgto") or "").strip() or None
            obs = (row.get("Obs") or "").strip() or None
            paid_at = date.today() if pago else None

            params = {
                "employee_id": emp_id,
                "ref_date": ref_date,
                "week_start": week_start,
                "week_end": week_end,
                "week_label": week_label,
                "gross": bruto,
                "inss": inss,
                "other_discounts": outros,
                "extras": extras,
                "net": liquido,
                "paid": pago,
                "paid_at": paid_at,
                "method": metodo,
                "note": obs,
            }

            try:
                qexec(
                    """
                    insert into resto.payroll_week(
                        employee_id, ref_date, week_start, week_end, week_label,
                        gross, inss, other_discounts, extras, net,
                        paid, paid_at, method, note
                    )
                    values (
                        %(employee_id)s, %(ref_date)s, %(week_start)s, %(week_end)s, %(week_label)s,
                        %(gross)s, %(inss)s, %(other_discounts)s, %(extras)s, %(net)s,
                        %(paid)s, %(paid_at)s, %(method)s, %(note)s
                    )
                    on conflict (employee_id, ref_date) do update set
                        week_start      = excluded.week_start,
                        week_end        = excluded.week_end,
                        week_label      = excluded.week_label,
                        gross           = excluded.gross,
                        inss            = excluded.inss,
                        other_discounts = excluded.other_discounts,
                        extras          = excluded.extras,
                        net             = excluded.net,
                        paid            = excluded.paid,
                        paid_at         = excluded.paid_at,
                        method          = excluded.method,
                        note            = excluded.note;
                    """,
                    params,
                )
                n_ok += 1
            except Exception as e:
                n_err += 1
                nome_func = row.get("Funcionário") or f"ID func {emp_id}"
                erros.append(f"{nome_func}: {e}")

        return n_ok, n_err, erros

    PAY_METHODS = ['dinheiro', 'pix', 'cartão débito', 'cartão crédito', 'boleto', 'transferência', 'outro']

    _ensure_folha_schema()

    header("👥 RH / Folha", "Cadastro de funcionários e folha de pagamento semanal.")
    tabs = st.tabs(["👤 Funcionários", "🧾 Folha semanal", "📊 Relatório"])

    # ============ Aba: Funcionários ============
    with tabs[0]:
        card_start()
        st.subheader("Cadastro de funcionários")

        with st.form("form_func"):
            c1, c2, c3 = st.columns([2, 1, 1])
            with c1:
                nome = st.text_input("Nome *")
                funcao = st.text_input("Função / cargo")
            with c2:
                cpf = st.text_input("CPF (opcional)")
                admissao = st.date_input("Data de admissão", value=date.today())
            with c3:
                salario_sem = st.number_input("Salário semanal (R$)", 0.0, 999999.99, 0.0, 0.01)
                ativo = st.checkbox("Ativo", value=True)
            c4, c5 = st.columns(2)
            with c4:
                metodo_pag = st.selectbox("Forma de pagamento padrão", PAY_METHODS)
            with c5:
                obs = st.text_input("Observações", value="")
            ok = st.form_submit_button("Salvar funcionário")

        if ok and nome.strip():
            cpf_digits = re.sub(r"\D", "", cpf or "") or None
            try:
                qexec("""
                    insert into resto.employee(name, cpf, role, admission_date, weekly_salary, active, payment_method, note)
                    values (%s,%s,%s,%s,%s,%s,%s,%s)
                    on conflict (cpf) where cpf is not null do update set
                      name = excluded.name,
                      role = excluded.role,
                      admission_date = excluded.admission_date,
                      weekly_salary  = excluded.weekly_salary,
                      active         = excluded.active,
                      payment_method = excluded.payment_method,
                      note           = excluded.note;
                """, (nome.strip(), cpf_digits, funcao or None, admissao,
                      float(salario_sem or 0), bool(ativo), metodo_pag, obs or None))
                st.success("Funcionário salvo/atualizado.")
                _rerun()
            except Exception as e:
                st.error(f"Erro ao salvar funcionário: {e}")

        # grid de funcionários
        rows = qall("""
            select id, name, cpf, role, admission_date, dismissal_date,
                   weekly_salary, active, payment_method
              from resto.employee
             order by name;
        """) or []
        df = pd.DataFrame(rows)
        if not df.empty:
            df["Excluir?"] = False
            cfg = {
                "id":              st.column_config.NumberColumn("ID", disabled=True),
                "name":            st.column_config.TextColumn("Nome"),
                "cpf":             st.column_config.TextColumn("CPF"),
                "role":            st.column_config.TextColumn("Função / cargo"),
                "admission_date":  st.column_config.DateColumn("Admissão"),
                "dismissal_date":  st.column_config.DateColumn("Demissão"),
                "weekly_salary":   st.column_config.NumberColumn("Salário semanal", format="%.2f", step=10.0),
                "active":          st.column_config.CheckboxColumn("Ativo"),
                "payment_method":  st.column_config.SelectboxColumn("Forma pgto", options=PAY_METHODS),
                "Excluir?":        st.column_config.CheckboxColumn("Excluir?"),
            }
            edited = st.data_editor(
                df[["id","name","cpf","role","admission_date","dismissal_date",
                    "weekly_salary","active","payment_method","Excluir?"]],
                column_config=cfg,
                hide_index=True,
                num_rows="fixed",
                use_container_width=True,
                key="grid_func"
            )

            col_a, col_b = st.columns(2)
            with col_a:
                apply = st.button("💾 Salvar alterações (funcionários)")
            with col_b:
                refresh = st.button("🔄 Atualizar lista")

            if refresh:
                _rerun()

            if apply:
                orig = df.set_index("id")
                new  = edited.set_index("id")
                upd = delc = err = 0

                # excluir marcados
                to_del = new.index[new["Excluir?"] == True].tolist()
                for fid in to_del:
                    try:
                        qexec("delete from resto.employee where id=%s;", (int(fid),))
                        delc += 1
                    except Exception:
                        err += 1

                # atualizar demais
                keep_ids = [i for i in new.index if i not in to_del]
                for fid in keep_ids:
                    a = orig.loc[fid]; b = new.loc[fid]
                    changed = any(str(a.get(f,"")) != str(b.get(f,"")) for f in
                                  ["name","cpf","role","admission_date","dismissal_date",
                                   "weekly_salary","active","payment_method"])
                    if not changed:
                        continue
                    try:
                        qexec("""
                            update resto.employee
                               set name=%s,
                                   cpf=%s,
                                   role=%s,
                                   admission_date=%s,
                                   dismissal_date=%s,
                                   weekly_salary=%s,
                                   active=%s,
                                   payment_method=%s
                             where id=%s;
                        """, (b.get("name"), b.get("cpf") or None, b.get("role") or None,
                              b.get("admission_date"), b.get("dismissal_date"),
                              float(b.get("weekly_salary") or 0),
                              bool(b.get("active")),
                              b.get("payment_method") or None,
                              int(fid)))
                        upd += 1
                    except Exception:
                        err += 1

                st.success(f"Funcionários: ✅ {upd} atualizado(s) • 🗑️ {delc} excluído(s) • ⚠️ {err} erro(s).")
                _rerun()
        else:
            st.caption("Nenhum funcionário cadastrado ainda.")

        card_end()

    # ============ Aba: Folha semanal ============
    with tabs[1]:
        card_start()
        st.subheader("Folha de pagamento semanal")

        ref = st.date_input("Semana de referência (escolha qualquer dia da semana)", value=date.today(), key="folha_ref")
        di, df_ = _week_range(ref)
        st.caption(f"Período considerado: **{di.strftime('%d/%m/%Y')} a {df_.strftime('%d/%m/%Y')}** (segunda a domingo)")
        week_lbl = _week_label(di, df_)

        funcs = qall("select id, name, weekly_salary, payment_method from resto.employee where active is true order by name;") or []
        if not funcs:
            st.warning("Cadastre pelo menos um funcionário na aba anterior.")
            card_end()
            return

        folha_rows = qall("""
            select id, employee_id, ref_date, gross, inss, other_discounts,
                   extras, net, paid, paid_at, method, note
              from resto.payroll_week
             where ref_date = %s;
        """, (di,))
        folha_by_emp = {r["employee_id"]: r for r in folha_rows or []}

        data = []
        for f in funcs:
            emp_id = f["id"]
            base = float(f.get("weekly_salary") or 0.0)
            existing = folha_by_emp.get(emp_id)
            if existing:
                row = {
                    "id": existing["id"],
                    "employee_id": emp_id,
                    "Funcionário": f["name"],
                    "Salário base": base,
                    "Bruto": float(existing.get("gross") or 0.0),
                    "INSS": float(existing.get("inss") or 0.0),
                    "Outros descontos": float(existing.get("other_discounts") or 0.0),
                    "Extras": float(existing.get("extras") or 0.0),
                    "Líquido": float(existing.get("net") or 0.0),
                    "Pago?": bool(existing.get("paid")),
                    "Forma pgto": existing.get("method") or f.get("payment_method") or "",
                    "Obs": existing.get("note") or "",
                }
            else:
                row = {
                    "id": None,
                    "employee_id": emp_id,
                    "Funcionário": f["name"],
                    "Salário base": base,
                    "Bruto": base,
                    "INSS": 0.0,
                    "Outros descontos": 0.0,
                    "Extras": 0.0,
                    "Líquido": base,
                    "Pago?": False,
                    "Forma pgto": f.get("payment_method") or "",
                    "Obs": "",
                }
            data.append(row)

        df_f = pd.DataFrame(data)
        cfg_f = {
            "id":               st.column_config.NumberColumn("ID folha", disabled=True),
            "employee_id":      st.column_config.NumberColumn("ID func", disabled=True),
            "Funcionário":      st.column_config.TextColumn("Funcionário", disabled=True),
            "Salário base":     st.column_config.NumberColumn("Salário base", format="%.2f", disabled=True),
            "Bruto":            st.column_config.NumberColumn("Bruto", format="%.2f", step=10.0),
            "INSS":             st.column_config.NumberColumn("INSS", format="%.2f", step=10.0),
            "Outros descontos": st.column_config.NumberColumn("Outros descontos", format="%.2f", step=10.0),
            "Extras":           st.column_config.NumberColumn("Extras", format="%.2f", step=10.0),
            "Líquido":          st.column_config.NumberColumn("Líquido", format="%.2f", step=10.0),
            "Pago?":            st.column_config.CheckboxColumn("Pago?"),
            "Forma pgto":       st.column_config.SelectboxColumn("Forma pgto", options=PAY_METHODS),
            "Obs":              st.column_config.TextColumn("Observações"),
        }
        edited_f = st.data_editor(
            df_f[["id","employee_id","Funcionário","Salário base","Bruto","INSS",
                  "Outros descontos","Extras","Líquido","Pago?","Forma pgto","Obs"]],
            column_config=cfg_f,
            hide_index=True,
            num_rows="fixed",
            use_container_width=True,
            key=f"grid_folha_{di.isoformat()}"
        )

        salvar_folha = st.button("💾 Salvar folha desta semana")
        if salvar_folha:
            n_ok, n_err, erros = _salvar_folha_semana(di, di, df_, week_lbl, edited_f)

            if n_ok:
                st.success(f"Folha salva. Registros gravados/atualizados: {n_ok}. Erros: {n_err}.")
            else:
                st.error(f"Nenhum registro salvo. Erros: {n_err}.")

            if erros:
                with st.expander("Ver detalhes dos erros (debug)"):
                    for msg in erros:
                        st.write("•", msg)

            _rerun()

        # resumo rápido da semana
        total_bruto = float(edited_f["Bruto"].sum())
        total_liq = float(edited_f["Líquido"].sum())
        st.markdown(
            f"**Total bruto da semana:** {money(total_bruto)} &nbsp;&nbsp;•&nbsp;&nbsp;"
            f"**Total líquido da semana:** {money(total_liq)}"
        )

        card_end()

    # ============ Aba: Relatório ============
    with tabs[2]:
        card_start()
        st.subheader("Relatório de folha (por período)")

        colr1, colr2 = st.columns(2)
        with colr1:
            # agora deixo claro que é pela data de referência (segunda da semana)
            r_ini = st.date_input(
                "De (data de referência)",
                value=date.today().replace(day=1),
                key="folha_rel_ini"
            )
        with colr2:
            r_fim = st.date_input(
                "Até (data de referência)",
                value=date.today(),
                key="folha_rel_fim"
            )

        # funcionários para filtro
        funcs_all = qall("select id, name from resto.employee order by name;") or []
        emp_opts = [(0, "— todos —")] + [(f["id"], f["name"]) for f in funcs_all]

        emp_sel = st.selectbox(
            "Funcionário",
            options=emp_opts,
            format_func=lambda x: x[1] if isinstance(x, tuple) else x,
            key="folha_rel_emp"
        )

        # filtro por p.ref_date (segunda-feira da semana)
        wh = ["p.ref_date >= %s", "p.ref_date <= %s"]
        pr = [r_ini, r_fim]

        if isinstance(emp_sel, tuple) and emp_sel[0] != 0:
            wh.append("p.employee_id = %s")
            pr.append(int(emp_sel[0]))

        sql_rel = f"""
            select p.ref_date,
                   p.week_start,
                   p.week_end,
                   p.week_label,
                   e.name as funcionario,
                   p.gross,
                   p.inss,
                   p.other_discounts,
                   p.extras,
                   p.net,
                   p.paid,
                   p.paid_at
              from resto.payroll_week p
              join resto.employee e on e.id = p.employee_id
             where {' and '.join(wh)}
             order by p.ref_date, e.name;
        """

        rows_rel = qall(sql_rel, tuple(pr)) or []
        df_rel = pd.DataFrame(rows_rel)

        if not df_rel.empty:
            # renomear colunas para ficar mais bonito
            df_rel = df_rel.rename(columns={
                "ref_date":        "Data ref.",
                "week_start":      "Início semana",
                "week_end":        "Fim semana",
                "week_label":      "Semana",
                "funcionario":     "Funcionário",
                "gross":           "Bruto",
                "inss":            "INSS",
                "other_discounts": "Outros descontos",
                "extras":          "Extras",
                "net":             "Líquido",
                "paid":            "Pago?",
                "paid_at":         "Pago em"
            })

            st.dataframe(
                df_rel,
                use_container_width=True,
                hide_index=True
            )

            tot_bruto = float(df_rel["Bruto"].sum())
            tot_liq = float(df_rel["Líquido"].sum())

            st.markdown(
                f"**Total bruto no período:** {money(tot_bruto)}"
                f"&nbsp;&nbsp;•&nbsp;&nbsp;"
                f"**Total líquido no período:** {money(tot_liq)}"
            )

            csv = df_rel.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Exportar CSV (folha)",
                data=csv,
                file_name="folha_pagamentos.csv",
                mime="text/csv"
            )
        else:
            st.caption("Nenhum lançamento de folha encontrado para o período/filtro.")

        card_end()

# ===================== PÁGINA: IMPORTAÇÕES IFOOD =====================
def page_importar_ifood():
    import pandas as pd
    import json
    from datetime import datetime

    _ensure_ifood_schema()

    # Estado da página
    st.session_state.setdefault("ifood_df", None)
    st.session_state.setdefault("ifood_tipo", None)
    st.session_state.setdefault("ifood_nome", None)

    header("🧾 Importações iFood", "Importe relatórios financeiros e de pedidos do iFood para conciliação bancária.")
    card_start()

    st.subheader("1) Importar novo arquivo do iFood")

    uploaded = st.file_uploader(
        "Selecione o arquivo do iFood (.xlsx / .xls / .csv)",
        type=["xlsx", "xls", "csv"],
        key="ifood_file"
    )

    col_b1, col_b2 = st.columns(2)
    with col_b1:
        btn_carregar = st.button("📥 Ler arquivo", use_container_width=True)
    with col_b2:
        btn_salvar = st.button(
            "💾 Salvar importação na base",
            use_container_width=True,
            disabled=(st.session_state.get("ifood_df") is None)
        )

    def _detect_ifood_tipo(df: pd.DataFrame) -> str | None:
        cols = set(df.columns)
        # Relatório de conciliação (arquivo 2025-11.xlsx)
        if "competencia" in cols and "pedido_associado_ifood" in cols:
            return "conciliacao"
        # Relatório de pedidos (arquivo relatorio-pedidos_....xlsx)
        if "ID COMPLETO DO PEDIDO" in cols or "ID CURTO DO PEDIDO" in cols:
            return "pedidos"
        return None

    def _load_ifood_file(file_obj):
        name = (file_obj.name or "").lower()

        if name.endswith(".csv"):
            # Caso algum relatório venha em CSV; se precisar, ajuste separador/decimal
            df_tmp = pd.read_csv(file_obj)
            tipo = _detect_ifood_tipo(df_tmp) or "desconhecido"
            return df_tmp, tipo

        # Padrão: XLSX com abas
        xls = pd.ExcelFile(file_obj)
        df_detectado = None
        tipo_detectado = None

        # tenta em todas as abas até encontrar um layout conhecido
        for sheet in xls.sheet_names:
            df_tmp = xls.parse(sheet)
            t = _detect_ifood_tipo(df_tmp)
            if t:
                df_detectado = df_tmp
                tipo_detectado = t
                break

        if df_detectado is None:
            # fallback: primeira aba
            df_detectado = xls.parse(xls.sheet_names[0])
            tipo_detectado = _detect_ifood_tipo(df_detectado) or "desconhecido"

        # remove linhas totalmente vazias
        df_detectado = df_detectado.dropna(how="all")

        return df_detectado, tipo_detectado

    # ========== CARREGAR ARQUIVO ==========
    if btn_carregar:
        if not uploaded:
            st.warning("Selecione um arquivo primeiro.")
        else:
            try:
                df, tipo = _load_ifood_file(uploaded)
                st.session_state["ifood_df"] = df
                st.session_state["ifood_tipo"] = tipo
                st.session_state["ifood_nome"] = uploaded.name
                st.success(f"Arquivo lido com sucesso. Tipo detectado: **{tipo}**. Linhas: **{len(df)}**.")
            except Exception as e:
                st.error(f"Erro ao ler o arquivo do iFood: {e}")

    df_preview = st.session_state.get("ifood_df")
    tipo_arquivo = st.session_state.get("ifood_tipo")
    nome_arquivo = st.session_state.get("ifood_nome")

    if df_preview is not None:
        st.markdown(
            f"**Arquivo:** `{nome_arquivo or ''}`  •  "
            f"**Tipo:** `{tipo_arquivo or 'desconhecido'}`  •  "
            f"**Linhas:** {len(df_preview)}"
        )
        st.caption("Pré-visualização (primeiras 50 linhas):")
        st.dataframe(df_preview.head(50), use_container_width=True, hide_index=True)

    # ========== SALVAR NO BANCO ==========
    if btn_salvar:
        df = st.session_state.get("ifood_df")
        tipo_arquivo = st.session_state.get("ifood_tipo") or "desconhecido"
        nome_arquivo = st.session_state.get("ifood_nome") or (uploaded.name if uploaded else "arquivo_ifood")

        if df is None:
            st.warning("Nenhum arquivo carregado. Leia o arquivo primeiro.")
        else:
            try:
                # cria o "lote" da importação
                row = qone(
                    "insert into resto.ifood_import_batch(file_name, file_type) "
                    "values (%s, %s) returning id;",
                    (nome_arquivo, tipo_arquivo)
                )
                batch_id = row["id"]

                def _row_to_dict(row):
                    d = {}
                    for col, val in row.items():
                        if pd.isna(val):
                            continue
                        # converte datas / timestamps pra string ISO
                        if isinstance(val, (pd.Timestamp, datetime)):
                            d[col] = val.isoformat()
                        else:
                            d[col] = val
                    return d

                def _extract_order_id(row, tipo: str):
                    if tipo == "conciliacao":
                        for k in ["pedido_associado_ifood", "pedido_associado_ifood_curto"]:
                            if k in row.index and not pd.isna(row[k]):
                                return str(row[k])
                    else:  # 'pedidos' ou outros
                        for k in ["ID COMPLETO DO PEDIDO", "ID CURTO DO PEDIDO"]:
                            if k in row.index and not pd.isna(row[k]):
                                return str(row[k])
                    return None

                inserted = 0
                for i, r in df.iterrows():
                    row_dict = _row_to_dict(r)
                    if not row_dict:
                        continue
                    order_id = _extract_order_id(r, tipo_arquivo)
                    json_str = json.dumps(row_dict, ensure_ascii=False, default=str)

                    try:
                        qexec(
                            "insert into resto.ifood_import_row(batch_id, row_number, order_id, data) "
                            "values (%s, %s, %s, %s::jsonb);",
                            (batch_id, int(i) + 1, order_id, json_str)
                        )
                        inserted += 1
                    except Exception:
                        # se alguma linha der erro, apenas pula e segue
                        continue

                st.success(
                    f"Importação salva com sucesso: **{inserted} linha(s)** "
                    f"para o arquivo `{nome_arquivo}` (tipo `{tipo_arquivo}`)."
                )
            except Exception as e:
                st.error(f"Erro ao salvar a importação no banco: {e}")

    card_end()

    # ========== GERENCIAR IMPORTAÇÕES / APAGAR ==========
    card_start()
    st.subheader("2) Importações já realizadas")

    batches = qall("""
        select
          b.id,
          b.file_name,
          b.file_type,
          b.imported_at,
          count(r.id) as linhas
        from resto.ifood_import_batch b
        left join resto.ifood_import_row r on r.batch_id = b.id
        group by b.id, b.file_name, b.file_type, b.imported_at
        order by b.imported_at desc
        limit 200;
    """) or []

    if not batches:
        st.caption("Nenhuma importação de iFood salva ainda.")
        card_end()
        return

    options = {}
    for b in batches:
        dt = b["imported_at"]
        dt_str = dt.strftime("%d/%m/%Y %H:%M") if isinstance(dt, datetime) else str(dt)
        label = f"[{b['file_type']}] {b['file_name']} ({b['linhas']} linha(s) em {dt_str})"
        options[label] = b["id"]

    sel_label = st.selectbox(
        "Escolha uma importação para apagar (irá remover todas as linhas desse arquivo):",
        options=list(options.keys())
    )
    sel_id = options.get(sel_label)

    col_del1, col_del2 = st.columns([1, 1])
    with col_del1:
        apagar = st.button("🗑️ Apagar importação selecionada", type="primary", use_container_width=True)
    with col_del2:
        st.caption("⚠️ Essa ação remove todas as linhas desse arquivo (não afeta outras importações).")

    if apagar and sel_id:
        try:
            qexec("delete from resto.ifood_import_batch where id = %s;", (int(sel_id),))
            st.success("Importação apagada com sucesso (lote e todas as linhas associadas).")
        except Exception as e:
            st.error(f"Erro ao apagar importação: {e}")

    card_end()
# ===================== PÁGINA: CONCILIAÇÃO IFOOD x BANCO =====================
def page_conciliacao_ifood():
    import pandas as pd
    from datetime import date, timedelta

    header(
        "📊 Conciliação iFood x Banco",
        "Resumo por dia, por repasse e por taxa usando os dados já importados."
    )
    card_start()

    # --- parâmetros de data (últimos 30 dias como padrão) ---
    hoje = date.today()
    padrao_ini = hoje - timedelta(days=30)
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        dt_ini = st.date_input("Data inicial", value=padrao_ini)
    with col_f2:
        dt_fim = st.date_input("Data final", value=hoje)

    if dt_ini > dt_fim:
        st.error("Período inválido: a data inicial é maior que a data final.")
        card_end()
        return

    # --- carrega dados do iFood (apenas arquivos tipo 'conciliacao') ---
    rows_ifood = qall(
        """
        select r.id,
               r.batch_id,
               r.row_number,
               r.order_id,
               r.data,
               b.file_name,
               b.file_type,
               b.imported_at
          from resto.ifood_import_row r
          join resto.ifood_import_batch b on b.id = r.batch_id
         where b.file_type = 'conciliacao'
        """
    )

    if not rows_ifood:
        st.info("Nenhuma importação de iFood do tipo 'conciliacao' encontrada ainda.")
        card_end()
        return

    df_raw = pd.DataFrame(rows_ifood)

    if "data" not in df_raw.columns:
        st.error("A tabela resto.ifood_import_row não possui coluna 'data' (jsonb).")
        card_end()
        return

    # normaliza o JSON do ifood em colunas
    df_json = pd.json_normalize(df_raw["data"])
    df_ifood = pd.concat([df_raw.drop(columns=["data"]), df_json], axis=1)

    # ====== MAPEAMENTO FLEXÍVEL DE COLUNAS ======
    cols = list(df_ifood.columns)

    def _guess_col(substrs):
        for c in cols:
            cl = c.lower()
            if any(s in cl for s in substrs):
                return c
        return None

    col_data_guess = _guess_col(["competencia", "data", "dt"])
    col_liq_guess = _guess_col(["líquido", "liquido", "repasse", "vl_liquido", "vl_liq"])
    col_bruto_guess = _guess_col(["bruto", "total pedido", "vl_total_bruto"])
    tax_candidates = [c for c in cols if any(s in c.lower() for s in ["taxa", "comissão", "comissao"])]

    st.markdown("#### Mapeamento de colunas do relatório iFood")
    st.caption(
        "Escolha quais colunas do arquivo do iFood representam **data**, "
        "**valor líquido**, **valor bruto** e **taxas**. "
        "Isso deixa o relatório flexível pra diferentes layouts do iFood."
    )

    col_c1, col_c2 = st.columns(2)
    with col_c1:
        col_data = st.selectbox(
            "Coluna de data / competência",
            options=cols,
            index=cols.index(col_data_guess) if col_data_guess in cols else 0,
        )
        col_bruto = st.selectbox(
            "Coluna de valor bruto",
            options=cols,
            index=cols.index(col_bruto_guess) if col_bruto_guess in cols else 0,
        )
    with col_c2:
        col_liq = st.selectbox(
            "Coluna de valor líquido / repasse",
            options=cols,
            index=cols.index(col_liq_guess) if col_liq_guess in cols else 0,
        )
        col_repasse = st.selectbox(
            "Coluna identificador do repasse (se existir)",
            options=["(não usar)"] + cols,
            index=0,
        )

    tax_cols = st.multiselect(
        "Colunas de taxas (uma ou mais)",
        options=cols,
        default=[c for c in tax_candidates[:3]],
    )

    def _to_number_series(s):
        if s is None:
            return pd.Series([0.0] * len(df_ifood))
        return pd.to_numeric(
            s.astype(str)
             .str.replace("R$", "", regex=False)
             .str.replace(".", "", regex=False)
             .str.replace(",", ".", regex=False)
             .str.strip(),
            errors="coerce",
        ).fillna(0.0)

    # coluna de data convertida para date
    df_ifood["_data"] = pd.to_datetime(df_ifood[col_data], errors="coerce").dt.date
    df_ifood = df_ifood.dropna(subset=["_data"])
    df_ifood = df_ifood[(df_ifood["_data"] >= dt_ini) & (df_ifood["_data"] <= dt_fim)]

    if df_ifood.empty:
        st.warning("Não há registros do iFood no período selecionado.")
        card_end()
        return

    df_ifood["_bruto"] = _to_number_series(df_ifood[col_bruto])
    df_ifood["_liq"] = _to_number_series(df_ifood[col_liq])

    tax_internal_cols = []
    for c in tax_cols:
        internal = f"_tax_{c}"
        df_ifood[internal] = _to_number_series(df_ifood[c])
        tax_internal_cols.append(internal)

    if tax_internal_cols:
        df_ifood["_tax_total"] = df_ifood[tax_internal_cols].sum(axis=1)
    else:
        df_ifood["_tax_total"] = 0.0

    # ====== DADOS DO BANCO (CASHBOOK) ======
    rows_bank = qall(
        """
        select entry_date, description, amount, kind
          from resto.cashbook
         where entry_date between %s and %s
        """,
        (dt_ini, dt_fim),
    )
    df_bank = pd.DataFrame(rows_bank) if rows_bank else pd.DataFrame(
        columns=["entry_date", "description", "amount", "kind"]
    )
    if not df_bank.empty:
        df_bank["entry_date"] = pd.to_datetime(df_bank["entry_date"], errors="coerce").dt.date

    st.markdown("#### Opções do extrato bancário")
    only_ifood_bank = st.checkbox(
        "Considerar apenas lançamentos com 'IFOOD' na descrição",
        value=True
    )

    if not df_bank.empty:
        if only_ifood_bank:
            df_bank_filt = df_bank[
                df_bank["description"].astype(str).str.upper().str.contains("IFOOD", na=False)
            ].copy()
        else:
            df_bank_filt = df_bank.copy()
        df_bank_filt = df_bank_filt[
            df_bank_filt["kind"].astype(str).str.upper() == "IN"
        ]
    else:
        df_bank_filt = df_bank.copy()

    # ==================== 1) RESUMO POR DIA ====================
    st.markdown("### 1) Resumo por dia")

    df_ifood_dia = (
        df_ifood.groupby("_data", as_index=False)
        .agg(
            pedidos=("order_id", "count"),
            valor_bruto=("_bruto", "sum"),
            taxas=("_tax_total", "sum"),
            valor_liquido=("_liq", "sum"),
        )
    )

    if not df_bank_filt.empty:
        df_bank_dia = (
            df_bank_filt.groupby("entry_date", as_index=False)
            .agg(credito_banco=("amount", "sum"))
        )
    else:
        df_bank_dia = pd.DataFrame(columns=["entry_date", "credito_banco"])

    df_resumo_dia = pd.merge(
        df_ifood_dia,
        df_bank_dia,
        left_on="_data",
        right_on="entry_date",
        how="outer",
    )

    df_resumo_dia["data"] = df_resumo_dia["_data"].combine_first(df_resumo_dia["entry_date"])
    df_resumo_dia["valor_bruto"] = df_resumo_dia["valor_bruto"].fillna(0.0)
    df_resumo_dia["taxas"] = df_resumo_dia["taxas"].fillna(0.0)
    df_resumo_dia["valor_liquido"] = df_resumo_dia["valor_liquido"].fillna(0.0)
    df_resumo_dia["credito_banco"] = df_resumo_dia["credito_banco"].fillna(0.0)
    df_resumo_dia["diferença_banco_menos_ifood"] = (
        df_resumo_dia["credito_banco"] - df_resumo_dia["valor_liquido"]
    )
    df_resumo_dia["pedidos"] = df_resumo_dia["pedidos"].fillna(0).astype(int)

    cols_out = [
        "data",
        "pedidos",
        "valor_bruto",
        "taxas",
        "valor_liquido",
        "credito_banco",
        "diferença_banco_menos_ifood",
    ]
    df_resumo_dia = df_resumo_dia[cols_out].sort_values("data")

    st.dataframe(
        df_resumo_dia,
        use_container_width=True,
        hide_index=True,
    )

    # ==================== 2) RESUMO POR REPASSE ====================
    st.markdown("### 2) Resumo por repasse (apenas iFood)")

    if col_repasse != "(não usar)":
        df_rep = (
            df_ifood.groupby(col_repasse, as_index=False)
            .agg(
                pedidos=("order_id", "count"),
                valor_bruto=("_bruto", "sum"),
                taxas=("_tax_total", "sum"),
                valor_liquido=("_liq", "sum"),
            )
        )

        if not df_bank_filt.empty:
            def _match_credito(rep_val):
                if rep_val is None:
                    return 0.0
                txt = str(rep_val).strip()
                if not txt:
                    return 0.0
                mask = df_bank_filt["description"].astype(str).str.contains(
                    txt, case=False, na=False
                )
                return float(df_bank_filt.loc[mask, "amount"].sum() or 0.0)

            df_rep["credito_banco"] = df_rep[col_repasse].apply(_match_credito)
            df_rep["diferença_banco_menos_ifood"] = (
                df_rep["credito_banco"] - df_rep["valor_liquido"]
            )
        else:
            df_rep["credito_banco"] = 0.0
            df_rep["diferença_banco_menos_ifood"] = -df_rep["valor_liquido"]

        st.dataframe(df_rep, use_container_width=True, hide_index=True)
    else:
        st.caption(
            "Selecione uma coluna de repasse para ver o resumo por repasse "
            "(ex.: número ou ID do repasse)."
        )

    # ==================== 3) RESUMO POR TIPO DE TAXA ====================
    st.markdown("### 3) Resumo por taxa (apenas iFood)")

    if not tax_cols:
        st.caption("Nenhuma coluna de taxa selecionada.")
    else:
        linhas_taxa = []
        total_liq = float(df_ifood["_liq"].sum() or 0.0)
        total_bruto = float(df_ifood["_bruto"].sum() or 0.0)

        for c in tax_cols:
            internal = f"_tax_{c}"
            total_taxa = float(df_ifood[internal].sum() or 0.0)
            perc_bruto = (total_taxa / total_bruto * 100.0) if total_bruto else 0.0
            perc_liq = (total_taxa / total_liq * 100.0) if total_liq else 0.0
            linhas_taxa.append(
                {
                    "taxa_coluna": c,
                    "total_taxa": total_taxa,
                    "% sobre bruto": perc_bruto,
                    "% sobre líquido": perc_liq,
                }
            )

        df_taxa = pd.DataFrame(linhas_taxa)
        st.dataframe(df_taxa, use_container_width=True, hide_index=True)

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
    page = st.sidebar.radio("Menu", ["PAINEL", "CADASTROS", "COMPRAS","LISTA DE COMPRAS", "VENDAS", "PREÇOS", "PRODUÇÃO", "MANIPULAR PRODUÇÃO","ESTOQUE", "FINANCEIRO","CONCILIAÇÃO IFOOD","AGENDA DE CONTAS A PAGAR","RH/FOLHA","RELATÓRIOS","IMPORTAÇÕES BANCÁRIAS","IMPORTAÇÕES IFOOD"], index=0)

    if page == "PAINEL": page_dashboard()
    elif page == "CADASTROS": page_cadastros()
    elif page == "COMPRAS": page_compras()
    elif page == "LISTA DE COMPRAS": page_lista_compras()   # <— NOVO
    elif page == "VENDAS": page_vendas()
    elif page == "PREÇOS": page_receitas_precos()
    elif page == "PRODUÇÃO": page_producao()
    elif page == "MANIPULAR PRODUÇÃO": page_producao_cancelar()
    elif page == "ESTOQUE": page_estoque()
    elif page == "FINANCEIRO": page_financeiro()
    elif page == "CONCILIAÇÃO IFOOD": page_conciliacao_ifood()
    elif page == "AGENDA DE CONTAS A PAGAR": page_agenda_contas()   # <— NOVO
    elif page == "RH/FOLHA":page_folha()
    elif page == "RELATÓRIOS": page_relatorios()
    elif page == "IMPORTAÇÕES BANCÁRIAS": page_importar_extrato()
    elif page == "IMPORTAÇÕES IFOOD":page_importar_ifood()

if __name__ == "__main__":
    main()
