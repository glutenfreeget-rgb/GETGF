
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

# ===================== Importador: utilitários (GENÉRICO primeiro) =====================
import unicodedata

def _read_text_guess(raw: bytes) -> str:
    for enc in ("utf-8-sig", "latin1", "utf-16", "utf-16le", "utf-16be"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")

def _strip_accents(s: str) -> str:
    if s is None:
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(ch))

def _norm(s: str) -> str:
    s = _strip_accents(str(s)).lower().strip()
    for ch in ("\xa0", "\t"):
        s = s.replace(ch, " ")
    while "  " in s:
        s = s.replace("  ", " ")
    return s

def _parse_brl_amount(x) -> float:
    s = str(x or "").strip()
    s = s.replace("R$","").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        try:
            return float(s.replace(",", "."))
        except Exception:
            return 0.0

def _guess_sep(sample_txt: str) -> str:
    # Tenta detectar ; , \t |
    seps = [";", ",", "\t", "|"]
    best = ","
    best_cols = 1
    lines = [ln for ln in sample_txt.splitlines()[:5] if ln.strip()]
    for sep in seps:
        cols = max((len(ln.split(sep)) for ln in lines), default=1)
        if cols > best_cols:
            best_cols = cols
            best = sep
    return best

def _load_csv_generic(raw: bytes):
    """Tenta reconhecer qualquer CSV com Data/Descrição/(Valor ou Crédito/Débito)."""
    import pandas as pd
    from io import StringIO
    txt = _read_text_guess(raw)
    if not txt.strip():
        return None
    # Pandas com inferência de separador (engine='python') ajuda em casos com ;/,/tab
    try:
        df = pd.read_csv(StringIO(txt), sep=None, engine="python", dtype=str, keep_default_na=False)
    except Exception:
        sep = _guess_sep(txt)
        df = pd.read_csv(StringIO(txt), sep=sep, dtype=str, keep_default_na=False)

    # mapa normalizado
    nmap = { _norm(c): c for c in df.columns }

    # datas
    date_key = None
    for k in (
        "data lancamento", "data lançamento", "data contabil", "data contab il", "data",
        "date", "dt", "entry_date", "data mov", "data mov."
    ):
        if k in nmap: date_key = nmap[k]; break

    # descrições
    desc_key = None
    for k in (
        "descricao", "descrição", "description", "historico", "histórico",
        "titulo", "título", "detalhe", "detalhes", "complemento", "observacao", "observação"
    ):
        if k in nmap: desc_key = nmap[k]; break

    # valores (uma coluna) ou crédito/débito (duas colunas)
    val_key = None
    for k in ("valor", "amount", "valor lancamento", "valor lançamento", "valor final", "valor total"):
        if k in nmap: val_key = nmap[k]; break

    cred_key = None
    for k in ("entrada(r$)", "credito", "crédito", "credit", "valor credito", "valor crédito", "receita"):
        if k in nmap: cred_key = nmap[k]; break

    deb_key = None
    for k in ("saida(r$)", "saída(r$)", "debito", "débito", "debit", "valor debito", "valor débito", "despesa"):
        if k in nmap: deb_key = nmap[k]; break

    if not (date_key and desc_key and (val_key or (cred_key and deb_key))):
        return None

    out = pd.DataFrame({
        "entry_date": pd.to_datetime(df[date_key], dayfirst=True, errors="coerce").dt.date,
        "description": df[desc_key].astype(str),
    })
    if val_key:
        out["amount"] = df[val_key].apply(_parse_brl_amount)
    else:
        out["amount"] = df[cred_key].apply(_parse_brl_amount) - df[deb_key].apply(_parse_brl_amount)
    return out.dropna(subset=["entry_date"])

def _load_c6_csv(raw: bytes):
    """Reconhece C6 mesmo com preâmbulo e pequenas variações."""
    import pandas as pd
    from io import StringIO
    txt = _read_text_guess(raw)
    # pega a primeira linha que tenha TODOS os cabeçalhos esperados (ordem indiferente)
    headers_req = [
        "data lancamento","data contabil","titulo","descricao","entrada(r$)","saida(r$)","saldo do dia(r$)"
    ]
    lines = txt.splitlines()
    header_idx = None
    for i, ln in enumerate(lines[:50]):  # procura nos primeiros 50
        norm = _norm(ln)
        if all(h in norm for h in headers_req):
            header_idx = i
            break
    if header_idx is None:
        return None
    csv_body = "\n".join(lines[header_idx:])
    try:
        df = pd.read_csv(StringIO(csv_body), sep=",", dtype=str, keep_default_na=False)
    except Exception:
        # pode estar com ';'
        df = pd.read_csv(StringIO(csv_body), sep=";", dtype=str, keep_default_na=False)

    nmap = { _norm(c): c for c in df.columns }
    d_lanc = nmap.get("data lancamento") or nmap.get("data")
    desc   = nmap.get("descricao") or nmap.get("titulo") or nmap.get("título")
    ent    = nmap.get("entrada(r$)")
    sai    = nmap.get("saida(r$)") or nmap.get("saída(r$)")
    if not (d_lanc and desc and ent and sai):
        return None

    out = pd.DataFrame({
        "entry_date": pd.to_datetime(df[d_lanc], dayfirst=True, errors="coerce").dt.date,
        "description": df[desc].astype(str),
    })
    out["amount"] = df[ent].apply(_parse_brl_amount) - df[sai].apply(_parse_brl_amount)
    return out.dropna(subset=["entry_date"])

def _load_bank_file(up):
    """Carrega o CSV do uploader. Tenta primeiro genérico (mais tolerante)."""
    raw = up.read()
    df = _load_csv_generic(raw)
    if df is not None and not df.empty:
        return df
    df = _load_c6_csv(raw)
    if df is not None and not df.empty:
        return df
    import pandas as pd
    return pd.DataFrame()

# ===================== Importador: utilitários robustos de CSV =====================
import unicodedata

def _read_text_guess(raw: bytes) -> str:
    for enc in ("utf-8-sig", "latin1", "utf-16", "utf-16le", "utf-16be"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")

def _strip_accents(s: str) -> str:
    if s is None:
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(ch))

def _norm(s: str) -> str:
    s = _strip_accents(s).lower().strip()
    s = s.replace("\xa0"," ").replace("\t"," ").replace("  "," ")
    return s

def _parse_brl_amount(x) -> float:
    s = str(x or "").strip()
    s = s.replace("R$","").replace(" ", "")
    # Converte formatos pt-BR: 1.234,56 -> 1234.56
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        try:
            return float(s.replace(",", "."))
        except Exception:
            return 0.0

def _guess_sep(sample_txt: str) -> str:
    # Tenta ; , \t |
    seps = [";", ",", "\t", "|"]
    best = ","
    best_cols = 1
    first_lines = [ln for ln in sample_txt.splitlines()[:5] if ln.strip()]
    for sep in seps:
        counts = [len(ln.split(sep)) for ln in first_lines] or [1]
        cols = max(counts)
        if cols > best_cols:
            best_cols = cols
            best = sep
    return best

