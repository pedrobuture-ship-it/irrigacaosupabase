from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import math
import requests
import pandas as pd
import streamlit as st
import os


# ============================================================
# CONFIGURAÇÃO
# ============================================================

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Diretório base do projeto (onde está o app.py)
BASE_DIR = Path(__file__).resolve().parent

# Pasta de dados
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

SUPABASE_URL = st.secrets["SUPABASE_URL"] if "SUPABASE_URL" in st.secrets else os.getenv("SUPABASE_URL")
SUPABASE_KEY = st.secrets["SUPABASE_KEY"] if "SUPABASE_KEY" in st.secrets else os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError(
        "Defina SUPABASE_URL e SUPABASE_KEY em st.secrets ou variáveis de ambiente."
    )

REST_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}


def _rest_url(table: str) -> str:
    return f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}"


def sb_select(
    table: str,
    select_cols: str = "*",
    filters: Optional[Dict[str, str]] = None,
    order_by: Optional[str] = None,
    ascending: bool = True,
    limit: Optional[int] = None,
) -> List[dict]:
    params: Dict[str, str] = {"select": select_cols}
    if filters:
        params.update(filters)
    if order_by:
        params["order"] = f"{order_by}.{'asc' if ascending else 'desc'}"
    if limit is not None:
        params["limit"] = str(limit)

    response = requests.get(_rest_url(table), headers=REST_HEADERS, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def sb_insert(table: str, payload: dict) -> List[dict]:
    headers = {**REST_HEADERS, "Prefer": "return=representation"}
    response = requests.post(_rest_url(table), headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def sb_upsert(table: str, payload: dict, on_conflict: str) -> List[dict]:
    headers = {
        **REST_HEADERS,
        "Prefer": "resolution=merge-duplicates,return=representation",
    }
    response = requests.post(
        _rest_url(table),
        headers=headers,
        params={"on_conflict": on_conflict},
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def sb_delete(table: str, filters: Dict[str, str]) -> List[dict]:
    headers = {**REST_HEADERS, "Prefer": "return=representation"}
    response = requests.delete(_rest_url(table), headers=headers, params=filters, timeout=30)
    response.raise_for_status()
    return response.json()


# ============================================================
# BASE DE DADOS FIXA
# ============================================================

IRRIGATION_EFFICIENCY = {
    "aspersao": 0.80,
    "pivo central": 0.85,
    "gotejamento": 0.90,
    "microaspersao": 0.90,
    "sulco": 0.75,
}


@dataclass
class Crop:
    nome: str
    kc_in: float
    kc_cv: float
    kc_m: float
    kc_final: float
    z_m: float
    duracao_ep: int
    duracao_in: int
    duracao_cv: int
    duracao_medio: int
    duracao_final: int
    fator_f: float


CROPS: Dict[str, Crop] = {
    "milho": Crop("Milho", 0.4, 0.8, 1.15, 0.7, 0.4, 10, 20, 35, 40, 30, 0.55),
    "feijao": Crop("Feijão", 0.35, 0.7, 1.1, 0.3, 0.35, 10, 10, 25, 35, 20, 0.45),
    "algodao": Crop("Algodão", 0.45, 0.75, 1.15, 0.15, 0.55, 10, 30, 50, 55, 45, 0.65),
    "batata": Crop("Batata", 0.4, 0.8, 1.2, 0.75, 0.4, 10, 25, 30, 30, 20, 0.35),
    "soja": Crop("Soja", 0.35, 0.75, 1.1, 1.5, 0.35, 10, 15, 25, 55, 20, 0.50),
    "arroz": Crop("Arroz", 1.1, 1.3, 1.2, 1.0, 0.35, 7, 30, 30, 60, 30, 0.20),
    "banana": Crop("Banana", 0.45, 0.8, 1.05, 0.9, 0.6, 60, 90, 60, 60, 120, 0.35),
    "cana-de-acucar": Crop("Cana-de-açúcar", 0.4, 0.75, 1.2, 0.7, 0.5, 30, 60, 90, 135, 45, 0.65),
    "trigo": Crop("Trigo", 0.35, 0.75, 1.15, 0.45, 0.35, 10, 15, 25, 50, 30, 0.55),
    "aveia": Crop("Aveia", 0.35, 0.75, 1.15, 0.45, 0.35, 10, 15, 25, 50, 30, 0.55),
    "cevada": Crop("Cevada", 0.35, 0.75, 1.15, 0.45, 0.35, 10, 15, 25, 50, 30, 0.55),
    "cebola": Crop("Cebola", 0.5, 0.75, 1.05, 1.0, 0.3, 7, 15, 35, 155, 40, 0.30),
    "abobora": Crop("Abóbora", 0.45, 0.7, 0.9, 0.75, 0.45, 7, 25, 35, 50, 20, 0.45),
    "cafe": Crop("Café", 0.8, 0.9, 1.05, 0.7, 0.5, 30, 85, 150, 60, 40, 0.40),
    "pasto": Crop("Pasto", 0.8, 0.85, 1.0, 0.8, 0.5, 10, 20, 15, 15, 10, 0.50),
    "tomate": Crop("Tomate", 0.45, 0.75, 1.2, 0.7, 0.4, 10, 25, 40, 40, 25, 0.35),
    "melancia": Crop("Melancia", 0.45, 0.75, 1.0, 0.7, 0.5, 10, 15, 15, 25, 20, 0.35),
    "girassol": Crop("Girassol", 0.35, 0.75, 1.15, 0.75, 0.4, 10, 20, 35, 45, 25, 0.50),
    "laranja": Crop("Laranja", 0.5, 0.75, 0.9, 0.9, 0.55, 25, 90, 200, 365, 365, 0.50),
    "cenoura": Crop("Cenoura", 0.45, 0.75, 1.05, 0.9, 0.35, 10, 15, 25, 35, 20, 0.40),
    "pimenta": Crop("Pimenta", 0.35, 0.7, 1.1, 0.9, 0.3, 10, 25, 35, 50, 30, 0.30),
    "alface": Crop("Alface", 0.5, 0.8, 1.05, 1.0, 0.35, 5, 15, 20, 35, 10, 0.30),
    "rucula": Crop("Rúcula", 0.5, 0.8, 1.05, 1.0, 0.3, 5, 7, 13, 15, 5, 0.30),
    "repolho": Crop("Repolho", 0.45, 0.75, 1.1, 0.9, 0.55, 10, 23, 33, 20, 10, 0.45),
    "agriao": Crop("Agrião", 0.6, 0.85, 1.0, 0.95, 0.3, 5, 10, 20, 10, 5, 0.30),
    "espinafre": Crop("Espinafre", 0.5, 0.85, 1.05, 0.95, 0.35, 10, 10, 15, 25, 5, 0.20),
}


@dataclass
class Soil:
    ucc: float
    upmp: float
    ds: float


@dataclass
class WeatherDay:
    data: date
    precipitacao_mm: float
    eto_mm: float


@dataclass
class ResultDay:
    data: date
    dap: int
    fase: str
    kc: float
    akc: float
    kl: float
    ks: float
    eto_mm: float
    etc_mm: float
    p_mm: float
    irrigacao_real_mm: float
    deplecao_mm: float
    taw_mm: float
    raw_mm: float
    lli_mm: float
    lbi_mm: float


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================
def explain_phase_name(fase: str) -> str:
    nomes = {
        "inicial": "Fase inicial",
        "desenvolvimento": "Desenvolvimento vegetativo",
        "medio": "Fase média",
        "final": "Fase final",
        "apos_ciclo": "Após o ciclo"
    }
    return nomes.get(fase, fase)

def clamp(valor: float, minimo: float, maximo: float) -> float:
    return max(minimo, min(valor, maximo))


def normalize_name(texto: str) -> str:
    return (
        texto.strip()
        .lower()
        .replace("ã", "a")
        .replace("á", "a")
        .replace("â", "a")
        .replace("é", "e")
        .replace("ê", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ô", "o")
        .replace("ú", "u")
        .replace("ç", "c")
    )


def to_date(texto: str) -> date:
    return datetime.strptime(texto, "%Y-%m-%d").date()


def crops_to_df() -> pd.DataFrame:
    rows = []
    for key, crop in CROPS.items():
        rows.append({
            "chave": key,
            "nome": crop.nome,
            "kc_in": crop.kc_in,
            "kc_cv": crop.kc_cv,
            "kc_m": crop.kc_m,
            "kc_final": crop.kc_final,
            "z_m": crop.z_m,
            "duracao_ep": crop.duracao_ep,
            "duracao_in": crop.duracao_in,
            "duracao_cv": crop.duracao_cv,
            "duracao_medio": crop.duracao_medio,
            "duracao_final": crop.duracao_final,
            "fator_f": crop.fator_f,
        })
    return pd.DataFrame(rows).sort_values("nome").reset_index(drop=True)


# ============================================================
# BANCO SUPABASE
# ============================================================

def clear_app_caches():
    for fn_name in [
        "list_plantios",
        "get_plantio",
        "get_irrigation_map",
        "list_plantios_com_historico",
        "get_last_saved_day",
        "load_history_df",
        "load_solos_df",
    ]:
        fn = globals().get(fn_name)
        if fn is not None and hasattr(fn, "clear"):
            fn.clear()



def init_db():
    """
    No Supabase, as tabelas são criadas no SQL Editor.
    Esta função fica só para manter compatibilidade com o restante do app.
    """
    return None


def create_plantio(
    nome: str,
    local: str,
    latitude: float,
    longitude: float,
    timezone: str,
    cultura_key: str,
    sistema_irrigacao: str,
    data_plantio: date,
    ucc: float,
    upmp: float,
    ds: float,
    z_override_m: Optional[float] = None,
):
    payload = {
        "nome": nome,
        "local": local,
        "latitude": float(latitude),
        "longitude": float(longitude),
        "timezone": timezone,
        "cultura_key": cultura_key,
        "sistema_irrigacao": sistema_irrigacao,
        "data_plantio": data_plantio.isoformat(),
        "ucc": float(ucc),
        "upmp": float(upmp),
        "ds": float(ds),
        "z_override_m": float(z_override_m) if z_override_m is not None else None,
        "f_override": None,  # f passa a ser sempre definido pela cultura selecionada
    }
    return sb_insert("plantios", payload)


@st.cache_data(ttl=60, show_spinner=False)
def list_plantios() -> pd.DataFrame:
    data = sb_select(
        "plantios",
        select_cols="id,nome,local,cultura_key,sistema_irrigacao,data_plantio,latitude,longitude,timezone",
        order_by="created_at",
        ascending=False,
    )
    return pd.DataFrame(data)


@st.cache_data(ttl=60, show_spinner=False)
def get_plantio(plantio_id: str) -> Optional[dict]:
    data = sb_select("plantios", filters={"id": f"eq.{plantio_id}"}, limit=1)
    return data[0] if data else None


@st.cache_data(ttl=30, show_spinner=False)
def get_irrigation_map(plantio_id: str) -> Dict[date, float]:
    rows = sb_select(
        "historico_dias",
        select_cols="data,irrigacao_real_mm",
        filters={"plantio_id": f"eq.{plantio_id}"},
    )
    resultado: Dict[date, float] = {}
    for row in rows:
        resultado[to_date(row["data"])] = float(row.get("irrigacao_real_mm") or 0.0)
    return resultado



@st.cache_data(ttl=30, show_spinner=False)
def list_plantios_com_historico() -> pd.DataFrame:
    plantios_df = list_plantios()
    if plantios_df.empty:
        return plantios_df

    historico_rows = sb_select("historico_dias", select_cols="plantio_id")
    ids_com_historico = {
        str(row.get("plantio_id"))
        for row in historico_rows
        if row.get("plantio_id")
    }

    if not ids_com_historico:
        return plantios_df.iloc[0:0].copy()

    return plantios_df[plantios_df["id"].astype(str).isin(ids_com_historico)].copy()


@st.cache_data(ttl=30, show_spinner=False)
def get_last_saved_day(plantio_id: str) -> Optional[dict]:
    data = sb_select(
        "historico_dias",
        select_cols="data,dap,deplecao_mm,irrigou,irrigacao_real_mm,eto_mm,etc_mm,p_mm",
        filters={"plantio_id": f"eq.{plantio_id}"},
        order_by="data",
        ascending=False,
        limit=1,
    )
    return data[0] if data else None


def upsert_day_result(plantio_id: str, irrigou: bool, result: ResultDay):
    payload = {
        "plantio_id": plantio_id,
        "data": result.data.isoformat(),
        "irrigou": bool(irrigou),
        "irrigacao_real_mm": float(result.irrigacao_real_mm),
        "dap": int(result.dap),
        "fase": result.fase,
        "kc": float(result.kc),
        "akc": float(result.akc),
        "kl": float(result.kl),
        "ks": float(result.ks),
        "eto_mm": float(result.eto_mm),
        "etc_mm": float(result.etc_mm),
        "p_mm": float(result.p_mm),
        "deplecao_mm": float(result.deplecao_mm),
        "taw_mm": float(result.taw_mm),
        "raw_mm": float(result.raw_mm),
        "lli_mm": float(result.lli_mm),
        "lbi_mm": float(result.lbi_mm),
    }
    return sb_upsert("historico_dias", payload, on_conflict="plantio_id,data")


@st.cache_data(ttl=30, show_spinner=False)
def load_history_df(plantio_id: str) -> pd.DataFrame:
    rows = sb_select(
        "historico_dias",
        select_cols="data,dap,fase,kc,akc,kl,ks,eto_mm,etc_mm,p_mm,irrigou,irrigacao_real_mm,deplecao_mm,taw_mm,raw_mm,lli_mm,lbi_mm",
        filters={"plantio_id": f"eq.{plantio_id}"},
        order_by="data",
        ascending=True,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.rename(columns={
        "data": "Data",
        "dap": "DAP",
        "fase": "Fase",
        "kc": "Kc",
        "akc": "AKc",
        "kl": "Kl",
        "ks": "Ks",
        "eto_mm": "ETo (mm)",
        "etc_mm": "ETc (mm)",
        "p_mm": "P (mm)",
        "irrigou": "Irrigou",
        "irrigacao_real_mm": "I_real (mm)",
        "deplecao_mm": "Dr (mm)",
        "taw_mm": "TAW (mm)",
        "raw_mm": "RAW (mm)",
        "lli_mm": "LLI (mm)",
        "lbi_mm": "LBI (mm)",
    })
    df["Irrigou"] = df["Irrigou"].map({True: "Sim", False: "Não"})
    df["Data"] = pd.to_datetime(df["Data"]).dt.strftime("%d/%m/%Y")
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_solos_df() -> pd.DataFrame:
    data = sb_select("solos", select_cols="id,nome,ucc,upmp,ds,created_at", order_by="nome")
    return pd.DataFrame(data)


def create_solo(nome: str, ucc: float, upmp: float, ds: float):
    payload = {
        "nome": nome,
        "ucc": float(ucc),
        "upmp": float(upmp),
        "ds": float(ds),
    }
    return sb_insert("solos", payload)


def delete_solo(solo_id: str):
    return sb_delete("solos", {"id": f"eq.{solo_id}"})


def delete_history_day(plantio_id: str, data_iso: str):
    return sb_delete("historico_dias", {"plantio_id": f"eq.{plantio_id}", "data": f"eq.{data_iso}"})


def delete_all_history(plantio_id: str):
    return sb_delete("historico_dias", {"plantio_id": f"eq.{plantio_id}"})


def delete_plantio(plantio_id: str):
    sb_delete("historico_dias", {"plantio_id": f"eq.{plantio_id}"})
    return sb_delete("plantios", {"id": f"eq.{plantio_id}"})


# ============================================================
# OPEN-METEO
# ============================================================

@st.cache_data(show_spinner=False, ttl=1800)
def fetch_weather_open_meteo(
    latitude: float,
    longitude: float,
    start_date: date,
    end_date: date,
    timezone: str = "America/Sao_Paulo",
) -> List[WeatherDay]:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": "precipitation_sum,et0_fao_evapotranspiration",
        "timezone": timezone,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

    last_error = None

    for tentativa in range(3):
        try:
            response = requests.get(
                OPEN_METEO_URL,
                params=params,
                timeout=45,
            )

            if response.status_code == 429:
                if tentativa < 2:
                    import time
                    time.sleep(2 * (tentativa + 1))
                    continue
                raise RuntimeError(
                    "A API do Open-Meteo recebeu requisições demais em pouco tempo. "
                    "Espere alguns segundos e tente novamente."
                )

            response.raise_for_status()
            data = response.json()

            if "daily" not in data:
                raise RuntimeError("Resposta da Open-Meteo não contém 'daily'.")

            daily = data["daily"]
            times = daily.get("time", [])
            precipitation = daily.get("precipitation_sum", [])
            eto = daily.get("et0_fao_evapotranspiration", [])

            if not (len(times) == len(precipitation) == len(eto)):
                raise RuntimeError("Open-Meteo retornou listas com tamanhos diferentes.")

            result = []
            for t, p, e in zip(times, precipitation, eto):
                result.append(
                    WeatherDay(
                        data=datetime.strptime(t, "%Y-%m-%d").date(),
                        precipitacao_mm=float(p or 0.0),
                        eto_mm=float(e or 0.0),
                    )
                )
            return result

        except requests.RequestException as e:
            last_error = e
            if tentativa < 2:
                import time
                time.sleep(2 * (tentativa + 1))
            else:
                raise RuntimeError(f"Erro ao consultar Open-Meteo: {e}") from e

    raise RuntimeError(f"Erro ao consultar Open-Meteo: {last_error}")

def merge_weather_data_by_date(*weather_lists: List[WeatherDay]) -> List[WeatherDay]:
    """Une listas de clima sem duplicar datas, preservando a primeira ocorrência de cada dia."""
    merged: Dict[date, WeatherDay] = {}
    for weather_list in weather_lists:
        for wd in weather_list:
            if wd.data not in merged:
                merged[wd.data] = wd
    return [merged[d] for d in sorted(merged.keys())]


# ============================================================
# CÁLCULOS AGRONÔMICOS
# ============================================================
def build_planilha_prof_df(
    results: List[ResultDay],
    soil: Soil,
    crop: Crop,
    eficiencia: float,
    pef_mode: str = "igual_p",
    pef_percentual: float = 1.0,
) -> pd.DataFrame:
    """
    Monta uma tabela no estilo da planilha do professor.

    pef_mode:
    - "igual_p": Pef = P
    - "percentual": Pef = P * pef_percentual
    """

    rows = []

    # TAW e RAW "do dia" já vêm em cada ResultDay
    # Vamos reconstruir LA, DRA, DTA, DP e campos auxiliares.
    for i, r in enumerate(results):
        # Pef
        if pef_mode == "percentual":
            pef = r.p_mm * pef_percentual
        else:
            pef = r.p_mm

        # SR em mm
        # Como no app o SR depende de Z, e Z pode variar conforme o modo,
        # reconstruímos a partir de Kl quando possível seria ruim.
        # Então usamos a relação do próprio modo do dia através de TAW:
        # TAW = 1000 * (theta_fc - theta_wp) * Z
        theta_fc = soil.ucc * soil.ds
        theta_wp = soil.upmp * soil.ds
        denom = 1000.0 * max(theta_fc - theta_wp, 1e-12)
        z_m_estimado = r.taw_mm / denom if denom > 0 else 0.0
        sr_mm = z_m_estimado * 1000.0

        # DTA e DRA
        dta_mm = r.taw_mm
        dra_mm = r.raw_mm

        # Água armazenada
        la_in = dta_mm if i == 0 else rows[-1]["LA f"]
        la_antes_irrig = la_in + pef + r.irrigacao_real_mm - r.etc_mm
        la_f = max(0.0, min(dta_mm, la_antes_irrig))

        # Lâmina mínima de armazenamento antes de irrigar
        la_mi = dta_mm - dra_mm

        # Balanços
        p_menos_etc = pef - r.etc_mm
        p_i_menos_etc = pef + r.irrigacao_real_mm - r.etc_mm

        # LLI e LBI no estilo da planilha
        # Se LA final caiu abaixo da LA mínima, precisa repor até DTA
        if la_f <= la_mi:
            lli = dta_mm - la_f
        else:
            lli = 0.0

        lbi = lli / eficiencia if eficiencia > 0 else 0.0

        # Aqui estou usando a irrigação real do dia como "LLI aplicada"
        lli_aplicada = r.irrigacao_real_mm

        # DP = depleção acumulada do dia (Dr)
        dp = 0.0 if r.deplecao_mm < r.raw_mm else r.deplecao_mm

        rows.append({
            "Data": r.data.strftime("%d/%m/%Y"),
            "DAP": r.dap,
            "Pef": round(pef, 3),
            "ETo": round(r.eto_mm, 3),
            "Kc": round(r.kc, 4),
            "Ks": round(r.ks, 4),
            "ETc (mm)": round(r.etc_mm, 3),
            "SR (mm)": round(sr_mm, 3),
            "P-ETc": round(p_menos_etc, 3),
            "(P+I-ETc)": round(p_i_menos_etc, 3),
            "DTA (mm)": round(dta_mm, 3),
            "DRA (mm)": round(dra_mm, 3),
            "LA in": round(la_in, 3),
            "LA antes irrigação": round(la_antes_irrig, 3),
            "LA f": round(la_f, 3),
            "LA mi": round(la_mi, 3),
            "LLI": round(lli, 3),
            "LBI": round(lbi, 3),
            "LLI aplicada": round(lli_aplicada, 3),
            "DP": round(dp, 3),
        })

    return pd.DataFrame(rows)


def build_future_weather_data(
    start_date: date,
    num_days: int,
    eto_mm: float,
    precipitacao_mm: float,
) -> List[WeatherDay]:
    """Gera dados sintéticos para testar dias futuros sem esperar datas reais."""
    future_days: List[WeatherDay] = []
    for i in range(num_days):
        future_days.append(
            WeatherDay(
                data=start_date + timedelta(days=i),
                precipitacao_mm=float(precipitacao_mm),
                eto_mm=float(eto_mm),
            )
        )
    return future_days

def compute_effective_z_m(
    crop: Crop,
    dap: int,
    modo_calculo: str,
    z_override_m: Optional[float] = None,
) -> float:
    """
    Retorna o Z efetivo do dia.

    modo_calculo:
    - "fao56": usa Z fixo da cultura (ou override manual)
    - "planilha": usa SR = 100 mm nos primeiros 10 dias, isto é, Z = 0,10 m;
                  após isso usa o Z final da cultura (ou override manual)
    """
    z_final = z_override_m if z_override_m is not None else crop.z_m

    if modo_calculo == "planilha":
        if dap <= 10:
            return 0.10  # SR = 100 mm
        return z_final

    return z_final


def compute_kl_from_sr_mm(sr_mm: float) -> float:
    return clamp(0.1 * math.sqrt(sr_mm), 0.0, 1.0)


def compute_taw_mm_from_z(soil: Soil, z_m: float) -> float:
    theta_fc = soil.ucc * soil.ds
    theta_wp = soil.upmp * soil.ds
    return max(0.0, 1000.0 * (theta_fc - theta_wp) * z_m)

def compute_sr_mm(z_m: float) -> float:
    return z_m * 1000.0


def compute_kl(z_m: float) -> float:
    sr_mm = compute_sr_mm(z_m)
    return clamp(0.1 * math.sqrt(sr_mm), 0.0, 1.0)


def compute_taw_mm(soil: Soil, z_m: float) -> float:
    theta_fc = soil.ucc * soil.ds
    theta_wp = soil.upmp * soil.ds
    return max(0.0, 1000.0 * (theta_fc - theta_wp) * z_m)


def compute_raw_mm(taw_mm: float, f: float) -> float:
    return max(0.0, taw_mm * f)


def stage_limits(crop: Crop) -> Dict[str, int]:
    fim_in = crop.duracao_in
    fim_cv = fim_in + crop.duracao_cv
    fim_medio = fim_cv + crop.duracao_medio
    fim_final = fim_medio + crop.duracao_final
    return {
        "fim_in": fim_in,
        "fim_cv": fim_cv,
        "fim_medio": fim_medio,
        "fim_final": fim_final,
    }


def akc_values(crop: Crop) -> Dict[str, float]:
    akc_in = crop.kc_in
    akc_cv = (crop.kc_m - crop.kc_cv) / crop.duracao_cv if crop.duracao_cv > 0 else 0.0
    akc_m = 0.0
    akc_final = (crop.kc_final - crop.kc_m) / crop.duracao_final if crop.duracao_final > 0 else 0.0
    return {
        "akc_in": akc_in,
        "akc_cv": akc_cv,
        "akc_m": akc_m,
        "akc_final": akc_final,
    }


def compute_phase_kc_akc(crop: Crop, dap: int) -> Tuple[str, float, float]:
    limits = stage_limits(crop)

    # incremento da fase inicial (crescimento linear até kc_cv)
    akc_inicial = (
        (crop.kc_cv - crop.kc_in) / crop.duracao_in
        if crop.duracao_in > 0 else 0.0
    )

    if dap <= limits["fim_in"]:
        fase = "inicial"
        dias_na_fase = max(dap - 1, 0)
        kc = crop.kc_in + akc_inicial * dias_na_fase
        akc_usado = akc_inicial

    elif dap <= limits["fim_cv"]:
        fase = "desenvolvimento"

        # último valor da fase inicial
        kc_inicio_desenvolvimento = crop.kc_in + akc_inicial * (crop.duracao_in - 1)

        dias_na_fase = dap - limits["fim_in"]

        akc_cv = (
            (crop.kc_m - kc_inicio_desenvolvimento) / crop.duracao_cv
            if crop.duracao_cv > 0 else 0.0
        )

        kc = kc_inicio_desenvolvimento + akc_cv * dias_na_fase
        akc_usado = akc_cv

    elif dap <= limits["fim_medio"]:
        fase = "medio"
        kc = crop.kc_m
        akc_usado = 0.0

    elif dap <= limits["fim_final"]:
        fase = "final"
        dias_na_fase = dap - limits["fim_medio"]

        akc_final = (
            (crop.kc_final - crop.kc_m) / crop.duracao_final
            if crop.duracao_final > 0 else 0.0
        )

        kc = crop.kc_m + akc_final * dias_na_fase
        akc_usado = akc_final

    else:
        fase = "apos_ciclo"
        kc = crop.kc_final
        akc_usado = 0.0

    return fase, round(kc, 4), round(akc_usado, 5)


def compute_ks(dr_mm: float, taw_mm: float, raw_mm: float) -> float:
    if taw_mm <= 0:
        return 1.0
    if dr_mm <= raw_mm:
        return 1.0

    denom = taw_mm - raw_mm
    if denom <= 0:
        return 1.0

    return clamp((taw_mm - dr_mm) / denom, 0.0, 1.0)


def gross_irrigation(lli_mm: float, eficiencia: float) -> float:
    if eficiencia <= 0:
        return 0.0
    return lli_mm / eficiencia


def simulate_irrigation(
    crop: Crop,
    soil: Soil,
    sistema_irrigacao: str,
    data_plantio: date,
    weather_data: List[WeatherDay],
    z_override_m: Optional[float] = None,
    irrigacao_real_por_dia: Optional[Dict[date, float]] = None,
    modo_automatico: bool = True,
    modo_calculo: str = "fao56",
) -> List[ResultDay]:
    sistema_key = normalize_name(sistema_irrigacao)
    if sistema_key not in IRRIGATION_EFFICIENCY:
        raise ValueError(f"Sistema de irrigação inválido: {sistema_irrigacao}")

    eficiencia = IRRIGATION_EFFICIENCY[sistema_key]
    f = crop.fator_f

    irrigacao_real_por_dia = irrigacao_real_por_dia or {}

    resultados: List[ResultDay] = []
    dr_mm = 0.0

    for wd in weather_data:
        dap = (wd.data - data_plantio).days + 1
        if dap < 1:
            continue

        # Z efetivo do dia conforme o modo escolhido
        z_m = compute_effective_z_m(
            crop=crop,
            dap=dap,
            modo_calculo=modo_calculo,
            z_override_m=z_override_m,
        )

        sr_mm = compute_sr_mm(z_m)
        kl = compute_kl_from_sr_mm(sr_mm)
        taw_mm = compute_taw_mm_from_z(soil, z_m)
        raw_mm = compute_raw_mm(taw_mm, f)

        fase, kc, akc = compute_phase_kc_akc(crop, dap)
        ks = compute_ks(dr_mm, taw_mm, raw_mm)

        etc_mm = wd.eto_mm * kc * ks * kl
        irrig_real_mm = float(irrigacao_real_por_dia.get(wd.data, 0.0))

        dr_mm = dr_mm - wd.precipitacao_mm - irrig_real_mm + etc_mm
        dr_mm = clamp(dr_mm, 0.0, taw_mm)

        lli_mm = 0.0
        lbi_mm = 0.0

        if modo_automatico and dr_mm >= raw_mm:
            lli_mm = dr_mm
            lbi_mm = gross_irrigation(lli_mm, eficiencia)

        resultados.append(
            ResultDay(
                data=wd.data,
                dap=dap,
                fase=fase,
                kc=round(kc, 4),
                akc=round(akc, 5),
                kl=round(kl, 4),
                ks=round(ks, 4),
                eto_mm=round(wd.eto_mm, 3),
                etc_mm=round(etc_mm, 3),
                p_mm=round(wd.precipitacao_mm, 3),
                irrigacao_real_mm=round(irrig_real_mm, 3),
                deplecao_mm=round(dr_mm, 3),
                taw_mm=round(taw_mm, 3),
                raw_mm=round(raw_mm, 3),
                lli_mm=round(lli_mm, 3),
                lbi_mm=round(lbi_mm, 3),
            )
        )

    return resultados


def results_to_dataframe(results: List[ResultDay]) -> pd.DataFrame:
    rows = []
    for r in results:
        rows.append({
            "Data": r.data.strftime("%d/%m/%Y"),
            "DAP": r.dap,
            "Fase": r.fase,
            "Kc": r.kc,
            "AKc": r.akc,
            "Kl": r.kl,
            "Ks": r.ks,
            "ETo (mm)": r.eto_mm,
            "ETc (mm)": r.etc_mm,
            "P (mm)": r.p_mm,
            "I_real (mm)": r.irrigacao_real_mm,
            "Dr (mm)": r.deplecao_mm,
            "TAW (mm)": r.taw_mm,
            "RAW (mm)": r.raw_mm,
            "LLI (mm)": r.lli_mm,
            "LBI (mm)": r.lbi_mm,
        })
    return pd.DataFrame(rows)


# ============================================================
# APP STREAMLIT
# ============================================================

# ============================================================
# APP STREAMLIT
# ============================================================

init_db()

st.set_page_config(
    page_title="Controle de Irrigação",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Controle de Irrigação")
st.caption("Acompanhamento diário da cultura, clima e necessidade de irrigação")


def format_plantio_label(row: pd.Series) -> str:
    return (
        f"{row['nome']} | {CROPS.get(row['cultura_key'], Crop(row['cultura_key'],0,0,0,0,0,0,0,0,0,0,0)).nome if row['cultura_key'] in CROPS else row['cultura_key']} "
        f"| plantio em {pd.to_datetime(row['data_plantio']).strftime('%d/%m/%Y')}"
    )


def render_empty_state(message: str):
    st.info(message)


def render_sidebar() -> str:
    with st.sidebar:
        st.header("Navegação")
        pagina = st.radio(
            "Escolha a área",
            [
                "Novo plantio",
                "Operação diária",
                "Histórico",
                "Cadastros",
                "Cálculos",
            ],
            key="pagina_principal",
        )

        st.divider()
        st.subheader("Orientação")
        st.caption(
            "Use a operação diária para consultar o clima do dia, registrar a irrigação realizada "
            "e projetar os próximos dias."
        )

    return pagina


def render_resumo_plantio_card(plantio: dict):
    crop = CROPS[plantio["cultura_key"]]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Cultura", crop.nome)
    c2.metric("Sistema", str(plantio["sistema_irrigacao"]).title())
    c3.metric("Plantio", pd.to_datetime(plantio["data_plantio"]).strftime("%d/%m/%Y"))
    c4.metric("Local", plantio["local"])

    st.caption(
        f"Latitude: {plantio['latitude']:.6f} | Longitude: {plantio['longitude']:.6f} | "
        f"Timezone: {plantio['timezone']}"
    )


def render_novo_plantio():
    st.subheader("Cadastrar novo plantio")
    st.write("Informe os dados básicos da área, cultura, solo e irrigação.")

    solos_df = load_solos_df()

    with st.form("form_novo_plantio", clear_on_submit=False):
        st.markdown("### Identificação da área")
        c1, c2, c3 = st.columns(3)
        nome = c1.text_input("Nome do plantio ou talhão", value="Talhão A")
        local = c2.text_input("Local", value="UEPG")
        data_plantio = c3.date_input("Data de plantio", value=date.today())

        st.markdown("### Localização")
        c4, c5, c6 = st.columns(3)
        latitude = c4.number_input("Latitude", value=-25.095000, format="%.6f")
        longitude = c5.number_input("Longitude", value=-50.161900, format="%.6f")
        timezone = c6.text_input("Timezone", value="America/Sao_Paulo")

        st.markdown("### Cultura e irrigação")
        c7, c8 = st.columns(2)
        cultura_key = c7.selectbox(
            "Cultura",
            list(CROPS.keys()),
            format_func=lambda x: CROPS[x].nome,
        )
        sistema_irrigacao = c8.selectbox(
            "Sistema de irrigação",
            list(IRRIGATION_EFFICIENCY.keys()),
            format_func=lambda x: x.title(),
        )

        crop = CROPS[cultura_key]
        st.caption(
            f"Profundidade radicular padrão (Z): {crop.z_m:.2f} m | "
            f"Fator f da cultura: {crop.fator_f:.2f}"
        )

        st.markdown("### Solo")
        usar_solo_cadastrado = st.checkbox("Usar solo cadastrado", value=True)

        ucc = 0.0
        upmp = 0.0
        ds = 0.0

        if usar_solo_cadastrado and not solos_df.empty:
            opcoes_solo = {
                f"{row['nome']} | Ucc={row['ucc']:.3f} | Upmp={row['upmp']:.3f} | Ds={row['ds']:.3f}": row
                for _, row in solos_df.iterrows()
            }
            solo_escolhido_label = st.selectbox("Solo cadastrado", list(opcoes_solo.keys()))
            solo_sel = opcoes_solo[solo_escolhido_label]
            ucc = float(solo_sel["ucc"])
            upmp = float(solo_sel["upmp"])
            ds = float(solo_sel["ds"])

            c9, c10, c11 = st.columns(3)
            c9.number_input("Ucc", value=ucc, format="%.3f", disabled=True)
            c10.number_input("Upmp", value=upmp, format="%.3f", disabled=True)
            c11.number_input("Ds", value=ds, format="%.3f", disabled=True)
        else:
            c9, c10, c11 = st.columns(3)
            ucc = c9.number_input("Ucc", min_value=0.0, value=0.30, format="%.3f")
            upmp = c10.number_input("Upmp", min_value=0.0, value=0.15, format="%.3f")
            ds = c11.number_input("Ds", min_value=0.0, value=1.30, format="%.3f")

        st.markdown("### Ajustes opcionais")
        z_override_flag = st.checkbox("Definir profundidade radicular manualmente", value=False)
        z_override_m = None
        if z_override_flag:
            z_override_m = st.number_input(
                "Profundidade radicular (m)",
                min_value=0.01,
                value=float(crop.z_m),
                format="%.2f",
            )

        salvar = st.form_submit_button("Salvar plantio", use_container_width=True)

    if salvar:
        try:
            create_plantio(
                nome=nome,
                local=local,
                latitude=latitude,
                longitude=longitude,
                timezone=timezone,
                cultura_key=cultura_key,
                sistema_irrigacao=sistema_irrigacao,
                data_plantio=data_plantio,
                ucc=ucc,
                upmp=upmp,
                ds=ds,
                z_override_m=z_override_m,
            )
            clear_app_caches()
            st.success("Plantio cadastrado com sucesso.")
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao cadastrar plantio: {e}")


def render_operacao_diaria():
    st.subheader("Operação diária")
    st.write("Consulte o dia atual, registre a irrigação realizada e projete os próximos dias.")

    plantios_df = list_plantios()
    if plantios_df.empty:
        render_empty_state("Nenhum plantio cadastrado ainda.")
        return

    opcoes = {
        format_plantio_label(row): str(row["id"])
        for _, row in plantios_df.iterrows()
    }

    with st.form("form_contexto_operacao"):
        st.markdown("### Seleção do plantio")
        plantio_label = st.selectbox("Escolha o plantio", list(opcoes.keys()))
        plantio_id = opcoes[plantio_label]

        plantio = get_plantio(plantio_id)
        data_plantio = to_date(plantio["data_plantio"])

        st.markdown("### Parâmetros de cálculo")
        c1, c2, c3 = st.columns(3)
        data_operacao = c1.date_input(
            "Data de operação",
            value=max(date.today(), data_plantio),
            min_value=data_plantio,
            key=f"data_operacao_{plantio_id}",
        )
        modo_calculo_key = c2.selectbox(
            "Método de cálculo",
            ["fao56", "planilha"],
            format_func=lambda x: "FAO-56" if x == "fao56" else "Planilha",
            key=f"modo_calculo_{plantio_id}",
        )
        modo_auto = c3.selectbox(
            "Modo de irrigação na simulação",
            [True, False],
            format_func=lambda x: "Automático" if x else "Manual",
            key=f"modo_auto_{plantio_id}",
        )

        carregar = st.form_submit_button("Atualizar análise", use_container_width=True)

    if not carregar and "operacao_plantio_id" not in st.session_state:
        return

    st.session_state["operacao_plantio_id"] = plantio_id
    st.session_state["operacao_data"] = data_operacao
    st.session_state["operacao_modo"] = modo_calculo_key
    st.session_state["operacao_auto"] = modo_auto

    plantio = get_plantio(st.session_state["operacao_plantio_id"])
    data_operacao = st.session_state["operacao_data"]
    modo_calculo_key = st.session_state["operacao_modo"]
    modo_auto = st.session_state["operacao_auto"]

    crop = CROPS[plantio["cultura_key"]]
    soil = Soil(
        ucc=float(plantio["ucc"]),
        upmp=float(plantio["upmp"]),
        ds=float(plantio["ds"]),
    )
    data_plantio = to_date(plantio["data_plantio"])
    irrigacao_map = get_irrigation_map(plantio["id"])

    render_resumo_plantio_card(plantio)

    try:
        weather_data = fetch_weather_open_meteo(
            latitude=plantio["latitude"],
            longitude=plantio["longitude"],
            start_date=data_plantio,
            end_date=data_operacao,
            timezone=plantio["timezone"],
        )

        resultados_antes = simulate_irrigation(
            crop=crop,
            soil=soil,
            sistema_irrigacao=plantio["sistema_irrigacao"],
            data_plantio=data_plantio,
            weather_data=weather_data,
            z_override_m=plantio["z_override_m"],
            irrigacao_real_por_dia=irrigacao_map,
            modo_automatico=modo_auto,
            modo_calculo=modo_calculo_key,
        )

        if not resultados_antes:
            st.error("Não foi possível gerar os resultados para a data escolhida.")
            return

        hoje_previsto = resultados_antes[-1]

        st.markdown("### Resumo da operação do dia")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DAP", hoje_previsto.dap)
        m2.metric("Depleção atual (Dr)", f"{hoje_previsto.deplecao_mm:.3f} mm")
        m3.metric("LLI recomendada", f"{hoje_previsto.lli_mm:.3f} mm")
        m4.metric("LBI recomendada", f"{hoje_previsto.lbi_mm:.3f} mm")

        c_info1, c_info2 = st.columns([1.15, 1])
        with c_info1:
            st.markdown("### Condição do dia")
            resumo_df = pd.DataFrame([{
                "Data": hoje_previsto.data.strftime("%d/%m/%Y"),
                "Fase": explain_phase_name(hoje_previsto.fase),
                "Kc": hoje_previsto.kc,
                "Ks": hoje_previsto.ks,
                "Kl": hoje_previsto.kl,
                "ETo (mm)": hoje_previsto.eto_mm,
                "ETc (mm)": hoje_previsto.etc_mm,
                "Precipitação (mm)": hoje_previsto.p_mm,
                "RAW (mm)": hoje_previsto.raw_mm,
                "TAW (mm)": hoje_previsto.taw_mm,
            }])
            st.dataframe(resumo_df, width="stretch", hide_index=True)

        with c_info2:
            st.markdown("### Registrar decisão do dia")
            with st.form(f"form_salvar_dia_{plantio['id']}"):
                decisao = st.radio(
                    "Irrigação realizada",
                    ["Não irrigar", "Irrigar"],
                    horizontal=True,
                )

                irrigacao_informada = 0.0
                if decisao == "Irrigar":
                    irrigacao_informada = st.number_input(
                        "Lâmina real aplicada (mm)",
                        min_value=0.0,
                        value=float(hoje_previsto.lli_mm),
                        format="%.3f",
                    )

                salvar_dia = st.form_submit_button("Salvar operação do dia", use_container_width=True)

            if salvar_dia:
                try:
                    irrigacao_map_atualizada = dict(irrigacao_map)
                    irrigacao_map_atualizada[data_operacao] = float(irrigacao_informada)

                    resultados_finais = simulate_irrigation(
                        crop=crop,
                        soil=soil,
                        sistema_irrigacao=plantio["sistema_irrigacao"],
                        data_plantio=data_plantio,
                        weather_data=weather_data,
                        z_override_m=plantio["z_override_m"],
                        irrigacao_real_por_dia=irrigacao_map_atualizada,
                        modo_automatico=modo_auto,
                        modo_calculo=modo_calculo_key,
                    )

                    dia_final = resultados_finais[-1]
                    upsert_day_result(
                        plantio_id=plantio["id"],
                        irrigou=(decisao == "Irrigar"),
                        result=dia_final,
                    )

                    clear_app_caches()
                    st.success("Operação diária salva com sucesso.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao salvar a operação do dia: {e}")

        st.markdown("### Evolução até a data selecionada")
        df_resultados = results_to_dataframe(resultados_antes)
        st.dataframe(df_resultados, width="stretch", hide_index=True)

        st.markdown("### Projeção futura")
        st.caption("Esta área agora ficou dentro da operação diária, usando a mesma base do plantio selecionado.")

        with st.form(f"form_simulacao_futura_{plantio['id']}"):
            s1, s2, s3 = st.columns(3)
            dias_futuros = s1.number_input(
                "Número de dias para projetar",
                min_value=1,
                max_value=30,
                value=7,
                step=1,
            )
            pef_mode_sim = s2.selectbox(
                "Precipitação efetiva",
                ["igual_p", "percentual"],
                format_func=lambda x: "Usar precipitação total" if x == "igual_p" else "Usar percentual da precipitação",
            )
            pef_percentual_sim = s3.number_input(
                "Percentual da precipitação efetiva",
                min_value=0.0,
                max_value=1.0,
                value=1.0,
                step=0.05,
                format="%.2f",
                disabled=(pef_mode_sim != "percentual"),
            )

            simular_futuro = st.form_submit_button("Gerar projeção futura", use_container_width=True)

        if simular_futuro:
            try:
                previsao_inicio = data_operacao
                previsao_fim = data_operacao + timedelta(days=int(dias_futuros) - 1)

                future_weather = fetch_weather_open_meteo(
                    latitude=plantio["latitude"],
                    longitude=plantio["longitude"],
                    start_date=previsao_inicio,
                    end_date=previsao_fim,
                    timezone=plantio["timezone"],
                )

                weather_data_expandido = merge_weather_data_by_date(weather_data, future_weather)

                resultados_expandido = simulate_irrigation(
                    crop=crop,
                    soil=soil,
                    sistema_irrigacao=plantio["sistema_irrigacao"],
                    data_plantio=data_plantio,
                    weather_data=weather_data_expandido,
                    z_override_m=plantio["z_override_m"],
                    irrigacao_real_por_dia=irrigacao_map,
                    modo_automatico=True,
                    modo_calculo=modo_calculo_key,
                )

                resultados_futuros = [
                    res for res in resultados_expandido
                    if data_operacao <= res.data <= previsao_fim
                ]

                if not resultados_futuros:
                    st.warning("Não foi possível gerar a projeção futura.")
                else:
                    eficiencia_sim = IRRIGATION_EFFICIENCY[normalize_name(plantio["sistema_irrigacao"])]
                    df_sim = build_planilha_prof_df(
                        results=resultados_futuros,
                        soil=soil,
                        crop=crop,
                        eficiencia=eficiencia_sim,
                        pef_mode=pef_mode_sim,
                        pef_percentual=pef_percentual_sim,
                    )

                    ctab1, ctab2 = st.tabs(["Tabela da projeção", "Gráfico da projeção"])

                    with ctab1:
                        st.write(
                            f"Projeção de **{previsao_inicio.strftime('%d/%m/%Y')}** até "
                            f"**{previsao_fim.strftime('%d/%m/%Y')}**."
                        )
                        st.dataframe(df_sim, width="stretch", hide_index=True)

                        st.download_button(
                            "Baixar projeção em CSV",
                            data=df_sim.to_csv(index=False).encode("utf-8-sig"),
                            file_name=f"projecao_futura_plantio_{plantio['id']}.csv",
                            mime="text/csv",
                            key=f"download_sim_csv_{plantio['id']}",
                            use_container_width=True,
                        )

                    with ctab2:
                        df_sim_grafico = df_sim.copy()
                        for col in ["DRA (mm)", "DP", "LLI", "LBI", "ETc (mm)", "Pef", "LA f"]:
                            if col in df_sim_grafico.columns:
                                df_sim_grafico[col] = pd.to_numeric(df_sim_grafico[col], errors="coerce")

                        colunas_plot = [c for c in ["DP", "DRA (mm)", "LA f"] if c in df_sim_grafico.columns]
                        if colunas_plot:
                            st.line_chart(
                                df_sim_grafico.set_index("Data")[colunas_plot],
                                height=320,
                            )
                        else:
                            st.info("Não há colunas suficientes para exibir o gráfico.")
            except Exception as e:
                st.error(f"Erro ao processar a projeção futura: {e}")

    except Exception as e:
        st.error(f"Erro ao processar a operação diária: {e}")


def render_historico():
    st.subheader("Histórico do plantio")
    st.write("Consulte os dias já salvos e exporte o histórico em CSV.")

    plantios_df = list_plantios_com_historico()

    if plantios_df.empty:
        render_empty_state("Ainda não há plantios com histórico salvo.")
        return

    opcoes_hist = {
        format_plantio_label(row): str(row["id"])
        for _, row in plantios_df.iterrows()
    }

    plantio_hist_label = st.selectbox("Escolha o plantio", list(opcoes_hist.keys()), key="hist")
    plantio_hist_id = opcoes_hist[plantio_hist_label]

    hist_df = load_history_df(plantio_hist_id)

    if hist_df.empty:
        st.warning("Esse plantio ainda não possui dias salvos.")
        return

    st.dataframe(hist_df, width="stretch", hide_index=True)

    csv_bytes = hist_df.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button(
        "Baixar histórico em CSV",
        data=csv_bytes,
        file_name=f"historico_plantio_{plantio_hist_id}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    st.markdown("### Ações de exclusão")
    c1, c2 = st.columns(2)

    with c1:
        with st.form("form_excluir_dia_hist"):
            data_excluir = st.text_input("Data para excluir (AAAA-MM-DD)")
            excluir_dia = st.form_submit_button("Excluir dia")

        if excluir_dia:
            try:
                delete_history_day(plantio_hist_id, data_excluir)
                clear_app_caches()
                st.success("Dia excluído com sucesso.")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao excluir dia: {e}")

    with c2:
        with st.form("form_excluir_todo_hist"):
            excluir_tudo = st.form_submit_button("Excluir todo o histórico", use_container_width=True)

        if excluir_tudo:
            try:
                delete_all_history(plantio_hist_id)
                clear_app_caches()
                st.success("Histórico excluído com sucesso.")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao excluir histórico: {e}")


def render_cadastros():
    st.subheader("Cadastros")
    st.write("Gerencie os registros base do sistema.")

    tab_solos, tab_plantios, tab_culturas = st.tabs(["Solos", "Plantios / Talhões", "Culturas"])

    with tab_solos:
        solos_df = load_solos_df()

        col_form, col_lista = st.columns([1, 1.3])

        with col_form:
            st.markdown("### Novo solo")
            with st.form("form_novo_solo"):
                nome_solo = st.text_input("Nome do solo")
                c1, c2, c3 = st.columns(3)
                ucc = c1.number_input("Ucc", min_value=0.0, value=0.30, format="%.3f")
                upmp = c2.number_input("Upmp", min_value=0.0, value=0.15, format="%.3f")
                ds = c3.number_input("Ds", min_value=0.0, value=1.30, format="%.3f")
                salvar_solo = st.form_submit_button("Salvar solo", use_container_width=True)

            if salvar_solo:
                try:
                    create_solo(nome_solo, ucc, upmp, ds)
                    clear_app_caches()
                    st.success("Solo cadastrado com sucesso.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao cadastrar solo: {e}")

        with col_lista:
            st.markdown("### Solos cadastrados")
            if solos_df.empty:
                st.info("Nenhum solo cadastrado.")
            else:
                solos_exibicao = solos_df.copy()
                if "created_at" in solos_exibicao.columns:
                    solos_exibicao = solos_exibicao.drop(columns=["created_at"], errors="ignore")

                st.dataframe(solos_exibicao, width="stretch", hide_index=True)

                opcoes_delete = {
                    f"{row['nome']} | Ucc={row['ucc']:.3f} | Upmp={row['upmp']:.3f} | Ds={row['ds']:.3f}": row["id"]
                    for _, row in solos_df.iterrows()
                }

                with st.form("form_excluir_solo"):
                    solo_delete_label = st.selectbox("Selecione um solo para excluir", list(opcoes_delete.keys()))
                    excluir_solo = st.form_submit_button("Excluir solo", use_container_width=True)

                if excluir_solo:
                    try:
                        delete_solo(opcoes_delete[solo_delete_label])
                        clear_app_caches()
                        st.success("Solo excluído com sucesso.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro ao excluir solo: {e}")

    with tab_plantios:
        st.markdown("### Plantios e talhões cadastrados")
        plantios_df = list_plantios()

        if plantios_df.empty:
            st.info("Nenhum plantio ou talhão cadastrado.")
        else:
            plantios_exibicao = plantios_df.copy()

            if "data_plantio" in plantios_exibicao.columns:
                plantios_exibicao["data_plantio"] = pd.to_datetime(
                    plantios_exibicao["data_plantio"], errors="coerce"
                ).dt.strftime("%d/%m/%Y")

            if "cultura_key" in plantios_exibicao.columns:
                plantios_exibicao["cultura"] = plantios_exibicao["cultura_key"].apply(
                    lambda x: CROPS[x].nome if x in CROPS else x
                )

            colunas_preferidas = [
                "nome",
                "local",
                "cultura",
                "sistema_irrigacao",
                "data_plantio",
                "latitude",
                "longitude",
                "timezone",
            ]
            colunas_existentes = [c for c in colunas_preferidas if c in plantios_exibicao.columns]

            st.dataframe(
                plantios_exibicao[colunas_existentes],
                width="stretch",
                hide_index=True,
            )

            st.markdown("### Excluir plantio ou talhão")
            opcoes_plantio_delete = {
                format_plantio_label(row): str(row["id"])
                for _, row in plantios_df.iterrows()
            }

            with st.form("form_excluir_plantio"):
                plantio_delete_label = st.selectbox(
                    "Selecione o plantio ou talhão",
                    list(opcoes_plantio_delete.keys()),
                )
                excluir_plantio_confirm = st.checkbox(
                    "Confirmo que desejo excluir este cadastro e todo o histórico relacionado",
                    value=False,
                )
                excluir_plantio_btn = st.form_submit_button(
                    "Excluir plantio / talhão",
                    use_container_width=True,
                )

            if excluir_plantio_btn:
                if not excluir_plantio_confirm:
                    st.warning("Marque a confirmação para excluir o cadastro.")
                else:
                    try:
                        delete_plantio(opcoes_plantio_delete[plantio_delete_label])
                        clear_app_caches()
                        st.success("Plantio ou talhão excluído com sucesso.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro ao excluir plantio ou talhão: {e}")

    with tab_culturas:
        st.markdown("### Culturas disponíveis")
        st.dataframe(crops_to_df(), width="stretch", hide_index=True)

def build_calculos_completos_df(
    plantio: dict,
    crop: Crop,
    soil: Soil,
    data_final: date,
    modo_calculo: str,
    pef_mode: str,
    pef_percentual: float,
) -> pd.DataFrame:
    data_plantio = to_date(plantio["data_plantio"])

    weather_data = fetch_weather_open_meteo(
        latitude=plantio["latitude"],
        longitude=plantio["longitude"],
        start_date=data_plantio,
        end_date=data_final,
        timezone=plantio["timezone"],
    )

    irrigacao_map = get_irrigation_map(plantio["id"])

    resultados = simulate_irrigation(
        crop=crop,
        soil=soil,
        sistema_irrigacao=plantio["sistema_irrigacao"],
        data_plantio=data_plantio,
        weather_data=weather_data,
        z_override_m=plantio["z_override_m"],
        irrigacao_real_por_dia=irrigacao_map,
        modo_automatico=True,
        modo_calculo=modo_calculo,
    )

    eficiencia = IRRIGATION_EFFICIENCY[normalize_name(plantio["sistema_irrigacao"])]

    df_planilha = build_planilha_prof_df(
        results=resultados,
        soil=soil,
        crop=crop,
        eficiencia=eficiencia,
        pef_mode=pef_mode,
        pef_percentual=pef_percentual,
    )

    rows = []
    for i, r in enumerate(resultados):
        z_m = compute_effective_z_m(
            crop=crop,
            dap=r.dap,
            modo_calculo=modo_calculo,
            z_override_m=plantio["z_override_m"],
        )
        sr_mm = compute_sr_mm(z_m)

        # Kc p = Kc potencial do dia, antes do efeito de estresse hídrico e Kl
        kc_p = round(r.kc * r.kl, 4)

        row_plan = df_planilha.iloc[i].to_dict()

        rows.append({
            "Data": r.data.strftime("%d/%m/%Y"),
            "DAP": r.dap,
            "Fase": explain_phase_name(r.fase),
            "AKc": round(r.akc, 5),
            "Kc p": kc_p,
            "Kc in": round(crop.kc_in, 4),
            "Kc cv": round(crop.kc_cv, 4),
            "Kc m": round(crop.kc_m, 4),
            "Kc final": round(crop.kc_final, 4),
            "Kc": round(r.kc, 4),
            "Kl": round(r.kl, 4),
            "Ks": round(r.ks, 4),
            "Pef": row_plan["Pef"],
            "ETo": row_plan["ETo"],
            "ETc (mm)": row_plan["ETc (mm)"],
            "SR (mm)": round(sr_mm, 3),
            "P-ETc": row_plan["P-ETc"],
            "(P+I-ETc)": row_plan["(P+I-ETc)"],
            "DTA (mm)": row_plan["DTA (mm)"],
            "DRA (mm)": row_plan["DRA (mm)"],
            "LA in": row_plan["LA in"],
            "LA antes irrigação": row_plan["LA antes irrigação"],
            "LA f": row_plan["LA f"],
            "LA mi": row_plan["LA mi"],
            "LLI": row_plan["LLI"],
            "LBI": row_plan["LBI"],
            "LLI aplicada": row_plan["LLI aplicada"],
            "DP": row_plan["DP"],
        })

    return pd.DataFrame(rows)

def render_calculos():
    st.subheader("Cálculos")
    st.write("Confira os parâmetros da cultura e a memória completa dos cálculos diários do plantio.")

    plantios_df = list_plantios()
    if plantios_df.empty:
        render_empty_state("Cadastre um plantio para usar esta área.")
        return

    opcoes = {
        format_plantio_label(row): str(row["id"])
        for _, row in plantios_df.iterrows()
    }

    with st.form("form_calculos_completos"):
        st.markdown("### Seleção do plantio")
        plantio_calc_label = st.selectbox("Escolha o plantio", list(opcoes.keys()))
        plantio_calc_id = opcoes[plantio_calc_label]
        plantio = get_plantio(plantio_calc_id)

        data_plantio = to_date(plantio["data_plantio"])

        st.markdown("### Configuração da análise")
        c1, c2, c3, c4 = st.columns(4)

        data_final = c1.date_input(
            "Calcular até a data",
            value=max(date.today(), data_plantio),
            min_value=data_plantio,
            key=f"data_final_calculos_{plantio_calc_id}",
        )

        modo_calculo = c2.selectbox(
            "Método de cálculo",
            ["fao56", "planilha"],
            format_func=lambda x: "FAO-56" if x == "fao56" else "Planilha",
            key=f"modo_calculo_calc_{plantio_calc_id}",
        )

        pef_mode = c3.selectbox(
            "Precipitação efetiva",
            ["igual_p", "percentual"],
            format_func=lambda x: "Usar precipitação total" if x == "igual_p" else "Usar percentual da precipitação",
            key=f"pef_mode_calc_{plantio_calc_id}",
        )

        pef_percentual = c4.number_input(
            "Percentual da precipitação efetiva",
            min_value=0.0,
            max_value=1.0,
            value=1.0,
            step=0.05,
            format="%.2f",
            disabled=(pef_mode != "percentual"),
            key=f"pef_percentual_calc_{plantio_calc_id}",
        )

        calcular = st.form_submit_button("Gerar memória de cálculo", use_container_width=True)

    if not calcular and "calc_plantio_id" not in st.session_state:
        return

    st.session_state["calc_plantio_id"] = plantio_calc_id
    st.session_state["calc_data_final"] = data_final
    st.session_state["calc_modo"] = modo_calculo
    st.session_state["calc_pef_mode"] = pef_mode
    st.session_state["calc_pef_percentual"] = pef_percentual

    plantio = get_plantio(st.session_state["calc_plantio_id"])
    crop = CROPS[plantio["cultura_key"]]
    soil = Soil(
        ucc=float(plantio["ucc"]),
        upmp=float(plantio["upmp"]),
        ds=float(plantio["ds"]),
    )

    data_final = st.session_state["calc_data_final"]
    modo_calculo = st.session_state["calc_modo"]
    pef_mode = st.session_state["calc_pef_mode"]
    pef_percentual = st.session_state["calc_pef_percentual"]

    try:
        render_resumo_plantio_card(plantio)

        st.markdown("### Parâmetros fixos usados na conta")
        p1, p2, p3, p4, p5 = st.columns(5)
        p1.metric("Kc in", f"{crop.kc_in:.4f}")
        p2.metric("Kc cv", f"{crop.kc_cv:.4f}")
        p3.metric("Kc m", f"{crop.kc_m:.4f}")
        p4.metric("Kc final", f"{crop.kc_final:.4f}")
        p5.metric("Fator f", f"{crop.fator_f:.2f}")

        p6, p7, p8, p9 = st.columns(4)
        z_ref = plantio["z_override_m"] if plantio["z_override_m"] is not None else crop.z_m
        p6.metric("Z de referência (m)", f"{float(z_ref):.2f}")
        p7.metric("Ucc", f"{soil.ucc:.3f}")
        p8.metric("Upmp", f"{soil.upmp:.3f}")
        p9.metric("Ds", f"{soil.ds:.3f}")

        df_calculos = build_calculos_completos_df(
            plantio=plantio,
            crop=crop,
            soil=soil,
            data_final=data_final,
            modo_calculo=modo_calculo,
            pef_mode=pef_mode,
            pef_percentual=pef_percentual,
        )

        if df_calculos.empty:
            st.warning("Não foi possível gerar os cálculos para o período informado.")
            return

        st.markdown("### Memória completa de cálculo")
        st.dataframe(df_calculos, width="stretch", hide_index=True)

        st.markdown("### Indicadores do último dia calculado")
        ultimo = df_calculos.iloc[-1]

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("DAP", int(ultimo["DAP"]))
        m2.metric("Kc do dia", f"{float(ultimo['Kc']):.4f}")
        m3.metric("ETc do dia", f"{float(ultimo['ETc (mm)']):.3f} mm")
        m4.metric("LLI do dia", f"{float(ultimo['LLI']):.3f} mm")
        m5.metric("LBI do dia", f"{float(ultimo['LBI']):.3f} mm")

        st.markdown("### Gráficos")
        tab_g1, tab_g2, tab_g3 = st.tabs(["Balanço hídrico", "Coeficientes", "Lâminas"])

        with tab_g1:
            graf1 = df_calculos.copy()
            for col in ["Pef", "ETc (mm)", "DRA (mm)", "DTA (mm)", "DP"]:
                graf1[col] = pd.to_numeric(graf1[col], errors="coerce")
            st.line_chart(
                graf1.set_index("Data")[["Pef", "ETc (mm)", "DRA (mm)", "DTA (mm)", "DP"]],
                height=320,
            )

        with tab_g2:
            graf2 = df_calculos.copy()
            for col in ["AKc", "Kc p", "Kc", "Ks"]:
                graf2[col] = pd.to_numeric(graf2[col], errors="coerce")
            st.line_chart(
                graf2.set_index("Data")[["AKc", "Kc p", "Kc", "Ks"]],
                height=320,
            )

        with tab_g3:
            graf3 = df_calculos.copy()
            for col in ["LLI", "LBI", "LLI aplicada", "LA in", "LA f", "LA mi"]:
                graf3[col] = pd.to_numeric(graf3[col], errors="coerce")
            st.line_chart(
                graf3.set_index("Data")[["LLI", "LBI", "LLI aplicada", "LA in", "LA f", "LA mi"]],
                height=320,
            )

        st.markdown("### Exportação")
        st.download_button(
            "Baixar memória de cálculo em CSV",
            data=df_calculos.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"memoria_calculo_plantio_{plantio['id']}.csv",
            mime="text/csv",
            use_container_width=True,
        )

        st.markdown("### Fórmulas resumidas")
        st.code(
            "\n".join([
                "θfc = Ucc × Ds",
                "θwp = Upmp × Ds",
                "Z = profundidade radicular efetiva do dia",
                "SR = Z × 1000",
                "Kl = 0,1 × √SR",
                "TAW = 1000 × (θfc - θwp) × Z",
                "RAW = TAW × f",
                "AKc = variação diária do Kc na fase",
                "Kc p = Kc × Kl",
                "Ks = 1, se Dr ≤ RAW",
                "Ks = (TAW - Dr) / (TAW - RAW), se Dr > RAW",
                "ETc = ETo × Kc × Ks × Kl",
                "LA antes irrigação = LA in + Pef + I_real - ETc",
                "LA f = limite entre 0 e DTA",
                "LA mi = DTA - DRA",
                "LLI = DTA - LA f, se LA f ≤ LA mi; senão 0",
                "LBI = LLI / eficiência",
                "DP = Dr, quando Dr ≥ RAW; senão 0",
            ]),
            language="text"
        )

    except Exception as e:
        st.error(f"Erro ao gerar a memória de cálculo: {e}")


pagina = render_sidebar()

if pagina == "Novo plantio":
    render_novo_plantio()
elif pagina == "Operação diária":
    render_operacao_diaria()
elif pagina == "Histórico":
    render_historico()
elif pagina == "Cadastros":
    render_cadastros()
elif pagina == "Cálculos":
    render_calculos()
