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


def list_plantios() -> pd.DataFrame:
    data = sb_select(
        "plantios",
        select_cols="id,nome,local,cultura_key,sistema_irrigacao,data_plantio,latitude,longitude,timezone",
        order_by="created_at",
        ascending=False,
    )
    return pd.DataFrame(data)


def get_plantio(plantio_id: str) -> Optional[dict]:
    data = sb_select("plantios", filters={"id": f"eq.{plantio_id}"}, limit=1)
    return data[0] if data else None


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

init_db()

st.set_page_config(page_title="Controle de Irrigação", layout="wide")
st.title("Controle de Irrigação - FAO-56")
st.caption("Com estado anterior, operação diária, histórico, culturas e solos")

aba1, aba2, aba3, aba4, aba5 = st.tabs(["Novo plantio", "Operação diária", "Histórico", "Cadastros", "Cálculos"])


with aba1:
    st.subheader("Cadastrar novo plantio")

    with st.form("form_novo_plantio"):
        c1, c2, c3 = st.columns(3)
        nome = c1.text_input("Nome do plantio / talhão", value="Talhão A")
        local = c2.text_input("Local", value="UEPG")
        data_plantio = c3.date_input("Data de plantio", value=date.today())

        c4, c5, c6 = st.columns(3)
        latitude = c4.number_input("Latitude", value=-25.095000, format="%.6f")
        longitude = c5.number_input("Longitude", value=-50.161900, format="%.6f")
        timezone = c6.text_input("Timezone", value="America/Sao_Paulo")

        c7, c8 = st.columns(2)
        cultura_key = c7.selectbox(
            "Cultura",
            list(CROPS.keys()),
            format_func=lambda x: CROPS[x].nome
        )
        sistema_irrigacao = c8.selectbox(
            "Sistema de irrigação",
            list(IRRIGATION_EFFICIENCY.keys()),
            format_func=lambda x: x.title()
        )

        st.markdown("### Solo")

        solos_df = load_solos_df()
        usar_solo_cadastrado = st.checkbox("Usar solo cadastrado", value=True)

        if usar_solo_cadastrado and not solos_df.empty:
            opcoes_solo = {
                f"{row['nome']} | Ucc={row['ucc']:.3f} | Upmp={row['upmp']:.3f} | Ds={row['ds']:.3f}": row
                for _, row in solos_df.iterrows()
            }
            solo_label = st.selectbox("Escolha o solo", list(opcoes_solo.keys()))
            solo_row = opcoes_solo[solo_label]

            c9, c10, c11 = st.columns(3)
            ucc = c9.number_input("Ucc (g/g)", min_value=0.0, value=float(solo_row["ucc"]), format="%.3f")
            upmp = c10.number_input("Upmp (g/g)", min_value=0.0, value=float(solo_row["upmp"]), format="%.3f")
            ds = c11.number_input("Ds (g/cm³)", min_value=0.0, value=float(solo_row["ds"]), format="%.3f")
        else:
            c9, c10, c11 = st.columns(3)
            ucc = c9.number_input("Ucc (g/g)", min_value=0.0, value=0.32, format="%.3f")
            upmp = c10.number_input("Upmp (g/g)", min_value=0.0, value=0.20, format="%.3f")
            ds = c11.number_input("Ds (g/cm³)", min_value=0.0, value=1.25, format="%.3f")

        st.markdown("### Overrides opcionais")
        c12, c13 = st.columns(2)
        usar_z = c12.checkbox("Usar Z manual")
        crop = CROPS[cultura_key]
       

        z_override_m = st.number_input("Z manual (m)", min_value=0.0, value=0.35, format="%.3f") if usar_z else None

        salvar_plantio = st.form_submit_button("Salvar plantio")

        if salvar_plantio:
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
            st.success("Plantio cadastrado com sucesso.")
            st.rerun()


with aba2:
    st.subheader("Operação diária")

    plantios_df = list_plantios()

    if plantios_df.empty:
        st.info("Cadastre um plantio primeiro.")
    else:
        opcoes = {
            f"ID {row['id']} - {row['nome']} - {row['cultura_key']} - plantio {row['data_plantio']}": str(row["id"])
            for _, row in plantios_df.iterrows()
        }

        selecionado_label = st.selectbox("Escolha o plantio", list(opcoes.keys()))
        plantio_id = opcoes[selecionado_label]
        plantio = get_plantio(plantio_id)

        crop = CROPS[plantio["cultura_key"]]
        soil = Soil(ucc=plantio["ucc"], upmp=plantio["upmp"], ds=plantio["ds"])
        data_plantio = to_date(plantio["data_plantio"])

        c1, c2 = st.columns(2)
        data_operacao = c1.date_input("Data da operação", value=date.today())
        modo_auto = c2.checkbox("Mostrar recomendação automática (LLI/LBI)", value=True)

        modo_calculo = st.radio(
            "Modo de cálculo",
            ["FAO-56", "Planilha"],
            horizontal=True,
            help="FAO-56 usa Z fixo da cultura. Planilha usa SR = 100 mm (Z = 0,10 m) nos primeiros 10 dias."
        )
        modo_calculo_key = "fao56" if modo_calculo == "FAO-56" else "planilha"

        ultimo_dia = get_last_saved_day(plantio_id)

        st.markdown("### Estado anterior")
        if ultimo_dia:
            st.success(
                f"Estado anterior carregado. Último dia salvo: "
                f"{datetime.strptime(ultimo_dia['data'], '%Y-%m-%d').strftime('%d/%m/%Y')} | "
                f"DAP: {ultimo_dia['dap']} | Dr: {ultimo_dia['deplecao_mm']:.3f} mm"
            )
        else:
            st.warning("Nenhum estado anterior salvo. Este será o primeiro registro do plantio.")

        if data_operacao < data_plantio:
            st.error("A data da operação não pode ser anterior à data de plantio.")
        else:
            irrigacao_map = get_irrigation_map(plantio_id)

            try:
                weather_data = fetch_weather_open_meteo(
                    latitude=float(plantio["latitude"]),
                    longitude=float(plantio["longitude"]),
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
                    st.error("Não foi possível gerar resultados para o período.")
                else:
                    hoje_previsto = resultados_antes[-1]

                    c3, c4, c5, c6 = st.columns(4)
                    c3.metric("DAP", hoje_previsto.dap)
                    c4.metric("Dr atual (mm)", f"{hoje_previsto.deplecao_mm:.3f}")
                    c5.metric("LLI recomendada (mm)", f"{hoje_previsto.lli_mm:.3f}")
                    c6.metric("LBI recomendada (mm)", f"{hoje_previsto.lbi_mm:.3f}")

                    st.markdown("### Resumo do dia")
                    resumo_df = pd.DataFrame([{
                        "Data": hoje_previsto.data.strftime("%d/%m/%Y"),
                        "Modo": modo_calculo,
                        "DAP": hoje_previsto.dap,
                        "Fase": hoje_previsto.fase,
                        "Kc": hoje_previsto.kc,
                        "Ks": hoje_previsto.ks,
                        "Kl": hoje_previsto.kl,
                        "ETo (mm)": hoje_previsto.eto_mm,
                        "ETc (mm)": hoje_previsto.etc_mm,
                        "P (mm)": hoje_previsto.p_mm,
                        "Dr (mm)": hoje_previsto.deplecao_mm,
                        "LLI (mm)": hoje_previsto.lli_mm,
                        "LBI (mm)": hoje_previsto.lbi_mm,
                    }])
                    st.dataframe(resumo_df, width="stretch")

                    st.markdown("### Registrar decisão do dia")

                    decisao = st.radio(
                        "O que deseja registrar para este dia?",
                        ["Não irrigar", "Irrigar"]
                    )

                    irrigacao_informada = 0.0
                    if decisao == "Irrigar":
                        irrigacao_informada = st.number_input(
                            "Lâmina real aplicada (mm)",
                            min_value=0.0,
                            value=float(hoje_previsto.lli_mm),
                            format="%.3f"
                        )

                    if st.button("Salvar dia"):
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
                            plantio_id=plantio_id,
                            irrigou=(decisao == "Irrigar"),
                            result=dia_final,
                        )

                        st.success("Dia salvo com sucesso.")
                        st.rerun()

                    st.markdown("### Evolução até a data selecionada")
                    df_resultados = results_to_dataframe(resultados_antes)
                    st.dataframe(df_resultados, width="stretch")

            except Exception as e:
                st.error(f"Erro ao processar operação diária: {e}")


with aba3:
    st.subheader("Histórico do plantio")

    plantios_df = list_plantios_com_historico()

    if plantios_df.empty:
        st.info("Ainda não há plantios com histórico salvo.")
    else:
        opcoes_hist = {
            f"ID {row['id']} - {row['nome']} - {row['cultura_key']} - plantio {row['data_plantio']}": str(row["id"])
            for _, row in plantios_df.iterrows()
        }

        selecionado_hist = st.selectbox("Escolha o plantio para ver o histórico", list(opcoes_hist.keys()), key="hist")
        plantio_hist_id = opcoes_hist[selecionado_hist]

        hist_df = load_history_df(plantio_hist_id)

        if hist_df.empty:
            st.warning("Esse plantio ainda não possui dias salvos.")
        else:
            st.dataframe(hist_df, width="stretch")

            csv_bytes = hist_df.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button(
                "Baixar histórico em CSV",
                data=csv_bytes,
                file_name=f"historico_plantio_{plantio_hist_id}.csv",
                mime="text/csv",
            )

            st.markdown("### Ações de exclusão")

            c1, c2 = st.columns(2)

            with c1:
                st.markdown("#### Apagar um dia do histórico")
                datas_hist = hist_df["Data"].tolist()
                data_escolhida = st.selectbox("Escolha a data para apagar", datas_hist)

                if st.button("Apagar dia selecionado"):
                    data_iso = datetime.strptime(data_escolhida, "%d/%m/%Y").strftime("%Y-%m-%d")
                    delete_history_day(plantio_hist_id, data_iso)
                    st.success("Dia apagado com sucesso.")
                    st.rerun()

            with c2:
                st.markdown("#### Apagar tudo")
                confirmar_apagar_hist = st.checkbox("Confirmo apagar todo o histórico deste plantio")
                if st.button("Apagar histórico completo"):
                    if confirmar_apagar_hist:
                        delete_all_history(plantio_hist_id)
                        st.success("Histórico completo apagado.")
                        st.rerun()
                    else:
                        st.warning("Marque a confirmação antes de apagar.")

            st.markdown("### Remover plantio inteiro")
            confirmar_apagar_plantio = st.checkbox("Confirmo apagar o plantio e todo o histórico dele")

            if st.button("Apagar plantio"):
                if confirmar_apagar_plantio:
                    delete_plantio(plantio_hist_id)
                    st.success("Plantio apagado com sucesso.")
                    st.rerun()
                else:
                    st.warning("Marque a confirmação antes de apagar o plantio.")


with aba4:
    st.subheader("Banco de culturas e solos")

    subaba1, subaba2 = st.tabs(["Culturas", "Solos"])

    with subaba1:
        st.markdown("### Banco de culturas")
        culturas_df = crops_to_df()
        st.dataframe(culturas_df, width="stretch")

        csv_culturas = culturas_df.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button(
            "Baixar culturas em CSV",
            data=csv_culturas,
            file_name="banco_culturas.csv",
            mime="text/csv",
        )

    with subaba2:
        st.markdown("### Banco de solos")
        solos_df = load_solos_df()
        st.dataframe(solos_df, width="stretch")

        st.markdown("### Cadastrar novo solo")
        with st.form("form_solo"):
            s1, s2, s3, s4 = st.columns(4)
            solo_nome = s1.text_input("Nome do solo", value="Novo solo")
            solo_ucc = s2.number_input("Ucc", min_value=0.0, value=0.32, format="%.3f")
            solo_upmp = s3.number_input("Upmp", min_value=0.0, value=0.20, format="%.3f")
            solo_ds = s4.number_input("Ds", min_value=0.0, value=1.25, format="%.3f")

            salvar_solo = st.form_submit_button("Salvar solo")

            if salvar_solo:
                try:
                    create_solo(solo_nome, solo_ucc, solo_upmp, solo_ds)
                    st.success("Solo cadastrado com sucesso.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao salvar solo: {e}")

        if not solos_df.empty:
            st.markdown("### Apagar solo")
            opcoes_delete_solo = {
                f"ID {row['id']} - {row['nome']}": str(row["id"])
                for _, row in solos_df.iterrows()
            }
            solo_delete_label = st.selectbox("Escolha o solo para apagar", list(opcoes_delete_solo.keys()))
            solo_delete_id = opcoes_delete_solo[solo_delete_label]

            confirmar_solo = st.checkbox("Confirmo apagar este solo")
            if st.button("Apagar solo"):
                if confirmar_solo:
                    delete_solo(solo_delete_id)
                    st.success("Solo apagado com sucesso.")
                    st.rerun()
                else:
                    st.warning("Marque a confirmação antes de apagar.")


with aba5:
    st.subheader("Explicação dos cálculos agronômicos")

    plantios_df = list_plantios()

    if plantios_df.empty:
        st.info("Cadastre um plantio primeiro para visualizar os cálculos.")
    else:
        opcoes_calc = {
            f"ID {row['id']} - {row['nome']} - {row['cultura_key']} - plantio {row['data_plantio']}": str(row["id"])
            for _, row in plantios_df.iterrows()
        }

        selecionado_calc = st.selectbox(
            "Escolha o plantio para explicar os cálculos",
            list(opcoes_calc.keys()),
            key="calc_plantio"
        )

        plantio_calc_id = opcoes_calc[selecionado_calc]
        plantio = get_plantio(plantio_calc_id)

        crop = CROPS[plantio["cultura_key"]]
        soil = Soil(ucc=plantio["ucc"], upmp=plantio["upmp"], ds=plantio["ds"])
        data_plantio = to_date(plantio["data_plantio"])

        data_calculo = st.date_input(
            "Data de referência para explicar os cálculos",
            value=date.today(),
            key="calc_data"
        )

        modo_calculo_calc = st.radio(
            "Modo de cálculo",
            ["FAO-56", "Planilha"],
            horizontal=True,
            key="calc_modo",
            help="FAO-56 usa Z fixo da cultura. Planilha usa SR = 100 mm (Z = 0,10 m) nos primeiros 10 dias."
        )
        modo_calculo_calc_key = "fao56" if modo_calculo_calc == "FAO-56" else "planilha"

        if data_calculo < data_plantio:
            st.error("A data de referência não pode ser anterior à data de plantio.")
        else:
            try:
                irrigacao_map = get_irrigation_map(plantio_calc_id)

                weather_data = fetch_weather_open_meteo(
                    latitude=float(plantio["latitude"]),
                    longitude=float(plantio["longitude"]),
                    start_date=data_plantio,
                    end_date=data_calculo,
                    timezone=plantio["timezone"],
                )

                resultados = simulate_irrigation(
                    crop=crop,
                    soil=soil,
                    sistema_irrigacao=plantio["sistema_irrigacao"],
                    data_plantio=data_plantio,
                    weather_data=weather_data,
                    z_override_m=plantio["z_override_m"],
                    irrigacao_real_por_dia=irrigacao_map,
                    modo_automatico=True,
                    modo_calculo=modo_calculo_calc_key,
                )

                if not resultados:
                    st.warning("Não há resultados para a data selecionada.")
                else:
                    r = resultados[-1]

                    z_m = compute_effective_z_m(
                        crop=crop,
                        dap=r.dap,
                        modo_calculo=modo_calculo_calc_key,
                        z_override_m=plantio["z_override_m"],
                    )
                    f = crop.fator_f
                    theta_fc = soil.ucc * soil.ds
                    theta_wp = soil.upmp * soil.ds
                    sr_mm = compute_sr_mm(z_m)
                    kl = compute_kl_from_sr_mm(sr_mm)
                    taw = compute_taw_mm_from_z(soil, z_m)
                    raw = compute_raw_mm(taw, f)

                    st.markdown("## 1. Dados usados no cálculo")
                    dados_df = pd.DataFrame([{
                        "Plantio": plantio["nome"],
                        "Cultura": crop.nome,
                        "Sistema de irrigação": plantio["sistema_irrigacao"],
                        "Modo de cálculo": modo_calculo_calc,
                        "Data de plantio": data_plantio.strftime("%d/%m/%Y"),
                        "Data analisada": data_calculo.strftime("%d/%m/%Y"),
                        "DAP": r.dap,
                        "Fase": explain_phase_name(r.fase),
                        "Ucc": soil.ucc,
                        "Upmp": soil.upmp,
                        "Ds": soil.ds,
                        "Z (m)": z_m,
                        "f": f,
                        "ETo (mm)": r.eto_mm,
                        "P (mm)": r.p_mm,
                        "Irrigação real (mm)": r.irrigacao_real_mm,
                    }])
                    st.dataframe(dados_df, width="stretch")

                    st.markdown("## 2. Cálculos do solo")

                    calc_solo_df = pd.DataFrame([
                        {
                            "Variável": "θfc",
                            "Fórmula": "θfc = Ucc × Ds",
                            "Substituição": f"{soil.ucc:.3f} × {soil.ds:.3f}",
                            "Resultado": round(theta_fc, 4),
                            "Explicação": "Estimativa da umidade volumétrica na capacidade de campo."
                        },
                        {
                            "Variável": "θwp",
                            "Fórmula": "θwp = Upmp × Ds",
                            "Substituição": f"{soil.upmp:.3f} × {soil.ds:.3f}",
                            "Resultado": round(theta_wp, 4),
                            "Explicação": "Estimativa da umidade volumétrica no ponto de murcha permanente."
                        },
                        {
                            "Variável": "SR",
                            "Fórmula": "SR = Z × 1000",
                            "Substituição": f"{z_m:.3f} × 1000",
                            "Resultado": round(sr_mm, 3),
                            "Explicação": "Profundidade efetiva do sistema radicular em milímetros."
                        },
                        {
                            "Variável": "Kl",
                            "Fórmula": "Kl = 0,1 × √SR",
                            "Substituição": f"0,1 × √{sr_mm:.3f}",
                            "Resultado": round(kl, 4),
                            "Explicação": "Coeficiente ligado à profundidade radicular conforme a lógica usada na planilha."
                        },
                        {
                            "Variável": "TAW",
                            "Fórmula": "TAW = 1000 × (θfc - θwp) × Z",
                            "Substituição": f"1000 × ({theta_fc:.4f} - {theta_wp:.4f}) × {z_m:.3f}",
                            "Resultado": round(taw, 3),
                            "Explicação": "Água total disponível no solo."
                        },
                        {
                            "Variável": "RAW",
                            "Fórmula": "RAW = TAW × f",
                            "Substituição": f"{taw:.3f} × {f:.3f}",
                            "Resultado": round(raw, 3),
                            "Explicação": "Água facilmente disponível antes de ocorrer estresse hídrico."
                        },
                    ])
                    st.dataframe(calc_solo_df, width="stretch")

                    st.markdown("## 3. Cálculos da cultura")

                    akc = akc_values(crop)
                    limites = stage_limits(crop)

                    calc_cultura_df = pd.DataFrame([
                        {
                            "Item": "DAP",
                            "Fórmula": "DAP = (data atual - data de plantio) + 1",
                            "Resultado": r.dap,
                            "Explicação": "Dias após o plantio."
                        },
                        {
                            "Item": "Fase",
                            "Fórmula": "Definida pelos limites das durações",
                            "Resultado": explain_phase_name(r.fase),
                            "Explicação": (
                                f"Fim inicial={limites['fim_in']}, "
                                f"fim desenvolvimento={limites['fim_cv']}, "
                                f"fim médio={limites['fim_medio']}, "
                                f"fim final={limites['fim_final']}."
                            )
                        },
                        {
                            "Item": "AKc inicial",
                            "Fórmula": "AKc_in = Kc_in",
                            "Resultado": round(akc["akc_in"], 5),
                            "Explicação": "Valor usado na fase inicial."
                        },
                        {
                            "Item": "AKc desenvolvimento",
                            "Fórmula": "AKc_cv = (Kc_m - Kc_cv) / duração_CV",
                            "Resultado": round(akc["akc_cv"], 5),
                            "Explicação": "Inclinação da variação do Kc no desenvolvimento."
                        },
                        {
                            "Item": "AKc médio",
                            "Fórmula": "AKc_m = 0",
                            "Resultado": round(akc["akc_m"], 5),
                            "Explicação": "Na fase média o Kc fica constante."
                        },
                        {
                            "Item": "AKc final",
                            "Fórmula": "AKc_final = (Kc_final - Kc_m) / duração_final",
                            "Resultado": round(akc["akc_final"], 5),
                            "Explicação": "Inclinação da variação do Kc na fase final."
                        },
                        {
                            "Item": "Kc do dia",
                            "Fórmula": "Depende da fase fenológica",
                            "Resultado": round(r.kc, 4),
                            "Explicação": "Coeficiente de cultura calculado para o DAP selecionado."
                        },
                    ])
                    st.dataframe(calc_cultura_df, width="stretch")

                    st.markdown("## 4. Cálculos hídricos do dia")

                    dr_anterior = 0.0
                    if len(resultados) >= 2:
                        dr_anterior = resultados[-2].deplecao_mm

                    calc_hidrico_df = pd.DataFrame([
                        {
                            "Variável": "Ks",
                            "Fórmula": (
                                "Ks = 1, se Dr ≤ RAW; "
                                "senão Ks = (TAW - Dr)/(TAW - RAW)"
                            ),
                            "Resultado": round(r.ks, 4),
                            "Explicação": "Coeficiente de estresse hídrico."
                        },
                        {
                            "Variável": "ETc",
                            "Fórmula": "ETc = ETo × Kc × Ks × Kl",
                            "Substituição": f"{r.eto_mm:.3f} × {r.kc:.4f} × {r.ks:.4f} × {r.kl:.4f}",
                            "Resultado": round(r.etc_mm, 3),
                            "Explicação": "Evapotranspiração da cultura ajustada."
                        },
                        {
                            "Variável": "Dr anterior",
                            "Fórmula": "Valor acumulado do dia anterior",
                            "Resultado": round(dr_anterior, 3),
                            "Explicação": "Depleção existente antes do balanço do dia."
                        },
                        {
                            "Variável": "Balanço hídrico",
                            "Fórmula": "Dr = Dr_anterior - P - I_real + ETc",
                            "Substituição": f"{dr_anterior:.3f} - {r.p_mm:.3f} - {r.irrigacao_real_mm:.3f} + {r.etc_mm:.3f}",
                            "Resultado": round(r.deplecao_mm, 3),
                            "Explicação": "Atualiza a depleção diária de água no solo."
                        },
                        {
                            "Variável": "LLI",
                            "Fórmula": "LLI = Dr, se Dr ≥ RAW; senão 0",
                            "Resultado": round(r.lli_mm, 3),
                            "Explicação": "Lâmina líquida necessária para repor a água no solo."
                        },
                        {
                            "Variável": "LBI",
                            "Fórmula": "LBI = LLI / eficiência",
                            "Substituição": f"{r.lli_mm:.3f} / {IRRIGATION_EFFICIENCY[normalize_name(plantio['sistema_irrigacao'])]:.2f}",
                            "Resultado": round(r.lbi_mm, 3),
                            "Explicação": "Lâmina bruta, considerando a eficiência do sistema."
                        },
                    ])
                    st.dataframe(calc_hidrico_df, width="stretch")

                    st.markdown("## 5. Interpretação do resultado")

                    st.write(f"**Data analisada:** {r.data.strftime('%d/%m/%Y')}")
                    st.write(f"**DAP:** {r.dap}")
                    st.write(f"**Fase da cultura:** {explain_phase_name(r.fase)}")
                    st.write(f"**Kc do dia:** {r.kc:.4f}")
                    st.write(f"**Ks do dia:** {r.ks:.4f}")
                    st.write(f"**Kl do dia:** {r.kl:.4f}")
                    st.write(f"**ETo:** {r.eto_mm:.3f} mm")
                    st.write(f"**ETc:** {r.etc_mm:.3f} mm")
                    st.write(f"**Precipitação:** {r.p_mm:.3f} mm")
                    st.write(f"**Irrigação real registrada:** {r.irrigacao_real_mm:.3f} mm")
                    st.write(f"**Depleção final (Dr):** {r.deplecao_mm:.3f} mm")
                    st.write(f"**RAW:** {r.raw_mm:.3f} mm")
                    st.write(f"**LLI recomendada:** {r.lli_mm:.3f} mm")
                    st.write(f"**LBI recomendada:** {r.lbi_mm:.3f} mm")

                    if r.deplecao_mm >= r.raw_mm:
                        st.warning(
                            "Neste dia, a depleção ficou igual ou acima da RAW. "
                            "Por isso o sistema recomenda irrigação."
                        )
                    else:
                        st.success(
                            "Neste dia, a depleção ficou abaixo da RAW. "
                            "Por isso ainda não há necessidade de irrigação pela regra automática."
                        )

                    st.markdown("## 6. Simulação dos próximos dias")
                    st.caption("Use esta área para testar o comportamento do balanço hídrico sem precisar esperar os próximos dias reais.")

                    simular_futuro = st.checkbox(
                        "Ativar simulação futura",
                        value=False,
                        key=f"simular_futuro_{plantio_calc_id}"
                    )

                    if simular_futuro:
                        c_sim1, c_sim2, c_sim3 = st.columns(3)
                        dias_futuros = int(c_sim1.number_input(
                            "Quantidade de dias futuros",
                            min_value=1,
                            max_value=60,
                            value=7,
                            step=1,
                            key=f"dias_futuros_{plantio_calc_id}"
                        ))
                        eto_futuro = float(c_sim2.number_input(
                            "ETo futuro fixo (mm/dia)",
                            min_value=0.0,
                            value=float(r.eto_mm),
                            step=0.1,
                            format="%.3f",
                            key=f"eto_futuro_{plantio_calc_id}"
                        ))
                        chuva_futura = float(c_sim3.number_input(
                            "Precipitação futura fixa (mm/dia)",
                            min_value=0.0,
                            value=float(r.p_mm),
                            step=0.1,
                            format="%.3f",
                            key=f"chuva_futura_{plantio_calc_id}"
                        ))

                        c_sim4, c_sim5 = st.columns(2)
                        pef_mode_sim = c_sim4.selectbox(
                            "Como mostrar o Pef na tabela simulada",
                            ["igual_p", "percentual"],
                            format_func=lambda x: "Pef = P" if x == "igual_p" else "Pef = percentual da precipitação",
                            key=f"pef_mode_sim_{plantio_calc_id}"
                        )
                        pef_percentual_sim = float(c_sim5.number_input(
                            "Percentual do Pef",
                            min_value=0.0,
                            max_value=1.0,
                            value=1.0,
                            step=0.05,
                            format="%.2f",
                            key=f"pef_percentual_sim_{plantio_calc_id}",
                            disabled=(pef_mode_sim != "percentual")
                        ))

                        previsao_inicio = data_calculo
                        previsao_fim = data_calculo + timedelta(days=dias_futuros - 1)

                        future_weather = fetch_weather_open_meteo(
                            latitude=plantio["latitude"],
                            longitude=plantio["longitude"],
                            start_date=previsao_inicio,
                            end_date=previsao_fim,
                            timezone=plantio["timezone"],
                        )

                        # Junta clima já carregado com previsão futura sem duplicar datas,
                        # preservando primeiro os dados já existentes do período atual.
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
                            modo_calculo=modo_calculo_calc_key,
                        )

                        resultados_futuros = [
                            res for res in resultados_expandido
                            if data_calculo <= res.data <= previsao_fim
                        ]

                        if resultados_futuros:
                            eficiencia_sim = IRRIGATION_EFFICIENCY[normalize_name(plantio["sistema_irrigacao"])]
                            df_sim = build_planilha_prof_df(
                                results=resultados_futuros,
                                soil=soil,
                                crop=crop,
                                eficiencia=eficiencia_sim,
                                pef_mode=pef_mode_sim,
                                pef_percentual=pef_percentual_sim,
                            )

                            st.write(
                                f"Simulação usando a previsão da Open-Meteo de **{data_calculo.strftime('%d/%m/%Y')}** até "
                                f"**{previsao_fim.strftime('%d/%m/%Y')}**."
                            )
                            st.dataframe(df_sim, width="stretch")

                            df_sim_grafico = df_sim.copy()
                            for col in ["DRA (mm)", "DP", "LLI", "LBI", "ETc (mm)", "Pef", "LA f"]:
                                df_sim_grafico[col] = pd.to_numeric(df_sim_grafico[col], errors="coerce")
                            st.line_chart(
                                df_sim_grafico.set_index("Data")[["DP", "DRA (mm)", "LA f"]],
                                height=320,
                            )

                            st.download_button(
                                "Baixar simulação futura em CSV",
                                data=df_sim.to_csv(index=False).encode("utf-8-sig"),
                                file_name=f"simulacao_futura_plantio_{plantio_calc_id}.csv",
                                mime="text/csv",
                                key=f"download_sim_csv_{plantio_calc_id}"
                            )
                        else:
                            st.info("Não foi possível gerar resultados futuros para a configuração atual.")

                    st.markdown("## 7. Fórmulas resumidas")
                    st.code(
                        "\n".join([
                            "θfc = Ucc × Ds",
                            "θwp = Upmp × Ds",
                            "SR = Z × 1000",
                            "Kl = 0,1 × √SR",
                            "TAW = 1000 × (θfc - θwp) × Z",
                            "RAW = TAW × f",
                            "Ks = 1, se Dr ≤ RAW",
                            "Ks = (TAW - Dr) / (TAW - RAW), se Dr > RAW",
                            "ETc = ETo × Kc × Ks × Kl",
                            "Dr = Dr_anterior - P - I_real + ETc",
                            "LLI = Dr, se Dr ≥ RAW; senão 0",
                            "LBI = LLI / eficiência",
                        ]),
                        language="text"
                    )

            except Exception as e:
                st.error(f"Erro ao explicar os cálculos: {e}")
