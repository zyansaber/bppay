# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import logging
from datetime import datetime
from typing import List, Any, Optional, Dict, Tuple

import pandas as pd
import pyodbc

# =========================
# Config
# =========================
HANA_HOST = os.getenv("HANA_HOST", "10.11.2.25")
HANA_PORT = os.getenv("HANA_PORT", "30241")
HANA_UID  = os.getenv("HANA_UID",  "BAOJIANFENG")
HANA_PWD  = os.getenv("HANA_PWD",  "Xja@2025ABC")

SCHEMA = "SAPHANADB"
SALES_ORG = "3120"

# ✅ 关键：只取你要的 Client
MANDT_FILTER = "800"     # 改这里即可：比如 "800"

PARTNER_FUNCS = ["AG", "RE", "RG", "WE"]

# 已付款口径：收款凭证类型（你当前用DZ）
PAYMENT_DOC_TYPES = ["DZ"]

# 账簿 / 过滤
LEDGER_FILTER = "0L"
EXCLUDE_PARKED = True
EXCLUDE_REVERSAL = True

# 可选：按过账日期过滤
DATE_FROM: Optional[str] = None
DATE_TO: Optional[str] = None

OUTPUT_XLSX = ""

# ✅ 不显示的 BP（你要求）
EXCLUDE_BP_EXACT = {"0000201371"}          # 精确排除
EXCLUDE_BP_PREFIXES = ("00000031",)        # 前缀排除：00000031XX

# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("BP_SO_AR_REPORT_WITH_CHASSIS")

# =========================
# HANA connect
# =========================
def hana_connect() -> pyodbc.Connection:
    conn_str = (
        "DRIVER={HDBODBC};"
        f"SERVERNODE={HANA_HOST}:{HANA_PORT};"
        f"UID={HANA_UID};PWD={HANA_PWD};"
    )
    conn = pyodbc.connect(conn_str, autocommit=True, timeout=60)
    try:
        conn.setdecoding(pyodbc.SQL_CHAR, encoding="latin1")
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-16le")
        conn.setdecoding(pyodbc.SQL_WMETADATA, encoding="utf-16le")
        conn.setencoding(encoding="utf-8")
    except Exception as e:
        log.warning(f"setdecoding/setencoding not applied. Reason: {e}")
    return conn

def qname(table: str) -> str:
    return f"\"{SCHEMA}\".\"{table.upper()}\""

# =========================
# Metadata helpers
# =========================
def get_table_columns(conn: pyodbc.Connection, table: str) -> pd.DataFrame:
    sql = """
    SELECT COLUMN_NAME, DATA_TYPE_NAME
      FROM SYS.TABLE_COLUMNS
     WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
     ORDER BY POSITION
    """
    return pd.read_sql(sql, conn, params=[SCHEMA.upper(), table.upper()])

def pick_first(cols: set[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c.upper() in cols:
            return c.upper()
    return None

def detect_client_col(conn: pyodbc.Connection, table: str) -> Optional[str]:
    cols = set(get_table_columns(conn, table)["COLUMN_NAME"].astype(str).str.upper())
    return pick_first(cols, ["MANDT", "RCLNT", "CLIENT"])

def client_cond_for_table(
    conn: pyodbc.Connection,
    table: str,
    alias: str,
    params: List[Any],
) -> Tuple[str, Optional[str]]:
    if not MANDT_FILTER:
        return "", None
    ccol = detect_client_col(conn, table)
    if not ccol:
        return "", None
    params.append(MANDT_FILTER)
    return f' AND {alias}."{ccol}" = ?', ccol

# =========================
# Detect needed ACDOCA fields
# =========================
def detect_acdoca_needed_fields(conn: pyodbc.Connection) -> Dict[str, Optional[str]]:
    df = get_table_columns(conn, "ACDOCA")
    cols = set(df["COLUMN_NAME"].astype(str).str.upper())

    client_col = pick_first(cols, ["MANDT", "RCLNT", "CLIENT"])
    bp_col      = pick_first(cols, ["KUNNR"])
    blart_col   = pick_first(cols, ["BLART"])
    budat_col   = pick_first(cols, ["BUDAT", "BUDAT_H", "BUDAT_K"])
    ledger_col  = pick_first(cols, ["RLDNR"])
    bstat_col   = pick_first(cols, ["BSTAT"])
    drcr_col    = pick_first(cols, ["DRCRK", "SHKZG"])
    amt_col     = pick_first(cols, ["HSL", "TSL", "KSL", "WSL"])
    reversal_col= pick_first(cols, ["XREVERSED", "XREVERSAL", "STOKZ", "XREVERSING"])
    koart_col   = pick_first(cols, ["KOART"])
    rrcty_col   = pick_first(cols, ["RRCTY"])
    bukrs_col   = pick_first(cols, ["RBUKRS", "BUKRS"])
    gjahr_col   = pick_first(cols, ["GJAHR"])
    belnr_col   = pick_first(cols, ["BELNR"])
    line_col    = pick_first(cols, ["DOCLN", "BUZEI", "BUZEI_ACDOCA"])

    clear_doc_col  = pick_first(cols, ["AUGBL"])
    clear_date_col = pick_first(cols, ["AUGDT"])

    due_col = pick_first(cols, ["NETDT", "FAEDN", "FAEDT", "FADAT", "ZFBDT"])

    if not bp_col or not blart_col or not drcr_col or not amt_col:
        raise RuntimeError(
            f"ACDOCA missing required columns. "
            f"bp={bp_col}, blart={blart_col}, drcr={drcr_col}, amt={amt_col}"
        )

    return dict(
        client_col=client_col,
        bp_col=bp_col,
        blart_col=blart_col,
        budat_col=budat_col,
        ledger_col=ledger_col,
        bstat_col=bstat_col,
        drcr_col=drcr_col,
        amt_col=amt_col,
        reversal_col=reversal_col,
        koart_col=koart_col,
        rrcty_col=rrcty_col,
        bukrs_col=bukrs_col,
        gjahr_col=gjahr_col,
        belnr_col=belnr_col,
        line_col=line_col,
        clear_doc_col=clear_doc_col,
        clear_date_col=clear_date_col,
        due_col=due_col,
    )

def build_date_cond(fields: Dict[str, Optional[str]], alias: str, params: List[Any]) -> str:
    col = fields.get("budat_col")
    if not col:
        return ""
    cond = ""
    if DATE_FROM:
        params.append(DATE_FROM)
        cond += f' AND {alias}."{col}" >= ?'
    if DATE_TO:
        params.append(DATE_TO)
        cond += f' AND {alias}."{col}" <= ?'
    return cond

def build_finance_filters(fields: Dict[str, Optional[str]], alias: str, params: List[Any]) -> str:
    cond = ""
    if fields.get("ledger_col") and LEDGER_FILTER:
        params.append(LEDGER_FILTER)
        cond += f' AND {alias}."{fields["ledger_col"]}" = ?'
    if EXCLUDE_PARKED and fields.get("bstat_col"):
        cond += f" AND TRIM(COALESCE({alias}.\"{fields['bstat_col']}\", '')) = ''"
    if EXCLUDE_REVERSAL and fields.get("reversal_col"):
        cond += f" AND TRIM(COALESCE({alias}.\"{fields['reversal_col']}\", '')) NOT IN ('X','1')"
    # 关键：限制为应收应付子分类账行，避免ACDOCA里同一BP在其他总账行重复导致金额倍增
    if fields.get("koart_col"):
        cond += f" AND {alias}.\"{fields['koart_col']}\" = 'D'"
    # 只取Actual，避免结转/统计记录参与应付金额汇总导致放大
    if fields.get("rrcty_col"):
        cond += f" AND {alias}.\"{fields['rrcty_col']}\" = '0'"
    return cond

def build_open_condition(fields: Dict[str, Optional[str]], alias: str = "a") -> Optional[str]:
    cd = fields.get("clear_doc_col")
    ct = fields.get("clear_date_col")
    parts = []
    if cd:
        parts.append(f"TRIM(COALESCE({alias}.\"{cd}\", '')) = ''")
    if ct:
        parts.append(f"TRIM(COALESCE({alias}.\"{ct}\", '')) = ''")
    if not parts:
        return None
    return "(" + " AND ".join(parts) + ")"

def build_acdoca_dedupe_partition(fields: Dict[str, Optional[str]], alias: str = "a") -> str:
    dedupe_cols = [
        fields.get("bp_col"),
        fields.get("bukrs_col"),
        fields.get("gjahr_col"),
        fields.get("belnr_col"),
        fields.get("line_col"),
        fields.get("drcr_col"),
        fields.get("blart_col"),
        fields.get("amt_col"),
    ]
    dedupe_cols = [c for c in dedupe_cols if c]
    return ", ".join([f'{alias}."{c}"' for c in dedupe_cols])

# =========================
# SQL: SO -> BP
# =========================
def sql_so_bp(conn: pyodbc.Connection) -> Tuple[str, List[Any], Dict[str, Optional[str]]]:
    pf_placeholders = ",".join(["?"] * len(PARTNER_FUNCS))
    params: List[Any] = []
    params.extend(PARTNER_FUNCS)
    params.append(SALES_ORG)

    vp_mandt_cond, vp_client_col = client_cond_for_table(conn, "VBPA", "vp", params)
    vk_mandt_cond, vk_client_col = client_cond_for_table(conn, "VBAK", "vk", params)

    sql = f"""
    WITH so_bp_raw AS (
        SELECT
            TO_NVARCHAR(vp."VBELN") AS "SO",
            MAX(CASE WHEN vp."PARVW"='AG' THEN TO_NVARCHAR(vp."KUNNR") END) AS "BP_AG",
            MAX(CASE WHEN vp."PARVW"='RE' THEN TO_NVARCHAR(vp."KUNNR") END) AS "BP_RE",
            MAX(CASE WHEN vp."PARVW"='RG' THEN TO_NVARCHAR(vp."KUNNR") END) AS "BP_RG",
            MAX(CASE WHEN vp."PARVW"='WE' THEN TO_NVARCHAR(vp."KUNNR") END) AS "BP_WE"
        FROM {qname("VBPA")} vp
        INNER JOIN {qname("VBAK")} vk
            ON vk."VBELN" = vp."VBELN"
        WHERE vp."PARVW" IN ({pf_placeholders})
          AND vk."VKORG" = ?
          {vp_mandt_cond}
          {vk_mandt_cond}
        GROUP BY vp."VBELN"
    )
    SELECT
        "SO",
        COALESCE("BP_AG","BP_RE","BP_RG","BP_WE") AS "BP",
        "BP_AG","BP_RE","BP_RG","BP_WE"
    FROM so_bp_raw
    WHERE COALESCE("BP_AG","BP_RE","BP_RG","BP_WE") IS NOT NULL
      AND COALESCE("BP_AG","BP_RE","BP_RG","BP_WE") <> ''
    """
    meta = {"VBPA_client_col": vp_client_col, "VBAK_client_col": vk_client_col}
    return sql, params, meta

def sql_so_content(conn: pyodbc.Connection) -> Tuple[str, List[Any], Dict[str, Optional[str]]]:
    cols = set(get_table_columns(conn, "VBAK")["COLUMN_NAME"].astype(str).str.upper())
    sel = ['TO_NVARCHAR(vk."VBELN") AS "SO"', 'vk."VKORG" AS "SalesOrg"']
    if "VKBUR" in cols: sel.append('vk."VKBUR" AS "SalesOffice"')
    if "NETWR" in cols: sel.append('vk."NETWR" AS "SO_NETWR"')
    if "WAERK" in cols: sel.append('vk."WAERK" AS "Currency"')
    if "ERDAT" in cols: sel.append('vk."ERDAT" AS "CreatedDate"')
    if "AUART" in cols: sel.append('vk."AUART" AS "OrderType"')

    params: List[Any] = [SALES_ORG]
    vk_mandt_cond, vk_client_col = client_cond_for_table(conn, "VBAK", "vk", params)

    sql = f"""
    SELECT {", ".join(sel)}
    FROM {qname("VBAK")} vk
    WHERE vk."VKORG" = ?
      {vk_mandt_cond}
    """
    meta = {"VBAK_client_col": vk_client_col}
    return sql, params, meta

# =========================
# SQL: SO -> Chassis (SER02 + OBJK)
# =========================
def sql_so_chassis(conn: pyodbc.Connection) -> Tuple[str, List[Any], Dict[str, Optional[str]]]:
    """
    Chassis number 来自：
      SER02.SDAUFNR (销售订单号) + SER02.OBKNR -> OBJK.SERNR
    输出：SO, CHASSIS_NO
    - 若一个SO对应多个SERNR：这里用 MAX() 取一个（与你提供的SQL一致）
      需要拼接多个时再改 STRING_AGG。
    """
    ser_cols = set(get_table_columns(conn, "SER02")["COLUMN_NAME"].astype(str).str.upper())
    obj_cols = set(get_table_columns(conn, "OBJK")["COLUMN_NAME"].astype(str).str.upper())

    # 你们系统确定有 SDAUFNR
    ser_so = pick_first(ser_cols, ["SDAUFNR", "SD_AUFNR", "SDAUFNR"])  # 冗余写法防大小写差异
    ser_obknr = pick_first(ser_cols, ["OBKNR"])
    obj_obknr = pick_first(obj_cols, ["OBKNR"])
    obj_sernr = pick_first(obj_cols, ["SERNR"])

    if not (ser_so and ser_obknr and obj_obknr and obj_sernr):
        raise RuntimeError(
            f"Cannot build chassis mapping. Need SER02.SDAUFNR/OBKNR and OBJK.OBKNR/SERNR. "
            f"Detected: SER02(SDAUFNR={ser_so}, OBKNR={ser_obknr}), OBJK(OBKNR={obj_obknr}, SERNR={obj_sernr})"
        )

    params: List[Any] = []
    s_mandt_cond, s_client_col = client_cond_for_table(conn, "SER02", "s", params)
    o_mandt_cond, o_client_col = client_cond_for_table(conn, "OBJK", "o", params)

    sql = f"""
    SELECT
        TO_NVARCHAR(s."{ser_so}") AS "SO",
        MAX(TO_NVARCHAR(o."{obj_sernr}")) AS "CHASSIS_NO"
    FROM {qname("SER02")} s
    JOIN {qname("OBJK")} o
      ON s."{ser_obknr}" = o."{obj_obknr}"
    WHERE 1=1
      {s_mandt_cond}
      {o_mandt_cond}
      AND TRIM(COALESCE(s."{ser_so}", '')) <> ''
      AND TRIM(COALESCE(o."{obj_sernr}", '')) <> ''
    GROUP BY TO_NVARCHAR(s."{ser_so}")
    """
    meta = {
        "SER02_client_col": s_client_col,
        "OBJK_client_col": o_client_col,
        "SER02_so_col": ser_so,
        "SER02_obknr_col": ser_obknr,
        "OBJK_sernr_col": obj_sernr,
    }
    return sql, params, meta

# =========================
# SQL: BP name (KNA1)
# =========================
def sql_bp_name(conn: pyodbc.Connection) -> Tuple[str, List[Any], Dict[str, Optional[str]]]:
    cols = set(get_table_columns(conn, "KNA1")["COLUMN_NAME"].astype(str).str.upper())

    if "KUNNR" not in cols:
        raise RuntimeError("KNA1 missing KUNNR column, cannot fetch BP name.")

    name1_col = "NAME1" if "NAME1" in cols else None
    name2_col = "NAME2" if "NAME2" in cols else None
    if not name1_col and not name2_col:
        raise RuntimeError("KNA1 has no NAME1/NAME2, cannot fetch BP name.")

    sel = ['TO_NVARCHAR(k."KUNNR") AS "BP"']
    if name1_col and name2_col:
        sel.append(
            'TRIM(COALESCE(k."NAME1", \'\')) || '
            'CASE WHEN TRIM(COALESCE(k."NAME2", \'\')) <> \'\' THEN \' \' || TRIM(k."NAME2") ELSE \'\' END '
            'AS "BP_NAME"'
        )
    elif name1_col:
        sel.append('TRIM(COALESCE(k."NAME1", \'\')) AS "BP_NAME"')
    else:
        sel.append('TRIM(COALESCE(k."NAME2", \'\')) AS "BP_NAME"')

    params: List[Any] = []
    k_mandt_cond, k_client_col = client_cond_for_table(conn, "KNA1", "k", params)

    sql = f"""
    SELECT {", ".join(sel)}
    FROM {qname("KNA1")} k
    WHERE 1=1
      {k_mandt_cond}
    """
    meta = {"KNA1_client_col": k_client_col}
    return sql, params, meta

# =========================
# SQL: BP financial aggregation (BP汇总后贴到SO，后续做“只显示一次”防重复)
# =========================
def sql_bp_financial(conn: pyodbc.Connection, fields: Dict[str, Optional[str]]) -> Tuple[str, List[Any], Dict[str, Optional[str]]]:
    pf_placeholders = ",".join(["?"] * len(PARTNER_FUNCS))
    pay_placeholders = ",".join(["?"] * len(PAYMENT_DOC_TYPES))

    params: List[Any] = []
    params.extend(PARTNER_FUNCS)
    params.append(SALES_ORG)

    vp_mandt_cond, vp_client_col = client_cond_for_table(conn, "VBPA", "vp", params)
    vk_mandt_cond, vk_client_col = client_cond_for_table(conn, "VBAK", "vk", params)

    params.extend(PAYMENT_DOC_TYPES)
    a_mandt_cond, a_client_col = client_cond_for_table(conn, "ACDOCA", "a", params)

    bp = fields["bp_col"]
    blart = fields["blart_col"]
    drcr = fields["drcr_col"]
    amt = fields["amt_col"]

    debit_cond = f'a."{drcr}" IN (\'S\',\'D\')'
    credit_cond = f'a."{drcr}" IN (\'H\',\'C\')'

    open_cond = build_open_condition(fields, alias="a")
    if open_cond:
        open_amt_expr = f"""
        SUM(
            CASE WHEN {open_cond} THEN
                CASE WHEN {credit_cond} THEN -ABS(a."{amt}") ELSE ABS(a."{amt}") END
            ELSE 0 END
        ) AS "OPEN_AMT"
        """
    else:
        open_amt_expr = 'CAST(NULL AS DECIMAL(23,2)) AS "OPEN_AMT"'

    due_col = fields.get("due_col")
    if due_col and open_cond:
        due_expr = f'MIN(CASE WHEN {open_cond} AND {debit_cond} THEN a."{due_col}" END) AS "EARLIEST_DUE"'
    else:
        due_expr = 'CAST(NULL AS DATE) AS "EARLIEST_DUE"'

    date_cond = build_date_cond(fields, "a", params)
    fin_cond = build_finance_filters(fields, "a", params)

    dedupe_partition = build_acdoca_dedupe_partition(fields, alias="a")

    if dedupe_partition:
        acdoca_src = f"""
    acdoca_src AS (
        SELECT *
        FROM (
            SELECT
                a.*,
                ROW_NUMBER() OVER (PARTITION BY {dedupe_partition} ORDER BY a.\"{bp}\") AS __rn
            FROM {qname("ACDOCA")} a
            INNER JOIN bps x
                ON x.\"BP\" = a.\"{bp}\"
            WHERE a.\"{bp}\" IS NOT NULL
              AND a.\"{bp}\" <> ''
              {a_mandt_cond}
              {date_cond}
              {fin_cond}
        ) s
        WHERE s.__rn = 1
    )
        """
    else:
        acdoca_src = f"""
    acdoca_src AS (
        SELECT a.*
        FROM {qname("ACDOCA")} a
        INNER JOIN bps x
            ON x.\"BP\" = a.\"{bp}\"
        WHERE a.\"{bp}\" IS NOT NULL
          AND a.\"{bp}\" <> ''
          {a_mandt_cond}
          {date_cond}
          {fin_cond}
    )
        """

    sql = f"""
    WITH so_bp_raw AS (
        SELECT
            vp."VBELN" AS "SO",
            MAX(CASE WHEN vp."PARVW"='AG' THEN vp."KUNNR" END) AS "BP_AG",
            MAX(CASE WHEN vp."PARVW"='RE' THEN vp."KUNNR" END) AS "BP_RE",
            MAX(CASE WHEN vp."PARVW"='RG' THEN vp."KUNNR" END) AS "BP_RG",
            MAX(CASE WHEN vp."PARVW"='WE' THEN vp."KUNNR" END) AS "BP_WE"
        FROM {qname("VBPA")} vp
        INNER JOIN {qname("VBAK")} vk
            ON vk."VBELN" = vp."VBELN"
        WHERE vp."PARVW" IN ({pf_placeholders})
          AND vk."VKORG" = ?
          {vp_mandt_cond}
          {vk_mandt_cond}
        GROUP BY vp."VBELN"
    ),
    bps AS (
        SELECT DISTINCT COALESCE("BP_AG","BP_RE","BP_RG","BP_WE") AS "BP"
        FROM so_bp_raw
        WHERE COALESCE("BP_AG","BP_RE","BP_RG","BP_WE") IS NOT NULL
          AND COALESCE("BP_AG","BP_RE","BP_RG","BP_WE") <> ''
    ),
    {acdoca_src}
    SELECT
        TO_NVARCHAR(a."{bp}") AS "BP",
        SUM(CASE WHEN {debit_cond} THEN ABS(a."{amt}") ELSE 0 END) AS "INVOICE_AMT",
        SUM(CASE WHEN {credit_cond} AND a."{blart}" IN ({pay_placeholders}) THEN ABS(a."{amt}") ELSE 0 END) AS "PAID_AMT",
        {open_amt_expr},
        {due_expr}
    FROM acdoca_src a
    GROUP BY TO_NVARCHAR(a."{bp}")
    """
    meta = {"VBPA_client_col": vp_client_col, "VBAK_client_col": vk_client_col, "ACDOCA_client_col": a_client_col}
    return sql, params, meta

def sql_bp_financial_detail(conn: pyodbc.Connection, fields: Dict[str, Optional[str]]) -> Tuple[str, List[Any], Dict[str, Optional[str]]]:
    pf_placeholders = ",".join(["?"] * len(PARTNER_FUNCS))

    params: List[Any] = []
    params.extend(PARTNER_FUNCS)
    params.append(SALES_ORG)

    vp_mandt_cond, vp_client_col = client_cond_for_table(conn, "VBPA", "vp", params)
    vk_mandt_cond, vk_client_col = client_cond_for_table(conn, "VBAK", "vk", params)
    a_mandt_cond, a_client_col = client_cond_for_table(conn, "ACDOCA", "a", params)

    bp = fields["bp_col"]
    blart = fields["blart_col"]
    drcr = fields["drcr_col"]
    amt = fields["amt_col"]

    date_cond = build_date_cond(fields, "a", params)
    fin_cond = build_finance_filters(fields, "a", params)
    dedupe_partition = build_acdoca_dedupe_partition(fields, alias="a")
    dedupe_rn = f"ROW_NUMBER() OVER (PARTITION BY {dedupe_partition} ORDER BY a.\"{bp}\")" if dedupe_partition else "1"
    dedupe_cnt = f"COUNT(1) OVER (PARTITION BY {dedupe_partition})" if dedupe_partition else "1"

    detail_fields = [
        ("RBUKRS", fields.get("bukrs_col")),
        ("GJAHR", fields.get("gjahr_col")),
        ("BELNR", fields.get("belnr_col")),
        ("DOC_LINE", fields.get("line_col")),
        ("BUDAT", fields.get("budat_col")),
        ("KOART", fields.get("koart_col")),
        ("RRCTY", fields.get("rrcty_col")),
        ("AUGBL", fields.get("clear_doc_col")),
        ("AUGDT", fields.get("clear_date_col")),
    ]
    detail_select = [
        f'TO_NVARCHAR(a."{bp}") AS "BP"',
        f'a."{blart}" AS "BLART"',
        f'a."{drcr}" AS "DRCR"',
        f'a."{amt}" AS "AMT"',
    ]
    for out_name, col in detail_fields:
        if col:
            detail_select.append(f'a."{col}" AS "{out_name}"')
    detail_select.extend([
        f"{dedupe_cnt} AS \"DUP_CNT\"",
        f"{dedupe_rn} AS \"DUP_RN\"",
        'CASE WHEN ' + dedupe_rn + ' = 1 THEN 1 ELSE 0 END AS "KEPT_IN_SUM"',
    ])

    sql = f"""
    WITH so_bp_raw AS (
        SELECT
            vp."VBELN" AS "SO",
            MAX(CASE WHEN vp."PARVW"='AG' THEN vp."KUNNR" END) AS "BP_AG",
            MAX(CASE WHEN vp."PARVW"='RE' THEN vp."KUNNR" END) AS "BP_RE",
            MAX(CASE WHEN vp."PARVW"='RG' THEN vp."KUNNR" END) AS "BP_RG",
            MAX(CASE WHEN vp."PARVW"='WE' THEN vp."KUNNR" END) AS "BP_WE"
        FROM {qname("VBPA")} vp
        INNER JOIN {qname("VBAK")} vk
            ON vk."VBELN" = vp."VBELN"
        WHERE vp."PARVW" IN ({pf_placeholders})
          AND vk."VKORG" = ?
          {vp_mandt_cond}
          {vk_mandt_cond}
        GROUP BY vp."VBELN"
    ),
    bps AS (
        SELECT DISTINCT COALESCE("BP_AG","BP_RE","BP_RG","BP_WE") AS "BP"
        FROM so_bp_raw
        WHERE COALESCE("BP_AG","BP_RE","BP_RG","BP_WE") IS NOT NULL
          AND COALESCE("BP_AG","BP_RE","BP_RG","BP_WE") <> ''
    )
    SELECT
        {", ".join(detail_select)}
    FROM {qname("ACDOCA")} a
    INNER JOIN bps x
        ON x."BP" = a."{bp}"
    WHERE a."{bp}" IS NOT NULL
      AND a."{bp}" <> ''
      {a_mandt_cond}
      {date_cond}
      {fin_cond}
    ORDER BY "BP", "DUP_CNT" DESC, "DUP_RN", "AMT" DESC
    """
    meta = {"VBPA_client_col": vp_client_col, "VBAK_client_col": vk_client_col, "ACDOCA_client_col": a_client_col}
    return sql, params, meta

# =========================
# Helpers: filter + de-duplicate display
# =========================
def is_excluded_bp(bp: Any) -> bool:
    if bp is None:
        return False
    s = str(bp).strip()
    if not s:
        return False
    if s in EXCLUDE_BP_EXACT:
        return True
    return any(s.startswith(p) for p in EXCLUDE_BP_PREFIXES)

def dedupe_bp_amount_display(df: pd.DataFrame, bp_col: str, amount_cols: List[str]) -> pd.DataFrame:
    """
    关键：同一个BP可能对应多个SO行。为了避免Excel汇总“倍增”，
    我们让金额列只在该BP出现的第一行显示，其余行置空。
    """
    if bp_col not in df.columns:
        return df
    df = df.copy()
    df.sort_values([bp_col, "SO"], inplace=True, kind="mergesort")
    dup = df.duplicated(subset=[bp_col], keep="first")
    for c in amount_cols:
        if c in df.columns:
            df.loc[dup, c] = pd.NA
    return df

def normalize_bp(val: Any) -> Optional[str]:
    if val is None or pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None
    if s.isdigit() and len(s) < 10:
        s = s.zfill(10)
    return s

def collapse_by_key(df: pd.DataFrame, key_col: str, first_cols: List[str], sum_cols: Optional[List[str]] = None) -> pd.DataFrame:
    if df.empty or key_col not in df.columns:
        return df
    df = df.copy()
    agg: Dict[str, Any] = {}
    for c in first_cols:
        if c in df.columns and c != key_col:
            agg[c] = "first"
    for c in (sum_cols or []):
        if c in df.columns:
            agg[c] = "sum"
    return df.groupby(key_col, as_index=False, dropna=False).agg(agg)

def build_dup_diag(df: pd.DataFrame, key_col: str, name: str) -> pd.DataFrame:
    if df.empty or key_col not in df.columns:
        return pd.DataFrame(columns=["SOURCE", "KEY", "CNT"])
    s = df.groupby(key_col, dropna=False).size().reset_index(name="CNT")
    s = s[s["CNT"] > 1].copy()
    if s.empty:
        return pd.DataFrame(columns=["SOURCE", "KEY", "CNT"])
    s.rename(columns={key_col: "KEY"}, inplace=True)
    s.insert(0, "SOURCE", name)
    return s

def ensure_col(df: pd.DataFrame, col: str, default: Any = pd.NA) -> pd.DataFrame:
    if col not in df.columns:
        df[col] = default
    return df

# =========================
# Main
# =========================
def main() -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_XLSX.strip() or f"bp_so_ar_summary_vkorg{SALES_ORG}_mandt{MANDT_FILTER}_{ts}.xlsx"

    log.info("Connecting to SAP HANA...")
    conn = hana_connect()
    log.info("Connected.")

    fields = detect_acdoca_needed_fields(conn)
    log.info(f"ACDOCA fields used: {fields}")

    # SO->BP
    so_bp_sql, so_bp_params, so_bp_meta = sql_so_bp(conn)
    df_so_bp = pd.read_sql(so_bp_sql, conn, params=so_bp_params)
    if df_so_bp.empty:
        raise RuntimeError(f"No SO found for VKORG={SALES_ORG}. Check VBAK.VKORG and MANDT/RCLNT filter.")

    # ✅ 过滤不要的BP（在merge前后都可以，这里先按BP过滤主BP）
    df_so_bp = df_so_bp[~df_so_bp["BP"].apply(is_excluded_bp)].copy()
    df_so_bp["BP_KEY"] = df_so_bp["BP"].apply(normalize_bp)
    df_so_bp["SO_KEY"] = df_so_bp["SO"].astype(str).str.strip()
    if "BP_AG" in df_so_bp.columns: df_so_bp["BP_AG"] = df_so_bp["BP_AG"].apply(normalize_bp)
    if "BP_RE" in df_so_bp.columns: df_so_bp["BP_RE"] = df_so_bp["BP_RE"].apply(normalize_bp)
    if "BP_RG" in df_so_bp.columns: df_so_bp["BP_RG"] = df_so_bp["BP_RG"].apply(normalize_bp)
    if "BP_WE" in df_so_bp.columns: df_so_bp["BP_WE"] = df_so_bp["BP_WE"].apply(normalize_bp)
    df_so_bp = collapse_by_key(df_so_bp, "SO_KEY", ["SO", "BP", "BP_KEY", "BP_AG", "BP_RE", "BP_RG", "BP_WE"])

    # SO content
    so_cnt_sql, so_cnt_params, so_cnt_meta = sql_so_content(conn)
    df_so_cnt = pd.read_sql(so_cnt_sql, conn, params=so_cnt_params)
    if not df_so_cnt.empty and "SO" in df_so_cnt.columns:
        df_so_cnt["SO_KEY"] = df_so_cnt["SO"].astype(str).str.strip()
        df_so_cnt = collapse_by_key(df_so_cnt, "SO_KEY", list(df_so_cnt.columns))
    df_so_cnt = ensure_col(df_so_cnt, "SO_KEY")

    # BP financial
    bp_fin_sql, bp_fin_params, bp_fin_meta = sql_bp_financial(conn, fields)
    df_bp_fin = pd.read_sql(bp_fin_sql, conn, params=bp_fin_params)
    if not df_bp_fin.empty and "BP" in df_bp_fin.columns:
        df_bp_fin["BP_KEY"] = df_bp_fin["BP"].apply(normalize_bp)
        for c in ["INVOICE_AMT", "PAID_AMT", "OPEN_AMT"]:
            if c in df_bp_fin.columns:
                df_bp_fin[c] = pd.to_numeric(df_bp_fin[c], errors="coerce").fillna(0)
        agg_cols = ["INVOICE_AMT", "PAID_AMT", "OPEN_AMT"]
        first_cols = ["BP", "BP_KEY", "EARLIEST_DUE"]
        df_bp_fin = collapse_by_key(df_bp_fin, "BP_KEY", first_cols=first_cols, sum_cols=agg_cols)
    df_bp_fin = ensure_col(df_bp_fin, "BP_KEY")
    df_bp_fin = ensure_col(df_bp_fin, "INVOICE_AMT", 0)
    df_bp_fin = ensure_col(df_bp_fin, "PAID_AMT", 0)
    df_bp_fin = ensure_col(df_bp_fin, "OPEN_AMT", 0)

    # BP financial detail（逐行诊断：有几行显示几行）
    bp_fin_dtl_sql, bp_fin_dtl_params, _ = sql_bp_financial_detail(conn, fields)
    df_bp_fin_dtl = pd.read_sql(bp_fin_dtl_sql, conn, params=bp_fin_dtl_params)

    # ✅ BP name
    bp_name_sql, bp_name_params, bp_name_meta = sql_bp_name(conn)
    df_bp_name = pd.read_sql(bp_name_sql, conn, params=bp_name_params)
    if not df_bp_name.empty and "BP" in df_bp_name.columns:
        df_bp_name["BP_KEY"] = df_bp_name["BP"].apply(normalize_bp)
        df_bp_name = collapse_by_key(df_bp_name, "BP_KEY", ["BP", "BP_KEY", "BP_NAME"])
    df_bp_name = ensure_col(df_bp_name, "BP_KEY")

    # ✅ SO chassis
    so_chs_sql, so_chs_params, so_chs_meta = sql_so_chassis(conn)
    df_so_chs = pd.read_sql(so_chs_sql, conn, params=so_chs_params)
    if not df_so_chs.empty and "SO" in df_so_chs.columns:
        df_so_chs["SO_KEY"] = df_so_chs["SO"].astype(str).str.strip()
        df_so_chs = collapse_by_key(df_so_chs, "SO_KEY", ["SO", "SO_KEY", "CHASSIS_NO"])
    df_so_chs = ensure_col(df_so_chs, "SO_KEY")

    # 合并前重复诊断（快速定位是否来自merge乘法）
    df_merge_diag = pd.concat([
        build_dup_diag(df_so_bp, "SO_KEY", "SO_BP"),
        build_dup_diag(df_so_cnt, "SO_KEY", "SO_CONTENT"),
        build_dup_diag(df_so_chs, "SO_KEY", "SO_CHASSIS"),
        build_dup_diag(df_bp_fin, "BP_KEY", "BP_FIN"),
        build_dup_diag(df_bp_name, "BP_KEY", "BP_NAME"),
    ], ignore_index=True)

    conn.close()

    # Merge
    df = (
        df_so_bp
        .merge(df_so_cnt, on="SO_KEY", how="left", suffixes=("", "_CNT"))
        .merge(df_so_chs, on="SO_KEY", how="left", suffixes=("", "_CHS"))      # ✅ chassis
        .merge(df_bp_fin, on="BP_KEY", how="left", suffixes=("", "_FIN"))      # BP汇总金额
        .merge(df_bp_name, on="BP_KEY", how="left", suffixes=("", "_NAME"))     # BP名称（主BP）
    )

    # 合并后恢复展示主键字段
    if "SO" not in df.columns and "SO_CNT" in df.columns:
        df["SO"] = df["SO_CNT"]
    for c in ["BP", "BP_NAME"]:
        alt = f"{c}_NAME"
        if c not in df.columns and alt in df.columns:
            df[c] = df[alt]
    if "SO" in df.columns and "SO_CHS" in df.columns:
        df["SO"] = df["SO"].fillna(df["SO_CHS"])
    if "BP" in df.columns and "BP_FIN" in df.columns:
        df["BP"] = df["BP"].fillna(df["BP_FIN"])

    # ✅ 付款方RG 加 name（收钱方名称）
    # 用 KNA1 名称表做映射：BP -> BP_NAME
    bp_name_map = df_bp_name.set_index("BP_KEY")["BP_NAME"] if (not df_bp_name.empty and "BP_KEY" in df_bp_name.columns and "BP_NAME" in df_bp_name.columns) else None
    if bp_name_map is not None and "BP_RG" in df.columns:
        df["BP_RG_NAME"] = df["BP_RG"].apply(normalize_bp).map(bp_name_map)
    else:
        df["BP_RG_NAME"] = pd.NA

    # ========== 未付/重付逻辑 ==========
    if "OPEN_AMT" in df.columns and df["OPEN_AMT"].notna().any():
        df["BP未付金额"] = df["OPEN_AMT"].fillna(0).clip(lower=0)
        df["BP重付金额"] = (-df["OPEN_AMT"].fillna(0)).clip(lower=0)
    else:
        diff = df["INVOICE_AMT"].fillna(0) - df["PAID_AMT"].fillna(0)
        df["BP未付金额"] = diff.clip(lower=0)
        df["BP重付金额"] = (-diff).clip(lower=0)

    # ✅ 再次过滤不要的BP（以防merge后有脏数据）
    df = df[~df["BP"].apply(is_excluded_bp)].copy()

    # ✅ 解决“重复倍增”：金额列只在每个BP第一行显示
    bp_amount_cols = ["INVOICE_AMT", "PAID_AMT", "OPEN_AMT", "EARLIEST_DUE", "BP未付金额", "BP重付金额"]
    df = dedupe_bp_amount_display(df, bp_col="BP", amount_cols=bp_amount_cols)

    # Rename to CN
    rename_cn = {
        "BP": "业务伙伴(BP)",
        "BP_NAME": "业务伙伴名称",
        "SO": "销售订单",
        "SalesOrg": "销售组织",
        "SalesOffice": "销售办公室",
        "SO_NETWR": "订单净值",
        "Currency": "订单币种",
        "CreatedDate": "订单创建日期",
        "OrderType": "订单类型",
        "CHASSIS_NO": "底盘号(Chassis Number)",
        "INVOICE_AMT": "BP应付金额(借方合计)",
        "PAID_AMT": "BP已付金额(付款类凭证)",
        "EARLIEST_DUE": "BP最早到期日",
        "BP_AG": "售达方(AG)",
        "BP_RE": "收票方(RE)",
        "BP_RG": "付款方(RG)",
        "BP_RG_NAME": "付款方名称(RG Name)",
        "BP_WE": "送达方(WE)",
    }
    df_out = df.rename(columns=rename_cn)

    # Column order
    col_order = [
        "业务伙伴(BP)",
        "业务伙伴名称",
        "销售订单",
        "底盘号(Chassis Number)",
        "销售组织",
        "销售办公室",
        "订单类型",
        "订单净值",
        "订单币种",
        "订单创建日期",
        "售达方(AG)",
        "收票方(RE)",
        "付款方(RG)",
        "付款方名称(RG Name)",
        "送达方(WE)",
        "BP应付金额(借方合计)",
        "BP已付金额(付款类凭证)",
        "BP未付金额",
        "BP重付金额",
        "BP最早到期日",
    ]
    df_out = df_out[[c for c in col_order if c in df_out.columns]]

    # Fields used / client columns actually used
    used_rows = [
        {"TABLE": "VBPA",  "PURPOSE": "SO->BP 映射", "FIELDS": "VBELN, PARVW, KUNNR", "CLIENT_COL": so_bp_meta.get("VBPA_client_col")},
        {"TABLE": "VBAK",  "PURPOSE": "SO 内容",     "FIELDS": "VBELN, VKORG, (NETWR), (WAERK), (ERDAT), (AUART)", "CLIENT_COL": so_cnt_meta.get("VBAK_client_col")},
        {"TABLE": "SER02", "PURPOSE": "SO->底盘号",  "FIELDS": f"{so_chs_meta.get('SER02_vbeln_col')} / {so_chs_meta.get('SER02_obknr_col')}", "CLIENT_COL": so_chs_meta.get("SER02_client_col")},
        {"TABLE": "OBJK",  "PURPOSE": "SO->底盘号",  "FIELDS": f"{so_chs_meta.get('OBJK_sernr_col')} (Serial/Chassis)", "CLIENT_COL": so_chs_meta.get("OBJK_client_col")},
        {"TABLE": "ACDOCA","PURPOSE": "BP 金额/到期","FIELDS": ", ".join([f"{k}={v}" for k, v in fields.items()]), "CLIENT_COL": bp_fin_meta.get("ACDOCA_client_col")},
        {"TABLE": "ACDOCA","PURPOSE": "已付口径",   "FIELDS": f"BLART IN {PAYMENT_DOC_TYPES} AND 贷方(credit)", "CLIENT_COL": bp_fin_meta.get("ACDOCA_client_col")},
        {"TABLE": "KNA1",  "PURPOSE": "BP 名称",    "FIELDS": "KUNNR, (NAME1), (NAME2)", "CLIENT_COL": bp_name_meta.get("KNA1_client_col")},
        {"NOTE": "去重显示", "PURPOSE": "避免倍增", "FIELDS": "同一BP多SO时，金额列只在BP第一行显示，其余置空，Excel求和不再双倍/三倍", "CLIENT_COL": ""},
        {"NOTE": "BP过滤", "PURPOSE": "不显示特定BP", "FIELDS": f"排除BP={list(EXCLUDE_BP_EXACT)} 以及前缀={list(EXCLUDE_BP_PREFIXES)}", "CLIENT_COL": ""},
    ]
    df_fields = pd.DataFrame(used_rows)

    # Export
    log.info(f"Export -> {out_path}")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        pd.DataFrame([{
            "schema": SCHEMA,
            "vkorg": SALES_ORG,
            "mandt_filter": MANDT_FILTER,
            "partner_funcs": ",".join(PARTNER_FUNCS),
            "payment_doc_types_for_paid": ",".join(PAYMENT_DOC_TYPES),
            "ledger_filter": LEDGER_FILTER,
            "exclude_parked": EXCLUDE_PARKED,
            "exclude_reversal": EXCLUDE_REVERSAL,
            "date_from": DATE_FROM,
            "date_to": DATE_TO,
            "note": (
                "SO视角输出；加入底盘号(SER02+OBJK)与付款方名称(RG Name)。"
                "ACDOCA金额汇总增加KOART='D'限制，避免同一BP在总账扩展行重复导致应付金额放大。"
                "新增BP_FIN_DETAIL明细页，逐行展示参与汇总的ACDOCA数据，并标注DUP_CNT/DUP_RN/KEPT_IN_SUM用于排查重复来源。"
                "新增MERGE_DUP_DIAG页，定位是否由merge键重复造成倍增。"
                "为避免同一BP对应多SO导致Excel求和倍增，金额列只在每个BP第一行显示。"
                "已过滤 BP=0000201371 及 BP以00000031开头。"
            )
        }]).to_excel(writer, index=False, sheet_name="README")

        df_out.to_excel(writer, index=False, sheet_name="BP_SO_REPORT")
        df_fields.to_excel(writer, index=False, sheet_name="FIELDS_USED")
        df_bp_fin_dtl.to_excel(writer, index=False, sheet_name="BP_FIN_DETAIL")
        df_merge_diag.to_excel(writer, index=False, sheet_name="MERGE_DUP_DIAG")

    log.info("DONE")

if __name__ == "__main__":
    main()
