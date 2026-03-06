import streamlit as st
import pandas as pd
import sqlite3
import os
import io
from processor import clean_data

# --- 💡 세션 기반 메모리 DB 초기화 ---
if 'db_conn' not in st.session_state:
    st.session_state.db_conn = sqlite3.connect(':memory:', check_same_thread=False)

conn = st.session_state.db_conn

st.set_page_config(page_title="데이터 통합 도구", layout="wide")
st.title("Excel to DB (판매 데이터 통합 및 SQLite 변환)")

# --- 사이드바 ---
with st.sidebar:
    st.header("📂 데이터 업로드")
    excel_files = st.file_uploader(
        "1️⃣ ERP 엑셀 파일 (SLSSPN / BILBIV)", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True
    )
    st.divider()
    uploaded_db = st.file_uploader("2️⃣ 기존 SQLite DB 파일 (.db)", type=["db"])

# --- 로직 1: 업로드된 DB 파일 처리 ---
if uploaded_db:
    with open("temp_uploaded.db", "wb") as f:
        f.write(uploaded_db.getbuffer())
    with sqlite3.connect("temp_uploaded.db") as temp_conn:
        temp_conn.backup(st.session_state.db_conn)
    os.remove("temp_uploaded.db")
    st.sidebar.success("✅ DB 파일 로드 완료")

# --- 로직 2: 엑셀 파일 처리 ---
if excel_files:
    for file in excel_files:
        fname = file.name
        
        # 파일 유형별 설정
        str_converters = {}
        exclude_cols = []  # 중복 판단에서 제외할 컬럼 리스트

        if "SLSSPN" in fname:
            target_table = "sales_plan_data"
            target_type = "SLSSPN"
            str_converters = {'매출처': str, '품목코드': str}
            # [수정] SLSSPN에서 중복 판단 시 무시할 컬럼 정의
            exclude_cols = [] 
            
        elif "BILBIV" in fname:
            target_table = "sales_actual_data"
            target_type = "BILBIV"
            str_converters = {'매출처': str, '품목': str, '수금처': str, '납품처': str}
            # [수정] BILBIV에서 중복 판단 시 무시할 컬럼 정의
            exclude_cols = ['No'] 
        else:
            continue

        # 엑셀 읽기 및 전처리
        df = pd.read_excel(file, converters=str_converters)
        df = clean_data(df, target_type)

        # 매출리스트 합계 제외 로직
        if target_type == "BILBIV" and '매출번호' in df.columns:
            df = df[df['매출번호'].astype(str).str.contains('합계') == False]

        try:
            # 기존 테이블 구조에 맞춰 컬럼 보정 (스키마 유지)
            cursor = conn.execute(f"SELECT * FROM {target_table} LIMIT 0")
            existing_columns = [description[0] for description in cursor.description]
            
            for col in existing_columns:
                if col not in df.columns:
                    df[col] = None
            
            if existing_columns:
                df = df[existing_columns]
                
            df.to_sql(target_table, conn, if_exists="append", index=False)
        except Exception:
            # 테이블이 없으면 신규 생성
            df.to_sql(target_table, conn, if_exists="replace", index=False)

        # --- [수정된 중복 제거 로직] ---
        # 1. 현재 테이블의 전체 컬럼 중 제외할 컬럼을 뺀 '기준 컬럼' 리스트 생성
        all_cols = pd.read_sql(f"SELECT * FROM {target_table} LIMIT 0", conn).columns.tolist()
        key_columns = [col for col in all_cols if col not in exclude_cols]
        
        # 2. SQL용 컬럼 문자열 생성 (공백이나 특수문자 대비 따옴표 처리)
        safe_key_cols = [f'"{col}"' for col in key_columns]
        group_key_string = ", ".join(safe_key_cols)

        try:
            # 기준 컬럼들이 동일한 행들 중 rowid가 가장 작은(먼저 들어온) 행만 남기고 삭제
            delete_query = f"""
                DELETE FROM {target_table} 
                WHERE rowid NOT IN (
                    SELECT MIN(rowid) 
                    FROM {target_table} 
                    GROUP BY {group_key_string}
                )
            """
            conn.execute(delete_query)
            conn.commit()
            st.success(f"✅ {fname} 반영 완료 (중복 기준: 제외 컬럼 외 {len(key_columns)}개 항목)")
        except sqlite3.OperationalError as e:
            st.error(f"⚠️ {fname} 중복 제거 중 SQL 오류 발생: {e}")

# --- 데이터 확인 (Tab) ---
st.divider()
tab1, tab2 = st.tabs(["판매계획 (Sales Plan)", "매출리스트 (Sales Actual)"])

with tab1:
    try:
        df_p = pd.read_sql("SELECT * FROM sales_plan_data", conn)
        if not df_p.empty:
            st.write(f"현재 데이터: **{len(df_p)}** 행")
            st.dataframe(df_p, use_container_width=True)
        else: st.info("데이터가 비어있습니다.")
    except: st.info("판매계획 테이블이 아직 생성되지 않았습니다.")

with tab2:
    try:
        df_a = pd.read_sql("SELECT * FROM sales_actual_data", conn)
        if not df_a.empty:
            st.write(f"현재 데이터: **{len(df_a)}** 행")
            st.dataframe(df_a, use_container_width=True)
        else: st.info("데이터가 비어있습니다.")
    except: st.info("매출리스트 테이블이 아직 생성되지 않았습니다.")

# --- 내보내기 ---
st.divider()
col1, col2 = st.columns(2)
with col1:
    temp_db_path = "export.db"
    with sqlite3.connect(temp_db_path) as export_conn:
        st.session_state.db_conn.backup(export_conn)
    with open(temp_db_path, "rb") as f:
        st.download_button("💾 SQLite DB 다운로드", f, "integrated_data.db")
    if os.path.exists(temp_db_path): os.remove(temp_db_path)

with col2:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        try: 
            pd.read_sql("SELECT * FROM sales_plan_data", conn).to_excel(writer, sheet_name='sales_plan_data', index=False)
        except: pass
        try: 
            pd.read_sql("SELECT * FROM sales_actual_data", conn).to_excel(writer, sheet_name='sales_actual_data', index=False)
        except: pass
    st.download_button("📊 Excel 통합 파일 다운로드", output.getvalue(), "integrated_data.xlsx")
