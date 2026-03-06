import streamlit as st
import pandas as pd
import sqlite3
import os
import io
from processor import clean_data, run_deduplication, get_duplicates

# --- 💡 세션 기반 메모리 DB 초기화 ---
if 'db_conn' not in st.session_state:
    st.session_state.db_conn = sqlite3.connect(':memory:', check_same_thread=False)

conn = st.session_state.db_conn

st.set_page_config(page_title="데이터 통합 도구", layout="wide")
st.title("📊 ERP 데이터 통합 및 관리 시스템")

# --- 사이드바: 데이터 업로드 ---
with st.sidebar:
    st.header("📂 데이터 로드")
    excel_files = st.file_uploader("1️⃣ ERP 엑셀 파일 (SLSSPN / BILBIV)", type=["xlsx", "xls"], accept_multiple_files=True)
    st.divider()
    uploaded_db = st.file_uploader("2️⃣ 기존 SQLite DB 파일 (.db)", type=["db"])
    
    if uploaded_db:
        with open("temp_uploaded.db", "wb") as f:
            f.write(uploaded_db.getbuffer())
        with sqlite3.connect("temp_uploaded.db") as temp_conn:
            temp_conn.backup(st.session_state.db_conn)
        os.remove("temp_uploaded.db")
        st.success("✅ DB 로드 완료")

# --- 로직: 엑셀 파일 처리 ---
if excel_files:
    for file in excel_files:
        fname = file.name
        str_converters = {}
        exclude_cols = []

        if "SLSSPN" in fname:
            target_table, target_type = "sales_plan_data", "SLSSPN"
            str_converters = {'매출처': str, '품목코드': str}
            exclude_cols = []
        elif "BILBIV" in fname:
            target_table, target_type = "sales_actual_data", "BILBIV"
            str_converters = {'매출처': str, '품목': str, '수금처': str, '납품처': str}
            exclude_cols = ['No']
        else: continue

        df = pd.read_excel(file, converters=str_converters)
        df = clean_data(df, target_type)

        if target_type == "BILBIV" and '매출번호' in df.columns:
            df = df[df['매출번호'].astype(str).str.contains('합계') == False]

        # DB 저장 (스키마 보정 포함)
        try:
            cursor = conn.execute(f"SELECT * FROM {target_table} LIMIT 0")
            existing_columns = [d[0] for d in cursor.description]
            for col in existing_columns:
                if col not in df.columns: df[col] = None
            if existing_columns: df = df[existing_columns]
            df.to_sql(target_table, conn, if_exists="append", index=False)
        except:
            df.to_sql(target_table, conn, if_exists="replace", index=False)

        # 업로드 직후 자동 중복 제거
        run_deduplication(conn, target_table, exclude_cols)
        st.sidebar.write(f"✔️ {fname} 반영됨")

# --- 메인 화면: 탭 구성 ---
st.divider()
tab1, tab2, tab3 = st.tabs(["📅 판매계획", "💰 매출실적", "🔍 중복 데이터 관리"])

def display_table(table_name, tab_obj):
    with tab_obj:
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            if not df.empty:
                st.write(f"현재 데이터: **{len(df)}** 행")
                st.dataframe(df, use_container_width=True)
            else: st.info("데이터가 비어있습니다.")
        except: st.info("테이블이 존재하지 않습니다.")

display_table("sales_plan_data", tab1)
display_table("sales_actual_data", tab2)

# --- 탭 3: 중복 관리 전용 ---
with tab3:
    st.subheader("🕵️ 중복 데이터 분석 및 정리")
    col_a, col_b = st.columns(2)
    
    tables = {"판매계획": "sales_plan_data", "매출실적": "sales_actual_data"}
    excl_map = {"sales_plan_data": [], "sales_actual_data": ['No']}
    
    selected_tab = st.selectbox("분석할 테이블 선택", list(tables.keys()))
    target = tables[selected_tab]
    
    # 중복 데이터 조회
    df_dup = get_duplicates(conn, target, excl_map[target])
    
    if not df_dup.empty:
        st.warning(f"⚠️ 중복이 의심되는 그룹이 **{len(df_dup)}개** 발견되었습니다.")
        st.dataframe(df_dup, use_container_width=True)
        
        if st.button(f"🗑️ {selected_tab} 중복 데이터 즉시 삭제", type="primary"):
            run_deduplication(conn, target, excl_map[target])
            st.success("중복 정리가 완료되었습니다!")
            st.rerun()
    else:
        st.balloons()
        st.success("✅ 현재 중복된 데이터가 없습니다. 깔끔하네요!")

# --- 내보내기 구역 ---
st.divider()
# ... (기존 내보내기 코드와 동일) ...
