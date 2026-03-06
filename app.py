import streamlit as st
import pandas as pd
import sqlite3
import os
import io
# 작성한 로직 파일에서 함수 불러오기
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
    excel_files = st.file_uploader(
        "1️⃣ ERP 엑셀 파일 (SLSSPN / BILBIV)", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True
    )
    st.divider()
    uploaded_db = st.file_uploader("2️⃣ 기존 SQLite DB 파일 (.db)", type=["db"])
    
    if uploaded_db:
        with open("temp_uploaded.db", "wb") as f:
            f.write(uploaded_db.getbuffer())
        with sqlite3.connect("temp_uploaded.db") as temp_conn:
            temp_conn.backup(st.session_state.db_conn)
        os.remove("temp_uploaded.db")
        st.success("✅ DB 로드 완료")

# --- 로직 2: 엑셀 파일 처리 ---
if excel_files:
    for file in excel_files:
        fname = file.name
        str_converters = {}
        exclude_cols = []

        if "SLSSPN" in fname:
            target_table, target_type = "sales_plan_data", "SLSSPN"
            str_converters = {'매출처': str, '품목코드': str}
            exclude_cols = [] # 판매계획은 전체 일치 기준 (필요시 추가)
        elif "BILBIV" in fname:
            target_table, target_type = "sales_actual_data", "BILBIV"
            str_converters = {'매출처': str, '품목': str, '수금처': str, '납품처': str}
            exclude_cols = ['No'] # 'No' 컬럼은 달라도 동일 데이터로 간주
        else:
            continue

        df = pd.read_excel(file, converters=str_converters)
        df = clean_data(df, target_type)

        if target_type == "BILBIV" and '매출번호' in df.columns:
            df = df[df['매출번호'].astype(str).str.contains('합계') == False]

        try:
            # 기존 테이블 구조에 맞춰 컬럼 보정
            cursor = conn.execute(f"SELECT * FROM {target_table} LIMIT 0")
            existing_columns = [d[0] for d in cursor.description]
            for col in existing_columns:
                if col not in df.columns: df[col] = None
            if existing_columns: df = df[existing_columns]
            df.to_sql(target_table, conn, if_exists="append", index=False)
        except:
            df.to_sql(target_table, conn, if_exists="replace", index=False)

        # 업로드 시 자동 중복 제거 실행
        run_deduplication(conn, target_table, exclude_cols)
        st.sidebar.write(f"✔️ {fname} 처리 완료")

# --- 메인 화면: 탭 구성 ---
st.divider()
tab1, tab2, tab3 = st.tabs(["📅 판매계획 (Plan)", "💰 매출실적 (Actual)", "🔍 중복 데이터 관리"])

def display_table(table_name, tab_obj):
    with tab_obj:
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            if not df.empty:
                st.write(f"현재 데이터: **{len(df)}** 행")
                st.dataframe(df, use_container_width=True)
            else: st.info("데이터가 비어있습니다.")
        except: st.info("데이터가 없습니다. 파일을 먼저 업로드해주세요.")

display_table("sales_plan_data", tab1)
display_table("sales_actual_data", tab2)

# --- 탭 3: 중복 관리 구역 ---
with tab3:
    st.subheader("🕵️ 중복 데이터 분석 및 정리")
    
    tables_map = {"판매계획": "sales_plan_data", "매출실적": "sales_actual_data"}
    excl_map = {"sales_plan_data": [], "sales_actual_data": ['No']}
    
    selected_tab = st.selectbox("조회할 테이블 선택", list(tables_map.keys()))
    target = tables_map[selected_tab]
    
    # 중복 데이터 조회 (processor 함수 사용)
    df_dup = get_duplicates(conn, target, excl_map[target])
    
    if not df_dup.empty:
        st.warning(f"⚠️ **{selected_tab}** 테이블에 중복 의심 그룹이 **{len(df_dup)}개** 발견되었습니다.")
        st.write("아래 데이터는 제외 컬럼(예: 'No')을 무시하고 동일한 값들입니다.")
        st.dataframe(df_dup, use_container_width=True)
        
        if st.button(f"🗑️ {selected_tab} 중복 데이터 즉시 삭제", type="primary"):
            run_deduplication(conn, target, excl_map[target])
            st.success("✅ 중복 데이터가 성공적으로 정리되었습니다.")
            st.rerun()
    else:
        st.success(f"✅ {selected_tab} 테이블은 중복 없이 깨끗합니다!")

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
        try: pd.read_sql("SELECT * FROM sales_plan_data", conn).to_excel(writer, sheet_name='sales_plan_data', index=False)
        except: pass
        try: pd.read_sql("SELECT * FROM sales_actual_data", conn).to_excel(writer, sheet_name='sales_actual_data', index=False)
        except: pass
    st.download_button("📊 Excel 통합 파일 다운로드", output.getvalue(), "integrated_data.xlsx")
