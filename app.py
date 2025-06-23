import pandas as pd
import streamlit as st
from rapidfuzz import process, fuzz
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

# 固定读取 GitHub 上的产品信息表（raw 格式）
PRODUCT_INFO_URL = "https://raw.githubusercontent.com/your_user/your_repo/main/product_info.xlsx"

st.set_page_config(page_title="体积计算工具", layout="wide")
st.title("📦 TRF 体积计算工具")

# 用户上传仓库出库表
warehouse_file = st.file_uploader("上传仓库出库表 (Excel 格式)", type=["xlsx"])

# 用户设定列号（1-based）
col1 = st.number_input("产品名称列号（从左往右数）", min_value=1, value=4)
col2 = st.number_input("订单号列号", min_value=1, value=2)
col3 = st.number_input("数量列号", min_value=1, value=5)

# 加载产品信息表
@st.cache_data
def load_product_info():
    df = pd.read_excel(PRODUCT_INFO_URL)
    names = df["Product Name"].fillna("").astype(str).tolist()
    cbms = df["CBM"].tolist()
    return dict(zip(names, cbms)), names

product_dict, product_names = load_product_info()

# 匹配函数
def match_product(name):
    if name in product_dict:
        return product_dict[name]
    match, score, _ = process.extractOne(name, product_names, scorer=fuzz.partial_ratio)
    return product_dict[match] if score >= 80 else None

# 执行体积计算
def process_file(file, p_col, q_col):
    df = pd.read_excel(file)
    names = df.iloc[:, p_col].fillna("").astype(str).tolist()
    quantities = pd.to_numeric(df.iloc[:, q_col], errors="coerce").fillna(0)

    volumes = []
    total = len(names)

    def worker(start, end):
        local = []
        for i in range(start, end):
            vol = match_product(names[i].strip()) if names[i] else None
            local.append(vol)
        return local

    futures = []
    chunk = total // 4
    with ThreadPoolExecutor(max_workers=4) as ex:
        for i in range(4):
            s, e = i * chunk, (i + 1) * chunk if i < 3 else total
            futures.append(ex.submit(worker, s, e))
        for f in futures:
            volumes.extend(f.result())

    df["Volume"] = pd.to_numeric(volumes, errors="coerce").fillna(0)
    df["Total Volume"] = df["Volume"] * quantities
    df = pd.concat([df, pd.DataFrame({"Total Volume": [df["Total Volume"].sum()]})], ignore_index=True)
    return df

# 主操作按钮
if warehouse_file and st.button("开始计算"):
    with st.spinner("正在处理文件..."):
        try:
            df_result = process_file(warehouse_file, col1 - 1, col3 - 1)
            st.success("计算完成！点击下方按钮下载结果")

            # 提供下载按钮
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_result.to_excel(writer, index=False)
            output.seek(0)
            st.download_button("📥 下载 Excel 结果", data=output, file_name="TRF_volumes_result.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as e:
            st.error(f"出错了：{e}")
