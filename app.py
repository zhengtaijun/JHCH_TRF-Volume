import pandas as pd
import streamlit as st
from rapidfuzz import process, fuzz
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import requests

# GitHub 上的产品信息表 raw 链接
PRODUCT_INFO_URL = "https://raw.githubusercontent.com/zhengtaijun/JHCH_TRF-Volume/main/product_info.xlsx"

st.set_page_config(page_title="TRF 体积计算工具", layout="centered")
st.title("📦 TRF 体积计算工具（上传出库表，自动计算体积）")

# 上传出库表（xlsx）
warehouse_file = st.file_uploader("请上传仓库出库表 (Excel 文件)", type=["xlsx"])

# 选择列号（1-based 显示）
col_product = st.number_input("产品名称列号（从左往右数第几列）", min_value=1, value=4)
col_order = st.number_input("订单号列号", min_value=1, value=2)
col_quantity = st.number_input("数量列号", min_value=1, value=5)

# 加载产品信息表（从 GitHub）
@st.cache_data
def load_product_info():
    response = requests.get(PRODUCT_INFO_URL)
    response.raise_for_status()
    df = pd.read_excel(BytesIO(response.content))

    st.write("✅ 成功加载产品信息表，列名如下：", df.columns.tolist())

    # 检查列是否存在
    if "Product Name" not in df.columns or "CBM" not in df.columns:
        raise ValueError("❌ Excel 中必须包含 'Product Name' 和 'CBM' 列")

    # 直接从 df 提取 Series，不使用 .values（避免返回 numpy）
    product_names_series = df["Product Name"].fillna("").astype(str)
    cbm_series = pd.to_numeric(df["CBM"], errors="coerce").fillna(0)

    # 转换为列表并构建字典
    product_names = product_names_series.tolist()
    cbms = cbm_series.tolist()
    return dict(zip(product_names, cbms)), product_names


product_dict, product_name_list = load_product_info()

# 匹配逻辑
def match_product(name):
    if name in product_dict:
        return product_dict[name]
    match, score, _ = process.extractOne(name, product_name_list, scorer=fuzz.partial_ratio)
    return product_dict[match] if score >= 80 else None

# 主处理函数
def process_warehouse_file(file, p_col, q_col):
    df = pd.read_excel(file)
    product_names = df.iloc[:, p_col].fillna("").astype(str).tolist()
    quantities = pd.to_numeric(df.iloc[:, q_col], errors="coerce").fillna(0)

    total = len(product_names)
    results = []

    def worker(start, end):
        partial = []
        for i in range(start, end):
            name = product_names[i].strip()
            vol = match_product(name) if name else None
            partial.append(vol)
        return partial

    with ThreadPoolExecutor(max_workers=4) as pool:
        step = total // 4
        futures = []
        for i in range(4):
            s = i * step
            e = (i + 1) * step if i < 3 else total
            futures.append(pool.submit(worker, s, e))
        for f in futures:
            results.extend(f.result())

    df["Volume"] = pd.to_numeric(results, errors="coerce").fillna(0)
    df["Total Volume"] = df["Volume"] * quantities

    # 总体积行
    total_row = pd.DataFrame({"Total Volume": [df["Total Volume"].sum()]})
    df = pd.concat([df, total_row], ignore_index=True)

    return df

# 开始计算
if warehouse_file and st.button("📐 开始计算体积"):
    with st.spinner("正在计算..."):
        try:
            df_result = process_warehouse_file(warehouse_file, col_product - 1, col_quantity - 1)

            st.success("✅ 计算完成！您可以下载结果文件：")

            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df_result.to_excel(writer, index=False)
            output.seek(0)

            st.download_button(
                "📥 下载 Excel 文件",
                data=output,
                file_name="TRF_Volume_Result.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"❌ 出错了：{e}")
