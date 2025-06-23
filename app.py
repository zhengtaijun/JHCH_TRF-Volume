import pandas as pd
import streamlit as st
from rapidfuzz import process, fuzz
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import requests

# ────────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────────
PRODUCT_INFO_URL = (
    "https://raw.githubusercontent.com/zhengtaijun/JHCH_TRF-Volume/main/product_info.xlsx"
)

st.set_page_config(page_title="TRF Volume Calculator By Andy Wang", layout="centered")
st.title("📦 Jory Henley CHC TRF Volume Calculator")
st.caption("App author: **Andy Wang**")

# ────────────────────────────────────────────────────────────────
# FILE UPLOAD + COLUMN SETTINGS
# ────────────────────────────────────────────────────────────────
warehouse_file = st.file_uploader(
    "Upload warehouse export file (Excel)", type=["xlsx"]
)

col_product = st.number_input(
    "Column number of *Product Name* (1 = first column)", min_value=1, value=3
)
col_order = st.number_input(
    "Column number of *Order Number*", min_value=1, value=7
)
col_quantity = st.number_input(
    "Column number of *Quantity*", min_value=1, value=8
)

# ────────────────────────────────────────────────────────────────
# LOAD PRODUCT INFO FROM GITHUB
# ────────────────────────────────────────────────────────────────
@st.cache_data
def load_product_info():
    response = requests.get(PRODUCT_INFO_URL)
    response.raise_for_status()

    df = pd.read_excel(BytesIO(response.content))
    st.write("✅ Product-info file loaded. Columns found:", df.columns.tolist())

    if {"Product Name", "CBM"} - set(df.columns):
        raise ValueError("The Excel file must contain 'Product Name' and 'CBM' columns")

    names = df["Product Name"].fillna("").astype(str)
    cbms = pd.to_numeric(df["CBM"], errors="coerce").fillna(0)

    product_dict = dict(zip(names.tolist(), cbms.tolist()))
    return product_dict, names.tolist()


product_dict, product_name_list = load_product_info()

# ────────────────────────────────────────────────────────────────
# MATCHING FUNCTION
# ────────────────────────────────────────────────────────────────
def match_product(name: str):
    """Exact match first, then fuzzy match (partial_ratio ≥ 80)."""
    if name in product_dict:
        return product_dict[name]
    match, score, _ = process.extractOne(
        name, product_name_list, scorer=fuzz.partial_ratio
    )
    return product_dict[match] if score >= 80 else None


# ────────────────────────────────────────────────────────────────
# MAIN PROCESSING FUNCTION
# ────────────────────────────────────────────────────────────────
def process_warehouse_file(file, p_col, q_col):
    df = pd.read_excel(file)
    st.write("📊 Warehouse file loaded. Shape:", df.shape)

    product_names = df.iloc[:, p_col].fillna("").astype(str).tolist()
    quantities = pd.to_numeric(df.iloc[:, q_col], errors="coerce").fillna(0)

    st.write("🧾 Sample product names:", product_names[:5])
    st.write("🔢 Sample quantities:", quantities.head().tolist())

    total = len(product_names)
    volumes: list[float | None] = []

    def worker(start: int, end: int):
        partial = []
        for i in range(start, end):
            name = product_names[i].strip()
            vol = match_product(name) if name else None
            partial.append(vol)
        return partial

    with ThreadPoolExecutor(max_workers=4) as pool:
        step = max(total // 4, 1)
        futures = []
        for i in range(4):
            s = i * step
            e = (i + 1) * step if i < 3 else total
            futures.append(pool.submit(worker, s, e))
        for f in futures:
            volumes.extend(f.result())

    st.write("🧮 Volume matching done. First 10:", volumes[:10])

    df["Volume"] = pd.to_numeric(pd.Series(volumes), errors="coerce").fillna(0)
    df["Total Volume"] = df["Volume"] * quantities
    st.write("✅ Columns ‘Volume’ and ‘Total Volume’ added")

    total_row = pd.DataFrame({"Total Volume": [df["Total Volume"].sum()]})
    df = pd.concat([df, total_row], ignore_index=True)
    return df


# ────────────────────────────────────────────────────────────────
# RUN CALCULATION
# ────────────────────────────────────────────────────────────────
if warehouse_file and st.button("📐 Calculate"):
    with st.spinner("Processing…"):
        try:
            result_df = process_warehouse_file(
                warehouse_file, col_product - 1, col_quantity - 1
            )

            st.success("✅ Calculation complete! Download below:")
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                result_df.to_excel(writer, index=False)
            output.seek(0)

            st.download_button(
                "📥 Download Excel",
                data=output,
                file_name="TRF_Volume_Result.xlsx",
                mime=(
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
                ),
            )
        except Exception as e:
            st.error(f"❌ Error: {e}")

# ────────────────────────────────────────────────────────────────
# FOOTER
# ────────────────────────────────────────────────────────────────
st.caption("© 2025 • App author: **Andy Wang**")
