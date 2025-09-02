# abfu_model_dashboard.py
# One-file "model + app" to analyze many Excel files (ABFU style) and visualize fails
# -----------------------------------------------------------------------------------
# HOW TO RUN (choose one):
#   1) DASHBOARD (recommended):
#         streamlit run abfu_model_dashboard.py
#
#   2) COMMAND LINE (exports CSVs only):
#         python abfu_model_dashboard.py --folder "C:\path\to\your\data"
#
# LIBRARIES NEEDED (you already have them, but for reference):
#   pip install pandas openpyxl streamlit altair reportlab watchdog

#to run commands
#streamlit run /Users/abhimanysingh/Downloads/Internship/abfu_model_dashboard.py
#/Users/abhimanyusingh/Downloads/Internship/sorted/abfu
#python abfu_model_dashboard.py --watchdog "/Users/abhimanysingh/Downloads/Internship/sorted/abfu"


from __future__ import annotations
import os, re, io, argparse, sys
from dataclasses import dataclass
import time
from typing import List, Dict, Optional, Tuple
import pandas as pd


# --- Streamlit is optional (only used for the dashboard) ---
try:
    import streamlit as st
    import altair as alt
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet
    HAVE_STREAMLIT = True
except ImportError:
    HAVE_STREAMLIT = False

# --- Watchdog is optional (only used for auto folder monitoring) ---
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    HAVE_WATCHDOG = True
except ImportError:
    HAVE_WATCHDOG = False


# ==============
# Helper "Model"
# ==============
@dataclass
class FailureRecord:
    file: str
    sheet: str
    row_index: int
    test_name: Optional[str]
    lru: Optional[str]
    port_a: Optional[str]
    port_b: Optional[str]
    port_pair: Optional[str]
    result: Optional[str]
    context: str


class FailureModel:
    """
    A robust parser that 'learns' structure from either:
      - tidy tables with columns like Result/Port/LRU, OR
      - raw text rows that contain "FAIL", "Port J##", etc.

    Call .fit(folder) once, then use the aggregation methods.
    """

    # Regexes are the "knowledge" this model uses
    RE_FAIL = re.compile(r"\bFAIL\b", re.IGNORECASE)
    RE_PASS = re.compile(r"\bPASS\b", re.IGNORECASE)
    RE_PORT = re.compile(r"\bPort\s*(J\d+)\b", re.IGNORECASE)
    RE_PORT_PAIR = re.compile(r"\bPort\s*(J\d+).{0,30}Port\s*(J\d+)", re.IGNORECASE)
    RE_LRU = re.compile(
        r"\b(?:LRU|UUT|DUT|BOARD|CARD|MODULE|ASSY|UNIT|PART)\s*[:\-]?\s*([A-Za-z0-9._\-]+)",
        re.IGNORECASE,
    )
    # --- [NEW] Regex to find test names ---
    RE_TEST_NAME = re.compile(r"([\w\s]+Test)\b", re.IGNORECASE)


    def __init__(self):
        self.records: List[FailureRecord] = []
        self.read_notes: List[Tuple[str, str, str]] = []  # (file, sheet, note)

    # ---- public API ----
    def fit(self, folder: str):
        """Scan all .xlsx/.xls files in folder and build a list of FailureRecord."""
        folder = os.path.expanduser(folder)
        if not os.path.isdir(folder):
            raise FileNotFoundError(f"Folder not found: {folder}")

        excel_files = [
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.lower().endswith((".xlsx", ".xls"))
        ]
        if not excel_files:
            raise FileNotFoundError(f"No Excel files found in: {folder}")

        self.records.clear()
        self.read_notes.clear()

        for fpath in sorted(excel_files):
            self._process_file(fpath)

        return self

    # ---- aggregations ----
    def to_dataframe(self) -> pd.DataFrame:
        if not self.records:
            return pd.DataFrame(
                columns=[
                    "File", "Sheet", "Row_Index", "Test_Name", "LRU",
                    "Port_A", "Port_B", "Port_Pair", "Result", "Context"
                ]
            )
        return pd.DataFrame([r.__dict__ for r in self.records]).rename(
            columns={
                "file":"File","sheet":"Sheet","row_index":"Row_Index", "test_name": "Test_Name", "lru":"LRU",
                "port_a":"Port_A","port_b":"Port_B","port_pair":"Port_Pair",
                "result":"Result","context":"Context"
            }
        )

    def fails_only(self) -> pd.DataFrame:
        df = self.to_dataframe()
        if df.empty: return df
        return df[df["Result"].str.lower().eq("fail")]

    def passes_only(self) -> pd.DataFrame:
        df = self.to_dataframe()
        if df.empty: return df
        return df[df["Result"].str.lower().eq("pass")]

    def fails_by_lru(self) -> pd.DataFrame:
        df = self.fails_only()
        if df.empty: return pd.DataFrame(columns=["LRU","Fail_Count"])
        t = df["LRU"].fillna("UNKNOWN").value_counts().rename_axis("LRU").reset_index(name="Fail_Count")
        return t

    def fails_by_port(self) -> pd.DataFrame:
        df = self.fails_only()
        if df.empty: return pd.DataFrame(columns=["Port","Fail_Count"])
        ports = pd.concat([df["Port_A"], df["Port_B"]], ignore_index=True).dropna()
        return ports.value_counts().rename_axis("Port").reset_index(name="Fail_Count")

    def fails_by_portpair(self) -> pd.DataFrame:
        df = self.fails_only()
        if df.empty: return pd.DataFrame(columns=["Port_Pair","Fail_Count"])
        t = df["Port_Pair"].dropna().value_counts().rename_axis("Port_Pair").reset_index(name="Fail_Count")
        return t

    def lru_port_matrix(self) -> pd.DataFrame:
        """Pivot table: rows=LRU, cols=Port, values=Fail_Count."""
        df = self.fails_only()
        if df.empty: return pd.DataFrame()
        ports = pd.concat([
            df[["LRU","Port_A"]].rename(columns={"Port_A":"Port"}),
            df[["LRU","Port_B"]].rename(columns={"Port_B":"Port"}),
        ], ignore_index=True).dropna()
        mat = ports.value_counts().rename("Fail_Count").reset_index()
        return mat.pivot_table(index="LRU", columns="Port", values="Fail_Count", fill_value=0)

    def lru_fail_ratio(self) -> pd.DataFrame:
        """If PASS rows exist, compute FAIL/(FAIL+PASS) per LRU; else only FAIL counts."""
        df_all = self.to_dataframe()
        if df_all.empty:
            return pd.DataFrame(columns=["LRU","Fail_Count","Total_Tests","Fail_Ratio"])
        # derive counts
        g = df_all.groupby(["LRU","Result"], dropna=False).size().unstack(fill_value=0)
        g = g.rename(columns=str.upper).reset_index()
        g["LRU"] = g["LRU"].fillna("UNKNOWN")
        g["Total_Tests"] = g.get("FAIL", 0) + g.get("PASS", 0)
        # avoid division by zero
        g["Fail_Ratio"] = g.apply(lambda r: (r.get("FAIL", 0) / r["Total_Tests"]) if r["Total_Tests"] else 0, axis=1)
        # keep tidy cols always present
        for col in ("FAIL","PASS"):
            if col not in g.columns:
                g[col] = 0
        g = g.rename(columns={"FAIL":"Fail_Count","PASS":"Pass_Count"})
        return g[["LRU","Fail_Count","Total_Tests","Fail_Ratio"]].sort_values("Fail_Count", ascending=False)

    def read_summary(self) -> pd.DataFrame:
        """What was read, sheet by sheet, plus notes."""
        if not self.read_notes:
            return pd.DataFrame(columns=["File","Sheet","Note"])
        return pd.DataFrame(self.read_notes, columns=["File","Sheet","Note"])

    # NEW METHODS FOR ADDITIONAL CHARTS
    def fails_by_file(self) -> pd.DataFrame:
        """Failure count grouped by file."""
        df = self.fails_only()
        if df.empty: return pd.DataFrame(columns=["File","Fail_Count"])
        return df["File"].value_counts().rename_axis("File").reset_index(name="Fail_Count")

    def fails_by_sheet(self) -> pd.DataFrame:
        """Failure count grouped by sheet across all files."""
        df = self.fails_only()
        if df.empty: return pd.DataFrame(columns=["Sheet","Fail_Count"])
        return df["Sheet"].value_counts().rename_axis("Sheet").reset_index(name="Fail_Count")

    def port_utilization_stats(self) -> pd.DataFrame:
        """Statistics on port usage across all tests."""
        df = self.to_dataframe()
        if df.empty: return pd.DataFrame()

        ports = pd.concat([df["Port_A"], df["Port_B"]], ignore_index=True).dropna()
        if ports.empty: return pd.DataFrame()

        stats = ports.value_counts().rename_axis("Port").reset_index(name="Total_Usage")

        # Add fail counts
        fail_ports = pd.concat([self.fails_only()["Port_A"], self.fails_only()["Port_B"]], ignore_index=True).dropna()
        fail_counts = fail_ports.value_counts().rename_axis("Port").reset_index(name="Fail_Count")

        stats = stats.merge(fail_counts, on="Port", how="left").fillna(0)
        stats["Pass_Count"] = stats["Total_Usage"] - stats["Fail_Count"]
        stats["Fail_Rate"] = stats["Fail_Count"] / stats["Total_Usage"]

        return stats.sort_values("Total_Usage", ascending=False)

    def test_distribution_by_file(self) -> pd.DataFrame:
        """Distribution of test results by file."""
        df = self.to_dataframe()
        if df.empty: return pd.DataFrame()

        dist = df.groupby(["File", "Result"]).size().unstack(fill_value=0)
        if "FAIL" not in dist.columns: dist["FAIL"] = 0
        if "PASS" not in dist.columns: dist["PASS"] = 0

        dist = dist.reset_index()
        dist["Total"] = dist["FAIL"] + dist["PASS"]
        dist["Fail_Rate"] = dist["FAIL"] / dist["Total"]

        return dist.sort_values("FAIL", ascending=False)

    def most_failing_port_per_file(self) -> pd.DataFrame:
        """Finds the port with the most failures for each file."""
        df = self.fails_only()
        if df.empty:
            return pd.DataFrame(columns=["File", "Port", "Fail_Count"])

        # Unpivot ports to a single column
        ports = pd.concat([
            df[["File", "Port_A"]].rename(columns={"Port_A": "Port"}),
            df[["File", "Port_B"]].rename(columns={"Port_B": "Port"})
        ], ignore_index=True).dropna()

        if ports.empty:
            return pd.DataFrame(columns=["File", "Port", "Fail_Count"])

        # Count failures per file and port
        port_counts = ports.groupby(["File", "Port"]).size().reset_index(name="Fail_Count")

        # Find the port with the maximum count for each file
        most_failing = port_counts.loc[port_counts.groupby("File")["Fail_Count"].idxmax()]

        return most_failing.sort_values("Fail_Count", ascending=False)
    
    def port_stats_per_file(self) -> pd.DataFrame:
        """Calculates pass/fail counts for each port within each file."""
        df = self.to_dataframe()
        if df.empty:
            return pd.DataFrame(columns=["File", "Port", "Pass_Count", "Fail_Count"])

        ports = pd.concat([
            df[["File", "Port_A", "Result"]].rename(columns={"Port_A": "Port"}),
            df[["File", "Port_B", "Result"]].rename(columns={"Port_B": "Port"})
        ], ignore_index=True).dropna(subset=["Port"])

        if ports.empty:
            return pd.DataFrame(columns=["File", "Port", "Pass_Count", "Fail_Count"])

        port_stats = ports.groupby(["File", "Port", "Result"]).size().unstack(fill_value=0)
        if "PASS" not in port_stats.columns:
            port_stats["PASS"] = 0
        if "FAIL" not in port_stats.columns:
            port_stats["FAIL"] = 0
        
        port_stats = port_stats.rename(columns={"PASS": "Pass_Count", "FAIL": "Fail_Count"}).reset_index()
        
        return port_stats


    # ---- file parsing (unchanged) ----
    def _process_file(self, fpath: str):
        try:
            xl = pd.ExcelFile(fpath, engine="openpyxl")
            sheets = xl.sheet_names
        except Exception as e:
            self.read_notes.append((os.path.basename(fpath), "-", f"Unreadable: {e}"))
            return

        for sh in sheets:
            try:
                # try as tidy first
                df = pd.read_excel(fpath, sheet_name=sh, engine="openpyxl")
                if self._looks_structured(df):
                    self._parse_structured(os.path.basename(fpath), sh, df)
                    self.read_notes.append((os.path.basename(fpath), sh, "Parsed as structured table"))
                    continue
            except Exception:
                pass

            # fallback: raw text scan (header=None)
            try:
                raw = pd.read_excel(fpath, sheet_name=sh, engine="openpyxl", header=None, dtype=str)
                self._parse_unstructured(os.path.basename(fpath), sh, raw)
                self.read_notes.append((os.path.basename(fpath), sh, "Parsed by text scan (Context-Aware)"))
            except Exception as e:
                self.read_notes.append((os.path.basename(fpath), sh, f"Sheet read error: {e}"))

    def _looks_structured(self, df: pd.DataFrame) -> bool:
        cols = {c.strip().lower() for c in df.columns.astype(str)}
        needed = {"result"}
        return len(cols & needed) > 0  # has at least "result"

    def _parse_structured(self, file: str, sheet: str, df: pd.DataFrame):
        # Normalize columns
        cols_lower = {c.lower(): c for c in df.columns}
        col_result = cols_lower.get("result")
        col_port = cols_lower.get("port")
        col_port_a = cols_lower.get("port_a") or cols_lower.get("porta")
        col_port_b = cols_lower.get("port_b") or cols_lower.get("portb")
        col_lru = cols_lower.get("lru") or cols_lower.get("uut") or cols_lower.get("dut") or cols_lower.get("board")
        col_test_name = cols_lower.get("test_name") or cols_lower.get("test")


        for i, row in df.iterrows():
            result = str(row[col_result]).strip() if col_result in df.columns else None
            if result is None or (not self.RE_FAIL.search(str(result)) and not self.RE_PASS.search(str(result))):
                # skip rows that are neither PASS nor FAIL
                continue

            # Ports
            port_a, port_b = None, None
            if col_port_a in df.columns: port_a = _clean_port(row[col_port_a])
            if col_port_b in df.columns: port_b = _clean_port(row[col_port_b])
            if (not port_a and not port_b) and col_port in df.columns:
                # try to parse single "Port" col for one or two ports
                pa, pb = self._infer_ports_from_text(str(row[col_port]))
                port_a, port_b = port_a or pa, port_b or pb

            # LRU
            lru = None
            if col_lru in df.columns:
                lru = str(row[col_lru]).strip() if pd.notna(row[col_lru]) else None
            if not lru:
                # try file name as LRU hint
                lru = self._infer_lru_from_text(file) or "UNKNOWN"
            
            # Test Name
            test_name = None
            if col_test_name in df.columns:
                test_name = str(row[col_test_name]).strip() if pd.notna(row[col_test_name]) else None

            # Context
            context = " | ".join(
                str(x) for x in row.fillna("").astype(str).tolist() if str(x).strip()
            )[:1000]

            self._add_record(file, sheet, i, test_name, lru, port_a, port_b, result, context)

    # MODIFIED an improved method to handle your specific file format
    def _parse_unstructured(self, file: str, sheet: str, raw: pd.DataFrame):
        """
        Scans rows for FAIL/PASS. Remembers the last-seen port from header
        rows to attribute failures correctly.
        """
        last_seen_ports = (None, None)
        last_seen_lru = None
        last_seen_test_name = None

        for i, row in raw.iterrows():
            text = " ".join(str(x) for x in row.tolist() if pd.notna(x))
            if not text.strip():
                continue

            # Always check for context, like a Port or LRU mentioned in a header row
            current_ports = self._infer_ports_from_text(text)
            if current_ports[0] or current_ports[1]:
                last_seen_ports = current_ports

            current_lru = self._infer_lru_from_text(text)
            if current_lru:
                last_seen_lru = current_lru
            
            # --- [NEW] Check for test name ---
            test_name_match = self.RE_TEST_NAME.search(text)
            if test_name_match:
                last_seen_test_name = test_name_match.group(1).strip()

            # Now, check if this is a result row (contains PASS or FAIL)
            is_fail = bool(self.RE_FAIL.search(text))
            is_pass = bool(self.RE_PASS.search(text))

            if is_fail or is_pass:
                result = "FAIL" if is_fail else "PASS"

                # Use ports from this line if found, otherwise use the last ones we saw
                pa, pb = current_ports
                if not pa and not pb:
                    pa, pb = last_seen_ports

                # Use LRU from this line if found, otherwise use last seen one or infer from filename
                lru = current_lru or last_seen_lru or self._infer_lru_from_text(file) or "UNKNOWN"

                self._add_record(file, sheet, i, last_seen_test_name, lru, pa, pb, result, text[:1000])

    def _add_record(self, file, sheet, idx, test_name, lru, pa, pb, result, context):
        port_pair = None
        if pa and pb:
            # canonicalize order so "J1-J2" equals "J2-J1"
            a, b = sorted([pa, pb])
            port_pair = f"{a}-{b}"

        self.records.append(
            FailureRecord(
                file=file,
                sheet=sheet,
                row_index=idx,
                test_name=test_name,
                lru=lru,
                port_a=pa,
                port_b=pb,
                port_pair=port_pair,
                result=result.upper() if result else None,
                context=context,
            )
        )

    # --- inference helpers ---
    def _infer_ports_from_text(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        # Try explicit pair first
        m = self.RE_PORT_PAIR.search(text)
        if m:
            return _clean_port(m.group(1)), _clean_port(m.group(2))
        # Else find any single "Port J##"
        ports = [ _clean_port(p) for p in self.RE_PORT.findall(text) ]
        if len(ports) >= 2: return ports[0], ports[1]
        if len(ports) == 1: return ports[0], None
        return None, None

    def _infer_lru_from_text(self, text: str) -> Optional[str]:
        m = self.RE_LRU.search(text)
        if m:
            return m.group(1).strip().strip("-_:")
        # Try filename-like chunk as fallback (e.g., "LRU123.xlsx" or "XYZ_CARD")
        base = os.path.basename(text)
        stem = os.path.splitext(base)[0]
        # Heuristic: keep alnum/_/- and drop common words
        stem_clean = re.sub(r"[^A-Za-z0-9._\-]", "_", stem)
        if len(stem_clean) >= 3:
            return stem_clean
        return None


# ---------------
# Utility funcs
# ---------------
def _clean_port(x) -> Optional[str]:
    if x is None or (isinstance(x, float) and pd.isna(x)): return None
    s = str(x).strip().upper()
    # normalize "PORT J12" / "J12" / "PORTJ12" -> "J12"
    m = re.search(r"J(\d+)", s)
    return f"J{m.group(1)}" if m else None


# ------------------ Watchdog Mode ------------------
class WatchHandler(FileSystemEventHandler):
    def __init__(self, folder, model_class):
        self.folder = folder
        self.model_class = model_class

    def on_created(self, event):
        if event.src_path.endswith((".xlsx", ".xls")):
            print(f"\n📂 New file detected: {event.src_path}")
            model = self.model_class().fit(self.folder)
            fails = model.fails_only()
            if fails.empty:
                print("✅ No FAILS in new file.")
            else:
                print(f"❌ Detected {len(fails)} FAIL rows in {os.path.basename(event.src_path)}")

def run_watchdog(folder: str, model_class):
    if not HAVE_WATCHDOG:
        print("⚠️ watchdog package not installed. Run: pip install watchdog")
        return
    print(f"👀 Watchdog started, monitoring: {folder}")
    event_handler = WatchHandler(folder, model_class)
    observer = Observer()
    observer.schedule(event_handler, folder, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Watchdog stopped.")
        observer.stop()
    observer.join()

# ============================
# Dashboard (Streamlit) Mode
# ============================
def show_welcome_page():
    st.header("👋 Welcome to the Failure Model Dashboard!")
    st.markdown("""
        This application helps you analyze Excel files containing test results to quickly identify and visualize failures.

        **🚀 How to get started:**

        1.  **Enter the folder path** in the text box on the left sidebar.
        2.  **Click the "Analyze" button.**
        3.  The dashboard will automatically process all `.xlsx` and `.xls` files in that folder and display the results.

        Once the analysis is complete, you can use the filters in the sidebar to drill down into the data.
    """)

def run_app():
    st.set_page_config(page_title="Failure Model", layout="wide")

    


    st.title("🔎 Failure Model")

    # --- FIX: Initialize session state ---
    if 'model' not in st.session_state:
        st.session_state.model = None
        st.session_state.df_all = pd.DataFrame()
        st.session_state.df_fails = pd.DataFrame()

    st.sidebar.header("Data Source")
    DEFAULT_FOLDER = "" # e.g., r"C:\Users\YourUser\Documents\TestData"
    folder = st.sidebar.text_input("Folder with Excel files", value=DEFAULT_FOLDER)

    if st.sidebar.button("Analyze"):
        try:
            with st.spinner(f"Analyzing files in {folder}..."):
                model = FailureModel().fit(folder)
                # --- FIX: Store results in session state ---
                st.session_state.model = model
                st.session_state.df_all = model.to_dataframe()
                st.session_state.df_fails = model.fails_only()
        except Exception as e:
            st.error(f"❌ {e}")
            # Clear state on error
            st.session_state.model = None
            st.session_state.df_all = pd.DataFrame()
            st.session_state.df_fails = pd.DataFrame()


    # --- FIX: Check if data exists in session state before proceeding ---
    if st.session_state.model is None:
        show_welcome_page()
        return

    # Use data from session state
    model = st.session_state.model
    df_all = st.session_state.df_all
    df_fails = st.session_state.df_fails

    # --- INTERACTIVE FILTERS IN SIDEBAR ---
    st.sidebar.header("Display Filters")

    lru_options = ["All"] + sorted(df_fails["LRU"].unique().tolist())
    lru_filter = st.sidebar.selectbox("Filter by Model (LRU)", lru_options)

    port_pair_options = ["All"] + sorted(df_fails["Port_Pair"].dropna().unique().tolist())
    port_pair_filter = st.sidebar.selectbox("Filter by Failure Type (Port Pair)", port_pair_options)

    sheet_options = ["All"] + sorted(df_fails["Sheet"].unique().tolist())
    sheet_filter = st.sidebar.selectbox("Filter by Severity (Sheet)", sheet_options)

    # --- [NEW] Test Name Filter ---
    test_name_options = ["All"] + sorted(df_fails["Test_Name"].dropna().unique().tolist())
    test_name_filter = st.sidebar.selectbox("Filter by Test Name", test_name_options)


    # Apply filters
    filtered_df = df_fails.copy()
    if lru_filter != "All":
        filtered_df = filtered_df[filtered_df["LRU"] == lru_filter]
    if port_pair_filter != "All":
        filtered_df = filtered_df[filtered_df["Port_Pair"] == port_pair_filter]
    if sheet_filter != "All":
        filtered_df = filtered_df[filtered_df["Sheet"] == sheet_filter]
    if test_name_filter != "All":
        filtered_df = filtered_df[filtered_df["Test_Name"] == test_name_filter]


    df_ratio = model.lru_fail_ratio()

    st.subheader("📥 Export Reports")

    st.download_button(
        label="📥 Download Filtered Failures (CSV)",
        data=filtered_df.to_csv(index=False).encode("utf-8"),
        file_name="filtered_failures.csv",
        mime="text/csv",
    )

    def make_pdf(dataframe):
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer)
        styles = getSampleStyleSheet()
        flowables = [Paragraph("ABFU Failure Report", styles["Title"]), Spacer(1, 20)]
        for _, row in dataframe.iterrows():
            text = (f"• <b>File:</b> {row.get('File', 'N/A')} | "
                    f"<b>Test:</b> {row.get('Test_Name', 'N/A')} | "
                    f"<b>LRU:</b> {row.get('LRU', 'N/A')} | "
                    f"<b>Port Pair:</b> {row.get('Port_Pair', 'N/A')} | "
                    f"<b>Result:</b> {row.get('Result', 'N/A')}")
            flowables.append(Paragraph(text, styles["Normal"]))
        doc.build(flowables)
        buffer.seek(0)
        return buffer

    pdf_data = make_pdf(filtered_df)
    st.download_button(
        label="📄 Download Filtered Failures (PDF)",
        data=pdf_data,
        file_name="filtered_failures.pdf",
        mime="application/pdf",
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Files Parsed", len(set(df_all["File"])) if not df_all.empty else 0)
    c2.metric("Sheets Parsed", len(set(zip(df_all["File"], df_all["Sheet"]))) if not df_all.empty else 0)
    c3.metric("Total FAILs", len(df_fails))
    c4.metric("Total Tests", len(df_all))

    st.divider()

    with st.expander("Read Summary (what happened per sheet)"):
        st.dataframe(model.read_summary(), use_container_width=True, height=260)

    with st.expander("View Filtered Failure Rows"):
        st.dataframe(filtered_df, use_container_width=True, height=300)

    filtered_fails_by_lru = filtered_df["LRU"].fillna("UNKNOWN").value_counts().rename_axis("LRU").reset_index(name="Fail_Count")
    filtered_ports = pd.concat([filtered_df["Port_A"], filtered_df["Port_B"]], ignore_index=True).dropna()
    filtered_fails_by_port = filtered_ports.value_counts().rename_axis("Port").reset_index(name="Fail_Count")
    filtered_fails_by_portpair = filtered_df["Port_Pair"].dropna().value_counts().rename_axis("Port_Pair").reset_index(name="Fail_Count")
    filtered_fails_by_file = filtered_df["File"].value_counts().rename_axis("File").reset_index(name="Fail_Count")

    def make_download(df, name):
        if df.empty:
            st.caption(f"({name}: no data)")
            return
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        st.download_button(f"Download {name} (CSV)", buf, file_name=f"{name}.csv", mime="text/csv", key=f"dl_{name}")

    st.markdown("##### Downloads")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        make_download(df_all, "all_rows")
        make_download(filtered_df, "filtered_fails_only")
    with col2:
        make_download(filtered_fails_by_lru, "fails_by_lru")
        make_download(filtered_fails_by_port, "fails_by_port")
    with col3:
        make_download(filtered_fails_by_portpair, "fails_by_portpair")
        make_download(df_ratio, "lru_fail_ratio")
    with col4:
        make_download(filtered_fails_by_file, "fails_by_file")
        make_download(model.port_utilization_stats(), "port_stats")

    st.divider()
    st.subheader("📊 Aggregations & Charts")

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["📈 LRU Analysis", "🔌 Port Analysis", "📁 File Analysis", "📊 Visual Breakdown", "🧪 Test Analysis", "📄 File Port Analysis", "📋 Summary Stats"])

    with tab1:
        if not filtered_fails_by_lru.empty:
            st.markdown("**Fails by LRU (Filtered)**")
            st.dataframe(filtered_fails_by_lru, use_container_width=True, height=260)
            
            chart = (
                alt.Chart(filtered_fails_by_lru.head(20))
                .mark_bar()
                .encode(
                    x=alt.X("LRU:N", sort="-y", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("Fail_Count:Q"),
                    color=alt.Color("Fail_Count:Q", scale=alt.Scale(scheme="viridis")),
                    tooltip=["LRU","Fail_Count"]
                )
                .properties(height=400, title="Top 20 LRUs by Failure Count (Filtered)")
            )
            st.altair_chart(chart, use_container_width=True)

        if not df_ratio.empty:
            st.markdown("**Fail Ratio per LRU (Unfiltered)**")
            st.dataframe(df_ratio, use_container_width=True, height=280)

    with tab2:
        if not filtered_fails_by_port.empty:
            st.markdown("**Fails by Port (Filtered)**")
            
            # --- [NEW] Calculate total counts for ports ---
            port_total_counts = df_all.copy()
            port_total_counts = pd.concat([port_total_counts["Port_A"], port_total_counts["Port_B"]], ignore_index=True).dropna().value_counts().rename_axis("Port").reset_index(name="Total_Count")
            
            port_analysis_df = pd.merge(filtered_fails_by_port, port_total_counts, on="Port", how="left").fillna(0)
            
            st.dataframe(port_analysis_df, use_container_width=True, height=260)

            chart2 = (
                alt.Chart(port_analysis_df.head(30))
                .mark_bar()
                .encode(
                    x=alt.X("Port:N", sort="-y"),
                    y=alt.Y("Fail_Count:Q"),
                    color=alt.Color("Fail_Count:Q", scale=alt.Scale(scheme="viridis")),
                    tooltip=["Port","Fail_Count", "Total_Count"]
                )
                .properties(height=400, title="Port Failure Analysis (Filtered)")
            )
            st.altair_chart(chart2, use_container_width=True)

        if not filtered_fails_by_portpair.empty:
            st.markdown("**Fails by Port Pair (A–B) (Filtered)**")
            
            # --- [NEW] Calculate total counts for port pairs ---
            port_pair_total_counts = df_all['Port_Pair'].dropna().value_counts().rename_axis("Port_Pair").reset_index(name="Total_Count")
            port_pair_analysis_df = pd.merge(filtered_fails_by_portpair, port_pair_total_counts, on="Port_Pair", how="left").fillna(0)
            
            st.dataframe(port_pair_analysis_df, use_container_width=True, height=260)

            chart3 = (
                 alt.Chart(port_pair_analysis_df.head(20))
                 .mark_bar()
                 .encode(
                     x=alt.X("Port_Pair:N", sort="-y", axis=alt.Axis(labelAngle=-45)),
                     y=alt.Y("Fail_Count:Q"),
                     color=alt.Color("Fail_Count:Q", scale=alt.Scale(scheme="viridis")),
                     tooltip=["Port_Pair","Fail_Count", "Total_Count"]
                 )
                 .properties(height=400, title="Port Pair Failure Analysis (Filtered)")
             )
            st.altair_chart(chart3, use_container_width=True)


    with tab3:
        if not filtered_fails_by_file.empty:
            st.markdown("**Failures by File (Filtered)**")
            
            # --- [NEW] Calculate total counts for files ---
            file_total_counts = df_all['File'].value_counts().rename_axis("File").reset_index(name="Total_Count")
            file_analysis_df = pd.merge(filtered_fails_by_file, file_total_counts, on="File", how="left").fillna(0)
            
            st.dataframe(file_analysis_df, use_container_width=True, height=260)
            
            file_chart = (
                alt.Chart(file_analysis_df)
                .mark_bar()
                .encode(
                    x=alt.X("File:N", sort="-y", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("Fail_Count:Q"),
                    color=alt.Color("Fail_Count:Q", scale=alt.Scale(scheme="viridis")),
                    tooltip=["File", "Fail_Count", "Total_Count"]
                )
                .properties(height=400, title="Failure Distribution by File (Filtered)")
            )
            st.altair_chart(file_chart, use_container_width=True)

    with tab4:
        st.subheader("📊 Failure Breakdown (Filtered)")
        if not filtered_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                pie_chart_df = filtered_df.copy().dropna(subset=['Port_Pair'])
                if not pie_chart_df.empty:
                    pie_chart = alt.Chart(pie_chart_df).mark_arc().encode(
                        theta=alt.Theta("count():Q"),
                        color=alt.Color("Port_Pair:N", title="Failure Type (Port Pair)"),
                        tooltip=["Port_Pair", "count()"]
                    ).properties(title="Pie Chart of Failure Types")
                    st.altair_chart(pie_chart, use_container_width=True)
            with col2:
                heatmap_df = filtered_df.copy().dropna(subset=['LRU', 'Port_Pair'])
                if not heatmap_df.empty:
                    heatmap = alt.Chart(heatmap_df).mark_rect().encode(
                        x=alt.X("LRU:N", title="Model (LRU)"),
                        y=alt.Y("Port_Pair:N", title="Failure Type (Port Pair)"),
                        color=alt.Color("count():Q", title="Count of Failures"),
                        tooltip=["LRU", "Port_Pair", "count()"]
                    ).properties(title="Heatmap of Failures: Model vs. Failure Type")
                    st.altair_chart(heatmap, use_container_width=True)
        else:
            st.info("No data to display for the selected filters.")
    
    with tab5:
        st.header("🧪 Test Failure Analysis (Filtered)")
        if not filtered_df.empty and 'Test_Name' in filtered_df.columns:
            # --- [NEW] Calculate test summary with pass/fail counts ---
            test_summary = df_all.groupby('Test_Name')['Result'].value_counts().unstack(fill_value=0)
            if 'FAIL' in test_summary.columns:
                 test_summary = test_summary.rename(columns={'FAIL': 'Fail_Count'})
            if 'PASS' in test_summary.columns:
                 test_summary = test_summary.rename(columns={'PASS': 'Pass_Count'})
            
            test_summary['Total_Tests'] = test_summary.sum(axis=1)
            test_summary = test_summary.reset_index()

            st.dataframe(test_summary, use_container_width=True)

            # --- [NEW] Stacked bar chart for test results ---
            test_chart_data = test_summary.melt(id_vars='Test_Name', value_vars=['Pass_Count', 'Fail_Count'], var_name='Status', value_name='Count')
            
            test_chart = alt.Chart(test_chart_data).mark_bar().encode(
                x=alt.X('Test_Name:N', sort='-y', axis=alt.Axis(labelAngle=-45)),
                y=alt.Y('Count:Q'),
                color=alt.Color('Status:N', scale=alt.Scale(domain=['Pass_Count', 'Fail_Count'], range=['#2ca02c', '#d62728'])),
                tooltip=['Test_Name', 'Status', 'Count']
            ).properties(height=400, title="Test Results Overview")
            st.altair_chart(test_chart, use_container_width=True)
            
            st.markdown("---")
            st.subheader("Most Failing Port per Test")
            
            # Unpivot ports to a single column
            ports = pd.concat([
                filtered_df[["Test_Name", "Port_A"]].rename(columns={"Port_A": "Port"}),
                filtered_df[["Test_Name", "Port_B"]].rename(columns={"Port_B": "Port"})
            ], ignore_index=True).dropna()

            if not ports.empty:
                # Count failures per test and port
                port_counts = ports.groupby(["Test_Name", "Port"]).size().reset_index(name="Fail_Count")

                # Find the port with the maximum count for each test
                most_failing = port_counts.loc[port_counts.groupby("Test_Name")["Fail_Count"].idxmax()]
                st.dataframe(most_failing, use_container_width=True)

                # --- [NEW] Pie chart for most failing port per test ---
                pie_chart = alt.Chart(most_failing).mark_arc().encode(
                    theta=alt.Theta("Fail_Count:Q"),
                    color=alt.Color("Port:N"),
                    tooltip=["Test_Name", "Port", "Fail_Count"]
                ).properties(title="Distribution of Most Failing Ports per Test")
                st.altair_chart(pie_chart, use_container_width=True)
            else:
                st.info("No port data available for the selected tests.")
        else:
            st.info("No test name data to display.")


    with tab6:
        st.header("📄 Most Failing Port per File (Unfiltered)")
        port_stats = model.port_stats_per_file()
        if not port_stats.empty:
            st.dataframe(port_stats, use_container_width=True)

            # Pie chart of the most failing ports
            pie_chart = alt.Chart(port_stats).mark_arc().encode(
                theta=alt.Theta("Fail_Count:Q"),
                color=alt.Color("Port:N"),
                tooltip=["File", "Port", "Pass_Count", "Fail_Count"]
            ).properties(title="Distribution of Most Failing Ports")
            st.altair_chart(pie_chart, use_container_width=True)
        else:
            st.info("No port failure data to display.")


    with tab7:
        st.header("📋 Summary Statistics (Unfiltered)")
        st.markdown("---")

        # Top 10 Failing LRUs
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Top 10 Failing LRUs")
            top_lrus = model.fails_by_lru().head(10)
            st.dataframe(top_lrus, use_container_width=True)
        with col2:
            lru_chart = alt.Chart(top_lrus).mark_bar().encode(
                x=alt.X("LRU:N", sort="-y"),
                y=alt.Y("Fail_Count:Q"),
                tooltip=["LRU", "Fail_Count"]
            ).properties(title="Top 10 Failing LRUs")
            st.altair_chart(lru_chart, use_container_width=True)
        st.markdown("---")

        # Top 10 Failing Ports
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Top 10 Failing Ports")
            top_ports = model.fails_by_port().head(10)
            st.dataframe(top_ports, use_container_width=True)
        with col2:
            port_chart = alt.Chart(top_ports).mark_bar().encode(
                x=alt.X("Port:N", sort="-y"),
                y=alt.Y("Fail_Count:Q"),
                tooltip=["Port", "Fail_Count"]
            ).properties(title="Top 10 Failing Ports")
            st.altair_chart(port_chart, use_container_width=True)
        st.markdown("---")

        # Top 10 Failing Port Pairs
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Top 10 Failing Port Pairs")
            top_port_pairs = model.fails_by_portpair().head(10)
            st.dataframe(top_port_pairs, use_container_width=True)
        with col2:
            port_pair_chart = alt.Chart(top_port_pairs).mark_bar().encode(
                x=alt.X("Port_Pair:N", sort="-y"),
                y=alt.Y("Fail_Count:Q"),
                tooltip=["Port_Pair", "Fail_Count"]
            ).properties(title="Top 10 Failing Port Pairs")
            st.altair_chart(port_pair_chart, use_container_width=True)


# ========================
# Command Line (CLI) Mode
# ========================
def run_cli(folder: str):
    """
    Runs the model and exports key dataframes to CSV files.
    """
    print("--- Running ABFU Failure Model (CLI Mode) ---")
    print(f"Analyzing folder: {folder}")
    try:
        model = FailureModel().fit(folder)
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return

    print("✅ Analysis complete.")

    # Exporting data
    print("Exporting data to CSV files in current directory...")
    try:
        model.to_dataframe().to_csv("output_all_rows.csv", index=False)
        model.fails_only().to_csv("output_fails_only.csv", index=False)
        model.fails_by_lru().to_csv("output_fails_by_lru.csv", index=False)
        model.fails_by_port().to_csv("output_fails_by_port.csv", index=False)
        model.fails_by_portpair().to_csv("output_fails_by_portpair.csv", index=False)
        model.lru_fail_ratio().to_csv("output_lru_fail_ratio.csv", index=False)
        model.lru_port_matrix().to_csv("output_lru_port_matrix.csv") # index=True is good here
        print("Successfully exported 7 CSV files (e.g., 'output_all_rows.csv').")
    except Exception as e:
        print(f"❌ ERROR during CSV export: {e}")

# ========================
# Main Execution
# ========================
def main():
    """
    Determines whether to run the Streamlit app, CLI version, or Watchdog mode.
    """
    parser = argparse.ArgumentParser(
        description="Analyze ABFU Excel files for failures. "
                    "Run without arguments for Streamlit dashboard, "
                    "or with --folder for CLI mode, "
                    "or with --watchdog for live monitoring."
    )
    parser.add_argument(
        "-f", "--folder",
        type=str,
        help="Path to the folder containing Excel files (for command-line execution)."
    )
    parser.add_argument(
        "-w", "--watchdog",
        type=str,
        help="Run Watchdog mode: monitor folder for new files"
    )
    args = parser.parse_args()

    if args.folder:
        run_cli(args.folder)
    elif args.watchdog:
        if not HAVE_WATCHDOG:
            print("⚠️ Watchdog is not installed. Run: pip install watchdog")
        else:
            run_watchdog(args.watchdog, FailureModel)  # ⚠️ replace FailureModel if your class name differs
    elif HAVE_STREAMLIT:
        run_app()
    else:
        print("⚠️ Streamlit not installed. Install with: pip install streamlit altair")
        print("Or run CLI mode with --folder, or watchdog mode with --watchdog")


if __name__ == "__main__":
    main()