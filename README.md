# 🔎 Failure Model Dashboard

[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

📊 A one-file **data model + app** that parses Excel test result files, detects **failures**, and provides visual dashboards & exports.

---

## 🚀 Features
- ✅ Parse **structured & unstructured** Excel reports
- ✅ Detect **FAIL/PASS**, ports, LRUs, test names, and context
- ✅ Export summary reports as **CSV** or **PDF**
- ✅ Interactive **Streamlit dashboard** with charts (Altair)
- ✅ **Command Line mode** to batch process files
- ✅ **Watchdog mode** – auto-monitor a folder and process new data

---

## 📦 Installation

Clone the repo and install dependencies:

```bash
git clone https://github.com/<your-username>/Failure-Model-Dashboard.git
cd Failure-Model-Dashboard
pip install -r requirements.txt


Run Dashboard - 
streamlit run Failure_model_dashboard.py

Run CLI -
python Failure_model_dashboard.py --folder "/path/to/data"

Run Watchdog
python Failure_model_dashboard.py --watchdog "/path/to/data"
