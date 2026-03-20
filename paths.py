"""
paths.py — 경로 설정 파일
"""
import os

# ── 이 파일이 있는 폴더 (report 폴더)
BASE_DIR = os.path.dirname("C:/Users/User/Downloads/report")

# ── 엑셀 파일 경로
EXCEL_PATH = os.path.join(BASE_DIR, "us_data.xlsx")

# ── DB 경로
DB_PATH = os.path.join(BASE_DIR, "comments.db")

# ── data.json 경로
DATA_PATH = os.path.join(BASE_DIR, "data.json")

# ── 포트
PORT = 5000
