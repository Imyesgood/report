# Daily Market Report

## 설치 (최초 1회)

```bash
pip install flask openpyxl yfinance pandas
```

## 구조

```
report/
├── run.bat          ← 더블클릭으로 실행 (Windows)
├── app.py           ← Flask 서버
├── parser.py        ← 엑셀 파싱 엔진
├── config.json      ← 지수↔컬럼 매핑 설정
├── report.html      ← 레포트 UI
└── data/
    ├── data.json    ← 파싱 결과 (자동 생성)
    └── comments.db  ← 코멘트 DB (자동 생성)
```

## 매일 워크플로우

1. 인포맥스 엑셀 열기 → 새로고침 → 저장 → 닫기
2. `데이터.xlsx`를 이 폴더에 복사 (또는 config.json에서 경로 지정)
3. `run.bat` 더블클릭
4. 브라우저에서 코멘트 작성 → `저장` (Ctrl+S)

## config.json 수정 방법

지수 추가/수정 시 `config.json`의 `indices_left` 또는 `indices_right` 배열 편집.

```json
{
  "name": "표시 이름",
  "sheet": "엑셀 시트명",
  "index": "Row1 인덱스명 (엑셀 첫 행 값)",
  "value_col": "사용할 컬럼명 (Row2 값)",
  "fallback_col": "없을 때 대체 컬럼 (선택)",
  "fmt": "rate | bp | fx | fx4 | index2 | price",
  "unit": "% | bp | $ | (빈값)"
}
```

## fmt 종류

| fmt     | 예시         | 용도          |
|---------|-------------|---------------|
| rate    | 3.250       | 금리, 수익률  |
| bp      | 116.5       | 스프레드(bp)  |
| fx      | 1,466.50    | 환율 2자리    |
| fx4     | 1.1613      | 환율 4자리    |
| index2  | 5,580.06    | 주가지수      |
| price   | 88.31       | 가격          |

## 데이터 경로 변경

`config.json`에서 `excel_path`를 절대경로로 변경 가능:

```json
"excel_path": "C:/Users/나/Desktop/데이터.xlsx"
```

## 코멘트 DB 위치 변경 (Dropbox 백업 등)

```json
"db_path": "C:/Dropbox/report/comments.db"
```
