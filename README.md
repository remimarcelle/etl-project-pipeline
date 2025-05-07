# Café ETL Pipeline with GUI and Docker

This project is a complete ETL (Extract, Transform, Load) pipeline designed for a pop-up café scenario. It features a GUI built with Tkinter, an ETL core orchestrated in Python, and a local MySQL database running in Docker.

---

## Features

- Upload CSV files through a graphical interface
- Automatically cleans and transforms café order data
- Removes duplicates and sensitive information (PII)
- Normalises product and branch data
- Loads clean data into a MySQL Docker container
- Modular structure, with testable logic and CLI compatibility

---

## GUI Demo

(![GUI Screenshot](https://github.com/user-attachments/assets/7ddcb55e-719f-42dc-bd4f-5e1bd96495dc)

---

## Technologies Used

- Python 3.10+
- Tkinter (GUI)
- Docker + Docker Compose
- MySQL (via container)
- Adminer (DB viewer)
- `pymysql`, `dotenv`, `unittest/pytest`

---

## Project Structure
```
etl-project/
├── data/                   # Raw input CSVs
│   └── raw-data.csv
├── output/                 # Output (transformed) CSVs
├── db/
│   ├── .env                # Database credentials
│   ├── docker-compose.yml  # Docker setup for MySQL + Adminer
│   └── db-loadsql.sql      # Table creation script
├── src/
│   ├── app.py              # ETL controller
│   ├── gui.py              # Tkinter GUI
│   ├── extract/            # CSV extraction logic
│   ├── transform/          # Cleaning, PII removal, normalisation
│   └── db/                 # Database loader
├── tests/
│   └── test_gui.py         # Optional GUI test (Tkinter threads may interfere)
```

## How to Run the App

### 1. Start Docker Containers

From the project root, run:

```bash
docker-compose up -d
```

This starts:

- MySQL container (host: `db`, port: `3306`)
- Adminer container (port `8080`)

### 2. Launch the GUI

```bash
python src/gui.py
```

In the interface:
- Click **"Run My Files"**
- Select your CSV(s)
- The ETL process will extract, clean, transform, and (if not in test mode) upload the data to MySQL

## View the Data (via Adminer)

1. Open [http://localhost:8080](http://localhost:8080)
2. Log in with:

| Field     | Value                         |
|-----------|-------------------------------|
| Server    | db                            |
| Username  | root                          |
| Password  | *(from `.env`)*               |
| Database  | cafe |

3. Use Adminer to browse the `transactions`, `products`, `branches`, etc.

## Testing

Basic test suite included (GUI tests are limited by Tkinter threading):

- `test_gui.py` uses `test_mode=True` to simulate ETL calls without GUI updates
- Threaded `insert()`/`config()` actions are skipped or printed during tests

_Note: GUI integration tests may fail in pytest unless run in a fully synchronous context_

## Architecture Overview

```
User (CSV Upload via GUI)
        ↓
Tkinter GUI (gui.py)
        ↓
ETL Orchestrator (app.py)
        ↓
[ Extract ] → [ Transform ] → [ Load ]
        ↓                    ↓
  CSV logic         MySQL (Docker)
                           ↑
                    Adminer (web GUI)
```

## Known Limitations

- Tkinter GUI logging is not thread-safe, requiring test-mode bypass
- GUI does not start or stop Docker (expected to be managed externally)
- Requires Docker + Python 3.10+ to be installed

## Author

Remi Braithwaite  
[GitHub](https://github.com/remimarcelle)  
[LinkedIn](https://linkedin.com/in/remibraithwaite)

## Submission Status

Final version submitted for Generation UK & Ireland — Data Engineering Bootcamp (2025)
