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

![GUI Screenshot](https://github.com/user-attachments/assets/7ddcb55e-719f-42dc-bd4f-5e1bd96495dc)

![GUI SCREENSHOT 2](https://github.com/user-attachments/assets/710b7f4a-e5dc-4785-9fe0-1c337d0f124d)

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
etl-project-pipeline/
├── data/
│   └── raw-data.csv
├── output/
├── db/
│   ├── .env
│   ├── db-loadsql.sql
│   ├── db-seed.sql
│   ├── dbsetup.txt
│   ├── docker-compose.yml
│   ├── Dockerfile
│   └── db_cafe_alt_solution.py
├── logs/
│   ├── app.log
│   ├── db.inspect.log
│   ├── db.db_cafe_alt_solution.log
│   ├── extract.log
│   ├── load.log
│   ├── logger.log
│   ├── remove_duplicates.log
│   ├── remove_sensitive_data.log
│   ├── test_transform.log
│   └── transform.log
├── monitoring/
│   ├── cloudwatch-config.json
│   └── grafana-dashboard.json
├── redundcode/
├── src/
│   ├── app.py
│   ├── gui.py
│   ├── extract/
│   │   └── extract.py
│   ├── transform/
│   │   ├── transform.py
│   │   ├── remove_duplicates.py
│   │   └── remove_sensitive_data.py
├── tests/
│   ├── test_gui.py
│   └── ...
├── utils/
│   ├── config_loader.py
│   └── logger.py
├── config.json
├── requirements.txt
├── pytest.ini
├── .gitignore
└── README.md

```

## How to Run the App

### 1. Start Docker Containers

From the project root, run:

```bash
docker-compose up -d
```

This starts:

- MySQL container (host: `mysql`, port: `3306`)
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
| Server    | mysql                            |
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
