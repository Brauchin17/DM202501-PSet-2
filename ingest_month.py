import requests
import time
def ingest_month(year: str, month: str, fuente: str = "green"):
    url = "http://localhost:6789/api/pipeline_schedules/1/pipeline_runs/d50c3e81bcf7441ebdab6448c600a3e9"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    data = {
        "pipeline_run": {
            "variables": {
                "anio": year,
                "fuente": fuente,
                "mes": month
            }
        }
    }

    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()


if __name__ == "__main__":
    tipo = "green"  # "green" o "yellow"
    for year in range(2015, 2026):  # 2025 → 2025
        if year == 2025:
            for month in range(1, 9):  # 1 → 8
                month_str = f"{month:02d}"  # formato "01", "02", ...
                print(f"Ingesting data for {year}-{month_str} {tipo}")
                ingest_month(str(year), month_str, tipo)
                time.sleep(120)  # Espera de 1 minutos entre solicitudes
        else:
            for month in range(1, 13):   # 1 → 12
                month_str = f"{month:02d}"  # formato "01", "02", ...
                print(f"Ingesting data for {year}-{month_str} {tipo}")
                ingest_month(str(year), month_str, tipo)
                time.sleep(120)  # Espera de 1 minutos entre solicitudes