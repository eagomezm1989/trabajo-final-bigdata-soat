# Reserva SOAT 2025: impacto de la emisión mensual en la reserva de prima no devengada

Trabajo final del curso Big Data, Maestría en Analítica Aplicada, Universidad de La Sabana.

**Integrantes:** Evelyn Andrea Gómez Marín y Carlos Jaramillo
**Profesor:** Rodolfo Meza

## De qué se trata

Analizamos la base de la reserva de prima no devengada (RPND) del SOAT con corte a diciembre de 2025 para responder una pregunta: **¿cómo influye lo que se vende cada mes en la reserva que queda al cierre del año?** La unidad de análisis es la póliza y seguimos la metodología CRISP-DM.

## Los datos

- Base de la reserva SOAT a diciembre de 2025: archivo de texto de 1,65 GB, 3.316.704 registros y 71 columnas (1,90 GB en memoria).
- Base de siniestros SOAT del mismo corte (142.370 registros), para el análisis complementario.
- Antes de trabajar, el número de póliza se reemplazó por un código irreversible y se eliminaron los datos personales.
- **Los datos no están en este repositorio**, ni la clave de anonimización.

## Herramientas

- **Python y Dask:** lectura del archivo completo por bloques, conversión a Parquet y comparación de dos configuraciones de workers y memoria.
- **Prefect:** flujo que convierte, calcula las tablas de resultados y las carga en la nube; se puede repetir cada mes.
- **PostgreSQL en Amazon RDS:** solo guarda tablas de resultados resumidos.
- **Grafana Cloud:** tablero gerencial conectado a la base en AWS.

## Archivos

| Archivo | Qué contiene |
|---|---|
| `01_eda.ipynb` | Conversión a Parquet, comparación de Dask, análisis exploratorio, tablas de resultados y carga a AWS |
| `etl_soat_prefect.py` | Flujo de Prefect: extraer → transformar → cargar en AWS |
| `anonimizar.py` | Anonimización de las bases antes del análisis |
| `.gitignore` | Excluye datos, salidas por póliza y la clave de anonimización |

## Cómo ejecutarlo

1. Crear el entorno virtual e instalar las librerías **desde la terminal**:
   ```
   python -m venv .venv
   .venv\Scripts\Activate.ps1
   pip install "dask[complete]" prefect pyarrow pandas matplotlib sqlalchemy psycopg2-binary ipykernel
   ```
2. Ubicar los archivos anonimizados en la carpeta `datos_anonimizados/`.
3. Ejecutar el notebook `01_eda.ipynb`.
4. Para el flujo de Prefect, en una terminal `prefect server start` y en otra:
   ```
   prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
   $env:RDS_PASSWORD = Read-Host "Contraseña RDS" -MaskInput
   python etl_soat_prefect.py
   ```

## Resultados principales

- Importa más **cuándo** se vende que cuánto: lo vendido en el último trimestre deja el 50 % de la reserva, y diciembre solo, el 22,2 %.
- La compensación entre aseguradoras explica el 15,2 % de la reserva y se concentra en la sucursal virtual.
- Las pólizas que reciben compensación tienen unas diez veces más siniestros por cada mil pólizas que las que pagan.
- Con Dask, la misma memoria (4 GB) repartida en 2 workers fue tres veces más rápida que en 4 workers.
