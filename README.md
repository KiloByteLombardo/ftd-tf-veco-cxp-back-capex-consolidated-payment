# CAPEX Consolidated Payment API

API para el procesamiento y consolidación de pagos CAPEX para Venezuela y Colombia. Automatiza la generación de reportes Excel, integración con BigQuery y Google Cloud Storage.

## Tabla de Contenidos

- [Arquitectura](#arquitectura)
- [Requisitos](#requisitos)
- [Configuración](#configuración)
- [Instalación](#instalación)
- [Endpoints API](#endpoints-api)
- [Flujo de Procesamiento](#flujo-de-procesamiento)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Despliegue](#despliegue)
- [Tasas de Cambio](#tasas-de-cambio)
- [Cierre de Mes](#cierre-de-mes)

---

## Arquitectura

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Frontend      │────▶│   Flask API     │────▶│   BigQuery      │
│   (Analistas)   │     │   (Cloud Run)   │     │   (Datos CAPEX) │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │  Google Cloud   │
                        │    Storage      │
                        │  (Templates/    │
                        │   Logs/Tmp)     │
                        └─────────────────┘
```

### Componentes principales:

| Componente | Descripción |
|------------|-------------|
| **Flask API** | Servicio REST que procesa archivos Excel |
| **BigQuery** | Almacenamiento de datos CAPEX y tasas BCV |
| **GCS** | Almacenamiento de plantillas, logs y archivos temporales |
| **Cloud Run** | Plataforma de despliegue serverless |

---

## Requisitos

- Python 3.11+
- Docker & Docker Compose
- Google Cloud SDK (`gcloud`)
- Cuenta GCP con permisos en BigQuery y GCS

---

## Configuración

### Variables de Entorno

Crear archivo `.env` en la raíz del proyecto:

```env
# ===== Google Cloud =====
GCP_PROJECT_ID=tu-proyecto-gcp
GOOGLE_APPLICATION_CREDENTIALS=credentials.json

# ===== BigQuery - Venezuela =====
BIGQUERY_DATASET=cxp_vzla
BIGQUERY_TABLE=capex_pagos_vzla
BIGQUERY_TABLE_RESPONSABLE=capex_responsable_vzla
BIGQUERY_TABLE_DIFERENCIA=capex_diferencia_vzla

# ===== BigQuery - Colombia =====
BIGQUERY_DATASET_COP=cxp_col
BIGQUERY_TABLE_COP=capex_pagos_col
BIGQUERY_TABLE_RESPONSABLE_COP=capex_responsable_col
BIGQUERY_TABLE_DIFERENCIA_COP=capex_diferencia_col

# ===== Google Cloud Storage =====
GCS_BUCKET_NAME=tu-bucket-capex

# ===== Tasas de Cambio =====
TC_FTD_ENDPOINT=https://tu-endpoint-tasas-ftd/

# ===== Servidor =====
PORT=5000
DEBUG=False
```

### Estructura del Bucket GCS

```
bucket/
├── template/
│   ├── vzla/
│   │   └── consolidado_capex_ve_2025_2026_template.xlsx
│   └── col/
│       └── consolidado_capex_col_template.xlsx
├── tmp/
│   └── Bosqueto_VZLA_20260128_120000.xlsx
└── logs/
    ├── 2026-01-27/
    │   └── Consolidado_VZLA_20260127_180000.xlsx
    └── 2026-01-28/
        └── Consolidado_VZLA_20260128_180000.xlsx
```

---

## Instalación

### Desarrollo Local (con Docker)

```bash
# 1. Clonar repositorio
git clone <repo-url>
cd capex_consolidated_payment

# 2. Crear archivo .env con las variables necesarias
cp .env.example .env
# Editar .env con tus valores

# 3. Colocar credentials.json en la raíz

# 4. Levantar con Docker Compose (desarrollo)
docker-compose -f docker-compose.local.yml up --build

# 5. Verificar que esté corriendo
curl http://localhost:5000/health
```

### Sin Docker (desarrollo)

```bash
# 1. Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\activate   # Windows

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Ejecutar
cd src
python api.py
```

---

## Endpoints API

### Health Check

```http
GET /health
```

Verifica el estado del servicio.

**Respuesta:**
```json
{
  "status": "healthy",
  "timestamp": "2026-01-28T12:00:00"
}
```

---

### Procesar Bosqueto

```http
POST /api/v1/procesar-bosqueto
Content-Type: multipart/form-data
```

Procesa archivos de entrada y genera la hoja BOSQUETO.

**Parámetros:**
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `archivo_consolidado` | File | Excel con datos consolidados |
| `archivo_reporte` | File | Excel con reporte absoluto |
| `pais` | String | `vzla` o `col` |

**Respuesta:**
- Retorna archivo Excel con hoja BOSQUETO
- Sube copia a `GCS/tmp/`

---

### Procesar Detalle

```http
POST /api/v1/procesar-detalle
Content-Type: multipart/form-data
```

Recibe el BOSQUETO modificado, procesa y genera el consolidado final.

**Parámetros:**
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `archivo_bosqueto` | File | Excel con BOSQUETO modificado |
| `pais` | String | `vzla` o `col` |

**Flujo:**
1. Extrae datos del BOSQUETO
2. Estandariza campos para BigQuery
3. Carga datos a BigQuery
4. Extrae DETALLE CORREGIDO desde BigQuery
5. Descarga plantilla de GCS
6. Pega BOSQUETO y DETALLE en plantilla
7. Ejecuta cierre de mes (si aplica)
8. Sube resultado a `GCS/logs/{fecha}/`

**Respuesta:**
```json
{
  "success": true,
  "mensaje": "Procesamiento completado",
  "archivo_gcs": "logs/2026-01-28/Consolidado_VZLA_20260128_180000.xlsx",
  "registros_cargados": 150,
  "cierre_mes": true
}
```

---

### Upload Bosqueto

```http
POST /api/v1/upload-bosqueto
Content-Type: multipart/form-data
```

Sube archivo BOSQUETO directamente a GCS/tmp.

---

### Listar Logs

```http
GET /api/v1/logs
```

Lista archivos de logs agrupados por fecha con links de descarga.

**Respuesta:**
```json
{
  "success": true,
  "logs": {
    "2026-01-28": [
      {
        "nombre": "Consolidado_VZLA_20260128_180000.xlsx",
        "url": "https://storage.googleapis.com/...",
        "tamaño": "2.5 MB",
        "fecha_creacion": "2026-01-28T18:00:00"
      }
    ]
  }
}
```

---

### Test Connection

```http
GET /api/v1/test-connection
```

Prueba conexión a BigQuery y muestra información del proyecto.

---

### Test GCS

```http
GET /api/v1/test-gcs
```

Prueba conexión a Google Cloud Storage.

---

### Bucket Info

```http
GET /api/v1/bucket-info
```

Información detallada del bucket configurado.

---

### Table Info

```http
GET /api/v1/table-info
```

Información de las tablas BigQuery configuradas.

---

### Test Cierre de Mes

```http
GET /api/v1/test-cierre-mes
```

Endpoint de prueba para simular el cierre de mes.

---

## Flujo de Procesamiento

```
┌──────────────────────────────────────────────────────────────────┐
│                    FLUJO COMPLETO DE CAPEX                       │
└──────────────────────────────────────────────────────────────────┘

     Analista                    API                         GCP
        │                         │                           │
        │  1. Sube archivos       │                           │
        │  (Consolidado+Reporte)  │                           │
        ├────────────────────────▶│                           │
        │                         │  2. Limpia tmp/           │
        │                         ├──────────────────────────▶│
        │                         │                           │
        │                         │  3. Genera BOSQUETO       │
        │                         │                           │
        │                         │  4. Sube a tmp/           │
        │                         ├──────────────────────────▶│
        │  5. Descarga BOSQUETO   │                           │
        │◀────────────────────────┤                           │
        │                         │                           │
        │  6. Modifica BOSQUETO   │                           │
        │  (ajustes manuales)     │                           │
        │                         │                           │
        │  7. Sube BOSQUETO       │                           │
        │  modificado             │                           │
        ├────────────────────────▶│                           │
        │                         │  8. Carga a BigQuery      │
        │                         ├──────────────────────────▶│
        │                         │                           │
        │                         │  9. Extrae DETALLE        │
        │                         │◀──────────────────────────┤
        │                         │                           │
        │                         │  10. Descarga plantilla   │
        │                         │◀──────────────────────────┤
        │                         │                           │
        │                         │  11. Pega datos en        │
        │                         │      plantilla            │
        │                         │                           │
        │                         │  12. Cierre de mes        │
        │                         │      (si semana 1)        │
        │                         │                           │
        │                         │  13. Sube a logs/         │
        │                         ├──────────────────────────▶│
        │  14. Recibe respuesta   │                           │
        │◀────────────────────────┤                           │
        │                         │                           │
```

---

## Estructura del Proyecto

```
capex_consolidated_payment/
├── src/
│   ├── api.py              # Endpoints Flask principales
│   ├── app.py              # Configuración de la aplicación
│   ├── main.py             # Entry point alternativo
│   ├── utils.py            # Utilidades y helpers
│   ├── tasas.py            # Consulta de tasas BCV desde BigQuery
│   ├── testing.py          # Scripts de prueba
│   └── countries/
│       ├── venezuela.py    # Lógica específica Venezuela
│       ├── colombia.py     # Lógica específica Colombia
│       └── argentina.py    # Lógica específica Argentina (futuro)
├── tmp/                    # Archivos temporales locales
├── docker-compose.yml      # Producción (usa ADC)
├── docker-compose.local.yml# Desarrollo (usa credentials.json)
├── Dockerfile              # Imagen Docker
├── deploy-cloud-run.ps1    # Script de despliegue PowerShell
├── requirements.txt        # Dependencias Python
├── .env                    # Variables de entorno (no versionado)
├── .gitignore
└── README.md
```

---

## Despliegue

### Cloud Run (PowerShell)

```powershell
# Despliegue básico
.\deploy-cloud-run.ps1

# Con parámetros personalizados
.\deploy-cloud-run.ps1 `
    -ProjectId "tu-proyecto" `
    -Region "us-central1" `
    -ServiceName "capex-api" `
    -Memory "1Gi" `
    -Timeout 600 `
    -Concurrency 40
```

### Parámetros del Script

| Parámetro | Default | Descripción |
|-----------|---------|-------------|
| `-ProjectId` | `gtf-cxp` | ID del proyecto GCP |
| `-Region` | `us-central1` | Región de despliegue |
| `-ServiceName` | `capex-consolidated-payment` | Nombre del servicio |
| `-Memory` | `1Gi` | Memoria asignada |
| `-CPU` | `1` | CPUs asignadas |
| `-Timeout` | `600` | Timeout en segundos |
| `-Concurrency` | `40` | Requests concurrentes |
| `-MinInstances` | `0` | Instancias mínimas |
| `-MaxInstances` | `10` | Instancias máximas |
| `-SkipBuild` | `false` | Saltar build de Docker |
| `-SkipPush` | `false` | Saltar push al registry |

---

## Tasas de Cambio

### Fuentes de Tasas

| Columna | Fuente | Descripción |
|---------|--------|-------------|
| **TC FTD** | Endpoint FTD | Tasa Farmatodo desde `TC_FTD_ENDPOINT` |
| **TC BCV** | BigQuery | Tasa BCV desde `cxp_vzla.bcv_tasas` |

### Tabla de Tasas BCV en BigQuery

```sql
-- Estructura de la tabla cxp_vzla.bcv_tasas
CREATE TABLE cxp_vzla.bcv_tasas (
    Date DATE,
    USD FLOAT64,
    EUR FLOAT64
);
```

### Módulo tasas.py

El módulo `tasas.py` maneja la consulta de tasas BCV:

```python
from tasas import obtener_tasa_bcv, precargar_tasas_bcv

# Pre-cargar todas las tasas (recomendado al inicio)
cache = precargar_tasas_bcv()

# Obtener tasa para una fecha específica
tasa = obtener_tasa_bcv('2026-01-28')
print(f"Tasa BCV: {tasa} VES/USD")
```

---

## Cierre de Mes

El sistema ejecuta automáticamente el cierre de mes cuando se detecta que es la **semana 1 de un nuevo mes** (días 1-7).

### Operaciones de Cierre

1. **Actualización de títulos** en hojas:
   - **Graficos**: Celdas G6, H6, I6 → Mes/Año actual
   - **Presupuesto Mensual**: Celdas C18, D18, E18 → Mes anterior/actual

2. **Traspaso de Diferencia a Remanente**:
   - Filas afectadas: 20, 22-32 (excluyendo 21)
   - Fórmula: `Remanente (C) = E - D + C` (Diferencia del mes anterior)

### Ejemplo de Títulos

| Celda | Antes (Enero) | Después (Febrero) |
|-------|---------------|-------------------|
| G6 | PPTO Enero-2026 | PPTO Febrero-2026 |
| H6 | Pagado Enero-2026 | Pagado Febrero-2026 |
| C18 | Remanente Diciembre-2025 | Remanente Enero-2026 |
| D18 | Presupuesto Enero-2026 | Presupuesto Febrero-2026 |

---

## Columnas del Detalle Corregido

El orden de columnas para la hoja "Detalle Corregido" es:

1. Número de Factura
2. Proveedor
3. Descripción
4. ...
5. MONTO A PAGAR CAPEX
6. **MONEDA DE PAGO** *(nueva)*
7. **FECHA PAGO** *(nueva)*
8. **TC FTD** *(nueva)*
9. **TC BCV** *(nueva)*
10. **CONVERSION VES** *(nueva)*
11. **CONVERSION TC FTD** *(nueva)*
12. **REAL RECONVERTIDO** *(nueva)*
13. **REAL MES RECONVERTIDO** *(nueva)*

---

## Troubleshooting

### Error: "No se pudo conectar a BigQuery"

1. Verificar que `GOOGLE_APPLICATION_CREDENTIALS` apunte al archivo correcto
2. Verificar permisos de la cuenta de servicio en BigQuery
3. Verificar que el proyecto GCP esté correctamente configurado

### Error: "Bucket no encontrado"

1. Verificar variable `GCS_BUCKET_NAME`
2. Verificar permisos de la cuenta de servicio en GCS
3. Verificar que el bucket exista en el proyecto

### Timeout en BigQuery

El procesamiento tiene timeouts configurados:
- Carga de datos: 300s
- Conteo de registros: 120s
- Extracción por lotes: 300s

Para archivos muy grandes, considerar incrementar estos valores.

### Logs del contenedor

```bash
# Ver logs en tiempo real
docker logs -f capex-automation-api

# Ver últimas 100 líneas
docker logs --tail 100 capex-automation-api
```

---

## Mantenimiento

### Limpiar cache de tasas

```python
from tasas import obtener_helper_tasas

helper = obtener_helper_tasas()
helper.limpiar_cache()  # Fuerza recarga desde BigQuery
```

### Actualizar plantillas

1. Subir nueva plantilla a `GCS/template/{pais}/`
2. Mantener el mismo nombre de archivo
3. El sistema usará automáticamente la nueva versión

---

## Licencia

Proyecto interno - Farmatodo

---

## Contacto

Para soporte técnico, contactar al equipo de desarrollo.
