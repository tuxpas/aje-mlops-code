# Datos — catálogo de inputs, outputs y paths

> Referencia de dónde vive qué, cómo se llama, qué columnas tiene, quién lo produce y quién lo consume.

---

## Mapa de alto nivel

```
                 Upstream (no en este repo)
                          │
        ┌─────────────────┴──────────────────┐
        ▼                                     ▼
   REDSHIFT                              S3 INPUT
   dwh-cloud-storage-                    aje-prd-analytics-
   salesforce-prod                       artifacts-s3
        │                                     │
        └─────────────┬───────────────────────┘
                      ▼
        ┌─────────────────────────────┐
        │  Pipeline PS (este repo)    │
        │  por país (×8)              │
        └──────────────┬──────────────┘
                       ▼
              S3 BACKUP (por país)
              aje-analytics-ps-backup
                       │
                       ▼
              Reporting (este repo)
                       │
        ┌──────────────┴─────────────┐
        ▼                             ▼
   S3 BACKUP FINAL              S3 CENTRAL ORDERS
   aje-analytics-ps-backup      aje-prd-pedido-
   /Output/0_Final_PS/          sugerido-orders-s3
                                       │
                                       ▼
                                 SALESFORCE
```

---

## Inputs de upstream

### Redshift

| Cluster | Database | Conexión |
|---|---|---|
| `dwh-cloud-storage-salesforce-prod` | `dwh_prod` | `awswrangler` + `redshift-connector` |

**Tablas clave:**

| Tabla | Para qué | Cuándo se lee |
|---|---|---|
| `comercial_<pais>.dim_producto` | Maestro de productos vigente (SKUs, marcas, categorías). | Paso 1 (limpieza) — se descarga a CSV antes del pipeline. |
| Varias tablas de `comercial_*` | Stock, precios, mapeos adicionales según país. | Scripts de limpieza. |

### S3 `aje-prd-analytics-artifacts-s3`

Bucket **externo al pipeline** — lo llena un ETL upstream (Salesforce → Redshift → S3).

**Estructura:**

```
aje-prd-analytics-artifacts-s3/
└── pedido_sugerido/
    └── data-v1/
        ├── peru/
        │   ├── visitas_peru000.parquet
        │   ├── ventas_peru000.parquet
        │   ├── maestro_productos_peru000.csv
        │   └── stock/
        ├── mexico/
        │   ├── visitas_mexico000.parquet
        │   ├── ventas_mexico000.parquet
        │   └── ...
        ├── ecuador/
        ├── guatemala/
        ├── nicaragua/
        ├── panama/
        ├── costarica/
        └── econoredes_ecuador/
```

**Convención importante**: el pipeline valida que el archivo tenga `fecha_modificación = hoy` antes de procesarlo. Si el ETL upstream falló, el pipeline aborta.

### Archivos Excel manuales

Los sube negocio a mano en el bucket / a la máquina de `deploy/`.

| Archivo | Uso | Quién lo sube |
|---|---|---|
| `LISTA DE PRECIOS <PAIS>_YYYY-MM-DD.xlsx` | Hoja `LISTA SKUS` — exclusiones manuales (regla 5.-4). | Equipo comercial. |
| `MX_SKUS.xlsx` | Lista de SKUs objetivos para México. | Equipo comercial. |
| `MX_compa_sucursal_*.csv` | Mapeo compañía-sucursal para México. | Equipo comercial. |

---

## Datasets de entrada — esquemas

### `visitas_<pais>000.parquet`

Listado de visitas programadas/realizadas del día.

| Columna | Tipo | Descripción |
|---|---|---|
| `id_cliente` (crudo) | str | ID crudo del cliente (se normalizará). |
| `id_compania` | int | Compañía del vendedor. |
| `id_sucursal` (ruta) | int | Ruta / sucursal. |
| `fecha_proceso` | date | Día al que pertenece el registro. |
| `ultima_visita` | datetime | Timestamp de la última visita al cliente. |
| otras | varias | Datos del vendedor, ruta, etc. |

### `ventas_<pais>000.parquet`

Historial de ventas (típicamente últimos 12+ meses).

| Columna | Tipo | Descripción |
|---|---|---|
| `id_cliente` | str | ID cliente. |
| `id_compania` | int | Compañía. |
| `id_producto` | str | Contiene `cod_articulo_magic` embebido. |
| `fecha_liquidacion` | date | Fecha de la transacción. |
| `unidades`, `cajas` | numeric | Cantidad. |
| `monto` | numeric | Importe. |

---

## Datos intermedios (generados por el pipeline)

Estos viven en los **Processing Output** de SageMaker (S3 de la pipeline) o en `Processed/` del deploy local.

### Después del Paso 1 (limpieza)

| Archivo | Contenido |
|---|---|
| `data_limpia.parquet` | Visitas + ventas unidas, con `id_cliente` normalizado y filtrado a `fecha_proceso = hoy`. |
| `<pais>_ventas_manana.parquet` (deploy MX) | Clientes con visita hoy. |
| `ventas_<pais>_12m.parquet` (deploy MX) | Historial 12 meses para el modelo. |

### Después del Paso 2 (modelado)

| Archivo | Contenido |
|---|---|
| `recomendaciones.parquet` | `id_cliente`, `r1`, `r2`, …, `rN` — ranking posicional del ALS. |

---

## Output final

### Por país — backup

**Bucket:** `aje-analytics-ps-backup`

**Path:**

```
s3://aje-analytics-ps-backup/PS_<Pais>/Output/PS_piloto_v1/D_base_pedidos_YYYY-MM-DD.csv
```

**Esquema:**

| Columna | Tipo | Descripción |
|---|---|---|
| `Pais` | str | `PE, MX, EC, GT, NI, PA, CR`. |
| `Compania` | int | Código de compañía. |
| `Sucursal` | int | Ruta / sucursal. |
| `Cliente` | int/str | ID cliente. |
| `Modulo` | str | `PS` / `PE` / `PR`. |
| `Producto` | int | `cod_articulo_magic`. |
| `Cajas` | int | Default `1`. |
| `Unidades` | int | Default `0`. |
| `Fecha` | date | Fecha de proceso. |
| `tipoRecomendacion` | str | `PS1, PS2, …, PS20` (posición en ranking final). |
| `ultFecha` | date | Última vez que el cliente compró este SKU. |
| `Destacar` | int | `0` / `1` — resaltar en Salesforce. |

**Retención:** un archivo por día, sin TTL configurado (archivo histórico completo).

### Consolidado global — backup

**Bucket:** `aje-analytics-ps-backup`

**Path:**

```
s3://aje-analytics-ps-backup/Output/0_Final_PS/base_pedidos_final_YYYY-MM-DD.csv
```

Mismo esquema que los CSVs por país, pero concatenado (una fila por país · compañía · sucursal · cliente · producto).

### Consolidado global — central (para Salesforce)

**Bucket:** `aje-prd-pedido-sugerido-orders-s3`

**Path:**

```
s3://aje-prd-pedido-sugerido-orders-s3/PE/pedidos/base_pedidos.csv
```

> ⚠️ **Se sobrescribe cada día** — no es histórico. Es el "inbox" de Salesforce.

---

## Resumen de buckets

| Bucket | Uso | Lectura / Escritura |
|---|---|---|
| `aje-prd-analytics-artifacts-s3` | Datos crudos diarios (upstream). | Pipeline **lee**. Llenado por ETL externo. |
| `<pipeline-bucket-sagemaker>` | Artefactos intermedios del Pipeline (limpieza, modelado). | Pipeline **lee/escribe**. |
| `aje-analytics-ps-backup` | Salidas por país (backup) + consolidado histórico. | Pipeline y Reporting **escriben**. Reporting **lee** de aquí. |
| `aje-prd-pedido-sugerido-orders-s3` | Archivo central que Salesforce consume. | Reporting **escribe**. Salesforce **lee**. |

---

## Tablas Redshift consumidas (resumen)

| Schema | Tabla | Uso |
|---|---|---|
| `comercial_peru` | `dim_producto` | Maestro Perú. |
| `comercial_mexico` | `dim_producto` | Maestro México. |
| `comercial_ecuador` | `dim_producto` | Maestro Ecuador. |
| `comercial_guatemala` | `dim_producto` | Maestro Guatemala. |
| `comercial_nicaragua` | `dim_producto` | Maestro Nicaragua. |
| `comercial_panama` | `dim_producto` | Maestro Panamá. |
| `comercial_costarica` | `dim_producto` | Maestro Costa Rica. |

(Otras tablas pueden consultarse según el país — ver cada script de limpieza.)

---

## Frecuencia y volumen

| Métrica | Orden de magnitud |
|---|---|
| Frecuencia de ejecución | 1× por día (por país) |
| Tamaño de `visitas_*.parquet` (Perú) | ~10-30 MB |
| Tamaño de `ventas_*.parquet` (Perú, 12m) | ~300-600 MB |
| Matriz de entrenamiento ALS (Perú) | ~200k clientes × ~2k SKUs ≈ 50M celdas no-cero |
| Tamaño CSV final (Perú) | ~150-300 MB |
| Tamaño CSV final consolidado | ~400-800 MB |

---

## Ciclo de vida

```
00:00 → ETL upstream → S3 input (fresco)
07:00 → Pipelines PS por país (×8 en paralelo)
09:30 → Todos los países terminan
10:00 → Reporting consolida, envía email, sube central
10:05 → Salesforce levanta el CSV central
         y lo disponibiliza a vendedores en sus apps
```

Los horarios son aproximados y pueden ajustarse. No hay un orquestador global hoy — cada pipeline se lanza independientemente (manual o via EventBridge por país).
