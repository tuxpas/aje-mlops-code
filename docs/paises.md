# Países — diferencias y particularidades

> Tabla comparativa y detalles por país. Todos siguen la misma arquitectura base; lo que cambia son parámetros, rutas, compañías y SKUs excluidos.

---

## Resumen comparativo

| País | Carpeta | Código | Módulos | Compañías | Rutas | Tamaño | Particularidad |
|---|---|---|---|---|---|---|---|
| Perú | `PS_Peru/` | PE | PS | 10 | ~500 (10000-14610) | **Grande** | Lógica especial Lima Centro (10600-10670). Lista extensa de SKUs sin precio. |
| México | `PS_Mexico/` | MX | PS | 30 | 4 (piloto) | Piloto | **Único con `deploy/`** para producción local. |
| Ecuador (main) | `PS_Ecuador/` | EC | PS | — | varias | Medio | Pipeline PS estándar. |
| Ecuador Econoredes | `PS_Econoredes_Ecuador/` | EC_ECO | PS | — | canal Econoredes | Medio | Variante del canal Econoredes. |
| Ecuador Estratégico | `PEs_Ecuador/` | EC | **PE** | — | rutas específicas | — | **No usa ML**. Producto cartesiano clientes × lista fija de SKUs. |
| Guatemala | `PS_Guatemala/` | GT | PS | — | — | Medio | Estándar. |
| Nicaragua | `PS_Nicaragua/` | NI | PS | — | — | Pequeño | Estándar. |
| Panamá | `PS_Panama/` | PA | PS | — | — | Pequeño | Estándar. |
| Costa Rica | `PS_CostaRica/` | CR | PS | — | — | Pequeño | Estándar. |

---

## Perú (`PS_Peru/`)

**El más complejo** — mercado más grande, más compañías, más rutas y más overrides manuales.

### Rutas

- **Rango general:** 10000 - 14610.
- **Exclusiones Lima Centro:** rutas 10600 - 10670 se manejan aparte.
- Lista hardcoded `SUCURSALES_LIMA` con lógica especial para sucursales como `TRES_CRUCES`.

### Compañías

Compañía principal: `10`. Otras compañías existen pero el pipeline se configura para la principal.

### SKUs sin precio

Lista con **50+ SKUs hardcodeados** que se excluyen por no tener precio vigente:

```python
SKUS_SIN_PRECIO_PE = [
    508505, 508506, 508507, 508562, 510456,
    # ... (ver TEST_PS_PE_3_reglas_negocio.py)
]
```

### Archivos

- `PROD_PS_Peru.ipynb` — referencia todo-en-uno.
- `TEST_PS_PE_1_limpieza.py`, `_2_modelado.py`, `_3_reglas_negocio.py` — scripts de producción.
- `TEST_PS_PE_4_orquestador_pipeline.ipynb` — lanza el pipeline SageMaker.
- `TEST_PS_PE_solo_paso3.ipynb` — relanzar **solo** el paso 3 sin reentrenar (útil para iterar reglas).

---

## México (`PS_Mexico/`)

**El único país con pipeline productivo local**: además de los scripts SageMaker, tiene una carpeta `deploy/` con 5 scripts secuenciales que se pueden correr localmente, sin depender del Pipeline de SageMaker.

### Estado

- **Piloto** con 4 rutas: `1155, 1158, 1074, 1065`.
- Compañía: `30`.

### Archivos distintivos

```
PS_Mexico/
├── deploy/
│   ├── ps_1_dwld_input.py         ← descarga inputs (Redshift + S3)
│   ├── ps_2_process_input.py      ← limpieza
│   ├── ps_3_run_model.py          ← ALS
│   ├── ps_4_reglas_negocio.py     ← reglas
│   ├── ps_5_subir_a_SF.py         ← upload Salesforce
│   ├── ps_9_run_all.py            ← orquestador local
│   └── requirements.txt           ← dependencias congeladas
├── D_PS_Mexico_2024-Copy1.ipynb
├── D_PS_Mexico_2024_no_pyspark.ipynb   ← versión sin pyspark (legacy)
├── PROD_PS_Mexico.ipynb
├── PS_MX_script_test.py
└── TEST_PS_MX_1_limpieza.py, _2_modelado.py, _3_reglas_negocio.py, _4_orquestador_pipeline.ipynb
```

Ver [deploy-mexico.md](deploy-mexico.md) para el detalle del pipeline local.

### Archivo adicional

- `MX_compa_sucursal_2025-08-04.csv` — mapeo compañía-sucursal específico de México.

---

## Ecuador — tres pipelines en uno

Ecuador es el país con **más módulos**. Tres carpetas independientes:

### 1. `PS_Ecuador/` — Pedido Sugerido principal

Pipeline PS estándar. Mismo patrón que los demás países.

### 2. `PS_Econoredes_Ecuador/` — canal Econoredes

Variante del pipeline PS para el canal Econoredes (cadenas económicas). Los datos de entrada y las reglas son distintos al canal tradicional.

Nota: esta carpeta contiene un `__pycache__/` (rastro de ejecución local).

### 3. `PEs_Ecuador/` — Plan Estratégico (no ML)

**No usa modelo ML.** Es un pipeline de una sola etapa que genera un producto cartesiano:

```python
# Pseudo-código
productos_estrategicos = [508462, 524187, 530001, ...]  # lista manual
clientes_ruta = ventas_df.filter(ruta in rutas_objetivo).select("id_cliente").distinct()

recomendaciones = clientes_ruta.crossJoin(productos_estrategicos)
```

El objetivo es **forzar** que ciertos SKUs estratégicos aparezcan recomendados en clientes específicos, sin depender de que el ALS los "descubra".

Luego Reporting los concatena junto con PS y PR bajo la columna `tipoRecomendacion = PE1, PE2, ...`.

---

## Países "estándar"

### Guatemala (`PS_Guatemala/`)
### Nicaragua (`PS_Nicaragua/`)
### Panamá (`PS_Panama/`)
### Costa Rica (`PS_CostaRica/`)

Los 4 siguen **exactamente** la misma estructura:

```
PS_<Pais>/
├── PROD_PS_<Pais>.ipynb
├── TEST_PS_XX_1_limpieza.py
├── TEST_PS_XX_2_modelado.py
├── TEST_PS_XX_3_reglas_negocio.py
└── TEST_PS_XX_4_orquestador_pipeline.ipynb
```

Las diferencias entre ellos son:

- Compañía / sucursales: propias del país.
- Lista de SKUs sin precio: hardcoded y específica.
- Rutas activas: definidas en el script de limpieza.
- Parámetros del ALS: en algunos países `rank=5` en vez de `rank=10`.

No suelen tener overrides de lógica relevantes. Si encuentras un comportamiento distinto en uno, revisa primero el script de limpieza, ahí es donde se filtran compañías/rutas.

---

## Cómo añadir un país nuevo

1. **Clonar** una carpeta base (recomendado: `PS_Guatemala/` por ser mínima).
2. **Renombrar** scripts con el código del país nuevo (ej. `TEST_PS_CO_...` para Colombia).
3. **Configurar** en `_1_limpieza.py`:
   - Código de país (`pais = "CO"`).
   - Compañía(s) activa(s).
   - Rango de rutas válidas.
   - Paths S3 de inputs.
4. **Ajustar** en `_3_reglas_negocio.py`:
   - Lista `SKUS_SIN_PRECIO_XX`.
   - Path de output S3 (`s3://aje-analytics-ps-backup/PS_<Pais>/Output/...`).
5. **Copiar y editar** el notebook orquestador `_4_orquestador_pipeline.ipynb`:
   - Nombre del Pipeline.
   - Referencias a los scripts.
6. **Añadir** el país al script de Reporting `TEST_RPT_1_reporte_todos_paises.py`:
   - Agregar el path del bucket en `cargar_todos_los_paises()`.
   - Asegurar que aparezca en las métricas.
7. **Probar** la ejecución punta-a-punta en un día con datos completos.
8. **Monitorear** el primer mes — clientes nuevos, cobertura, SKUs que caen por stock/precio.

---

## Matriz de parámetros (referencia rápida)

| Parámetro | Peru | Mexico | Ecuador | Guate | Nic | Pan | CR |
|---|---|---|---|---|---|---|---|
| ALS `rank` | 10 | 10 | 10 | 5/10 | 5 | 5 | 5 |
| ALS `maxIter` | 5 | 5 | 5 | 5 | 5 | 5 | 5 |
| Ventana disponibilidad | 14d | 14d | 14d | 14d | 14d | 14d | 14d |
| Stock mínimo | 3× | 3× | 3× | 3× | 3× | 3× | 3× |
| Dedup histórica | 14d | 14d | 14d | 14d | 14d | 14d | 14d |
| Nº rec. por cliente (top-N) | 20 | 20 | 20 | 20 | 20 | 20 | 20 |

Los valores concretos pueden cambiar — esta tabla es una referencia. Para exacto, revisar el script de cada país.
