# El modelo ALS

> Cómo funciona el motor recomendador: ALS implícito de PySpark MLlib.

---

## TL;DR

Usamos **Alternating Least Squares (ALS) con preferencias implícitas** de PySpark MLlib. El modelo aprende *embeddings* latentes de clientes y SKUs a partir de la frecuencia de compra, y recomienda los SKUs con mayor score para cada cliente.

```python
from pyspark.ml.recommendation import ALS

als = ALS(
    rank=10,                        # factores latentes
    maxIter=5,                      # iteraciones
    implicitPrefs=True,             # preferencias implícitas (no ratings)
    ratingCol="rating",             # = count distinct fecha_liquidacion
    itemCol="cod_articulo_magic",   # SKU
    userCol="clienteId_numeric",    # cliente numérico
    coldStartStrategy="drop"        # ignora usuarios/items nuevos
)
model = als.fit(train_df)
recs = model.recommendForAllUsers(N)
```

---

## ¿Qué es ALS implícito?

**ALS** (Alternating Least Squares) es el método clásico de filtrado colaborativo por factorización de matrices. Descompone la matriz **cliente × SKU** en dos matrices de factores latentes:

```
  R (cliente × SKU)  ≈  U (cliente × k)  ·  Vᵀ (k × SKU)
```

Cada cliente se representa como un vector `k`-dimensional, cada SKU también, y el score de afinidad es el producto punto.

**¿Por qué "implícito"?** Porque no tenemos estrellas o likes. Solo observamos **comportamiento** (compra / no compra, frecuencia). El paper de referencia es *Hu, Koren, Volinsky 2008 — "Collaborative Filtering for Implicit Feedback Datasets"*, que Spark implementa directamente.

En el caso implícito:
- El "rating" **no es una preferencia**, es una **confianza** (`c_{ui} = 1 + α·r_{ui}`).
- Todos los pares no observados son 0 (no preferencia) pero con baja confianza.
- El modelo optimiza una función de costo ponderada por confianza.

---

## La "señal": frecuencia de compra

No usamos montos ni cantidades. Usamos:

```python
rating = count(distinct fecha_liquidacion)  # por (id_cliente, cod_articulo_magic)
```

Ejemplo:

| id_cliente | cod_articulo_magic | rating |
|---|---|---|
| PE\|10\|12345 | 508462 | 8 |
| PE\|10\|12345 | 524187 | 3 |
| PE\|10\|12345 | 530001 | 1 |
| PE\|10\|67890 | 508462 | 12 |

El cliente `12345` compró el SKU `508462` en **8 fechas distintas** durante el período → confianza alta de que lo quiere.

**Por qué "fechas distintas" y no "cantidad total":**
- Dos clientes pueden comprar 100 unidades del mismo SKU con patrones muy distintos — uno de una sola vez (1 fecha) y otro en 50 visitas (50 fechas). Para este negocio, el segundo es una señal **mucho más fuerte** de afinidad recurrente.

---

## Preparación de datos

ALS necesita `userId` y `itemId` **numéricos**. Nuestro `id_cliente` es un string (`PE|10|12345`), así que:

```python
from pyspark.ml.feature import StringIndexer

indexer = StringIndexer(inputCol="id_cliente", outputCol="clienteId_numeric")
indexer_model = indexer.fit(df)
df_indexed = indexer_model.transform(df)
```

Importante:
- Guardamos el `indexer_model` para poder **invertir** el mapeo tras generar las recomendaciones.
- `cod_articulo_magic` ya es numérico, no requiere indexación.

---

## Parámetros y elección

| Parámetro | Valor | Intuición |
|---|---|---|
| `rank` | 10 | Nº de factores latentes. Más = mejor ajuste pero mayor riesgo de overfit y más cómputo. 10 es un balance razonable para el tamaño de los catálogos. |
| `maxIter` | 5 | Pocas iteraciones suelen bastar en ALS implícito — el beneficio marginal baja rápido. |
| `implicitPrefs` | `True` | **Obligatorio**, no tenemos ratings explícitos. |
| `alpha` | default (1.0) | Parámetro de confianza del paper; no se tunea. |
| `regParam` | default (0.1) | Regularización L2; default ha funcionado. |
| `coldStartStrategy` | `"drop"` | Si un cliente/SKU no tiene datos de entrenamiento, no se recomienda (en vez de NaN). |
| `nonnegative` | `False` | Permite factores negativos — no afecta mucho en implícito. |

**Parámetros variables por país:** en algunos países la configuración es ligeramente distinta (por ej. `rank=5`). Ver [paises.md](paises.md).

---

## Generación de recomendaciones

```python
recs = model.recommendForAllUsers(N)   # N ~ 20
```

`recs` tiene la forma:

```
clienteId_numeric, recommendations: [(itemId, score), (itemId, score), ...]
```

Se des-pivota a columnas posicionales:

```python
# pseudo-código post-procesamiento
recs_flat = recs.explode("recommendations")
            .withColumn("posicion", F.row_number().over(Window.partitionBy("clienteId_numeric").orderBy(F.desc("score"))))

pivot = recs_flat.groupBy("clienteId_numeric").pivot("posicion").agg(F.first("itemId"))
# Columnas resultantes: 1, 2, 3, ... → renombradas a r1, r2, r3, ...
```

Luego se revierte el `StringIndexer` para recuperar el `id_cliente` original.

---

## Forma del output del paso 2

```
id_cliente,       r1,     r2,     r3,     r4,     ...,   r20
PE|10|12345,      508462, 524187, 530001, 508505, ...,   541298
PE|10|67890,      508462, 530001, 524187, 541298, ...,   508505
```

Este ranking es solo un **candidato**: el paso 3 (reglas de negocio) va a filtrar y despriorizar muchas de estas entradas antes de emitir el CSV final.

---

## Rendimiento

| Operación | Tamaño típico (Perú) | Tiempo |
|---|---|---|
| Carga de matriz implícita | ~200k clientes × ~2k SKUs ≈ 50M filas | ~1 min |
| `StringIndexer.fit` | | <1 min |
| `ALS.fit` (rank=10, iter=5) | | 5-10 min |
| `recommendForAllUsers(20)` | | 2-5 min |

En `ml.m5.4xlarge` (16 vCPU · 64 GB RAM) con 1 instancia. Para escalar, aumentar `spark_processor.instance_count` o cambiar a una instancia más grande.

---

## Limitaciones conocidas

1. **Cold start**: un cliente nuevo (sin historial) no recibe recomendaciones (`coldStartStrategy="drop"`). El paso 3 no tiene mitigación para esto — estos clientes simplemente no aparecen en el CSV final.
2. **SKUs nuevos**: lo mismo — sin historial, no se recomiendan. El módulo PE (Plan Estratégico) de Ecuador es en parte una respuesta a esto: forzar productos estratégicos en base a reglas, sin ML.
3. **Estacionalidad**: el modelo usa 12 meses de historial sin peso temporal. Un SKU que solo se vende en diciembre puede recomendarse en junio. Las reglas de negocio (especialmente 5.-9 "disponibilidad") compensan parcialmente filtrando por ventas recientes.
4. **Diversidad**: ALS tiende a recomendar los SKUs más populares. No hay un término de diversificación explícito en el ranking.

---

## Variantes y casos especiales

### KMeans (deprecado)

En versiones antiguas del pipeline había un pre-clustering con **KMeans** sobre los clientes antes del ALS (para generar recomendaciones por segmento). Se removió. En `PS_Mexico/deploy/ps_3_run_model.py` aún queda código comentado de referencia.

### Plan Estratégico (PE) — Ecuador

No usa ML. Simplemente hace el **producto cartesiano** `clientes × productos_estratégicos` (lista fija, ej. `[508462, 524187, ...]`) y emite recomendaciones para todos. Ver `PEs_Ecuador/TEST_PE_EC_1_estrategico.py`. Se usa para empujar SKUs prioritarios por negocio que el ALS no "descubre" por falta de historial.

### Pedido Recurrente (PR) — Ecuador (legacy)

Módulo viejo que genera pedidos basándose en **patrones de recurrencia** (heurística, no ML). Hoy solo Ecuador lo mantiene. El Reporting lo concatena junto con PS y PE.
