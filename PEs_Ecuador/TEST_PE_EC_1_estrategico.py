import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pyarrow==17.0.0"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "numpy", "pandas"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "awswrangler[redshift]", "--no-build-isolation"])

import os
import io
import random
import boto3
import pandas as pd
import awswrangler as wr
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
my_session = boto3.Session(region_name="us-east-2")

# Semilla para reproducibilidad
random.seed(42)

# Fecha de mañana
manana_lima = datetime.now(ZoneInfo("America/Lima")) + timedelta(days=1)
FECHA_REC = manana_lima.strftime("%Y-%m-%d")

# Parámetros
BUCKET_ARTIFACTS = "aje-prd-analytics-artifacts-s3"
FILE_KEY_TOMORROW = "pedido_sugerido/data-v1/ecuador/ventas_ecuador_tomorrow.csv"
S3_BUCKET_BACKUP = "aje-analytics-ps-backup"

SUCURSALES = ["EC|0090|07"]
RUTAS_ADICIONALES = [1903, 1703]
PRODUCTOS = [508462, 524187, 524188, 524189, 515517, 515519, 500010, 599653, 500147]


def generar_pedido_estrategico():
    """Genera el pedido estratégico: producto cartesiano de clientes x productos fijos."""
    print("Generando Pedido Estratégico...")

    # 1. Leer clientes a visitar mañana
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=FILE_KEY_TOMORROW)
    cl = pd.read_csv(io.BytesIO(response['Body'].read()))

    # 2. Filtrar por sucursales y rutas adicionales
    cl = cl[(cl.id_sucursal.isin(SUCURSALES)) | (cl.cod_ruta.isin(RUTAS_ADICIONALES))][
        ["cod_compania", "cod_sucursal", "cod_modulo", "cod_cliente"]
    ].drop_duplicates().reset_index(drop=True)
    print(f"Clientes filtrados: {cl.shape[0]}")

    # 3. Preparar columnas base
    df_unicos = cl[['cod_compania', 'cod_cliente']].drop_duplicates()
    cl["Pais"] = "EC"
    cl["Cajas"] = 1
    cl["Unidades"] = 0
    cl["Fecha"] = FECHA_REC

    # 4. Producto cartesiano clientes x productos
    df_prod = pd.DataFrame({'Producto': PRODUCTOS})
    df_combinado = df_unicos.merge(df_prod, how='cross')
    df_final = df_combinado.merge(cl, on=['cod_compania', 'cod_cliente'])

    # 5. Formatear columnas
    df_final.columns = ['Compania', 'Cliente', 'Producto', 'Sucursal', 'Modulo', 'Pais', 'Cajas', 'Unidades', 'Fecha']
    df_final = df_final[['Pais', 'Compania', 'Sucursal', 'Cliente', 'Modulo', 'Producto', 'Cajas', 'Unidades', 'Fecha']]

    # 6. tipoRecomendacion PE1, PE2...
    secuencia = df_final.groupby(['Compania', 'Cliente']).cumcount() + 1
    df_final['tipoRecomendacion'] = 'PE' + secuencia.astype(str)
    df_final["ultFecha"] = ''
    df_final["Destacar"] = "true"

    # 7. Formatear tipos
    df_final["Cliente"] = df_final["Cliente"].astype(int)
    df_final["Producto"] = df_final["Producto"].astype(int)
    df_final["Unidades"] = df_final["Unidades"].astype(int)
    df_final["Cajas"] = df_final["Cajas"].astype(int)
    df_final["Compania"] = df_final["Compania"].astype(str).str.zfill(4)
    df_final["Sucursal"] = df_final["Sucursal"].astype(str).str.zfill(2)

    return df_final


def excluir_recurrente_y_sugerido(df_final):
    """Quita productos que ya están en Pedido Recurrente y Pedido Sugerido."""
    print("Excluyendo productos de Recurrente y Sugerido...")

    # Leer Recurrente y Sugerido desde S3
    try:
        pr_ec = wr.s3.read_csv(
            f"s3://{S3_BUCKET_BACKUP}/Pedido_Recurrente/Ecuador/Output/recu_base_pedidos_{FECHA_REC}.csv",
            boto3_session=my_session
        )[["Compania", "Cliente", "Producto"]]
    except Exception as e:
        print(f"No se pudo leer Recurrente: {e}")
        pr_ec = pd.DataFrame(columns=["Compania", "Cliente", "Producto"])

    try:
        ps_ec = wr.s3.read_csv(
            f"s3://{S3_BUCKET_BACKUP}/PS_Ecuador/Output/PS_piloto_v1/D_base_pedidos_{FECHA_REC}.csv",
            boto3_session=my_session
        )[["Compania", "Cliente", "Producto"]]
    except Exception as e:
        print(f"No se pudo leer Sugerido: {e}")
        ps_ec = pd.DataFrame(columns=["Compania", "Cliente", "Producto"])

    quitar_temp = pd.concat([pr_ec, ps_ec], ignore_index=True).drop_duplicates()
    quitar_temp["Compania"] = quitar_temp["Compania"].astype(str).str.zfill(4)
    quitar_temp["id_cliente"] = "EC|" + quitar_temp["Compania"] + "|" + quitar_temp["Cliente"].astype(str)
    quitar_temp.rename(columns={"Producto": "cod_articulo_magic"}, inplace=True)

    # Crear id_cliente en df_final para el merge
    df_final["id_cliente"] = "EC|" + df_final["Compania"] + "|" + df_final["Cliente"].astype(str)
    df_final.rename(columns={"Producto": "cod_articulo_magic"}, inplace=True)

    # Excluir pares que ya existen en Recurrente o Sugerido
    merge_temp = df_final.merge(
        quitar_temp[["id_cliente", "cod_articulo_magic"]],
        on=["id_cliente", "cod_articulo_magic"], how="left", indicator=True
    )
    rec_sin = merge_temp[merge_temp["_merge"] == "left_only"].drop(columns=["_merge"])
    rec_sin.rename(columns={"cod_articulo_magic": "Producto"}, inplace=True)
    rec_sin.drop(columns=["id_cliente"], inplace=True)

    # Top 3 por cliente
    df_final = rec_sin.groupby(['Pais', 'Compania', 'Sucursal', 'Cliente']).head(3).reset_index(drop=True)

    # Recalcular tipoRecomendacion
    secuencia = df_final.groupby(['Compania', 'Cliente']).cumcount() + 1
    df_final['tipoRecomendacion'] = 'PE' + secuencia.astype(str)

    print(f"Estratégico final: {df_final.shape[0]} filas, {df_final.Cliente.nunique()} clientes")
    return df_final


def exportar_y_concatenar(df_estrategico):
    """Guarda backup de estratégico, concatena los 3 pedidos y sube al bucket de orders."""
    print("Exportando resultados...")

    # 1. Guardar backup de estratégico
    s3_path_backup = f"s3://{S3_BUCKET_BACKUP}/Pedido_Estrategico/Ecuador/Output/estr_base_pedidos_{FECHA_REC}.csv"
    wr.s3.to_csv(df_estrategico, s3_path_backup, index=False, boto3_session=my_session)
    print(f"Backup estratégico guardado en {s3_path_backup}")

    # 2. Leer los 3 pedidos para concatenar
    # Recurrente
    try:
        pr = wr.s3.read_csv(
            f"s3://{S3_BUCKET_BACKUP}/Pedido_Recurrente/Ecuador/Output/recu_base_pedidos_{FECHA_REC}.csv",
            boto3_session=my_session
        )
        pr = pr[["Pais", "Compania", "Sucursal", "Cliente", "Modulo", "Producto", "Cajas", "Unidades", "Fecha", "tipoRecomendacion", "ultFecha", "Destacar"]]
        print(f"Recurrente: {pr.shape[0]} filas")
    except Exception as e:
        print(f"No se pudo leer Recurrente: {e}")
        pr = pd.DataFrame()

    # Sugerido
    try:
        ps = wr.s3.read_csv(
            f"s3://{S3_BUCKET_BACKUP}/PS_Ecuador/Output/PS_piloto_v1/D_base_pedidos_{FECHA_REC}.csv",
            boto3_session=my_session
        )
        # Asegurar que tenga las 12 columnas
        if "tipoRecomendacion" not in ps.columns:
            ps["tipoRecomendacion"] = ps.groupby(["Pais", "Compania", "Sucursal", "Cliente"]).cumcount().apply(lambda x: f"PS{x+1}")
            ps["ultFecha"] = ''
            ps["Destacar"] = "true"
        ps = ps[["Pais", "Compania", "Sucursal", "Cliente", "Modulo", "Producto", "Cajas", "Unidades", "Fecha", "tipoRecomendacion", "ultFecha", "Destacar"]]
        print(f"Sugerido: {ps.shape[0]} filas")
    except Exception as e:
        print(f"No se pudo leer Sugerido: {e}")
        ps = pd.DataFrame()

    # Estratégico (ya tiene las 12 columnas)
    pe = df_estrategico[["Pais", "Compania", "Sucursal", "Cliente", "Modulo", "Producto", "Cajas", "Unidades", "Fecha", "tipoRecomendacion", "ultFecha", "Destacar"]]
    print(f"Estratégico: {pe.shape[0]} filas")

    # 3. Concatenar los 3 pedidos
    pedidos_concat = pd.concat([pr, ps, pe], ignore_index=True)
    print(f"Total concatenado: {pedidos_concat.shape[0]} filas")
    print(f"Tipos de recomendación: {pedidos_concat.tipoRecomendacion.str[:2].value_counts().to_dict()}")

    # 4. Subir al bucket de orders
    s3_path_orders = "s3://aje-prd-pedido-sugerido-orders-s3/PE/pedidos/base_pedidos.csv"
    wr.s3.to_csv(pedidos_concat, s3_path_orders, index=False, boto3_session=my_session)
    print(f"Archivo concatenado subido a {s3_path_orders}")


def main():
    print("--- INICIANDO PEDIDO ESTRATÉGICO (Ecuador) ---")

    # 1. Generar pedido estratégico
    df_estrategico = generar_pedido_estrategico()

    # 2. Excluir productos de Recurrente y Sugerido
    df_estrategico = excluir_recurrente_y_sugerido(df_estrategico)

    # 3. Exportar y concatenar los 3 pedidos
    exportar_y_concatenar(df_estrategico)

    print("--- PROCESO FINALIZADO ---")


if __name__ == "__main__":
    main()
