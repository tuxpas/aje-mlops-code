"""
Orquestador Multi-País - Pedido Sugerido
Crea y ejecuta un pipeline de SageMaker por país usando imágenes ECR personalizadas
con los scripts PS_MP_X.py ya incluidos en cada imagen.

Antes de ejecutar y construir, subir las imágenes.

El archivo paises_config.json debe estar previamente disponible en S3:
    s3://<BUCKET_ARTIFACTS>/<CONFIG_S3_PREFIX>/paises_config.json
"""

import os
import boto3
import sagemaker
from sagemaker import get_execution_role
from botocore.exceptions import ClientError
from sagemaker.processing import Processor, ProcessingInput, ProcessingOutput
from sagemaker.workflow.steps import ProcessingStep, CacheConfig
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.functions import Join

REGION = "us-east-2"

BUCKET_ARTIFACTS = "aje-dev-analytics-artifacts-s3" # Bucket for storing the inputs for the sm pipeline
BUCKET_CONFIG = "aje-dev-analytics-artifacts-s3" # Bucket for storing the specific configs of the countries
BUCKET_BACKUP = "aje-analytics-ps-backup" # Bucket for storing the results of the reglas_negocio pipeline step
BUCKET_STEPS_RESULTS = "aje-dev-analytics-artifacts-s3" # Bucket To replace the use of filysistem for the results of the pipeline steps

CONFIG_S3_PREFIX = "config/MP" ##

PROCESSING_IMAGE_NAME = "aje-ps-processing"
SPARK_IMAGE_NAME = "aje-ps-spark"
IMAGE_TAG = "latest"

country = ParameterString(name="country")  # "EC", "MX", "PE", "CR", "GT", "NI" or "PA"


def get_ecr_image_uri(image_name, account_id, region=REGION, tag=IMAGE_TAG):
    return f"{account_id}.dkr.ecr.{region}.amazonaws.com/{image_name}:{tag}"


def verify_config_in_s3(bucket, key):
    """Raises an error if paises_config.json is not present in S3."""
    s3 = boto3.client("s3", region_name=REGION)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        print(f"Config encontrado en s3://{bucket}/{key}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            raise FileNotFoundError(
                f"paises_config.json no encontrado en s3://{bucket}/{key}. "
                "Súbelo manualmente antes de ejecutar el pipeline."
            ) from e
        raise


def crear_pipeline(sagemaker_session, role, default_bucket, account_id):
    """
    Creates a single 3-step SageMaker Pipeline parameterized by country.
    Each step uses a custom ECR image that already contains the processing script.
    """
    prefix_config = f"s3://{BUCKET_ARTIFACTS}/{CONFIG_S3_PREFIX}"
    ecr_processing = get_ecr_image_uri(PROCESSING_IMAGE_NAME, account_id)
    ecr_spark = get_ecr_image_uri(SPARK_IMAGE_NAME, account_id)

    # ---------- ProcessingStep 1: Limpieza ----------
    proc_1 = Processor(
        image_uri=ecr_processing,
        role=role,
        instance_count=1,
        instance_type="ml.m5.4xlarge",
        entrypoint=["python3", "/opt/program/PS_MP_1_limpieza.py"],
        base_job_name="ps-mp-limpieza",
        sagemaker_session=sagemaker_session,
    )

    step_limpieza = ProcessingStep(
        name="Limpieza",
        processor=proc_1,
        inputs=[
            ProcessingInput(
                input_name="config",
                source=f"{prefix_config}/paises_config.json",
                destination="/opt/ml/processing/input/config",
            ),
        ],
        outputs=[
            ProcessingOutput(
                output_name="output-limpieza",
                source="/opt/ml/processing/output/limpieza",
                destination=Join(on="/", values=[f"s3://{default_bucket}/ps-mp-pipeline", country, "limpieza"]),
            )
        ],
        cache_config=CacheConfig(enable_caching=True, expire_after="P3D"),
        environment={
            "PAIS": country,
            "BUCKET_ARTIFACTS": BUCKET_ARTIFACTS,
            "CONFIG_S3_PREFIX": CONFIG_S3_PREFIX,
            "BUCKET_CONFIG": BUCKET_CONFIG,
            "BUCKET_STEPS_RESULTS": BUCKET_STEPS_RESULTS
        },
    )

    # ---------- ProcessingStep 2: Modelado (PySpark / ALS) ----------
    proc_2 = Processor(
        image_uri=ecr_spark,
        role=role,
        instance_count=1,
        instance_type="ml.m5.2xlarge",
        entrypoint=["python3", "/opt/program/PS_MP_2_modelado.py"],
        base_job_name="ps-mp-modelado",
        sagemaker_session=sagemaker_session,
        max_runtime_in_seconds=7200,
    )

    step_modelado = ProcessingStep(
        name="Modelado",
        processor=proc_2,
        inputs=[
            ProcessingInput(
                input_name="limpieza",
                source=step_limpieza.properties.ProcessingOutputConfig.Outputs[
                    "output-limpieza"
                ].S3Output.S3Uri,
                destination="/opt/ml/processing/input",
            ),
            ProcessingInput(
                input_name="config",
                source=f"{prefix_config}/paises_config.json",
                destination="/opt/ml/processing/input/config",
            ),
        ],
        outputs=[
            ProcessingOutput(
                output_name="output-modelado",
                source="/opt/ml/processing/output/modelado",
                destination=Join(on="/", values=[f"s3://{default_bucket}/ps-mp-pipeline", country, "modelado"]),
            )
        ],
        cache_config=CacheConfig(enable_caching=True, expire_after="P3D"),
        environment={
            "PAIS": country,
            "CONFIG_S3_PREFIX": CONFIG_S3_PREFIX,
            "BUCKET_CONFIG": BUCKET_CONFIG,
            "BUCKET_STEPS_RESULTS": BUCKET_STEPS_RESULTS
        },
    )

    # ---------- ProcessingStep 3: Reglas de Negocio ----------
    proc_3 = Processor(
        image_uri=ecr_processing,
        role=role,
        instance_count=1,
        instance_type="ml.m5.4xlarge",
        entrypoint=["python3", "/opt/program/PS_MP_3_reglas_negocio.py"],
        base_job_name="ps-mp-reglas",
        sagemaker_session=sagemaker_session,
    )

    step_reglas = ProcessingStep(
        name="ReglasNegocio",
        processor=proc_3,
        inputs=[
            ProcessingInput(
                input_name="limpieza",
                source=step_limpieza.properties.ProcessingOutputConfig.Outputs[
                    "output-limpieza"
                ].S3Output.S3Uri,
                destination="/opt/ml/processing/input/limpieza",
            ),
            ProcessingInput(
                input_name="modelado",
                source=step_modelado.properties.ProcessingOutputConfig.Outputs[
                    "output-modelado"
                ].S3Output.S3Uri,
                destination="/opt/ml/processing/input/modelado",
            ),
            ProcessingInput(
                input_name="config",
                source=f"{prefix_config}/paises_config.json",
                destination="/opt/ml/processing/input/config",
            ),
        ],
        outputs=[
            ProcessingOutput(
                output_name="output-reglas",
                source="/opt/ml/processing/output/reglas",
                destination=Join(on="/", values=[f"s3://{default_bucket}/ps-mp-pipeline", country, "reglas"]),
            )
        ],
        environment={
            "PAIS": country,
            "CONFIG_S3_PREFIX": CONFIG_S3_PREFIX,
            "BUCKET_CONFIG": BUCKET_CONFIG,
            "BUCKET_BACKUP": BUCKET_BACKUP,
            "BUCKET_STEPS_RESULTS": BUCKET_STEPS_RESULTS
        },
    )

    pipeline = Pipeline(
        name="Pipeline-PedidoSugerido-OneRun",
        parameters=[country],
        steps=[step_limpieza, step_modelado, step_reglas],
        sagemaker_session=sagemaker_session,
    )
    return pipeline


def main(pais=None, ejecutar=False):
    """
    Creates (or updates) the single-country SageMaker Pipeline and optionally starts it.

    Args:
        pais: country code to run (e.g. 'EC', 'MX'). Reads PAIS env var if not provided.
        ejecutar: if True, starts the pipeline execution. If False, only upserts the pipeline.
    """
    sagemaker_session = sagemaker.Session()
    role = os.getenv("SAGEMAKER_ROLE_ARN") or get_execution_role()
    default_bucket = sagemaker_session.default_bucket()
    sts = boto3.client("sts", region_name=REGION)
    account_id = sts.get_caller_identity()["Account"]

    print(f"SageMaker role: {role}")
    print(f"Default bucket: {default_bucket}")
    print(f"Account ID: {account_id}")

    config_key = f"{CONFIG_S3_PREFIX}/paises_config.json"
    verify_config_in_s3(BUCKET_ARTIFACTS, config_key)

    pipeline = crear_pipeline(sagemaker_session, role, default_bucket, account_id)
    pipeline.upsert(role_arn=role)
    print("Pipeline creado/actualizado en SageMaker.")

    if ejecutar:
        if pais is None:
            pais = os.environ.get("PAIS", "").strip().upper()
        if not pais:
            raise ValueError(
                "Se requiere especificar el país. "
                "Pasa el argumento 'pais' o define la variable de entorno PAIS."
            )
        print(f"Iniciando ejecución para: {pais}")
        execution = pipeline.start(parameters={"country": pais})
        print(f"Ejecución iniciada: {execution.arn}")
        return execution
    else:
        print("Pipeline listo (ejecutar=False, no se inició).")
        return None


if __name__ == "__main__":
    main()

