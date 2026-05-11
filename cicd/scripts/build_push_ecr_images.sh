#!/bin/bash
# Build and push ECR images for the Pedido Sugerido Multi-País pipeline.
# Run from the ecr/ directory:  bash build_push_ecr_images.sh
set -e

STAGE="${STAGE:-dev}"
REGION="${REGION:-us-east-2}"
ACCOUNT_ID="${ACCOUNT:-$(aws sts get-caller-identity --query Account --output text)}"
ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

PROCESSING_REPO="aje-${STAGE}-ps-processing"
SPARK_REPO="aje-${STAGE}-ps-spark"
TAG="latest"

# Ensure we're in the ecr/ directory (build context)
cd "$(dirname "$0")/.."

echo "==> Authenticating with ECR..."
aws ecr get-login-password --region "${REGION}" | \
    docker login --username AWS --password-stdin "${ECR_REGISTRY}"

# ---------- Processing image (steps 1 & 3: limpieza + reglas de negocio) ----------
echo ""
echo "==> Building processing image..."
docker build -f docker/Dockerfile.processing -t "${PROCESSING_REPO}:${TAG}" .
docker tag "${PROCESSING_REPO}:${TAG}" "${ECR_REGISTRY}/${PROCESSING_REPO}:${TAG}"
docker push "${ECR_REGISTRY}/${PROCESSING_REPO}:${TAG}"
echo "Pushed: ${ECR_REGISTRY}/${PROCESSING_REPO}:${TAG}"

# ---------- Spark image (step 2: modelado ALS) ----------
echo ""
echo "==> Building spark image..."
docker build -f docker/Dockerfile.spark -t "${SPARK_REPO}:${TAG}" .
docker tag "${SPARK_REPO}:${TAG}" "${ECR_REGISTRY}/${SPARK_REPO}:${TAG}"
docker push "${ECR_REGISTRY}/${SPARK_REPO}:${TAG}"
echo "Pushed: ${ECR_REGISTRY}/${SPARK_REPO}:${TAG}"

echo ""
echo "Done. Use these URIs in the orchestrator (auto-resolved via account ID):"
echo "  ECR_PROCESSING_IMAGE = ${ECR_REGISTRY}/${PROCESSING_REPO}:${TAG}"
echo "  ECR_SPARK_IMAGE      = ${ECR_REGISTRY}/${SPARK_REPO}:${TAG}"
