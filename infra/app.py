#!/usr/bin/env python3
import os

import aws_cdk as cdk

from infra.infra_stack import AjePsInfraStack


app = cdk.App()

stage = app.node.try_get_context("stage") or "dev"
if stage not in ("dev", "test", "prod"):
    raise ValueError(f"Invalid env '{stage}'. Must be one of: dev, test, prod")

region = app.node.try_get_context("region") or "us-east-2"
account = app.node.try_get_context("account") or "832257724409"

AjePsInfraStack(
    app, 
    f"Aje{stage.capitalize()}PsInfraStack",
    stage=stage,
    env=cdk.Environment(account=account, region=region)
)

app.synth()
