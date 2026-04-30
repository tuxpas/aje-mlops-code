# AjeDevPsInfraStack — Template Explained

Synthesized file: `infra/cdk.out/AjeDevPsInfraStack.template.json`
Source CDK: `infra/infra/infra_stack.py`

---

## 1. Summary

This stack provisions the runtime + CI/CD plumbing for AJE's **Pedido Sugerido** SageMaker ML pipeline in the `dev` environment. At runtime, seven EventBridge cron rules (one per country: CR, EC, GT, MX, NI, PA, PE) fire daily at 11:00 UTC and invoke a Lambda that starts the SageMaker pipeline. On every push to GitLab `main`, a CodePipeline rebuilds the Docker image (CodeBuild → ECR) and redeploys the SageMaker pipeline definition. CloudWatch alarms + an SNS topic handle failure notifications, and a DynamoDB table holds per-country config.

**Services involved:** Lambda, EventBridge, DynamoDB, CodePipeline, CodeBuild, CodeStar Connections (GitLab), ECR, SageMaker (referenced, not created here), S3, SNS, CloudWatch, IAM.

---

## 2. Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           RUNTIME (daily cron)                             │
│                                                                            │
│   ┌──────────────┐   cron(0 11 ? * *)    ┌───────────────────────────┐     │
│   │ EventBridge  │ ─────────────────────▶│ Lambda                    │     │
│   │ 7 rules      │   {code_country: "X"} │ aje-dev-ps-               │     │
│   │ CR EC GT MX  │                       │ triggerfunction-lambda    │─┐   │
│   │ NI PA PE     │                       └───────────────────────────┘ │   │
│   └──────────────┘                                                     │   │
│                                                                        ▼   │
│                                         ┌───────────────────────────────┐  │
│                                         │ SageMaker Pipeline            │  │
│                                         │ aje-dev-ps-pipeline           │  │
│                                         │ (deployed via CI/CD below)    │  │
│                                         └───────────────────────────────┘  │
│                                                       │                    │
│                                                       ▼ emits metrics      │
│   ┌──────────────────────────────────────────────────────────────────┐     │
│   │ CloudWatch:  ExecutionsFailed / ExecutionsStopped                │     │
│   │   Alarms ─▶ SNS topic  aje-dev-ps-alert-topic-sns                │     │
│   │   Dashboard aje-dev-ps-monitoringdashboard-cloudwatch            │     │
│   └──────────────────────────────────────────────────────────────────┘     │
│                                                                            │
│   ┌────────────────────────────────────────────────────┐                   │
│   │ DynamoDB  aje-dev-ps-configtable-dynamodb          │                   │
│   │ PK: code_country (String)                          │                   │
│   └────────────────────────────────────────────────────┘                   │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│                       CI/CD (on push to GitLab main)                       │
│                                                                            │
│   GitLab ──(CodeStar connection)──▶ CodePipeline                           │
│                                     aje-dev-ps-pipeline-codepipeline       │
│                                       role: pipelinerole-iam               │
│                                         │                                  │
│             ┌───────────────────────────┼───────────────────────────┐      │
│             ▼ assume                    ▼ assume                    ▼ assume
│   ┌─────────────────┐         ┌──────────────────┐        ┌──────────────┐ │
│   │ Source          │────────▶│ Build            │───────▶│ Deploy       │ │
│   │ GitLab          │         │ CodeBuild        │        │ CodeBuild    │ │
│   │ branch: main    │         │ Docker → ECR     │        │ upsert SM    │ │
│   │ sourceaction-   │         │ buildaction-     │        │ deployaction-│ │
│   │  role-iam       │         │  role-iam        │        │  role-iam    │ │
│   └─────────────────┘         └──────────────────┘        └──────────────┘ │
│                                                                            │
│   Artifact store: S3  aje-dev-ps-codepipelinebucket-s3                     │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Template Walk-through

### 3.1 Artifact Bucket
- **`AjeDevPsCodePipelineBucketS3`** — explicit-named S3 bucket for CodePipeline artifacts. `DeletionPolicy: Retain`, so the bucket survives stack deletion.

### 3.2 Lambda block
- **`AjeDevPsLambdaPolicyIam`** — explicit inline policy with two statements:
  1. `sagemaker:StartPipelineExecution` on `arn:aws:sagemaker:us-east-2:832257724409:pipeline/aje-dev-ps-pipeline`.
  2. `logs:CreateLogGroup | CreateLogStream | PutLogEvents` on `*` (replaces the AWS-managed `AWSLambdaBasicExecutionRole`).
- **`AjeDevPsLambdaRoleIam`** — role shell, trust = `lambda.amazonaws.com`.
- **`AjeDevPsTriggerFunctionLambda`** — Python 3.12 function, 30s timeout, env vars `code_country=EC` + `stage=dev`. Code comes from a CDK asset bucket.
- **`AjeDevPsTriggerFunctionLambdaLogGroup`** — ⚡ *auto-generated* by CDK (you didn't write it). `RetentionInDays: 731`, `DeletionPolicy: Retain`.

### 3.3 EventBridge rules (7 countries)
Each country gets:
- **`AjeDevPsScheduleRule<XX>EventBridge`** — `cron(0 11 ? * * *)`, target = the Lambda with `Input: {"code_country":"XX"}`.
- **`…AllowEventRule…`** — *auto-generated* `AWS::Lambda::Permission` granting `events.amazonaws.com` `lambda:InvokeFunction`. This is the resource-based policy on the Lambda; CDK synthesizes it silently from `rule.add_target(targets.LambdaFunction(fn))`.

### 3.4 DynamoDB
- **`AjeDevPsConfigTableDynamoDB`** — PK `code_country` (String), `PAY_PER_REQUEST`, `DeletionPolicy: Delete` (⚠️ stack deletion drops the table + data).

### 3.5 CodeBuild — Build project
- **`AjeDevPsBuildPolicyIam`** — your explicit inline policy (ECR push/pull, logs, S3, STS).
- **`AjeDevPsBuildRoleIam`** — role shell, trust = `codebuild.amazonaws.com`.
- **`AjeDevPsBuildRoleIamDefaultPolicy…`** — ⚡ *auto-generated DefaultPolicy* CDK attaches because the role is used by a `codebuild.Project` inside a CodePipeline. It grants CDK asset bucket read, CodeBuild log-group writes, report-group writes, and S3 artifact-bucket read/write. **Without this, the build would fail.**
- **`AjeDevPsBuildProjectCodeBuildA41BF858`** — Linux standard:7.0 image, privileged mode (Docker), `EncryptionKey: alias/aws/s3`.

### 3.6 CodeBuild — Deploy project
- **`AjeDevPsDeployPolicyIam`** — your explicit inline policy (SageMaker CRUD + `iam:PassRole` + logs + S3 + STS).
- **`AjeDevPsDeployRoleIam`** — role shell.
- **`AjeDevPsDeployRoleIamDefaultPolicy…`** — ⚡ auto-generated, same purpose as the build one.
- **`AjeDevPsDeployProjectCodeBuild50707E29`** — Linux standard:7.0, non-privileged, env vars `EJECUTAR=false`, `PAIS=""`.

### 3.7 CodePipeline block — two-level role structure
- **`AjeDevPsPipelinePolicyIam`** — your explicit inline policy (4 statements: S3 artifact I/O scoped to `aje-dev*`, `codestar-connections:UseConnection` on the real connection ARN, `codebuild:StartBuild` on the two projects, `iam:PassRole` on `aje-dev-ps-*`).
- **`AjeDevPsPipelineRoleIam`** — role shell, trust = `codepipeline.amazonaws.com`.
- **`AjeDevPsPipelineRoleIamDefaultPolicy…`** — ⚡ auto-generated, grants artifact-bucket write + `sts:AssumeRole` into the three per-action roles.
- **Per-action roles (explicit names, no explicit policy)** — each trusts the pipeline role via `ArnPrincipal`, with its permissions coming from a CDK-auto-generated `DefaultPolicy`:
  - **Source**: `AjeDevPsSourceActionRoleIam` (`aje-dev-ps-sourceactionrole-iam`) + `DefaultPolicy` granting `codestar-connections:UseConnection` on the connection ARN + S3 artifact bucket I/O.
  - **Build**: `AjeDevPsBuildActionRoleIam` (`aje-dev-ps-buildactionrole-iam`) + `DefaultPolicy` granting `codebuild:StartBuild/Stop/BatchGet` on the build project.
  - **Deploy**: `AjeDevPsDeployActionRoleIam` (`aje-dev-ps-deployactionrole-iam`) + `DefaultPolicy` granting the same three CodeBuild actions on the deploy project.
- **`AjeDevPsPipelineCodePipeline`** — `PipelineType: V2`, 3 stages (Source, Build, Deploy), `RoleArn` points to your explicit pipeline role, each action's `RoleArn` points to its explicit action role. Artifact store is the explicit `AjeDevPsCodePipelineBucketS3` (not a CDK-generated one). The `ConnectionArn` is now populated with a real GitLab connection ARN.

### 3.8 Observability
- **`AjeDevPsAlertTopicSns`** — SNS topic (no subscriptions defined — add via console or CDK).
- **`AjeDevPsExecutionFailedAlarmCloudWatch`** — alarm on `ExecutionsFailed ≥ 1` per hour, fires both `AlarmActions` and `OKActions` to SNS.
- **`AjeDevPsExecutionStoppedAlarmCloudWatch`** — alarm on `ExecutionsStopped ≥ 1` per hour, fires `AlarmActions` only.
- **`AjeDevPsMonitoringDashboardCloudWatch`** — dashboard with a time-series graph (Succeeded/Failed/Stopped) and two alarm widgets.

---

## 4. CDK → CloudFormation Mapping

| CDK construct (Python) | CloudFormation resource(s) produced |
|---|---|
| `iam.Policy(policy_name=..., statements=[...])` | 1× `AWS::IAM::Policy` (inline — materialized via `Roles:` list ref) |
| `iam.Role(role_name=..., assumed_by=...)` | 1× `AWS::IAM::Role` (trust policy only) |
| `_lambda.Function(...)` | `AWS::Lambda::Function` + auto `AWS::Logs::LogGroup` |
| `events.Rule(...).add_target(LambdaFunction(fn))` | `AWS::Events::Rule` + auto `AWS::Lambda::Permission` |
| `dynamodb.Table(...)` | `AWS::DynamoDB::Table` |
| `codebuild.Project(role=...)` in a pipeline | `AWS::CodeBuild::Project` + **auto `AWS::IAM::Policy` DefaultPolicy on the role** |
| `codepipeline.Pipeline(role=..., stages=[...])` | `AWS::CodePipeline::Pipeline` + **1 auto DefaultPolicy on pipeline role** |
| Action with explicit `role=iam.Role(...)` | Uses your role directly + **auto DefaultPolicy attached to that role** |
| `sns.Topic(...)` | `AWS::SNS::Topic` |
| `cloudwatch.Alarm(...).add_alarm_action(cw_actions.SnsAction(topic))` | `AWS::CloudWatch::Alarm` with inline `AlarmActions` referencing SNS |
| `cloudwatch.Dashboard(...).add_widgets(...)` | `AWS::CloudWatch::Dashboard` with a `DashboardBody` JSON string |

**Scaffolding ratio:** ~140 lines of Python CDK → ~1580 lines of CloudFormation. L2 constructs still emit considerable IAM wiring (each `iam.Role` used by CodeBuild/CodePipeline accumulates an additional `DefaultPolicy`).

---

## 5. Gotchas

### ✅ 5.1 `ConnectionArn` populated
The GitLab connection ARN is now hardcoded into the synthesized template:
```
arn:aws:codeconnections:us-east-2:832257724409:connection/e6c4eaf1-9fad-43fb-b697-2d57cee65ff0
```
It appears in three places (the pipeline's Source action configuration, the pipeline policy's `codestar-connections:UseConnection` statement, and the Source action role's DefaultPolicy). That's the real connection wired in via `--context gitlab_connection_arn=...` at synth time.

⚠️ **Secret-in-template caveat:** connection ARNs aren't secrets per se (you can't use them without the connection being authorized), but they're infrastructure identifiers. If this template is committed to git and you ever rotate the connection, you'll need to re-synth and redeploy.

### 🪤 5.2 Hidden DefaultPolicies do the work for action roles
The three action roles now have **no explicit policy attached** — they rely entirely on CDK's auto-generated DefaultPolicy. The template contains six DefaultPolicies total:
- `AjeDevPsBuildRoleIamDefaultPolicy23DE4BE6` — attached to the CodeBuild build service role
- `AjeDevPsDeployRoleIamDefaultPolicyB2BFB9E6` — attached to the CodeBuild deploy service role
- `AjeDevPsPipelineRoleIamDefaultPolicy47F8E9AE` — attached to the pipeline role
- `AjeDevPsSourceActionRoleIamDefaultPolicyDC9D1C1A` — attached to the Source action role
- `AjeDevPsBuildActionRoleIamDefaultPolicy1714B774` — attached to the Build action role
- `AjeDevPsDeployActionRoleIamDefaultPolicyD208CFD8` — attached to the Deploy action role

These logical IDs don't start with `aje-dev` — but they're **inline** policies authorized via `iam:PutRolePolicy` against the *role ARN*, so your `iam:PutRolePolicy` permission on `aje-dev*` role ARNs still covers them. No action needed.

### 💥 5.3 Explicit role names break CloudFormation replacement
Every IAM role here has a literal `RoleName`. If you ever change a property that requires replacement (e.g., trust policy), CloudFormation can't create the new role because the old name is still in use. You'd have to manually delete the role first.

### 🪵 5.4 Missing log-group retention on CodeBuild
The CodeBuild DefaultPolicies grant writes to `/aws/codebuild/<project>` log groups, but no `AWS::Logs::LogGroup` is declared for them. CodeBuild creates them implicitly with **infinite retention**.

### 🗑️ 5.5 DynamoDB `DeletionPolicy: Delete`
`AjeDevPsConfigTableDynamoDB` has `DeletionPolicy: Delete` and `UpdateReplacePolicy: Delete`. If the stack is destroyed or the table needs replacement, all country config is lost. Consider `RemovalPolicy.RETAIN` on the CDK construct for anything non-ephemeral.

### 🏷️ 5.6 EventBridge `code_country` overrides Lambda env var
The Lambda has env var `code_country=EC`, but every EventBridge rule passes an `Input` transformer with `{"code_country": "<XX>"}`. The event payload wins — the env var is only the default for invocations that don't come through EventBridge.

### 📜 5.7 CodeBuild project artifacts `NO_ARTIFACTS`
Both CodeBuild projects declare `Artifacts: { Type: "NO_ARTIFACTS" }` in their own resource. In a CodePipeline context, the *pipeline* overrides this at runtime via the action's `InputArtifacts`/`OutputArtifacts`, so the buildspec can still produce outputs. This is normal CDK behavior for `codebuild.Project` when driven by CodePipeline, but surprising if you read the CodeBuild resource in isolation.

### 🔗 5.8 Pipeline-level `codestar-connections:UseConnection` is redundant
The pipeline role's explicit policy (`AjeDevPsPipelinePolicyIam`) grants `codestar-connections:UseConnection` on the connection ARN — but the pipeline role doesn't actually invoke the source action directly. The Source *action role* does (via its DefaultPolicy). The pipeline role statement is harmless but unused in practice. Could be removed if you want to declutter.

---

## 6. Resources Table

| Logical ID | Type | Name / Identifier | Key Properties |
|---|---|---|---|
| `AjeDevPsCodePipelineBucketS3C54C17FD` | `AWS::S3::Bucket` | `aje-dev-ps-codepipelinebucket-s3` | `DeletionPolicy: Retain` |
| `AjeDevPsLambdaPolicyIam3AD28B68` | `AWS::IAM::Policy` | `aje-dev-ps-lambdapolicy-iam` | Inline; grants `sagemaker:StartPipelineExecution` + CloudWatch logs |
| `AjeDevPsLambdaRoleIam6A69D591` | `AWS::IAM::Role` | `aje-dev-ps-lambdarole-iam` | Trust: `lambda.amazonaws.com` |
| `AjeDevPsTriggerFunctionLambdaF027E69C` | `AWS::Lambda::Function` | `aje-dev-ps-triggerfunction-lambda` | Python 3.12, 30s timeout, env: `code_country=EC`, `stage=dev` |
| `AjeDevPsTriggerFunctionLambdaLogGroup029047F8` | `AWS::Logs::LogGroup` | `/aws/lambda/aje-dev-ps-triggerfunction-lambda` | Retention 731 days, `DeletionPolicy: Retain` (auto-generated) |
| `AjeDevPsScheduleRule{CR,EC,GT,MX,NI,PA,PE}EventBridge…` | `AWS::Events::Rule` (×7) | `aje-dev-ps-schedulerule-<CC>-eventbridge` | `cron(0 11 ? * * *)`, target = Lambda with `{"code_country":"<CC>"}` |
| `AjeDevPsScheduleRule<CC>EventBridgeAllowEventRule…` | `AWS::Lambda::Permission` (×7) | — | `events.amazonaws.com` → `lambda:InvokeFunction` (auto-generated) |
| `AjeDevPsConfigTableDynamoDBC3648322` | `AWS::DynamoDB::Table` | `aje-dev-ps-configtable-dynamodb` | PK: `code_country` (S), `PAY_PER_REQUEST`, `DeletionPolicy: Delete` |
| `AjeDevPsBuildPolicyIam2F371F39` | `AWS::IAM::Policy` | `aje-dev-ps-buildpolicy-iam` | Inline; ECR push/pull, logs, S3 objects, STS |
| `AjeDevPsBuildRoleIam2AE14257` | `AWS::IAM::Role` | `aje-dev-ps-buildrole-iam` | Trust: `codebuild.amazonaws.com` |
| `AjeDevPsBuildRoleIamDefaultPolicy23DE4BE6` | `AWS::IAM::Policy` | `AjeDevPsBuildRoleIamDefaultPolicy23DE4BE6` | Auto-generated; CDK assets bucket, CodeBuild logs, report groups, artifact bucket |
| `AjeDevPsBuildProjectCodeBuildA41BF858` | `AWS::CodeBuild::Project` | `aje-dev-ps-buildproject-codebuild` | `standard:7.0`, `PrivilegedMode: true` |
| `AjeDevPsDeployPolicyIam31A5B468` | `AWS::IAM::Policy` | `aje-dev-ps-deploypolicy-iam` | Inline; SageMaker CRUD, `iam:PassRole`, logs, S3 |
| `AjeDevPsDeployRoleIamAA4D9CD7` | `AWS::IAM::Role` | `aje-dev-ps-deployrole-iam` | Trust: `codebuild.amazonaws.com` |
| `AjeDevPsDeployRoleIamDefaultPolicyB2BFB9E6` | `AWS::IAM::Policy` | `AjeDevPsDeployRoleIamDefaultPolicyB2BFB9E6` | Auto-generated; same shape as build default policy |
| `AjeDevPsDeployProjectCodeBuild50707E29` | `AWS::CodeBuild::Project` | `aje-dev-ps-deployproject-codebuild` | `standard:7.0`, `PrivilegedMode: false`, env: `EJECUTAR=false`, `PAIS=""` |
| `AjeDevPsPipelinePolicyIam37B40FFC` | `AWS::IAM::Policy` | `aje-dev-ps-pipelinepolicy-iam` | Inline; S3 (`aje-dev*`), `codestar-connections:UseConnection` (connection ARN), `codebuild:StartBuild`, `iam:PassRole` (`aje-dev-ps-*`) |
| `AjeDevPsPipelineRoleIam193864ED` | `AWS::IAM::Role` | `aje-dev-ps-pipelinerole-iam` | Trust: `codepipeline.amazonaws.com` |
| `AjeDevPsPipelineRoleIamDefaultPolicy47F8E9AE` | `AWS::IAM::Policy` | `AjeDevPsPipelineRoleIamDefaultPolicy47F8E9AE` | Auto-generated; artifact bucket + `sts:AssumeRole` to 3 action roles |
| `AjeDevPsSourceActionRoleIam84266BD7` | `AWS::IAM::Role` | `aje-dev-ps-sourceactionrole-iam` | Trust: pipeline role ARN (ArnPrincipal) |
| `AjeDevPsSourceActionRoleIamDefaultPolicyDC9D1C1A` | `AWS::IAM::Policy` | `AjeDevPsSourceActionRoleIamDefaultPolicyDC9D1C1A` | Auto-generated; `codestar-connections:UseConnection` (connection ARN) + artifact bucket I/O |
| `AjeDevPsBuildActionRoleIamF484CB8E` | `AWS::IAM::Role` | `aje-dev-ps-buildactionrole-iam` | Trust: pipeline role ARN |
| `AjeDevPsBuildActionRoleIamDefaultPolicy1714B774` | `AWS::IAM::Policy` | `AjeDevPsBuildActionRoleIamDefaultPolicy1714B774` | Auto-generated; `codebuild:StartBuild/Stop/BatchGet` on build project |
| `AjeDevPsDeployActionRoleIamE765A304` | `AWS::IAM::Role` | `aje-dev-ps-deployactionrole-iam` | Trust: pipeline role ARN |
| `AjeDevPsDeployActionRoleIamDefaultPolicyD208CFD8` | `AWS::IAM::Policy` | `AjeDevPsDeployActionRoleIamDefaultPolicyD208CFD8` | Auto-generated; `codebuild:StartBuild/Stop/BatchGet` on deploy project |
| `AjeDevPsPipelineCodePipelineA565D2D1` | `AWS::CodePipeline::Pipeline` | `aje-dev-ps-pipeline-codepipeline` | `PipelineType: V2`, 3 stages (Source/Build/Deploy), explicit artifact bucket, real `ConnectionArn` |
| `AjeDevPsAlertTopicSns70C78523` | `AWS::SNS::Topic` | `aje-dev-ps-alert-topic-sns` | Display: "AJE Pedido Sugerido – Pipeline Alerts" |
| `AjeDevPsExecutionFailedAlarmCloudWatchB964D9BD` | `AWS::CloudWatch::Alarm` | `aje-dev-ps-executionfailedalarm-cloudwatch` | `ExecutionsFailed ≥ 1`, period 3600s, alarm + OK → SNS |
| `AjeDevPsExecutionStoppedAlarmCloudWatch26ABDF4D` | `AWS::CloudWatch::Alarm` | `aje-dev-ps-executionstoppedalarm-cloudwatch` | `ExecutionsStopped ≥ 1`, period 3600s, alarm → SNS |
| `AjeDevPsMonitoringDashboardCloudWatch80D69C0E` | `AWS::CloudWatch::Dashboard` | `aje-dev-ps-monitoringdashboard-cloudwatch` | Graph + 2 alarm widgets |
| `CDKMetadata` | `AWS::CDK::Metadata` | — | Analytics payload |

**Resource count:** 33 (7 IAM Roles, 10 IAM Policies [4 explicit + 6 DefaultPolicy], 7 EventBridge Rules, 7 Lambda Permissions, 1 Lambda Function, 1 Log Group, 1 DynamoDB Table, 2 CodeBuild Projects, 1 CodePipeline Pipeline, 1 S3 Bucket, 1 SNS Topic, 2 CloudWatch Alarms, 1 CloudWatch Dashboard, 1 CDK Metadata).

**Convention check:** ✅ Every `AWS::IAM::Role` has a literal `RoleName` matching `aje-dev-ps-*`. Your `permisos.json` restriction on `iam:CreateRole → arn:aws:iam::832257724409:role/aje-dev*` accepts all seven role creations. The six `DefaultPolicy` resources have CDK-generated names but are inline, so they're covered by `iam:PutRolePolicy` on the compliant role ARNs. The four explicitly-named policies (`lambdapolicy`, `buildpolicy`, `deploypolicy`, `pipelinepolicy`) all match `aje-dev-ps-*`.

---

## 7. Real AWS Usage Notes

### When this architecture fits
- A recurring ML pipeline that must be triggered on a schedule for multiple tenants (countries), with centralized deploy via GitLab → AWS.
- Dev-scale traffic: 7 invocations per day + occasional deploys. Not designed for high-throughput / on-demand invocation.

### Scalability
- **Lambda + EventBridge** scale trivially for this workload (7 invocations/day).
- **CodePipeline V2** supports parallel actions if you later fan out by country; current config is sequential.
- **SageMaker pipeline** (the actual workhorse) is outside this stack — that's where scaling decisions live (instance type, parallelism).

### Cost
- **EventBridge**: effectively free (first 14M events/month free).
- **Lambda**: 7 × 30s × 128MB / day → cents per month.
- **DynamoDB on-demand**: billed per request, negligible for a config table.
- **CodePipeline V2**: ~$1/month per active pipeline (V2 is billed differently than V1 — V1 was ~$1/month flat, V2 is per-action-execution).
- **CodeBuild**: ~$0.005/min for `BUILD_GENERAL1_SMALL` Linux. Docker builds with privileged mode use the same small compute.
- **CloudWatch**: log ingestion is the dominant variable cost. Set retention aggressively on CodeBuild log groups (currently unlimited — gotcha #5.4).
- **SageMaker** (not in this stack): the real cost driver.

### Security
- **Stage isolation by naming:** `aje-{stage}-*` ensures dev's IAM user can't accidentally touch prod. Prod deploys need a separate IAM identity.
- **Least-privilege action roles:** each CodePipeline action has a dedicated role scoped to a single CodeBuild project. The Source role is scoped to the specific GitLab connection ARN.
- **Wildcard resources** on ECR (`ecr:*` → `*`) and SageMaker (`sagemaker:*` → `*`) inside the CodeBuild service roles remain broad — acceptable for dev, tighten for prod (specific repo ARNs, specific pipeline ARN).
- **`iam:PassRole` on `aje-dev-ps-*`** in the pipeline policy is better than `*` but could add a `"iam:PassedToService": "codebuild.amazonaws.com"` condition for defense in depth.
- **Privileged CodeBuild** (for Docker-in-Docker) is intentional but means the build container has root on its host. Keep the buildspec and source repo access tightly controlled.
- **SNS topic has no subscriptions** — failure alarms fire silently until you subscribe an email/Slack/PagerDuty.
- **No KMS** — the pipeline artifact bucket uses `alias/aws/s3` (AWS-managed key). If compliance requires CMK, add one and grant the pipeline/build/action roles `kms:Decrypt` + `kms:GenerateDataKey`.

---

*Generated from `infra/cdk.out/AjeDevPsInfraStack.template.json` on 2026-04-30.*
