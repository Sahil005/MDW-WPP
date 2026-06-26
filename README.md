
<!-- ![assets/acceleration-logo.png](assets/acceleration-logo.png) -->

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/acceleration-logo.png">
    <img src="assets/acceleration-logo.png" alt="Acceleration">
  </picture>
</p>

# Media Data Warehouse

This repository provides an end-to-end data processing architecture supplying digital and offline media data to support a variety of marketing use-cases:
* Media Mix Modelling
* Value Based Bidding
* Other Marketing Analytics Workloads

It provisions all cloud infrastructure required to support data ingestion, processing and distribution.

## Project structure

The project is structured as follows:

```
├── infrastructure          # Terraform configuration for provisioning cloud infrastructure.
├── assets                  # Images/diagrams for technical docs
└── source_code             # Main source of the project.
    ├── scripts             # Scripts for environment configuration etc.
    ├── sql                 # SQL code executed in ELT pipelines.
    │   ├── procedures      # stored procedure definitions.
    │   └── views           # view definitions.
    └── airflow             # source code for airflow DAGs and operators for workflow orchestration.
```

## Running the project

### Requirements For Local Development

All infrastructure is provisioned using terraform, and all workloads are deployed using GitHub Actions as a CI/CD tool. So while we don't generally execute code locally on our machines, a few tools are required on the local machine for testing beforehand:

1. VSCode - Local development environment
2. Docker Desktop

Follow these setps to configure your environment:

1. Open the repository in VS Code.
2. We will work inside a docker container using the devcontainer plugin. Use the plugin to reopen the repo in your docker container, which has all the dependencies pre-installed.
3. Once you are in the dev container, set up your gcloud sdk by running the following and follow the prompts:

```bash
gcloud init
```

To set your application default credentials (required for Terraform), run the following and follow the prompts:

```bash
gcloud auth application-default login
```

4. To make infrastructure changes, navigate into the infrastructure directory and intialize terraform. Before committing any changes, run a terraform plan and pre-commit.

```bash
cd infrastructure
...
terraform init --backend-config=environments/dev.tfbackend
terraform plan --var-file=environments/dev.tfvars
```

### Working in GCP
In general, data processing will be orchestrated using [Cloud Composer](modules/cloud_composer), Dataform, or a combination of the two.

Resources are created/destroyed by GitHub Actions in `dev` and `prod` environments, see below for further details. In general, you will deploy features to GCP via a CI/CD pipeline, although some local development may be required for authoring Airflow pipelines, for which a local Airflow environment is provided.

If working on Airflow/Cloud Composer, you may want to test your DAGs in a local environment. This is scripted for you.

```bash
cd airflow
source start_local_airflow_server.sh
```

This script will spin up a local server running on localhost. You can log in as admin using the password stored in `standalone_admin_password.txt`

Once you're happy that your code is error free (you may not be able to fully test it), you can test in the dev Composer environment by merging your branch into `dev`

```bash
git checkout dev
git merge feat/my-new-dag
git push
```

### Git Workflow
This repo has two long-lived branches: `dev` and `main` (This is the prod branch).

When making infrastructure changes, create a feature branch with a descriptive name e.g. `feat/example-feature`. Test your feature by merging it into the `dev` branch. This will trigger a GitHub Actions workflow to deploy your change into the dev gcp project. Once tested, create a pull request into the prod branch using the pull request template supplied. Once it passes peer review, it will be merged into the prod environment and provisioned using the same workflow in prod.

Some useful examples:
```bash
# clone this repo:
git clone git@github.com:groupm-global/accel_gcp_mdw.git

# Checkout the prod branch:
git checkout main

# Make sure you are up to date with the latest changes:
git pull

# Create a feature branch:
git checkout -b feat/example-feature

# ... make changes and stage them
git add .

# ... pre-commit lints code and checkov flags any antipatterns or vulnerabilities
pre-commit

# Once pre-commit hooks pass, commit your changes.
git commit -m 'feat(bigquery): add stored procedure'
git push

# test in dev:
git checkout dev
git pull
git merge feat/example-feature
git push
```
