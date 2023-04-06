##################### Initializing | needs run once at project start

# Initializing Git
initialize_git:
	@echo "Initializing git..."
	git init

# Initializing DVC, assumes a .gitignore
initialize_dvc:
	@echo "Initializing dvc..."
	dvc init
	dvc add data
	git add data.dvc

# Installing Poetry
install: 
	@echo "Installing..."
	poetry install
	poetry run pre-commit install

# Initializing gcloud
initialize_gcloud:
	@echo "Authenticating with Google Cloud..."
	gcloud auth application-default login

##################### Startup | needs run everytime you open the Command line

# Activate Virtual Env
activate:
	@echo "Activating virtual environment"
	poetry shell

##################### Testing functions | needs to run before committing new functions

test:
	pytest

##################### Bigquery

create_tables:
	@echo "Creating Bigquery Tables..."
	python src/create_bigquery_tables.py

##################### Prefect

prefect_authenticate:
	@echo "Authenticating with Prefect Cloud..."	
	prefect cloud login

pipeline/request:
	@echo "Requesting API data..."
	python src/request_google_jobs.py

pipeline/split:
	@echo "Splitting GCS File into multiple ones..."
	python src/split_gcs_files.py

pipeline/gcs_to_bq:
	@echo "Loading GCS files into Bigquery..."
	python src/gcs_to_bigquery.py

pipeline/final_bq:
	@echo "Creating final Bigquery Tables..."
	python src/load_final_bigquery_tables.py

pipeline:
	@echo "Running full pipeline..."
	python src/request_google_jobs.py
	python src/split_gcs_files.py
	python src/gcs_to_bigquery.py

##################### Deployment

deployment:
	poetry export -o "requirements.txt" --without-hashes --without-urls
	docker build -t europe-west3-docker.pkg.dev/ecommerce-web-analysis/data-job-pipeline/my-image:2.7.6-python3.9 .
	docker push europe-west3-docker.pkg.dev/ecommerce-web-analysis/data-job-pipeline/my-image:2.7.6-python3.9
	prefect deployment build -n "Split GCS File into multiple" -sb gcs/data-job-pipeline -ib cloud-run-job/data-job-instance src/split_gcs_files.py:split_gcs_files_flow -q default -a -o "deployments/split_gcs_files_deployment.yaml"

##################### Documentation

docs_view:
	@echo View API documentation... 
	PYTHONPATH=src pdoc src --http localhost:8080

docs_save:
	@echo Save documentation to docs... 
	PYTHONPATH=src pdoc src -o docs

##################### Clean up

# Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache