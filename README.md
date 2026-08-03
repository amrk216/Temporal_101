# Temporal 101 Course

Hands-on Python examples for learning Temporal IO through a small progression of projects:

- a plain Python PDF pipeline
- the same PDF pipeline implemented with Temporal workflows and activities
- a larger AI contract review workflow with fan-out child workflows and human-in-the-loop review

Course video:

- [Temporal 101 course on YouTube](https://www.youtube.com/watch?v=-Zib7r3HxyY)

## What this repository covers

This project is structured as a teaching repo, not a single production application. Each app demonstrates a different part of the Temporal model.

- `apps/pdf-extraction-01-plain`: baseline Python script that downloads a PDF from S3, extracts markdown, and uploads the result back to S3.
- `apps/pdf-extraction-02-temporal`: the same pipeline expressed as a Temporal workflow with three activities: download, extract, upload.
- `apps/client-app`: FastAPI client that starts workflows, queries status, and interacts with the contract-review workflow.
- `apps/ai-contract-review`: advanced Temporal example using parent/child workflows, LLM calls, and human review via queries, signals, and updates.
- `setup/samples-server`: local Temporal server setup using Docker Compose.

Architecture notes live here:

- [architecture-diagrams.md](architecture-diagrams.md)
- [ai-agent-diagrams.md](ai-agent-diagrams.md)

## Repository layout

```text
.
├── apps/
│   ├── ai-contract-review/
│   ├── client-app/
│   ├── pdf-extraction-01-plain/
│   └── pdf-extraction-02-temporal/
├── setup/
│   ├── samples-server/
│   └── services/
├── architecture-diagrams.md
└── ai-agent-diagrams.md
```

## Prerequisites

You will need:

- Python 3.11+
- Docker and Docker Compose
- Access to an S3-compatible bucket
- An OpenRouter API key for the AI contract review example

The Python apps use these main libraries:

- `temporalio`
- `fastapi`
- `uvicorn`
- `boto3`
- `pymupdf4llm`
- `openai`

## Environment files

Each app now includes a matching `.env.example` file. Copy the example file to `.env` before running that app.

Examples:

```bash
cp apps/pdf-extraction-01-plain/.env.example apps/pdf-extraction-01-plain/.env
cp apps/pdf-extraction-02-temporal/.env.example apps/pdf-extraction-02-temporal/.env
cp apps/client-app/.env.example apps/client-app/.env
cp apps/ai-contract-review/.env.example apps/ai-contract-review/.env
cp setup/samples-server/compose/.env.example setup/samples-server/compose/.env
```

## Quick start

### 1. Start Temporal locally

This repo includes the Temporal samples-server compose setup under `setup/samples-server/compose`.

```bash
cd setup/samples-server/compose
docker compose -f docker-compose-postgres.yml up -d
```

Useful local endpoints:

- Temporal Frontend: `localhost:7233`
- Temporal Web UI: `http://localhost:8080`

### 2. Install Python dependencies

Create a virtual environment if you want, then install dependencies per app.

```bash
cd apps/pdf-extraction-01-plain && pip install -r requirements.txt
cd ../pdf-extraction-02-temporal && pip install -r requirements.txt
cd ../client-app && pip install -r requirements.txt
cd ../ai-contract-review && pip install -r requirements.txt
```

### 3. Fill in the `.env` files

At minimum, configure:

- AWS or S3-compatible credentials
- `AWS_S3_ENDPOINT_URL`
- `S3_BUCKET`
- `TEMPORAL_HOST`
- `TEMPORAL_NAMESPACE`
- task queue names
- `OPENROUTER_API_KEY` for the AI workflow

## Running the examples

### Example 1: Plain PDF extraction

This is the non-Temporal baseline.

```bash
cd apps/pdf-extraction-01-plain
python process_pdf.py s3://your-bucket/path/to/file.pdf
```

What it does:

1. Downloads a PDF from S3.
2. Extracts markdown with `pymupdf4llm`.
3. Uploads the generated `.md` file back to S3.

### Example 2: Temporal PDF extraction

Start the worker:

```bash
cd apps/pdf-extraction-02-temporal
python worker.py
```

Start the API client in another terminal:

```bash
cd apps/client-app
uvicorn main:app --reload --port 5000
```

Run the workflow synchronously:

```bash
curl -X POST http://localhost:5000/process-pdf/execute \
	-H "Content-Type: application/json" \
	-d '{"s3_path": "s3://temporal-dev/files/cisco-88xx-user-guide.pdf"}'
```

Start the workflow asynchronously:

```bash
curl -X POST http://localhost:5000/process-pdf/start \
	-H "Content-Type: application/json" \
	-d '{"s3_path": "s3://temporal-dev/files/cisco-88xx-user-guide.pdf"}'
```

Check workflow status:

```bash
curl http://localhost:5000/workflow/status/<workflow-id>
```

### Example 3: AI contract review with Temporal

Start the contract-review worker:

```bash
cd apps/ai-contract-review
python worker.py
```

If the client API is not already running, start it:

```bash
cd apps/client-app
uvicorn main:app --reload --port 5000
```

Start a review workflow:

```bash
curl -X POST http://localhost:5000/contract-review/start \
	-H "Content-Type: application/json" \
	-d '{
		"s3_paths": [
			"s3://temporal-dev/legal-docs/vendor-service-agreement.pdf",
			"s3://temporal-dev/legal-docs/nda-innovate-consultpro.pdf",
			"s3://temporal-dev/legal-docs/software-license-globalsoft.pdf"
		]
	}'
```

Check workflow state:

```bash
curl http://localhost:5000/contract-review/<workflow-id>/status
```

Fetch the generated report:

```bash
curl http://localhost:5000/contract-review/<workflow-id>/report
```

Assign a reviewer:

```bash
curl -X POST http://localhost:5000/contract-review/<workflow-id>/assign \
	-H "Content-Type: application/json" \
	-d '{"name": "Reviewer Name"}'
```

Request a revision:

```bash
curl -X POST http://localhost:5000/contract-review/<workflow-id>/revise \
	-H "Content-Type: application/json" \
	-d '{"feedback": "Please rewrite the report in Arabic."}'
```

Approve the report:

```bash
curl http://localhost:5000/contract-review/<workflow-id>/approve
```

## What you are learning from each stage

### Plain Python pipeline

- a normal synchronous batch script
- external dependency handling
- file movement between S3 and local temp storage

### Temporal PDF pipeline

- workflow versus activity boundaries
- retries and timeouts
- running workers separately from clients
- durable orchestration around the same business flow

### AI contract review

- parent and child workflows
- parallel fan-out across multiple documents
- LLM integration through activities
- human-in-the-loop review using Temporal queries, signals, and updates

## Notes

- `apps/client-app` is intentionally thin. It starts workflows by name and does not execute workflow logic itself.
- The workers own the workflow and activity code.
- `setup/samples-server` is infrastructure for running Temporal locally.
- Systemd service examples are included for local automation under `setup/services` and `apps/pdf-extraction-02-temporal/temporal-worker.service`.

## Suggested learning order

1. Run the plain PDF script.
2. Run the Temporal version of the same pipeline.
3. Inspect the Temporal Web UI while the workflow runs.
4. Move on to the AI contract review example.
5. Experiment with reviewer assignment, revise, and approve flows.
