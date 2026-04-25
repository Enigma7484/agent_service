# Agent Service

A lightweight service component for the `bills-agent` project. This module hosts the backend logic, API endpoints, and runtime configuration for the agent service.

## Overview

`agent_service` provides:
- API endpoints for agent interactions
- request handling and routing
- configuration management
- integration with billing and ML components

This README covers installation, setup, local development, configuration, and usage.

## Features

- REST API for agent requests
- Configurable service settings
- Logging and basic error handling
- Health check endpoint
- Extensible architecture for model or data integrations

## Prerequisites

- Python 3.11+ (or target runtime version)
- pip
- Git
- Local environment variables or `.env` support
- Optional: Docker for containerized deployment

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/<your-org>/bills-agent.git
    cd bills-agent/agent_service
    ```

2. Create a virtual environment:
    ```bash
    python -m venv .venv
    source .venv/bin/activate   # macOS/Linux
    .venv\Scripts\activate      # Windows
    ```

3. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

`agent_service` uses environment variables or a configuration file to set runtime behavior.

Example variables:
- `AGENT_SERVICE_HOST` — host to bind service
- `AGENT_SERVICE_PORT` — port to listen on
- `LOG_LEVEL` — log verbosity
- `DATABASE_URL` — storage backend connection
- `API_KEY` — service authentication token

Create a `.env` file in the service root:
```env
AGENT_SERVICE_HOST=0.0.0.0
AGENT_SERVICE_PORT=8000
LOG_LEVEL=info
DATABASE_URL=sqlite:///./agent.db
API_KEY=your_api_key_here
```

## Running Locally

Start the service:
```bash
python -m agent_service.main
```

Verify health:
```bash
curl http://localhost:8000/health
```

## API Endpoints

Common endpoints may include:

- `GET /health`
- `POST /api/agent/query`
- `GET /api/agent/status`

Example request:
```bash
curl -X POST http://localhost:8000/api/agent/query \
  -H "Content-Type: application/json" \
  -d '{"query": "fetch bill summary"}'
```

## Development

- Use code formatting tools (e.g. `black`, `ruff`)
- Run tests with `pytest`
- Keep service logic modular and testable
- Add new endpoints under `agent_service/api` or equivalent package

## Testing

Run the test suite:
```bash
pytest
```

## Deployment

Deploy as a normal Python service or containerize using Docker.

Example Docker workflow:
- Build image
- Configure environment variables
- Run container exposing service port

## Contributing

- Follow repository contribution guidelines
- Open issues for bugs or feature requests
- Submit pull requests with clear descriptions
- Keep changes scoped to `agent_service` behavior and APIs

## License

Specify the project license in the repo root, for example:
- MIT License
- Apache 2.0

If no license is defined, consult repository maintainers before reuse.