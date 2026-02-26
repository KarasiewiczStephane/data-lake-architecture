"""Tests for Docker Compose and GitHub Actions CI configuration."""

import os

import yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))


class TestDockerCompose:
    """Validate docker-compose.yml configuration."""

    def setup_method(self) -> None:
        path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
        with open(path) as f:
            self.config = yaml.safe_load(f)

    def test_valid_yaml(self) -> None:
        """docker-compose.yml is valid YAML."""
        assert self.config is not None

    def test_minio_service(self) -> None:
        """MinIO service is configured."""
        services = self.config["services"]
        assert "minio" in services
        minio = services["minio"]
        assert "9000:9000" in minio["ports"]
        assert "9001:9001" in minio["ports"]
        assert minio["environment"]["MINIO_ROOT_USER"] == "minioadmin"

    def test_minio_healthcheck(self) -> None:
        """MinIO has a healthcheck."""
        minio = self.config["services"]["minio"]
        assert "healthcheck" in minio
        assert "test" in minio["healthcheck"]

    def test_minio_init_service(self) -> None:
        """MinIO init service creates buckets."""
        services = self.config["services"]
        assert "minio-init" in services
        init = services["minio-init"]
        assert "minio" in init["depends_on"]

    def test_datalake_service(self) -> None:
        """Data lake app service is configured."""
        services = self.config["services"]
        assert "datalake" in services
        app = services["datalake"]
        assert "build" in app
        assert app["environment"]["MINIO_ENDPOINT"] == "minio:9000"

    def test_network_defined(self) -> None:
        """Custom network is defined."""
        assert "datalake" in self.config["networks"]

    def test_volume_defined(self) -> None:
        """MinIO data volume is defined."""
        assert "minio_data" in self.config["volumes"]


class TestDockerfile:
    """Validate Dockerfile configuration."""

    def setup_method(self) -> None:
        path = os.path.join(PROJECT_ROOT, "Dockerfile")
        with open(path) as f:
            self.content = f.read()

    def test_python_base_image(self) -> None:
        """Uses Python 3.12 base image."""
        assert "python:3.12" in self.content

    def test_workdir_set(self) -> None:
        """Working directory is set."""
        assert "WORKDIR /app" in self.content

    def test_requirements_installed(self) -> None:
        """Requirements are installed."""
        assert "requirements.txt" in self.content
        assert "pip install" in self.content

    def test_pythonpath_set(self) -> None:
        """PYTHONPATH environment variable is set."""
        assert "PYTHONPATH=/app" in self.content

    def test_source_copied(self) -> None:
        """Source code is copied."""
        assert "COPY src/ src/" in self.content
        assert "COPY configs/ configs/" in self.content


class TestCIWorkflow:
    """Validate GitHub Actions CI workflow."""

    def setup_method(self) -> None:
        path = os.path.join(PROJECT_ROOT, ".github", "workflows", "ci.yml")
        with open(path) as f:
            self.config = yaml.safe_load(f)

    def test_valid_yaml(self) -> None:
        """CI workflow is valid YAML."""
        assert self.config is not None
        assert self.config["name"] == "CI"

    def test_trigger_on_push(self) -> None:
        """Workflow triggers on push to main/master."""
        # YAML parses 'on' as boolean True
        triggers = self.config[True]
        assert "push" in triggers
        assert "main" in triggers["push"]["branches"]

    def test_trigger_on_pr(self) -> None:
        """Workflow triggers on pull requests."""
        triggers = self.config[True]
        assert "pull_request" in triggers

    def test_lint_job_exists(self) -> None:
        """Lint job is defined."""
        assert "lint" in self.config["jobs"]
        lint = self.config["jobs"]["lint"]
        assert lint["runs-on"] == "ubuntu-latest"

    def test_test_job_exists(self) -> None:
        """Test job is defined and depends on lint."""
        assert "test" in self.config["jobs"]
        test_job = self.config["jobs"]["test"]
        assert "lint" in test_job["needs"]

    def test_terraform_validate_job(self) -> None:
        """Terraform validation job is defined."""
        assert "terraform-validate" in self.config["jobs"]

    def test_python_version(self) -> None:
        """Python 3.12 is used in CI."""
        lint_steps = self.config["jobs"]["lint"]["steps"]
        python_step = next(s for s in lint_steps if s.get("name") == "Set up Python")
        assert python_step["with"]["python-version"] == "3.12"

    def test_coverage_upload(self) -> None:
        """Coverage upload step exists."""
        test_steps = self.config["jobs"]["test"]["steps"]
        coverage_step = next(
            s for s in test_steps if s.get("name") == "Upload coverage"
        )
        assert "codecov" in coverage_step["uses"]


class TestMakefile:
    """Validate Makefile targets."""

    def setup_method(self) -> None:
        path = os.path.join(PROJECT_ROOT, "Makefile")
        with open(path) as f:
            self.content = f.read()

    def test_install_target(self) -> None:
        """Install target exists."""
        assert "install:" in self.content

    def test_test_target(self) -> None:
        """Test target runs pytest with coverage."""
        assert "test:" in self.content
        assert "pytest" in self.content
        assert "--cov" in self.content

    def test_lint_target(self) -> None:
        """Lint target runs ruff."""
        assert "lint:" in self.content
        assert "ruff" in self.content

    def test_up_down_targets(self) -> None:
        """Docker up/down targets exist."""
        assert "up:" in self.content
        assert "down:" in self.content
        assert "docker-compose" in self.content

    def test_shell_target(self) -> None:
        """Shell target exists."""
        assert "shell:" in self.content
