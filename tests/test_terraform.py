"""Tests for Terraform reference templates."""

import os

import pytest

TERRAFORM_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "terraform")

EXPECTED_FILES = [
    "main.tf",
    "variables.tf",
    "s3.tf",
    "glue.tf",
    "athena.tf",
    "iam.tf",
    "lambda.tf",
    "outputs.tf",
]


class TestTerraformFilesExist:
    """Verify all required Terraform files are present."""

    @pytest.mark.parametrize("filename", EXPECTED_FILES)
    def test_file_exists(self, filename: str) -> None:
        """Each expected .tf file exists."""
        path = os.path.join(TERRAFORM_DIR, filename)
        assert os.path.isfile(path), f"Missing Terraform file: {filename}"

    @pytest.mark.parametrize("filename", EXPECTED_FILES)
    def test_file_not_empty(self, filename: str) -> None:
        """Each .tf file has content."""
        path = os.path.join(TERRAFORM_DIR, filename)
        assert os.path.getsize(path) > 0, f"Empty Terraform file: {filename}"


class TestMainTf:
    """Validate main.tf structure."""

    @pytest.fixture
    def content(self) -> str:
        with open(os.path.join(TERRAFORM_DIR, "main.tf")) as f:
            return f.read()

    def test_required_version(self, content: str) -> None:
        """Terraform version constraint is set."""
        assert 'required_version = ">= 1.0"' in content

    def test_aws_provider(self, content: str) -> None:
        """AWS provider is configured."""
        assert 'source  = "hashicorp/aws"' in content

    def test_backend_configured(self, content: str) -> None:
        """Remote state backend is configured."""
        assert 'backend "s3"' in content

    def test_common_tags(self, content: str) -> None:
        """Common tags local is defined."""
        assert "common_tags" in content
        assert "ManagedBy" in content


class TestVariablesTf:
    """Validate variables.tf structure."""

    @pytest.fixture
    def content(self) -> str:
        with open(os.path.join(TERRAFORM_DIR, "variables.tf")) as f:
            return f.read()

    def test_aws_region_variable(self, content: str) -> None:
        """AWS region variable exists with default."""
        assert '"aws_region"' in content
        assert '"us-east-1"' in content

    def test_project_name_variable(self, content: str) -> None:
        """Project name variable exists."""
        assert '"project_name"' in content

    def test_environment_variable(self, content: str) -> None:
        """Environment variable with validation."""
        assert '"environment"' in content
        assert "dev" in content
        assert "staging" in content
        assert "prod" in content

    def test_all_variables_have_descriptions(self, content: str) -> None:
        """Every variable block has a description."""
        blocks = content.split('variable "')
        for block in blocks[1:]:
            assert "description" in block, f"Variable missing description: {block[:50]}"


class TestS3Tf:
    """Validate s3.tf structure."""

    @pytest.fixture
    def content(self) -> str:
        with open(os.path.join(TERRAFORM_DIR, "s3.tf")) as f:
            return f.read()

    def test_three_buckets(self, content: str) -> None:
        """Bronze, silver, and gold buckets are defined."""
        for layer in ["bronze", "silver", "gold"]:
            assert f'aws_s3_bucket" "{layer}"' in content

    def test_versioning_enabled(self, content: str) -> None:
        """Bucket versioning is enabled."""
        assert "aws_s3_bucket_versioning" in content
        assert '"Enabled"' in content

    def test_lifecycle_rules(self, content: str) -> None:
        """Lifecycle transitions are configured."""
        assert "aws_s3_bucket_lifecycle_configuration" in content
        assert "STANDARD_IA" in content
        assert "GLACIER" in content

    def test_encryption(self, content: str) -> None:
        """Server-side encryption is configured."""
        assert "aws_s3_bucket_server_side_encryption_configuration" in content
        assert "aws:kms" in content

    def test_public_access_blocked(self, content: str) -> None:
        """Public access is blocked on all buckets."""
        assert "aws_s3_bucket_public_access_block" in content
        assert "block_public_acls" in content
        assert "block_public_policy" in content


class TestGlueTf:
    """Validate glue.tf structure."""

    @pytest.fixture
    def content(self) -> str:
        with open(os.path.join(TERRAFORM_DIR, "glue.tf")) as f:
            return f.read()

    def test_catalog_database(self, content: str) -> None:
        """Glue catalog database is defined."""
        assert "aws_glue_catalog_database" in content

    def test_crawlers_exist(self, content: str) -> None:
        """Crawlers for each layer are defined."""
        assert 'aws_glue_crawler" "bronze"' in content
        assert 'aws_glue_crawler" "silver"' in content
        assert 'aws_glue_crawler" "gold"' in content

    def test_schema_change_policy(self, content: str) -> None:
        """Schema change policy is set."""
        assert "schema_change_policy" in content
        assert "UPDATE_IN_DATABASE" in content


class TestAthenaTf:
    """Validate athena.tf structure."""

    @pytest.fixture
    def content(self) -> str:
        with open(os.path.join(TERRAFORM_DIR, "athena.tf")) as f:
            return f.read()

    def test_workgroup(self, content: str) -> None:
        """Athena workgroup is defined."""
        assert "aws_athena_workgroup" in content

    def test_encryption(self, content: str) -> None:
        """Query result encryption is configured."""
        assert "encryption_configuration" in content
        assert "SSE_S3" in content

    def test_query_limit(self, content: str) -> None:
        """Bytes scanned cutoff is set."""
        assert "bytes_scanned_cutoff_per_query" in content


class TestIamTf:
    """Validate iam.tf structure."""

    @pytest.fixture
    def content(self) -> str:
        with open(os.path.join(TERRAFORM_DIR, "iam.tf")) as f:
            return f.read()

    def test_glue_role(self, content: str) -> None:
        """Glue IAM role is defined."""
        assert 'aws_iam_role" "glue"' in content
        assert "glue.amazonaws.com" in content

    def test_lambda_role(self, content: str) -> None:
        """Lambda IAM role is defined."""
        assert 'aws_iam_role" "lambda"' in content
        assert "lambda.amazonaws.com" in content

    def test_least_privilege_s3(self, content: str) -> None:
        """S3 policies use specific actions, not wildcard."""
        assert "s3:*" not in content
        assert "s3:GetObject" in content
        assert "s3:PutObject" in content


class TestLambdaTf:
    """Validate lambda.tf structure."""

    @pytest.fixture
    def content(self) -> str:
        with open(os.path.join(TERRAFORM_DIR, "lambda.tf")) as f:
            return f.read()

    def test_lambda_function(self, content: str) -> None:
        """Lambda function resource is defined."""
        assert "aws_lambda_function" in content
        assert "python3.12" in content

    def test_s3_trigger(self, content: str) -> None:
        """S3 event notification triggers Lambda."""
        assert "aws_s3_bucket_notification" in content
        assert "s3:ObjectCreated:*" in content

    def test_invoke_permission(self, content: str) -> None:
        """Lambda permission for S3 invocation is set."""
        assert "aws_lambda_permission" in content
        assert "s3.amazonaws.com" in content


class TestOutputsTf:
    """Validate outputs.tf structure."""

    @pytest.fixture
    def content(self) -> str:
        with open(os.path.join(TERRAFORM_DIR, "outputs.tf")) as f:
            return f.read()

    def test_bucket_outputs(self, content: str) -> None:
        """Bucket name and ARN outputs exist."""
        for layer in ["bronze", "silver", "gold"]:
            assert f"{layer}_bucket_name" in content
            assert f"{layer}_bucket_arn" in content

    def test_all_outputs_have_descriptions(self, content: str) -> None:
        """Every output has a description."""
        blocks = content.split("output ")
        for block in blocks[1:]:
            assert "description" in block, f"Output missing description: {block[:50]}"

    def test_glue_output(self, content: str) -> None:
        """Glue database name output exists."""
        assert "glue_database_name" in content

    def test_athena_output(self, content: str) -> None:
        """Athena workgroup output exists."""
        assert "athena_workgroup" in content
