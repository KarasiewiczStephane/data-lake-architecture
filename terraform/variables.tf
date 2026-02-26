variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "datalake"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "bronze_retention_days" {
  description = "Days before transitioning bronze data to Infrequent Access"
  type        = number
  default     = 90
}

variable "glacier_transition_days" {
  description = "Days before transitioning bronze data to Glacier"
  type        = number
  default     = 365
}

variable "athena_query_limit_bytes" {
  description = "Maximum bytes scanned per Athena query (default 10GB)"
  type        = number
  default     = 10737418240
}

variable "glue_crawler_schedule" {
  description = "Cron schedule for Glue crawlers"
  type        = string
  default     = "cron(0 */6 * * ? *)"
}

variable "lambda_memory_mb" {
  description = "Memory allocation for event processing Lambda"
  type        = number
  default     = 256
}

variable "lambda_timeout_seconds" {
  description = "Timeout for event processing Lambda"
  type        = number
  default     = 300
}
