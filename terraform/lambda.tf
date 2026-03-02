# Event processing Lambda triggered by S3 uploads to bronze
resource "aws_lambda_function" "event_processor" {
  function_name = "${var.project_name}-event-processor"
  role          = aws_iam_role.lambda.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  timeout       = var.lambda_timeout_seconds
  memory_size   = var.lambda_memory_mb

  filename         = "${path.module}/lambda/event_processor.zip"
  source_code_hash = filebase64sha256("${path.module}/lambda/event_processor.zip")

  environment {
    variables = {
      PROJECT_NAME  = var.project_name
      ENVIRONMENT   = var.environment
      SILVER_BUCKET = aws_s3_bucket.silver.id
    }
  }

  tags = local.common_tags
}

# S3 event notification for bronze bucket uploads
resource "aws_s3_bucket_notification" "bronze_events" {
  bucket = aws_s3_bucket.bronze.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.event_processor.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}

# Permission for S3 to invoke Lambda
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.event_processor.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.bronze.arn
}
