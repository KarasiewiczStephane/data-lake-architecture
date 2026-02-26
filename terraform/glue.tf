# Glue catalog database
resource "aws_glue_catalog_database" "datalake" {
  name = "${var.project_name}_${var.environment}"
}

# Bronze layer crawler
resource "aws_glue_crawler" "bronze" {
  name          = "${var.project_name}-bronze-crawler"
  database_name = aws_glue_catalog_database.datalake.name
  role          = aws_iam_role.glue.arn
  schedule      = var.glue_crawler_schedule

  s3_target {
    path = "s3://${aws_s3_bucket.bronze.id}/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = local.common_tags
}

# Silver layer crawler
resource "aws_glue_crawler" "silver" {
  name          = "${var.project_name}-silver-crawler"
  database_name = aws_glue_catalog_database.datalake.name
  role          = aws_iam_role.glue.arn
  schedule      = var.glue_crawler_schedule

  s3_target {
    path = "s3://${aws_s3_bucket.silver.id}/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = local.common_tags
}

# Gold layer crawler
resource "aws_glue_crawler" "gold" {
  name          = "${var.project_name}-gold-crawler"
  database_name = aws_glue_catalog_database.datalake.name
  role          = aws_iam_role.glue.arn
  schedule      = var.glue_crawler_schedule

  s3_target {
    path = "s3://${aws_s3_bucket.gold.id}/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = local.common_tags
}
