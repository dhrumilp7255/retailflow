# ============================================
# RETAIL ETL PIPELINE - TERRAFORM
# Provisions:
#   - S3 buckets (raw, processed, athena)
#   - IAM role for Glue
#   - Glue database
# ============================================

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── Variables ────────────────────────────────
variable "aws_region" {
  default = "us-east-1"
}

variable "project_name" {
  default = "retail-etl"
}

# ── S3 Buckets ───────────────────────────────
resource "aws_s3_bucket" "raw_data" {
  bucket = "${var.project_name}-raw-data"

  tags = {
    Project     = var.project_name
    Environment = "dev"
    Layer       = "raw"
  }
}

resource "aws_s3_bucket" "processed_data" {
  bucket = "${var.project_name}-processed-data"

  tags = {
    Project     = var.project_name
    Environment = "dev"
    Layer       = "processed"
  }
}

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_name}-athena-results"

  tags = {
    Project     = var.project_name
    Environment = "dev"
    Layer       = "athena"
  }
}

# ── IAM Role for Glue ────────────────────────
resource "aws_iam_role" "glue_role" {
  name = "RetailETLGlueRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# ── Glue Database ────────────────────────────
resource "aws_glue_catalog_database" "retail_db" {
  name        = "retail_etl_db"
  description = "Retail ETL processed data catalog"
}

# ── Outputs ──────────────────────────────────
output "raw_bucket_name" {
  value = aws_s3_bucket.raw_data.bucket
}

output "processed_bucket_name" {
  value = aws_s3_bucket.processed_data.bucket
}

output "athena_results_bucket" {
  value = aws_s3_bucket.athena_results.bucket
}

output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}