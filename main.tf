terraform {
	required_providers {
        aws = {
			source  = "hashicorp/aws"
			version = "~> 5.0"
		}
	}
}

data "aws_caller_identity" "current" {}

provider "aws" {
	region = "us-east-1"
    profile = "default"
}

locals {
  users = {
    "bees-aws" = {
      name  = "Bees Terraform AWS User"
      email = "bees_aws@example.com"
    } 
  }
}

resource "aws_iam_user" "create_user" {
    for_each = local.users
    
    name = "bees-aws"
    force_destroy = false
}

resource "aws_s3_bucket" "bronze" {
  bucket = "bees-openbrewerydb-bronze"

  tags = {
    Name = "bees-openbrewerydb-bronze"
    Env = "dev"
    Tier = "bronze"
  }
}

resource "aws_s3_bucket" "silver" {
  bucket = "bees-openbrewerydb-silver"

  tags = {
    Name = "bees-openbrewerydb-silver"
    Env = "stg"
    Tier = "silver"
  }
}

resource "aws_s3_bucket" "gold" {
  bucket = "bees-openbrewerydb-gold"

  tags = {
    Name = "bees-openbrewerydb-gold"
    Env = "prd"
    Tier = "gold"
  }
}

resource "aws_s3_object" "athena_queries" {
  bucket = aws_s3_bucket.gold.id
  key    = "athena/" 
}

resource "aws_athena_data_catalog" "bees_brewerydb" {
  name        = "bees_brewerydb"
  description = "Bees BreweryDB Glue based Data Catalog"
  type        = "GLUE"

  parameters = {
    "catalog-id" = data.aws_caller_identity.current.account_id
  }
}

# IAM Role for Glue Crawler
resource "aws_iam_role" "glue_role" {
  name        = "glue-crawler-role"
  description = "Role for Glue Crawler to access S3 and Glue"
  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "glue.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  })
}

# Managed IAM policy granting CloudWatch Logs write for Glue
resource "aws_iam_policy" "glue_cloudwatch_logs" {
  name        = "GlueCloudWatchLogsWrite-${var.aws_region}"
  description = "Allow AWS Glue to create log streams and put log events into /aws-glue/crawlers"
  policy      = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "AllowGlueCloudWatchLogsWrite",
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:DescribeLogStreams",
          "logs:PutLogEvents"
        ],
        Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws-glue/crawlers:*"
      }
    ]
  })
}

resource "aws_iam_policy" "glue_cloudwatch_jobs" {
  name        = "GlueCloudWatchLogsJobs-${var.aws_region}"
  description = "Allow AWS Glue jobs to write CloudWatch logs"
  policy      = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "AllowGlueJobLogs",
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:DescribeLogStreams",
          "logs:PutLogEvents"
        ],
        Resource = [
          "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws-glue/jobs/*",
          "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws-glue/jobs/*:*"
        ]
      }
    ]
  })
}

# IAM Policy for Glue and Athena access
resource "aws_iam_policy" "athena_glue_policy" {
  name        = "athena-glue-dbt-policy"
  description = "Policy to access Glue and Athena"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:UpdateDatabase",
          "glue:DeleteDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition",
          "glue:CreateCrawler",
          "glue:DeleteCrawler",
          "glue:GetCrawler",
          "glue:GetCrawlers",
          "glue:StartCrawler",
          "glue:StopCrawler",
          "glue:UpdateCrawler",
          "glue:ListCrawlers",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:GetPartitionIndexes",
          "glue:BatchGetPartition",
          "glue:GetJob",
          "glue:CreateJob",
          "glue:UpdateJob",
          "glue:DeleteJob"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:StopQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:ListQueryExecutions",
          "athena:GetWorkGroup",
          "athena:ListWorkGroups",
          "athena:GetDataCatalog",
          "athena:ListDataCatalogs",
          "athena:CreateDataCatalog",
          "athena:UpdateDataCatalog",
          "athena:DeleteDataCatalog"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketVersioning",
          "s3:ListBucketVersions",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::bees-openbrewerydb-bronze",
          "arn:aws:s3:::bees-openbrewerydb-bronze/*",
          "arn:aws:s3:::bees-openbrewerydb-silver",
          "arn:aws:s3:::bees-openbrewerydb-silver/*",
          "arn:aws:s3:::bees-openbrewerydb-gold",
          "arn:aws:s3:::bees-openbrewerydb-gold/*"
        ]
      },
      {
        Effect = "Allow"
        Action = "iam:PassRole"
        Resource = aws_iam_role.glue_role.arn
      }
    ]
  })
}

# Attach policy to the bees-aws user
resource "aws_iam_user_policy_attachment" "terraform_aws_policy" {
  for_each = local.users
  user       = aws_iam_user.create_user[each.key].name
  policy_arn = aws_iam_policy.athena_glue_policy.arn
}

# Attach AdministratorAccess to the existing bees-aws user
resource "aws_iam_user_policy_attachment" "admin_fix" {
  for_each = local.users

  user       = aws_iam_user.create_user[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

# Attach policy to the Glue Role
resource "aws_iam_role_policy_attachment" "glue_role_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.athena_glue_policy.arn
}

# Attach the managed policy to the existing Glue role
resource "aws_iam_role_policy_attachment" "attach_glue_cwl" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_cloudwatch_logs.arn

  # ensure role exists before attachment
  depends_on = [aws_iam_role.glue_role, aws_iam_policy.glue_cloudwatch_logs]
}

# Attach the managed policy to the existing Glue role
resource "aws_iam_role_policy_attachment" "attach_glue_job" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_cloudwatch_jobs.arn

  # ensure role exists before attachment
  depends_on = [aws_iam_role.glue_role, aws_iam_policy.glue_cloudwatch_jobs]
}
