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


