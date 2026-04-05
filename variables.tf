variable "aws_region" {
  type        = string
  description = "The AWS region to deploy resources in."
  default     = "us-east-1"
}

variable "aws_account_id" {
  type = string
  description = "The AWS Account ID used to create the infrastructure, change it to your own"
  default = "552738622317"
}