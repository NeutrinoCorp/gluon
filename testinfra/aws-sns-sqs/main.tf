terraform {
  required_providers {
    aws = {
      version = ">= 2.7.0"
      source  = "hashicorp/aws"
    }
  }
}

provider "aws" {
  region  = "us-east-1"
  allowed_account_ids = [ "1234567890" ]

  default_tags {
    tags = {
      platform    = "test"
      environment = "development"
    }
  }
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

resource "aws_sns_topic" "default" {
  for_each = toset(local.topics)
  name = each.key
}

resource "aws_sqs_queue" "dead_letter" {
  for_each = local.queues

  name = "${each.key}-dlq"
  message_retention_seconds = 604800 // 7 days
}

resource "aws_sqs_queue" "default" {
  for_each = local.queues
  name = each.key

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter[each.key].arn,
    maxReceiveCount = 3
  })

  policy = jsonencode({
    "Version": "2008-10-17",
    "Id": "__default_policy_ID",
    "Statement": [
      {
        "Sid": "__owner_statement",
        "Effect": "Allow",
        "Principal": {
          "AWS": "arn:aws:iam::${var.aws_account_id}:root"
        },
        "Action": "SQS:*",
        "Resource": "arn:aws:sqs:${var.aws_region}:${var.aws_account_id}:${each.key}"
      },
      {
        "Sid": "topic-subscription-${each.value}",
        "Effect": "Allow",
        "Principal": {
          "AWS": "*"
        },
        "Action": "SQS:SendMessage",
        "Resource": "arn:aws:sqs:${var.aws_region}:${var.aws_account_id}:${each.key}",
        "Condition": {
          "ArnLike": {
            "aws:SourceArn": "arn:aws:sns:${var.aws_region}:${var.aws_account_id}:${each.value}"
          }
        }
      }
    ]
  })

  depends_on = [aws_sqs_queue.dead_letter]
}

resource "aws_sns_topic_subscription" "default" {
  for_each = local.queues
  protocol  = "sqs"
  topic_arn = "arn:aws:sns:${local.aws_region}:${local.aws_account_id}:${each.value}"
  endpoint  = "arn:aws:sqs:${local.aws_region}:${local.aws_account_id}:${each.key}"

  depends_on = [aws_sns_topic.default, aws_sqs_queue.default]
}
