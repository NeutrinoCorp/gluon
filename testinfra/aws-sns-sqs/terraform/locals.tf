locals {
  aws_region = data.aws_region.current.name
  aws_account_id = data.aws_caller_identity.current.account_id
  topics = [
    "ncorp-places-marketplace-prod-2-event-item-paid",
    "ncorp-places-warehouse-prod-1-event-package-sent",
    "ncorp-places-warehouse-prod-1-event-package-delivered",
    "ncorp-places-warehouse-prod-1-event-package-delivered"
  ]

  // One consumer-group (queue) per processing job when using AWS SQS
  queues_v1 = [
    "ncorp-places-warehouse-prod-1-send_order-on-item_paid",
    "ncorp-places-warehouse-prod-1-deliver_order-on-order_sent",
    "ncorp-places-analytics-prod-1-ingest_data-on-order_delivered",
    "ncorp-places-marketplace-prod-1" // default queue
  ]

  queues = zipmap(local.queues_v1, local.topics)
}