# kafka_topics.tf
# This file CREATES the actual Kafka topics
# Uses the provider from provider.tf and variables from variables.tf

resource "kafka_topic" "topics" {
  # "resource" = "Create something real"
  # "kafka_topic" = the type of thing we're creating (Kafka topic)
  # "topics" = the name of this resource in Terraform (for reference)
  
  for_each = var.kafka_topics
  # "for_each" = loop through each topic in var.kafka_topics
  # It's like a for loop in programming
  
  # Example: if var.kafka_topics has:
  # {
  #   "news" = { partitions = 1, replication_factor = 1 }
  #   "alerts" = { partitions = 1, replication_factor = 1 }
  # }
  # 
  # Then this loop runs TWICE:
  # - Once for "news"
  # - Once for "alerts"
  
  # In each loop:
  # - each.key = topic name ("news", "alerts", etc.)
  # - each.value = the object (partitions, replication_factor)
  
  name = each.key
  # "name" = the Kafka topic name
  # each.key = the dictionary key from var.kafka_topics
  # If topic is "news": name = "news" ✓
  # If topic is "alerts": name = "alerts" ✓
  
  partitions = each.value.partitions
  # "partitions" = how many partitions for this topic
  # each.value.partitions = get the "partitions" field from the object
  # Example: if topic is "news" and we defined partitions = 1
  # Then: partitions = 1 ✓
  
  replication_factor = each.value.replication_factor
  # "replication_factor" = how many copies to keep
  # each.value.replication_factor = get the "replication_factor" field
  # Example: if topic is "news" and we defined replication_factor = 1
  # Then: replication_factor = 1 ✓
}