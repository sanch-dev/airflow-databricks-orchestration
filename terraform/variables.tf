# variables.tf
# This file defines INPUT variables for Terraform
# Think of variables as "settings" you can change without editing other files

variable "kafka_bootstrap_servers" {
  # "variable" = define a reusable value
  # "kafka_bootstrap_servers" = variable name
  
  description = "Kafka bootstrap servers"
  # "description" = human-readable explanation
  # Shows up when you run: terraform plan
  
  type = list(string)
  # "type" = what kind of value is this?
  # list(string) = a list of text values
  # Example: ["localhost:9092", "localhost:9093"]
  
  default = ["kafka:9092"]
  # "default" = if no one provides a value, use this
  # So: bootstrap_servers = ["localhost:9092"]
  # You can override it later if needed
}

variable "kafka_topics" {
  # "kafka_topics" = variable for Kafka topics
  # This is more complex than bootstrap_servers
  
  description = "Kafka topics to create"
  
  type = map(object({
    partitions       = number
    replication_factor = number
  }))
  # "type" explanation:
  # map(object(...)) = a dictionary where each value is an object
  # Each topic has: partitions (number) and replication_factor (number)
  
  # Example of what this looks like:
  # {
  #   "news" = {
  #     partitions = 1
  #     replication_factor = 1
  #   }
  #   "alerts" = {
  #     partitions = 3
  #     replication_factor = 3
  #   }
  # }
  
  default = {
    "news" = {
      partitions       = 1
      replication_factor = 1
    }
    "alerts" = {
      partitions       = 1
      replication_factor = 1
    }
  }
  # "default" = if no one provides topics, create these two
  # You can change this without editing other files
}