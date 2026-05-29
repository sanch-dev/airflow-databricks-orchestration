# main.tf

output "created_topics" {
  description = "List of created Kafka topics"
  # "output" = show this information after terraform apply
  
  value = keys(kafka_topic.topics)
  # "keys(...)" = get all the keys from the resource
  # Returns: ["news", "alerts"]
}