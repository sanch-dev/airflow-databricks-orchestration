# provider.tf
# This file tells Terraform what tools it needs and how to connect to Kafka

terraform {
  # "terraform" block = Terraform's own configuration
  # Everything inside here is about Terraform itself, not about resources
  
  required_providers {
    # "required_providers" = "I need these plugins to work"
    # A provider = a plugin that knows how to manage a service (AWS, Kafka, Azure, etc.)
    
    kafka = {
      # "kafka" = the name of the provider we need
      # This becomes available as "kafka_*" in our code
      
      source  = "Mongey/kafka"
      # "source" = where to download this provider from
      # "Mongey/kafka" = from Terraform Registry
      # Mongey = username (developer who made this)
      # kafka = provider name
      # Format: NAMESPACE/TYPE
      
      version = "~> 0.5"
      # "version" = which version of this provider to use
      # Why specific? Reproducibility - same version everywhere
    }
    # Could add more providers here if needed:
    # aws = { ... }
    # azurerm = { ... }
  }
}

provider "kafka" {
  # "provider" block = configure how to connect to Kafka
  # This is where we tell Terraform: "Here's how to reach Kafka"
  
  bootstrap_servers = var.kafka_bootstrap_servers
  # "bootstrap_servers" = Kafka configuration option
  # This tells the Kafka provider which server(s) to connect to
  # var.kafka_bootstrap_servers = use the variable we'll define in variables.tf
  # We use a variable so we can change it without editing this file
  # Example: ["localhost:9092"] or ["20.193.141.136:9092"]
  
  # Other options we COULD add (but don't need for learning):
  # tls_enabled = false
  # sasl_enabled = false
  # client_id = "terraform-kafka"
}