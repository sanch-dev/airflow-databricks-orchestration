# Dockerfile
FROM hashicorp/terraform:latest

WORKDIR /terraform

# Copy all Terraform files into container
COPY . .

CMD ["terraform", "init"]