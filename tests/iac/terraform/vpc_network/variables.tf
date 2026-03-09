variable "prefix" {
  description = "Name prefix for all resources"
  type        = string
  default     = "iac-test"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}
