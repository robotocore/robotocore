output "user_pool_id" {
  description = "The ID of the Cognito user pool"
  value       = aws_cognito_user_pool.main.id
}

output "client_id" {
  description = "The ID of the Cognito user pool client"
  value       = aws_cognito_user_pool_client.main.id
}
