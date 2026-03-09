output "api_id" {
  description = "API Gateway REST API ID"
  value       = aws_api_gateway_rest_api.api.id
}

output "function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.hello.function_name
}

output "invoke_url" {
  description = "Full invoke URL for the /hello endpoint"
  value       = "${aws_api_gateway_stage.live.invoke_url}/hello"
}
