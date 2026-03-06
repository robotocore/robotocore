# Ruby Lambda bootstrap — reads event from stdin, invokes handler, writes result to stdout.
#
# Usage: ruby bootstrap.rb
# Handler is read from _HANDLER env var (e.g., "lambda_function.handler")
#
# Protocol:
#   stdin  -> JSON event
#   stdout -> JSON result
#   stderr -> logs

require 'json'
require 'securerandom'

handler_spec = ENV['_HANDLER'] || ARGV[0]
unless handler_spec
  $stdout.write(JSON.generate({
    errorMessage: 'No handler specified',
    errorType: 'Runtime.HandlerNotFound'
  }))
  exit(1)
end

dot_index = handler_spec.rindex('.')
unless dot_index
  $stdout.write(JSON.generate({
    errorMessage: "Bad handler format: #{handler_spec}",
    errorType: 'Runtime.HandlerNotFound'
  }))
  exit(1)
end

module_name = handler_spec[0...dot_index]
method_name = handler_spec[dot_index + 1..]

# Read event from stdin
input_data = $stdin.read
event = begin
  JSON.parse(input_data)
rescue JSON::ParserError
  {}
end

# Build context
context_data = {
  function_name: ENV['AWS_LAMBDA_FUNCTION_NAME'] || 'test-function',
  function_version: '$LATEST',
  invoked_function_arn: "arn:aws:lambda:#{ENV['AWS_REGION'] || 'us-east-1'}:#{ENV['AWS_ACCOUNT_ID'] || '123456789012'}:function:#{ENV['AWS_LAMBDA_FUNCTION_NAME'] || 'test-function'}",
  memory_limit_in_mb: (ENV['AWS_LAMBDA_FUNCTION_MEMORY_SIZE'] || '128').to_i,
  aws_request_id: SecureRandom.uuid,
  log_group_name: "/aws/lambda/#{ENV['AWS_LAMBDA_FUNCTION_NAME'] || 'test-function'}",
  log_stream_name: "#{Time.now.strftime('%Y/%m/%d')}/$LATEST",
}

# Simple context object with method access
LambdaContext = Struct.new(*context_data.keys, keyword_init: true) do
  def get_remaining_time_in_millis
    timeout = (ENV['AWS_LAMBDA_FUNCTION_TIMEOUT'] || '3').to_i
    timeout * 1000
  end
end
context = LambdaContext.new(**context_data)

# Load the handler module
module_file = File.join(Dir.pwd, "#{module_name}.rb")
unless File.exist?(module_file)
  $stdout.write(JSON.generate({
    errorMessage: "Cannot find module '#{module_name}': #{module_file}",
    errorType: 'Runtime.ImportModuleError'
  }))
  exit(1)
end

begin
  require module_file
rescue => e
  $stdout.write(JSON.generate({
    errorMessage: "Error loading module '#{module_name}': #{e.message}",
    errorType: 'Runtime.ImportModuleError',
    stackTrace: e.backtrace || []
  }))
  exit(1)
end

# Find the handler method (top-level defs are private in Ruby, use respond_to? with true)
unless respond_to?(method_name.to_sym, true)
  $stdout.write(JSON.generate({
    errorMessage: "Handler method '#{method_name}' not found in '#{module_name}'",
    errorType: 'Runtime.HandlerNotFound'
  }))
  exit(1)
end

# Invoke
begin
  result = send(method_name.to_sym, event: event, context: context)
  $stdout.write(JSON.generate(result))
rescue => e
  $stderr.write("#{e.class}: #{e.message}\n#{e.backtrace&.join("\n")}\n")
  $stdout.write(JSON.generate({
    errorMessage: e.message,
    errorType: e.class.name,
    stackTrace: e.backtrace || []
  }))
  exit(1)
end
