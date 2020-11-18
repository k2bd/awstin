# -- AWS Lambda environment variables

#: Lambda function handler
_HANDLER = "_HANDLER"

#: Region
AWS_REGION = "AWS_REGION"

#: Runtime ID
AWS_EXECUTION_ENV = "AWS_EXECUTION_ENV"

#: Lambda name
AWS_LAMBDA_FUNCTION_NAME = "AWS_LAMBDA_FUNCTION_NAME"

#: Lambda memory parameter
AWS_LAMBDA_FUNCTION_MEMORY_SIZE = "AWS_LAMBDA_FUNCTION_MEMORY_SIZE"

#: Lambda version
AWS_LAMBDA_FUNCTION_VERSION = "AWS_LAMBDA_FUNCTION_VERSION"

#: Log group name
AWS_LAMBDA_LOG_GROUP_NAME = "AWS_LAMBDA_LOG_GROUP_NAME"

#: Log stream name
AWS_LAMBDA_LOG_STREAM_NAME = "AWS_LAMBDA_LOG_STREAM_NAME"

#: AWS access key (from execution role)
AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"

#: AWS secret key (from execution role)
AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"

#: Session token (from execution role)
AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN"

#: Runtime API: None unless using a custom runtime
AWS_LAMBDA_RUNTIME_API = "AWS_LAMBDA_RUNTIME_API"

#: Path to the lambda
LAMBDA_TASK_ROOT = "LAMBDA_TASK_ROOT"

#: Path to runtime libraries
LAMBDA_RUNTIME_DIR = "LAMBDA_RUNTIME_DIR"

#: Timezone
TZ = "TZ"


# -- Testing Environment Variables

#: DynamodDB service endpoint, for example if running a DynamoDB container
TEST_DYNAMODB_ENDPOINT = "TEST_DYNAMODB_ENDPOINT"

#: SNS service endpoint for testing
TEST_SNS_ENDPOINT = "TEST_SNS_ENDPOINT"
