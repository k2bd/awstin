import json
import os


def handler(event, context):
    messages_file = os.environ.get("SNS_SUB_MESSAGES_FILE")
    with open(messages_file, "a+", encoding="utf-8") as f:
        records = [json.dumps(record["Sns"]) for record in event["Records"]]
        for line in records:
            f.write(line + "\n")
