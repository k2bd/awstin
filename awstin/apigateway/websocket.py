import boto3


class Websocket:
    """
    Serverless-to-client push via websocket
    """
    def __init__(self, domain_name, stage=None):
        """
        Parameters
        ----------
        connection_id : str
            Callback URL of the websocket
        """
        endpoint_url = f"https://{domain_name}"
        if stage:
            endpoint_url += f"/{stage}"
        self.api_client = boto3.client(
            'apigatewaymanagementapi',
            endpoint_url=endpoint_url,
        )

    def send(self, connection_id, message):
        """
        Send a message to the user
        """
        self.api_client.post_to_connection(
            Data=message,
            ConnectionId=connection_id,
        )
