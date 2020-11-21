import boto3


class Websocket:
    """
    Serverless-to-client push via websocket
    """
    def __init__(self, connection_id):
        """
        Parameters
        ----------
        connection_id : str
            Callback URL of the websocket
        """
        self.connection_id = connection_id
        self.api_client = boto3.client('apigatewaymanagementapi')

    def send(self, message):
        """
        Send a message to the user
        """
        self.api_client.post_to_connection(
            Data=message,
            ConnectionId=self.connection_id,
        )
