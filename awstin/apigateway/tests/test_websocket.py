import unittest
from unittest import mock

from awstin.apigateway.websocket import __name__ as WS_NAME, Websocket


class TestWebsocket(unittest.TestCase):
    def test_websocket_send(self):
        mock_api_client = mock.Mock()
        mock_aws_client = mock.patch(
            WS_NAME + ".boto3.client",
            return_value=mock_api_client,
        )
        with mock_aws_client as m_aws:
            socket = Websocket("endpointurl", stage="dev")
            socket.send("callbackurl", "message")

        m_aws.assert_called_once_with(
            "apigatewaymanagementapi",
            endpoint_url="https://endpointurl/dev"
        )
        mock_api_client.post_to_connection.assert_called_once_with(
            Data="message",
            ConnectionId="callbackurl",
        )
