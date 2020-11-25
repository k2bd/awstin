
def accept(principal_id, resource_arn):
    """
    Return an auth lambda response granting access to the given resource ARN
    to the given principle ID.

    Parameters
    ----------
    principal_id : str
        The principal ID to grant access to
    resource_arn : str
        The ARN of the resource to grant access to

    Returns
    -------
    dict
        Auth lambda response
    """
    return _auth_response(principal_id, resource_arn, "Allow")


def reject(principal_id, resource_arn):
    """
    Return an auth lambda response rejecting access to the given resource ARN
    to the given principle ID.

    Parameters
    ----------
    principal_id : str
        The principal ID to reject access to
    resource_arn : str
        The ARN of the resource to reject access to

    Returns
    -------
    dict
        Auth lambda response
    """
    return _auth_response(principal_id, resource_arn, "Deny")


def unauthorized(body='Unauthorized'):
    """
    Return an auth lambda response indicating the requester is unauthorized.

    Parameters
    ----------
    body : str, optional
        Optional resposnse body. Default "Unauthorized"

    Returns
    -------
    dict
        Auth lambda response
    """
    return {
        'statusCode': 401,
        'body': body,
    }


def invalid(body='Invalid'):
    """
    Return an auth lambda response indicating the request is invalid.

    Parameters
    ----------
    body : str, optional
        Optional resposnse body. Default "Invalid"

    Returns
    -------
    dict
        Auth lambda response
    """
    return {
        'statusCode': 500,
        'body': body,
    }


def _auth_response(principal_id, resource_arn, effect):
    return {
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": [
                        "execute-api:Invoke",
                    ],
                    "Effect": effect,
                    "Resource": [resource_arn],
                }
            ]
        },
    }
