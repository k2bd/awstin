import logging

logger = logging.getLogger(__name__)


def lambda_handler(event_parser):
    """
    Decorator factory for wrapping a lambda handler in a boilerplate event
    logger and parser.

    Parameters
    ----------
    event_parser : callable
        Parser of a lambda handler input. Should take as an input the event and
        context as (dict, dict), and return a list of arguments that the
        wrapped handler should use.

    Returns
    -------
    handler : callable
        Decorator for the Lambda handler, accepting a LambdaEvent. It logs
        the raw incoming event and the result.
    """

    def handler(func):
        def wrapped(event, context):
            logger.info(f"Event: {event!r}")
            args = event_parser(event, context)
            if isinstance(args, tuple):
                result = func(*args)
            elif isinstance(args, dict):
                result = func(**args)
            else:
                result = func(args)
            logger.info(f"Result: {result!r}")
            return result
        return wrapped
    return handler
