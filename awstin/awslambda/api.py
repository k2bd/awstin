import logging
from abc import ABC, abstractclassmethod

logger = logging.getLogger(__name__)


class LambdaEvent(ABC):
    """
    Abstract data model for handling Lambda events.

    Subclasses must implement the abstract class method ``_from_event``,
    returning an instance of the data model parsed from the loaded event and
    context. The public method ``from_event`` should then be used to parse
    events.

    Using the public method allows ``raw_event`` and ``raw_context`` to be
    accessed if needed, though doing so will emit a warning.
    """

    @property
    def raw_event(self):
        msg = (
            "Using the raw event is discouraged! "
            "Consider expanding the event's data model."
        )
        logger.warning(msg)
        return self._raw_event

    @property
    def raw_context(self):
        msg = (
            "Using the raw context is discouraged! "
            "Consider expanding the event's data model."
        )
        logger.warning(msg)
        return self._raw_context

    @classmethod
    def from_event(cls, event, context):
        """
        Return an instance of the event data model from the raw event and
        context objects.

        The event and context are stored on ``raw_event`` and ``raw_context``
        if needed.

        Parameters
        ----------
        event : dict(str, Any)
            Lambda event JSON
        context : dict(str, Any)
            Lambda context JSON
        """
        result = cls._from_event(event, context)
        result._raw_event = event
        result._raw_context = context

        return result

    @abstractclassmethod
    def _from_event(cls, event, context):
        pass


def lambda_handler(event_type):
    """
    Decorator factory for wrapping a lambda handler in a boilerplate event
    logger and parser.

    Parameters
    ----------
    event_type : LambdaEvent
        The type of event the handler is handling. The handler should accept
        this type of object.

    Returns
    -------
    handler : callable
        Decorator for the Lambda handler, accepting a LambdaEvent. It logs
        the raw incoming event and the result.
    """

    def handler(func):
        def wrapped(event, context):
            logger.info(f"Event: {event!r}")
            parsed = event_type.from_event(event, context)
            result = func(parsed)
            logger.info(f"Result: {result!r}")
            return result
        return wrapped
    return handler
