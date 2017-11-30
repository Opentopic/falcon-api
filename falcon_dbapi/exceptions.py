class ApiException(Exception):
    """
    Base class for all API exceptions.
    """
    pass


class ParamException(ApiException):
    """
    Should be raised in custom clean methods when value is invalid.
    """
    pass
