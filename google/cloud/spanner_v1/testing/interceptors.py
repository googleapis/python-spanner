from collections import defaultdict
from grpc_interceptor import ClientInterceptor
from google.api_core.exceptions import Aborted


class MethodCountInterceptor(ClientInterceptor):
    """Test interceptor that counts number of times a method is being called."""

    def __init__(self):
        self._counts = defaultdict(int)

    def intercept(self, method, request_or_iterator, call_details):
        """Count number of times a method is being called."""
        self._counts[call_details.method] += 1
        return method(request_or_iterator, call_details)


class MethodAbortInterceptor(ClientInterceptor):
    """Test interceptor that throws Aborted exception for a specific method."""

    def __init__(self):
        self._method_to_abort = None
        self._count = 0
        self._max_retry_count = 1

    def intercept(self, method, request_or_iterator, call_details):
        if (
            self._count < self._max_retry_count
            and call_details.method == self._method_to_abort
        ):
            self._count += 1
            raise Aborted("Thrown from ClientInterceptor for testing")
        return method(request_or_iterator, call_details)

    def set_method_to_abort(self, method_to_abort):
        self._method_to_abort = method_to_abort

    def reset(self):
        """Reset the interceptor to the original state."""
        self._method_to_abort = None
        self._count = 0
