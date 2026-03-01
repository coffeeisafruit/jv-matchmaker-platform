"""
Django middleware for request-level correlation ID tracking.
"""
import uuid

from config.logging_filters import set_correlation_id


class CorrelationIdMiddleware:
    """Generate or propagate a correlation ID for every request."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        cid = request.headers.get("X-Correlation-ID", str(uuid.uuid4())[:8])
        set_correlation_id(cid)
        response = self.get_response(request)
        response["X-Correlation-ID"] = cid
        return response
