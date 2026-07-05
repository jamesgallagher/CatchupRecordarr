"""Section 14 - never leak provider credentials into logs or action
responses via an exception's own string representation.

`requests` exceptions (and Dispatcharr's own XtreamClient, which wraps
and re-raises them - core/xtream_codes.py) commonly embed the full
request URL, including the Xtream username/password in the query string
or path, directly in str(exc). A real leak of exactly this kind happened
during development (Session 35): a failed timeshift fetch's exception
was logged and returned to the UI action response with credentials in
plain text, because str(exc) was interpolated directly.

Every place that logs or surfaces an exception from a provider-facing
HTTP call (archive.py, provider.py, download.py) must go through
safe_error_string() instead of str(exc)/%s-formatting the exception
object directly.
"""


def safe_error_string(exc):
    """A generic, credential-free description of exc - the exception
    type name, plus an HTTP status code if the exception carries a
    response object. Never includes the request URL, headers, or body,
    regardless of what the underlying exception's own __str__ contains.
    """
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None) if response is not None else None
    if status:
        return f"HTTP {status} ({type(exc).__name__})"
    return type(exc).__name__
