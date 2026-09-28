"""Small WSGI adapter for the framework-neutral HTTP signaling protocol."""

from http import HTTPStatus

from .relay import HTTPResult, relative_path


class SignalingWSGI:
    def __init__(self, protocol, prefix="/signal"):
        self.protocol = protocol
        self.prefix = prefix

    def __call__(self, environ, start_response):
        try:
            length = int(environ.get("CONTENT_LENGTH") or "0")
        except ValueError:
            length = -1
        if length < 0:
            result = HTTPResult(400)
        elif length > self.protocol.max_message_bytes:
            result = HTTPResult(413)
        else:
            body = environ["wsgi.input"].read(length)
            headers = {
                name[5:].replace("_", "-"): value
                for name, value in environ.items() if name.startswith("HTTP_")
            }
            path = relative_path(environ.get("PATH_INFO", ""), self.prefix)
            result = self.protocol.handle(
                environ["REQUEST_METHOD"], path,
                environ.get("QUERY_STRING", ""), headers, body,
            )
        response_headers = {"Cache-Control": "no-store",
                            "Content-Length": str(len(result.body))}
        response_headers.update(result.headers)
        status = HTTPStatus(result.status)
        start_response(f"{status.value} {status.phrase}", list(response_headers.items()))
        return [result.body]
