from fastapi import Request, Response
from fastapi.responses import RedirectResponse, JSONResponse

from fps.hooks import register_exception_handler
from fps.logging import get_configured_logger

logger = get_configured_logger("fps_file_server")


class RedirectException(Exception):
    def __init__(self, reason, redirect_to):
        self.reason = reason
        self.redirect_to = redirect_to


class FileServerError(Exception):
    def __init__(self, msg=None, code=500, status_code=200, data=None):
        self.code = code
        self.status_code = status_code
        self.data = data

        if msg:
            self.msg = msg
        else:
            self.msg = 'error'

    def __str__(self):
        return f'code:{self.code} msg:{self.msg}'


async def exception_handler(request: Request, exc: RedirectException) -> Response:
    logger.warning(f"'{exc.reason}' caused redirection to '{exc.redirect_to}'")
    return RedirectResponse(url=exc.redirect_to)


async def file_server_error_handler(request: Request, exc: FileServerError) -> Response:
    logger.warning(f"file_server_error:{exc}")
    return JSONResponse({'code': exc.code, 'msg': exc.msg, 'data': exc.data}, status_code=exc.status_code)


h = register_exception_handler(RedirectException, exception_handler)
h2 = register_exception_handler(FileServerError, exception_handler)
