import ctypes
import signal
import subprocess
import sys
from ctypes import wintypes


def _enable_ctrl_c() -> None:
    kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
    set_console_ctrl_handler = kernel32.SetConsoleCtrlHandler
    set_console_ctrl_handler.argtypes = [ctypes.c_void_p, wintypes.BOOL]
    set_console_ctrl_handler.restype = wintypes.BOOL

    if not set_console_ctrl_handler(None, False):
        raise ctypes.WinError(ctypes.get_last_error())


def main() -> int:
    if sys.platform != 'win32':
        raise RuntimeError('the Windows process launcher can only run on Windows')
    if len(sys.argv) < 2:
        raise RuntimeError('missing child command')

    # CREATE_NEW_PROCESS_GROUP starts with Ctrl+C disabled. Re-enable it before
    # spawning the real command so the child inherits normal Ctrl+C handling.
    _enable_ctrl_c()
    child = subprocess.Popen(sys.argv[1:])

    # The launcher must stay alive while the child handles Ctrl+C. If graceful
    # shutdown times out, CTRL_BREAK_EVENT invokes this handler and force-stops
    # the direct child before the launcher exits.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    def force_stop(signum: int, frame: object) -> None:
        child.kill()

    signal.signal(signal.SIGBREAK, force_stop)
    return child.wait()


if __name__ == '__main__':
    raise SystemExit(main())
