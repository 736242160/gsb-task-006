import ctypes
import ctypes.wintypes as wt
import subprocess
import sys

CREATE_NO_WINDOW = 0x08000000
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)


class STARTUPINFO(ctypes.Structure):
    _fields_ = [
        ("cb", wt.DWORD), ("lpReserved", wt.LPWSTR), ("lpDesktop", wt.LPWSTR),
        ("lpTitle", wt.LPWSTR), ("dwX", wt.DWORD), ("dwY", wt.DWORD),
        ("dwXSize", wt.DWORD), ("dwYSize", wt.DWORD),
        ("dwXCountChars", wt.DWORD), ("dwYCountChars", wt.DWORD),
        ("dwFillAttribute", wt.DWORD), ("dwFlags", wt.DWORD),
        ("wShowWindow", wt.WORD), ("cbReserved2", wt.WORD),
        ("lpReserved2", ctypes.POINTER(ctypes.c_byte)),
        ("hStdInput", wt.HANDLE), ("hStdOutput", wt.HANDLE),
        ("hStdError", wt.HANDLE),
    ]


class PROCESS_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("hProcess", wt.HANDLE), ("hThread", wt.HANDLE),
        ("dwProcessId", wt.DWORD), ("dwThreadId", wt.DWORD),
    ]


def create_process(exe, args):
    cmd = subprocess.list2cmdline([exe] + args)
    si = STARTUPINFO()
    si.cb = ctypes.sizeof(STARTUPINFO)
    pi = PROCESS_INFORMATION()
    ok = kernel32.CreateProcessW(
        None, ctypes.create_unicode_buffer(cmd), None, None, False,
        CREATE_NO_WINDOW, None, None, ctypes.byref(si), ctypes.byref(pi))
    if not ok:
        raise ctypes.WinError(ctypes.get_last_error())
    kernel32.WaitForSingleObject(pi.hProcess, 0xFFFFFFFF)
    code = wt.DWORD()
    kernel32.GetExitCodeProcess(pi.hProcess, ctypes.byref(code))
    kernel32.CloseHandle(pi.hThread)
    kernel32.CloseHandle(pi.hProcess)
    return code.value


def main():
    exe = sys.argv[1]
    patch_path = sys.argv[2]
    with open(patch_path, "r", encoding="utf-8") as f:
        patch = f.read()
    rc = create_process(exe, ["--codex-run-as-apply-patch", patch])
    sys.exit(rc)


if __name__ == "__main__":
    main()