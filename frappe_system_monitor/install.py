import subprocess
import sys


REQUIRED_PACKAGES = {
    "psutil": "psutil>=5.9.0",
    "websockets": "websockets>=11.0",
    "pymysql": "pymysql>=1.0.0",
    "redis": "redis>=4.0.0",
}


def _is_installed(package_name):
    try:
        __import__(package_name)
        return True
    except ImportError:
        return False


def _pip_install(package_spec):
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", package_spec, "--quiet"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def before_install():
    missing = []
    for pkg_name, pkg_spec in REQUIRED_PACKAGES.items():
        if not _is_installed(pkg_name):
            missing.append(pkg_spec)

    if missing:
        import frappe

        frappe.msgprint(
            "Installing required packages: " + ", ".join(missing),
            alert=True,
        )
        for spec in missing:
            try:
                _pip_install(spec)
            except Exception as e:
                frappe.msgprint(
                    f"Failed to install {spec}: {e}",
                    raise_exception=True,
                )
