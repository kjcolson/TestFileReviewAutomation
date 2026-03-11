"""
Launcher for the PIVOT Test File Review Dashboard.

Usage:
    py run_dashboard.py                  # starts on port 8000
    py run_dashboard.py --port 8080      # custom port
    py run_dashboard.py --no-browser     # don't auto-open browser
"""
import argparse
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description="PIVOT Dashboard Launcher")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    dist = ROOT / "dashboard" / "dist"
    if not dist.exists():
        print("=" * 60)
        print("  Dashboard frontend not built yet.")
        print("  Run the following first:")
        print("    cd dashboard")
        print("    npm install")
        print("    npm run build")
        print("=" * 60)
        sys.exit(1)

    try:
        import uvicorn  # noqa: F401
        import fastapi  # noqa: F401
    except ImportError as exc:
        print("=" * 60)
        print(f"  ERROR: Missing dependency — {exc}")
        print("  Run setup.bat to install required packages.")
        print("=" * 60)
        sys.exit(1)

    url = f"http://localhost:{args.port}"
    print("=" * 60)
    print("  PIVOT Test File Review Dashboard")
    print(f"  Starting server at {url}")
    print("  Press Ctrl+C to stop.")
    print("=" * 60)

    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn",
            "api.main:app",
            "--port", str(args.port),
            "--host", "127.0.0.1",
        ],
        cwd=str(ROOT),
    )

    if not args.no_browser:
        time.sleep(2.0)
        if proc.poll() is not None:
            print("=" * 60)
            print(f"  ERROR: Server exited immediately (code {proc.returncode}).")
            print("  Check the error above, then run setup.bat if packages are missing.")
            print("=" * 60)
            sys.exit(1)
        webbrowser.open(url)

    try:
        exit_code = proc.wait()
    except KeyboardInterrupt:
        print("\nShutting down…")
        proc.terminate()
        exit_code = proc.wait()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
