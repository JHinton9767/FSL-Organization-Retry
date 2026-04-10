from __future__ import annotations

import sys
from pathlib import Path

from streamlit.web import cli as stcli


def main() -> None:
    target = Path(__file__).resolve().parent / "app" / "main.py"
    sys.argv = ["streamlit", "run", str(target)]
    raise SystemExit(stcli.main())


if __name__ == "__main__":
    main()
