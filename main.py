import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parent
    src_path = root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    if len(sys.argv) > 1:
        from pdf2xlsx.cli import main as cli_main

        return cli_main()

    from pdf2xlsx.gui.app_pygame import start as gui_start

    gui_start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
