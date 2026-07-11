"""Entry point -- kept at the repo root so `python app.py` and the
Dockerfile's `CMD` keep working unchanged. Actual code lives in
cartridge_commander/.
"""
from cartridge_commander.main import run

if __name__ == "__main__":
    run()
