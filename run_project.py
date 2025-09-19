import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent

#Simplified with no error handling for time
def main():
    # 1) ETL
    subprocess.run([sys.executable, str(ROOT / "load.py")], check=True)
    # 2) Analysis / charts
    subprocess.run([sys.executable, str(ROOT / "questions.py")], check=True)
    
if __name__ == "__main__":
    main()
