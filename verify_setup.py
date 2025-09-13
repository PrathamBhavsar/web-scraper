import os
import sys

def verify_setup():
    """Verify that all required files are present"""
    required_files = [
        'main_scraper.py',
        'config.json',
        'parser_gui.py'
    ]

    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)

    if missing_files:
        print(" Missing required files:")
        for file in missing_files:
            print(f"   - {file}")
        return False
    else:
        print(" All required files found!")
        return True

def check_python():
    """Check Python version and tkinter availability"""
    print(f"Python version: {sys.version}")

    try:
        import tkinter
        print(" tkinter available")
        return True
    except ImportError:
        print(" tkinter not available - GUI won't work")
        return False

if __name__ == "__main__":
    print("=== Web Parser GUI Setup Verification ===\n")

    files_ok = verify_setup()
    python_ok = check_python()

    print()
    if files_ok and python_ok:
        print(" Setup verified! Ready to run the GUI.")
        print("   Double-click 'run_parser_gui.bat' to start!")
    else:
        print("  Setup incomplete. Fix the issues above first.")
