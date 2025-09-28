import streamlit as st
import json
import time
import subprocess
import psutil
import sys
from pathlib import Path
from scraper.config_loader import load_config
from scraper.storage_manager import total_size_gb

class ScraperController:
    def __init__(self):
        self.config_file = "config.json"
        self.progress_file = "progress.json"
        self.log_file = "logs/scraper.log"

    def load_config(self):
        return load_config()

    def save_config(self, config):
        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        return True

    def load_progress(self):
        if Path(self.progress_file).exists():
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def get_scraper_status(self):
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['cmdline']:
                    cmdline = ' '.join(proc.info['cmdline'])
                    if ('main.py' in cmdline and 'python' in cmdline.lower()):
                        return True, proc.info['pid']
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return False, None

    def start_scraper(self):
        is_running, _ = self.get_scraper_status()
        if is_running:
            return False, "Scraper already running"
        if sys.platform == "win32":
            subprocess.Popen([sys.executable, 'main.py'], creationflags=subprocess.CREATE_NEW_CONSOLE)
        else:
            subprocess.Popen([sys.executable, 'main.py'])
        return True, "Scraper started successfully"

    def stop_scraper(self):
        stopped = 0
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['cmdline']:
                    cmdline = ' '.join(proc.info['cmdline'])
                    if ('main.py' in cmdline and 'python' in cmdline.lower()):
                        proc.terminate()
                        stopped += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return stopped > 0, f"Stopped {stopped} process(es)" if stopped > 0 else "No processes found"

    def get_logs(self, lines=50):
        if Path(self.log_file).exists():
            with open(self.log_file, 'r', encoding='utf-8', errors='ignore') as f:
                log_lines = f.readlines()
            return ''.join(log_lines[-lines:])
        return "No logs available"

def main():
    st.markdown("""
    <style>
        .main-header {background: linear-gradient(90deg, #1f4e79 0%, #2d5aa0 100%);
            padding: 1.5rem; border-radius: 10px; text-align: center; margin-bottom: 2rem;}
        .terminal {background: #000; color: #00ff41; font-family: 'Courier New', monospace;
            padding: 1rem; border-radius: 8px; border: 1px solid #404040; max-height: 350px; overflow-y: auto; font-size: 13px;}
        .stat-bar {background: #262730; padding: 1rem; border-radius: 8px; border: 1px solid #404040; margin-bottom: 1rem;}
        .storage-bar {margin-top: 1rem;}
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="main-header">
        <h1>🎬 Video Scraper Dashboard</h1>
        <p>Minimal control panel for professional scraping</p>
    </div>
    """, unsafe_allow_html=True)

    controller = ScraperController()
    config = controller.load_config()
    progress = controller.load_progress()
    is_running, pid = controller.get_scraper_status()

    # Storage calculation (live from disk)
    download_root = Path(config['general']['download_path'])
    storage_used = total_size_gb(download_root) if download_root.exists() else 0
    max_storage = config['general']['max_storage_gb']

    # Storage bar
    storage_percentage = (storage_used / max_storage * 100) if max_storage > 0 else 0
    st.markdown(f"""
    <div class="storage-bar">
        <b>Storage Usage:</b> {storage_used:.2f} GB / {max_storage} GB
        <div style="background:#222;height:18px;border-radius:8px;overflow:hidden;">
            <div style="background:#00d084;width:{min(storage_percentage,100)}%;height:18px;"></div>
        </div>
        <span style="color:{'#ff453a' if storage_percentage>=100 else '#00d084'};">
            {storage_percentage:.1f}%
        </span>
    </div>
    """, unsafe_allow_html=True)

    # Storage limit adjustment
    st.markdown("<br>", unsafe_allow_html=True)
    new_max_storage = st.number_input("Set Storage Limit (GB)", min_value=1.0, max_value=1000.0, value=float(max_storage), step=0.5)
    if new_max_storage != max_storage:
        config['general']['max_storage_gb'] = new_max_storage
        controller.save_config(config)
        st.success(f"Storage limit updated to {new_max_storage} GB")
        time.sleep(1)
        st.rerun()

    # Start/Stop buttons
    col1, col2 = st.columns([1,1])
    with col1:
        if not is_running and storage_used < new_max_storage:
            if st.button("▶️ Start Scraper", use_container_width=True):
                success, msg = controller.start_scraper()
                if success:
                    st.success(msg)
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error(msg)
        elif is_running:
            st.markdown(f"<b>Scraper Running (PID: {pid})</b>", unsafe_allow_html=True)
    with col2:
        if is_running:
            if st.button("⏹️ Stop Scraper", use_container_width=True):
                success, msg = controller.stop_scraper()
                if success:
                    st.success(msg)
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error(msg)

    # Live terminal/logs
    st.markdown("<h3>Live Terminal</h3>", unsafe_allow_html=True)
    st.markdown(f"""
    <div class="terminal">
        <pre>{controller.get_logs(50)}</pre>
    </div>
    """, unsafe_allow_html=True)

    # Stats from progress.json
    st.markdown("<h3>Scraper Stats</h3>", unsafe_allow_html=True)
    stats = {
        "Last Run At": progress.get("last_run_at", "N/A"),
        "Current Page": progress.get("current_page", "N/A"),
        "Last Scraped Page": progress.get("last_scraped_page", "N/A"),
        "Total Videos Downloaded": progress.get("total_videos_downloaded", "N/A"),
        "Total Size (GB, progress.json)": f"{progress.get('total_size_gb', 0):.2f}"
    }
    for k, v in stats.items():
        st.markdown(f"<div class='stat-bar'><b>{k}:</b> {v}</div>", unsafe_allow_html=True)

    # Storage full message
    if storage_used >= new_max_storage:
        st.error("Storage limit reached! Downloads stopped. Increase limit or cleanup before starting.")

if __name__ == "__main__":
    main()