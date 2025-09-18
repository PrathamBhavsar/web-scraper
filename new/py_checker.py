import os

def check_folders(base_path):
    jpg_folders = set()
    png_folders = set()
    mp4_folders = set()
    all_folders = set()

    for root, dirs, files in os.walk(base_path):
        if root == base_path:  # skip the base directory itself
            continue  

        folder_name = os.path.basename(root)  # only folder name
        all_folders.add(folder_name)

        has_jpg = any(f.lower().endswith(".jpg") for f in files)
        has_png = any(f.lower().endswith(".json") for f in files)
        has_mp4 = any(f.lower().endswith(".mp4") for f in files)

        if has_jpg:
            jpg_folders.add(folder_name)
        if has_png:
            png_folders.add(folder_name)
        if has_mp4:
            mp4_folders.add(folder_name)

    # Folders that have all 3
    pass_folders = jpg_folders & png_folders & mp4_folders

    # Folders that fail (missing at least one)
    fail_folders = all_folders - pass_folders

    # Results
    print(f"Total folders scanned: {len(all_folders)}")
    print(f"Folders with .jpg: {len(jpg_folders)}")
    print(f"Folders with .png: {len(png_folders)}")
    print(f"Folders with .mp4: {len(mp4_folders)}\n")

    print("✅ Folders containing all 3 (.jpg, .png, .mp4):")
    print(", ".join(sorted(pass_folders)) if pass_folders else "None")

    print("\n❌ Folders missing at least one:")
    print(", ".join(sorted(fail_folders)) if fail_folders else "None")

    # Double-check totals
    assert len(pass_folders) + len(fail_folders) == len(all_folders), "Count mismatch!"

# Example usage
if __name__ == "__main__":
    base_directory = r"downloads"  # <-- change this
    check_folders(base_directory)
