from utils import load_metadata, save_metadata, get_size, get_file_hash

import os
import json
import shutil
from pathlib import Path
from rich import print
from datetime import datetime
from zipfile import ZipFile


def backup_file(
        file_path: Path,    # required
        destination: Path,  # required
        meta_file: Path,
        threshold=100*1024*1024  # 100 MB
):
    if not meta_file:
        meta_file = f"{destination}\\backup_meta.json"
    metadata: dict = {}
    if Path(meta_file).exists():
        metadata = load_metadata(meta_file)
    else:
        print("[yellow][!] Backup [red]Meta File[/red] cannot be Found.[/yellow]")

    name, ext = os.path.splitext(os.path.basename(file_path))
    current_hash = get_file_hash(file_path)
    if metadata and metadata.get(name) == current_hash:
        print("[yellow]No changes detected, skipping backup.[/yellow]")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_size = get_size(file_path)
    if file_size < threshold:
        dest_file = os.path.join(
            destination, f"{name}_{timestamp}{ext}"
        )
        shutil.copy2(file_path, dest_file)
        print(
            f"Copied [yellow]{file_path}[/yellow] → [green]{dest_file}[/green]"
        )
    else:
        dest_zip = os.path.join(
            destination, f"{os.path.basename(file_path)}_{timestamp}.zip")
        with ZipFile(dest_zip, "w") as zipf:
            if os.path.isfile(file_path):
                zipf.write(file_path, os.path.basename(file_path))
            else:
                for root, _, files in os.walk(file_path):
                    for fname in files:
                        fpath = os.path.join(root, fname)
                        arcname = os.path.relpath(fpath, file_path)
                        zipf.write(fpath, arcname)
        print(f"Compressed {file_path} → {dest_zip}")

    # update metadata
    metadata[name] = current_hash
    save_metadata(meta_file, metadata)
