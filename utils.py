import os
import json
import py7zr
import tempfile
import time
from multiprocessing import Manager
from rich import print
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TransferSpeedColumn
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from blake3 import blake3

CHUNK_SIZE = 16 * 1024 * 1024  # 16 MB


def get_file_hash(path: Path) -> str:
    """Return BLAKE3 hash of a single file."""
    hasher = blake3()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_folder_hash(path: Path, workers: int = max(1, os.cpu_count() or 1)) -> str:
    """Return combined BLAKE3 hash of all files in a folder."""
    hasher = blake3()

    # Collect all files
    files = []
    for root, _, filenames in os.walk(path):
        for fname in filenames:
            files.append(os.path.join(root, fname))
    files.sort()  # ensure consistent ordering
    total_files = len(files)

    with Progress(
        BarColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True
    ) as progress, ProcessPoolExecutor(max_workers=workers) as executor:

        task = progress.add_task("Hashing files...", total=total_files)

        # Dispatch hashing jobs
        futures = {executor.submit(get_file_hash, f): f for f in files}

        for future in as_completed(futures):
            fpath = futures[future]
            try:
                file_hash = future.result()
                hasher.update(file_hash.encode())
            except Exception as e:
                print(f"[!] Failed to hash {fpath}: {e}")
            progress.update(
                task, advance=1, description=f"[blue]Hashing [yellow]{fpath[:80]}...")

    return hasher.hexdigest()


def load_metadata(meta_file) -> dict:
    if os.path.exists(meta_file):
        with open(meta_file, "r") as f:
            return json.load(f)
    return {}


def save_metadata(meta_file, data):
    with open(meta_file, "w") as f:
        json.dump(data, f, indent=2)


def get_size(path) -> int:
    """
    Parameters
    -------
    path (Path)
        The path to file or folder.

    Returns
    -------
    int (bytes)
        Return the size of a file or folder in bytes.
    """
    if os.path.isfile(path):
        return os.path.getsize(path)
    total_size = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            total_size += os.path.getsize(os.path.join(dirpath, f))
    return total_size


# ============ 7 ZIP ============

SKIP_EXT = {
    # Audio
    ".mp3", ".aac", ".ogg", ".opus", ".flac", ".wma", ".m4a",

    # Video
    ".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm",

    # Images
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic", ".tif", ".tiff", ".bmp",

    # Archives / Packages
    ".zip", ".7z", ".rar", ".tar", ".gz", ".bz2", ".xz", ".lz4",
    ".cab", ".iso", ".vhd", ".vhdx",

    # Game Data (custom compressed formats)
    ".pak", ".p4k", ".vpk", ".cpk", ".wad", ".pk3", ".dat",

    # Documents
    ".pdf", ".docx", ".xlsx", ".pptx", ".odt", ".ods", ".odp"
}


def compress_selected_files(src_folder: Path, dest: Path, skip_ext: set = SKIP_EXT):
    """
    Compress selected files into a .7z archive.
    """
    with py7zr.SevenZipFile(dest, 'w') as archive, Progress() as progress:
        # Collect all files first
        files = [f for f in src_folder.rglob("*") if f.is_file()]
        task = progress.add_task(
            "[cyan]Compressing files...", total=len(files))

        for file in files:
            if file.suffix.lower() not in skip_ext:
                rel_path = file.relative_to(src_folder)
                archive.write(file, arcname=str(rel_path))
                progress.console.log(f"✔ Added: {rel_path}")
            else:
                progress.console.log(f"⏩ Skipped: {file}")
            progress.update(task, advance=1)


def decompress_archive(src: Path, dest_folder: Path):
    """
    Decompress a .7z archive into the given destination folder.

    :param src: Path to the .7z archive
    :param dest_folder: Folder where files will be extracted
    """
    with py7zr.SevenZipFile(src, 'r') as archive:
        archive.extractall(path=dest_folder)
