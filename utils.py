import os
import json
import py7zr
import tempfile
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
    ".mp3", ".ogg", ".aac", ".opus", ".flac", ".wma", ".m4a", ".mid", ".midi",

    # Video
    ".mp4", ".avi", ".mkv", ".mov", ".wmv", ".webm", ".flv",

    # Images / Textures
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tga", ".dds", ".webp", ".ico",

    # Archives / Packages
    ".zip", ".rar", ".7z", ".gz", ".tar", ".tgz", ".xz", ".bz2",
    ".pak", ".vpk", ".bnk", ".pck", ".p4k",

    # Game Assets / Misc
    ".apk", ".iso", ".cab", ".cpk", ".dat"
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


def compress_subfolder(subfolder: Path, output_dir: Path, skip_ext: set[str], queue) -> Path:
    """Compress a subfolder into its own temporary .7z archive."""
    archive_path = output_dir / f"{subfolder.name}.7z"
    with py7zr.SevenZipFile(archive_path, 'w') as archive:
        for file in subfolder.rglob("*"):
            if file.is_file() and file.suffix.lower() not in skip_ext:
                queue.put(
                    f"Compressing [bold]{file.name}[/bold] in {subfolder.name}...")
                archive.write(file, arcname=str(
                    file.relative_to(subfolder.parent)))
    return archive_path


def merge_archives(output_file: Path, temp_archives: list[Path]):
    """Extract multiple .7z archives and recompress them into one final archive."""
    with tempfile.TemporaryDirectory() as extract_dir:
        extract_dir = Path(extract_dir)

        # Extract all temporary archives into one folder
        for arc in temp_archives:
            with py7zr.SevenZipFile(arc, 'r') as sub_archive:
                sub_archive.extractall(path=extract_dir)

        # Repack everything into a single .7z
        with py7zr.SevenZipFile(output_file, 'w') as final_archive:
            final_archive.writeall(extract_dir, arcname=".")


def parallel_compress(folder: Path, output_file: Path, skip_ext: set[str] = SKIP_EXT, workers: int = max(1, (os.cpu_count() or 1) - 2)):
    subfolders = [f for f in folder.iterdir() if f.is_dir()]

    if not subfolders:  # no subfolders, compress folder directly
        with py7zr.SevenZipFile(output_file, 'w') as archive:
            for file in folder.rglob("*"):
                if file.is_file() and file.suffix.lower() not in skip_ext:
                    archive.write(
                        file, arcname=str(file.relative_to(folder.parent)))
        return

    with tempfile.TemporaryDirectory() as tmpdir, Manager() as manager:
        tmpdir = Path(tmpdir)
        temp_archives = []
        queue = manager.Queue()

        with Progress(
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            "[progress.description]{task.description}",
        ) as progress, ProcessPoolExecutor(max_workers=workers) as executor:

            task_main = progress.add_task(
                "[cyan]Compressing subfolders...", total=len(subfolders))
            task_secondary = progress.add_task(
                "[green]Waiting for file updates...", total=None)

            futures = {
                executor.submit(compress_subfolder, sf, tmpdir, skip_ext, queue): sf
                for sf in subfolders
            }

            while futures:
                done, _ = as_completed(futures, timeout=0.1), futures

                # Process finished tasks
                for future in list(futures):
                    if future.done():
                        sf = futures.pop(future)
                        try:
                            archive_path = future.result()
                            temp_archives.append(archive_path)
                        except Exception as e:
                            progress.console.print(
                                f"[red]Error compressing {sf}: {e}[/red]")
                        progress.update(
                            task_main, description=f"[cyan]Done: {sf.name}")
                        progress.advance(task_main)

                # Process live file updates from workers
                while not queue.empty():
                    msg = queue.get()
                    progress.update(
                        task_secondary, description=f"[green]{msg}")

        merge_archives(output_file, temp_archives)


def decompress_archive(src: Path, dest_folder: Path):
    """
    Decompress a .7z archive into the given destination folder.

    :param src: Path to the .7z archive
    :param dest_folder: Folder where files will be extracted
    """
    with py7zr.SevenZipFile(src, 'r') as archive:
        archive.extractall(path=dest_folder)
