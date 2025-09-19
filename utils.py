import zstandard as zstd
import time
import os
import json
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


def compress_file(input_path: Path, output_path: Path, level: int = 3, chunk_size: int = 16 * 1024 * 1024):
    output_path = output_path.joinpath(f"{input_path.name}.zst")
    cctx = zstd.ZstdCompressor(level=level, threads=-1)
    with open(input_path, "rb") as fin, open(output_path, "wb") as fout:
        with cctx.stream_writer(fout) as compressor:
            while chunk := fin.read(chunk_size):
                compressor.write(chunk)
    return input_path, output_path


def compress_files_parallel(file_pairs: list[tuple[Path, Path]], level: int = 3, max_workers: int | None = None):
    """
    file_pairs: [(input_path, output_path), ...]
    """
    results = []

    with Progress(
        TimeRemainingColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.1f}%",
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        total_size = sum(inp.stat().st_size for inp, _ in file_pairs)
        task = progress.add_task(
            f"[cyan]Compressing {len(file_pairs)} files ({total_size/1024/1024:.2f} MB)",
            total=total_size
        )
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(compress_file, inp, out, level): (
                inp, out) for inp, out in file_pairs}
            for future in as_completed(future_map):
                inp, out = future_map[future]
                try:
                    future.result()
                    results.append((inp, out))
                    progress.update(task, advance=inp.stat().st_size,
                                    description=f"[cyan]File: {inp.name[:40]}")
                except Exception as e:
                    print(f"❌ Error compressing {inp}: {e}")
    return results


def decompress_file(input_path: Path, output_path: Path, chunk_size: int = 16 * 1024 * 1024):
    dctx = zstd.ZstdDecompressor()
    with open(input_path, "rb") as fin, open(output_path, "wb") as fout:
        with dctx.stream_reader(fin) as reader:
            while chunk := reader.read(chunk_size):
                fout.write(chunk)
    return input_path, output_path


def decompress_files_parallel(file_pairs: list[tuple[Path, Path]], max_workers: int | None = None):
    """
    file_pairs: [(input_path, output_path), ...]
    """
    results = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(decompress_file, inp, out): (
            inp, out) for inp, out in file_pairs}
        for future in as_completed(future_map):
            inp, out = future_map[future]
            try:
                future.result()
                results.append((inp, out))
            except Exception as e:
                print(f"❌ Error decompressing {inp}: {e}")
    return results


# Temporary
def zst_per_file_decompression(
        source: Path,
        destination: Path,
        chunk_size: int = 16 * 1024 * 1024
):
    start_time = time.time()
    destination = destination.joinpath(source.name)
    destination.mkdir(parents=True, exist_ok=True)

    print(f"• [cyan]Restoring backup [bold]{source.name}[/bold]...[/cyan]")

    # Collect all .zst files
    zst_files = [
        Path(root) / f for root, _, files in os.walk(source)
        for f in files if f.endswith(".zst")
    ]
    total_size = sum(f.stat().st_size for f in zst_files)

    with Progress(
        TimeRemainingColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.1f}%",
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        task_files = progress.add_task(
            f"[cyan]Decompressing {len(zst_files)} files", total=len(zst_files))
        task_size = progress.add_task(
            f"[green]Reading {total_size/1024/1024:.2f} MB", total=total_size)

        dctx = zstd.ZstdDecompressor()

        for zst_file in zst_files:
            # Restore original path (remove .zst)
            relative_path = zst_file.relative_to(source)
            out_path = destination / relative_path.with_suffix("")
            out_path.parent.mkdir(parents=True, exist_ok=True)

            # Decompress
            with open(zst_file, "rb") as fin, open(out_path, "wb") as fout:
                with dctx.stream_reader(fin) as reader:
                    while chunk := reader.read(chunk_size):
                        fout.write(chunk)

            progress.update(task_files, advance=1,
                            description=f"[cyan]File: {out_path.name[:40]}")
            progress.update(task_size, advance=zst_file.stat().st_size)

    elapsed = time.time() - start_time
    print(
        f"  [bold green]Restore complete![/bold green] → {destination} ([yellow]Elapsed[/yellow] {elapsed:.2f}s)")
