from utils import load_metadata, save_metadata, get_size, get_file_hash, get_folder_hash, compress_file, compress_files_parallel

import os
import json
import shutil
import time
import tarfile
import zstandard as zstd
from pathlib import Path
from rich import print
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TransferSpeedColumn, TimeRemainingColumn
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED


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
        # Compressing logic
        print(f"Compressing {name.replace(' ', '_')}...")
        dest_zip = os.path.join(
            destination, f"{name.replace(' ', '-')}_{timestamp}.zip")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True
        ) as progress:
            task = progress.add_task("Compressing...", total=None)
            with ZipFile(dest_zip, "w", compression=ZIP_DEFLATED, compresslevel=9) as zipf:
                progress.update(
                    task, description=f"Compressing {name}..."
                )
                zipf.write(file_path, os.path.basename(file_path))

        print(
            f"Compressed [yellow]{file_path}[/yellow] → [green]{dest_zip}[/green]"
        )

    # update metadata
    metadata[name] = current_hash
    save_metadata(meta_file, metadata)


def backup_folder(folder_path: Path, destination: Path, meta_filename: str = ""):
    if not meta_filename:
        meta_filename = f"{folder_path.name}_meta.json"
        print(f"[green]Meta Filename : {meta_filename}[/green]")

    start_time = time.time()
    destination = destination.joinpath(folder_path.name)
    metadata_path = destination.joinpath(f"metadata\\{meta_filename}")

    metadata = load_metadata(metadata_path)
    if not metadata:
        destination.joinpath("metadata").mkdir(exist_ok=True)
        print('[yellow]Metadata not found.[/yellow]')

    # Steam folder change...
    if folder_path.name == "SteamLibrary":
        folder_path = folder_path.joinpath("steamapps\\common\\")
        print(
            f"[yellow]Steam Library Detected\nSteam games path = [bold]{folder_path}[/bold][/yellow]")

    # Checking and creating folder
    if not destination.exists():
        destination.mkdir(exist_ok=True)

    # Hashing folders...
    print(f'\n\n[bold blue]{f" {folder_path} ":=^50}[/bold blue]\n')
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True
    ) as progress:
        task = progress.add_task(
            "Checking...", total=None
        )
        for item in folder_path.iterdir():  # Iterating through items in folder
            iter_start = time.time()
            progress.update(
                task, description=f"Checking {item.name}...")

            if not any(item.iterdir()):
                print(
                    f"• [yellow][bold]{item}[/bold] is empty. [{time.time() - iter_start} second][/yellow]"
                )
                progress.update(task, advance=1)
                continue

            current_hash = get_folder_hash(item)

            # Checking if back up is needed
            if metadata.get(item.name) == current_hash:
                print(
                    f"• [green][bold]{item.name}[/bold] → Up To Date. [{time.time() - iter_start} second][/green]"
                )
                continue

            progress.update(
                task,
                description=f"Backing up {item.name}..."
            )

            # Compressing files
            dest_folder = destination.joinpath(f"{item.name}")
            dest_folder.mkdir(exist_ok=True)
            files_tuple: list = []

            for root, _, files in os.walk(item):
                root_path = Path(root)
                relative = root_path.relative_to(item)
                dest_relative = dest_folder.joinpath(relative)
                dest_relative.mkdir(exist_ok=True)
                for file in sorted(files):
                    files_tuple.append(
                        (root_path.joinpath(file), dest_relative)
                    )

            compress_files_parallel(files_tuple)

            # Save metadata
            metadata[item.name] = current_hash
            save_metadata(metadata_path, metadata)
            print(
                f"• [green][bold]{item.name}[/bold] Added to metadata. [{time.time() - iter_start:.2f} second][/green]"
            )

    elapsed = time.time() - start_time
    minute = 0
    if elapsed > 60:
        minute = elapsed / 60
    print(
        f"\n[bold cyan]{f" Time Elapsed : {minute:.2f} Minute {elapsed:.2f} Second ":=^50}[/bold cyan]\n\n")
    return
