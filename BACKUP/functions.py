from utils import load_metadata, save_metadata, get_size, get_file_hash, get_folder_hash

import os
import json
import shutil
import time
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
                if os.path.isfile(file_path):
                    progress.update(
                        task, description=f"Compressing {name}...")
                    zipf.write(file_path, os.path.basename(file_path))
                else:
                    for root, _, files in os.walk(file_path):
                        for fname in files:
                            progress.update(
                                task, description=f"Compressing {fname}...")
                            fpath = os.path.join(root, fname)
                            arcname = os.path.relpath(fpath, file_path)
                            zipf.write(fpath, arcname)

        print(
            f"Compressed [yellow]{file_path}[/yellow] → [green]{dest_zip}[/green]"
        )

    # update metadata
    metadata[name] = current_hash
    save_metadata(meta_file, metadata)


def backup_folder(folder_path: Path, destination: Path, meta_filename: str = ""):
    start_time = time.time()
    destination = destination.joinpath(folder_path.name)

    if not meta_filename:
        meta_filename = f"{folder_path.name}_meta.json"
        print(f"[green]Meta Filename : {meta_filename}[/green]")

    metadata = load_metadata(f"{destination}\\{meta_filename}")
    if not metadata:
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
    print(f'\n\n[bold blue]{f" {folder_path.name} ":=^50}[/bold blue]\n')
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True
    ) as progress:
        task = progress.add_task(
            "Hashing...", total=None
        )
        for item in folder_path.iterdir():
            if not any(item.iterdir()):
                print(f"• [yellow][bold]{item}[/bold] is empty.[/yellow]")
                progress.update(task, advance=1)
                continue

            progress.update(
                task, description=f"Hashing and Backing Up {item.name}...")
            current_hash = get_folder_hash(item)

            if metadata.get(item.name) == current_hash:
                print(
                    f"• [green][bold]{item.name}[/bold] is Up To Date.[/green]")
                continue

            # Compressing files
            dest_zip = destination.joinpath(f"{item.name}.zip")
            zip_compression(dest_zip, item)

            # Save metadata
            metadata[item.name] = current_hash
            save_metadata(f"{destination}\\{meta_filename}", metadata)

    elapsed = time.time() - start_time
    print(
        f"\n[bold cyan]{f" Time Elapsed : {elapsed:.2f} second ":=^50}[/bold cyan]\n\n")
    return


def zip_compression(destination: Path, folder: Path):
    start_time = time.time()
    print(f"• Backing Up {folder.name}...")
    # Collect all files first (to know size & counts)
    all_files = []
    total_size = 0
    for root, dirs, files in os.walk(folder):
        for fname in files:
            fpath = os.path.join(root, fname)
            all_files.append(fpath)
            total_size += os.path.getsize(fpath)

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
        # Task for files count
        task_files = progress.add_task(
            f"[cyan]Zipping {len(all_files)} files", total=len(all_files)
        )

        # Task for file size
        task_size = progress.add_task(
            f"[green]Writing {total_size/1024/1024:.2f} MB", total=total_size
        )

        with ZipFile(destination, "w", compression=ZIP_DEFLATED, compresslevel=1) as zipf:
            for fpath in all_files:
                arcname = os.path.relpath(fpath, folder)

                # Add to zip
                zipf.write(fpath, arcname)

                # Update progress
                progress.update(task_files, advance=1,
                                description=f"[cyan]File: {arcname}")
                progress.update(task_size, advance=os.path.getsize(fpath))

    # Elapsed time
    elapsed = time.time() - start_time
    print(f"[bold green]Backup complete![/bold green] → {destination}")
    print(
        f"[bold green]Time Elapsed[/bold green] {elapsed:.2f} [green]seconds[/green]")
