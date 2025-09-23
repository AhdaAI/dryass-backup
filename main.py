""" BACKUP PROGRAM
-- Author : Ahda Akmalul Ilmi

The program will check for file size and make decision to compress it or not, then save the hash for later be used to check any file changes.

This program main purpose is to be used as a steam and epic games backup.
"""

from BACKUP import backup_file, backup_folder
from utils import get_file_hash, compress_selected_files, load_metadata, save_metadata

import multiprocessing
import time
import os
from pathlib import Path
from rich import print

import typer

app = typer.Typer(no_args_is_help=True)


@app.command(no_args_is_help=True)
def backup(source: Path, destination: Path, meta_path: Path = Path.home()):
    """
    Backup your file or folder.

    --source        : "C:/Program Files/HWiNFO64/HWiNFO Manual.pdf"

    --destination   : "C:/Program Files"
    """
    print(f"[cyan]Preparing...")
    source = source.resolve()
    destination = destination.resolve().joinpath(f"{source.name}_backup")
    meta_fname = f"{source.name}_meta.json"
    if meta_path:
        meta_file = meta_path.joinpath(meta_fname)
    else:
        meta_file = destination.joinpath(meta_fname)

    if not destination.exists():
        destination.mkdir(exist_ok=True)

    if source.is_file():  # === File Compression and Zipped ===
        metadata = load_metadata(meta_file)
        file_hash = get_file_hash(source)
        if metadata.get(source.name) == file_hash:
            return print(f"[green]No change detected.")
        compress_selected_files(source, destination)
        metadata[source.name] = file_hash
        save_metadata(meta_file, metadata)
        return print(["[green]File successfully compressed."])

    struct = destination.joinpath("structure.json")
    struct_data = {}
    for root, _, files in os.walk(source):
        files_path = []
        for file in files:
            files_path.append(file)
        struct_data[root] = files_path

    save_metadata(struct, struct_data)

    return


@app.command(no_args_is_help=True)
def restore(source: Path, destination: Path):
    return


if __name__ == "__main__":
    multiprocessing.freeze_support()
    app()  # Typer
