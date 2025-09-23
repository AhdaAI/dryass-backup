""" BACKUP PROGRAM
-- Author : Ahda Akmalul Ilmi

The program will check for file size and make decision to compress it or not, then save the hash for later be used to check any file changes.

This program main purpose is to be used as a steam and epic games backup.
"""

from utils import get_file_hash, compress_selected_files, load_metadata, save_metadata, SKIP_EXT

import multiprocessing
import time
import os
import shutil
import zipfile
from tempfile import TemporaryDirectory
from pathlib import Path
from rich import print

import typer

app = typer.Typer(no_args_is_help=True)


@app.command(no_args_is_help=True)
def backup(source: Path, destination: Path, meta_path: Path | None = None):
    """
    Backup your file or folder.

    --source        : "C:/Program Files/HWiNFO64/HWiNFO Manual.pdf"

    --destination   : "C:/Program Files"
    """
    print(f"[cyan]Preparing...")
    source = source.resolve()
    destination = destination.resolve().joinpath(f"{source.name}_backup")
    meta_fname = f"{source.name}_meta.json"

    if source.is_file():  # === File Compression and Zipped ===
        if meta_path:
            meta_file = meta_path.joinpath(meta_fname)
        else:
            meta_file = destination.joinpath(meta_fname)

        destination.mkdir(exist_ok=True)
        metadata = load_metadata(meta_file)
        file_hash = get_file_hash(source)
        if metadata.get(source.name) == file_hash:
            return print(f"[green]No change detected.")
        compress_selected_files(source, destination)
        metadata[source.name] = file_hash
        save_metadata(meta_file, metadata)
        return print(["[green]File successfully compressed."])

    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        temp_compressed = temp_path.joinpath(f"{source.name}_compressed.zip")
        temp_meta_path = temp_path.joinpath(f"metadata")

        structure = {}

        with zipfile.ZipFile(
            temp_compressed,
            "w",
            zipfile.ZIP_DEFLATED
        ) as zipf:
            for files in source.rglob("*"):
                arcname = files.relative_to(source)

                if files.is_file and files.suffix.lower() not in SKIP_EXT:
                    zipf.write(files, arcname)
                    continue

                shutil.copy2(files, temp_dir)

        # === Storing process
        # == This function only store and not compress, this is useful to retain data that are not worth it to compress
        with zipfile.ZipFile(
            destination.with_suffix(".zip"),
            "w",
            zipfile.ZIP_STORED
        ) as zipf:
            for items in temp_path.iterdir():
                arcname = items.relative_to(temp_path)
                zipf.write(items, arcname)

    # destination.mkdir(exist_ok=True)
    # backup_data.mkdir(exist_ok=True)

    # struct_data = {}
    # for root, _, files in os.walk(source):
    #     files_path = []
    #     for file in files:
    #         files_path.append(file)
    #     struct_data[root] = files_path

    # struct = backup_data.joinpath("structure.json")
    # save_metadata(struct, struct_data)

    return


@app.command(no_args_is_help=True)
def restore(source: Path, destination: Path):
    return


if __name__ == "__main__":
    multiprocessing.freeze_support()
    app()  # Typer
