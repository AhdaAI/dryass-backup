""" BACKUP PROGRAM
-- Author : Ahda Akmalul Ilmi

The program will check for file size and make decision to compress it or not, then save the hash for later be used to check any file changes.

This program main purpose is to be used as a steam and epic games backup.
"""

from utils import get_file_hash, compress_selected_files, load_metadata, save_metadata, SKIP_EXT
from multi_thread import multi_hash, multi_copy

import multiprocessing
import time
import os
import shutil
import zipfile
from tempfile import TemporaryDirectory
from pathlib import Path
from rich import print
from rich.progress import Progress, BarColumn, TextColumn

import typer

app = typer.Typer(no_args_is_help=True)


@app.command(no_args_is_help=True)
def backup(source: Path, destination: Path, meta_path: Path | None = None):
    """
    Backup your file or folder.

    --source        : "C:/Program Files/HWiNFO64/HWiNFO Manual.pdf"

    --destination   : "C:/Program Files"
    """
    start_time = time.time()
    source = source.resolve()
    destination = destination.resolve()
    print(f"[cyan]• Source : [bold]{source.name}")
    print(f"[cyan]• Destination : [bold]{destination.name}")
    destination = destination.joinpath(f"{source.name}_backup")
    meta_fname = f"{source.name}_meta.json"

    if source.is_file():  # === File Compression and Zipped ===
        print(f"[green][+] [cyan]Source is a [bold]file[/bold].")
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

    print(f"[green][+] [cyan]Source is a [bold]folder[/bold].")
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        temp_compressed = temp_path.joinpath(f"{source.name}_compressed.zip")
        temp_meta_path = temp_path.joinpath(f"metadata")
        temp_meta_path.mkdir(exist_ok=True)

        metadata = {}
        files = [f for f in source.rglob("*") if f.is_file()]
        hashes, to_zip, to_copy = multi_hash(files, source)

        multi_copy(to_copy, temp_path)  # === Copying files
        metadata['hashes'] = hashes       # === Hashes metadata
        save_metadata(
            temp_meta_path.joinpath(f"{source.name}_metadata.json"),
            metadata
        )
        with Progress(
            BarColumn(),
            "•",
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            "•",
            TextColumn("[progress.description]{task.description}"),
        ) as progress:
            compress_task = progress.add_task(
                "[yellow]Compressing files...", total=len(to_zip))
            with zipfile.ZipFile(
                temp_compressed,
                "w",
                zipfile.ZIP_DEFLATED
            ) as zipf:
                for arcname, file in to_zip:
                    zipf.write(file, arcname)
                    progress.update(
                        compress_task,
                        description=f"[yellow]Compressed [bold]{file.name}.",
                        advance=1
                    )

        # === Storing process
        with Progress(
            BarColumn(),
            "•",
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            "•",
            TextColumn("[progress.description]{task.description}"),
        ) as progress:
            store_task = progress.add_task("[yellow]Storing files...", total=sum(
                len(files) for _, _, files in os.walk(temp_path)))

            with zipfile.ZipFile(
                destination.with_suffix(".zip"),
                "w",
                zipfile.ZIP_STORED
            ) as zf:
                for root, _, files in os.walk(temp_path):
                    for file in files:
                        fpath = Path(root).joinpath(file)
                        arcname = fpath.relative_to(temp_path)
                        zf.write(fpath, arcname)
                        progress.update(
                            store_task,
                            description=f"[yellow]Moved [bold]{fpath.name}.",
                            advance=1
                        )

    elapse = time.time() - start_time
    print(f"[cyan]• Destination : [bold green]{destination}")
    print(f"[cyan]{f" {elapse:.2f} Second ":=^80}")

    return


@app.command(no_args_is_help=True)
def restore(source: Path, destination: Path):
    return


if __name__ == "__main__":
    multiprocessing.freeze_support()
    app()  # Typer
