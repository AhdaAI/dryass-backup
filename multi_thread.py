from utils import SKIP_EXT, get_file_hash

import shutil
from pathlib import Path
from rich.progress import Progress, BarColumn, TextColumn
from concurrent.futures import ProcessPoolExecutor, as_completed


def process_file(file: Path, source: Path) -> tuple[str, str | None, Path | None]:
    """
    Process a single file:
    - Compute hash
    - If compressible, return arcname + file path for later zipping
    - If skipped, return path for copying

    :params: file: Path

    Full path to the corresponding file.

    :params: source: Path

    Root path of the file.

    :return: tuple(arcname, file hash)
    """
    arcname = file.relative_to(source)
    file_hash = get_file_hash(file)

    if file.suffix.lower() not in SKIP_EXT:
        # compressible file → return for zip writing
        return (str(arcname), file_hash, file)
    else:
        # skipped → must copy later
        return (str(arcname), file_hash, None)


def copy_file(file: tuple[str, Path], destination: Path) -> tuple[str, Path]:
    arcname, fpath = file
    destination = destination.joinpath(arcname)
    destination.parent.mkdir(parents=True, exist_ok=True)

    shutil.copy2(
        fpath,
        destination
    )

    return file


def multi_hash(files: list[Path] | tuple[Path], source: Path) -> tuple[dict, list[tuple[str, Path]], list[tuple[str, Path]]]:
    """
    :return: tuple(hashes, to_zip, to_copy)
    """
    hashes = {}
    to_zip = []
    to_copy = []

    with Progress(
        BarColumn(),
        "•",
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        "•",
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        task = progress.add_task(
            "[yellow]Hashing and classifying files...", total=len(files)
        )

        # Multi threaded
        with ProcessPoolExecutor() as executor:
            futures = {executor.submit(
                process_file, f, source): f for f in files}

            for fut in as_completed(futures):
                arcname, hsh, compressible = fut.result()
                hashes[arcname] = hsh

                if compressible:
                    to_zip.append((arcname, compressible))
                    desc = f"[green]Ready to compress [bold]{compressible.name}"
                else:
                    to_copy.append((arcname, futures[fut]))
                    desc = f"[blue]Marked for copy [bold]{futures[fut].name}"

                progress.update(task, advance=1, description=desc)

        return (hashes, to_zip, to_copy)


def multi_copy(files: list | tuple[Path], destination: Path):
    with Progress(
        BarColumn(),
        "•",
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        "•",
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        task = progress.add_task(
            "[yellow]Copying file...",
            total=len(files)
        )

        with ProcessPoolExecutor() as executor:
            futures = {executor.submit(
                copy_file, f, destination): f for f in files}  # type: ignore

            for fut in as_completed(futures):
                arcname, _ = fut.result()
                progress.update(
                    task,
                    description=f"[yellow]Copied [bold]{arcname}.",
                    advance=1
                )
