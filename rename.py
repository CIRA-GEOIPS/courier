#!/usr/bin/env python3

import os
import sys
import shutil
import argparse
from concurrent.futures import ThreadPoolExecutor, Future
from typing import List, Tuple, Set, Dict, Callable, Optional, Any, Iterator
import fnmatch
from functools import partial

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, TaskID
from rich.panel import Panel
from rich.text import Text
from rich import print as rprint
import logging
import rich_argparse

# --- Configuration ---
PLACEHOLDER: str = "PROJECT"  # The exact string to find and replace
DEFAULT_IGNORE_DIRS: Set[str] = {'.git', '.svn', '.hg', '__pycache__', 'node_modules', 'venv', '.venv'}
DEFAULT_IGNORE_FILES: Set[str] = {'*.pyc', '*.pyo', '*.so', '*.dll', '*.exe', '*.bin', '*.jpg', '*.png', '*.gif'}
DEFAULT_MAX_FILE_SIZE: int = 10 * 1024 * 1024  # 10MB

# Set up rich console and logging
console: Console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, console=console)]
)
log = logging.getLogger("project_renamer")

def is_binary_file(file_path: str, sample_size: int = 8000) -> bool:
    """
    Check if a file is binary by reading a sample.
    
    Parameters
    ----------
    file_path : str
        Path to the file to check
    sample_size : int, optional
        Number of bytes to read for sampling, by default 8000
    
    Returns
    -------
    bool
        True if the file appears to be binary, False otherwise
    
    Notes
    -----
    This function uses two heuristics to detect binary files:
    1. Presence of null bytes
    2. Failure to decode as UTF-8
    """
    try:
        if os.path.getsize(file_path) == 0:
            return False
            
        with open(file_path, 'rb') as f:
            sample = f.read(sample_size)
            
        # Files with null bytes are likely binary
        if b'\x00' in sample:
            return True
            
        # Try to decode as text
        try:
            sample.decode('utf-8')
            return False
        except UnicodeDecodeError:
            return True
    except (IOError, OSError):
        return True  # If we can't open/read the file, treat as binary to be safe

def matches_any_pattern(name: str, patterns: Set[str]) -> bool:
    """
    Check if a name matches any of the given patterns.
    
    Parameters
    ----------
    name : str
        The name to check against patterns
    patterns : Set[str]
        Set of glob patterns to match against
    
    Returns
    -------
    bool
        True if the name matches any pattern, False otherwise
    """
    return any(fnmatch.fnmatch(name, pattern) for pattern in patterns)

def should_ignore_path(path: str, ignore_dirs: Set[str], ignore_files: Set[str]) -> bool:
    """
    Check if a path should be ignored based on configured patterns.
    
    Parameters
    ----------
    path : str
        The file or directory path to check
    ignore_dirs : Set[str]
        Set of directory patterns to ignore
    ignore_files : Set[str]
        Set of file patterns to ignore
    
    Returns
    -------
    bool
        True if the path should be ignored, False otherwise
    """
    path_parts = path.split(os.sep)
    
    # Check if any part of the path matches ignored directories
    if any(matches_any_pattern(part, ignore_dirs) for part in path_parts):
        return True
    
    # Check if filename matches ignored files
    if os.path.isfile(path) and matches_any_pattern(os.path.basename(path), ignore_files):
        return True
        
    return False

def read_file_content(file_path: str) -> Optional[str]:
    """
    Read and return file content, or None if file can't be read.
    
    Parameters
    ----------
    file_path : str
        Path to the file to read
    
    Returns
    -------
    Optional[str]
        File content as string if successful, None otherwise
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read()
    except Exception:
        return None

def write_file_content(file_path: str, content: str) -> bool:
    """
    Write content to file, return success status.
    
    Parameters
    ----------
    file_path : str
        Path to the file to write
    content : str
        Content to write to the file
    
    Returns
    -------
    bool
        True if write was successful, False otherwise
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    except Exception:
        return False

def replace_content(content: str, placeholder: str, replacement: str) -> Tuple[str, bool]:
    """
    Replace placeholder with replacement in content.
    
    Parameters
    ----------
    content : str
        The original content
    placeholder : str
        The placeholder string to find
    replacement : str
        The replacement string
    
    Returns
    -------
    Tuple[str, bool]
        A tuple containing:
        - The new content with replacements
        - Boolean indicating whether any replacements were made
    """
    new_content = content.replace(placeholder, replacement)
    return new_content, new_content != content

def rename_file_if_needed(file_path: str, placeholder: str, replacement: str) -> Tuple[str, bool, str]:
    """
    Rename file if needed.
    
    Parameters
    ----------
    file_path : str
        Path to the file to potentially rename
    placeholder : str
        The placeholder string to find in the filename
    replacement : str
        The replacement string for the filename
    
    Returns
    -------
    Tuple[str, bool, str]
        A tuple containing:
        - The new file path (or original if not renamed)
        - Boolean indicating whether file was renamed
        - Log message describing the action taken
    """
    dir_path, filename = os.path.split(file_path)
    
    if placeholder not in filename:
        return file_path, False, ""
        
    new_filename = filename.replace(placeholder, replacement)
    new_file_path = os.path.join(dir_path, new_filename)
    
    if os.path.exists(new_file_path):
        return file_path, False, f"SKIPPING file rename: Target '{new_file_path}' already exists."
    
    try:
        shutil.move(file_path, new_file_path)
        return new_file_path, True, f"Renamed file: '{file_path}' -> '{new_file_path}'"
    except OSError as e:
        return file_path, False, f"ERROR renaming file '{file_path}' to '{new_file_path}': {str(e)}"

def process_file(file_path: str, placeholder: str, replacement: str, max_size: int) -> Tuple[str, bool, bool]:
    """
    Process a single file: update content and/or rename if needed.
    
    Parameters
    ----------
    file_path : str
        Path to the file to process
    placeholder : str
        The placeholder string to find and replace
    replacement : str
        The replacement string
    max_size : int
        Maximum file size in bytes to process
    
    Returns
    -------
    Tuple[str, bool, bool]
        A tuple containing:
        - Log message describing actions taken
        - Boolean indicating whether content was updated
        - Boolean indicating whether file was renamed
    """
    if os.path.getsize(file_path) > max_size:
        return (f"Skipped (too large): {file_path}", False, False)
        
    if is_binary_file(file_path):
        return (f"Skipped (binary): {file_path}", False, False)
    
    # Replace content if needed
    content = read_file_content(file_path)
    if not content:
        return (f"Error reading file: {file_path}", False, False)
    
    new_content, content_updated = replace_content(content, placeholder, replacement)
    
    log_messages: List[str] = []
    if content_updated:
        if write_file_content(file_path, new_content):
            log_messages.append(f"Updated content in: '{file_path}'")
        else:
            log_messages.append(f"Error updating content in: '{file_path}'")
            content_updated = False
    
    # Rename file if needed
    new_path, file_renamed, rename_log = rename_file_if_needed(file_path, placeholder, replacement)
    if rename_log:
        log_messages.append(rename_log)
    
    return ("\n".join(log_messages), content_updated, file_renamed)

def collect_paths(root_dir: str, ignore_dirs: Set[str], ignore_files: Set[str]) -> Tuple[List[str], List[str]]:
    """
    Collect all directories and files to process.
    
    Parameters
    ----------
    root_dir : str
        Root directory to start the search from
    ignore_dirs : Set[str]
        Set of directory patterns to ignore
    ignore_files : Set[str]
        Set of file patterns to ignore
    
    Returns
    -------
    Tuple[List[str], List[str]]
        A tuple containing:
        - List of directory paths to process
        - List of file paths to process
    
    Notes
    -----
    Directories are sorted by depth (deepest first) to ensure proper processing order.
    """
    all_dirs: List[str] = []
    all_files: List[str] = []
    
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=True):
        # Filter out directories to ignore
        dirnames[:] = [d for d in dirnames if not should_ignore_path(os.path.join(dirpath, d), ignore_dirs, ignore_files)]
        
        all_dirs.extend(os.path.join(dirpath, dirname) for dirname in dirnames)
        all_files.extend(
            os.path.join(dirpath, filename) for filename in filenames 
            if not should_ignore_path(os.path.join(dirpath, filename), ignore_dirs, ignore_files)
        )
    
    # Sort directories by depth (descending) to process deepest directories first
    all_dirs.sort(key=lambda x: x.count(os.sep), reverse=True)
    
    return all_dirs, all_files

def rename_directory(dir_path: str, placeholder: str, replacement: str, dry_run: bool) -> Tuple[bool, str]:
    """
    Rename a directory if needed.
    
    Parameters
    ----------
    dir_path : str
        Path to the directory to potentially rename
    placeholder : str
        The placeholder string to find in the directory name
    replacement : str
        The replacement string for the directory name
    dry_run : bool
        If True, only report what would be done without making changes
    
    Returns
    -------
    Tuple[bool, str]
        A tuple containing:
        - Boolean indicating success or whether the directory would be renamed
        - Log message describing the action taken or that would be taken
    """
    dir_name = os.path.basename(dir_path)
    
    if placeholder not in dir_name:
        return False, ""
        
    new_dir_name = dir_name.replace(placeholder, replacement)
    parent_dir = os.path.dirname(dir_path)
    new_dir_path = os.path.join(parent_dir, new_dir_name)
    
    if os.path.exists(new_dir_path):
        return False, f"SKIPPING directory rename: Target '{new_dir_path}' already exists."
        
    if dry_run:
        return True, f"Would rename directory: '{dir_path}' -> '{new_dir_path}'"
    
    try:
        shutil.move(dir_path, new_dir_path)
        return True, f"Renamed directory: '{dir_path}' -> '{new_dir_path}'"
    except OSError as e:
        return False, f"ERROR renaming directory '{dir_path}' to '{new_dir_path}': {e}"

def process_dry_run_file(file_path: str, placeholder: str, replacement: str, max_size: int) -> Tuple[bool, bool, str]:
    """
    Check if a file would be updated or renamed in dry run mode.
    
    Parameters
    ----------
    file_path : str
        Path to the file to check
    placeholder : str
        The placeholder string to find
    replacement : str
        The replacement string
    max_size : int
        Maximum file size in bytes to process
    
    Returns
    -------
    Tuple[bool, bool, str]
        A tuple containing:
        - Boolean indicating whether content would be updated
        - Boolean indicating whether file would be renamed
        - Log message describing what would be done
    """
    if os.path.getsize(file_path) > max_size or is_binary_file(file_path):
        return False, False, ""
        
    filename = os.path.basename(file_path)
    would_rename = placeholder in filename
    would_update = False
    log_messages: List[str] = []
    
    content = read_file_content(file_path)
    if content:
        would_update = placeholder in content
    
    if would_update:
        log_messages.append(f"Would update content in: '{file_path}'")
    
    if would_rename:
        new_filename = filename.replace(placeholder, replacement)
        new_path = os.path.join(os.path.dirname(file_path), new_filename)
        log_messages.append(f"Would rename file: '{file_path}' -> '{new_path}'")
    
    return would_update, would_rename, "\n".join(log_messages)

def rename_and_replace(
    root_dir: str, 
    new_project_name: str, 
    ignore_dirs: Set[str] = DEFAULT_IGNORE_DIRS,
    ignore_files: Set[str] = DEFAULT_IGNORE_FILES,
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    dry_run: bool = False
) -> None:
    """
    Renames directories, files, and replaces content within files.
    
    Parameters
    ----------
    root_dir : str
        Root directory to process
    new_project_name : str
        New name to replace the placeholder with
    ignore_dirs : Set[str], optional
        Set of directory patterns to ignore, by default DEFAULT_IGNORE_DIRS
    ignore_files : Set[str], optional
        Set of file patterns to ignore, by default DEFAULT_IGNORE_FILES
    max_file_size : int, optional
        Maximum file size in bytes to process, by default DEFAULT_MAX_FILE_SIZE
    dry_run : bool, optional
        If True, only report what would be done without making changes, by default False
    
    Returns
    -------
    None
    
    Notes
    -----
    The function processes in the following order:
    1. Directories (deepest first)
    2. Files (content and names)
    """
    if new_project_name == PLACEHOLDER:
        log.warning(f"New project name ('{new_project_name}') is the same as the placeholder ('{PLACEHOLDER}').")
        log.warning("No changes will be made.")
        return

    root_dir = os.path.abspath(root_dir)
    
    # Display intro panel
    title = "[bold blue]Project Renamer[/bold blue]"
    if dry_run:
        title += " [bold yellow](DRY RUN)[/bold yellow]"
    
    intro_text = Text.from_markup(
        f"Processing directory: [cyan]{root_dir}[/cyan]\n"
        f"Replacing all instances of [yellow]'{PLACEHOLDER}'[/yellow] with [green]'{new_project_name}'[/green]"
    )
    console.print(Panel(intro_text, title=title))

    # Collect all directories and files
    with console.status("[bold green]Scanning files and directories...[/bold green]"):
        all_dirs, all_files = collect_paths(root_dir, ignore_dirs, ignore_files)
    
    # --- Step 1: Rename Directories (bottom-up) ---
    console.print("\n[bold]Renaming Directories[/bold]", style="blue")
    
    rename_dir_fn = partial(rename_directory, placeholder=PLACEHOLDER, replacement=new_project_name, dry_run=dry_run)
    
    renamed_dirs: List[str] = []
    with Progress() as progress:
        task = progress.add_task("[cyan]Processing directories...", total=len(all_dirs))
        
        for dir_path in all_dirs:
            success, log_msg = rename_dir_fn(dir_path)
            if success and log_msg:
                renamed_dirs.append(log_msg)
            progress.update(task, advance=1)
    
    for log in renamed_dirs:
        if "Would rename" in log:
            console.print(f"[yellow]{log}[/yellow]")
        else:
            console.print(f"[green]{log}[/green]")
    
    renamed_dirs_count = len(renamed_dirs)
    if renamed_dirs_count == 0:
        console.print("[dim]No directories needed renaming or matched the placeholder.[/dim]")

    # --- Step 2: Process Files (update content and rename) ---
    console.print("\n[bold]Updating File Contents and Renaming Files[/bold]", style="blue")
    
    # Update the file paths if directories were renamed
    if renamed_dirs_count > 0 and not dry_run:
        # Re-scan for files since directory paths have changed
        with console.status("[bold green]Re-scanning files after directory renames...[/bold green]"):
            _, all_files = collect_paths(root_dir, ignore_dirs, ignore_files)
    
    stats: Dict[str, int] = {"updated": 0, "renamed": 0, "skipped": 0}
    
    if dry_run:
        # Process files in dry run mode
        dry_run_fn = partial(process_dry_run_file, placeholder=PLACEHOLDER, replacement=new_project_name, max_size=max_file_size)
        
        with Progress() as progress:
            task = progress.add_task("[cyan]Checking files...", total=len(all_files))
            
            for file_path in all_files:
                would_update, would_rename, log = dry_run_fn(file_path)
                if log:
                    console.print(f"[yellow]{log}[/yellow]")
                if would_update:
                    stats["updated"] += 1
                if would_rename:
                    stats["renamed"] += 1
                if not (would_update or would_rename):
                    stats["skipped"] += 1
                progress.update(task, advance=1)
    else:
        # Process files in parallel for better performance
        with Progress() as progress:
            task = progress.add_task("[cyan]Processing files...", total=len(all_files))
            processed_count = 0
            
            def update_progress(future: Future) -> None:
                """
                Callback to update progress bar when a task completes.
                
                Parameters
                ----------
                future : Future
                    The completed future object
                """
                nonlocal processed_count
                processed_count += 1
                progress.update(task, completed=processed_count)
            
            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                process_fn = partial(process_file, placeholder=PLACEHOLDER, replacement=new_project_name, max_size=max_file_size)
                
                # Submit all tasks and register the callback
                futures: List[Future] = []
                for file_path in all_files:
                    future = executor.submit(process_fn, file_path)
                    future.add_done_callback(update_progress)
                    futures.append(future)
                
                # Process results as they complete
                for future in futures:
                    try:
                        log_message, content_updated, file_renamed = future.result()
                        if log_message:
                            if "Error" in log_message:
                                console.print(f"[red]{log_message}[/red]")
                            elif "Skipped" in log_message:
                                console.print(f"[dim]{log_message}[/dim]")
                            else:
                                console.print(f"[green]{log_message}[/green]")
                        
                        if content_updated:
                            stats["updated"] += 1
                        if file_renamed:
                            stats["renamed"] += 1
                        if not (content_updated or file_renamed) and "Skipped" in log_message:
                            stats["skipped"] += 1
                    except Exception as e:
                        log.error(f"Error processing file: {e}")

    # --- Summary ---
    summary_text = Text()
    summary_text.append("\nSUMMARY:\n", style="bold")
    summary_text.append(f"  Directories {'that would be' if dry_run else ''} renamed: ", style="dim")
    summary_text.append(f"{renamed_dirs_count}\n", style="green" if renamed_dirs_count > 0 else "dim")
    
    summary_text.append(f"  Files {'that would have' if dry_run else 'with'} updated content: ", style="dim")
    summary_text.append(f"{stats['updated']}\n", style="green" if stats['updated'] > 0 else "dim")
    
    summary_text.append(f"  Files {'that would be' if dry_run else ''} renamed: ", style="dim")
    summary_text.append(f"{stats['renamed']}\n", style="green" if stats['renamed'] > 0 else "dim")
    
    summary_text.append(f"  Files skipped: ", style="dim")
    summary_text.append(f"{stats['skipped']}", style="yellow" if stats['skipped'] > 0 else "dim") 

    console.print(Panel(summary_text, title="[bold blue]Results[/bold blue]"))

def main() -> None:
    """
    Main entry point for the script.
    
    Parses command line arguments and calls the rename_and_replace function.
    """
    parser = argparse.ArgumentParser(
        description=f"Replace '{PLACEHOLDER}' with your project name in directories, files, and file contents.",
        formatter_class=rich_argparse.RichHelpFormatter
    )
    
    parser.add_argument('project_name', 
                       help=f"New project name to replace '{PLACEHOLDER}'")
    
    parser.add_argument('--directory', '-d', 
                       default='.', 
                       help="Root directory to process (default: current directory)")
    
    parser.add_argument('--dry-run', 
                       action='store_true', 
                       help="Show what would be changed without making actual changes")
    
    parser.add_argument('--max-size', 
                       type=int, 
                       default=DEFAULT_MAX_FILE_SIZE,
                       help=f"Maximum file size in bytes to process (default: {DEFAULT_MAX_FILE_SIZE})")
    
    parser.add_argument('--ignore-dirs', 
                       nargs='+', 
                       default=list(DEFAULT_IGNORE_DIRS),
                       help=f"Directories to ignore (default: {', '.join(DEFAULT_IGNORE_DIRS)})")
    
    parser.add_argument('--ignore-files', 
                       nargs='+', 
                       default=list(DEFAULT_IGNORE_FILES),
                       help=f"File patterns to ignore (default: {', '.join(DEFAULT_IGNORE_FILES)})")
    
    args = parser.parse_args()
    
    try:
        rename_and_replace(
            args.directory, 
            args.project_name,
            ignore_dirs=set(args.ignore_dirs),
            ignore_files=set(args.ignore_files),
            max_file_size=args.max_size,
            dry_run=args.dry_run
        )
    except KeyboardInterrupt:
        console.print("\n[bold red]Operation cancelled by user.[/bold red]")
        sys.exit(1)
    except Exception as e:
        log.exception(f"An error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
