#!/usr/bin/env python3
"""
Plex Shared Downloader w/ JSON Config, Session-Level Resume, File-Level Resume,
AND a fix for "Invalid URL '/library/metadata/...' No scheme supplied" by ensuring
we store and reconnect to the specific shared PlexServer using clientIdentifier.

Features:
  - JSON config (plex_config.json) for storing Plex creds (username/password or token).
  - Connects to MyPlex, lists SHARED servers (not owned by you).
  - Creates or resumes a "download job" (download_job.json) with multi-episode sessions.
  - File-level resume using .tmp + Range requests.
  - Session-level resume skipping completed episodes.
  - ALWAYS fetch items via `server.fetchItem(ratingKey)` from the correct server.
"""

import json
import os
import sys
import logging
import requests
import re
from colorama import init, Fore, Style
from plexapi.myplex import MyPlexAccount
from plexapi.server import PlexServer
from plexapi.video import Episode
from requests.exceptions import RequestException

init(autoreset=True)

# ------------------------------
# CONSTANTS & CONFIG
# ------------------------------
CONFIG_FILE = "plex_config.json"     # Stores username/password/token
JOB_FILE = "download_job.json"       # Tracks multi-episode download jobs
CHUNK_SIZE = 1024 * 256             # 256KB for chunk-based downloads
LOG_LEVEL = logging.WARNING             # Set to logging.DEBUG for more logs
# ------------------------------

# ------------------------------
# LOGGING SETUP
# ------------------------------
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ==============================================================================
# CONFIG / CREDENTIALS FUNCTIONS
# ==============================================================================
def load_config(config_file=CONFIG_FILE):
    """
    Loads Plex configuration (username/password/token) from a JSON file.
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file '{config_file}' not found.")

    with open(config_file, "r") as f:
        data = json.load(f)

    username = data.get("username")
    password = data.get("password")
    token = data.get("token")

    logger.info("Loaded config from '%s'. username=%s token=%s", config_file, username, token)
    return {"username": username, "password": password, "token": token}


def save_config(username=None, password=None, token=None, config_file=CONFIG_FILE):
    """
    Saves Plex configuration (username, password, token) to a JSON file.
    """
    data = {
        "username": username,
        "password": password,
        "token": token
    }
    with open(config_file, "w") as f:
        json.dump(data, f, indent=4)
    logger.info("Saved config to '%s'.", config_file)


def get_plex_token(username, password):
    """
    Retrieve a Plex token by authenticating to MyPlex with username/password.
    """
    logger.info("Attempting to retrieve Plex token from MyPlexAccount.")
    account = MyPlexAccount(username, password)
    token = account.authenticationToken
    logger.info("Got a token from MyPlex.")
    return token


def connect_myplex(username=None, password=None, token=None):
    """
    Connect to MyPlex. If token is provided, use that. Otherwise, use username/password.
    """
    if token:
        logger.info("Connecting to MyPlex with an existing token.")
        return MyPlexAccount(token=token)
    elif username and password:
        logger.info("No token provided; trying to get a new one via MyPlexAccount.")
        new_token = get_plex_token(username, password)
        return MyPlexAccount(token=new_token)
    else:
        raise ValueError("No valid token or username/password for MyPlex connection.")


# ==============================================================================
# JOB STATE (SESSION-LEVEL RESUME)
# ==============================================================================
def load_job(job_file=JOB_FILE):
    """Loads a download job from JSON if it exists, else None."""
    if not os.path.exists(job_file):
        return None
    with open(job_file, "r") as f:
        return json.load(f)


def save_job(job, job_file=JOB_FILE):
    """Saves the job dict to a JSON file."""
    with open(job_file, "w") as f:
        json.dump(job, f, indent=2)
    logger.info("Saved job to '%s'.", job_file)


def create_job(episodes, download_folder, server_client_id, server_name, library_name, show_name):
    """
    Create a new job dict for a set of episodes.
    We store 'serverClientId' so we can reconnect to the correct server for fetchItem.
    """
    job = {
        "serverClientId": server_client_id,   # crucial for reconnecting the correct PlexServer
        "serverName": server_name,
        "libraryName": library_name,
        "showName": show_name,
        "downloadFolder": download_folder,
        "episodes": []
    }
    for ep in episodes:
        job["episodes"].append({
            "ratingKey": ep.ratingKey,
            "title": ep.title or "",
            "seasonNumber": ep.seasonNumber,
            "episodeNumber": ep.index,
            "status": "pending"
        })
    return job


# ==============================================================================
# FILE-LEVEL RESUME LOGIC
# ==============================================================================
def safe_filename(name):
    """Remove or replace characters invalid in most file systems."""
    return "".join(c if c.isalnum() or c in " ._-'()" else "_" for c in (name or ""))


def download_with_resume(episode_obj, plex_server, output_folder):
    """
    Download an episode with file-level resume using .tmp + Range requests.
    - If <file>.mp4 exists, skip.
    - If <file>.tmp exists, try to resume from partial.
    - Returns True if successful, False otherwise.
    """
    show_title = safe_filename(episode_obj.show().title)
    ep_title = safe_filename(episode_obj.title) or ""
    if ep_title:
        filename = f"{show_title} - S{episode_obj.seasonNumber:02}E{episode_obj.index:02} - {ep_title}.mp4"
    else:
        filename = f"{show_title} - S{episode_obj.seasonNumber:02}E{episode_obj.index:02}.mp4"

    final_path = os.path.join(output_folder, filename)
    tmp_path = final_path + ".tmp"

    # If final .mp4 exists, skip
    if os.path.exists(final_path):
        print(Fore.YELLOW + f"[SKIP] {filename} already exists.")
        return True

    if not episode_obj.media or not episode_obj.media[0].parts:
        logger.warning("No media parts found for episode '%s'. Skipping.", episode_obj.title)
        print(Fore.RED + f"[SKIP] No media for '{episode_obj.title}'")
        return False

    part = episode_obj.media[0].parts[0]
    file_url = plex_server.url(part.key)  # ensures a full absolute URL
    token = plex_server._token

    # Check for partial .tmp
    already_downloaded = 0
    mode = "wb"
    if os.path.exists(tmp_path):
        already_downloaded = os.path.getsize(tmp_path)
        if already_downloaded > 0:
            print(Fore.GREEN + f"[RESUME] Found partial .tmp with {already_downloaded} bytes. Attempting Range resume.")
            mode = "ab"
        else:
            os.remove(tmp_path)

    headers = {"X-Plex-Token": token}
    if already_downloaded > 0:
        headers["Range"] = f"bytes={already_downloaded}-"

    # Start HTTP request
    try:
        response = requests.get(file_url, headers=headers, stream=True)
        # if server doesn't honor Range, we get 200 OK instead of 206
        if already_downloaded > 0 and response.status_code != 206:
            logger.warning("Server did not honor Range request (status=%d). Restarting from 0.", response.status_code)
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            already_downloaded = 0
            mode = "wb"
            headers.pop("Range", None)
            response = requests.get(file_url, headers=headers, stream=True)

        response.raise_for_status()

    except RequestException as e:
        logger.error("Failed to request media for '%s': %s", filename, e)
        print(Fore.RED + f"[ERROR] {filename} - request failed: {e}")
        return False

    total_size = int(response.headers.get("Content-Length", 0))
    content_range = response.headers.get("Content-Range")
    full_size = None
    if content_range:
        # e.g. "bytes 1234-9999999/10000000"
        match = re.match(r"bytes (\d+)-(\d+)/(\d+)", content_range)
        if match:
            full_size = int(match.group(3))

    downloaded = already_downloaded
    print(Fore.GREEN + f"[DOWNLOADING] {filename}")
    try:
        with open(tmp_path, mode) as f:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)

                # Display progress
                if full_size:
                    percent_done = 100.0 * downloaded / full_size
                    print(f"\r  => {percent_done:.2f}% of {filename}", end='', flush=True)
                elif total_size and response.status_code == 200:
                    percent_done = 100.0 * downloaded / total_size
                    print(f"\r  => {percent_done:.2f}% of {filename}", end='', flush=True)

        print()  # new line
        final_size = os.path.getsize(tmp_path)
        if final_size == 0:
            logger.warning("Download of '%s' ended up 0 bytes. Removing partial.", filename)
            os.remove(tmp_path)
            print(Fore.RED + f"[FAIL] {filename} is 0 bytes.")
            return False

        os.rename(tmp_path, final_path)
        print(Fore.GREEN + f"[DONE] {filename} ({final_size} bytes)")
        return True

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt while downloading '%s'. Keeping partial for resume.", filename)
        print(Fore.RED + f"\n[INTERRUPT] {filename} paused at {downloaded} bytes.")
        raise
    except Exception as e:
        logger.exception("Error while downloading '%s'.", filename)
        print(Fore.RED + f"[ERROR] {filename} => {e}")
        return False


# ==============================================================================
# FETCHING EPISODES FROM THE CORRECT SHARED SERVER
# ==============================================================================
def find_shared_server_by_client_id(myplex, client_id):
    """
    Given a MyPlexAccount and a clientIdentifier, return the connected PlexServer object.
    """
    for resource in myplex.resources():
        if 'server' in resource.provides and resource.clientIdentifier == client_id:
            return resource.connect()
    return None


def prompt_int_in_range(prompt_msg, min_val, max_val):
    """
    Prompts the user for an integer in [min_val, max_val].
    """
    while True:
        raw = input(prompt_msg).strip()
        try:
            num = int(raw)
            if min_val <= num <= max_val:
                return num
        except ValueError:
            pass
        print(Fore.RED + f"Invalid input. Enter a number between {min_val} and {max_val}.")


def parse_season_ranges(input_str, max_val):
    """
    Parse a comma/range string like '1,2,4-6' into sorted ints <= max_val.
    """
    all_vals = set()
    parts = [x.strip() for x in input_str.split(',')]
    for p in parts:
        if '-' in p:
            s, e = p.split('-', 1)
            if s.isdigit() and e.isdigit():
                s_i, e_i = int(s), int(e)
                if s_i > e_i:
                    s_i, e_i = e_i, s_i
                for v in range(s_i, e_i + 1):
                    if 1 <= v <= max_val:
                        all_vals.add(v)
        else:
            if p.isdigit():
                v = int(p)
                if 1 <= v <= max_val:
                    all_vals.add(v)
    return sorted(all_vals)


def resume_or_create_job(myplex):
    """
    Checks if download_job.json exists. If so, let the user resume or create new.
    Otherwise, create a new job. We store server.clientIdentifier in the job.
    """
    existing_job = load_job(JOB_FILE)
    if existing_job:
        print(Fore.YELLOW + "A download job already exists! Resume (R) or create new (N)?")
        choice = input("Enter R or N [R]: ").strip().lower() or "r"
        if choice.startswith("r"):
            print(Fore.GREEN + "Resuming existing download job.")
            return existing_job
        else:
            print(Fore.CYAN + "Starting a brand new job.")
            # continue to new creation
    # create a new job

    # 1) List shared servers
    shared_resources = []
    for resource in myplex.resources():
        if 'server' in resource.provides and not resource.owned:
            try:
                server = resource.connect()
                shared_resources.append((resource, server))
            except Exception as e:
                logger.warning("Couldn't connect to shared server '%s': %s", resource.name, e)

    if not shared_resources:
        print(Fore.RED + "No shared servers found in your MyPlex account.")
        sys.exit(0)

    print("\nSelect a shared server:")
    for i, (res, srv) in enumerate(shared_resources, start=1):
        print(f"  {i}. {res.name} ({srv.friendlyName})")
    sv_choice = prompt_int_in_range(f"Enter a number (1-{len(shared_resources)}): ", 1, len(shared_resources))
    chosen_resource, chosen_server = shared_resources[sv_choice - 1]

    # 2) pick a TV library
    tv_sections = [s for s in chosen_server.library.sections() if s.type == 'show']
    if not tv_sections:
        print(Fore.RED + "No TV Show libraries found on this server.")
        sys.exit(0)

    print("\nSelect a TV library:")
    for i, sec in enumerate(tv_sections, start=1):
        print(f"  {i}. {sec.title}")
    lib_choice = prompt_int_in_range(f"Enter number (1-{len(tv_sections)}): ", 1, len(tv_sections))
    library_section = tv_sections[lib_choice - 1]

    # 3) pick a show
    shows = library_section.all()
    print("\nSelect a Show to download episodes from:")
    for i, show in enumerate(shows, start=1):
        print(f"  {i}. {show.title}")
    show_choice = prompt_int_in_range(f"Enter number (1-{len(shows)}): ", 1, len(shows))
    selected_show = shows[show_choice - 1]

    # 4) pick how to download
    print(Fore.CYAN + "\nDownload Options:")
    print("  1. All episodes")
    print("  2. A single season")
    print("  3. Multiple seasons (comma/range, e.g. '1,2,4-6')")
    print("  4. A single episode")

    dl_choice = prompt_int_in_range("Enter choice (1-4): ", 1, 4)
    episodes_to_download = []

    if dl_choice == 1:
        episodes_to_download = selected_show.episodes()
    elif dl_choice == 2:
        seasons = selected_show.seasons()
        if not seasons:
            print(Fore.RED + "No seasons found.")
            sys.exit(0)
        for i, s in enumerate(seasons, start=1):
            print(f"  {i}. {s.title}")
        sindex = prompt_int_in_range(f"Pick a season (1-{len(seasons)}): ", 1, len(seasons))
        chosen_season = seasons[sindex - 1]
        episodes_to_download = chosen_season.episodes()
    elif dl_choice == 3:
        # multiple
        seasons = selected_show.seasons()
        if not seasons:
            print(Fore.RED + "No seasons found.")
            sys.exit(0)
        for i, s in enumerate(seasons, start=1):
            print(f"  {i}. {s.title}")
        raw = input("Enter season numbers or ranges (e.g. '1,2,4-6'): ")
        chosen = parse_season_ranges(raw, len(seasons))
        if not chosen:
            print(Fore.RED + "No valid seasons selected.")
            sys.exit(0)
        for c in chosen:
            episodes_to_download.extend(seasons[c - 1].episodes())
    else:
        # single episode
        seasons = selected_show.seasons()
        if not seasons:
            print(Fore.RED + "No seasons found.")
            sys.exit(0)
        for i, s in enumerate(seasons, start=1):
            print(f"  {i}. {s.title}")
        sindex = prompt_int_in_range(f"Pick a season (1-{len(seasons)}): ", 1, len(seasons))
        chosen_season = seasons[sindex - 1]
        eps = chosen_season.episodes()
        if not eps:
            print(Fore.RED + "No episodes found in that season.")
            sys.exit(0)
        for i, e in enumerate(eps, start=1):
            title = e.title or f"S{e.seasonNumber:02}E{e.index:02}"
            print(f"  {i}. {title}")
        eindex = prompt_int_in_range(f"Pick an episode (1-{len(eps)}): ", 1, len(eps))
        episodes_to_download.append(eps[eindex - 1])

    if not episodes_to_download:
        print(Fore.YELLOW + "No episodes selected.")
        sys.exit(0)

    download_folder = input("\nEnter the folder to save downloads (default './downloads'): ") or "./downloads"
    os.makedirs(download_folder, exist_ok=True)

    # Create the job with server clientIdentifier and name
    job = create_job(
        episodes=episodes_to_download,
        download_folder=download_folder,
        server_client_id=chosen_resource.clientIdentifier,
        server_name=chosen_resource.name,
        library_name=library_section.title,
        show_name=selected_show.title
    )
    save_job(job, JOB_FILE)
    print(Fore.GREEN + f"Created new job with {len(job['episodes'])} episode(s).")
    return job


def run():
    """
    Main function. Loads config, connects MyPlex, loads or creates a job,
    reconnects to the correct shared server by clientIdentifier,
    then downloads episodes with chunk-based resume.
    """
    print(Fore.CYAN + "=== Plex Shared Downloader (Fixing 'No scheme supplied') ===\n")

    # 1) Load or create config
    config = {}
    try:
        config = load_config(CONFIG_FILE)
        print(Fore.GREEN + f"Loaded config from {CONFIG_FILE}.")
    except FileNotFoundError:
        print(Fore.YELLOW + f"No config file found ({CONFIG_FILE}). Let's create it.")
        config = {}
    except ValueError as e:
        print(Fore.RED + f"Invalid config file: {e}")
        config = {}

    if not config or (not config.get("token") and not config.get("username")):
        choice = input("No valid Plex config. Create one now? [Y/n]: ").strip().lower() or "y"
        if choice.startswith("y"):
            username = input("Plex.tv username (or blank if you have a token): ").strip()
            password = ""
            token = ""
            if username:
                password = input("Plex.tv password: ").strip()
                try:
                    token = get_plex_token(username, password)
                except Exception as ex:
                    print(Fore.RED + f"Failed to get token: {ex}")
                    sys.exit(1)
                save_config(username=username, password=password, token=token, config_file=CONFIG_FILE)
            else:
                token = input("Enter your existing Plex token: ").strip()
                save_config(token=token, config_file=CONFIG_FILE)

            config = load_config(CONFIG_FILE)
        else:
            print(Fore.RED + "Cannot proceed without config. Exiting.")
            sys.exit(0)

    # 2) Connect to MyPlex
    try:
        myplex = connect_myplex(
            username=config.get("username"),
            password=config.get("password"),
            token=config.get("token")
        )
        print(Fore.GREEN + "Connected to MyPlex successfully!")
    except Exception as e:
        print(Fore.RED + f"Error connecting to MyPlex: {e}")
        sys.exit(1)

    # 3) Load or create job
    job = resume_or_create_job(myplex)
    if not job:
        print(Fore.RED + "No job available. Exiting.")
        sys.exit(0)

    # 4) Reconnect to the correct shared server by clientIdentifier
    client_id = job["serverClientId"]
    server = find_shared_server_by_client_id(myplex, client_id)
    if not server:
        print(Fore.RED + f"Could not find the shared server with clientIdentifier={client_id}.")
        sys.exit(1)

    # 5) Download episodes that are pending
    download_folder = job["downloadFolder"]
    episodes_info = job["episodes"]
    print(Fore.GREEN + f"\nStarting or continuing job with {len(episodes_info)} episodes.")

    try:
        for ep_info in episodes_info:
            if ep_info["status"] == "completed":
                print(Fore.YELLOW + f"Skipping '{ep_info['title']}', already completed.")
                continue

            ratingKey = ep_info["ratingKey"]
            try:
                # Use server.fetchItem(...) to avoid "No scheme supplied" errors
                episode_obj = server.fetchItem(ratingKey)
                if not isinstance(episode_obj, Episode):
                    raise ValueError("Item is not an Episode.")
            except Exception as ex:
                logger.error("Could not fetch episode ratingKey=%s: %s", ratingKey, ex)
                print(Fore.RED + f"[ERROR] Could not fetch episode '{ep_info['title']}' => {ex}")
                ep_info["status"] = "failed"
                save_job(job, JOB_FILE)
                continue

            success = download_with_resume(episode_obj, server, download_folder)
            ep_info["status"] = "completed" if success else "failed"
            save_job(job, JOB_FILE)

        print(Fore.GREEN + "All downloads (attempted).")
    except KeyboardInterrupt:
        print(Fore.RED + "\n[INTERRUPT] User cancelled. Partial progress saved.")
        sys.exit(1)


def main():
    """Wrap run() in a try/except for global error handling."""
    try:
        run()
    except KeyboardInterrupt:
        logger.info("User pressed Ctrl+C, exiting.")
        print(Fore.RED + "\nUser interrupted with Ctrl+C. Goodbye!")
        sys.exit(1)
    except Exception as e:
        logger.exception("A fatal error occurred.")
        print(Fore.RED + f"\n[ERROR] Fatal error: {e}. Check logs for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
