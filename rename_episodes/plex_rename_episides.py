#!/usr/bin/env python3

import json
import os
import re
import sys
import logging

from colorama import init, Fore, Style
from plexapi.server import PlexServer
from plexapi.exceptions import NotFound, Unauthorized
from plexapi.myplex import MyPlexAccount

# Initialize colorama so ANSI escape codes work on Windows/macOS/Linux consistently
init(autoreset=True)

# ---------------------------------------
# LOGGING SETUP
# ---------------------------------------
# Configure basic logging. You could later expand to a more sophisticated setup
# (e.g., logging to a file, different loggers/handlers, etc.) if desired.
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)

CONFIG_FILE = "plex_config.json"


class PlexManager:
    """
    A class to manage Plex server configuration, connections, and episode-renaming functionality.

    Attributes:
        config_file (str): Path to the JSON config file containing Plex connection details.
        base_url (str): The base URL (including protocol and port) of the Plex server.
        token (str): The Plex authentication token.
        plex (PlexServer or None): An instance of the PlexServer (from plexapi) if connected.

    Methods:
        load_config():
            Loads Plex configuration (base_url/token) from a JSON file.
        save_config(base_url, token):
            Saves the provided base_url and token to a JSON file.
        connect(base_url=None, token=None):
            Connects to Plex with the provided or already-loaded base_url/token.
        validate_ip(ip_address):
            Checks if ip_address is a valid IPv4 address.
        validate_port(port):
            Checks if port is numeric and in the valid range (1–65535).
        get_plex_token(ip, port, username, password, existing_token):
            Retrieves a Plex token (if existing_token not provided).
        rename_episodes_by_filename(library_name, show_name, season_name):
            Renames episodes by setting the title to the file name.
        is_connected -> bool:
            Returns True if self.plex is not None (i.e., connected).
    """

    def __init__(self, config_file=CONFIG_FILE):
        """
        Initialize the PlexManager with a path to the config file.
        Initially, base_url/token/plex are None until loaded or set.
        """
        self.config_file = config_file
        self.base_url = None
        self.token = None
        self.plex = None

    @property
    def is_connected(self):
        """
        Property indicating whether we're currently connected to Plex.
        Returns True if self.plex is not None, False otherwise.
        """
        return self.plex is not None

    def load_config(self):
        """
        Loads Plex server configuration from a JSON file.
        Raises FileNotFoundError if the file does not exist.
        Raises ValueError if base_url or token is missing/invalid.
        """
        logger.info("Attempting to load config file: %s", self.config_file)
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Configuration file '{self.config_file}' not found.")

        try:
            with open(self.config_file, "r") as f:
                data = json.load(f)
                self.base_url = data.get("base_url")
                self.token = data.get("token")
        except (json.JSONDecodeError, IOError) as e:
            logger.exception("Error reading the config file.")
            raise ValueError(f"Error reading the config file: {e}")

        if not self.base_url or not self.token:
            logger.error("Config file is missing 'base_url' or 'token'.")
            raise ValueError("Invalid configuration. 'base_url' or 'token' missing.")

        logger.info("Config loaded successfully. Base URL: %s, Token: %s", self.base_url, self.token)

    def save_config(self, base_url, token):
        """
        Save Plex server configuration (base_url and token) to a JSON file.

        Args:
            base_url (str): e.g. 'http://192.168.1.10:32400'
            token (str): A valid Plex authentication token.
        """
        data = {
            "base_url": base_url,
            "token": token
        }
        try:
            with open(self.config_file, "w") as f:
                json.dump(data, f, indent=4)
            logger.info("Configuration saved to '%s'.", self.config_file)
        except Exception as e:
            logger.exception("Failed to save config.")
            raise e

    def connect(self, base_url=None, token=None):
        """
        Connect to the Plex server using either the loaded config or the provided base_url/token.
        If successful, self.plex is set to a PlexServer instance.
        Otherwise, raises an exception (e.g., Unauthorized or ValueError).

        Args:
            base_url (str, optional): Override for self.base_url.
            token (str, optional): Override for self.token.
        """
        if base_url and token:
            self.base_url = base_url
            self.token = token

        if not self.base_url or not self.token:
            logger.error("Attempted to connect without valid base_url or token.")
            raise ValueError("Base URL or token is missing. Cannot connect to Plex.")

        logger.info("Connecting to Plex server at '%s'.", self.base_url)
        self.plex = PlexServer(self.base_url, self.token)
        logger.info("Connected to Plex server successfully.")

    def validate_ip(self, ip_address):
        """
        Validate IPv4 address using a regex. Returns True if valid, False otherwise.

        Args:
            ip_address (str): e.g. '192.168.1.10'

        Returns:
            bool: True if valid, False otherwise.
        """
        pattern = (
            r"^(25[0-5]|2[0-4]\d|[01]?\d?\d)\."
            r"(25[0-5]|2[0-4]\d|[01]?\d?\d)\."
            r"(25[0-5]|2[0-4]\d|[01]?\d?\d)\."
            r"(25[0-5]|2[0-4]\d|[01]?\d?\d)$"
        )
        return bool(re.match(pattern, ip_address))

    def validate_port(self, port):
        """
        Validate port is an integer in the range 1–65535.

        Args:
            port (str): e.g. '32400'

        Returns:
            bool: True if valid, False otherwise.
        """
        try:
            port_int = int(port)
            return 1 <= port_int <= 65535
        except ValueError:
            return False

    def get_plex_token(self, ip_address, port, username=None, password=None, existing_token=None):
        """
        Retrieve a Plex token. If existing_token is provided, returns it immediately.
        Otherwise logs in to Plex.tv with username/password.

        Args:
            ip_address (str): IP of the Plex server (optional if using universal token).
            port (str): Port of the Plex server (optional if using universal token).
            username (str): Plex.tv username (for retrieving a token).
            password (str): Plex.tv password (for retrieving a token).
            existing_token (str): If you already have a token, pass it here.

        Returns:
            str: A valid Plex authentication token.

        Raises:
            ValueError: If username/password not provided but needed.
            plexapi.myplex.MyPlexAccount.signin exceptions if credentials invalid.
        """
        if existing_token:
            logger.info("Using existing Plex token.")
            return existing_token

        if not username or not password:
            logger.error("Username/password required to retrieve a new token, but missing.")
            raise ValueError("Username and password are required to retrieve a new token.")

        logger.info("Signing into Plex.tv to retrieve token.")
        account = MyPlexAccount(username, password)
        token = account.authenticationToken
        logger.info("Retrieved new Plex token from Plex.tv.")
        return token

    def rename_episodes_by_filename(self, library_name, show_name, season_name=None):
        """
        Renames episodes by setting the episode title to the file's base name.

        Args:
            library_name (str): The name of the Plex library containing the show (e.g. 'TV Shows').
            show_name (str): The name of the show to be renamed (as in Plex).
            season_name (str, optional): e.g., 'Season 2' or 'ALL'. If None or 'ALL', rename all seasons.

        Returns:
            int: The count of episodes renamed.

        Raises:
            ValueError: If library, show, or season is not found.
        """
        if not self.is_connected:
            raise ValueError("Not connected to Plex. Cannot rename episodes.")

        logger.info("Renaming episodes in library='%s', show='%s', season='%s'",
                    library_name, show_name, season_name or "ALL")

        # Retrieve the library (section)
        try:
            library_section = self.plex.library.section(library_name)
        except NotFound:
            logger.error("Library '%s' not found.", library_name)
            raise ValueError(f"Library '{library_name}' not found on the Plex server.")

        # Retrieve the show
        try:
            show = library_section.get(show_name)
        except NotFound:
            logger.error("Show '%s' not found in library '%s'.", show_name, library_name)
            raise ValueError(f"Show '{show_name}' not found in library '{library_name}'.")

        # If the user provided a specific season name, rename only that season
        # Otherwise rename all seasons
        if season_name and season_name.lower() != "all":
            seasons = [s for s in show.seasons() if s.title.lower() == season_name.lower()]
            if not seasons:
                logger.error("Season '%s' not found in show '%s'.", season_name, show_name)
                raise ValueError(f"Season '{season_name}' not found in show '{show_name}'.")
        else:
            seasons = show.seasons()

        renamed_count = 0
        for season in seasons:
            for episode in season.episodes():
                media = episode.media
                if not media or len(media) == 0:
                    print(f"No media found for '{episode.title}'. Skipping.")
                    continue

                media_part = media[0].parts[0]  # Usually only 1 part
                file_path = media_part.file
                if not file_path:
                    print(f"No file path found for '{episode.title}'. Skipping.")
                    continue

                # Extract the base file name (minus extension)
                filename = os.path.splitext(os.path.basename(file_path))[0]
                old_title = episode.title
                new_title = filename

                if old_title != new_title:
                    logger.info("Renaming episode from '%s' -> '%s'", old_title, new_title)
                    print(f"Renaming Episode:\n"
                          f"  Old Title: {old_title}\n"
                          f"  New Title: {new_title}")
                    episode.edit(**{"title.value": new_title, "title.locked": 1})
                    episode.reload()
                    renamed_count += 1

        logger.info("Renamed %d episode(s).", renamed_count)
        print(f"\nDone! Renamed {renamed_count} episode(s).")
        return renamed_count


def prompt_menu(options, title="Choose an option"):
    """
    Utility function to present a numbered list of options and prompt the user to choose.

    Args:
        options (list of str): A list of option labels to display.
        title (str): A label/title for the prompt.

    Returns:
        int: The zero-based index of the chosen option.
    """
    print(f"\n{title}:")
    for idx, option in enumerate(options, start=1):
        print(f"  {idx}. {option}")

    while True:
        choice = input("Enter the number of your choice: ")
        try:
            choice_int = int(choice)
            if 1 <= choice_int <= len(options):
                return choice_int - 1
        except ValueError:
            pass
        print("Invalid selection. Please try again.")


def manage_config_menu(plex_mgr):
    """
    Menu for managing (loading/creating) the config and optionally connecting to Plex.

    Args:
        plex_mgr (PlexManager): The PlexManager instance to configure/connect.
    """
    while True:
        # Show sub-menu
        options = [
            "Load Config from File",
            "Create/Overwrite Config",
            "Connect to Plex",
            "Return to Main Menu"
        ]
        choice = prompt_menu(options, title="Manage Config Menu")

        if choice == 0:
            # Load config from file
            load_config_flow(plex_mgr)

        elif choice == 1:
            # Create or overwrite config
            create_new_config_flow(plex_mgr)

        elif choice == 2:
            # Connect to Plex using loaded config
            connect_to_plex_flow(plex_mgr)

        else:
            # Return
            break


def load_config_flow(plex_mgr):
    """
    Attempts to load the config file (CONFIG_FILE) into plex_mgr.
    If successful, we do not automatically connect to Plex.
    """
    try:
        plex_mgr.load_config()
        print(Fore.GREEN + "Config file loaded successfully.")
        print(f"  Base URL: {plex_mgr.base_url}")
        print(f"  Token   : {plex_mgr.token}")
    except FileNotFoundError:
        print(Fore.RED + f"File '{plex_mgr.config_file}' not found. Please create/overwrite config first.")
    except ValueError as e:
        print(Fore.RED + f"Failed to load config: {e}")


def create_new_config_flow(plex_mgr):
    """
    Wizard function to create or overwrite an existing configuration.
    Guides the user through IP/port, token retrieval, and saving.

    Args:
        plex_mgr (PlexManager): The PlexManager instance.
    """
    print("\nCreating/Overwriting Plex Configuration...")

    method_options = [
        "Enter IP/Port + Existing Plex Token",
        "Enter IP/Port + Plex.tv Username/Password to Retrieve New Token",
        "Cancel & Return"
    ]
    method_choice = prompt_menu(method_options, title="Configuration Method")

    if method_choice == 2:
        print("Cancelled creation of new config.")
        return

    # Prompt for IP/Port
    ip_address = input("Server IP Address (default 192.168.1.20): ") or "192.168.1.20"
    port = input("Server Port (default 32400): ") or "32400"

    # Validate IP/Port
    if not plex_mgr.validate_ip(ip_address):
        print(Fore.RED + f"Error: '{ip_address}' is not a valid IPv4 address.")
        return
    if not plex_mgr.validate_port(port):
        print(Fore.RED + f"Error: '{port}' is not a valid port number.")
        return

    base_url = f"http://{ip_address}:{port}"
    token = None

    if method_choice == 0:
        # IP/Port + existing token
        token = input("Enter your existing Plex token: ").strip()
    else:
        # IP/Port + username/password
        username = input("Plex.tv Username: ").strip()
        password = input("Plex.tv Password: ").strip()
        try:
            token = plex_mgr.get_plex_token(ip_address, port, username=username, password=password)
        except Exception as e:
            print(Fore.RED + f"Error retrieving token: {e}")
            return

    # Ask if we want to save
    do_save = input("Would you like to save this configuration? (y/n) [y]: ") or "y"
    if do_save.lower().startswith("y"):
        plex_mgr.save_config(base_url, token)
        print(Fore.GREEN + f"Configuration saved to '{plex_mgr.config_file}'.")
    else:
        print("Configuration NOT saved. (In-memory only.)")


def connect_to_plex_flow(plex_mgr):
    """
    Attempts to connect to Plex using the current plex_mgr's base_url/token.
    Tells the user if it succeeded or failed.
    """
    try:
        plex_mgr.connect()  # uses whatever is loaded in plex_mgr
        print(Fore.GREEN + "Successfully connected to Plex!")
    except FileNotFoundError:
        print(Fore.RED + "Config file not found. Please load or create config first.")
    except ValueError as e:
        print(Fore.RED + f"Error: {e}")
        print("Make sure config is loaded or created first.")
    except Unauthorized:
        print(Fore.RED + "Token is invalid or server denied access.")
    except Exception as e:
        print(Fore.RED + f"Error connecting to Plex: {e}")


def rename_menu(plex_mgr):
    """
    A menu-driven function for renaming episodes.
    Provides a sub-menu to choose different rename methods (extensible in the future).

    Args:
        plex_mgr (PlexManager): An instance of PlexManager (already connected).
    """
    # If not connected, let the user know
    if not plex_mgr.is_connected:
        print(Fore.RED + "You are not connected to Plex. Please connect before renaming.")
        return

    while True:
        options = [
            "Rename episodes by filename",
            "Placeholder for future rename method(s)",
            "Return to Main Menu"
        ]
        choice = prompt_menu(options, title="Rename Menu")

        if choice == 0:
            rename_by_filename_flow(plex_mgr)
        elif choice == 1:
            print("No alternative rename methods implemented yet.\n")
        else:
            break


def rename_by_filename_flow(plex_mgr):
    """
    Guides the user through selecting a library, show, and season,
    then invokes rename_episodes_by_filename.
    """
    if not plex_mgr.is_connected:
        print(Fore.RED + "You are not connected to Plex! Cannot rename.")
        return

    # Fetch all sections from Plex
    try:
        libraries = plex_mgr.plex.library.sections()
    except Exception as e:
        print(Fore.RED + f"Error fetching libraries: {e}")
        return

    # Filter only 'show'-type libraries
    valid_libraries = [lib for lib in libraries if lib.type.lower() in ('show', 'tvshows', 'tv')]
    if not valid_libraries:
        print("No TV-type libraries found. Cannot rename episodes.")
        return

    # Prompt user to select a library
    library_titles = [lib.title for lib in valid_libraries]
    lib_index = prompt_menu(library_titles, title="Select a Library")
    selected_library = library_titles[lib_index]

    # Fetch all shows in that library
    try:
        library_section = plex_mgr.plex.library.section(selected_library)
        shows = library_section.all()
    except Exception as e:
        print(Fore.RED + f"Error fetching shows from library '{selected_library}': {e}")
        return

    if not shows:
        print(f"No shows found in library '{selected_library}'.")
        return

    # Prompt user to select a show
    show_titles = [show.title for show in shows]
    show_index = prompt_menu(show_titles, title="Select a Show")
    selected_show = show_titles[show_index]

    # Fetch seasons for this show
    try:
        show_obj = library_section.get(selected_show)
        seasons = show_obj.seasons()
    except Exception as e:
        print(Fore.RED + f"Error fetching seasons for show '{selected_show}': {e}")
        return

    if not seasons:
        print(f"No seasons found for '{selected_show}'.")
        return

    # Add "ALL" as an option
    season_titles = ["ALL"] + [s.title for s in seasons]
    season_index = prompt_menu(season_titles, title="Select a Season (or ALL)")
    selected_season = season_titles[season_index]

    # Perform the rename
    try:
        plex_mgr.rename_episodes_by_filename(selected_library, selected_show, selected_season)
    except ValueError as e:
        print(Fore.RED + f"Error during rename: {e}")
    except Exception as e:
        print(Fore.RED + f"Unexpected error: {e}")


def run():
    """
    The core logic, providing the top-level menu with status indicators,
    and calling submenus as appropriate.
    """
    plex_mgr = PlexManager(config_file=CONFIG_FILE)

    while True:
        # Determine current Plex connection status (color-coded)
        if plex_mgr.is_connected:
            status_str = Fore.GREEN + "Connected" + Style.RESET_ALL
        else:
            status_str = Fore.RED + "Not Connected" + Style.RESET_ALL

        print(f"\nPlex Status: {status_str}")
        main_options = [
            "Manage Config (Load/Create/Connect)",
            "Rename Episodes",
            "Quit"
        ]
        choice = prompt_menu(main_options, title="Main Menu")

        if choice == 0:
            # Manage Config
            manage_config_menu(plex_mgr)
        elif choice == 1:
            # Rename
            rename_menu(plex_mgr)
        else:
            print("Goodbye!")
            sys.exit(0)


def main():
    """
    Main entry point. Wraps `run()` in a try/except to handle unexpected errors and keyboard interrupts gracefully.
    """
    try:
        run()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt detected. Exiting gracefully.")
        print("\n" + Fore.RED + "User interrupted the process with Ctrl+C. Goodbye!")
        sys.exit(0)
    except Exception as exc:
        logger.exception("An unexpected error occurred in the main loop.")
        print(
            Fore.RED +
            f"\nA fatal error occurred: {exc}\n"
            "See logs for details. Exiting now."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
