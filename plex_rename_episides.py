#!/usr/bin/env python3

import json
import os
import re
import sys
from plexapi.server import PlexServer
from plexapi.exceptions import NotFound, Unauthorized
from plexapi.myplex import MyPlexAccount

CONFIG_FILE = "plex_config.json"


class PlexManager:
    """
    A class to manage Plex server configuration, connections, and episode-renaming functionality.

    Attributes:
        config_file (str): Path to the JSON config file containing Plex connection details.
        base_url (str): The base URL (including protocol and port) of the Plex server.
        token (str): The Plex authentication token.
        plex (PlexServer): An instance of the PlexServer (from plexapi) if connected.

    Methods:
        load_config():
            Loads the Plex configuration from a JSON file.
        save_config(base_url, token):
            Saves the provided base_url and token to a JSON file.
        connect(base_url=None, token=None):
            Connects to Plex using the provided or already-loaded base_url and token.
        validate_ip(ip_address):
            Validates that a string is a proper IPv4 address.
        validate_port(port):
            Validates that a string is numeric and in the valid port range (1–65535).
        get_plex_token(ip, port, username, password, existing_token):
            Retrieves a Plex token, either directly or from Plex.tv login.
        rename_episodes_by_filename(library_name, show_name, season_name):
            Renames episodes in the specified show/season by using the underlying file name.
    """

    def __init__(self, config_file=CONFIG_FILE):
        """
        Initialize the PlexManager with a path to the config file.
        """
        self.config_file = config_file
        self.base_url = None
        self.token = None
        self.plex = None

    def load_config(self):
        """
        Load Plex server configuration from a JSON file. Raises FileNotFoundError if not present,
        or ValueError if the file lacks base_url/token or is otherwise invalid.
        """
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Configuration file '{self.config_file}' not found.")

        try:
            with open(self.config_file, "r") as f:
                data = json.load(f)
                self.base_url = data.get("base_url")
                self.token = data.get("token")
        except (json.JSONDecodeError, IOError) as e:
            raise ValueError(f"Error reading the config file: {e}")

        if not self.base_url or not self.token:
            raise ValueError("Invalid configuration. 'base_url' or 'token' missing.")

    def save_config(self, base_url, token):
        """
        Save Plex server configuration (base_url and token) to a JSON file.
        Overwrites existing config if present.

        Args:
            base_url (str): e.g. 'http://192.168.1.10:32400'
            token (str): A valid Plex authentication token.
        """
        data = {
            "base_url": base_url,
            "token": token
        }
        with open(self.config_file, "w") as f:
            json.dump(data, f, indent=4)

    def connect(self, base_url=None, token=None):
        """
        Connect to the Plex server using either the loaded config or the provided base_url/token.

        Args:
            base_url (str, optional): If provided, sets/overrides self.base_url.
            token (str, optional): If provided, sets/overrides self.token.

        Raises:
            ValueError: If neither self.base_url nor the provided base_url is valid.
            Unauthorized: If token is invalid or server denies access.
        """
        if base_url and token:
            self.base_url = base_url
            self.token = token

        if not self.base_url or not self.token:
            raise ValueError("Base URL or token is missing. Cannot connect to Plex.")

        self.plex = PlexServer(self.base_url, self.token)

    def validate_ip(self, ip_address):
        """
        Validate IPv4 address using a regex. Returns True if valid, False otherwise.

        Args:
            ip_address (str): The IP address to validate (e.g., '192.168.1.10')

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
            port (str): The port string (e.g., '32400').

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
        Otherwise logs in to Plex.tv to get a token.

        Args:
            ip_address (str): IP of the Plex server (not strictly required if using a universal token).
            port (str): Port of the Plex server (not strictly required if using a universal token).
            username (str): Plex.tv username (for retrieving a token).
            password (str): Plex.tv password (for retrieving a token).
            existing_token (str): If already have a token, pass it here.

        Returns:
            str: A valid Plex authentication token.

        Raises:
            ValueError: If username/password not provided but needed.
            plexapi.myplex.MyPlexAccount.signin related exceptions if credentials are invalid.
        """
        if existing_token:
            return existing_token

        if not username or not password:
            raise ValueError("Username and password are required to retrieve a new token.")

        account = MyPlexAccount(username, password)
        return account.authenticationToken

    def rename_episodes_by_filename(self, library_name, show_name, season_name=None):
        """
        Renames episodes by setting the episode title to the underlying file name (minus extension).

        Args:
            library_name (str): The name of the Plex library containing the show (e.g. 'TV Shows').
            show_name (str): The name of the show to be renamed (e.g. 'Friends').
            season_name (str, optional): The exact name of the season (e.g., 'Season 2').
                If None or 'ALL', all seasons are renamed.

        Raises:
            ValueError: If library or show not found, or season doesn't exist.

        Returns:
            int: The number of episodes successfully renamed.
        """
        # Retrieve the library (section)
        try:
            library_section = self.plex.library.section(library_name)
        except NotFound:
            raise ValueError(f"Library '{library_name}' not found on the Plex server.")

        # Retrieve the show
        try:
            show = library_section.get(show_name)
        except NotFound:
            raise ValueError(f"Show '{show_name}' not found in library '{library_name}'.")

        # If the user provided a specific season, rename only that season
        # Otherwise rename all seasons
        if season_name and season_name.lower() != "all":
            seasons = [s for s in show.seasons() if s.title.lower() == season_name.lower()]
            if not seasons:
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

                # Typically there's 1 part per media
                media_part = media[0].parts[0]
                file_path = media_part.file
                if not file_path:
                    print(f"No file path found for '{episode.title}'. Skipping.")
                    continue

                # Extract the base file name (minus extension)
                filename = os.path.splitext(os.path.basename(file_path))[0]
                old_title = episode.title
                new_title = filename

                if old_title != new_title:
                    print(f"Renaming Episode: {episode.title}")
                    print(f"  Old Title: {old_title}")
                    print(f"  New Title: {new_title}")
                    # Update the metadata
                    episode.edit(**{"title.value": new_title, "title.locked": 1})
                    episode.reload()
                    renamed_count += 1

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


def setup_wizard(plex_mgr):
    """
    A menu-driven setup function that guides the user through:
    - Checking for existing config
    - Testing existing config
    - Creating new config
    - Saving config

    Args:
        plex_mgr (PlexManager): An instance of PlexManager to handle loading/saving and connecting.
    """
    while True:
        # Present a menu for setup actions
        options = []
        config_exists = os.path.exists(CONFIG_FILE)

        if config_exists:
            options = [
                "Use or Test Existing Configuration",
                "Create/Overwrite Configuration",
                "Return to Main Menu",
            ]
        else:
            options = [
                "Create New Configuration (No existing config found)",
                "Return to Main Menu"
            ]

        choice = prompt_menu(options, title="Setup Menu")

        if config_exists:
            # If there's an existing config, interpret the choice:
            if choice == 0:
                # Use or test existing config
                try:
                    plex_mgr.load_config()
                    # Test the connection
                    plex_mgr.connect()
                    print("Successfully connected using existing config!")
                    print(f"Base URL: {plex_mgr.base_url}")
                    print(f"Token  : {plex_mgr.token}")
                except Exception as e:
                    print(f"Failed to use existing config: {e}")
                # Return to this setup menu loop
            elif choice == 1:
                # Overwrite or create new config
                create_new_config(plex_mgr)
            else:
                # Return to main menu
                break
        else:
            # If there's no existing config, interpret the choice:
            if choice == 0:
                create_new_config(plex_mgr)
            else:
                # Return to main menu
                break


def create_new_config(plex_mgr):
    """
    Wizard function to create or overwrite an existing configuration.
    Guides the user through IP/port, token retrieval, and saving.

    Args:
        plex_mgr (PlexManager): An instance of PlexManager.
    """
    print("\nCreating/Overwriting Plex Configuration...")
    # Provide a sub-menu for how to get the token
    method_options = [
        "Enter IP/Port + Existing Plex Token",
        "Enter IP/Port + Plex.tv Username/Password to Retrieve New Token",
        "Cancel & Return"
    ]
    method_choice = prompt_menu(method_options, title="Configuration Method")

    if method_choice == 2:
        print("Cancelled creation of new config.")
        return

    ip_address = input("Server IP Address (default 192.168.1.20): ") or "192.168.1.20"
    port = input("Server Port (default 32400): ") or "32400"

    # Validate IP/Port
    if not plex_mgr.validate_ip(ip_address):
        print(f"Error: '{ip_address}' is not a valid IPv4 address.")
        return
    if not plex_mgr.validate_port(port):
        print(f"Error: '{port}' is not a valid port number.")
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
            print(f"Error retrieving token: {e}")
            return

    # Test the connection
    print("\nTesting connection with the provided details...")
    try:
        plex_mgr.connect(base_url=base_url, token=token)
        print("Successfully connected!")
        print(f"Base URL: {base_url}")
        print(f"Token  : {token}")
    except Exception as e:
        print(f"Failed to connect with the new details: {e}")
        return

    # Ask if we want to save
    do_save = input("Would you like to save this configuration? (y/n) [y]: ") or "y"
    if do_save.lower().startswith("y"):
        plex_mgr.save_config(base_url, token)
        print(f"Configuration saved to '{CONFIG_FILE}'")
    else:
        print("Configuration NOT saved. (In-memory only.)")


def rename_wizard(plex_mgr):
    """
    A menu-driven function for renaming episodes.
    Provides a sub-menu to choose different rename methods (extensible for future).

    Args:
        plex_mgr (PlexManager): An instance of PlexManager (already connected).
    """
    while True:
        options = [
            "Rename episodes by filename",
            "Placeholder for future rename method(s)",
            "Return to Main Menu"
        ]
        choice = prompt_menu(options, title="Rename Menu")

        if choice == 0:
            # Rename by filename
            rename_by_filename_flow(plex_mgr)
        elif choice == 1:
            print("No alternative rename methods implemented yet.\n")
        else:
            # Return to main menu
            break


def rename_by_filename_flow(plex_mgr):
    """
    Guides the user through selecting a library, show, and season, then invokes the
    rename_episodes_by_filename method of PlexManager.

    Args:
        plex_mgr (PlexManager): An instance of PlexManager (already connected).
    """
    # Get all sections from the Plex library
    try:
        libraries = plex_mgr.plex.library.sections()
    except Exception as e:
        print(f"Error fetching libraries: {e}")
        return

    # Filter only 'show'-type libraries
    valid_libraries = [lib for lib in libraries if lib.type.lower() in ('show', 'tvshows', 'tv')]
    if not valid_libraries:
        print("No TV-type libraries found. Cannot rename episodes.")
        return

    # Prompt user to select the library
    library_titles = [lib.title for lib in valid_libraries]
    lib_index = prompt_menu(library_titles, title="Select a Library")
    selected_library = library_titles[lib_index]

    # Fetch all shows in that library
    try:
        library_section = plex_mgr.plex.library.section(selected_library)
        shows = library_section.all()
    except Exception as e:
        print(f"Error fetching shows from library '{selected_library}': {e}")
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
        print(f"Error fetching seasons for show '{selected_show}': {e}")
        return

    if not seasons:
        print(f"No seasons found for '{selected_show}'.")
        return

    # Add "ALL" as an option
    season_titles = ["ALL"] + [s.title for s in seasons]
    season_index = prompt_menu(season_titles, title="Select a Season (or ALL)")
    selected_season = season_titles[season_index]

    # Perform the rename operation
    try:
        plex_mgr.rename_episodes_by_filename(selected_library, selected_show, selected_season)
    except ValueError as e:
        print(f"Error during rename process: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


def main():
    """
    Main entry point: A menu-driven interface to either setup or rename.
    You can easily add new top-level options here as needed.
    """
    # Instantiate our PlexManager (we'll connect or load config in the wizard)
    plex_mgr = PlexManager(config_file=CONFIG_FILE)

    while True:
        # Present the main menu
        main_options = [
            "Setup",
            "Rename",
            "Quit"
        ]
        choice = prompt_menu(main_options, title="Main Menu")

        if choice == 0:
            # Setup Wizard
            setup_wizard(plex_mgr)
        elif choice == 1:
            # Before rename, ensure we can connect. Load existing config or skip if none
            if not os.path.exists(CONFIG_FILE):
                print("\nNo configuration found. Please run setup first.\n")
                continue

            try:
                plex_mgr.load_config()
                plex_mgr.connect()  # tries connecting with loaded config
            except Exception as e:
                print(f"Could not connect with existing config: {e}")
                print("Please run setup to fix this.\n")
                continue

            # If we got here, we are connected
            rename_wizard(plex_mgr)
        else:
            # Quit
            print("Goodbye!")
            sys.exit(0)


if __name__ == "__main__":
    main()
