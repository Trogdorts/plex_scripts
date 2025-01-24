# Plex Episode Renaming Script

A Python script with a menu-driven interface to **configure** a Plex server connection, **connect** to Plex, and **rename** TV show episodes by their media file names.

## Features

1. **Menu-Driven**: Easily load or create config, connect to Plex, and rename episodes without memorizing CLI flags.  
2. **Color-Coded Output**: Green indicates successful connections or operations; red indicates problems.  
3. **Configurable**: Saves (or loads) Plex credentials in `plex_config.json`.  
4. **Logging**: Uses Python's `logging` library for easier troubleshooting.  
5. **Exception Handling**: Catches common errors (e.g., keyboard interrupt, invalid config) and logs them.

## Requirements

- Python 3.6+  
- [plexapi](https://pypi.org/project/plexapi/)  
- [colorama](https://pypi.org/project/colorama/) for cross-platform console colors  

Install dependencies via:
```bash
pip install plexapi colorama
# plex_scripts