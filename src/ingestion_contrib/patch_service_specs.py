import inspect
import os
import shutil
from pathlib import Path

import ingestion_contrib
import metadata
import yaml


def main():
    with open("./config.yaml") as config_file:
        config: dict = yaml.safe_load(config_file)

    patch_files: dict | None = config.get("patch-files")
    if not patch_files:
        print("[*] No entries defined with key \"patch-files\" in config. Exiting now")
        exit(0)

    print(f"[*] Patch configuration [#{len(patch_files)}]: {patch_files}")

    ingestion_contrib_location = Path(inspect.getmodule(ingestion_contrib).__spec__.submodule_search_locations[0])

    # get the location of the metadata package from the module itself
    metadata_root_path: Path = Path(inspect.getmodule(metadata).__spec__.submodule_search_locations[0])

    # get the paths of all specified files of this project and instantiate relative Path objects
    source_paths = []
    for source_dir, config in patch_files.items():
        for dirpath, _, _ in os.walk(ingestion_contrib_location):
            if (dirname := dirpath.split(os.sep)[-1]) == source_dir:
                subpath = dirpath.replace(str(ingestion_contrib_location), "").replace(dirname, "")
        source_paths.append((source_dir, patch_files[source_dir]["target_dir"], subpath, [file_path for file_path in patch_files[source_dir]["files"]]))

    print(f"[*] Found {len(source_paths)} source directories for patching")

    print("[*] Copying following files:" if len(source_paths) > 0 else "[*] No files will be copied")

    for source_dir, destination_dir_name, subpath, file_paths in source_paths:
        destination_dir: Path = metadata_root_path / Path(subpath) / Path(destination_dir_name)
        for file_path in file_paths:
            destination = destination_dir / Path(file_path)
            print(f"\t- {Path(ingestion_contrib_location, subpath, source_dir, file_path)} -> {destination}")
            shutil.copyfile(src=Path(ingestion_contrib_location, subpath, source_dir, file_path), dst=destination)



if __name__ == "__main__":
    main()
