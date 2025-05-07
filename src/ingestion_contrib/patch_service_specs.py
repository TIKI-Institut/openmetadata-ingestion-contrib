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

    patch_service_specs = dict(config.get("patch-service-specs", []))
    if not patch_service_specs:
        print("[*] No service specs defined with key \"patch-service-specs\" in config. Exiting now")
        exit(0)

    print(f"[*] Patch configuration [#{len(patch_service_specs)}]: {patch_service_specs}")

    ingestion_contrib_location = Path(inspect.getmodule(ingestion_contrib).__spec__.submodule_search_locations[0])

    # get the paths of all service_spec.py files of this project recursively and instantiate relative Path objects
    service_spec_source_paths: list[(Str, Path)] = [
        (dirpath.split(os.sep)[-1], os.path.relpath(Path(dirpath + "/service_spec.py"), ingestion_contrib_location))
        for dirpath, _, filename in os.walk(ingestion_contrib_location)
        if "service_spec.py" in filename
        and dirpath.split(os.sep)[-1] in patch_service_specs]

    print(f"[*] Found {len(service_spec_source_paths)} service_spec.py files to patch")

    # get the location of the metadata package from the module itself
    metadata_root_path: Path = Path(inspect.getmodule(metadata).__spec__.submodule_search_locations[0])

    print("[*] Copying following files:" if len(service_spec_source_paths) > 0 else "[*] No files will be copied")

    for name, service_spec_path in service_spec_source_paths:
        destination_file: Path = metadata_root_path / Path(str(service_spec_path).replace(name, patch_service_specs[name]))
        if destination_file.is_file():
            print(f"\t- {service_spec_path} to {destination_file}")
            shutil.copyfile(src=Path(ingestion_contrib_location, service_spec_path), dst=destination_file)


if __name__ == "__main__":
    main()
