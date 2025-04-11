import os
from pathlib import Path
import inspect
import metadata
import shutil
import yaml

with open("./config.yml") as config_file:
    config: dict = yaml.safe_load(config_file)

patch_service_specs = config.get("patch-service-specs", [])
if patch_service_specs is None or patch_service_specs == []:
    print("[*] No service specs defined with key \"patch-service-specs\" in config. Exiting now")
    exit(0)

print(f"[*] Searching for service_spec.py files in {config['rootdir']}")
# get the paths of all service_spec.py files in the root folder recursively and instantiate Path objects
service_spec_source_paths: list[Path] = [Path(dirpath + "/service_spec.py")
                                         for dirpath, _, filename in os.walk(config["rootdir"])
                                         if "service_spec.py" in filename
                                         and dirpath.split(os.sep)[-1] in patch_service_specs]

print(f"[*] Found {len(service_spec_source_paths)} service_spec.py files")

# get the location of the metadata package from the module itself
metadata_root_path: Path = Path(inspect.getmodule(metadata).__spec__.submodule_search_locations[0])

print("[*] Copying following files:" if len(service_spec_source_paths) > 0 else "[*] No files will be copied")

for service_spec_path in service_spec_source_paths:
    destination_file: Path = metadata_root_path / service_spec_path
    if destination_file.is_file():
        print(f"\t- {str(service_spec_path)} to {destination_file}")
        shutil.copyfile(src=service_spec_path, dst=destination_file)