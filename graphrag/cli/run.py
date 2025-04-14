from graphrag.cli.index import index_cli
from pathlib import Path

if __name__ == "__main__":
    index_cli(root_dir=Path("/Users/renzhiping/workspace2/graphrag_root"), method="standard", verbose=True, memprofile=True, cache=True, logger="rich", config_filepath=None, dry_run=False, skip_validation=False, output_dir=None)
