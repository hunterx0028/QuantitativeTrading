# -*- coding: utf-8 -*-
"""Stage shared application code and a small Docker entry point.

Run before copying docker_space's application files to the Docker build
folder. No trading source is rewritten or imported by this packager.
"""
import ast
import shutil
from pathlib import Path

STRATEGY_FILENAME = "execute_strategy_broken_high_falling_singleton_v17.py"
OVERWRITE_OUTPUT = True


def output_name_for(strategy_filename: str) -> str:
    if "_singleton_" not in strategy_filename:
        raise ValueError("Strategy filename must contain '_singleton_'")
    return strategy_filename.replace("_singleton_", "_docker_", 1)


def transform_singleton_to_docker(source: str, strategy_filename: str = STRATEGY_FILENAME) -> str:
    """Compatibility API: produce an entry point, not a modified strategy copy."""
    tree = ast.parse(source)
    if not any(isinstance(node, ast.FunctionDef) and node.name == "main" for node in tree.body):
        raise ValueError("Shared strategy must define main()")
    module_name = Path(strategy_filename).stem
    if not module_name.isidentifier():
        raise ValueError("Strategy filename must be a valid Python module name")
    return (
        '# -*- coding: utf-8 -*-\n'
        '# Generated entry point. Trading logic lives in the shared application.\n'
        f'from {module_name} import main\n\n\n'
        'if __name__ == "__main__":\n'
        '    main(runtime_mode="docker")\n'
    )


def package_strategy(source_dir: Path, output_dir: Path, *,
                     strategy_filename: str = STRATEGY_FILENAME,
                     overwrite: bool = OVERWRITE_OUTPUT) -> list[Path]:
    """Validate and copy application code; runtime files remain external/S3."""
    source_dir, output_dir = Path(source_dir).resolve(), Path(output_dir).resolve()
    input_path = source_dir / strategy_filename
    output_path = output_dir / output_name_for(strategy_filename)
    if not overwrite and output_path.exists():
        raise FileExistsError(output_path)
    required = [input_path, source_dir / "runtime_environment.py"]
    required += [source_dir / "strategies" / name for name in ("__init__.py", "fall.py", "avg.py", "lower.py")]
    for path in required:
        if not path.is_file():
            raise FileNotFoundError(path)
    payload = [input_path, source_dir / "runtime_environment.py"]
    payload += sorted((source_dir / "strategies").rglob("*.py"))
    for path in payload:
        compile(path.read_text(encoding="utf-8"), str(path), "exec")
    entry_point = transform_singleton_to_docker(input_path.read_text(encoding="utf-8"), strategy_filename)
    compile(entry_point, str(output_path), "exec")
    destinations = []
    for path in payload:
        destination = output_dir / path.relative_to(source_dir)
        if destination.resolve() == path.resolve():
            raise ValueError("Output directory must differ from the application source directory")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
        destinations.append(destination)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path.write_text(entry_point, encoding="utf-8", newline="\n")
    destinations.append(output_path)
    return destinations


def main() -> None:
    output_dir = Path(__file__).resolve().parent
    paths = package_strategy(output_dir.parent, output_dir)
    for path in paths:
        print(f"Staged: {path}")


if __name__ == "__main__":
    main()
