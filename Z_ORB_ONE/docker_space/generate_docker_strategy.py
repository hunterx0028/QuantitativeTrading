# -*- coding: utf-8 -*-
"""
Generate an AWS/Fargate-ready docker strategy file from a singleton strategy.

Set STRATEGY_FILENAME below, then run:
    python generate_docker_strategy.py
"""

import ast
import os
import re
from pathlib import Path


STRATEGY_FILENAME = "execute_strategy_broken_high_falling_singleton_v17.py"
OVERWRITE_OUTPUT = True


DOCKER_HELPERS = '''

# ============ Docker/Fargate 專用工具 ============
S3_BUCKET = os.getenv("QUANT_S3_BUCKET", "leegueishen-quant-trading-17")
S3_PREFIX = os.getenv("QUANT_S3_PREFIX", "exchange").strip("/")
stock_data = None
selected_stocks = []
selected_limit_up_stocks = []
selected_limit_down_stocks = []
market_previous_close_indices = {}

REQUIRED_RUNTIME_FILES = (
    "config.ini",
    "stock_data.py",
    "T122260516_20260828.p12",
)


def is_truthy_env(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")


def should_download_from_s3() -> bool:
    """
    預設行為：
    - AWS/Fargate 環境：下載 S3 檔案。
    - 本機環境：若 /app 下缺少三個必要檔案，也嘗試下載。
    可用 QUANT_DOWNLOAD_FROM_S3=true/false 強制控制。
    """
    override = os.getenv("QUANT_DOWNLOAD_FROM_S3")
    if override is not None:
        return is_truthy_env("QUANT_DOWNLOAD_FROM_S3")

    running_in_aws = bool(os.getenv("AWS_EXECUTION_ENV") or os.getenv("ECS_CONTAINER_METADATA_URI_V4"))
    missing_file = any(not os.path.exists(os.path.join(BASE_DIR, fname)) for fname in REQUIRED_RUNTIME_FILES)
    return running_in_aws or missing_file


def download_runtime_files_from_s3():
    """從 S3 下載 config.ini、stock_data.py、p12 憑證到 /app。"""
    if not should_download_from_s3():
        print("===== Skip S3 download: runtime files already exist locally =====")
        return

    print("===== Download runtime files from S3 =====")
    for fname in REQUIRED_RUNTIME_FILES:
        s3_uri = f"s3://{S3_BUCKET}/{S3_PREFIX}/{fname}" if S3_PREFIX else f"s3://{S3_BUCKET}/{fname}"
        local_path = os.path.join(BASE_DIR, fname)
        print(f"[S3] {s3_uri} -> {local_path}")
        import subprocess
        subprocess.run(
            ["aws", "s3", "cp", s3_uri, local_path],
            check=True,
        )


def load_stock_data_from_runtime_file():
    """
    延後載入 stock_data.py。
    這樣在 Fargate 啟動時，可以先從 S3 下載 stock_data.py，再載入策略需要的資料。
    """
    global stock_data, selected_stocks, selected_limit_up_stocks
    global selected_limit_down_stocks, market_previous_close_indices

    import importlib.util
    import sys

    stock_data_path = os.path.join(BASE_DIR, "stock_data.py")
    if not os.path.exists(stock_data_path):
        raise FileNotFoundError(f"stock_data.py not found: {stock_data_path}")

    spec = importlib.util.spec_from_file_location("stock_data", stock_data_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load stock_data.py from {stock_data_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules["stock_data"] = module

    required_stock_data_fields = (
        "selected_stocks",
        "selected_limit_up_stocks",
        "selected_limit_down_stocks",
        "market_previous_close_indices",
    )
    missing_fields = [
        field_name
        for field_name in required_stock_data_fields
        if not hasattr(module, field_name)
    ]
    if missing_fields:
        raise AttributeError(
            "stock_data.py does not define required field(s): "
            + ", ".join(missing_fields)
        )

    stock_data = module
    selected_stocks = module.selected_stocks
    selected_limit_up_stocks = module.selected_limit_up_stocks
    selected_limit_down_stocks = module.selected_limit_down_stocks
    market_previous_close_indices = module.market_previous_close_indices
    return selected_stocks


def build_login_stdin() -> io.StringIO:
    """
    玉山 SDK 在 Linux container 內會互動式詢問密碼。
    主要由 patch_getpass_from_env() 回答密碼 prompt；這裡保留 stdin
    fallback，避免 SDK 內部使用 input() 時在 ECS/Fargate 卡住。
    """
    required_envs = ("ESUN_PASSWORD", "CERT_PASSWORD")
    missing = [name for name in required_envs if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    login_lines = [os.environ["ESUN_PASSWORD"], os.environ["CERT_PASSWORD"]] * 8
    return io.StringIO("\\n".join(login_lines) + "\\n")


def configure_noninteractive_keyring() -> None:
    """
    ECS/Fargate 沒有可互動的 OS keyring。
    若使用預設 keyring/keyrings.alt，可能會要求建立 keyring master password。
    """
    os.environ.setdefault("PYTHON_KEYRING_BACKEND", "keyring.backends.null.Keyring")
    try:
        import keyring
        from keyring.backends.null import Keyring

        keyring.set_keyring(Keyring())
        print("===== Python keyring backend: null (non-interactive) =====")
    except Exception as e:
        print(f"[WARN] Unable to force null keyring backend: {e}")


def patch_getpass_from_env():
    """讓 SDK 的 getpass prompt 改從環境變數取密碼。"""
    import getpass

    original_getpass = getpass.getpass
    patched_module_getpass = []

    last_password = None

    def env_getpass(prompt: str = "Password: ", stream: Any = None) -> str:
        nonlocal last_password

        prompt_lower = str(prompt or "").casefold()

        if "cert" in prompt_lower and "password" in prompt_lower:
            env_name = "CERT_PASSWORD"
            value = os.getenv(env_name)
        elif "confirm" in prompt_lower and "password" in prompt_lower:
            if last_password is None:
                raise RuntimeError(f"Received confirm prompt before any password prompt: {prompt!r}")
            env_name = "LAST_PASSWORD"
            value = last_password
        elif "password" in prompt_lower:
            env_name = "ESUN_PASSWORD"
            value = os.getenv(env_name)
        else:
            raise RuntimeError(f"Unknown password prompt: {prompt!r}")

        if not value:
            raise RuntimeError(f"Missing required environment variable for prompt {prompt!r}: {env_name}")

        last_password = value
        print(f"[AUTH] {prompt.strip()} -> {env_name}")
        return value

    getpass.getpass = env_getpass
    for module_name, module in list(sys.modules.items()):
        if not module_name.startswith(("esun_trade", "esun_marketdata")):
            continue
        if getattr(module, "getpass", None) is original_getpass:
            setattr(module, "getpass", env_getpass)
            patched_module_getpass.append(module)

    return getpass, original_getpass, patched_module_getpass


def login_sdks(config: ConfigParser) -> tuple[EsunMarketdata, SDK]:
    """建立並登入行情與交易 SDK。"""
    configure_noninteractive_keyring()
    realtime_sdk = EsunMarketdata(config)
    sdk = SDK(config)

    original_stdin = sys.stdin
    getpass_module, original_getpass, patched_module_getpass = patch_getpass_from_env()
    sys.stdin = build_login_stdin()
    try:
        print("===== Login EsunMarketdata =====")
        realtime_sdk.login()
        print("===== EsunMarketdata login success =====")

        print("===== Login Esun Trade SDK =====")
        sdk.login()
        print("===== Esun Trade SDK login success =====")
    finally:
        sys.stdin = original_stdin
        getpass_module.getpass = original_getpass
        for module in patched_module_getpass:
            module.getpass = original_getpass

    return realtime_sdk, sdk


def safe_logout_sdk(name: str, sdk_obj: Any):
    if sdk_obj is None:
        return
    try:
        sdk_obj.logout()
        print(f"===== {name} logout success =====")
    except Exception as e:
        print(f"[WARN] {name} logout skipped/failed: {e}")
'''


DOCKER_PERSIST_SELECTED_STOCKS = '''def persist_selected_stocks_to_stock_data(
    stocks: List[Tuple[str, int, float, float, float, float, str, float, Tuple[int, int]]]
):
    """
    Docker/Fargate 版不回寫 /app/stock_data.py。
    /app 是 task 生命週期內的暫存檔案系統，回寫 stock_data.py 不會持久化到 S3。
    selected_stocks 本身會在 initialize_states() 透過 stocks[:] 更新為過濾後名單。
    """
    return
'''


DOCKER_PERSIST_ENTRY_MODE = '''def persist_entry_mode_to_stock_data(entry_mode: int) -> None:
    """
    Docker/Fargate 版只更新 stock_data module 記憶體，不回寫 /app/stock_data.py。
    後續 get_current_entry_mode() 會讀取這個 module 屬性。
    """
    stock_data.entry_mode = entry_mode
'''


def output_name_for(strategy_filename: str) -> str:
    if "_singleton_" not in strategy_filename:
        raise ValueError("Strategy filename must contain '_singleton_'")
    return strategy_filename.replace("_singleton_", "_docker_", 1)


def replace_top_level_function(source: str, function_name: str, replacement: str) -> str:
    tree = ast.parse(source)
    lines = source.splitlines(keepends=True)

    for node in tree.body:
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == function_name
        ):
            start = node.lineno - 1
            end = node.end_lineno
            return "".join(lines[:start]) + replacement.rstrip() + "\n\n" + "".join(lines[end:])

    raise ValueError(f"Cannot find function: {function_name}")


def replace_once_required(source: str, old: str, new: str, description: str) -> str:
    """Replace one Docker-specific hook, failing instead of silently dropping behavior."""
    occurrences = source.count(old)
    if occurrences != 1:
        raise ValueError(
            f"Expected exactly one {description}, found {occurrences}; "
            "the singleton startup flow may have changed"
        )
    return source.replace(old, new, 1)


def transform_main_for_docker(source: str) -> str:
    """Keep the singleton main flow and patch only container-specific operations."""
    source = replace_once_required(
        source,
        "    market_index_ws = None\n\n    try:\n",
        "    realtime_sdk = None\n"
        "    sdk = None\n"
        "    market_index_ws = None\n\n"
        "    try:\n"
        "        print(\"===== Prepare runtime files =====\")\n"
        "        download_runtime_files_from_s3()\n"
        "        load_stock_data_from_runtime_file()\n\n",
        "main resource initialization",
    )
    source = replace_once_required(
        source,
        "        realtime_sdk = EsunMarketdata(config)\n"
        "        try:\n"
        "            realtime_sdk.login()\n"
        "            trade_log(\"LOGIN_OK\", api=\"marketdata\")\n"
        "        except Exception as exc:\n"
        "            trade_log(\"LOGIN_ERROR\", error=True, api=\"marketdata\", error_msg=repr(exc))\n"
        "            raise\n",
        "        realtime_sdk, sdk = login_sdks(config)\n",
        "market-data login block",
    )
    source = replace_once_required(
        source,
        "        sdk = SDK(config)\n"
        "        try:\n"
        "            sdk.login()\n"
        "            trade_log(\"LOGIN_OK\", api=\"trade\")\n"
        "        except Exception as exc:\n"
        "            trade_log(\"LOGIN_ERROR\", error=True, api=\"trade\", error_msg=repr(exc))\n"
        "            raise\n",
        "",
        "trade SDK login block",
    )
    source = replace_once_required(
        source,
        "        close_market_index_stream(market_index_ws)\n",
        "        close_market_index_stream(market_index_ws)\n"
        "        safe_logout_sdk(\"Esun Trade SDK\", sdk)\n"
        "        safe_logout_sdk(\"EsunMarketdata\", realtime_sdk)\n",
        "market-index cleanup hook",
    )
    return source


def transform_singleton_to_docker(source: str) -> str:
    source = source.replace("\r\n", "\n")

    # Docker 版必須先從 S3 下載 stock_data.py，不能在 module import 階段載入。
    # 分別移除兩種 import，避免 stock_data 新增匯出名稱時，因完整字串不符而殘留。
    source = re.sub(r"(?m)^import stock_data\s*\n", "", source, count=1)
    source = re.sub(r"(?m)^from stock_data import [^\n]+\n", "", source, count=1)
    source = source.replace("\nstock_data.entry_mode = ENTRY_MODE_NO_TRADE\n", "\n")

    marker = "# ============ 下單函式 ============"
    if marker not in source:
        raise ValueError(f"Cannot find insertion marker: {marker}")
    if "# ============ Docker/Fargate 專用工具 ============" not in source:
        source = source.replace(marker, DOCKER_HELPERS.rstrip() + "\n\n" + marker, 1)

    source = re.sub(
        r'def now_tpe\(\) -> datetime:\n    return datetime\.now\(pytz\.timezone\("Asia/Taipei"\)\)',
        "def now_tpe() -> datetime:\n    return datetime.now(TZ)",
        source,
        count=1,
    )

    source = replace_top_level_function(
        source,
        "persist_selected_stocks_to_stock_data",
        DOCKER_PERSIST_SELECTED_STOCKS,
    )
    source = replace_top_level_function(
        source,
        "persist_entry_mode_to_stock_data",
        DOCKER_PERSIST_ENTRY_MODE,
    )

    return transform_main_for_docker(source)


def main() -> None:
    output_dir = Path(__file__).resolve().parent
    source_dir = output_dir.parent
    input_path = source_dir / STRATEGY_FILENAME
    output_path = output_dir / output_name_for(STRATEGY_FILENAME)

    if not input_path.exists():
        raise FileNotFoundError(input_path)
    if output_path.exists() and not OVERWRITE_OUTPUT:
        raise FileExistsError(output_path)

    source = input_path.read_text(encoding="utf-8")
    output = transform_singleton_to_docker(source)
    output_path.write_text(output, encoding="utf-8", newline="\n")
    print(f"Generated: {output_path}")


if __name__ == "__main__":
    main()
