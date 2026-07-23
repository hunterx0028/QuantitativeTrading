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
    global stock_data, selected_stocks, market_previous_close_indices

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

    if not hasattr(module, "selected_stocks"):
        raise AttributeError("stock_data.py does not define selected_stocks")

    stock_data = module
    selected_stocks = module.selected_stocks
    market_previous_close_indices = getattr(module, "market_previous_close_indices", {})
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

    def env_getpass(prompt: str = "Password: ", stream: Any = None) -> str:
        prompt_lower = str(prompt or "").lower()
        if "cert" in prompt_lower or "憑證" in prompt_lower:
            env_name = "CERT_PASSWORD"
        elif "keyring" in prompt_lower:
            env_name = "KEYRING_PASSWORD" if os.getenv("KEYRING_PASSWORD") else "ESUN_PASSWORD"
        else:
            env_name = "ESUN_PASSWORD"

        value = os.getenv(env_name)
        if not value:
            raise RuntimeError(f"Missing required environment variable for prompt {prompt!r}: {env_name}")
        print(f"[AUTH] Answer getpass prompt from {env_name}: {prompt}")
        return value

    getpass.getpass = env_getpass
    return getpass, original_getpass


def login_sdks(config: ConfigParser) -> tuple[EsunMarketdata, SDK]:
    """建立並登入行情與交易 SDK。"""
    configure_noninteractive_keyring()
    realtime_sdk = EsunMarketdata(config)
    sdk = SDK(config)

    original_stdin = sys.stdin
    getpass_module, original_getpass = patch_getpass_from_env()
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


DOCKER_MAIN = '''if __name__ == "__main__":
    #print(calculate_range_fraction_prices(74.3, 72.1, "SHORT"))
    #print(10*get_tick_size(74.3))

    base_dir = os.path.dirname(__file__)
    execute_result_path = os.path.join(base_dir, "execute_strategy_result.txt")
    capture_buffer = io.StringIO()
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sys.stdout = TeeStream(original_stdout, capture_buffer)
    sys.stderr = TeeStream(original_stderr, capture_buffer)

    realtime_sdk = None
    sdk = None

    try:
        print("===== Prepare runtime files =====")
        download_runtime_files_from_s3()

        clear_state_dir()

        # 登入以操作 API
        config = ConfigParser()
        config.read("config.ini")

        realtime_sdk, sdk = login_sdks(config)

        candidate_symbols = load_stock_data_from_runtime_file()
        states = initialize_states(candidate_symbols, realtime_sdk)

        target_hour = 9 # 9
        target_minute = 1  # 0
        target_second = 0 # 15
        print(f"正在等待時間到 {target_hour:02d}:{target_minute:02d}:{target_second:02d} ...")

        while True:
            now = now_tpe()
            if (now.hour, now.minute, now.second) >= (target_hour, target_minute, target_second):
                print(f"⏰ 時間到！目前時間：{now.strftime('%H:%M:%S')}")
                break
            else:
                time.sleep(5)  # 每 N 秒

        # 對齊到下一個 5 秒邊界，避免第一輪跨分鐘造成額外更新
        align_now = now_tpe()
        align_next = ceil_next_interval(align_now, 5)
        align_sleep_sec = max(0.2, (align_next - align_now).total_seconds())
        time.sleep(align_sleep_sec)

        # 開始正式作業
        monitor(states, sdk, realtime_sdk)
    finally:
        safe_logout_sdk("Esun Trade SDK", sdk)
        safe_logout_sdk("EsunMarketdata", realtime_sdk)

        sys.stdout = original_stdout
        sys.stderr = original_stderr
        try:
            with open(execute_result_path, "w", encoding="utf-8") as f:
                f.write(capture_buffer.getvalue())
        except Exception as e:
            print(f"[WARN] 無法輸出 execute_strategy_result.txt：{e}", file=original_stderr)
'''


def output_name_for(strategy_filename: str) -> str:
    if "_singleton_" not in strategy_filename:
        raise ValueError("Strategy filename must contain '_singleton_'")
    return strategy_filename.replace("_singleton_", "_docker_", 1)


def replace_main_block(source: str, replacement: str) -> str:
    tree = ast.parse(source)
    lines = source.splitlines(keepends=True)

    for node in tree.body:
        if not isinstance(node, ast.If):
            continue
        test = node.test
        is_main_guard = (
            isinstance(test, ast.Compare)
            and isinstance(test.left, ast.Name)
            and test.left.id == "__name__"
            and len(test.ops) == 1
            and isinstance(test.ops[0], ast.Eq)
            and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Constant)
            and test.comparators[0].value == "__main__"
        )
        if is_main_guard:
            start = node.lineno - 1
            end = node.end_lineno
            return "".join(lines[:start]) + replacement.rstrip() + "\n" + "".join(lines[end:])

    raise ValueError('Cannot find if __name__ == "__main__" block')


def transform_singleton_to_docker(source: str) -> str:
    source = source.replace("\r\n", "\n")

    source = re.sub(
        r"\nimport stock_data\nfrom stock_data import selected_stocks, market_previous_close_indices\n+",
        "\n",
        source,
        count=1,
    )

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

    return replace_main_block(source, DOCKER_MAIN)


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    input_path = base_dir / STRATEGY_FILENAME
    output_path = base_dir / output_name_for(STRATEGY_FILENAME)

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
