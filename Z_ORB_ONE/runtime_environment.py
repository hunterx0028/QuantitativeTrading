"""本機／Docker 環境準備；不包含策略或下單邏輯，也不在匯入時讀取 stock_data。"""
from __future__ import annotations

import importlib.util
import io
import os
import sys
from configparser import ConfigParser
from dataclasses import dataclass
from typing import Any

REQUIRED_RUNTIME_FILES = ('config.ini', 'stock_data.py')
DEFAULT_CERT_FILE = 'T122260516_20270828.p12'


@dataclass
class RuntimeEnvironment:
    base_dir: str
    mode: str = "local"

    def __post_init__(self):
        self.base_dir = os.path.abspath(self.base_dir)
        if self.mode not in ("local", "docker"):
            raise ValueError("QUANT_RUNTIME_MODE 必須為 local 或 docker")

    @property
    def s3_bucket(self):
        return os.getenv('QUANT_S3_BUCKET', 'leegueishen-quant-trading-17')

    @property
    def s3_prefix(self):
        return os.getenv('QUANT_S3_PREFIX', 'exchange').strip('/')

    def prepare_stock_data(self):
        if self.mode == "docker":
            self.download_runtime_files_from_s3()
        stock_data_path = os.path.join(self.base_dir, "stock_data.py")
        spec = importlib.util.spec_from_file_location("stock_data", stock_data_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Unable to load stock_data.py from {stock_data_path}")
        module = importlib.util.module_from_spec(spec)
        previous = sys.modules.get("stock_data")
        sys.modules["stock_data"] = module
        try:
            spec.loader.exec_module(module)
            required = ("selected_stocks", "market_previous_close_indices", "total_stocks")
            missing = [name for name in required if not hasattr(module, name)]
            if missing:
                raise AttributeError("stock_data.py does not define required field(s): " + ", ".join(missing))
        except BaseException:
            if previous is None:
                sys.modules.pop("stock_data", None)
            else:
                sys.modules["stock_data"] = previous
            raise
        return module

    def login_sdk(self, sdk_obj):
        if self.mode == "local":
            sdk_obj.login()
            return
        self.configure_noninteractive_keyring()
        # Validate credentials before patching process-wide password handling.
        login_stdin = self.build_login_stdin()
        original_stdin = sys.stdin
        getpass_module, original_getpass, patched = self.patch_getpass_from_env()
        try:
            sys.stdin = login_stdin
            sdk_obj.login()
        finally:
            sys.stdin = original_stdin
            getpass_module.getpass = original_getpass
            for module in patched:
                module.getpass = original_getpass

    def normalize_runtime_relative_path(self, path_value: str) -> str:
        relative_path = str(path_value or '').strip().replace('\\', '/')
        if not relative_path:
            raise RuntimeError('Empty runtime file path')
        normalized_path = os.path.normpath(relative_path)
        if os.path.isabs(normalized_path) or normalized_path == '..' or normalized_path.startswith(f'..{os.sep}'):
            raise RuntimeError(f'Runtime file path must stay under BASE_DIR: {path_value!r}')
        return normalized_path

    def get_cert_runtime_file(self) -> str:
        config_path = os.path.join(self.base_dir, 'config.ini')
        if not os.path.exists(config_path):
            return DEFAULT_CERT_FILE
        runtime_config = ConfigParser()
        runtime_config.read(config_path)
        return self.normalize_runtime_relative_path(runtime_config.get('Cert', 'Path', fallback=DEFAULT_CERT_FILE))

    def is_truthy_env(self, name: str, default: str='false') -> bool:
        return os.getenv(name, default).strip().lower() in ('1', 'true', 'yes', 'y', 'on')

    def should_download_from_s3(self) -> bool:
        """
        預設行為：
        - AWS/Fargate 環境：下載 S3 檔案。
        - 本機環境：若 /app 下缺少三個必要檔案，也嘗試下載。
        可用 QUANT_DOWNLOAD_FROM_S3=true/false 強制控制。
        """
        override = os.getenv('QUANT_DOWNLOAD_FROM_S3')
        if override is not None:
            return self.is_truthy_env('QUANT_DOWNLOAD_FROM_S3')
        running_in_aws = bool(os.getenv('AWS_EXECUTION_ENV') or os.getenv('ECS_CONTAINER_METADATA_URI_V4'))
        runtime_files = list(REQUIRED_RUNTIME_FILES)
        if os.path.exists(os.path.join(self.base_dir, 'config.ini')):
            runtime_files.append(self.get_cert_runtime_file())
        else:
            runtime_files.append(DEFAULT_CERT_FILE)
        missing_file = any((not os.path.exists(os.path.join(self.base_dir, fname)) for fname in runtime_files))
        return running_in_aws or missing_file

    def download_one_runtime_file_from_s3(self, fname: str) -> None:
        runtime_path = self.normalize_runtime_relative_path(fname)
        s3_path = runtime_path.replace(os.sep, '/')
        s3_uri = f's3://{self.s3_bucket}/{self.s3_prefix}/{s3_path}' if self.s3_prefix else f's3://{self.s3_bucket}/{s3_path}'
        local_path = os.path.join(self.base_dir, runtime_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f'[S3] {s3_uri} -> {local_path}')
        import subprocess
        subprocess.run(['aws', 's3', 'cp', s3_uri, local_path], check=True)

    def download_runtime_files_from_s3(self):
        """從 S3 下載 config.ini、stock_data.py，以及 config.ini 指定的 p12 憑證到 /app。"""
        if not self.should_download_from_s3():
            print('===== Skip S3 download: runtime files already exist locally =====')
            return
        print('===== Download runtime files from S3 =====')
        for fname in REQUIRED_RUNTIME_FILES:
            self.download_one_runtime_file_from_s3(fname)
        self.download_one_runtime_file_from_s3(self.get_cert_runtime_file())

    def build_login_stdin(self) -> io.StringIO:
        """
        玉山 SDK 在 Linux container 內會互動式詢問密碼。
        主要由 patch_getpass_from_env() 回答密碼 prompt；這裡保留 stdin
        fallback，避免 SDK 內部使用 input() 時在 ECS/Fargate 卡住。
        """
        required_envs = ('ESUN_PASSWORD', 'CERT_PASSWORD')
        missing = [name for name in required_envs if not os.getenv(name)]
        if missing:
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
        login_lines = [os.environ['ESUN_PASSWORD'], os.environ['CERT_PASSWORD']] * 8
        return io.StringIO('\n'.join(login_lines) + '\n')

    def configure_noninteractive_keyring(self) -> None:
        """
        ECS/Fargate 沒有可互動的 OS keyring。
        若使用預設 keyring/keyrings.alt，可能會要求建立 keyring master password。
        """
        os.environ.setdefault('PYTHON_KEYRING_BACKEND', 'keyring.backends.null.Keyring')
        try:
            import keyring
            from keyring.backends.null import Keyring
            keyring.set_keyring(Keyring())
            print('===== Python keyring backend: null (non-interactive) =====')
        except Exception as e:
            print(f'[WARN] Unable to force null keyring backend: {e}')

    def patch_getpass_from_env(self):
        """讓 SDK 的 getpass prompt 改從環境變數取密碼。"""
        import getpass
        original_getpass = getpass.getpass
        patched_module_getpass = []
        last_password = None

        def env_getpass(prompt: str='Password: ', stream: Any=None) -> str:
            nonlocal last_password
            prompt_lower = str(prompt or '').casefold()
            if 'cert' in prompt_lower and 'password' in prompt_lower:
                env_name = 'CERT_PASSWORD'
                value = os.getenv(env_name)
            elif 'confirm' in prompt_lower and 'password' in prompt_lower:
                if last_password is None:
                    raise RuntimeError(f'Received confirm prompt before any password prompt: {prompt!r}')
                env_name = 'LAST_PASSWORD'
                value = last_password
            elif 'password' in prompt_lower:
                env_name = 'ESUN_PASSWORD'
                value = os.getenv(env_name)
            else:
                raise RuntimeError(f'Unknown password prompt: {prompt!r}')
            if not value:
                raise RuntimeError(f'Missing required environment variable for prompt {prompt!r}: {env_name}')
            last_password = value
            print(f'[AUTH] {prompt.strip()} -> {env_name}')
            return value
        getpass.getpass = env_getpass
        for module_name, module in list(sys.modules.items()):
            if not module_name.startswith(('esun_trade', 'esun_marketdata')):
                continue
            if getattr(module, 'getpass', None) is original_getpass:
                setattr(module, 'getpass', env_getpass)
                patched_module_getpass.append(module)
        return (getpass, original_getpass, patched_module_getpass)

    def safe_logout_sdk(self, name: str, sdk_obj: Any):
        if sdk_obj is None:
            return
        try:
            sdk_obj.logout()
            print(f'===== {name} logout success =====')
        except Exception as e:
            print(f'[WARN] {name} logout skipped/failed: {e}')
