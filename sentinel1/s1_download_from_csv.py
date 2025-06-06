#!/usr/bin/env python3
import os
import requests
import argparse
import zipfile
import tempfile
import yaml
import logging
from tqdm import tqdm
from typing import Optional


def setup_logger(log_level=logging.INFO) -> None:
    logging.basicConfig(
        level=log_level, format="%(asctime)s [%(levelname)s] [DOWNLOAD] %(message)s"
    )


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_access_token(username: str, password: str) -> str:
    token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    response = requests.post(token_url, data=data, timeout=10)
    response.raise_for_status()
    return response.json()["access_token"]


def download_scene(
    product_id: str, access_token: str, base_file_path: str, base_url: str
) -> Optional[str]:
    os.makedirs(base_file_path, exist_ok=True)
    url = base_url.format(product_id)
    headers = {"Authorization": f"Bearer {access_token}"}

    logging.info(f"[DOWNLOAD] Starting download for product {product_id}")
    response = requests.get(url, headers=headers, stream=True)
    if response.status_code != 200:
        logging.error(
            f"[DOWNLOAD] Failed to download product {product_id}. Status code: {response.status_code}"
        )
        logging.error(f"[DOWNLOAD] Response content: {response.content}")
        return None

    total_size = int(response.headers.get("content-length", 0))
    block_size = 8192

    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        temp_file_path = tmp_file.name
        for chunk in tqdm(
            response.iter_content(chunk_size=block_size),
            total=total_size // block_size,
            unit="KiB",
            unit_scale=True,
        ):
            if chunk:
                tmp_file.write(chunk)
    logging.info("[DOWNLOAD] Download complete. Processing archive...")

    try:
        with zipfile.ZipFile(temp_file_path, "r") as zip_ref:
            safe_folder = None
            for info in zip_ref.infolist():
                parts = info.filename.split("/")
                for part in parts:
                    if part.endswith(".SAFE"):
                        safe_folder = part
                        break
                if safe_folder:
                    break
            if not safe_folder:
                safe_folder = product_id
                logging.warning(
                    "[DOWNLOAD] No .SAFE folder found in archive, using product id as folder name"
                )
            else:
                logging.info(f"[DOWNLOAD] .SAFE folder detected: {safe_folder}")

            new_zip_path = os.path.join(base_file_path, f"{safe_folder}.zip")
            with zipfile.ZipFile(
                new_zip_path, "w", compression=zipfile.ZIP_DEFLATED
            ) as new_zip:
                for info in zip_ref.infolist():
                    parts = info.filename.split("/")
                    if safe_folder in parts:
                        index = parts.index(safe_folder)
                        new_name = "/".join(parts[index:])
                        if info.is_dir():
                            new_zip.writestr(new_name + "/", b"")
                        else:
                            file_data = zip_ref.read(info)
                            new_zip.writestr(new_name, file_data)
            logging.info(f"[DOWNLOAD] Saved filtered zip to {new_zip_path}")
    finally:
        os.remove(temp_file_path)

    return new_zip_path


def main():
    setup_logger()

    parser = argparse.ArgumentParser(
        description="Download Copernicus Sentinel-1 product by product id."
    )
    parser.add_argument("--product_id", required=True, help="Product ID to download")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument(
        "--output_dir",
        required=False,
        help="Override output directory to save downloads",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    creds = config.get("copernicus_credentials", {})
    s1_cfg = config.get("sentinel1", {})
    base_url = s1_cfg.get(
        "download_url",
        "https://zipper.dataspace.copernicus.eu/odata/v1/Products({})/$value",
    )

    # Use output_dir CLI argument if provided; else fallback to config
    if args.output_dir:
        base_file_path = args.output_dir
    else:
        base_file_path = s1_cfg.get("download_dir", ".")

    username = creds.get("username")
    password = creds.get("password")

    if not username or not password:
        logging.error(
            "[DOWNLOAD] Username or password missing in config under 'copernicus_credentials'"
        )
        return

    try:
        access_token = get_access_token(username, password)
    except Exception as e:
        logging.error(f"[DOWNLOAD] Failed to get access token: {e}")
        return

    downloaded_path = download_scene(
        args.product_id, access_token, base_file_path, base_url
    )
    if downloaded_path:
        logging.info(f"[DOWNLOAD] Download finished successfully: {downloaded_path}")
    else:
        logging.error("[DOWNLOAD] Download failed.")


if __name__ == "__main__":
    main()
