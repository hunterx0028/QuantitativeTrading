from PIL import Image
import numpy as np

# ---------- 參數設定 ----------
input_path = "../image/2025-12-27_173245.png"
output_path = "../image/stock_name_image_output.png"

# ---------- 工具：以純 NumPy 計算飽和度 S(0~255) ----------
def saturation_255(arr_uint8: np.ndarray) -> np.ndarray:
    arrf = arr_uint8.astype(np.float32)
    R, G, B = arrf[..., 0], arrf[..., 1], arrf[..., 2]
    M = np.maximum(np.maximum(R, G), B)
    m = np.minimum(np.minimum(R, G), B)
    diff = M - m
    S = np.zeros_like(M, dtype=np.float32)
    nz = M > 0
    S[nz] = diff[nz] / M[nz]  # 0..1
    return (S * 255.0).astype(np.int16)  # 0..255

# ---------- 前處理：移除灰色細橫線（不動厚的標題灰區） ----------
def remove_gray_horizontal_lines(
    arr: np.ndarray,
    sat_max: int = 38,
    y_min: int = 135,
    y_max: int = 240,
    row_ratio_thresh: float = 0.55,
    max_line_thickness: int = 4
) -> np.ndarray:
    out = arr.copy()
    R = arr[:, :, 0].astype(np.int16)
    G = arr[:, :, 1].astype(np.int16)
    B = arr[:, :, 2].astype(np.int16)
    Y = (0.299 * R + 0.587 * G + 0.114 * B)
    S255 = saturation_255(arr)

    is_gray = (S255 <= sat_max) & (Y >= y_min) & (Y <= y_max)
    gray_ratio_by_row = is_gray.mean(axis=1)

    h = arr.shape[0]
    i = 0
    while i < h:
        if gray_ratio_by_row[i] >= row_ratio_thresh:
            j = i + 1
            while j < h and gray_ratio_by_row[j] >= row_ratio_thresh:
                j += 1
            run_len = j - i
            if run_len <= max_line_thickness:
                out[i:j][is_gray[i:j]] = [255, 255, 255]
            i = j
        else:
            i += 1
    return out

# ---------- A/B 偵測：A=第一條藍列；B=由A往上找第一個「灰列」且保留白列安全間距 ----------
def top_cut_by_A_then_B(
    arr: np.ndarray,
    # A：藍列判斷（亮且偏藍）
    min_brightness: int = 200,
    blue_delta: int = 8,
    bluish_row_ratio: float = 0.15,
    search_limit_ratio: float = 0.35,   # 只在上方比例範圍找 A/B
    # B：灰列判斷（低飽和、不可太亮）
    sat_max: int = 35,
    y_max_gray: int = 230,
    gray_row_ratio: float = 0.55,
    # 重要：白列安全間距（避免吃掉第一列白底）
    min_white_gap_px: int = 14
) -> int:
    """
    回傳 top_cut（刪除頂部到 B(不含) 的高度，單位 px）。
    - 要求 A - B >= min_white_gap_px 才採用；否則退而求其次：top_cut = max(0, A - min_white_gap_px)
    若找不到 A 或 B，回傳 0（保守不切）。
    """
    h, w, _ = arr.shape
    scan_max = int(h * search_limit_ratio) if search_limit_ratio > 0 else h

    # 亮度與藍色優勢
    R = arr[:, :, 0].astype(np.int16)
    G = arr[:, :, 1].astype(np.int16)
    Bc = arr[:, :, 2].astype(np.int16)
    Y = (0.299 * R + 0.587 * G + 0.114 * Bc)

    is_bright = (Y >= min_brightness)
    is_bluish = (Bc - np.maximum(R, G)) >= blue_delta
    bluish_mask = is_bright & is_bluish
    bluish_ratio_by_row = bluish_mask.mean(axis=1)

    # 找 A：自頂向下第一條藍列
    A = None
    for i in range(min(scan_max, h)):
        if bluish_ratio_by_row[i] >= bluish_row_ratio:
            A = i
            break
    if A is None:
        return 0  # 找不到藍列，不切

    # 找 B：從 A-1 往上找第一條灰列，但必須留出白列安全間距
    S255 = saturation_255(arr)
    is_gray_pixel = (S255 <= sat_max) & (Y <= y_max_gray)
    gray_ratio_by_row = is_gray_pixel.mean(axis=1)

    B_candidate = None
    for i in range(A - 1, -1, -1):
        if gray_ratio_by_row[i] >= gray_row_ratio:
            # 檢查與 A 的距離是否足夠保留白列
            if (A - i) >= min_white_gap_px:
                B_candidate = i
                break
            else:
                # 雖然是灰，但太靠近 A，可能是白列內的細線/過渡，不採用，繼續往上找
                continue

    if B_candidate is not None:
        top_cut = B_candidate  # 切到 B(不含)
    else:
        # 找不到滿足安全距離的灰列，退而求其次：保證至少保留 min_white_gap_px 的內容
        top_cut = max(0, A - min_white_gap_px)

    # 防呆：不會切過 A
    top_cut = min(top_cut, A)
    return top_cut

# ---------- 主流程 ----------
def crop_right_side(
    input_path: str,
    output_path: str,
    cut_ratio: float = 0.8,
    lossless: bool = False,
    # 去灰色細線（偵測用）
    rm_sat_max: int = 38,
    rm_y_min: int = 135,
    rm_y_max: int = 240,
    rm_row_ratio_thresh: float = 0.55,
    rm_max_line_thickness: int = 4,
    # A/B 偵測
    min_brightness: int = 200,
    blue_delta: int = 8,
    bluish_row_ratio: float = 0.15,
    search_limit_ratio: float = 0.35,
    sat_max: int = 35,
    y_max_gray: int = 230,
    gray_row_ratio: float = 0.55,
    min_white_gap_px: int = 14,
    # 對最終輸出也去灰線（與偵測門檻可不同）
    out_rm_sat_max: int = 38,
    out_rm_y_min: int = 135,
    out_rm_y_max: int = 240,
    out_rm_row_ratio_thresh: float = 0.55,
    out_rm_max_line_thickness: int = 4,
):
    """
    1) 先去掉灰色細橫線（僅供偵測用，避免 B 被攔截）
    2) 依 A→B 規則求 top_cut；若 B 靠太近，保留 min_white_gap_px
    3) 右側裁切 cut_ratio
    4) 藍底→白；再把輸出中的灰色細橫線實際清除
    """
    img = Image.open(input_path).convert("RGB")
    width, height = img.size

    # 去灰線 (for detection)
    arr_full = np.array(img, dtype=np.uint8)
    arr_no_lines = remove_gray_horizontal_lines(
        arr_full,
        sat_max=rm_sat_max, y_min=rm_y_min, y_max=rm_y_max,
        row_ratio_thresh=rm_row_ratio_thresh,
        max_line_thickness=rm_max_line_thickness
    )

    # 依 A→B + 白列安全間距 求 top_cut
    top_cut = top_cut_by_A_then_B(
        arr_no_lines,
        min_brightness=min_brightness, blue_delta=blue_delta,
        bluish_row_ratio=bluish_row_ratio, search_limit_ratio=search_limit_ratio,
        sat_max=sat_max, y_max_gray=y_max_gray, gray_row_ratio=gray_row_ratio,
        min_white_gap_px=min_white_gap_px
    )

    # 右側裁切
    keep_width = int(width * (1 - cut_ratio))
    crop_box = (0, top_cut, keep_width, height)
    cropped = img.crop(crop_box)

    # 藍底 -> 白
    arr = np.array(cropped, dtype=np.uint8)
    R = arr[:, :, 0].astype(np.int16)
    G = arr[:, :, 1].astype(np.int16)
    Bc = arr[:, :, 2].astype(np.int16)
    Y = (0.299 * R + 0.587 * G + 0.114 * Bc)
    is_bright = (Y >= min_brightness)
    is_bluish = (Bc - np.maximum(R, G)) >= blue_delta
    arr[is_bright & is_bluish] = [255, 255, 255]

    # 對「最終輸出」也移除灰色細線
    arr = remove_gray_horizontal_lines(
        arr,
        sat_max=out_rm_sat_max, y_min=out_rm_y_min, y_max=out_rm_y_max,
        row_ratio_thresh=out_rm_row_ratio_thresh,
        max_line_thickness=out_rm_max_line_thickness
    )

    out = Image.fromarray(arr)
    if lossless:
        out.save(output_path, format="PNG")
    else:
        out.save(output_path, format="JPEG", quality=95, subsampling=0)

    print(f"top_cut={top_cut}px；完成去灰線偵測+白列保護、裁切與藍底轉白 -> {output_path}")

# ---------- 使用範例 ----------
if __name__ == "__main__":
    crop_right_side(
        input_path, output_path,
        cut_ratio=0.8, lossless=True,
        # 偵測用去線
        rm_sat_max=38, rm_y_min=135, rm_y_max=240,
        rm_row_ratio_thresh=0.55, rm_max_line_thickness=4,
        # A/B + 白列安全間距
        min_brightness=200, blue_delta=8,
        bluish_row_ratio=0.15, search_limit_ratio=0.35,
        sat_max=35, y_max_gray=230, gray_row_ratio=0.55,
        min_white_gap_px=14,
        # 輸出也去線
        out_rm_sat_max=38, out_rm_y_min=135, out_rm_y_max=240,
        out_rm_row_ratio_thresh=0.55, out_rm_max_line_thickness=4,
    )

'''
如果第一列仍偶爾被吃到：把 min_white_gap_px 再調大一點（例如 16～18）。
若標題灰區很厚、裁不乾淨：把 gray_row_ratio 降到 0.50 或把 sat_max 稍升到 38。
若又出現細灰線殘留：把 out_rm_row_ratio_thresh 降到 0.50 或 out_rm_max_line_thickness 調到 5。
這版會優先保障第一列白底，就算 B 被誤判太貼近 A，也會用 min_white_gap_px 拉開距離，避免再把第一列切掉。
'''