from pathlib import Path
from typing import List, Tuple, Optional
import re, unicodedata
import cv2, numpy as np
import easyocr

# 完成後，記得要把 temp_code_buffer.txt 最尾端非0無借券的股票去掉
# ========= 可調參數 =========
INPUT_IMAGE = "../image/stock_name_image_output.png"
OUTPUT_TEXT = "../papers/temp_code_buffer.txt"
USE_GPU = True

# 初次取框/行級拼字用
LANG_LIST_DET = ["en"]
ALLOWLIST_COARSE = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ()"

# 行尾括號 ROI 二次 OCR：要支援最後一碼任意字母
ALLOWLIST_BRACKET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ()"

MIN_WORKING_WIDTH = 800

# 超長圖垂直分塊（取框專用）
CHUNK_H = 1400
CHUNK_OVERLAP = 120

# y 分群自適應
Y_EPS_FACTOR = 0.7   # 0.6~0.9 可調

# 行尾括號 ROI
RIGHT_FRACTION = 0.45
SECOND_PASS_SCALE = 1.8

# EasyOCR 參數
OCR_DECODER = "greedy"
OCR_BEAM_WIDTH = 5
OCR_CONTRAST_THS = 0.05
OCR_ADJUST_CONTRAST = 0.7
OCR_TEXT_THS = 0.64
OCR_LOW_TEXT = 0.30
OCR_LINK_THS = 0.40
OCR_CANVAS_SIZE = 3600

# ========= 正規化 / 括號抽取 =========
_LAST_BRACKET = re.compile(r"[（(]([A-Z0-9]+)[)）](?!.*[（(][A-Z0-9]+[)）])")

def _nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)

def _alnum(s: str) -> str:
    return "".join(ch for ch in s if ch.isalnum())

def extract_last_bracket_token(text: str) -> Optional[str]:
    if not text:
        return None
    t = _nfkc(text)
    m = _LAST_BRACKET.search(t)
    if not m:
        return None
    inner = _alnum(m.group(1)).upper()
    return inner or None

# ========= 規格檢核（含極保守自動修正） =========
def enforce_code_spec(token: Optional[str]) -> Optional[str]:
    """
    驗證並極保守修正股票代碼：
    - 4 碼/5 碼：必為純數字（允許 O,Q,D→0；I,L→1；Z→2；S→5；B→8；G→6；T→7 的保守修正）
    - 6 碼：前 5 碼為數字（允許上述保守修正），最後一碼必須是字母；
            若最後一碼被誤判成數字，做極保守數字→字母修正（4→A, 8→B, 0→O, 1→I）
    其他長度直接拒絕。
    """
    if not token:
        return None

    t = _nfkc(token).upper()  # 正規化 + 大寫化（與原流程一致）

    # 共用：把少數易誤判的字母修正為數字
    letter_to_digit = {
        'O': '0', 'Q': '0', 'D': '0',
        'I': '1', 'L': '1',
        'Z': '2',
        'S': '5',
        'B': '8',
        'G': '6',
        'T': '7',
    }

    # 4 或 5 碼 → 必須是純數字，允許保守字母→數字的修正
    if len(t) in (4, 5):
        fixed = []
        for ch in t:
            if ch.isdigit():
                fixed.append(ch)
            elif ch in letter_to_digit:
                fixed.append(letter_to_digit[ch])
            else:
                return None  # 出現不合理字元，直接放棄
        num = "".join(fixed)
        return num if num.isdigit() else None

    # 6 碼 → 前 5 碼是數字（允許保守修正），最後一碼是字母（若為數字則極保守數字→字母）
    if len(t) == 6:
        prefix, last = t[:5], t[5]

        fixed_prefix = []
        for ch in prefix:
            if ch.isdigit():
                fixed_prefix.append(ch)
            elif ch in letter_to_digit:
                fixed_prefix.append(letter_to_digit[ch])
            else:
                return None
        prefix = "".join(fixed_prefix)
        if not prefix.isdigit():
            return None

        # 最後一碼：理想是字母；若為數字則做極保守映射
        if last.isalpha():
            return prefix + last

        digit_to_letter_conservative = {'4': 'A', '8': 'B', '0': 'O', '1': 'I'}
        if last in digit_to_letter_conservative:
            return prefix + digit_to_letter_conservative[last]

        return None

    # 其他長度一律不接受
    return None


# ========= 影像輔助 =========
def ensure_min_width(img: np.ndarray, min_w: int) -> np.ndarray:
    h, w = img.shape[:2]
    if w >= min_w:
        return img
    scale = float(min_w) / float(w)
    new_w, new_h = int(round(w*scale)), int(round(h*scale))
    up = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_CUBIC)
    print(f"[INFO] Upscale from {(h,w)} -> {(new_h,new_w)} (x{scale:.2f})")
    return up

# ========= OCR 呼叫 =========
def ocr_boxes(img: np.ndarray, reader: easyocr.Reader, allowlist: str):
    return reader.readtext(
        img, detail=1, paragraph=False,
        decoder=OCR_DECODER, beamWidth=OCR_BEAM_WIDTH,
        contrast_ths=OCR_CONTRAST_THS, adjust_contrast=OCR_ADJUST_CONTRAST,
        text_threshold=OCR_TEXT_THS, low_text=OCR_LOW_TEXT, link_threshold=OCR_LINK_THS,
        canvas_size=OCR_CANVAS_SIZE, mag_ratio=1.0,
        allowlist=allowlist
    )

# ========= 全域取框（支援超長圖分塊） =========
def collect_boxes_global(img: np.ndarray, reader: easyocr.Reader):
    H, W = img.shape[:2]
    boxes_all = []
    if H <= CHUNK_H:
        res = ocr_boxes(img, reader, ALLOWLIST_COARSE)
        boxes_all.extend(res)
        return boxes_all
    y = 0
    while y < H:
        y2 = min(H, y + CHUNK_H)
        seg = img[y:y2, :]
        res = ocr_boxes(seg, reader, ALLOWLIST_COARSE)
        for box, txt, conf in res:
            box2 = [[p[0], p[1] + y] for p in box]
            boxes_all.append((box2, txt, conf))
        if y2 == H:
            break
        y = y2 - CHUNK_OVERLAP
    return boxes_all

# ========= 由框分群成行 =========
def cluster_boxes_to_lines(boxes_texts, y_eps_factor=Y_EPS_FACTOR):
    if not boxes_texts:
        return []
    items, heights = [], []
    for box, txt, conf in boxes_texts:
        xs = [p[0] for p in box]; ys = [p[1] for p in box]
        x1, x2 = min(xs), max(xs); y1, y2 = min(ys), max(ys)
        cy = (y1 + y2) / 2.0
        h  = (y2 - y1 + 1)
        items.append((cy, x1, x2, y1, y2, box, txt, conf))
        heights.append(h)
    items.sort(key=lambda t: t[0])
    med_h = float(np.median(heights)) if heights else 18.0
    y_eps = max(12.0, med_h * y_eps_factor)

    lines = []
    cur, cur_min_y, cur_max_y = [], None, None
    for cy, x1, x2, y1, y2, box, txt, conf in items:
        if not cur:
            cur = [(cy, x1, x2, y1, y2, box, txt, conf)]
            cur_min_y, cur_max_y = y1, y2
            continue
        last_cy = cur[-1][0]
        if abs(cy - last_cy) <= y_eps:
            cur.append((cy, x1, x2, y1, y2, box, txt, conf))
            cur_min_y = min(cur_min_y, y1); cur_max_y = max(cur_max_y, y2)
        else:
            xs = [g[1] for g in cur] + [g[2] for g in cur]
            lines.append(((min(xs), cur_min_y, max(xs), cur_max_y), cur))
            cur = [(cy, x1, x2, y1, y2, box, txt, conf)]
            cur_min_y, cur_max_y = y1, y2
    if cur:
        xs = [g[1] for g in cur] + [g[2] for g in cur]
        lines.append(((min(xs), cur_min_y, max(xs), cur_max_y), cur))
    lines.sort(key=lambda L: L[0][1])
    return lines

# ========= 行尾 ROI 二次 OCR =========
def second_pass_on_right_roi(line_img: np.ndarray, reader: easyocr.Reader) -> Optional[str]:
    H, W = line_img.shape[:2]
    x1 = int(W * (1.0 - RIGHT_FRACTION))
    x1 = max(0, min(W-1, x1))
    roi = line_img[:, x1:W]
    roi_big = cv2.resize(roi, (int(roi.shape[1]*SECOND_PASS_SCALE), int(roi.shape[0]*SECOND_PASS_SCALE)),
                         interpolation=cv2.INTER_CUBIC)
    texts = reader.readtext(
        roi_big, detail=0, paragraph=True,
        decoder=OCR_DECODER, beamWidth=OCR_BEAM_WIDTH,
        contrast_ths=OCR_CONTRAST_THS, adjust_contrast=OCR_ADJUST_CONTRAST,
        text_threshold=OCR_TEXT_THS, low_text=OCR_LOW_TEXT, link_threshold=OCR_LINK_THS,
        canvas_size=OCR_CANVAS_SIZE, mag_ratio=1.0,
        allowlist=ALLOWLIST_BRACKET
    )
    raw = " ".join(t.strip() for t in texts if t and t.strip())
    return extract_last_bracket_token(raw)

def _char_before_last_open_paren(text: str) -> Optional[str]:
    """回傳最後一個 '(' 或 '（' 左邊的那個字元（已做 NFKC 與大寫），找不到則 None。"""
    if not text:
        return None
    t = _nfkc(text)
    idx = max(t.rfind('('), t.rfind('（'))
    if idx > 0:
        return t[idx - 1].upper()
    return None

# ========= 主流程 =========
def run_pipeline():
    img = cv2.imread(INPUT_IMAGE, cv2.IMREAD_COLOR)
    if img is None:
        raise FileNotFoundError(INPUT_IMAGE)
    print(f"[INFO] Input size: {img.shape[:2]}")

    img = ensure_min_width(img, MIN_WORKING_WIDTH)
    H, W = img.shape[:2]
    print(f"[INFO] Working size: {img.shape[:2]}")

    reader = easyocr.Reader(LANG_LIST_DET, gpu=USE_GPU)

    # 全域收集框 → 分群成行
    all_boxes = collect_boxes_global(img, reader)
    line_groups = cluster_boxes_to_lines(all_boxes)

    outputs: List[str] = []
    row_idx = 0

    for (x1, y1, x2, y2), group in line_groups:
        # 行級拼字先嘗試
        group_sorted = sorted(group, key=lambda g: g[1])
        line_text = " ".join(g[6] for g in group_sorted if g[6]).strip()
        token_line = extract_last_bracket_token(line_text)

        # 行尾 ROI 二次 OCR 再嘗試
        line_img = img[max(0, y1 - 1):min(H, y2 + 1), 0:W]
        token_roi = second_pass_on_right_roi(line_img, reader)

        # ===== 決策邏輯 =====
        final_token = None

        # (1) 你先前已加：ROI < 4 且 Line >= 4 → 用 Line 的最後 4 碼
        if (token_roi and len(token_roi) < 4) and (token_line and len(token_line) >= 4):
            candidate = token_line[-4:]
            candidate_valid = enforce_code_spec(candidate)
            if candidate_valid:
                final_token = candidate_valid

        # (1-b) 新增：ROI 與 Line 都只有 3 碼，且 '(' 左鄰是 1/I/L → 補成 '1' + 3 碼
        if final_token is None:
            if ((token_roi and len(token_roi) == 3) and
                    (token_line and len(token_line) == 3)):
                left_char = _char_before_last_open_paren(line_text)
                if left_char in ('1', 'I', 'L'):
                    candidate = '1' + token_roi  # 或 token_line，兩者等長
                    candidate_valid = enforce_code_spec(candidate)
                    if candidate_valid:
                        final_token = candidate_valid
                        # 可選：觀察補救是否觸發
                        print(f"[DEBUG] prefix-1 fallback: left='{left_char}', roi='{token_roi}' -> '{candidate_valid}'")

        # (2) 若仍沒有結果，走 A 邏輯：分別驗證 → 擇其一（ROI 優先）
        if final_token is None:
            valid_roi = enforce_code_spec(token_roi) if token_roi else None
            valid_line = enforce_code_spec(token_line) if token_line else None
            final_token = valid_roi or valid_line

        # (3) 輸出 or 警告（原樣）
        if final_token:
            outputs.append(final_token)
            print(f"[{row_idx:03d}] {final_token}")
            row_idx += 1
        else:
            preview = line_text if line_text else "<空行>"
            warnString = f"[WARN] 無效代碼，原始行內容：{preview}"
            outputs.append(warnString)
            print(warnString)
            row_idx += 1

    Path(OUTPUT_TEXT).parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_TEXT, "w", encoding="utf-8") as f:
        for t in outputs:
            f.write(t + "\n")
    print(f"[INFO] lines detected: {len(outputs)}")
    print("✅ 已輸出：", OUTPUT_TEXT)

if __name__ == "__main__":
    run_pipeline()
