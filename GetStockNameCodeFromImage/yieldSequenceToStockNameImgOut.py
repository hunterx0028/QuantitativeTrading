# row_by_row_numbering.py
from PIL import Image, ImageDraw, ImageFont
import numpy as np
from pathlib import Path

# 完成後，手動利用 stock_name_image_output_numbered_seq.png 來修補 temp_code_buffer.txt 中缺陷的序號
def number_rows_sequential(src, dst=None,
                           local_win=120, enter_ratio=0.22, hysteresis_ratio=0.60,
                           min_band_h=10, pad_after=2):
    """
    逐列偵測與編號：
      - 先做灰階→水平投影(hproj)→平滑
      - 從上往下掃，對每個 y 以 [y, y+local_win) 取得 local_max
      - 當 hproj >= local_max*enter_ratio 視為進入一列；
        在列內用 hysteresis_ratio 放寬退出門檻，直到掉回去為止
      - 每一列高度>=min_band_h 才計入
    """
    src = Path(src)
    if dst is None:
        dst = str(src.with_name(src.stem + "_numbered_seq.png"))

    img = Image.open(src).convert("RGB")
    gray = img.convert("L")
    arr = np.asarray(gray, dtype=np.uint8)
    ink = 255 - arr  # 黑字→高值

    # 水平投影 + 輕微平滑
    hproj = ink.sum(axis=1).astype(np.float32)
    k = 7
    kernel = np.ones(k, dtype=np.float32) / k
    hproj_s = np.convolve(hproj, kernel, mode="same")

    H, W = arr.shape

    # 左邊預留邊欄
    gutter_width = max(80, int(W * 0.08))
    out = Image.new("RGB", (gutter_width + W, H), (255, 255, 255))
    out.paste(img, (gutter_width, 0))
    draw = ImageDraw.Draw(out)

    # 字體
    try:
        font = ImageFont.truetype("DejaVuSansMono.ttf", 16)
    except Exception:
        font = ImageFont.load_default()

    rows = []
    y = 0
    while y < H:
        y2 = min(H, y + local_win)
        local_max = float(hproj_s[y:y2].max()) if y < y2 else 0.0
        enter_thr = max(5.0, local_max * enter_ratio)

        if hproj_s[y] >= enter_thr:
            y0 = y
            stay_thr = enter_thr * hysteresis_ratio
            y += 1
            while y < H and hproj_s[y] >= stay_thr:
                y += 1
            y1 = y  # exclusive
            if y1 - y0 >= min_band_h:
                rows.append((y0, y1))
            y = y1 + pad_after
        else:
            y += 1

    # 畫分隔線＋編號（置中）
    for idx, (y0, y1) in enumerate(rows, start=1):
        draw.line([(0, y0), (gutter_width + W, y0)], fill=(230, 230, 230), width=1)
        cy = (y0 + y1) // 2
        draw.text((10, max(0, cy - 8)), str(idx), fill=(0, 0, 0), font=font)

    out.save(dst, "PNG")
    return dst, rows

if __name__ == "__main__":
    src_img = "../image/stock_name_image_output.png"  # 換成你的檔名
    out_img, rows = number_rows_sequential(src_img)
    print("Saved:", out_img)
    print("Detected rows:", len(rows))
