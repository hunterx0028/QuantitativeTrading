import requests
from bs4 import BeautifulSoup

def fetch_yahoo_volume_top(n: int = 100):
    url = "https://tw.stock.yahoo.com/rank/volume"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    ranks = []

    # ✅ 用 CSS Selector 抓所有符合 Fz(24px) + Fw(b) 的 span（不管顏色 class）
    rank_tags = soup.select("span.Fz\\(24px\\).Fw\\(b\\)")

    for rank_tag in rank_tags:
        try:
            rank = int(rank_tag.get_text(strip=True))
        except ValueError:
            continue  # 跳過非數字

        name_tag = rank_tag.find_next("div", class_="Lh(20px) Fw(600) Fz(16px) Ell")
        code_tag = rank_tag.find_next("span", class_="Fz(14px) C(#979ba7) Ell")
        price_tag = rank_tag.find_next("span", class_="Jc(fe)")
        print_float = float(price_tag.get_text(strip=True).replace(',', ''))

        # if print_float < 50:
        #     continue  # 價格低於50元，跳過

        # if print_float > 300:
        #     continue  # 價格高於300元，跳過

         # 確保 name_tag 和 code_tag 都存在
        if name_tag and code_tag:
            name = name_tag.get_text(strip=True)
            code = code_tag.get_text(strip=True).upper()
            ranks.append((rank, f"{name}:{code}"))

    # 依排名排序
    ranks.sort(key=lambda x: x[0])
    return [v for _, v in ranks[:n]]

if __name__ == "__main__":
    result = fetch_yahoo_volume_top(100)
    print(f'orb_symbols = {result}')
