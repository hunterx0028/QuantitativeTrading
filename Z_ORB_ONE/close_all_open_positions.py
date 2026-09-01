import os
import sys
import time
from configparser import ConfigParser
from dataclasses import dataclass
from typing import Any, Optional

from esun_marketdata import EsunMarketdata
from esun_trade.constant import APCode, Action, PriceFlag, Trade
from esun_trade.order import OrderObject
from esun_trade.sdk import SDK


TRADE_SIDE_SHORT = "SHORT"
TRADE_SIDE_LONG = "LONG"
SHARES_PER_LOT = 1000


@dataclass
class InventoryPosition:
    stock_no: str
    stock_name: str
    side: str
    inventory_trade: str
    shares: int
    lots: int
    price_avg: str
    price_evn: str
    value_mkt: str


def type_place_order(
    mysdk,
    symbol_code: str,
    action_type,
    trade_type,
    quantity: int,
    price_flag=PriceFlag.Market,
    price: float = 0.0,
) -> Optional[bool]:
    price_info = price

    if price_flag == PriceFlag.Market:
        price = ""

    if price_flag in (PriceFlag.LimitUp, PriceFlag.LimitDown):
        price = None

    order = OrderObject(
        buy_sell=action_type,
        price_flag=price_flag,
        price=price,
        stock_no=symbol_code,
        quantity=quantity,
        ap_code=APCode.Common,
        trade=trade_type,
    )

    try:
        mysdk.place_order(order)
        time.sleep(0.1)
    except Exception as exc:
        print(f"[ERROR] {symbol_code} : {price_info} {action_type} x {quantity} - {trade_type} - {exc}")
        return False

    print(f"[ORDER] {symbol_code} : {price_info} {action_type} x {quantity} - {trade_type}")
    return True


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def load_inventory_positions(sdk) -> Optional[list[InventoryPosition]]:
    try:
        inventories = sdk.get_inventories()
    except Exception as exc:
        print(f"[ERROR] 查詢庫存失敗: {exc}")
        return None

    if not inventories:
        return []

    positions: list[InventoryPosition] = []
    for inventory in inventories:
        if not isinstance(inventory, dict):
            print(f"[WARN] 庫存資料格式無法解析，略過: {inventory}")
            continue

        stock_no = str(inventory.get("stk_no", "")).strip()
        stock_name = str(inventory.get("stk_na", "")).strip()
        if not stock_no:
            continue

        stock_details = inventory.get("stk_dats") or []
        if not isinstance(stock_details, list) or not stock_details:
            print(f"[WARN] {stock_name or stock_no} 沒有可辨識多空方向的庫存明細，略過")
            continue

        grouped_shares: dict[tuple[str, str], int] = {}
        for detail in stock_details:
            if not isinstance(detail, dict):
                print(f"[WARN] {stock_name or stock_no} 庫存明細格式無法解析，略過: {detail}")
                continue

            buy_sell = str(detail.get("buy_sell", "")).strip().upper()
            inventory_trade = str(detail.get("trade", "")).strip().upper()
            shares = parse_int(detail.get("qty"))
            if shares <= 0:
                continue

            if buy_sell == Action.Buy.value:
                side = TRADE_SIDE_LONG
            elif buy_sell == Action.Sell.value:
                side = TRADE_SIDE_SHORT
            else:
                print(
                    f"[WARN] {stock_name or stock_no} 無法辨識庫存買賣別 "
                    f"buy_sell={buy_sell or '-'}，略過 {shares} 股"
                )
                continue

            if inventory_trade in (Trade.Margin.value, Trade.Short.value):
                print(
                    f"[WARN] {stock_name or stock_no} 為未支援的融資/融券庫存 "
                    f"trade={inventory_trade}，略過 {shares} 股"
                )
                continue

            key = (side, inventory_trade)
            grouped_shares[key] = grouped_shares.get(key, 0) + shares

        for (side, inventory_trade), shares in grouped_shares.items():
            if shares % SHARES_PER_LOT != 0:
                print(
                    f"[WARN] {stock_name or stock_no} {side} 庫存股數 {shares} "
                    "不是 1000 的整數倍，略過避免下錯張數"
                )
                continue

            positions.append(
                InventoryPosition(
                    stock_no=stock_no,
                    stock_name=stock_name,
                    side=side,
                    inventory_trade=inventory_trade,
                    shares=shares,
                    lots=shares // SHARES_PER_LOT,
                    price_avg=str(inventory.get("price_avg", "")).strip(),
                    price_evn=str(inventory.get("price_evn", "")).strip(),
                    value_mkt=str(inventory.get("value_mkt", "")).strip(),
                )
            )

    return positions


def print_inventory_positions(positions: list[InventoryPosition]) -> None:
    print("目前仍有庫存股票:")
    for position in positions:
        name_text = f"{position.stock_name} " if position.stock_name else ""
        print(
            f"- {name_text}{position.stock_no} | "
            f"方向={position.side} | 庫存交易類別={position.inventory_trade or '-'} | "
            f"股數={position.shares} | 張數={position.lots} | "
            f"成交均價={position.price_avg or '-'} | "
            f"損益平衡價={position.price_evn or '-'} | "
            f"市值={position.value_mkt or '-'}"
        )


def ask_yes_no(prompt: str) -> bool:
    while True:
        answer = input(prompt).strip().upper()
        if answer == "Y":
            return True
        if answer == "N":
            return False
        print("請輸入 Y 或 N。")


def close_position(position: InventoryPosition, sdk) -> bool:
    if position.side == TRADE_SIDE_SHORT:
        market_result = type_place_order(
            sdk,
            position.stock_no,
            Action.Buy,
            Trade.Cash,
            quantity=position.lots,
            price_flag=PriceFlag.Market,
        )
        if market_result:
            return True

        print(f"[{position.stock_name or position.stock_no}] SHORT 市價平倉失敗，改用漲停買進")
        return bool(
            type_place_order(
                sdk,
                position.stock_no,
                Action.Buy,
                Trade.Cash,
                quantity=position.lots,
                price_flag=PriceFlag.LimitUp,
                price=0,
            )
        )

    if position.side != TRADE_SIDE_LONG:
        print(f"[ERROR] {position.stock_name or position.stock_no} 無法辨識平倉方向: {position.side}")
        return False

    market_result = type_place_order(
        sdk,
        position.stock_no,
        Action.Sell,
        Trade.DayTradingSell,
        quantity=position.lots,
        price_flag=PriceFlag.Market,
    )
    if market_result:
        return True

    print(f"[{position.stock_name or position.stock_no}] LONG 市價平倉失敗，改用跌停賣出")
    return bool(
        type_place_order(
            sdk,
            position.stock_no,
            Action.Sell,
            Trade.DayTradingSell,
            quantity=position.lots,
            price_flag=PriceFlag.LimitDown,
            price=0,
        )
    )


def main() -> int:
    base_dir = os.path.dirname(__file__)
    config_path = os.path.join(base_dir, "config.ini")

    config = ConfigParser()
    config.read(config_path)

    realtime_sdk = EsunMarketdata(config)
    realtime_sdk.login()

    sdk = SDK(config)
    sdk.login()

    positions = load_inventory_positions(sdk)
    if positions is None:
        return 1

    if not positions:
        print("無可自動平倉的整張庫存")
        return 0

    print_inventory_positions(positions)

    if not ask_yes_no("是否要平倉全部庫存股票？請輸入 Y/N: "):
        print("取消平倉，程式結束")
        return 0

    print(f"開始依庫存明細自動辨識方向並平倉，共 {len(positions)} 筆部位")

    success_count = 0
    failed_positions: list[InventoryPosition] = []
    for position in positions:
        print(
            f"[{position.stock_name or position.stock_no}] "
            f"{position.side} 平倉 {position.lots} 張"
        )
        if close_position(position, sdk):
            success_count += 1
            print(f"[{position.stock_name or position.stock_no}] 平倉委託已送出")
        else:
            failed_positions.append(position)
            print(f"[{position.stock_name or position.stock_no}] 平倉委託失敗，須手動確認")

    print(f"平倉委託完成: 成功 {success_count} 筆，失敗 {len(failed_positions)} 筆")
    if failed_positions:
        failed_text = ", ".join(f"{item.stock_name or item.stock_no}({item.stock_no})" for item in failed_positions)
        print(f"失敗清單: {failed_text}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
