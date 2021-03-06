'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
import logging

from yapic import json

from cryptofeed.defines import BINANCE_DELIVERY, OPEN_INTEREST, TICKER
from cryptofeed.exchange.binance import Binance
from cryptofeed.standards import symbol_exchange_to_std, timestamp_normalize

LOG = logging.getLogger('feedhandler')


class BinanceDelivery(Binance):
    id = BINANCE_DELIVERY

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # overwrite values previously set by the super class Binance
        self.ws_endpoint = 'wss://dstream.binance.com'
        self.rest_endpoint = 'https://dapi.binance.com/dapi/v1'
        self.address = self._address()

    def _address(self):
        address = self.ws_endpoint + '/stream?streams='
        for chan in self.channels if not self.subscription else self.subscription:
            if chan == OPEN_INTEREST:
                continue
            for pair in self.symbols if not self.subscription else self.subscription[chan]:
                pair = pair.lower()
                if chan == TICKER:
                    stream = f"{pair}@bookTicker/"
                else:
                    stream = f"{pair}@{chan}/"
                address += stream
        if address == f"{self.ws_endpoint}/stream?streams=":
            return None
        return address[:-1]

    def _check_update_id(self, pair: str, msg: dict) -> (bool, bool):
        skip_update = False
        forced = not self.forced[pair]

        if forced and msg['u'] < self.last_update_id[pair]:
            skip_update = True
        elif forced and msg['U'] <= self.last_update_id[pair] <= msg['u']:
            self.last_update_id[pair] = msg['u']
            self.forced[pair] = True
        elif not forced and self.last_update_id[pair] == msg['pu']:
            self.last_update_id[pair] = msg['u']
        else:
            self._reset()
            LOG.warning("%s: Missing book update detected, resetting book", self.id)
            skip_update = True
        return skip_update, forced

    async def _ticker(self, msg: dict, timestamp: float):
        """
        https://binance-docs.github.io/apidocs/delivery/cn/#symbol
        {
          "e":"bookTicker",         // 事件类型
          "u":17242169,             // 更新ID
          "s":"BTCUSD_200626",      // 交易对
          "ps":"BTCUSD",                 // 标的交易对
          "b":"9548.1",             // 买单最优挂单价格
          "B":"52",                 // 买单最优挂单数量
          "a":"9548.5",             // 卖单最优挂单价格
          "A":"11",                 // 卖单最优挂单数量
          "T":1591268628155,        // 撮合时间
          "E":1591268628166         // 事件时间
        }
        """
        pair = symbol_exchange_to_std(msg['s'])
        bid = Decimal(msg['b'])
        ask = Decimal(msg['a'])
        await self.callback(TICKER, feed=self.id,
                            symbol=pair,
                            bid=bid,
                            ask=ask,
                            bid_amount=Decimal(msg["B"]),
                            ask_amount=Decimal(msg["A"]),
                            timestamp=timestamp_normalize(self.id, msg['E']),
                            receipt_timestamp=timestamp)


    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        # Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
        # streamName is of format <symbol>@<channel>
        pair, _ = msg['stream'].split('@', 1)
        msg = msg['data']

        pair = pair.upper()

        msg_type = msg.get('e')
        if msg_type == 'bookTicker':
            await self._ticker(msg, timestamp)
        elif msg_type == 'depthUpdate':
            await self._book(msg, pair, timestamp)
        elif msg_type == 'aggTrade':
            await self._trade(msg, timestamp)
        elif msg_type == 'forceOrder':
            await self._liquidations(msg, timestamp)
        elif msg_type == 'markPriceUpdate':
            await self._funding(msg, timestamp)
        else:
            LOG.warning("%s: Unexpected message received: %s", self.id, msg)
