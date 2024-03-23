"""
    Description: For an exchange, get all trading pairs, their latest prices and trading volume for 24 hours
    Task: 
        Create a class inherited from the BaseExchange class. 
        Write the implementation of the methods and fill in the required fields (marked as "todo")
    Note: 
        Feel free to add another internal methods. 
        It is important that the example from the main function runs without errors
    The flow looks like this:
        1. Request data from the exchange
        2. We bring the ticker to the general format
        3. We extract from the ticker properties the last price, 
            the 24-hour trading volume of the base currency 
            and the 24-hour trading volume of the quoted currency. 
            (at least one of the volumes is required)
        4. Return the structure in the format: 
            {
                "BTC/USDT": TickerInfo(last=57000, baseVolume=11328, quoteVolume=3456789),
                "ETH/BTC": TickerInfo(last=4026, baseVolume=4567, quoteVolume=0)
            }
"""

import asyncio

import aiohttp
from dataclasses import dataclass


@dataclass
class TickerInfo:
    last: float  # Last price
    baseVolume: float  # Base currency volume_24h
    quoteVolume: float  # Target currency volume_24h


Symbol = str  # Trading pair like ETH/USDT


class BaseExchange:
    async def fetch_data(self, url: str):
        """
        :param url: URL to fetch the data from exchange
        :return: raw data
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp and resp.status == 200:
                    data = await resp.json()
                else:
                    raise Exception(resp)
        return data

    async def fetch_tickers(self) -> dict[Symbol, TickerInfo]:
        """
        Method fetch data from exchange and return all tickers in normalized format
        :return:
        """
        raise NotImplementedError

    def normalize_data(self, data: dict) -> dict[Symbol, TickerInfo]:
        """
        :param data: raw data received from the exchange
        :return: normalized data in a common format
        """
        raise NotImplementedError

    def _convert_symbol_to_ccxt(self, symbols: str) -> Symbol:
        """
        Trading pairs from the exchange can come in various formats like: btc_usdt, BTCUSDT, etc.
        Here we convert them to a value like: BTC/USDT.
        The format is as follows: separator "/" and all characters in uppercase
        :param symbols: Trading pair ex.: BTC_USDT
        :return: BTC/USDT
        """
        raise NotImplementedError

    async def load_markets(self):
        """
        Sometimes the exchange does not have a route to receive all the tickers at once.
        In this case, you first need to get a list of all trading pairs and save them to self.markets.(Ex.2)
        And then get all these tickers one at a time.
        Allow for delays between requests so as not to exceed the limits
        (you can find the limits in the documentation for the exchange API)
        """

    async def close(self):
        pass  # stub, not really needed


class narkasa(BaseExchange):
    """
    docs: https://apidocs.narkasa.com/#get-all-markets-24h
    """

    def __init__(self):
        self.id = "narkasa"
        self.base_url = "https://api.narkasa.com/"
        self.markets = {}  # {market: symbol} pairs
        self.symbols = {}  # {symbol: market} pairs
        self.code_ok = "00000"

    async def fetch_tickers(self) -> dict[Symbol, TickerInfo]:
        if not self.markets:
            await self.load_markets()
        result = {}
        for symbol in self.markets.values():
            print(f"Fetching: {symbol}")
            data = await self.fetch_data(
                self.base_url + "v3/api/market/24h?symbol=" + symbol
            )
            if data["code"] == self.code_ok:
                result.update(self.normalize_data(data))
        return result

    def normalize_data(self, data: dict) -> dict[Symbol, TickerInfo]:
        normalized_data = {}
        ticker = data.get("market", {})
        symbol = self._convert_symbol_to_ccxt(ticker.get("symbol", ""))
        normalized_data[symbol] = TickerInfo(
            last=float(ticker.get("last", 0)),
            baseVolume=float(ticker.get("volumeQty", 0)),
            quoteVolume=float(ticker.get("volume", 0)),
        )
        return normalized_data

    def _convert_symbol_to_ccxt(self, symbols: str) -> Symbol:
        if isinstance(symbols, str):
            if symbols in self.symbols:
                return self.symbols[symbols]
            raise ValueError(f"{symbols} invalid value")
        raise TypeError(f"{symbols} invalid type")

    async def load_markets(self):
        self.markets = {}
        self.symbols = {}
        data = await self.fetch_data(self.base_url + "v3/api/market/markets")
        if data.get("code") != self.code_ok:
            return
        symbols = data.get("markets", [])
        for symbol in symbols:
            base = symbol.get("firstSymbol")
            quote = symbol.get("secondSymbol")
            symbol_name = symbol.get("symbol")
            if base and quote:
                market = base + "/" + quote
                self.markets[market] = symbol_name
                self.symbols[symbol_name] = market


# EXAMPLE 1


class biconomy(BaseExchange):
    """
    docs: https://github.com/BiconomyOfficial/apidocs?tab=readme-ov-file#Getting-Started
    """

    def __init__(self):
        self.id = "biconomy"
        self.base_url = "https://www.biconomy.com/"
        self.markets = {}  # not really needed, just a stub

    async def fetch_tickers(self) -> dict[str, TickerInfo]:
        data = await self.fetch_data(self.base_url + "api/v1/tickers")
        return self.normalize_data(data)

    def _convert_symbol_to_ccxt(self, symbols: str) -> Symbol:
        if isinstance(symbols, str):
            symbols = symbols.replace("_", "/")
            return symbols
        raise TypeError(f"{symbols} invalid type")

    def normalize_data(self, data: dict) -> dict[Symbol, TickerInfo]:
        normalized_data = {}
        tickers = data.get("ticker", [])
        for ticker in tickers:
            symbol = self._convert_symbol_to_ccxt(ticker.get("symbol", ""))
            normalized_data[symbol] = TickerInfo(
                last=float(ticker.get("last", 0)),
                baseVolume=float(ticker.get("vol", 0)),
                quoteVolume=0,
            )
        return normalized_data


# Example 2  (with load markets)


class toobit(BaseExchange):
    """
    docs: https://toobit-docs.github.io/apidocs/spot/v1/en/#24hr-ticker-price-change-statistics
    """

    def __init__(self):
        self.id = "toobit"
        self.base_url = "https://api.toobit.com/"
        self.markets = {}

    async def fetch_tickers(self) -> dict[Symbol, TickerInfo]:
        if not self.markets:
            await self.load_markets()

        result = {}
        for symbol in self.markets.values():
            print(f"Fetching: {symbol}")
            data = await self.fetch_data(
                self.base_url + "quote/v1/ticker/24hr?symbol=" + symbol
            )
            result.update(self.normalize_data(data))
        return result

    async def load_markets(self):
        data = await self.fetch_data(self.base_url + "api/v1/exchangeInfo")
        symbols = data.get("symbols", [])
        for symbol in symbols:
            base = symbol["baseAsset"]
            quote = symbol["quoteAsset"]
            if base and quote:
                self.markets[base + "/" + quote] = base + quote

    def normalize_data(self, data: list) -> dict[Symbol, TickerInfo]:
        normalized_data = {}
        result = data[0]
        symbol = self._convert_symbol_to_ccxt(result.get("s"))
        normalized_data[symbol] = TickerInfo(
            last=float(result.get("c", 0)),
            baseVolume=float(result.get("v", 0)),
            quoteVolume=float(result.get("qv", 0)),
        )
        return normalized_data

    def _convert_symbol_to_ccxt(self, symbols: str) -> Symbol:
        if isinstance(symbols, str):
            if symbols.endswith("USDT"):
                symbols = symbols.replace("USDT", "/USDT")
            return symbols
        raise TypeError(f"{symbols} invalid type")


async def main():
    """
    Test yourself here. Verify prices and volumes here: https://www.coingecko.com/
    """
    exchange = narkasa()
    # exchange = biconomy()
    # exchange = toobit()
    await exchange.load_markets()
    tickers = await exchange.fetch_tickers()
    for symbol, prop in tickers.items():
        print(symbol, prop)

    assert isinstance(tickers, dict)
    for symbol, prop in tickers.items():
        assert isinstance(prop, TickerInfo)
        assert isinstance(symbol, Symbol)


if __name__ == "__main__":
    asyncio.run(main())
