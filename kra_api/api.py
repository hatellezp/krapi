import krakenex
import pandas as pd

"""
    what do i need ?
    - test the connection

    - all of this should be able to be done
      with a live connection and connecting each time to
        - get history
        - get assets (money and currencies)
        - get current orders
        - get asset price
        - put order
        - cancel order

"""

# TODO: see which methods do not require authentication and set them to
#       static methods
# TODO: I'm raising exceptions everywhere, this cannot happen, I need to account
#       for this situations


class Kapi:

    def __init__(
            self,
            max_amount,
            test_section=False,
            live_connection=False,
            key="",
            secret=""):
        """
        in fact all functions work in the same way, if the Kapi
        object is not live, each function will make a call to
        connection

        :param live_connection:
        :param test_section:
        :param max_amount: max amount allowed to use for trade
        :param key: the key used for kraken authentication
        :param secret: the secret used for kraken authentication
        """

        self.__api = krakenex.API(key, secret)
        self.__test_section = test_section
        self.__live_connection = live_connection
        self.__max_amount = max_amount

        pass

    def _public_query(self, method, data=None):
        """

        :param method: method asked to kraken: Ticker, Assets,...
        :param data: data passed to the method as asset pair
        :return: the error else the result wrapped in a dict
        """
        res = self.__api.query_public(method, data)

        if res["error"]:
            return res["error"]
        else:
            return {"result": res["result"]}

    def query_time(self):
        """

        :return: the time of the server, e.g.:
            {
                "error": [ ],
                "result":
                    {
                        "unixtime": 1616336594,
                        "rfc1123": "Sun, 21 Mar 21 14:23:14 +0000"
                    }
            }
        """
        return self._public_query("Time")

    def query_system_status(self):
        """

        :return: status of the server, e.g.:
            {
                "error": [],
                "result": {
                    "status": "online",
                    "timestamp": "2021-03-21T15:33:02Z"
                }
            }
        """
        return self._public_query("SystemStatus")

    def test_connection(self):
        """
        test the connection to the server
        :return: True if the connection is alive and the server online,
                 False otherwise
        """
        res = self.query_system_status()
        return "result" in res and res["result"]["status"] == "online"

    def query_assets(self, asset=None):
        """
        Get information about the assets that are available for deposit,
        withdrawal, trading and staking.
        :param asset: a particular pair to get the info on or a list of pairs, if None is passed
                      then gets all
        :return: a data frame with the information:
        {
            "error": [],
            "result": {
                "XXBT": {
                    "aclass": "currency",
                    "altname": "XBT",
                    "decimals": 10,
                    "display_decimals": 5
                },
                "ZEUR": {
                    "aclass": "currency",
                    "altname": "EUR",
                    "decimals": 4,
                    "display_decimals": 2
                },
                "ZUSD": {
                    "aclass": "currency",
                    "altname": "USD",
                    "decimals": 4,
                    "display_decimals": 2
                }
            }
        }
        to data frame:
            cryptocurrency    aclass altname decimals display_decimals
        0          False    currency   1INCH       10                5
        1          False    currency    AAVE       10                5
        2          False    currency     ACA       10                5
        3          False    currency     ADA        8                6
        4          False    currency   ADA.S        8                6
        """

        if asset is None:
            data = None
        elif isinstance(asset, list):
            data = {"asset": ",".join(asset)}
        elif isinstance(asset, str):
            data = {"asset": asset}
        else:
            raise Exception(f"Unknown argument for the asset: {asset}")

        res = self._public_query("Assets", data=data)

        if "result" in res:
            columns = [
                "cryptocurrency",
                "aclass",
                "altname",
                "decimals",
                "display_decimals"]
            data = pd.DataFrame(columns=columns)
            data_raw = res["result"]
            for currency in data_raw:
                inner_df = pd.DataFrame([[
                    currency.startswith("X"),
                    data_raw[currency]["aclass"],
                    data_raw[currency]["altname"],
                    data_raw[currency]["decimals"],
                    data_raw[currency]["display_decimals"],
                ]], columns=columns)
                data = pd.concat([data, inner_df], ignore_index=True)

            return data
        else:
            return res

    def query_asset_pairs(self, pair=None):
        """

        :param pair: a list of pairs to get info on or a lone pair as a string,
                     if none is provided it defaults to get for all the pairs
        :return: a dataframe with all the info
        """
        if pair is None:
            data = None
        elif isinstance(pair, list):
            data = {"pair": ",".join(pair)}
        elif isinstance(pair, str):
            data = {"asset": pair}
        else:
            raise Exception(f"Unknown argument for the asset: {pair}")

        res = self._public_query("AssetPairs", data)

        if "result" in res:
            columns = [
                "altname",
                "wsname",
                "aclass_base",
                "base",
                "aclass_quote",
                "quote",
                "lot",
                "pair_decimals",
                "lot_decimals",
                "lot_multiplier",
                "fees_0",
                "fees_50000",
                "fees_100000",
                "fees_250000",
                "fees_1000000",
                "fees_maker_0",
                "fees_maker_50000",
                "fees_maker_100000",
                "fees_maker_250000",
                "fees_maker_1000000",
                "fee_volume_currency",
                "margin_call",
                "margin_stop",
                "ordermin"]

            data = pd.DataFrame(columns=columns)
            data_raw = res["result"]
            for currency in data_raw:
                inner_df = pd.DataFrame([[
                    data_raw[currency]["altname"],
                    data_raw[currency]["wsname"],
                    data_raw[currency]["aclass_base"],
                    data_raw[currency]["base"],
                    data_raw[currency]["aclass_quote"],
                    data_raw[currency]["quote"],
                    data_raw[currency]["lot"],
                    data_raw[currency]["pair_decimals"],
                    data_raw[currency]["lot_decimals"],
                    data_raw[currency]["lot_multiplier"],
                    data_raw[currency]["fees"][0][1],
                    data_raw[currency]["fees"][1][1],
                    data_raw[currency]["fees"][2][1],
                    data_raw[currency]["fees"][3][1],
                    data_raw[currency]["fees"][4][1],
                    data_raw[currency]["fees_maker"][0][1],
                    data_raw[currency]["fees_maker"][1][1],
                    data_raw[currency]["fees_maker"][2][1],
                    data_raw[currency]["fees_maker"][3][1],
                    data_raw[currency]["fees_maker"][4][1],
                    data_raw[currency]["fee_volume_currency"],
                    data_raw[currency]["margin_call"],
                    data_raw[currency]["margin_stop"],
                    data_raw[currency]["ordermin"],
                ]], columns=columns)
                data = pd.concat([data, inner_df], ignore_index=True)

            return data
        else:
            return res

    def query_ticker(self, pair):
        """

        :param pair: a pair to get the current ticker info on
        :return: a dataframe with the info

        {
            "error": [ ],
            "result":
            {
                "XXBTZUSD":
                    {
                        "a":
                    [
                        "52609.60000",
                        "1",
                        "1.000"
                    ],
                    "b":
                    [
                        "52609.50000",
                        "1",
                        "1.000"
                    ],
                    "c":
                    [
                        "52641.10000",
                        "0.00080000"
                    ],
                    "v":
                    [
                        "1920.83610601",
                        "7954.00219674"
                    ],
                    "p":
                    [
                        "52389.94668",
                        "54022.90683"
                    ],
                    "t":
                    [
                        23329,
                        80463
                    ],
                    "l":
                    [
                        "51513.90000",
                        "51513.90000"
                    ],
                    "h":
                        [
                            "53219.90000",
                            "57200.00000"
                        ],
                        "o": "52280.40000"
                    }
                }
        }
        """
        res = self._public_query("Ticker", {"pair": pair})

    def get_prices_history(
            self,
            from_timestamp,
            to_timestamp,
            assets,
            timestep='min'):
        """

        :param from_timestamp: from when the history should be taken
        :param to_timestamp:  to when
        :param assets: a list of assets to get the history (e.g. ["BTC", "ETH", ..])
        :param timestep: what is the step to take
        :return:
        """

        if timestep not in ['sec', 'min', 'hour', 'day']:
            raise Exception(f"Unknown timestep: {timestep}")

        pass

    def get_trade_history(self, from_timestamp, to_timestamp, pairs):
        """

        :param from_timestamp:
        :param to_timestamp:
        :param pairs: a list of pairs to get the trades (e.g ["EURBCT"...] not sure about the
            writing though)
        :return:
        """
        pass

    def get_assets(self):
        """

        :return: should return a list of pairs [("BTC", moneny on BTC), ...]
        """
        pass

    def get_currency(self):
        """

        :return: the current money that we have
        """
        pass

    def get_asset_price(self, asset, currency="EUR"):
        """

        :param currency: a currency to get the price of the asset in
        :return: the current price of the asset for the currency
        """
        pass

    def _put_order(
            self,
            pair,
            amount,
            direction,
            order_type,
            current_hold_amount=None,
            **kwargs):
        """

        :param pair: for example "EURBTC" means buy or sell BTC for euros
        :param direction: to buy or to sell
        :param order_type: limit or others
        :param kwargs: I think it will depend on the order type
        :return: the id of the order (I think)
        """

        if direction not in ["sell", "buy"]:
            raise Exception(f"Unknown type of direction: {direction}")
        else:
            if direction == "buy" and self.__test_section and not self.can_buy_with_amount(
                    amount, self.__max_amount, current_hold_amount):
                raise Exception(
                    f"Cannot buy that amount, ouf of bonds: max amount: {self.__max_amount}, amount to trade: {amount}")
            elif direction == "buy" and not self.__test_section and not self.can_buy_with_amount(amount, self.__max_amount, self.get_currency()):
                raise Exception(
                    f"Cannot buy that amount, ouf of bonds: max amount: {self.__max_amount}, amount to trade: {amount}, current amount hold: {self.get_currency()}")
            else:
                pass

    def buy_order(self, pair, amount, order_type, **kwargs):
        self._put_order(pair, amount, "buy", order_type, kwargs)

    def sell_order(self, pair, amount, order_type, **kwargs):
        self._put_order(pair, amount, "sell", order_type, kwargs)

    def cancel_order(self, order_id) -> bool:
        """

        :param order_id:  the id of the order to cancel
        :return: True if the order was successfully cancelled, False otherwise
        """
        pass

    def can_buy_with_amount(
            self,
            amount_to_trade,
            max_amount,
            current_hold_amount=None):
        """

        :param amount_to_trade: the amount we want to trade
        :param max_amount: the maximal amount of currency allowed to trade
        :param current_hold_amount: in test mode the current hold amount is provided
        :return:
        """
        if self.__test_section and current_hold_amount is None:
            raise Exception(
                "For a test section a current amount must be provided")
        elif self.__test_section:
            return amount_to_trade + current_hold_amount <= max_amount
        else:
            pass
