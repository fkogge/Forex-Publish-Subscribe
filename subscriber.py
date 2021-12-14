"""
Simulates a publish-subscribe messaging pattern by implementing a subscriber
that listens for quotes from the Forex provider publishing server. All messages
are sent and received over a UDP socket. The subscriber runs the Bellman-Ford
algorithm on published quotes to detect arbitrage within the posted markets,
and prints any of these arbitrage opportunities.

Author: Francis Kogge
Date: 10/26/2021
Course: CPSC 5520 (Distributed Systems)
"""

import math
import sys
import socket
import time
import bellman_ford
import fxp_bytes_subscriber as fxp_bytes_sub
from datetime import datetime


LISTENER_ADDRESS = ('localhost', 0)  # Subscriber address
SUBSCRIPTION_TIME = 600  # 600 seconds = 10 minutes
MESSAGE_TIMEOUT = 60  # seconds -> 1 minute
BYTES_PER_QUOTE = 32  # Number of bytes expected from a single Forex quote
IN_FORCE_TIME = 1.5  # Seconds that a price is in force for
MY_CURRENCY = 'USD'  # Currency to trade for arbitrage
BUFFER_SIZE = 4096  # Message buffer size
TRADE_AMOUNT = 100  # Initial cash to trade
TOLERANCE = 1e-6  # Floating point comparison tolerance


class Subscriber(object):
    """
    Subscribes to the Forex provider publishing server. Listens for incoming
    messages via a listening server. All messages are sent/received over a UDP
    socket.
    """

    def __init__(self, host, port):
        """
        Initializes the Forex provider address, the listener socket and address,
        subscription variable, and the currency graph.
        :param host: Forex provider host name
        :param port: Forex provider port number
        """
        self.provider_address = (host, int(port))
        self.listener, self.listener_address = self.start_listening_server()
        self.resubscribe = True
        self.market_times = {}
        self.graph = bellman_ford.BellmanFordGraph()
        print('Started up listener on {} at [{}].'
              .format(self.listener_address, self.print_time(datetime.now())))

    def run(self):
        """
        Subscribes to the Forex provider and listens for incoming messages from
        it. Resubscribes after SUBSCRIPTION_TIME is up.
        """
        while self.resubscribe:
            self.subscribe_to_forex_provider()
            self.listen_for_quotes()

    def subscribe_to_forex_provider(self):
        """
        Subscribes to the Forex provider using a UDP socket.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as forex_sock:
            serialized_address = fxp_bytes_sub.serialize_address(self.listener_address)
            # Forex provider expects my serialized address as the message data
            forex_sock.sendto(serialized_address, self.provider_address)
        print('Subscribed to Forex Provider on {} at [{}]'
              .format(self.provider_address, self.print_time(datetime.now())))

    def listen_for_quotes(self, buffer_size=BUFFER_SIZE):
        """
        Listens for quotes from the Forex provider. If a message is received, it
        processes the foreign exchange quotes. If no message is received after
        set timeout limit (MESSAGE_TIMEOUT), the subscriber will shutdown.
        :param buffer_size: amount of bytes to receive
        """
        end_time = time.time() + SUBSCRIPTION_TIME
        # Timed loop which runs for duration of the subscription time
        while self.resubscribe and time.time() < end_time:
            self.listener.settimeout(MESSAGE_TIMEOUT)  # How long we'll wait
            start_time = datetime.now()  # When we started listening

            try:
                forex_byte_data = self.listener.recv(buffer_size)
                start_time = datetime.now()  # When this quote came in

            except (OSError, Exception) as e:
                # If we waited long enough for a message and haven't gotten
                # anything from the Forex provider, then shutdown
                print('Current Time: [{}] -> no message received since [{}]: shutting down.'
                      .format(self.print_time(datetime.now()), self.print_time(start_time)))
                self.resubscribe = False

            else:
                # Successfully received quote from Forex Provider
                self.process_quote(forex_byte_data, start_time.timestamp())
                self.remove_stale_quotes()

    def process_quote(self, forex_bytes, start_time, trade_curr=MY_CURRENCY):
        """
        Un-marshals and processes the Forex quote byte data and reports on any
        arbitrage opportunities, if a negative cycle is found in the currency
        graph.
        :param forex_bytes: forex message in byte form
        :param start_time: timestamp of when the quote was received
        :param trade_curr: currency (country) subscriber wants to trade
        """
        quote_list = fxp_bytes_sub.unmarshal_message(forex_bytes)
        for quote in quote_list:
            quote_time = quote['timestamp'].timestamp()
            print(self.print_forex_message(quote))

            # Check if quote time matches the time which the message was
            # was actually received (approximately because floating points
            # can vary slightly)
            if math.isclose(start_time, quote_time):
                price = quote['price']
                currency_1, currency_2 = quote['cross'].split(' ')
                self.market_times[(currency_1, currency_2)] = quote_time
                self.add_exchange_rates(currency_1, currency_2, price, quote_time)
            else:
                print('Ignoring out of sequence message.')

        dist, pred, neg_edge = self.graph.shortest_paths(trade_curr, TOLERANCE)
        if neg_edge:
            conversions = self.build_arbitrage_sequence(pred)
            self.print_arbitrage_sequence(conversions)

    def build_arbitrage_sequence(self, pred, start_currency=MY_CURRENCY):
        """
        Builds the currency exchange sequence which needs to be followed in
        order to achieve arbitrage. Returns the sequence of currencies in a
        list.
        :param pred: predecessors path received from the Bellman Ford shortest
                     paths algorithm, used to build the arbitrage path
        :param start_currency: starting currency
        :return: list of currencies that can be exchanged, in order of the
                 arbitrage sequence
        """
        # The last currency that cycles back to our original currency is the
        # predecessor of the original currency in the cycle
        currency = pred[start_currency]
        conversions = [currency]

        while start_currency != currency:
            if pred[currency] in conversions:
                # If no exchange rate is present from starting currency
                while not self.graph.has_edge(start_currency, currency):
                    # Remove last added currency so that the starting currency
                    # can find a currency to exchange with (it has an edge to)
                    conversions.pop()
                    currency = conversions[-1]
                currency = start_currency

            else:
                # Backtrack to predecessor of the current currency
                currency = pred[currency]

            conversions.append(currency)

        # We backtracked the arbitrage sequence so reverse to get the
        # correct order
        conversions.reverse()
        return conversions

    def print_arbitrage_sequence(self, conversions, start_currency=MY_CURRENCY,
                                 start_amount=TRADE_AMOUNT):
        """
        Prints the arbitrage sequence from the given list of currency exchanges.
        :param conversions: list of currencies, in order of the arbitrage
                            sequence
        :param start_currency: starting currency
        :param start_amount: starting amount/cash to trade
        """
        print('ARBITRAGE:')
        print('\tStart with {} {}'.format(start_currency, start_amount))
        # Continually exchange currency through the arbitrage path
        for i in range(len(conversions)):
            from_curr = conversions[i]
            to_curr = conversions[(i + 1) % len(conversions)]  # Wrap to 0
            neg_log_rate = self.graph.get_edge_weight(from_curr, to_curr)
            rate = 10 ** (-1 * neg_log_rate)  # Convert back to exchange rate
            start_amount *= rate  # Keep converting on the exchange rate
            print('\texchange {} for {} at {} --> {} {}'
                  .format(from_curr, to_curr, rate, to_curr, start_amount))

    def add_exchange_rates(self, currency_1, currency_2, rate, time):
        """
        Adds the negative logarithm of the exchange rate and inverse exchange
        rate to the currency graph.
        :param currency_1: currency involved
        :param currency_2: the other currency involved
        :param rate: exchange rate from currency_1 to currency_2
        :param time: timestamp of when the quote was posted
        """
        neg_log_rate = -1 * math.log10(rate)
        # Add forward and reverse quote edges for the provided and inverse rates
        self.graph.add_edge(currency_1, currency_2, neg_log_rate, time)
        self.graph.add_edge(currency_2, currency_1, -1 * neg_log_rate, time)

    def remove_stale_quotes(self):
        """
        Removes any market quotes that are stale and should no longer be in
        force (have been present in currency graph and market time list for
        longer than IN_FORCE_TIME limit).
        """
        remove_list = []
        for market, quote_time in self.market_times.items():
            time_diff = datetime.now().timestamp() - quote_time
            if time_diff > IN_FORCE_TIME:
                # Have to remove in both directions
                self.graph.remove_edge(market[0], market[1])
                self.graph.remove_edge(market[1], market[0])
                remove_list.append(market)
                print('Removed stale quote: {}/{} [{}s]'
                      .format(market[0], market[1], round(time_diff, 2)))

        for market in remove_list:
            del self.market_times[market]

    @staticmethod
    def start_listening_server():
        """
        Starts the subscriber's listening server, using a UDP socket. Returns
        the listener socket and IP address.
        :return: listener socket, listener address
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener.bind(LISTENER_ADDRESS)
        return listener, listener.getsockname()

    @staticmethod
    def print_forex_message(quote):
        """
        Helper method for printing a Forex quote.
        :param quote: the Forex quote
        """
        return ' '.join([str(quote['timestamp']),
                         str(quote['cross']),
                         str(quote['price'])])

    @staticmethod
    def print_time(date_time):
        """
        Printing helper for current timestamp.
        :param date_time: datetime object
        """
        return date_time.strftime('%H:%M:%S.%f')


def main():
    """
    Executes program from the main entry point.
    """
    # Expects 2 additional arguments
    if len(sys.argv) != 3:
        print('Usage: subscriber.py FOREX_PROVIDER_HOST FOREX_PROVIDER_PORT')
        exit(1)

    host, port = sys.argv[1:]
    subscriber = Subscriber(host, port)
    subscriber.run()


if __name__ == '__main__':
    main()
