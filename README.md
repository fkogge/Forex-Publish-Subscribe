# Forex-Publish-Subscribe
Coursework from Seattle University (MSCS) Distributed Systems.
## Description
This program simulates a publish-subscribe messaging pattern, using UDP sockets. The publisher is a Foreign Exchange provider that sends forex quotes to any subscriber that is listening, in the form of byte streams. The subscriber processes these quotes, and must unmarshal them into a readable format. 
We create a graph of the posted currencies and the negative logarithms of their exchange rates as edge weights, so that we can add the edge weights to detect arbitrage through negative cycles, using the Bellman-Ford shortest paths algorithm.
