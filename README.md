# E-Chord
Chord DHT implementation in Python.

## Description
E-Chord is a python implementation of the Chord protocol, a peer-to-peer lookup service.

## Features
On top of the features of the base Chord protocol, the following ones have been added or updated:

- **Seed Server**: The new node learns the identity of an existing E-Chord node by contacting the [Seed Server](https://github.com/notTypecast/E-Chord-Seed).
- **Concurrent Joins**: Multiple nodes joining concurrently is supported using a periodic stabilization routine.
- **Handling Massive Node Failures**: Network consistency is preserved after multiple nodes fail simultaneously. The stabilization routine is responsible for keeping each node's successor up to date.
- **Data Storage**: Each node can hold data in key-value pairs. Any node can be contacted to store such a pair, but the pair will be stored in a node determined by the network. As new nodes join the network, data is tranferred to them accordingly.
- **Load Balancing**: Data is shared between multiple nodes, such that no one node holds a much larger amount of keys than others. In this way, the load is balanced between the nodes.
- **Data Consistency**: Data inserted into the network has a very high probability not to be lost, even if multiple nodes join or leave the network concurrently.
- **Ring Split Prevention**: By having each node keep a successor list of size log(n), where n is the maximum number of nodes that can join the network, it is guaranteed that, in most situations, a massive node failure will not split the ring into multiple inconsistent parts.
- **Custom Parameters**: By editing the `config/params.json` file, various parameters regarding the network and scripts can be customized. It is required that all nodes run with the same parameters.

## Simulation Scripts
All simulation scripts assume nodes are running locally, on the same machine (`localhost`).

- **Visualizer**: Pings each node periodically, retrieving information regarding its successor, its predecessor, and how many keys that node has. Shows the information on the screen.
- **Mass Node**: Adds multiple nodes to the network. After waiting for a set amount of time, removes a percentage of those nodes from the network. Values can be customized in the `params.json` file.
- **Mass Data**: Adds or looks up data in the network. The input file name must be specified as the first parameter. The required format is JSON. The second parameter should either be `insert`/`i`, or `lookup`/`l`. The former will add the data to the network, whereas the latter will look up the data. Both scripts will display the failure percentage of their respective requests. 
 
## Potential Improvements
- **Data Backups**: If a node fails abruptly, all the data it contains will be lost. Data should be replicated across nodes to preserve consistency, even after massive node failures.

## References
[1] Ion Stoica, Robert Morris, David Karger, M. Frans Kaashoek, and Hari Balakrishnan. 2001. Chord: A scalable peer-to-peer lookup service for internet applications. SIGCOMM Comput. Commun. Rev. 31, 4 (October 2001), 149–160. DOI:https://doi.org/10.1145/964723.383071