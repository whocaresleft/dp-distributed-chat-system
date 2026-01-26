# Distributed chat system

Simple decentralized and persistent chat system with a single writer implemented using **Golang**, **ZeroMQ**, **Gorillamux** and **gRPC** where users can send messages to each other in private chats as well as in group chats.

## Description

This project implements a very simple chat system with users and group chats. However there isn't just one single chat server: what the clients would see as the *server* is actually a distributed system.

The nodes that make up the distributed system cooperate on the control plane in order to achieve, and maintain, system stability.
Not only that, as they also cooperate on the data plane to achieve both workload distribution and data consistency.

- Workload distribution is achieved by having multiple nodes marked as *input* nodes, *i.e.*, they are tasked with interacting with the users (the *clients*); this allows for multiple flows of messages inside the topology, rather than having a single reachable endpoint;
- As for consistency, multiple nodes are chosen to be *persistence* nodes, storing *users, groups* and *messages*. Little consideration here: in the end a single writer-multiple readers approach was taken, where the system aims towards an *eventual* consistency (readers could be milliseconds late in updates).

The problem of deciding which node performs which task is solved by a two step process: first, an election, second, a SPT construction. Given the arbitrary network topology of the system, the election is carried out using the *Yo-Yo* algorithm. <br>
Once the election is completed the Leader broadcasts it using SHOUT; these messages are also used to build the SPT.<br>
Finally, once the tree is completed, each node knows its role following these rules:
- The root of the SPT (the leader) is the single writer;
- The leaves of the SPT are *input* nodes;
- The children of the root become *persistence* nodes, replicas of the writer, only if they are not leaves, otherwise they would be *input* only.
  

Inter-node communication has been realized using ZeroMQ sockets ([github.com/pebbe/zmq4](https://github.com/pebbe/zmq4)) with a ROUTER-ROUTER approach, simply because ROUTER sockets are uniquely identified, and so are the nodes since we carry out elections (the same ID was used for both); it felt like a perfect match.

Once the roles are set, the actual *data plane* becomes operative:
- Input nodes handle *client* interaction with an HTTP server, and act as a proxy when answering the client's requests, as it just sends it to a node that is capable of handling it, and just returing the response;
- Both readers and  the writer can answer to READ operations, while only the writer can handle  WRITE operations;
- All of the other nodes, that are neither persistence of input, are just *forwarders*, receiving a message from a node and sending it to another.

Thanks to the SPT construction and the role calculation, each node can follow these simple rules:
- Each node knows that a request needs to travel UP the tree, until the root (or a capable children) can handle it;
- Each node also knows that a response needs to travel DOWN the tree, getting back to the original leaf (this is done by handling a *table* of next hops).

The topology setup is done via a central **Bootstrap** server that deploys a **gRPC** server. This server is tasked with creating a topology, each time a node connects, it responds with the local network knowledge of that node (It tells it "these are your neighbors, here are their IPs").

Also, to simplify connection from the client side, a second central server, a **Nameserver** is deployed. This is much simpler that the previous one: it has both a **gRPC** and an **HTTP** server. Basically, each input node connects to it, registering its IP address and HTTP port. Then when a user wants to connect to the server, it can just connect to the *Nameserver* and it will be automatically redirected onto an input node (currently it uses round round to distribute incoming connection among the input nodes).  
In other words, the clients do not need to know each possible node's IP and port, but just the *nameserver*'s (as for now, it's binded to :9999) and the *nameserver* will automatically redirect us towards an *input* node.

---
From the client's perspective, the chat system is abstracted as a single logical server accessible via a REST API.

The routes are:
```
GET /  Shows main page

GET /register   Shows register form
POST /register  Register user
GET /login      Shows login form
POST /login     Login user
GET /logout     Logout user

GET /users                                    Shows a search bar to search users
DELETE /users/{uuid}                          Deletes a user
GET /users/{username}                         Gets all the users with that username
GET /users/{username}/{tag:[0-9]{3,6}}        Gets the user that matches username and tag
POST /users/{username}/{tag:[0-9]{3,6}}/chat  Sends a message to the chat
GET /users/{username}/{tag:[0-9]{3,6}}/chat   Gets messages from the chat

GET /groups                                      Shows a form to create a group
POST /groups                                     Creates a group
DELETE /groups/{uuid}                            Deletes a group
GET /groups/{uuid}                               Gets the group with that uuid
DELETE /groups/{group-uuid}/members/{user-uuid}  Removes a user from the group
GET /groups/{uuid}/members                       Gets members of a group
POST /groups/{uuid}/members                      Adds a member to a group
POST /groups/{uuid}/chat                         Sends a message to the group
GET /groups/{uuid}/chat                          Gets messages from the group
```

## Getting Started

### Project structure


### Dependencies
The project is made up of three binaries *Bootstrap*, *Nameservice* and *Node*. The only binary that requires both an external development header and library is *Node*, with ZeroMQ.

The project was developed exclusively on Linux, and has been tested on the following configurations:
- Linux, Kubuntu 24.04 LTS with GCC C compiler;
- Linux, WSL with GCC C compiler.

#### Linux
Building the project requires ZeroMQ development headers and libraries to be installed. The header itself is not enough, as the library zmq4 is just a wrapper for the original ZeroMQ, it doesn't include the library itself.

On most popular Linux distributions, the library development package can be easily installed using the package manager, for example on Ubuntu:
```
apt-get install libzmq3-dev
```

#### Windows
**DISCLAIMER** | Building on Windows is possible, even though I personally were not able to. Just as before, since zm4 is just a wrapper, we would need both development headers and libraries of ZeroMQ to be installed. 

Unfortunately, since MSVC is not cross platform, another C compiler is required, either Msys64 or MinGW for example. Also the building should be performed with the following environment variables:
```
$env:CGO_CFLAGS='-ID:/dev/vcpkg/installed/x64-windows/include'
$env:CGO_LDFLAGS='-LD:/dev/vcpkg/installed/x64-windows/lib -l:libzmq-mt-4_3_4.lib'
```

### Compilation
To build the applications you can directly use **go build**, from the directory that contains **go.mod** (adding .exe on windows after the filename):
```
go build -o nameservice cmd/nameservice/main.go
go build -o bootstrap cmd/bootstrap/main.go
go build -o node cmd/app/main.go
```
A makefile was also given, to build with the makefile, just do:
```
make all
```
This builds the bootstrap (./bootstrap), nameserver (./nameserver) and the node (./node), while also running a ./init.sh script that creates the necessary folders and .cfg files for 4 nodes.
### Executing
The three binaries can be executed on their own, but they all require each other to work at best.

*bootstrap* and *nameservice* can be directly executed from command line with no required parameters:
```
./nameservice
./bootstrap
```
The makefile can also be used to simplify execution, just do, in three different terminals:
```
make run-bootstrap         % Starts the bootstrap
make run-nameserver        % Starts the nameserver
make run-nodes             % Starts 4 nodes, with IDs 1 to 4 (all the configuration files were created and visible under ./NODE_1 to ./NODE_4).
```

However, note that as for now, *bootstrap* expects to find a folder named **B0OTSTRAP** in the same directory as the executable; inside such folder, it looks for **bootstrap.cfg** and **bootstra.log**. The *.cfg* file stores the last topology the bootstrap saw, it would be best for  any first start to empty its content.

*node*, on the other hand, requires one command line argument, that is the folder used by the node during execution to store the log files.
The path is relative to the directory of the *node* executable. <br>
Also, the folder must contain a file named **.cfg** that is properly set up, you can look at template in the repository (the folder **NODE_TMPL**).<br>
If we were to have this setup:
```
folder
|_node
|_NODE_1      // Also all the log files and db, by default are all here
  |_.cfg      
```
And if **.cfg** had this content:
```
{
    "folder-path": "NODE_1",
    "node-id":1,
    "control-plane-port":46001,
    "data-plane-port":50001,
    "enable-logging":true,
    "bootstrap-server-addr":"127.0.0.1:45999", // Adjust accordingly
    "name-server-addr":"127.0.0.1:45998", // Adjust accordingly
    "db-name":"chat-db.sql",
    "http-server-port":"8081",
    "template-directory":"templates",
    "read-timeout":10,
    "write-timeout":10,
    "secret-key": "generic-robust-password-wink-wink"
}
```

Then it would be ok to execute
```
./node NODE_1
```
Actually, these fields can be modified by hand with no problem, it's just used to load the node's configuration.
If you want to run more nodes you can run `./node <path>` more times, each time with a different folder (that has *.cfg* inside).
---
Alternatively, a multi-stage Docker configuration is also available to achieve reproducibility and to facilitate local execution and/or testing, by isolating go binaries from the **ZeroMQ** runtime dependency (by using a Debian based configuration, to unsure libraries are up to date).

A configuration-first approach was also taken, by offering an *init* script. Both *init.sh* (for Linux) and *init.bat* (for Windows) generate the necessary directories and files, in particular: 
- The folder **B0OTSTRAP** with the two *bootstrap.log* and *bootstra.cfg* files, both empty at first;
- The environment for six nodes, from **NODE_1** to **NODE_6**, each containing a default *.cfg* file, that can be manipulated according to ones desires.

**Again, both the *bootstrap* and *node* (or *app*) binaries expect the corresponding folder in the same directory as the binary itself.**

### Stopping
All three application require SIGINT (like CTRL-C or kill -2) to interrupt, you can do that for both the nameserver and bootstrap with no problem.
However, nodes were started in background. To stop them, from the same directory they were started, do:
```
make stop-nodes
```

### Deploying

Deployment would be advised for a Linux server as target. In such case, ensure that all ZeroMQ shared libraries are installed, and binaries would be moved into a production folder;
still, make sure that you also move the folder containing *.cfg* to the same destination as the binary (this applies for *node*). Also networking rules should be configured to allow 
inbound traffic on the designated ports for the HTTP ports (This would allow using port 80, for instance, instead of port 8080).

Before deployment, the *.cfg* files for nodes should be configured to have the correct *nameserver* and *bootstrap* address, also making sure that the chosen ports (*control*, *data* and *http*) allow both inbound and outbound traffic.

Same goes for *bootstrap* and *nameserver*, make sure that the used ports (currently **45999** for *bootstrap* and both **45998** and **9999** for *nameserver*) allow both inbound and outbound traffic.

## Help

If the nodes seem to get stuck after connecting to bootstrap and they seem to not communicate with each other, try to stop every node, as well as the bootstrap. Then empty the *bootstrap.cfg* file, restart the bootstrap, and then the nodes.

## Authors

7199598 - Francesco Biribò [GitHub](https://github.com/whocaresleft/whocaresleft)

## License

This project is licensed under the MIT License - see the LICENSE.md file for details

## Acknowledgments

Inspiration, code snippets, etc.
* [awesome-readme](https://github.com/matiassingers/awesome-readme)
* [PurpleBooth](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
* [dbader](https://github.com/dbader/readme-template)
* [zenorocha](https://gist.github.com/zenorocha/4526327)