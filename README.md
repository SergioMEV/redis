# Bare-bones Redis
---
Hello! This repository contains my implementation of a basic [_Redis_](https://redis.io/docs/about/). As stated in the documentation, Redis is an "in-memory data structure store used as a database, cache, message broker, and streaming engine." My implementation here is not as fancy as that, but it does support multiple client connections and lets clients share access to a key-value map. This is only the implementation of the server side of Redis, maybe I will tackle the client side someday. Also, I opted for using threads instead of an event loop for this first try at implementing Redis, however I will be tackling the event loop one day. Hopefully, sooner rather than later.

---
## Commands Implemented

This implementation contains the following commands:
- __PING__: Command used to check whether a server is up.
- __ECHO__: Command that can be used to get the server to echo the given arguments.
- __SET__: Command to update the key-value hashmap with the given key and value. As per the actual Redis implementation, this can overwrite values for pre-existing keys. I have also implemented the expiry function of this command, in which you can set a time limit for the given key-value pair.
- __GET__: Command to query the hashmap in the server for a value with the given key.
---
## Next Steps
Next, I will be implementing the event loop in order to make this a single-threaded implementation of Redis, which is how the actual Redis works. The benefit of the event loop is that it allows you to ensure the atomicity of operations without the need for locks or any other primitives. This, in turn, makes the server more realiable.

---
## Acknowledgements
This was done as a coding challenge for [codecrafters](https://www.codecrafters.io), which is a great website that challenges you to build popular tools and does not hold your hand in the process. What I like about codecrafters is that they let you explore the topic on your own and give you little to no guidance on how to implement something, other than a set of goals and a tool to test your implementation. Overall, it is a really cool website, although a little pricey for students, and I really recommend it.