The producer.py script sends messages at 5-second intervals so that consumers have the opportunity to process them, and messages do not accumulate in the queue to avoid them ending up in a meter-long queue.
The producer sends messages with a key type called topic - meaning each message has its own key, and in turn, consumers receive messages according to the key.
In this program, in producer.py, there is a list called items containing dictionaries, each of which has both a key and a message.
