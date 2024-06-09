""" 
This consumer is setup to alert us if the temperature of food B changes
less than 1 degree F in 10 minutes. It pulls the data from the 03-food-B queue using 
RabbitMQ. 
There is at least one reading every 30 seconds, and the food time window is 10 minutes.
    Author: Luci McDaniel
    Date: June 7, 2024
"""
import pika
import sys
import time
import math

from util_logger import setup_logger
logger, logname = setup_logger(__file__)

from collections import deque
foodB_deque = deque(maxlen=20) #limited to the 20 most recent readings

# define a callback function to be called when a message is received
def foodB_callback(ch, method, properties, body):
    temp = ['0']
    message = body.decode()

    # decode the binary message body to a string
    logger.info(f" [x] Received {message}")

    #split the message by comma so only the temp is read
    parts = message.split(',')
    temp[0] = float(parts[1].strip())

    foodB_deque.append(temp)

    if len(foodB_deque) == 5:
        temp_diff = foodB_deque[-1][0] - foodB_deque[0][0]
        if temp_diff < 1:
          logger.info(f"Alert! Food B temperature has stalled. Current temp: {temp}")
        else:
            logger.info(f" [x] Current temp: {temp[0]}")
    else:
        logger.info(f" [x] Current temp: {temp[0]}")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main function to run the program
def main(hn: str = "localhost", qn: str ="03-food-B"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        logger.info()
        logger.info("ERROR: connection to RabbitMQ server failed.")
        logger.info(f"Verify the server is running on host={hn}.")
        logger.info(f"The error says: {e}")
        logger.info()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        ch = connection.channel()
        ch.queue_delete("03-food-B")
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue="03-food-B", durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        ch.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        ch.basic_consume( queue="03-food-B", auto_ack=False, on_message_callback=foodB_callback)

        # print a message to the console for the user
        logger.info(" [*] Ready to read food temperatures. To exit press CTRL+C")

        # start consuming messages via the communication channel
        ch.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logger.info()
        logger.info("ERROR: something went wrong.")
        logger.info(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info()
        logger.info(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.info("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost", "03-food-B")