"""
    This consumer is setup to alert us if the smart smoker overall temperature
    decreases by more than 15 degrees F in 2.5 minutes. It pulls the data from the 01-smoker queue using 
    RabbitMQ. 
    There is at least one reading every 30 seconds, and the smoker time window is 2.5 minutes.
    Author: Luci McDaniel
    Date: June 6, 2024
"""

import pika
import sys
import time
import math

from util_logger import setup_logger
logger, logname = setup_logger(__file__)

from collections import deque
smoker_deque = deque(maxlen=5) # the 5 most recent readings

# define a callback function to be called when the data is received.
def smoker_callback(ch, method, properties, body):
    temp = ['0']
    message = body.decode()
    
    # decode the binary message body to a string
    logger.info(f" [x] Received: {message}")

    #split the message by comma so only the temp is read
    parts = message.split(',')
    temp[0] = float(parts[1].strip())

    smoker_deque.append(temp) #append the new temperature to the deque
    #subtracting current temp from previously read temp to alert when difference of 15 degrees is reached.
    if len(smoker_deque) == 5:
    
        temp_diff = temp[-1] - temp[0]
        if temp_diff < -15:
            logger.info(f"Smoker Alert! The temperature has decreased by 15 degrees F. Current temperauture: {smoker_deque[-1]}F")
    # when done with task, tell the user
        else:
            logger.info(f" [x] Current temp: {temp[0]}")
    else:
        logger.info(f" [x] Current temp: {temp[0]}")
    
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)   #delete each queue before declaring a new one
    ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main function to run the program
def main(hn: str = "localhost", qn: str ="01-smoker"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        logger.info("ERROR: connection to RabbitMQ server failed.")
        logger.info(f"Verify the server is running on host={hn}.")
        logger.info(f"The error says: {e}")
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        ch = connection.channel()
        ch.queue_delete("01-smoker")
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue="01-smoker", durable=True)

     

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unacknowledged messages      
        ch.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named smoker_callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        ch.basic_consume( queue="01-smoker", auto_ack=False, on_message_callback=smoker_callback)

        # print a message to the console for the user
        logger.info(" [*] Ready to read temperatures. To exit press CTRL+C")

        # start consuming messages via the communication channel
        ch.start_consuming()
        
    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logger.info("ERROR: something went wrong.")
        logger.info(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
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
    main("localhost", "01-smoker")
    