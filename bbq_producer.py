"""
    This is the producer. This program is to simulate pulling "streaming" data from a smart smoker. 
    We are reading the smoker temperature, temperature of food A and temperature of food B. 
    The initial code is copied from my last assignment found on my GitHub profile.

    Author: Luci McDaniel
    Date: May 31, 2024

"""

import pika
import sys
import webbrowser
import csv
import time

from util_logger import setup_logger
logger, logname = setup_logger(__file__)
        
#Calling a function to ask the user if they want to see the Admin Webpage of RabbitMQ.
def offer_rabbitmq_admin_site(show_offer=True):
    """Offer to open the RabbitMQ Admin website"""
    if show_offer:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()
#queue_name = "01-smoker", "02-food-A", "03-food-B"
def send_temps(host: str, queue_name: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        
        ch.queue_declare(queue="01-smoker", durable=True)
        ch.queue_declare(queue="02-food-A", durable=True)
        ch.queue_declare(queue="03-food-B", durable=True)

        with open('smoker-temps.csv', 'r') as file:
            reader = csv.reader(file, delimiter=",")
            header = next(reader) #skipping the header row
            for row in reader:
                Time,Channel1,Channel2,Channel3 = row
                time.sleep(3)
                if Channel1: #ignoring null values
                    smoker_temp = ','.join([Time, Channel1])
                    ch.basic_publish(exchange="", routing_key="01-smoker", body=smoker_temp)
                    logger.info(f" [x] Smoker Temperature is {smoker_temp}")
                
                if Channel2:
                    food_A = ','.join([Time, Channel2])
                    ch.basic_publish(exchange="", routing_key="02-food-A", body=food_A)
                    logger.info(f" [x] Food A Temperature is {food_A}")

                if Channel3:
                    food_B = ','.join([Time, Channel3])
                    ch.basic_publish(exchange="", routing_key="03-food-B", body=food_B)
                    logger.info(f" [x] Food B Temperature is {food_B}")

    except pika.exceptions.AMQPConnectionError as e:
        logger.info(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
       

        # close the connection to the server
        conn.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site(show_offer=True) #when show_offer=False the user will not be asked to open RabbitMQ Admin site.

    # send the message to the queue
    send_temps("localhost","01-smoker")
    send_temps("localhost","02-food-A")
    send_temps("localhost","03-food-B")
   