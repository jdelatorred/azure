from azure.iot.device import IoTHubDeviceClient, Message, MethodRequest, MethodResponse #Azure IoT Libraries
import threading as th
import time
import json
import random
from datetime import datetime

#Function to send events to IoT Hub service
def SendPPM(client):
    customid = 0
    eventId = 0

    while (eventId < 500):
        try:
            now = datetime.now()
            current_date = now.strftime("%Y-%m-%d %H:%M:%S")

            if customid == 20: # up to 20 customers
                customid = 0

            eventId += 1

            #Adding normal and anomalies values
            if eventId % 10 == 0:
                ppm=random.randint(190,280) # Upper no normal range
            else:
                ppm=random.randint(60,160) #normal range

            customid += 1 

            customer="Customer "+str(customid)

            msg_json = '{{"customer": "{customer}","ppm": {ppm},"current_date": "{current_date}"}}'

            mgs_txt_formatted = msg_json.format(customer=customer,ppm=ppm,current_date=current_date)

            message = Message(mgs_txt_formatted)

            print("Message Sent: {}".format(message))
            client.send_message(message)
            print("Message Sent Sucessfully")
            print("EventId:  {}".format(eventId))
            time.sleep(1)
            client.disconnect()
        except Exception as e:
            print(e)
            print("Client Iot Hub Error")
            break

if __name__ == "__main__":

    start_time = time.perf_counter()

    connection_string = "IoT Hub Device Connection String"

    client = IoTHubDeviceClient.create_from_connection_string(connection_string)

    SendPPMThread = th.Thread(target=SendPPM, args=(client,))

    SendPPMThread.start()

    SendPPMThread.join()

    end_time = time.perf_counter()

    elapsed_time = end_time - start_time

    print("Elapsed time: ", elapsed_time)
