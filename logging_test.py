import logging
import time

def main():
    logging.basicConfig(
        format='%(asctime)s %(message)s',
        datefmt='%Y-%m-%d %I:%M:%S %p',
        filename='example.log',
        encoding='utf-8',
        level=logging.DEBUG)
    
    START_TIME = time.time()
    logging.info('Starting')
    time.sleep(5)
    TEMP_TIME = time.time()
    part_time = time.strftime("%H:%M:%S", time.gmtime(TEMP_TIME-START_TIME))
    message = "first cycle took: "+part_time
    logging.info(message)

    time.sleep(5)
    TEMP_TIME2 = time.time()
    part_time = time.strftime("%H:%M:%S", time.gmtime(TEMP_TIME2-TEMP_TIME))
    message = "second cycle took: "+part_time
    logging.info(message)

    END_TIME = time.time()
    total_time = time.strftime("%H:%M:%S", time.gmtime(END_TIME-START_TIME))

    logging.info('Ending')
    message = 'Total Time: '+total_time
    logging.info(message)

main()
