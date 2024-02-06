from tqdm import tqdm
from time import sleep

for x in tqdm(range(3)):
    y = 1
    y += x
    sleep(1)
    y = 1
    y += x
