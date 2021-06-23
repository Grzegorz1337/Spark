#Zadanie 3.3
import os
if __name__ == "__main__":
    os.system("cat data_small.txt | python3 mapper.py | sort | python3 reducer.py")