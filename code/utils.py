import secrets
from typing import Union, Optional, Tuple
import os
from threading import Lock
import json
from concurrent.futures import ThreadPoolExecutor

USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.3", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.1", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.3", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.1", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.1", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.1"]

def random_user_agent() -> str:
    "Generates a random user agent to bypass Python blockades"

    return secrets.choice(USER_AGENTS)

file_locks = dict()

def load(file_name: str, default: Union[dict, list] = dict()) -> Union[dict, list]:
    """
    Function to load a JSON file securely.

    :param file_name: The JSON file you want to load
    :param default: Returned if no data was found
    """

    if not os.path.isfile(file_name):
        if isinstance(default, list):
            return list()
        return dict()
    
    if file_name not in file_locks:
        file_locks[file_name] = Lock()

    with file_locks[file_name]:
        with open(file_name, "r", encoding = "utf-8") as file:
            data = json.load(file)
        return data 

def find_missing_numbers_in_range(range_start: int, range_end: int, data: list):
    """
    Finds missing numbers within a given range excluding the ones provided in the data.

    :param range_start: The start value of the range.
    :param range_end: The end value of the range.
    :param data: A list containing tuples of numbers and their associated data.
    """

    numbers = list(range(range_start + 1, range_end + 1))
    
    for item in data:
        if item[0] in numbers:
            numbers.remove(item[0])
    
    return numbers

class Block:
    "Functions for saving data in blocks instead of alone"

    def __init__(self, block_size: int, file_name: str) -> "Block":
        """
        :param block_size: How big each block is
        :param file_name: The name of the file to write the block to.
        """

        if block_size < 0: block_size = 4000
        self.block_size = block_size
        self.file_name = file_name

        self.executor = ThreadPoolExecutor(max_workers=5)

        self.blocks = {}
    
    def _get_id(self, index: int) -> int:
        """
        Returns the nearest block index based on the given index and block size.

        :param index: The index value.
        """

        remains = index % self.block_size
        
        if remains == 0: return index
        return index + (self.block_size - remains)
    
    def _write_data(self, block_data: tuple) -> None:
        """
        Writes data to a file while ensuring thread safety using locks.

        :param block_data: A tuple containing data to be written to the file.
        """

        if self.file_name not in file_locks:
            file_locks[self.file_name] = Lock()

        with file_locks[self.file_name]:
            if os.path.isfile(self.file_name):
                with open(self.file_name, "r", encoding="utf-8") as file:
                    data = json.load(file)
            else:
                data = []

            for _, new_data in block_data:
                if new_data is not None:
                    data.append(new_data)

            with open(self.file_name, "w", encoding="utf-8") as file:
                json.dump(data, file)
    
    def add_data(self, index: int, new_data: Optional[dict] = None) -> Tuple[bool, Optional[int]]:
        """
        Adds new data to the specified index in the data structure, and writes the block to file
        if all expected data within the block range is present.

        :param index: The index where the new data should be added.
        :param new_data: The data to be added, if any.
        """

        block_id = self._get_id(index)

        block = self.blocks.get(block_id, [])
        block.append((index, new_data))
        self.blocks[block_id] = block

        missing = find_missing_numbers_in_range(block_id - self.block_size, block_id, block)
        if 1 in missing: missing.remove(1)

        if len(missing) == 0:
            self.executor.submit(self._write_data, block)
            
            del self.blocks[block_id]

            return True, block_id
        return False, block_id