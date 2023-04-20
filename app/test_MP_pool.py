from multiprocessing import Pool
from logger import timed

@timed
def worker(start_value, end_value):
    res = []
    for val in range(start_value, end_value):
        if val > 1:
            for i in range(2, val):
                if (val % i) == 0:
                    res.append(val)
                    break
    return res

@timed
def find_prime_no_betwn(start_value, end_value, chunk_count, process_count):
    chunk_size = round((end_value - start_value)/chunk_count)
    chunks = [(x, x+chunk_size) for x in range(start_value, end_value, chunk_size)]
    with Pool(processes=process_count) as pool:
        output_values = pool.starmap(worker, chunks)
    # print("prime_numbers:", output_values)

if __name__ == '__main__':
    find_prime_no_betwn(0, 100000, 10, 4)
