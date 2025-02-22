import numpy as np
import matplotlib.pyplot as plt

def read_numbers_from_file(filename):
    with open(filename, 'r') as f:
        numbers = [float(line.strip()) for line in f if line.strip()]
    return numbers

def compute_cdf(data):
    sorted_data = np.sort(data)
    cdf = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
    return sorted_data, cdf

def plot_cdf(data1, data2, data3, label1='CRGC', label2='WRC', label3='No GC'):
    x1, cdf1 = compute_cdf(data1)
    x2, cdf2 = compute_cdf(data2)
    x3, cdf3 = compute_cdf(data3)
    
    plt.figure(figsize=(4, 2))
    step = 10000
    plt.plot(x1[::step], cdf1[::step], label=label1, marker='o', linestyle='-', alpha=0.7)
    plt.plot(x2[::step], cdf2[::step], label=label2, marker='s', linestyle='-', alpha=0.7)
    plt.plot(x3[::step], cdf3[::step], label=label3, marker='d', linestyle='-', alpha=0.7)

    plt.xlim([0, 1500])
    
    plt.xlabel('Actor life time (ms)')
    plt.legend()
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    file1_data = read_numbers_from_file("life-times-cyclic-crgc-f1000.csv")
    file2_data = read_numbers_from_file("life-times-cyclic-mac-f1000.csv")
    file3_data = read_numbers_from_file("life-times-cyclic-manual-f1000.csv")
    plot_cdf(file1_data, file2_data, file3_data)
    file1_data = read_numbers_from_file("life-times-acyclic-crgc-f1000.csv")
    file2_data = read_numbers_from_file("life-times-acyclic-mac-f1000.csv")
    file3_data = read_numbers_from_file("life-times-acyclic-manual-f1000.csv")
    plot_cdf(file1_data, file2_data, file3_data)
