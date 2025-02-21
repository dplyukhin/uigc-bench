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

def plot_cdf(data1, data2, data3, label1='CRGC', label2='WRC', label3='Manual'):
    x1, cdf1 = compute_cdf(data1)
    x2, cdf2 = compute_cdf(data2)
    x3, cdf3 = compute_cdf(data3)
    
    plt.figure(figsize=(8, 6))
    plt.plot(x1, cdf1, label=label1, marker='o', linestyle='-', alpha=0.7)
    plt.plot(x2, cdf2, label=label2, marker='s', linestyle='-', alpha=0.7)
    plt.plot(x3, cdf3, label=label3, marker='d', linestyle='-', alpha=0.7)
    
    plt.xlabel('Value')
    plt.ylabel('CDF')
    plt.title('Cumulative Distribution Function (CDF)')
    plt.legend()
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    file1_data = read_numbers_from_file("life-times-crgc-f100.csv")
    file2_data = read_numbers_from_file("life-times-mac-f100.csv")
    file3_data = read_numbers_from_file("life-times-manual-f100.csv")
    plot_cdf(file1_data, file2_data, file3_data)
