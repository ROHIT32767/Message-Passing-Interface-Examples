import matplotlib.pyplot as plt

# Data from the provided output
execution_times = [
    0.014792, 0.019684, 0.024145, 0.029454, 0.069469, 0.094572, 
    0.24203, 0.562576, 0.531891, 0.77823, 0.88077, 1.0855
]
# multiply execution times by 1000 to convert to milliseconds
num_processes = list(range(1, 13)) 

# Create the plot
plt.figure(figsize=(8, 6))
plt.plot(num_processes, execution_times, marker='o', linestyle='-')
plt.xlabel('Number of Processes')
plt.ylabel('Execution Time (seconds)')
plt.title('Execution Time vs. Number of Processes')
plt.grid(True)

# Show the plot
plt.show()


plt.savefig('execution_time_plot.png')