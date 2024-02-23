import subprocess
import matplotlib.pyplot as plt

# Run the performance test with different chunk sizes
chunk_size = []
query_time = []
for i in range(1000, 100000, 1000):
    process = subprocess.Popen(['python', 'src/performance_test.py', f'--chunk_size={i}'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    # Define the input values and their order
    input_values = ['U2120349E', '2', 'q']

    # Loop through the input values and send them to the subprocess
    for value in input_values:
        process.stdin.write(value + '\n')
        process.stdin.flush()

    # Wait for the process to finish and get the output
    output, error = process.communicate()

    print(output.split('\n')[-4])

    # Save the output to a list
    query_time.append(float(output.split('\n')[-5][12:-2]))

    # Save the chunk size to a list
    chunk_size.append(i)


# Plot the results
plt.plot(chunk_size, query_time)
plt.xlabel('Chunk Size')
plt.ylabel('Query Time (s)')

plt.show()