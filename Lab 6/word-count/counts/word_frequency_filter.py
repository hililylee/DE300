# Open the file and read its contents
with open('part-00000', 'r') as file1:
    lines1 = file1.readlines()
with open('part-00001', 'r') as file2:
    lines2 = file2.readlines()

# Initialize an empty dictionary to store word frequencies
word_frequencies = {}

# Function to process lines and update word frequencies
def process_lines(lines):
    for line in lines:
        # Strip parentheses and split the line into word and frequency
        word, frequency_str = line.strip().strip('()').split()
        
        # Convert frequency to an integer
        frequency = int(frequency_str)
        
        # Update word frequencies in the dictionary
        word_frequencies[word] = frequency

# Process lines from the first file
process_lines(lines1)

# Process lines from the second file
process_lines(lines2)

# Filter out words with frequency greater than or equal to three
filtered_words = [word for word, frequency in word_frequencies.items() if frequency >= 3]

# Write filtered words to a new file
with open('filtered_words.txt', 'w') as output_file:
    for word in filtered_words:
        output_file.write(word + '\n')
