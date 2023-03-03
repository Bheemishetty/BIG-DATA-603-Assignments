# import the MRJob library
from mrjob.job import MRJob

# define a new MRJob class for counting words
class WordCount(MRJob):

    # define a mapper function
    def mapper(self, _, line):
        # split the line into words and emit each word as a key with value 1
        for word in line.strip().split():
            # yield each word as a key with value 1
            # use lower() to convert the word to lowercase
            yield word.lower(), 1

    # define a reducer function
    def reducer(self, word, counts):
        # sum the counts for each word and emit the result
        # the word argument is the key, and counts is an iterator over the values 
        yield word, sum(counts)

#code runs the MRJob class 
if __name__ == '__main__':
    WordCount.run()
