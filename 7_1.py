from mrjob.job import MRJob
import re

#regex to get the words
WORD_RE = re.compile(r"[\w']+")


class WordCount(MRJob):

    def mapper(self, _, line):
        #get each word from the line
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        #sum each word
        yield (sum(counts), word)


if __name__ == '__main__':
    WordCount.run()