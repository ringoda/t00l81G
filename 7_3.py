from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")


class LongestWordAndNumberOfTimes(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_length),
            MRStep(reducer=self.reducer_find_longest)
        ]

    def mapper_get_words(self, _, line):
        #get each word
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        #count each word in line
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        #count words
        yield word, (sum(counts), word)

    def reducer_find_length(self, word, word_count_pairs):
        #get length of words and number of words
        yield None, (len(word), max(word_count_pairs))

    def reducer_find_longest(self, _, num_of_word):
        #find the longest word
        longest = max(num_of_word)

        result = "Longest word is '" + longest[1][1] + "' it is " + str(longest[0]) + " characters long and appears " + str(longest[1][0]) + " times."

        yield longest[1][1], result

if __name__ == '__main__':
    LongestWordAndNumberOfTimes.run()