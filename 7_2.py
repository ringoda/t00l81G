from mrjob.job import MRJob
from mrjob.step import MRStep


class EulerTour(MRJob):

    def mapper(self, _, line):
        #split the line up into the nodes and yield it and the count 1
        nodes = line.split()
        for node in nodes:
            yield node, 1

    def reducer(self, key, values):
        #count the values
        yield None, sum(values)

    def reducer_euler(self, _, edges):
        yield "Is there an Euler Tour present?", self.even_number(edges)

    def steps(self):
        #define the steps
        return [MRStep(mapper=self.mapper, reducer=self.reducer), 
                MRStep(reducer=self.reducer_euler)]

    #helper function to check if all numbers are even
    def even_number(self, num):
        #loop and check each number return False if one fails but true if none fails
        for node in num:
            if node % 2 != 0:
                return False
        return True

if __name__ == '__main__':
    EulerTour.run()