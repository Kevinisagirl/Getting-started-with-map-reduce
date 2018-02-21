from mrjob.job import MRJob
from mrjob.step import MRStep


class MRStartLetterWordCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_find),
            MRStep(reducer=self.reducer_count)
        ]

    def mapper(self, _, line):
        yield "", line.split()

    def reducer_find(self, key, values):
        for wordlst in values:
            for word in wordlst:
                yield word[0].upper(), 1

    def reducer_count(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRStartLetterWordCount.run()