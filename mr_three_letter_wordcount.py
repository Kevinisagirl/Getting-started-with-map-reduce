from mrjob.job import MRJob
from mrjob.step import MRStep


class MRThreeLetterWordCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_find_the),
            MRStep(reducer=self.reducer_count_the)
        ]

    def mapper(self, _, line):
        yield "", line.split()

    def reducer_find_the(self, key, values):
        for wordlst in values:
            for word in wordlst:
                if len(word) == 3:
                    yield "three letter words", 1

    def reducer_count_the(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRThreeLetterWordCount.run()