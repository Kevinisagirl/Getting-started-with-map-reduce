from mrjob.job import MRJob
from mrjob.step import MRStep
import statistics


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
                vowelmapper = list(map(word.lower().count, "aeiou"))
                yield len(word), sum(vowelmapper)

    def reducer_count(self, key, values):
        yield key, statistics.mean(values)


if __name__ == '__main__':
    MRStartLetterWordCount.run()