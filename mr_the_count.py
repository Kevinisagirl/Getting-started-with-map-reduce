from mrjob.job import MRJob


class MRTheFrequencyCount(MRJob):

    def mapper(self, _, line):
        for word in line.split():
            if word.lower() == "the":
                yield "the", 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRTheFrequencyCount.run()