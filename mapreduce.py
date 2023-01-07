from mrjob.job import MRJob
from mrjob.step import MRStep

class FashionDataBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_data,
                   reducer=self.reducer_count_data)
        ]

    def mapper_get_data(self, _, line):
        (brand_id,p_id,price,colour,brand_name,ratingCount) = line.split('\t')
        yield rating, 1

    def reducer_count_data(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    FashionDataBreakdown.run()
