from mrjob.job import MRJob
import csv

# Counts the number of each event type per state
class EventCountByStateAndType(MRJob):

    # Yields state and weather type
    def mapper(self, _, line):
        reader = csv.reader([line])
        for row in reader:
            if row[0] == 'EventId':
                return
            state = row[12]
            weather_type = row[1]
        
        yield (state, weather_type), 1

    # Sums values
    def reducer(self, key, counts):
        yield key, sum(counts)


if __name__ == '__main__':
    EventCountByStateAndType.run()
