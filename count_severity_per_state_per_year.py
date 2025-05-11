from mrjob.job import MRJob
import csv

# Counts the number of severe events per state per year
class SeverityCountPerStatePerYear(MRJob):

    # Yields state if severe
    def mapper(self, _, line):
        reader = csv.reader([line])
        for row in reader:
            if row[0] == 'EventId':
                return

            severity = row[2]
            if severity != 'Severe':
                return

            state = row[12]
            try:
                # Extract year from StartTime(UTC)
                date_parts = row[3].split('/')
                year = date_parts[2][:4]
            except (IndexError, ValueError): # Skip this line if date is malformed
                return  
        
        yield (state, year), 1

    # Sums values
    def reducer(self, key, counts):
        yield key, sum(counts)

if __name__ == '__main__':
    SeverityCountPerStatePerYear.run()
