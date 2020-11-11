from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
	vec = line.split(",")
	dia = vec[4]

	yield dia, 1
 
    def reducer(self, key, values):
        lista = list(values)
	
	tot = sum(lista)
		
	yield key, tot
	

if __name__ == '__main__':
    MRWordFrequencyCount.run()
