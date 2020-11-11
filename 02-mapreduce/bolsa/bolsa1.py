from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
	vec = line.split(",")
	
	nom = vec[0]        
	val = vec[1]

	yield nom, val

    def reducer(self, key, values):
        lista = list(values)

	mini = min(lista)
	maxi = max(lista)
	lis = [mini,maxi]
	yield key, lis

if __name__ == '__main__':
    MRWordFrequencyCount.run()
