from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
	vec = line.split(",")
	dia = vec[4]
	rat = int(vec[2])
	yield dia, rat
 
    def reducer(self, key, values):
        lista = list(values)
	
	
	rat = sum(lista)/len(lista)	
	
	yield key, rat

if __name__ == '__main__':
    MRWordFrequencyCount.run()
