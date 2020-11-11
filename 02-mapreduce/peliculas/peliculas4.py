from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
	vec = line.split(",")
	pel = vec[1]
	rat = int(vec[2])
	yield pel, rat
 
    def reducer(self, key, values):
        lista = list(values)
	
	num = len(lista)
	rat = sum(lista)/num	
	
	dat = [num, rat]	
	yield key, dat

if __name__ == '__main__':
    MRWordFrequencyCount.run()
