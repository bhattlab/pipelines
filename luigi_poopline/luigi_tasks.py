import luigi
import os
import subprocess

#Utility.  Runnable as is.
#====================================================================================	
def mkdir(directory):
	if not os.path.exists(directory):
		os.makedirs(directory)
		
#Find files on FS and verify presence.
class Verify_File(luigi.Task):
	file = luigi.Parameter()
	def output(self):
		return luigi.LocalTarget(self.file)

def run_cmd(cmd):
	print(cmd)
	p = subprocess.Popen(cmd, shell=True, universal_newlines=True)
	output = p.communicate()
	return output

#For my own reference.
#====================================================================================	
class Run_Generic_Process(luigi.Task):
	param = luigi.Parameter() #it's a thing that's needed.
	
	def run(self):
		#do a thing.
		pass
	def requires(self):
		#previous jobs.
		pass
	def output(self):
		#define a luigi.LocalTarget("name_of_file.ext")
		pass

#Bioinformatic Processing Steps Warehousing.  Not runnable as is.
#====================================================================================	

#QC
class QC(luigi.Task):
	sample_prefix = luigi.Parameter()
	
	global sample
	
	def run(self):
		print(self.sample_prefix)
		print("Running Quality Control.  Launching Trimmomatic.")
		reads1 = self.sample_prefix+'_1.fq.gz'
		reads2 = self.sample_prefix+'_2.fq.gz'
		sample = self.sample_prefix.split('/')[-1]
		trim_dir = "{sample_ph}/qc".format(sample_ph = sample)
		mkdir(trim_dir)
		cmd = "trimmomatic-0.32 PE -threads 5 -phred64 -trimlog {trim_dir_ph}/trim.log {reads1_ph} {reads2_ph} {trim_dir_ph}/trimmed_paired_1.fq.gz {trim_dir_ph}/trimmed_unpaired_1.fq.gz {trim_dir_ph}/trimmed_paired_2.fq.gz {trim_dir_ph}/trimmed_unpaired_2.fq.gz ILLUMINACLIP:/home/elimoss/moss/tools/trimmomatic-0.32/adapters/TruSeq3-PE.fa:2:30:10 LEADING:3 TRAILING:3 SLIDINGWINDOW:4:15 MINLEN:50 2>&1".format(trim_dir_ph = trim_dir, reads1_ph = reads1, reads2_ph = reads2)
		output = subprocess.check_output(cmd, shell = True)
		print output
		return output
		
	def requires(self):
		return None
		
	def output(self):
		if '/' in self.sample_prefix:
			sample = self.sample_prefix.split('/')[-1]
		else:
			sample = self.sample_prefix
		
		trim_dir = "{sample_ph}/qc".format(sample_ph = sample)
		target1 = "{trim_dir_ph}/trimmed_paired_1.fq.gz".format(trim_dir_ph = trim_dir)
		target2 = "{trim_dir_ph}/trimmed_paired_2.fq.gz".format(trim_dir_ph = trim_dir)
		return [luigi.LocalTarget(target1), luigi.LocalTarget(target2)]
