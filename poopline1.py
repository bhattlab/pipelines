#!/usr/bin/env python

#a note to self: this is called with the following command:
#nohup python ~/pipelines/poopline1.py Pipe --sample-list-loc *samples.list --workers=$(wc -l *samples.list | tr ' ' '\t' | cut -f1) --scheduler-host scg3-ln01 1> pipeline.log 2> pipeline.err &


from subprocess import check_output
import subprocess
import luigi
import os
import random

from luigi_tasks import *

FASTQ_SUFFIX_1 = '_1.fq.gz'
FASTQ_SUFFIX_2 = '_2.fq.gz'
    
#================================================================================================================



#Assembly - directly on Koh/Kim's filtered reads.
class Assembly(luigi.Task):
	sample_prefix = luigi.Parameter()
	
	def requires(self):
		return [Verify_File(self.sample_prefix + FASTQ_SUFFIX_1), Verify_File(self.sample_prefix + FASTQ_SUFFIX_2)]
		
	def output(self):
		sample = self.sample_prefix.split("/")[-1]
		output = luigi.LocalTarget("{samp}/0.assembly/contigs.fasta".format(samp = sample))
		return output
		
	def run(self):
		print("Launching Spades assembler")
		sample = self.sample_prefix.split("/")[-1]
		assembly_dir_val = "%s/0.assembly" % sample
		mkdir(assembly_dir_val)
		cmd = "/srv/gs1/software/spades/SPAdes-3.1.1-Linux/bin/spades.py -o {assembly_dir} -1 {sample_pref}{suffix1} -2 {sample_pref}{suffix2} &> {assembly_dir}/spades_output.log".format(assembly_dir = assembly_dir_val, sample_pref = self.sample_prefix, suffix1 = FASTQ_SUFFIX_1, suffix2 = FASTQ_SUFFIX_2)

		run_cmd(cmd)




#Align reads to assembled Contigs
class Snap_Index_Contigs(luigi.Task):
	sample_prefix = luigi.Parameter()
	
	def run(self):
		sample = self.sample_prefix.split('/')[-1]
		mkdir("{sampletmp}/1.alignment".format(sampletmp = sample))
		cmd = "snap index {sampletmp}/0.assembly/contigs.fasta {sampletmp}/1.alignment/contig_snap_index".format(sampletmp = sample)
		print cmd
		print run_cmd(cmd)
		
	def output(self):
		sample = self.sample_prefix.split('/')[-1]
		return luigi.LocalTarget("{sampletmp}/1.alignment/contig_snap_index/Genome".format(sampletmp = sample))
	
	def requires(self):
		return Assembly(self.sample_prefix)
		
		
		
		
class Align_To_Contig(luigi.Task):
	sample_prefix = luigi.Parameter()
	
	def requires(self):
		sample = self.sample_prefix.split('/')[-1]
		return Snap_Index_Contigs(self.sample_prefix)
		
		
	def output(self):
		sample = self.sample_prefix.split('/')[-1]
		working_dir = '1.alignment'
		return luigi.LocalTarget("%s/%s/aligned_to_contigs.bam" % (sample, working_dir)) 
		
		
	def run(self):
		sample = self.sample_prefix.split('/')[-1]
		working_dir = '1.alignment'
		path = "%s/%s" % (sample, working_dir)
		mkdir(path)		
		cmd = "snap paired {idx} {reads1} {reads2} -o {output_dir}/aligned_to_contigs.bam".format(idx="%s/contig_snap_index" % path, reads1 = self.sample_prefix + FASTQ_SUFFIX_1, reads2 = self.sample_prefix + FASTQ_SUFFIX_2, output_dir = path)
		
		
		run_cmd(cmd)

class Align_To_NCBI(luigi.Task):
	sample_prefix_list = luigi.Parameter()
	
	def requires(self):
		return [Align_To_Contig(sample_prefix) for sample_prefix in self.sample_prefix_list]
	def output(self):
		sample_list = [s.split('/')[-1] for s in self.sample_prefix_list]
		working_dir = '1.alignment'
		return [luigi.LocalTarget("%s/%s/aligned_to_ncbi.bam" % (sample, working_dir)) for sample in sample_list]
	def run(self):
		cmd = "snap"
		first = True
		for sample_prefix in self.sample_prefix_list:
			sample = sample_prefix.split("/")[-1]
			working_dir = '1.alignment'
			path = "%s/%s" % (sample, working_dir)
			mkdir(path)
		
			if not first:
				cmd += ' , '
				
			cmd += " paired ~/scratch/snap_indices/master_index %s %s -o %s/aligned_to_ncbi.bam" % (sample_prefix + FASTQ_SUFFIX_1, sample_prefix + FASTQ_SUFFIX_2, path)

			first = False
		
		
		run_cmd(cmd)

class Index_Sort_Bams(luigi.Task):
	sample_prefix_list = luigi.Parameter()
	
	def requires(self):
		samples = [sample_prefix.split('/')[-1] for sample_prefix in self.sample_prefix_list]
		return [Index_Contig_Aligned_Bam(sample, self.sample_prefix_list) for sample in samples] + [Sort_NCBI_Aligned_Bam(sample, self.sample_prefix_list) for sample in samples]

#Index Bam
class Index_Contig_Aligned_Bam(luigi.Task):
	sample = luigi.Parameter()
	sample_prefix_list = luigi.Parameter()
	
	def run(self):
		cmd = "samtools index %s/1.alignment/aligned_to_contigs_sorted.bam %s/1.alignment/aligned_to_contigs_sorted.bai" % (self.sample, self.sample)
		run_cmd(cmd)
	def output(self):
		return luigi.LocalTarget("%s/1.alignment/aligned_to_contigs_sorted.bai" % self.sample)
	def requires(self):
		return Sort_Contig_Aligned_Bam(self.sample, self.sample_prefix_list)

class Sort_Contig_Aligned_Bam(luigi.Task):
	sample = luigi.Parameter()
	sample_prefix_list = luigi.Parameter()
	
	def run(self):
		cmd = "samtools sort %s/1.alignment/aligned_to_contigs.bam %s/1.alignment/aligned_to_contigs_sorted" % (self.sample, self.sample)
		run_cmd(cmd)
	def output(self):
		return luigi.LocalTarget("%s/1.alignment/aligned_to_contigs_sorted.bam" % self.sample)
	def requires(self):
		return Align_To_NCBI(self.sample_prefix_list)

class Sort_NCBI_Aligned_Bam(luigi.Task):
	sample = luigi.Parameter()
	sample_prefix_list = luigi.Parameter()
	
	def run(self):
		cmd = "samtools sort -n %s/1.alignment/aligned_to_ncbi.bam %s/1.alignment/aligned_to_ncbi_sorted" % (self.sample, self.sample)
		run_cmd(cmd)
	def output(self):
		return luigi.LocalTarget("%s/1.alignment/aligned_to_ncbi_sorted.bam" % self.sample)
	def requires(self):
		return Align_To_NCBI(self.sample_prefix_list)
		
class Distruct(luigi.Task):
	sample_prefix_list = luigi.Parameter()
	
	def run(self):
		pass
	def output(self):
		pass
	def requires(self):
		return Humann(self.sample_prefix_list)
	
class Humann(luigi.Task):
	sample_prefix_list = luigi.Parameter()
	
	def run(self):
		pass
	def output(self):
		pass
	def requires(self):
		return [Blastx(sample_prefix) for sample_prefix in self.sample_prefix_list]

class Blastx(luigi.Task):
	sample_prefix = luigi.Parameter()
	
	def run(self):
		sample = self.sample_prefix.split('/')[-1]
		working_dir = '2.blast'
		mkdir("%s/%s" % (sample, working_dir))
		cmd = "zcat %s %s | fq2fa | blastx -db /home/elimoss/bhattlab/data/KEGG/blast/all_kegg_organisms_proteins -outfmt 6 > %s/%s/blastx_results_kegg_all_orgs.bls 2>%s/%s/blastx.err" % (self.sample_prefix + FASTQ_SUFFIX_1, self.sample_prefix + FASTQ_SUFFIX_2, sample, working_dir, sample, working_dir)
		run_cmd(cmd)
	def output(self):
		sample = self.sample_prefix.split('/')[-1]
		working_dir = '2.blast'
		return luigi.LocalTarget("%s/%s/blastx_results_kegg_all_orgs.bls" % (sample, working_dir))
	def requires(self):
		return [Verify_File(self.sample_prefix + FASTQ_SUFFIX_1), Verify_File(self.sample_prefix + FASTQ_SUFFIX_2)]
		
#Run the Pipe.
#================================================================================================================
class Pipe(luigi.Task):
	sample_list_loc = luigi.Parameter()
	global samples
	def run(self):
		print "Starting run..."
		return None

	def requires(self):
		sample_prefix_list = open(self.sample_list_loc).read().rstrip().split("\n")
		return [Index_Sort_Bams(sample_prefix_list), Distruct(sample_prefix_list)]

	def output(self):
		return None
	
		
if __name__=='__main__':
    luigi.run()