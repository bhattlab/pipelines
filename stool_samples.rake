#!/usr/bin/env ruby

def samp_name
	abort("Usage: rake -f stool_samples.rake SAMPLE=<sample_prefix>") if ENV['SAMPLE'] == nil	
	ENV["SAMPLE"].split('/').last
end

log = samp_name + '.log'
full_sample = ENV["SAMPLE"]

#FOLDERS
trim_dir = samp_name + '/trim'
assembly_dir = samp_name + '/assembly'
snap_dir = samp_name + '/snap'
post_dir = samp_name + "/post_process"
stats_dir = samp_name # + '/gaemr'

[trim_dir, assembly_dir, snap_dir, post_dir, stats_dir].each{|d| directory d}

#OUTPUT FILENAMES
trim_out = trim_dir + '/trimmed_paired_1.fq.gz'
assembly_out = assembly_dir + '/contigs.fasta'
snap_out = snap_dir + '/aligned.bam'
#post_out = post_dir + '/post-processed_output.krak'
stats_out = stats_dir + '/gaemr/chart/assembly.cumulative_sizes.png' #change this.
contig_snap_index = snap_dir + '/contig_snap_index/Genome'



#PIPELINE COMMANDS

#TRIM 
file trim_out => [trim_dir] do
	reads1 = full_sample+'_1.fastq.gz'
	reads2 = full_sample+'_2.fastq.gz'

	f = File.open(log, 'a')
	f.write(`trimmomatic-0.32 PE -threads 5 -phred64 -trimlog #{trim_dir}/trim.log #{reads1} #{reads2} #{trim_dir}/trimmed_paired_1.fq.gz #{trim_dir}/trimmed_unpaired_1.fq.gz #{trim_dir}/trimmed_paired_2.fq.gz #{trim_dir}/trimmed_unpaired_2.fq.gz ILLUMINACLIP:/home/elimoss/moss/tools/trimmomatic-0.32/adapters/TruSeq3-PE.fa:2:30:10 LEADING:3 TRAILING:3 SLIDINGWINDOW:4:15 MINLEN:50 2>&1`)
	f.close
end

#ASSEMBLE 
file assembly_out => [trim_out, assembly_dir] do
	f = File.open(log, 'a')
	f.write(`/srv/gs1/software/spades/SPAdes-3.1.1-Linux/bin/spades.py -o #{assembly_dir} -1 #{trim_dir}/trimmed_paired_1.fq.gz -2 #{trim_dir}/trimmed_paired_2.fq.gz 2>&1`)
	f.close
end

#CLASSIFY, ALIGN TO CONTIGS - snap
file snap_out => [assembly_out, snap_dir] do

	f.write("snap paired ~/scratch/snap_indices #{trim_dir}/trimmed_paired_1.fq.gz #{trim_dir}/trimmed_paired_2.fq.gz -o #{snap_dir}/aligned.bam") #command only, because snap can batch these in a helpful way (do that by hand)
	f.close
end

file contig_snap_index => [assembly_out, snap_dir] do 
	f = File.open(log, 'a')
	f.write(`snap index #{assembly_out} assembled_contigs_snap_idx`) 
	f.close
end




#start everything
task :default  => [snap_out, contig_snap_index] do 
	puts "Done"
end