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
kraken_dir = samp_name + '/kraken'
post_dir = samp_name + "/post_process"
stats_dir = samp_name #+ '/gaemr'
[trim_dir, assembly_dir, kraken_dir, post_dir, stats_dir].each{|d| directory d}

#OUTPUT FILENAMES
trim_out = trim_dir + '/trimmed_paired_1.fq.gz'
assembly_out = assembly_dir + '/contigs.fasta'
kraken_out = kraken_dir + '/output.krak'
post_out = post_dir + '/post-processed_output.krak'
stats_out = stats_dir + '/blar' #change this.

#PIPELINE COMMANDS


#TRIM 
file trim_out => [trim_dir] do
	reads1 = full_sample+'_1.fq.gz'
	reads2 = full_sample+'_2.fq.gz'

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

#CLASSIFY - kraken
file kraken_out => [assembly_out, kraken_dir] do
	f = File.open(log, 'a')
	f.write(`kraken --db ~/scratch/kraken_databases/kraken_drafts_human/ --fasta-input #{assembly_dir}/contigs.fasta  --preload --unclassified-out #{kraken_dir}/unclass.fasta --classified-out #{kraken_dir}/class.fasta --output #{kraken_dir}/output.krak 2>&1`)
	f.close
end

#POST-PROCESS
file post_out => [kraken_out, post_dir] do
	sh "cd #{post_dir}; kraken_post_processor ../../#{kraken_out}; cd -"
end

file stats_out => [post_out, stats_dir] do
	sh "cd #{stats_dir}; GAEMR.py -c ../#{assembly_out}; cd -"
end

task :run_bin do

end





#start everything
task :default  => stats_out do 
	puts "Done"
end