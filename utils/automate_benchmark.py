import subprocess
import sys
from datetime import datetime
import socket

sys.stdout = open('stdout_automate_benchmark.txt', 'a')
sys.stderr = open('stderr_automate_benchmark.txt', 'a')

hostname = socket.gethostbyname(socket.gethostname())

command_10_vep_samples = "bash es_scripts/delete_index_ten_samples_vep.sh; bash es_scripts/create_index_ten_samples_vep_and_put_mapping_ten_samples_vep.sh; python import_vcf_using_celery.py --hostname %s --port 9200 --index ten_samples_vep --type ten_samples_vep --label None --update False --vcf ~/vep_annotation/%s --mapping es_scripts/inspect_output_for_ten_samples_vep_ten_samples_vep.txt;"
command_100_vep_samples = "bash es_scripts/delete_index_hundred_samples_vep.sh; bash es_scripts/create_index_hundred_samples_vep_and_put_mapping_hundred_samples_vep.sh; python import_vcf_using_celery.py --hostname %s --port 9200 --index hundred_samples_vep --type hundred_samples_vep --label None --update False --vcf ~/vep_annotation/%s --mapping es_scripts/inspect_output_for_hundred_samples_vep_hundred_samples_vep.txt;"
command_500_vep_samples = "bash es_scripts/delete_index_five_hundred_samples_vep.sh; bash es_scripts/create_index_five_hundred_samples_vep_and_put_mapping_five_hundred_samples_vep.sh; python import_vcf_using_celery.py --hostname %s --port 9200 --index five_hundred_samples_vep --type five_hundred_samples_vep --label None --update False --vcf ~/vep_annotation/%s --mapping es_scripts/inspect_output_for_five_hundred_samples_vep_five_hundred_samples_vep.txt;"
command_1000_vep_samples = "bash es_scripts/delete_index_thousand_samples_vep.sh; bash es_scripts/create_index_thousand_samples_vep_and_put_mapping_thousand_samples_vep.sh; python import_vcf_using_celery.py --hostname %s --port 9200 --index thousand_samples_vep --type thousand_samples_vep --label None --update False --vcf ~/vep_annotation/%s --mapping es_scripts/inspect_output_for_thousand_samples_vep_thousand_samples_vep.txt;"



command_10_annovar_samples = "bash es_scripts/delete_index_ten_samples_annovar.sh; bash es_scripts/create_index_ten_samples_annovar_and_put_mapping_ten_samples_annovar.sh; python import_vcf_using_celery.py --hostname %s --port 9200 --index ten_samples_annovar --type ten_samples_annovar --label None --update False --vcf ~/annovar_annotation/%s --mapping es_scripts/inspect_output_for_ten_samples_annovar_ten_samples_annovar.txt;"
command_100_annovar_samples = "bash es_scripts/delete_index_hundred_samples_annovar.sh; bash es_scripts/create_index_hundred_samples_annovar_and_put_mapping_hundred_samples_annovar.sh; python import_vcf_using_celery.py --hostname %s --port 9200 --index hundred_samples_annovar --type hundred_samples_annovar --label None --update False --vcf ~/annovar_annotation/%s --mapping es_scripts/inspect_output_for_hundred_samples_annovar_hundred_samples_annovar.txt;"
command_500_annovar_samples = "bash es_scripts/delete_index_five_hundred_samples_annovar.sh; bash es_scripts/create_index_five_hundred_samples_annovar_and_put_mapping_five_hundred_samples_annovar.sh; python import_vcf_using_celery.py --hostname %s --port 9200 --index five_hundred_samples_annovar --type five_hundred_samples_annovar --label None --update False --vcf ~/annovar_annotation/%s --mapping es_scripts/inspect_output_for_five_hundred_samples_annovar_five_hundred_samples_annovar.txt;"
command_1000_annovar_samples = "bash es_scripts/delete_index_thousand_samples_annovar.sh; bash es_scripts/create_index_thousand_samples_annovar_and_put_mapping_thousand_samples_annovar.sh; python import_vcf_using_celery.py --hostname %s --port 9200 --index thousand_samples_annovar --type thousand_samples_annovar --label None --update False --vcf ~/annovar_annotation/%s --mapping es_scripts/inspect_output_for_thousand_samples_annovar_thousand_samples_annovar.txt;"

files = [
    (command_1000_annovar_samples, 'ALL.1kg.annovar.1000sample_5000000.vcf'),
    (command_1000_annovar_samples, 'ALL.1kg.annovar.1000sample_1000000.vcf'),
    (command_1000_annovar_samples, 'ALL.1kg.annovar.1000sample_500000.vcf'),
    (command_1000_annovar_samples, 'ALL.1kg.annovar.1000sample_100000.vcf'),
    (command_1000_annovar_samples, 'ALL.1kg.annovar.1000sample_50000.vcf'),
    (command_1000_annovar_samples, 'ALL.1kg.annovar.1000sample_10000.vcf'),

    (command_500_annovar_samples, 'ALL.1kg.annovar.500sample_5000000.vcf'),
    (command_500_annovar_samples, 'ALL.1kg.annovar.500sample_1000000.vcf'),
    (command_500_annovar_samples, 'ALL.1kg.annovar.500sample_500000.vcf'),
    (command_500_annovar_samples, 'ALL.1kg.annovar.500sample_100000.vcf'),
    (command_500_annovar_samples, 'ALL.1kg.annovar.500sample_50000.vcf'),
    (command_500_annovar_samples, 'ALL.1kg.annovar.500sample_10000.vcf'),

    (command_100_annovar_samples, 'ALL.1kg.annovar.100sample_5000000.vcf'),
    (command_100_annovar_samples, 'ALL.1kg.annovar.100sample_1000000.vcf'),
    (command_100_annovar_samples, 'ALL.1kg.annovar.100sample_500000.vcf'),
    (command_100_annovar_samples, 'ALL.1kg.annovar.100sample_100000.vcf'),
    (command_100_annovar_samples, 'ALL.1kg.annovar.100sample_50000.vcf'),
    (command_100_annovar_samples, 'ALL.1kg.annovar.100sample_10000.vcf'),

    (command_10_annovar_samples, 'ALL.1kg.annovar.10sample_5000000.vcf'),
    (command_10_annovar_samples, 'ALL.1kg.annovar.10sample_1000000.vcf'),
    (command_10_annovar_samples, 'ALL.1kg.annovar.10sample_500000.vcf'),
    (command_10_annovar_samples, 'ALL.1kg.annovar.10sample_100000.vcf'),
    (command_10_annovar_samples, 'ALL.1kg.annovar.10sample_50000.vcf'),
    (command_10_annovar_samples, 'ALL.1kg.annovar.10sample_10000.vcf'),

    (command_1000_vep_samples, 'ALL.1kg.everythingVEP.1000sample_5000000.vcf'),
    (command_1000_vep_samples, 'ALL.1kg.everythingVEP.1000sample_1000000.vcf'),
    (command_1000_vep_samples, 'ALL.1kg.everythingVEP.1000sample_500000.vcf'),
    (command_1000_vep_samples, 'ALL.1kg.everythingVEP.1000sample_100000.vcf'),
    (command_1000_vep_samples, 'ALL.1kg.everythingVEP.1000sample_50000.vcf'),
    (command_1000_vep_samples, 'ALL.1kg.everythingVEP.1000sample_10000.vcf'),

    (command_500_vep_samples, 'ALL.1kg.everythingVEP.500sample_5000000.vcf'),
    (command_500_vep_samples, 'ALL.1kg.everythingVEP.500sample_1000000.vcf'),
    (command_500_vep_samples, 'ALL.1kg.everythingVEP.500sample_500000.vcf'),
    (command_500_vep_samples, 'ALL.1kg.everythingVEP.500sample_100000.vcf'),
    (command_500_vep_samples, 'ALL.1kg.everythingVEP.500sample_50000.vcf'),
    (command_500_vep_samples, 'ALL.1kg.everythingVEP.500sample_10000.vcf'),

    (command_100_vep_samples, 'ALL.1kg.everythingVEP.100sample_5000000.vcf'),
    (command_100_vep_samples, 'ALL.1kg.everythingVEP.100sample_1000000.vcf'),
    (command_100_vep_samples, 'ALL.1kg.everythingVEP.100sample_500000.vcf'),
    (command_100_vep_samples, 'ALL.1kg.everythingVEP.100sample_100000.vcf'),
    (command_100_vep_samples, 'ALL.1kg.everythingVEP.100sample_50000.vcf'),
    (command_100_vep_samples, 'ALL.1kg.everythingVEP.100sample_10000.vcf'),

    (command_10_vep_samples, 'ALL.1kg.everythingVEP.10sample_5000000.vcf'),
    (command_10_vep_samples, 'ALL.1kg.everythingVEP.10sample_1000000.vcf'),
    (command_10_vep_samples, 'ALL.1kg.everythingVEP.10sample_500000.vcf'),
    (command_10_vep_samples, 'ALL.1kg.everythingVEP.10sample_100000.vcf'),
    (command_10_vep_samples, 'ALL.1kg.everythingVEP.10sample_50000.vcf'),
    (command_10_vep_samples, 'ALL.1kg.everythingVEP.10sample_10000.vcf'),

]


subprocess.run("curl -XDELETE '%s:9200/_all?pretty'" %(hostname), shell=True)
start = datetime.now()
for command, file in files:
    print(file)
    full_command = command %(hostname, file)
    inner_start = datetime.now()
    for no in range(5):
        subprocess.run(full_command, shell=True)
    inner_end = datetime.now()
    print('Total Time for:', file, inner_end-inner_start)

end = datetime.now()

print('Total time: %s' %(end-start))


# python prepare_elasticsearch_for_import.py --hostname 172.17.39.0 --port 9200 --index thousand_samples_annovar --type thousand_samples_annovar --info es_scripts/inspect_output_for_thousand_samples_annovar_thousand_samples_annovar.txt


#
