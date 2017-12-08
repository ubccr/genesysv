import subprocess
import sys
from datetime import datetime
sys.stdout = open('stdout_automate_benchmark.txt', 'a')
sys.stderr = open('stderr_automate_benchmark.txt', 'a')


command_10_samples = "bash es_scripts/delete_index_ten_samples.sh; bash es_scripts/create_index_ten_samples_and_put_mapping_ten_samples.sh; python import_vcf_using_celery.py --hostname %s --port 9200 --index ten_samples --type ten_samples --label None --update False --vcf ~/%s --mapping es_scripts/inspect_output_for_ten_samples_ten_samples.txt;"
command_100_samples = "bash es_scripts/delete_index_hundred_samples.sh; bash es_scripts/create_index_hundred_samples_and_put_mapping_hundred_samples.sh; python import_vcf_using_celery.py --hostname %s --port 9200 --index hundred_samples --type hundred_samples --label None --update False --vcf ~/%s --mapping es_scripts/inspect_output_for_hundred_samples_hundred_samples.txt;"
command_500_samples = "bash es_scripts/delete_index_five_hundred_samples.sh; bash es_scripts/create_index_five_hundred_samples_and_put_mapping_five_hundred_samples.sh; python import_vcf_using_celery.py --hostname %s --port 9200 --index five_hundred_samples --type five_hundred_samples --label None --update False --vcf ~/%s --mapping es_scripts/inspect_output_for_five_hundred_samples_five_hundred_samples.txt;"
command_1000_samples = "bash es_scripts/delete_index_thousand_samples.sh; bash es_scripts/create_index_thousand_samples_and_put_mapping_thousand_samples.sh; python import_vcf_using_celery.py --hostname %s --port 9200 --index thousand_samples --type thousand_samples --label None --update False --vcf ~/%s --mapping es_scripts/inspect_output_for_thousand_samples_thousand_samples.txt;"

files = [
    (command_1000_samples, 'ALL.1kg.everythingVEP.1000sample_5000000.vcf'),
    (command_500_samples, 'ALL.1kg.everythingVEP.500sample_5000000.vcf'),
    (command_1000_samples, 'ALL.1kg.everythingVEP.1000sample_1000000.vcf'),
    (command_500_samples, 'ALL.1kg.everythingVEP.500sample_1000000.vcf'),
    (command_10_samples, 'ALL.1kg.everythingVEP.10sample_10000.vcf'),
    (command_10_samples, 'ALL.1kg.everythingVEP.10sample_50000.vcf'),
    (command_10_samples, 'ALL.1kg.everythingVEP.10sample_100000.vcf'),
    (command_10_samples, 'ALL.1kg.everythingVEP.10sample_500000.vcf'),
    (command_10_samples, 'ALL.1kg.everythingVEP.10sample_1000000.vcf'),
    (command_10_samples, 'ALL.1kg.everythingVEP.10sample_5000000.vcf'),
    (command_100_samples, 'ALL.1kg.everythingVEP.100sample_10000.vcf'),
    (command_100_samples, 'ALL.1kg.everythingVEP.100sample_50000.vcf'),
    (command_100_samples, 'ALL.1kg.everythingVEP.100sample_100000.vcf'),
    (command_100_samples, 'ALL.1kg.everythingVEP.100sample_500000.vcf'),
    (command_100_samples, 'ALL.1kg.everythingVEP.100sample_1000000.vcf'),
    (command_100_samples, 'ALL.1kg.everythingVEP.100sample_5000000.vcf'),
    (command_500_samples, 'ALL.1kg.everythingVEP.500sample_10000.vcf'),
    (command_500_samples, 'ALL.1kg.everythingVEP.500sample_50000.vcf'),
    (command_500_samples, 'ALL.1kg.everythingVEP.500sample_100000.vcf'),
    (command_500_samples, 'ALL.1kg.everythingVEP.500sample_500000.vcf'),
    (command_1000_samples, 'ALL.1kg.everythingVEP.1000sample_10000.vcf'),
    (command_1000_samples, 'ALL.1kg.everythingVEP.1000sample_50000.vcf'),
    (command_1000_samples, 'ALL.1kg.everythingVEP.1000sample_100000.vcf'),
    (command_1000_samples, 'ALL.1kg.everythingVEP.1000sample_500000.vcf'),
]

hostname = "172.17.3.154"
subprocess.run("curl -XDELETE '%s:9200/_all?pretty'" %(hostname), shell=True)
start = datetime.now()
for command, file in files:
    print(file)
    full_command = command %(hostname, file)
    for no in range(5):
        subprocess.run(full_command, shell=True)
    inner_end = datetime.now()
    print('Total Time for:', file, inner_end-inner_start)

end = datetime.now()

print('Total time: %s' %(end-start))
