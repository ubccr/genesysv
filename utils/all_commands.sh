source /home/mkzia/GDW/env/bin/activate
export PATH=/home/mkzia/GDW/env/bin/python:$PATH

IPADDRESS=$(hostname --ip-address)

/home/mkzia/GDW/env/bin/python inspect_vcf.py --index ten_samples_annovar --type ten_samples_annovar --vcf ~/annovar_annotation/ALL.1kg.annovar.10sample_5000000.vcf --labels None;

/home/mkzia/GDW/env/bin/python inspect_vcf.py --index hundred_samples_annovar --type hundred_samples_annovar --vcf ~/annovar_annotation/ALL.1kg.annovar.100sample_5000000.vcf --labels None;

/home/mkzia/GDW/env/bin/python inspect_vcf.py --index five_hundred_samples_annovar --type five_hundred_samples_annovar --vcf ~/annovar_annotation/ALL.1kg.annovar.500sample_5000000.vcf --labels None;

/home/mkzia/GDW/env/bin/python inspect_vcf.py --index thousand_samples_annovar --type thousand_samples_annovar --vcf ~/annovar_annotation/ALL.1kg.annovar.1000sample_5000000.vcf --labels None;


/home/mkzia/GDW/env/bin/python inspect_vcf.py --index ten_samples_vep --type ten_samples_vep --vcf ~/vep_annotation/ALL.1kg.everythingVEP.10sample_5000000.vcf --labels None;

/home/mkzia/GDW/env/bin/python inspect_vcf.py --index hundred_samples_vep --type hundred_samples_vep --vcf ~/vep_annotation/ALL.1kg.everythingVEP.100sample_5000000.vcf --labels None;

/home/mkzia/GDW/env/bin/python inspect_vcf.py --index five_hundred_samples_vep --type five_hundred_samples_vep --vcf ~/vep_annotation/ALL.1kg.everythingVEP.500sample_5000000.vcf --labels None;

/home/mkzia/GDW/env/bin/python inspect_vcf.py --index thousand_samples_vep --type thousand_samples_vep --vcf ~/vep_annotation/ALL.1kg.everythingVEP.1000sample_5000000.vcf --labels None;


/home/mkzia/GDW/env/bin/python prepare_elasticsearch_for_import.py --hostname $IPADDRESS --port 9200 --index ten_samples_annovar --type ten_samples_annovar --info es_scripts/inspect_output_for_ten_samples_annovar_ten_samples_annovar.txt;

/home/mkzia/GDW/env/bin/python prepare_elasticsearch_for_import.py --hostname $IPADDRESS --port 9200 --index hundred_samples_annovar --type hundred_samples_annovar --info es_scripts/inspect_output_for_hundred_samples_annovar_hundred_samples_annovar.txt;

/home/mkzia/GDW/env/bin/python prepare_elasticsearch_for_import.py --hostname $IPADDRESS --port 9200 --index five_hundred_samples_annovar --type five_hundred_samples_annovar --info es_scripts/inspect_output_for_five_hundred_samples_annovar_five_hundred_samples_annovar.txt;

/home/mkzia/GDW/env/bin/python prepare_elasticsearch_for_import.py --hostname $IPADDRESS --port 9200 --index thousand_samples_annovar --type thousand_samples_annovar --info es_scripts/inspect_output_for_thousand_samples_annovar_thousand_samples_annovar.txt;

/home/mkzia/GDW/env/bin/python prepare_elasticsearch_for_import.py --hostname $IPADDRESS --port 9200 --index ten_samples_vep --type ten_samples_vep --info es_scripts/inspect_output_for_ten_samples_vep_ten_samples_vep.txt;


/home/mkzia/GDW/env/bin/python prepare_elasticsearch_for_import.py --hostname $IPADDRESS --port 9200 --index hundred_samples_vep --type hundred_samples_vep --info es_scripts/inspect_output_for_hundred_samples_vep_hundred_samples_vep.txt;

/home/mkzia/GDW/env/bin/python prepare_elasticsearch_for_import.py --hostname $IPADDRESS --port 9200 --index five_hundred_samples_vep --type five_hundred_samples_vep --info es_scripts/inspect_output_for_five_hundred_samples_vep_five_hundred_samples_vep.txt;

/home/mkzia/GDW/env/bin/python prepare_elasticsearch_for_import.py --hostname $IPADDRESS --port 9200 --index thousand_samples_vep --type thousand_samples_vep --info es_scripts/inspect_output_for_thousand_samples_vep_thousand_samples_vep.txt;



