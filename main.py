#!/usr/bin/env python3
import click
import os

from cpg_utils.dataproc import setup_dataproc
from cpg_utils.hail_batch import get_batch


@click.command()
@click.option('--vds', help='Input VDS file path', type=str, required=True)
@click.option('--output', help='Output VCF file path', type=str, required=True)
@click.option('--n_partitions', help='Number of partitions for the INFO table', type=int, default=2586)
@click.option('--regions', help='Regions to subset to', type=str, default=None)
@click.option('--regions_file', help='BED file containing regions to subset to', type=str, default=None)
@click.option('--n_workers', help='Number of workers', type=int, default=2)
@click.option('--max_age', help='Max age of the cluster', type=str, default='1h')
def main(vds, output, n_partitions, regions, regions_file, n_workers, max_age):
    batch = get_batch(name='vds_to_vcf')

    # get relative path of cwd to the script using os
    SCRIPT_PATH = os.path.join(
        os.path.relpath(os.path.dirname(__file__), os.getcwd()),
        'vds_to_vcf.py',
    )

    REGIONS_PARAM = ''
    if regions:
        REGIONS_PARAM = f'--regions {regions}'
    elif regions_file:
        REGIONS_PARAM = f'--regions_file {regions_file}'

    CMD = f'{SCRIPT_PATH} --vds {vds} --output {output} --n_partitions {n_partitions} {REGIONS_PARAM}'

    cluster = setup_dataproc(
        batch,
        max_age=max_age,
        num_workers=n_workers,
        packages=['click', 'gnomad'],
        cluster_name='hail_vds_to_vcf',
    )
    cluster.add_job(CMD, job_name='vds_to_vcf')

    # # Don't wait, which avoids resubmissions if this job gets preempted.
    batch.run(wait=False)