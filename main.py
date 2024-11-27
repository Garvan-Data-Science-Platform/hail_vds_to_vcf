#!/usr/bin/env python3
import click
import os

from cpg_utils.dataproc import setup_dataproc
from cpg_utils.hail_batch import get_batch
from cpg_utils.config import config_retrieve


@click.command()
@click.option('--vds', help='Input VDS file path', type=str, required=True)
@click.option('--output', help='Output VCF file path', type=str, required=True)
@click.option('--regions', help='Regions to subset to', type=str, default=None)
@click.option('--regions_file', help='BED file containing regions to subset to', type=str, default=None)
def main(
    vds: str,
    output: str,
    regions: str | None,
    regions_file: str | None,
):
    batch = get_batch(name='vds_to_vcf_dp')

    # get relative path of cwd to the script using os
    script_path = os.path.join(
        os.path.relpath(os.path.dirname(__file__), os.getcwd()),
        'vds_to_vcf.py',
    )

    # Get configuration details
    max_age = config_retrieve(key=['dataproc', 'max_age'], default='1h')
    num_workers = config_retrieve(key=['dataproc', 'num_workers'], default=2)
    n_partitions = config_retrieve(key=['vds_to_vcf', 'n_partitions'], default=2586)

    regions_param = ''
    if regions:
        regions_param = f'--regions {regions}'
    elif regions_file:
        regions_param = f'--regions_file {regions_file}'

    cmd = f'{script_path} --vds {vds} --output {output} --n_partitions {n_partitions} {regions_param}'

    cluster = setup_dataproc(
        batch,
        max_age=max_age,
        num_workers=num_workers,
        packages=['click', 'gnomad'],
        cluster_name='hail_vds_to_vcf',
    )
    cluster.add_job(cmd, job_name='vds_to_vcf')

    # # Don't wait, which avoids resubmissions if this job gets preempted.
    batch.run(wait=False)
