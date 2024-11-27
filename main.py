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
def main(
    vds: str,
    output: str,
    n_partitions: int,
    regions: str | None,
    regions_file: str | None,
    n_workers: int,
    max_age: str
):
    batch = get_batch(name='vds_to_vcf')

    # get relative path of cwd to the script using os
    script_path = os.path.join(
        os.path.relpath(os.path.dirname(__file__), os.getcwd()),
        'vds_to_vcf.py',
    )

    regions_param = ''
    if regions:
        regions_param = f'--regions {regions}'
    elif regions_file:
        regions_param = f'--regions_file {regions_file}'

    cmd = f'{script_path} --vds {vds} --output {output} --n_partitions {n_partitions} {regions_param}'

    cluster = setup_dataproc(
        batch,
        max_age=max_age,
        num_workers=n_workers,
        packages=['click', 'gnomad'],
        cluster_name='hail_vds_to_vcf',
    )
    cluster.add_job(cmd, job_name='vds_to_vcf')

    # # Don't wait, which avoids resubmissions if this job gets preempted.
    batch.run(wait=False)