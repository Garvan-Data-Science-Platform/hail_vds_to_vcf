#!/usr/bin/env python3
import click
import hail as hl
from gnomad.utils.sparse_mt import default_compute_info
from gnomad.utils.vcf import adjust_vcf_incompatible_types

from cpg_utils.config import get_config
from cpg_utils.hail_batch import get_batch

config = get_config()


@click.command()
@click.option('--vds', help='Input VDS file path', type=str, required=True)
@click.option('--output', help='Output VCF file path', type=str, required=True)
@click.option('--n_partitions', help='Number of partitions for the INFO table', type=int, default=2586)
@click.option('--regions', help='Regions to subset to', type=str, default=None)
@click.option('--regions_file', help='BED file containing regions to subset to', type=str, default=None)
def main(
    vds: str,
    output: str,
    n_partitions: int,
    regions: str | None,
    regions_file: str | None
):
    # Read in the data
    v = hl.vds.read_vds(vds)

    # Subset the data
    regions_to_keep = None
    if regions:
        # Expect a comma-separated list of regions
        # Each region should be in the format "chr:start[-end]"
        # Convert to an ArrayExpression of type tinterval
        regions_list = regions.split(',')
        regions_to_keep = hl.literal(
            [hl.parse_locus_interval(region, reference_genome='GRCh38') for region in regions_list]
        )
    elif regions_file:
        # Filter to regions in the BED file
        # Expect a BED file with 3 columns: chrom, start, end
        regions_to_keep = hl.import_bed(regions_file, reference_genome='GRCh38')

    # Filter to the regions
    if regions_to_keep:
        v = hl.vds.filter_intervals(v, regions_to_keep, keep=True)

    # Split multi-allelelic sites
    v = hl.vds.split_multi(v, filter_changed_loci=True)

    # Create a dense MT
    mt = hl.vds.to_dense_mt(v)

    # Check for required entry fields
    required_fields = {'LA', 'LGT', 'DP'}
    entry_fields = set(mt.entry)
    assert required_fields.issubset(entry_fields), f"Missing required entry fields: {required_fields}"

    # Convert LGT to GT if necessary
    if 'GT' not in list(mt.entry):
        mt = mt.annotate_entries(
            GT=hl.vds.lgt_to_gt(mt.LGT, mt.LA)
        )

    # Filter to non-ref sites and add site-level DP
    mt = mt.filter_rows(
        (hl.len(mt.alleles) > 1) &
        (hl.agg.any(mt.LGT.is_non_ref()))
    )
    mt = mt.annotate_rows(
        site_dp=hl.agg.sum(mt.DP)
    )

    # Create an INFO HT
    info_ht = default_compute_info(
        mt,
        site_annotations=True,
        n_partitions=n_partitions,
    )
    info_ht = info_ht.annotate(
        info=info_ht.info.annotate(
            DP=mt.rows()[info_ht.key].site_dp
        )
    )
    info_ht_adj = adjust_vcf_incompatible_types(info_ht, pipe_delimited_annotations=[])

    # Add the INFO field
    mt = mt.annotate_rows(
        info=info_ht_adj[mt.row_key].info
    )

    # Prepare for export to VCF
    fields_to_drop = ['gvcf_info']
    fields_to_drop = [f for f in fields_to_drop if f in list(mt.entry)]
    mt = mt.drop(*fields_to_drop)

    # Export to VCF
    hl.export_vcf(mt, output)


if __name__ == '__main__':
    main()