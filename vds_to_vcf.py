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
def main(vds, output, n_partitions, regions, regions_file):
    # Read in the data
    v = hl.vds.read_vds(vds)
    vd = v.variant_data
    vd.describe()

    # Subset the data
    # TODO: Implement region subsetting
    if regions:
        pass
    elif regions_file:
        pass

    # Create a dense MT
    mt = hl.vds.to_dense_mt(v)

    # Convert LGT to GT
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
    mt = mt.drop(*fields_to_drop)

    # Export to VCF
    hl.export_vcf(mt, output)


if __name__ == '__main__':
    main()