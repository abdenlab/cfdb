"""Ontology mappings for ENCODE metadata transformation to C2M2 format."""

# ENCODE file_format to EDAM format CV terms
# Reference: https://edamontology.org/page/formats
FILE_FORMAT_TO_EDAM = {
    # Sequence formats
    "fastq": {"id": "format:1930", "name": "FASTQ"},
    "fasta": {"id": "format:1929", "name": "FASTA"},
    # Alignment formats
    "bam": {"id": "format:2572", "name": "BAM"},
    "sam": {"id": "format:2573", "name": "SAM"},
    "cram": {"id": "format:3462", "name": "CRAM"},
    # Genomic interval formats
    "bed": {"id": "format:3003", "name": "BED"},
    "bedpe": {"id": "format:3003", "name": "BED"},  # BED paired-end
    "broadpeak": {"id": "format:3614", "name": "BroadPeak"},
    "narrowpeak": {"id": "format:3613", "name": "NarrowPeak"},
    "gappedpeak": {"id": "format:3003", "name": "BED"},  # gappedPeak is BED variant
    "bed narrowpeak": {"id": "format:3613", "name": "NarrowPeak"},
    "bed broadpeak": {"id": "format:3614", "name": "BroadPeak"},
    # Variant formats
    "vcf": {"id": "format:3016", "name": "VCF"},
    # Annotation formats
    "gtf": {"id": "format:2306", "name": "GTF"},
    "gff": {"id": "format:1975", "name": "GFF"},
    "gff3": {"id": "format:1975", "name": "GFF3"},
    # Signal/coverage formats
    "bigwig": {"id": "format:3006", "name": "bigWig"},
    "bigbed": {"id": "format:3004", "name": "bigBed"},
    "wig": {"id": "format:3005", "name": "WIG"},
    "bedgraph": {"id": "format:3583", "name": "bedGraph"},
    # Tabular formats
    "tsv": {"id": "format:3475", "name": "TSV"},
    "csv": {"id": "format:3752", "name": "CSV"},
    # Archive/compressed formats
    "tar": {"id": "format:3981", "name": "TAR"},
    "hdf5": {"id": "format:3590", "name": "HDF5"},
    # Other formats
    "json": {"id": "format:3464", "name": "JSON"},
    "txt": {"id": "format:2330", "name": "Plain text"},
    "pdf": {"id": "format:3508", "name": "PDF"},
    "png": {"id": "format:3603", "name": "PNG"},
    "jpg": {"id": "format:3579", "name": "JPG"},
    "hic": {"id": "format:3590", "name": "HDF5"},  # .hic files are HDF5-based
    "pairs": {"id": "format:2330", "name": "Plain text"},  # 4DN pairs format
    "cool": {"id": "format:3590", "name": "HDF5"},  # cooler format is HDF5-based
    "mcool": {"id": "format:3590", "name": "HDF5"},  # multi-resolution cooler
    "idat": {"id": "format:2333", "name": "Binary format"},
    "cel": {"id": "format:1638", "name": "CEL"},
    "rcc": {"id": "format:2330", "name": "Plain text"},
    "sra": {"id": "format:3698", "name": "SRA format"},
    "database": {"id": "format:2330", "name": "Plain text"},
    "starch": {"id": "format:3003", "name": "BED"},  # BEDOPS compressed BED archive
    "tagalign": {"id": "format:3003", "name": "BED"},  # tagAlign is a BED variant
    "biginteract": {"id": "format:3004", "name": "bigBed"},  # bigInteract is a bigBed variant
    "csfasta": {"id": "format:1929", "name": "FASTA"},  # color-space FASTA (SOLiD)
    "csqual": {"id": "format:2330", "name": "Plain text"},  # color-space quality scores
    "h5ad": {"id": "format:3590", "name": "HDF5"},  # AnnData HDF5 format
}

# ENCODE output_type to EDAM data CV terms
# Reference: https://edamontology.org/page/data
OUTPUT_TYPE_TO_EDAM = {
    # Raw sequence data
    "reads": {"id": "data:0924", "name": "Sequence trace"},
    "raw reads": {"id": "data:0924", "name": "Sequence trace"},
    # Alignments
    "alignments": {"id": "data:0863", "name": "Sequence alignment"},
    "unfiltered alignments": {"id": "data:0863", "name": "Sequence alignment"},
    "redacted alignments": {"id": "data:0863", "name": "Sequence alignment"},
    "transcriptome alignments": {"id": "data:0863", "name": "Sequence alignment"},
    # Peaks/regions
    "peaks": {"id": "data:3002", "name": "Annotation track"},
    "replicated peaks": {"id": "data:3002", "name": "Annotation track"},
    "stable peaks": {"id": "data:3002", "name": "Annotation track"},
    "conservative IDR thresholded peaks": {"id": "data:3002", "name": "Annotation track"},
    "optimal IDR thresholded peaks": {"id": "data:3002", "name": "Annotation track"},
    "pseudoreplicated IDR thresholded peaks": {"id": "data:3002", "name": "Annotation track"},
    "IDR ranked peaks": {"id": "data:3002", "name": "Annotation track"},
    "hotspots": {"id": "data:3002", "name": "Annotation track"},
    # Signal tracks
    "signal": {"id": "data:2884", "name": "Plot"},
    "signal p-value": {"id": "data:2884", "name": "Plot"},
    "signal of unique reads": {"id": "data:2884", "name": "Plot"},
    "signal of all reads": {"id": "data:2884", "name": "Plot"},
    "fold change over control": {"id": "data:2884", "name": "Plot"},
    "plus strand signal of unique reads": {"id": "data:2884", "name": "Plot"},
    "minus strand signal of unique reads": {"id": "data:2884", "name": "Plot"},
    "plus strand signal of all reads": {"id": "data:2884", "name": "Plot"},
    "minus strand signal of all reads": {"id": "data:2884", "name": "Plot"},
    "read-depth normalized signal": {"id": "data:2884", "name": "Plot"},
    "control normalized signal": {"id": "data:2884", "name": "Plot"},
    "percentage normalized signal": {"id": "data:2884", "name": "Plot"},
    # Gene expression
    "gene quantifications": {"id": "data:2603", "name": "Expression data"},
    "transcript quantifications": {"id": "data:2603", "name": "Expression data"},
    "exon quantifications": {"id": "data:2603", "name": "Expression data"},
    "microRNA quantifications": {"id": "data:2603", "name": "Expression data"},
    "splice junctions": {"id": "data:2603", "name": "Expression data"},
    # Methylation
    "methylation state at CpG": {"id": "data:1772", "name": "Methylation data"},
    "methylation state at CHG": {"id": "data:1772", "name": "Methylation data"},
    "methylation state at CHH": {"id": "data:1772", "name": "Methylation data"},
    # Variants
    "variant calls": {"id": "data:3498", "name": "Sequence variations"},
    # Chromatin structure
    "chromatin interactions": {"id": "data:0006", "name": "Data"},
    "contact matrix": {"id": "data:2082", "name": "Matrix"},
    "contact domains": {"id": "data:3002", "name": "Annotation track"},
    "loops": {"id": "data:3002", "name": "Annotation track"},
    "topologically associated domains": {"id": "data:3002", "name": "Annotation track"},
    # Annotations
    "genome annotations": {"id": "data:1255", "name": "Sequence features"},
    "element annotations": {"id": "data:1255", "name": "Sequence features"},
    "transcription start sites": {"id": "data:1255", "name": "Sequence features"},
    "enhancer predictions": {"id": "data:1255", "name": "Sequence features"},
    "long range chromatin interactions": {"id": "data:0006", "name": "Data"},
    # Reference data
    "genome reference": {"id": "data:2340", "name": "Genome identifier"},
    "sequence alignability": {"id": "data:0006", "name": "Data"},
    "blacklisted regions": {"id": "data:3002", "name": "Annotation track"},
    # QC/metrics
    "enrichment": {"id": "data:2048", "name": "Report"},
    "FRiP": {"id": "data:2048", "name": "Report"},
    "mapping quality thresholded": {"id": "data:0006", "name": "Data"},
    # Other
    "reporter code counts": {"id": "data:0006", "name": "Data"},
    "index": {"id": "data:0006", "name": "Data"},
    "genome index": {"id": "data:0006", "name": "Data"},
}

# ENCODE assay_title to OBI assay type CV terms
# Reference: http://purl.obolibrary.org/obo/obi
ASSAY_TITLE_TO_OBI = {
    # Chromatin accessibility
    "ATAC-seq": {"id": "OBI:0002039", "name": "ATAC-seq"},
    "DNase-seq": {"id": "OBI:0001853", "name": "DNase-seq"},
    "FAIRE-seq": {"id": "OBI:0001859", "name": "FAIRE-seq"},
    "MNase-seq": {"id": "OBI:0001924", "name": "MNase-seq"},
    # Histone modification
    "ChIP-seq": {"id": "OBI:0000716", "name": "ChIP-seq"},
    "Histone ChIP-seq": {"id": "OBI:0000716", "name": "ChIP-seq"},
    "Mint-ChIP-seq": {"id": "OBI:0000716", "name": "ChIP-seq"},
    "Control ChIP-seq": {"id": "OBI:0000716", "name": "ChIP-seq"},
    "TF ChIP-seq": {"id": "OBI:0000716", "name": "ChIP-seq"},
    # Transcription
    "RNA-seq": {"id": "OBI:0001271", "name": "RNA-seq"},
    "total RNA-seq": {"id": "OBI:0001271", "name": "RNA-seq"},
    "polyA plus RNA-seq": {"id": "OBI:0001271", "name": "RNA-seq"},
    "polyA minus RNA-seq": {"id": "OBI:0001271", "name": "RNA-seq"},
    "small RNA-seq": {"id": "OBI:0001271", "name": "RNA-seq"},
    "long read RNA-seq": {"id": "OBI:0001271", "name": "RNA-seq"},
    "microRNA-seq": {"id": "OBI:0001922", "name": "miRNA profiling by high throughput sequencing"},
    "miRNA-seq": {"id": "OBI:0001922", "name": "miRNA profiling by high throughput sequencing"},
    "shRNA knockdown followed by RNA-seq": {"id": "OBI:0001271", "name": "RNA-seq"},
    "siRNA knockdown followed by RNA-seq": {"id": "OBI:0001271", "name": "RNA-seq"},
    "CRISPR RNA-seq": {"id": "OBI:0001271", "name": "RNA-seq"},
    "single-cell RNA sequencing assay": {"id": "OBI:0002631", "name": "single-cell RNA-seq"},
    "scRNA-seq": {"id": "OBI:0002631", "name": "single-cell RNA-seq"},
    # Nascent transcription
    "GRO-seq": {"id": "OBI:0001677", "name": "GRO-seq"},
    "GRO-cap": {"id": "OBI:0001677", "name": "GRO-seq"},
    "PRO-seq": {"id": "OBI:0002084", "name": "PRO-seq"},
    "PRO-cap": {"id": "OBI:0002084", "name": "PRO-seq"},
    "CAGE": {"id": "OBI:0001674", "name": "CAGE"},
    "RAMPAGE": {"id": "OBI:0001864", "name": "RAMPAGE"},
    "MPRA": {"id": "OBI:0002675", "name": "MPRA"},
    "STARR-seq": {"id": "OBI:0002041", "name": "STARR-seq"},
    # DNA methylation
    "WGBS": {"id": "OBI:0001863", "name": "whole genome bisulfite sequencing"},
    "whole-genome shotgun bisulfite sequencing": {"id": "OBI:0001863", "name": "whole genome bisulfite sequencing"},
    "RRBS": {"id": "OBI:0001862", "name": "reduced representation bisulfite sequencing"},
    "MeDIP-seq": {"id": "OBI:0001861", "name": "MeDIP-seq"},
    "TAB-seq": {"id": "OBI:0002086", "name": "TAB-seq"},
    # Chromatin conformation
    "Hi-C": {"id": "OBI:0002042", "name": "Hi-C"},
    "in situ Hi-C": {"id": "OBI:0002042", "name": "Hi-C"},
    "intact Hi-C": {"id": "OBI:0002042", "name": "Hi-C"},
    "Capture Hi-C": {"id": "OBI:0002457", "name": "capture Hi-C"},
    "Micro-C": {"id": "OBI:0003102", "name": "Micro-C"},
    "ChIA-PET": {"id": "OBI:0001848", "name": "ChIA-PET"},
    "PLAC-seq": {"id": "OBI:0002457", "name": "capture Hi-C"},  # Similar to capture Hi-C
    "5C": {"id": "OBI:0001916", "name": "5C"},
    # Protein-DNA binding
    "CUT&RUN": {"id": "OBI:0003003", "name": "CUT&RUN"},
    "CUT&Tag": {"id": "OBI:0003004", "name": "CUT&Tag"},
    # Replication timing
    "Repli-seq": {"id": "OBI:0001917", "name": "Repli-seq"},
    "Repli-chip": {"id": "OBI:0001916", "name": "Repli-chip"},
    # RNA binding
    "RIP-seq": {"id": "OBI:0001857", "name": "RIP-seq"},
    "CLIP-seq": {"id": "OBI:0001919", "name": "CLIP-seq"},
    "iCLIP": {"id": "OBI:0001919", "name": "CLIP-seq"},
    "eCLIP": {"id": "OBI:0002111", "name": "eCLIP"},
    # Single cell
    "single-nucleus ATAC-seq": {"id": "OBI:0002762", "name": "snATAC-seq"},
    "snATAC-seq": {"id": "OBI:0002762", "name": "snATAC-seq"},
    "scATAC-seq": {"id": "OBI:0002762", "name": "snATAC-seq"},
    "10x multiome": {"id": "OBI:0002764", "name": "10x multiome"},
    # Genetic screens
    "genetic modification followed by DNase-seq": {"id": "OBI:0001853", "name": "DNase-seq"},
    "pooled clone sequencing": {"id": "OBI:0001271", "name": "RNA-seq"},
    # Other
    "whole genome sequencing": {"id": "OBI:0002117", "name": "whole genome sequencing"},
    "WGS": {"id": "OBI:0002117", "name": "whole genome sequencing"},
    "Bru-seq": {"id": "OBI:0002083", "name": "Bru-seq"},
    "BruUV-seq": {"id": "OBI:0002083", "name": "Bru-seq"},
    "BruChase-seq": {"id": "OBI:0002083", "name": "Bru-seq"},
    "HiChIP": {"id": "OBI:0002914", "name": "HiChIP"},
}

# Organism name to NCBI Taxonomy mapping
# Reference: https://www.ncbi.nlm.nih.gov/taxonomy
ORGANISM_TO_NCBI_TAXONOMY = {
    "Homo sapiens": {"id": "NCBI:txid9606", "name": "Homo sapiens", "clade": "species"},
    "human": {"id": "NCBI:txid9606", "name": "Homo sapiens", "clade": "species"},
    "Mus musculus": {"id": "NCBI:txid10090", "name": "Mus musculus", "clade": "species"},
    "mouse": {"id": "NCBI:txid10090", "name": "Mus musculus", "clade": "species"},
    "Drosophila melanogaster": {"id": "NCBI:txid7227", "name": "Drosophila melanogaster", "clade": "species"},
    "Caenorhabditis elegans": {"id": "NCBI:txid6239", "name": "Caenorhabditis elegans", "clade": "species"},
}

# Compression format mapping
COMPRESSION_TO_EDAM = {
    "gzip": "format:3989",
    "gz": "format:3989",
    "bz2": "format:3990",
    "zip": "format:3987",
}


def get_file_format(encode_format: str) -> dict | None:
    """
    Map ENCODE file_format to EDAM CV term.

    Args:
        encode_format: ENCODE file format string

    Returns:
        Dict with EDAM id and name, or None if not mapped
    """
    if not encode_format or not encode_format.strip():
        return None
    key = encode_format.lower()
    result = FILE_FORMAT_TO_EDAM.get(key)
    if result:
        return result
    base = key.split()[0]
    return FILE_FORMAT_TO_EDAM.get(base)


def get_data_type(output_type: str) -> dict | None:
    """
    Map ENCODE output_type to EDAM data CV term.

    Args:
        output_type: ENCODE output_type string

    Returns:
        Dict with EDAM id and name, or None if not mapped
    """
    if not output_type:
        return None
    return OUTPUT_TYPE_TO_EDAM.get(output_type)


def get_assay_type(assay_title: str) -> dict | None:
    """
    Map ENCODE assay_title to OBI CV term.

    Args:
        assay_title: ENCODE assay_title string

    Returns:
        Dict with OBI id and name, or None if not mapped
    """
    if not assay_title:
        return None
    return ASSAY_TITLE_TO_OBI.get(assay_title)


def get_taxonomy(organism: str) -> dict | None:
    """
    Map organism name to NCBI Taxonomy term.

    Args:
        organism: Organism name (e.g., "Homo sapiens", "human")

    Returns:
        Dict with NCBI taxonomy id, name, and clade, or None if not mapped
    """
    if not organism:
        return None
    return ORGANISM_TO_NCBI_TAXONOMY.get(organism)
