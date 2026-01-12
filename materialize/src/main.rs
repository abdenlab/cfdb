use anyhow::Result;
use bson::{doc, Document};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::sync::{Client, Collection};
use rayon::prelude::*;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

const DEFAULT_BATCH_SIZE: usize = 10000;
const WRITE_BATCH_SIZE: usize = 10000;

type LookupMap = HashMap<(String, String), Document>; // (submission, id) -> doc
type MultiMap = HashMap<(String, String), Vec<Document>>; // (namespace, local_id) -> [docs]

#[derive(Parser)]
#[command(name = "materialize")]
#[command(about = "Materialize denormalized file documents from C2M2 data")]
struct Args {
    /// Filter by DCC submission name (e.g., "4dn", "hubmap")
    #[arg(long)]
    submission: Option<String>,

    /// Number of files to process in each batch (default: 10000)
    #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
    batch_size: usize,

    /// Number of threads for parallel processing (default: CPU count)
    #[arg(long)]
    threads: Option<usize>,
}

struct LookupTables {
    dccs: HashMap<String, Document>,
    file_formats: LookupMap,
    data_types: LookupMap,
    assay_types: LookupMap,
    anatomies: LookupMap,
    collections: HashMap<(String, String), Document>,
    biosamples: HashMap<(String, String), Document>,
    file_in_collection: MultiMap,
    biosample_in_collection: MultiMap,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Configure thread pool if specified
    if let Some(threads) = args.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .ok();
        println!("Using {} threads", threads);
    } else {
        println!("Using {} threads (CPU count)", rayon::current_num_threads());
    }

    let uri = env::var("DATABASE_URL").unwrap_or_else(|_| "mongodb://localhost:27017".to_string());
    let client = Client::with_uri_str(&uri)?;
    let db = client.database("cfdb");

    if let Some(ref sub) = args.submission {
        println!("Materializing files for submission: {}", sub);
    } else {
        println!("Materializing all files");
    }

    println!("Batch size: {} files", args.batch_size);
    println!("\nLoading lookup tables...");

    let lookups = Arc::new(load_lookup_tables(&db, &args.submission));

    // Build file query filter
    let file_query = match &args.submission {
        Some(sub) => doc! { "submission": sub },
        None => doc! {},
    };

    // Count files
    let file_count = db
        .collection::<Document>("file")
        .count_documents(file_query.clone())
        .run()?;
    println!("\nProcessing {} files in batches of {}...", file_count, args.batch_size);

    let output: Collection<Document> = db.collection("files");

    // Delete existing documents first
    match &args.submission {
        Some(sub) => {
            let delete_result = output.delete_many(doc! { "submission": sub }).run()?;
            println!("Deleted {} existing {} documents", delete_result.deleted_count, sub);
        }
        None => {
            output.drop().run()?;
            println!("Dropped existing collection");
        }
    }

    // Progress bar for total file count
    let pb = ProgressBar::new(file_count);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec})")
            .unwrap()
            .progress_chars("#>-"),
    );

    // Process files in batches using cursor
    let file_coll = db.collection::<Document>("file");
    let mut cursor = file_coll
        .find(file_query)
        .batch_size(args.batch_size as u32)
        .run()?;

    let mut batch: Vec<Document> = Vec::with_capacity(args.batch_size);
    let mut total_written: u64 = 0;
    let mut batch_num: u64 = 0;
    let total_batches = (file_count + args.batch_size as u64 - 1) / args.batch_size as u64;

    loop {
        // Fill the batch
        batch.clear();
        while batch.len() < args.batch_size {
            match cursor.next() {
                Some(Ok(doc)) => batch.push(doc),
                Some(Err(e)) => {
                    eprintln!("Error reading document: {}", e);
                    continue;
                }
                None => break, // End of cursor
            }
        }

        if batch.is_empty() {
            break;
        }

        batch_num += 1;
        eprintln!(
            "Processing batch {}/{} ({} files)...",
            batch_num, total_batches, batch.len()
        );

        // Process batch in parallel
        let lookups_ref = Arc::clone(&lookups);
        let enriched: Vec<Document> = batch
            .par_iter()
            .map(|file| enrich_file(file.clone(), &lookups_ref))
            .collect();

        // Write batch results
        for chunk in enriched.chunks(WRITE_BATCH_SIZE) {
            output.insert_many(chunk).run()?;
        }

        total_written += enriched.len() as u64;
        pb.set_position(total_written);
        eprintln!("  Wrote {} documents (total: {})", enriched.len(), total_written);
    }

    pb.finish_with_message("Processing complete");

    // Create indexes
    println!("\nCreating indexes...");
    create_indexes(&output)?;

    println!("\nWrote {} enriched documents", total_written);
    println!("Done!");
    Ok(())
}

fn load_lookup_tables(db: &mongodb::sync::Database, submission_filter: &Option<String>) -> LookupTables {
    // Load DCCs keyed by submission
    let dccs: HashMap<String, Document> = load_collection(&db.collection("dcc"))
        .into_iter()
        .filter_map(|d| {
            let submission = d.get_str("submission").ok()?.to_string();
            Some((submission, d))
        })
        .collect();
    println!("  dcc: {} entries", dccs.len());

    // Load ontology lookups keyed by (submission, id)
    let file_formats = load_lookup_table(&db.collection("file_format"), submission_filter);
    println!("  file_format: {} entries", file_formats.len());

    let data_types = load_lookup_table(&db.collection("data_type"), submission_filter);
    println!("  data_type: {} entries", data_types.len());

    let assay_types = load_lookup_table(&db.collection("assay_type"), submission_filter);
    println!("  assay_type: {} entries", assay_types.len());

    let anatomies = load_lookup_table(&db.collection("anatomy"), submission_filter);
    println!("  anatomy: {} entries", anatomies.len());

    // Load collections keyed by (id_namespace, local_id)
    let collections = load_entity_table(&db.collection("collection"), submission_filter);
    println!("  collection: {} entries", collections.len());

    // Load biosamples keyed by (id_namespace, local_id)
    let biosamples = load_entity_table(&db.collection("biosample"), submission_filter);
    println!("  biosample: {} entries", biosamples.len());

    // Load junction tables as multi-maps
    let file_in_collection = load_file_in_collection(&db.collection("file_in_collection"), submission_filter);
    println!("  file_in_collection: {} entries", file_in_collection.len());

    let biosample_in_collection =
        load_biosample_in_collection(&db.collection("biosample_in_collection"), submission_filter);
    println!("  biosample_in_collection: {} entries", biosample_in_collection.len());

    LookupTables {
        dccs,
        file_formats,
        data_types,
        assay_types,
        anatomies,
        collections,
        biosamples,
        file_in_collection,
        biosample_in_collection,
    }
}

fn enrich_file(mut file: Document, lookups: &LookupTables) -> Document {
    let submission = file.get_str("submission").unwrap_or_default().to_string();
    let id_namespace = file.get_str("id_namespace").unwrap_or_default().to_string();
    let local_id = file.get_str("local_id").unwrap_or_default().to_string();

    // Lookup DCC
    if let Some(dcc) = lookups.dccs.get(&submission) {
        let mut dcc_copy = dcc.clone();
        dcc_copy.remove("_id");
        file.insert("dcc", dcc_copy);
    }

    // Lookup file_format (skip empty strings)
    if let Some(format_id) = file.get_str("file_format").ok() {
        if !format_id.is_empty() {
            if let Some(format) = lookups.file_formats.get(&(submission.clone(), format_id.to_string())) {
                let mut format_copy = format.clone();
                format_copy.remove("_id");
                file.insert("file_format", format_copy);
            }
        } else {
            file.remove("file_format");
        }
    }

    // Lookup data_type (skip empty strings)
    if let Some(type_id) = file.get_str("data_type").ok() {
        if !type_id.is_empty() {
            if let Some(dtype) = lookups.data_types.get(&(submission.clone(), type_id.to_string())) {
                let mut dtype_copy = dtype.clone();
                dtype_copy.remove("_id");
                file.insert("data_type", dtype_copy);
            }
        } else {
            file.remove("data_type");
        }
    }

    // Lookup assay_type (skip empty strings)
    if let Some(assay_id) = file.get_str("assay_type").ok() {
        if !assay_id.is_empty() {
            if let Some(assay) = lookups.assay_types.get(&(submission.clone(), assay_id.to_string())) {
                let mut assay_copy = assay.clone();
                assay_copy.remove("_id");
                file.insert("assay_type", assay_copy);
            }
        } else {
            file.remove("assay_type");
        }
    }

    // Build collections array with nested biosamples
    let file_key = (id_namespace.clone(), local_id.clone());
    let mut enriched_collections: Vec<Document> = Vec::new();

    if let Some(file_colls) = lookups.file_in_collection.get(&file_key) {
        for fc in file_colls {
            let coll_ns = fc
                .get_str("collection_id_namespace")
                .unwrap_or_default()
                .to_string();
            let coll_id = fc
                .get_str("collection_local_id")
                .unwrap_or_default()
                .to_string();
            let coll_key = (coll_ns.clone(), coll_id.clone());

            if let Some(coll) = lookups.collections.get(&coll_key) {
                let mut coll_copy = coll.clone();
                coll_copy.remove("_id");

                // Build biosamples array for this collection
                let mut enriched_biosamples: Vec<Document> = Vec::new();

                if let Some(bios_in_coll) = lookups.biosample_in_collection.get(&coll_key) {
                    for bc in bios_in_coll {
                        let bio_ns = bc
                            .get_str("biosample_id_namespace")
                            .unwrap_or_default()
                            .to_string();
                        let bio_id = bc
                            .get_str("biosample_local_id")
                            .unwrap_or_default()
                            .to_string();
                        let bio_key = (bio_ns, bio_id);

                        if let Some(biosample) = lookups.biosamples.get(&bio_key) {
                            let mut bio_copy = biosample.clone();
                            bio_copy.remove("_id");

                            // Lookup anatomy for biosample
                            if let Some(anatomy_id) = biosample.get_str("anatomy").ok() {
                                if let Some(anatomy) =
                                    lookups.anatomies.get(&(submission.clone(), anatomy_id.to_string()))
                                {
                                    let mut anatomy_copy = anatomy.clone();
                                    anatomy_copy.remove("_id");
                                    bio_copy.insert("anatomy", anatomy_copy);
                                }
                            }

                            enriched_biosamples.push(bio_copy);
                        }
                    }
                }

                coll_copy.insert("biosamples", enriched_biosamples);
                enriched_collections.push(coll_copy);
            }
        }
    }

    file.insert("collections", enriched_collections);
    file
}

fn load_collection(coll: &Collection<Document>) -> Vec<Document> {
    coll.find(doc! {})
        .run()
        .unwrap()
        .filter_map(|r| r.ok())
        .collect()
}

fn load_collection_filtered(coll: &Collection<Document>, submission: &Option<String>) -> Vec<Document> {
    let query = match submission {
        Some(sub) => doc! { "submission": sub },
        None => doc! {},
    };
    coll.find(query)
        .run()
        .unwrap()
        .filter_map(|r| r.ok())
        .collect()
}

fn load_lookup_table(coll: &Collection<Document>, submission: &Option<String>) -> LookupMap {
    load_collection_filtered(coll, submission)
        .into_iter()
        .filter_map(|d| {
            let sub = d.get_str("submission").ok()?.to_string();
            let id = d.get_str("id").ok()?.to_string();
            Some(((sub, id), d))
        })
        .collect()
}

fn load_entity_table(
    coll: &Collection<Document>,
    submission: &Option<String>,
) -> HashMap<(String, String), Document> {
    load_collection_filtered(coll, submission)
        .into_iter()
        .filter_map(|d| {
            let ns = d.get_str("id_namespace").ok()?.to_string();
            let id = d.get_str("local_id").ok()?.to_string();
            Some(((ns, id), d))
        })
        .collect()
}

fn load_file_in_collection(coll: &Collection<Document>, submission: &Option<String>) -> MultiMap {
    let mut map: MultiMap = HashMap::new();
    for doc in load_collection_filtered(coll, submission) {
        if let (Ok(ns), Ok(id)) = (doc.get_str("file_id_namespace"), doc.get_str("file_local_id")) {
            map.entry((ns.to_string(), id.to_string()))
                .or_default()
                .push(doc);
        }
    }
    map
}

fn load_biosample_in_collection(coll: &Collection<Document>, submission: &Option<String>) -> MultiMap {
    let mut map: MultiMap = HashMap::new();
    for doc in load_collection_filtered(coll, submission) {
        if let (Ok(ns), Ok(id)) = (
            doc.get_str("collection_id_namespace"),
            doc.get_str("collection_local_id"),
        ) {
            map.entry((ns.to_string(), id.to_string()))
                .or_default()
                .push(doc);
        }
    }
    map
}

fn create_indexes(coll: &Collection<Document>) -> Result<()> {
    use mongodb::IndexModel;

    let indexes = vec![
        doc! { "id_namespace": 1 },
        doc! { "local_id": 1 },
        doc! { "id_namespace": 1, "local_id": 1 },
        doc! { "persistent_id": 1 },
        doc! { "filename": 1 },
        doc! { "size_in_bytes": 1 },
        doc! { "sha256": 1 },
        doc! { "md5": 1 },
        doc! { "mime_type": 1 },
        doc! { "dcc.id": 1 },
        doc! { "dcc.dcc_name": 1 },
        doc! { "dcc.dcc_abbreviation": 1 },
        doc! { "file_format.id": 1 },
        doc! { "file_format.name": 1 },
        doc! { "data_type.id": 1 },
        doc! { "data_type.name": 1 },
        doc! { "assay_type.id": 1 },
        doc! { "assay_type.name": 1 },
        doc! { "collections.id_namespace": 1 },
        doc! { "collections.local_id": 1 },
        doc! { "collections.name": 1 },
        doc! { "collections.biosamples.id_namespace": 1 },
        doc! { "collections.biosamples.local_id": 1 },
        doc! { "collections.biosamples.anatomy.id": 1 },
        doc! { "collections.biosamples.anatomy.name": 1 },
        doc! { "data_access_level": 1 },
        doc! { "submission": 1 },
    ];

    let models: Vec<IndexModel> = indexes
        .into_iter()
        .map(|keys| IndexModel::builder().keys(keys).build())
        .collect();

    coll.create_indexes(models).run()?;
    println!("  Created {} indexes", 27);
    Ok(())
}
