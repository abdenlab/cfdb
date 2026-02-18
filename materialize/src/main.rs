use anyhow::Result;
use bson::{doc, Document};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::options::{ClientOptions, TlsOptions};
use mongodb::sync::{Client, Collection};
use rayon::prelude::*;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
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
    ncbi_taxonomies: LookupMap,
    projects: HashMap<(String, String), Document>,
    collections: HashMap<(String, String), Document>,
    biosamples: HashMap<(String, String), Document>,
    subjects: HashMap<(String, String), Document>,
    file_in_collection: MultiMap,
    biosample_in_collection: MultiMap,
    biosample_from_subject: MultiMap,
    subject_race: MultiMap,
    subject_role_taxonomy: MultiMap,
    collection_by_persistent_id: HashMap<String, (String, String)>,
    collection_anatomy: MultiMap,
    subject_in_collection: MultiMap,
}

/// Create MongoDB client with optional TLS authentication.
/// When TLS is enabled, SCRAM credentials are parsed from the URI automatically.
fn create_mongodb_client() -> Result<Client> {
    let uri = env::var("DATABASE_URL").unwrap_or_else(|_| "mongodb://localhost:27017".to_string());
    let tls_enabled = env::var("MONGODB_TLS_ENABLED")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);
    let retry_writes = env::var("MONGODB_RETRY_WRITES")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);

    let mut options = ClientOptions::parse(&uri).run()?;

    if !retry_writes {
        options.retry_writes = Some(false);
    }

    if tls_enabled {
        let ca_path = env::var("MONGODB_CA_PATH")
            .unwrap_or_else(|_| "/etc/cfdb/certs/global-bundle.pem".to_string());

        // Redact password from URI for logging
        let redacted = regex::Regex::new(r"://([^:]+):([^@]+)@")
            .unwrap()
            .replace(&uri, "://$1:***@");
        println!("Connecting to MongoDB at {} with TLS", redacted);

        let tls_options = TlsOptions::builder()
            .ca_file_path(Some(PathBuf::from(ca_path)))
            .build();

        options.tls = Some(mongodb::options::Tls::Enabled(tls_options));

        Ok(Client::with_options(options)?)
    } else {
        println!("Connecting to MongoDB at {} (no authentication)", uri);
        Ok(Client::with_options(options)?)
    }
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

    let client = create_mongodb_client()?;
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

    let ncbi_taxonomies = load_lookup_table(&db.collection("ncbi_taxonomy"), submission_filter);
    println!("  ncbi_taxonomy: {} entries", ncbi_taxonomies.len());

    // Load projects keyed by (id_namespace, local_id)
    let projects = load_entity_table(&db.collection("project"), submission_filter);
    println!("  project: {} entries", projects.len());

    // Load collections keyed by (id_namespace, local_id)
    let collections = load_entity_table(&db.collection("collection"), submission_filter);
    println!("  collection: {} entries", collections.len());

    // Load biosamples keyed by (id_namespace, local_id)
    let biosamples = load_entity_table(&db.collection("biosample"), submission_filter);
    println!("  biosample: {} entries", biosamples.len());

    // Load subjects keyed by (id_namespace, local_id)
    let subjects = load_entity_table(&db.collection("subject"), submission_filter);
    println!("  subject: {} entries", subjects.len());

    // Load junction tables as multi-maps
    let file_in_collection = load_file_in_collection(&db.collection("file_in_collection"), submission_filter);
    println!("  file_in_collection: {} entries", file_in_collection.len());

    let biosample_in_collection =
        load_biosample_in_collection(&db.collection("biosample_in_collection"), submission_filter);
    println!("  biosample_in_collection: {} entries", biosample_in_collection.len());

    let biosample_from_subject =
        load_biosample_from_subject(&db.collection("biosample_from_subject"), submission_filter);
    println!("  biosample_from_subject: {} entries", biosample_from_subject.len());

    let subject_race = load_subject_race(&db.collection("subject_race"), submission_filter);
    println!("  subject_race: {} entries", subject_race.len());

    let subject_role_taxonomy =
        load_subject_role_taxonomy(&db.collection("subject_role_taxonomy"), submission_filter);
    println!(
        "  subject_role_taxonomy: {} entries",
        subject_role_taxonomy.len()
    );

    // Build collection persistent_id lookup (for DOI-based file→collection matching)
    let collection_by_persistent_id =
        load_collection_by_persistent_id(&db.collection("collection"), submission_filter);
    println!(
        "  collection_by_persistent_id: {} entries",
        collection_by_persistent_id.len()
    );

    let collection_anatomy =
        load_collection_anatomy(&db.collection("collection_anatomy"), submission_filter);
    println!("  collection_anatomy: {} entries", collection_anatomy.len());

    let subject_in_collection =
        load_subject_in_collection(&db.collection("subject_in_collection"), submission_filter);
    println!(
        "  subject_in_collection: {} entries",
        subject_in_collection.len()
    );

    LookupTables {
        dccs,
        file_formats,
        data_types,
        assay_types,
        anatomies,
        ncbi_taxonomies,
        projects,
        collections,
        biosamples,
        subjects,
        file_in_collection,
        biosample_in_collection,
        biosample_from_subject,
        subject_race,
        subject_role_taxonomy,
        collection_by_persistent_id,
        collection_anatomy,
        subject_in_collection,
    }
}

/// Map file extension to enriched file format name for ambiguous container formats.
/// Returns Some(format_name) if the extension indicates a specific data format
/// that would otherwise be obscured by a generic container format (e.g., HDF5).
fn get_enriched_file_format(filename: &str) -> Option<&'static str> {
    // Extract extension (handle compound extensions like .pairs.gz)
    let lower = filename.to_lowercase();

    // Check compound extensions first
    if lower.ends_with(".pairs.gz") {
        return Some("pairs");
    }

    // Extract simple extension
    let ext = lower.rsplit('.').next()?;

    match ext {
        // Hi-C related formats (HDF5-based or custom binary)
        "mcool" => Some("mcool"),
        "cool" => Some("cool"),
        "hic" => Some("hic"),
        "pairs" => Some("pairs"),
        // Microscopy formats
        "r3d" => Some("r3d"),
        "nd2" => Some("nd2"),
        "flex" => Some("flex"),
        "spt" => Some("spt"),
        // Other specialized formats that may lack proper file_format
        "matrix" => Some("matrix"),
        _ => None,
    }
}

fn enrich_file(mut file: Document, lookups: &LookupTables) -> Document {
    let submission = file.get_str("submission").unwrap_or_default().to_string();
    let id_namespace = file.get_str("id_namespace").unwrap_or_default().to_string();
    let local_id = file.get_str("local_id").unwrap_or_default().to_string();
    let filename = file.get_str("filename").unwrap_or_default().to_string();

    // Lookup DCC
    if let Some(dcc) = lookups.dccs.get(&submission) {
        let mut dcc_copy = dcc.clone();
        dcc_copy.remove("_id");
        file.insert("dcc", dcc_copy);
    }

    // Lookup project via FK
    if let (Ok(proj_ns), Ok(proj_id)) = (
        file.get_str("project_id_namespace"),
        file.get_str("project_local_id"),
    ) {
        if !proj_ns.is_empty() && !proj_id.is_empty() {
            if let Some(project) = lookups.projects.get(&(proj_ns.to_string(), proj_id.to_string())) {
                let mut proj_copy = project.clone();
                proj_copy.remove("_id");
                file.insert("project", proj_copy);
            }
        }
    }

    // Lookup file_format (skip empty strings)
    let file_format_id = file.get_str("file_format").unwrap_or_default().to_string();
    if !file_format_id.is_empty() {
        if let Some(format) = lookups.file_formats.get(&(submission.clone(), file_format_id.clone())) {
            let mut format_copy = format.clone();
            format_copy.remove("_id");
            file.insert("file_format", format_copy);
        }
    } else {
        file.remove("file_format");
    }

    // Enrich file format for ambiguous container formats.
    // When file_format is a generic container (e.g., HDF5) or missing entirely,
    // derive a more specific format from the filename extension.
    if let Some(enriched_format) = get_enriched_file_format(&filename) {
        let is_ambiguous = file_format_id.is_empty()
            || file_format_id == "format:3590"  // HDF5
            || file_format_id == "format:2330";  // Plain text (e.g., .pairs)

        if is_ambiguous {
            let extra = file.entry("extra".to_string())
                .or_insert_with(|| bson::Bson::Document(Document::new()))
                .as_document_mut();
            if let Some(extra_doc) = extra {
                extra_doc.insert("enriched_file_format", enriched_format);
            }
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

    // Get collection keys - either from junction table or persistent_id match
    let mut coll_keys: Vec<(String, String)> = Vec::new();

    if let Some(file_colls) = lookups.file_in_collection.get(&file_key) {
        // Use junction table entries
        for fc in file_colls {
            let coll_ns = fc
                .get_str("collection_id_namespace")
                .unwrap_or_default()
                .to_string();
            let coll_id = fc
                .get_str("collection_local_id")
                .unwrap_or_default()
                .to_string();
            if !coll_ns.is_empty() && !coll_id.is_empty() {
                coll_keys.push((coll_ns, coll_id));
            }
        }
    }

    // Fallback: look up collection by persistent_id (DOI) match
    if coll_keys.is_empty() {
        if let Ok(persistent_id) = file.get_str("persistent_id") {
            if !persistent_id.is_empty() {
                if let Some((coll_ns, coll_id)) = lookups.collection_by_persistent_id.get(persistent_id) {
                    coll_keys.push((coll_ns.clone(), coll_id.clone()));
                }
            }
        }
    }

    // Process each collection
    for (coll_ns, coll_id) in coll_keys {
        let coll_key = (coll_ns.clone(), coll_id.clone());

        if let Some(coll) = lookups.collections.get(&coll_key) {
            let mut coll_copy = coll.clone();
            coll_copy.remove("_id");

            // Add anatomy terms from collection_anatomy
            let mut coll_anatomies: Vec<Document> = Vec::new();
            if let Some(ca_entries) = lookups.collection_anatomy.get(&coll_key) {
                for ca in ca_entries {
                    if let Ok(anatomy_id) = ca.get_str("anatomy") {
                        if let Some(anatomy) =
                            lookups.anatomies.get(&(submission.clone(), anatomy_id.to_string()))
                        {
                            let mut anatomy_copy = anatomy.clone();
                            anatomy_copy.remove("_id");
                            coll_anatomies.push(anatomy_copy);
                        }
                    }
                }
            }
            coll_copy.insert("anatomy", coll_anatomies);

            // Add subjects from subject_in_collection
            let mut coll_subjects: Vec<Document> = Vec::new();
            if let Some(sic_entries) = lookups.subject_in_collection.get(&coll_key) {
                for sic in sic_entries {
                    let subj_ns = sic
                        .get_str("subject_id_namespace")
                        .unwrap_or_default()
                        .to_string();
                    let subj_id = sic
                        .get_str("subject_local_id")
                        .unwrap_or_default()
                        .to_string();
                    let subj_key = (subj_ns, subj_id);

                    if let Some(subject) = lookups.subjects.get(&subj_key) {
                        let mut subj_copy = subject.clone();
                        subj_copy.remove("_id");

                        // Add race from subject_race
                        let mut races: Vec<String> = Vec::new();
                        if let Some(race_entries) = lookups.subject_race.get(&subj_key) {
                            for race_entry in race_entries {
                                if let Ok(race) = race_entry.get_str("race") {
                                    if !race.is_empty() {
                                        races.push(race.to_string());
                                    }
                                }
                            }
                        }
                        subj_copy.insert("race", races);

                        // Add taxonomy from subject_role_taxonomy
                        if let Some(srt_entries) = lookups.subject_role_taxonomy.get(&subj_key) {
                            if let Some(srt) = srt_entries.first() {
                                if let Ok(tax_id) = srt.get_str("taxonomy_id") {
                                    if let Some(taxonomy) =
                                        lookups.ncbi_taxonomies.get(&(submission.clone(), tax_id.to_string()))
                                    {
                                        let mut tax_copy = taxonomy.clone();
                                        tax_copy.remove("_id");
                                        subj_copy.insert("taxonomy", tax_copy);
                                    }
                                }
                            }
                        }

                        coll_subjects.push(subj_copy);
                    }
                }
            }
            coll_copy.insert("subjects", coll_subjects);

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

                        // Lookup subjects for this biosample
                        let mut enriched_subjects: Vec<Document> = Vec::new();
                        if let Some(bfs_junctions) = lookups.biosample_from_subject.get(&bio_key) {
                            for bfs in bfs_junctions {
                                let subj_ns = bfs
                                    .get_str("subject_id_namespace")
                                    .unwrap_or_default()
                                    .to_string();
                                let subj_id = bfs
                                    .get_str("subject_local_id")
                                    .unwrap_or_default()
                                    .to_string();
                                let subj_key = (subj_ns, subj_id);

                                if let Some(subject) = lookups.subjects.get(&subj_key) {
                                    let mut subj_copy = subject.clone();
                                    subj_copy.remove("_id");

                                    // Add age_at_sampling from junction table
                                    if let Ok(age) = bfs.get_f64("age_at_sampling") {
                                        subj_copy.insert("age_at_sampling", age);
                                    } else if let Ok(age_str) = bfs.get_str("age_at_sampling") {
                                        if let Ok(age) = age_str.parse::<f64>() {
                                            subj_copy.insert("age_at_sampling", age);
                                        }
                                    }

                                    // Add race(s) from subject_race junction table
                                    let mut races: Vec<String> = Vec::new();
                                    if let Some(race_entries) = lookups.subject_race.get(&subj_key) {
                                        for race_entry in race_entries {
                                            if let Ok(race) = race_entry.get_str("race") {
                                                if !race.is_empty() {
                                                    races.push(race.to_string());
                                                }
                                            }
                                        }
                                    }
                                    subj_copy.insert("race", races);

                                    // Add taxonomy from subject_role_taxonomy
                                    if let Some(srt_entries) = lookups.subject_role_taxonomy.get(&subj_key) {
                                        if let Some(srt) = srt_entries.first() {
                                            if let Ok(tax_id) = srt.get_str("taxonomy_id") {
                                                if let Some(taxonomy) =
                                                    lookups.ncbi_taxonomies.get(&(submission.clone(), tax_id.to_string()))
                                                {
                                                    let mut tax_copy = taxonomy.clone();
                                                    tax_copy.remove("_id");
                                                    subj_copy.insert("taxonomy", tax_copy);
                                                }
                                            }
                                        }
                                    }

                                    enriched_subjects.push(subj_copy);
                                }
                            }
                        }
                        bio_copy.insert("subjects", enriched_subjects);

                        enriched_biosamples.push(bio_copy);
                    }
                }
            }

            coll_copy.insert("biosamples", enriched_biosamples);
            enriched_collections.push(coll_copy);
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

fn load_biosample_from_subject(coll: &Collection<Document>, submission: &Option<String>) -> MultiMap {
    let mut map: MultiMap = HashMap::new();
    for doc in load_collection_filtered(coll, submission) {
        if let (Ok(ns), Ok(id)) = (
            doc.get_str("biosample_id_namespace"),
            doc.get_str("biosample_local_id"),
        ) {
            map.entry((ns.to_string(), id.to_string()))
                .or_default()
                .push(doc);
        }
    }
    map
}

fn load_subject_race(coll: &Collection<Document>, submission: &Option<String>) -> MultiMap {
    let mut map: MultiMap = HashMap::new();
    for doc in load_collection_filtered(coll, submission) {
        if let (Ok(ns), Ok(id)) = (
            doc.get_str("subject_id_namespace"),
            doc.get_str("subject_local_id"),
        ) {
            map.entry((ns.to_string(), id.to_string()))
                .or_default()
                .push(doc);
        }
    }
    map
}

fn load_subject_role_taxonomy(coll: &Collection<Document>, submission: &Option<String>) -> MultiMap {
    let mut map: MultiMap = HashMap::new();
    for doc in load_collection_filtered(coll, submission) {
        if let (Ok(ns), Ok(id)) = (
            doc.get_str("subject_id_namespace"),
            doc.get_str("subject_local_id"),
        ) {
            map.entry((ns.to_string(), id.to_string()))
                .or_default()
                .push(doc);
        }
    }
    map
}

fn load_collection_by_persistent_id(
    coll: &Collection<Document>,
    submission: &Option<String>,
) -> HashMap<String, (String, String)> {
    let mut map: HashMap<String, (String, String)> = HashMap::new();
    for doc in load_collection_filtered(coll, submission) {
        if let (Ok(persistent_id), Ok(ns), Ok(id)) = (
            doc.get_str("persistent_id"),
            doc.get_str("id_namespace"),
            doc.get_str("local_id"),
        ) {
            if !persistent_id.is_empty() {
                map.insert(persistent_id.to_string(), (ns.to_string(), id.to_string()));
            }
        }
    }
    map
}

fn load_collection_anatomy(coll: &Collection<Document>, submission: &Option<String>) -> MultiMap {
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

fn load_subject_in_collection(coll: &Collection<Document>, submission: &Option<String>) -> MultiMap {
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
        // Collection anatomy indexes
        doc! { "collections.anatomy.id": 1 },
        doc! { "collections.anatomy.name": 1 },
        // Collection subjects indexes
        doc! { "collections.subjects.id_namespace": 1 },
        doc! { "collections.subjects.local_id": 1 },
        doc! { "collections.subjects.granularity": 1 },
        doc! { "collections.subjects.sex": 1 },
        doc! { "collections.subjects.ethnicity": 1 },
        doc! { "collections.subjects.race": 1 },
        // Biosample indexes
        doc! { "collections.biosamples.id_namespace": 1 },
        doc! { "collections.biosamples.local_id": 1 },
        doc! { "collections.biosamples.anatomy.id": 1 },
        doc! { "collections.biosamples.anatomy.name": 1 },
        doc! { "collections.biosamples.subjects.id_namespace": 1 },
        doc! { "collections.biosamples.subjects.local_id": 1 },
        doc! { "collections.biosamples.subjects.granularity": 1 },
        doc! { "collections.biosamples.subjects.sex": 1 },
        doc! { "collections.biosamples.subjects.ethnicity": 1 },
        doc! { "collections.biosamples.subjects.age_at_enrollment": 1 },
        doc! { "collections.biosamples.subjects.age_at_sampling": 1 },
        doc! { "collections.biosamples.subjects.race": 1 },
        doc! { "collections.biosamples.subjects.taxonomy.id": 1 },
        doc! { "collections.biosamples.subjects.taxonomy.name": 1 },
        // Collection subject taxonomy indexes
        doc! { "collections.subjects.taxonomy.id": 1 },
        doc! { "collections.subjects.taxonomy.name": 1 },
        // Project indexes
        doc! { "project.id_namespace": 1 },
        doc! { "project.local_id": 1 },
        doc! { "project.name": 1 },
        doc! { "project.abbreviation": 1 },
        doc! { "data_access_level": 1 },
        doc! { "submission": 1 },
    ];

    let index_count = indexes.len();
    let models: Vec<IndexModel> = indexes
        .into_iter()
        .map(|keys| IndexModel::builder().keys(keys).build())
        .collect();

    coll.create_indexes(models).run()?;
    println!("  Created {} indexes", index_count);
    Ok(())
}
