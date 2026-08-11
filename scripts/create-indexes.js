// Create indexes on all queryable fields for performance
// Note: 'files' is a view, so indexes go on underlying collections

// ensureIndex(coll, keys, opts) — idempotent on matching spec, drops
// and recreates on differing options. Use this instead of bare
// ``createIndex`` for any index whose options (uniqueness,
// partialFilterExpression, TTL) might evolve over time. Without
// drop-and-recreate logic, re-running this script after a predicate
// change raises ``IndexOptionsConflict`` and aborts the rest of the
// script, leaving the database half-indexed.
//
// ``opts.name`` is required so we can match an existing index by name
// rather than guessing from the key shape (different option sets on
// the same key shape would otherwise collide).
function ensureIndex(coll, keys, opts) {
    if (!opts || !opts.name) {
        throw new Error("ensureIndex requires opts.name");
    }
    var name = opts.name;
    var existing = coll.getIndexes();
    var match = null;
    for (var i = 0; i < existing.length; i++) {
        if (existing[i].name === name) {
            match = existing[i];
            break;
        }
    }
    // Quick path: same name, same options shape — no-op.
    if (match) {
        var sameUnique = !!match.unique === !!opts.unique;
        var samePFE = JSON.stringify(match.partialFilterExpression || null)
            === JSON.stringify(opts.partialFilterExpression || null);
        var sameTTL = (match.expireAfterSeconds || null)
            === (opts.expireAfterSeconds || null);
        if (sameUnique && samePFE && sameTTL) {
            return;
        }
        print("ensureIndex: dropping " + name + " (options changed)");
        coll.dropIndex(name);
    }
    coll.createIndex(keys, opts);
}

print("Creating indexes on 'file' collection...");
db.file.createIndex({ id_namespace: 1 });
db.file.createIndex({ local_id: 1 });
db.file.createIndex({ accession_id: 1 });  // cross-DCC accession (case-folded)
db.file.createIndex({ id_namespace: 1, local_id: 1 });  // composite key
db.file.createIndex({ project_id_namespace: 1 });
db.file.createIndex({ project_local_id: 1 });
db.file.createIndex({ persistent_id: 1 });
db.file.createIndex({ size_in_bytes: 1 });
db.file.createIndex({ sha256: 1 });
db.file.createIndex({ md5: 1 });
db.file.createIndex({ filename: 1 });
db.file.createIndex({ file_format: 1 });
db.file.createIndex({ compression_format: 1 });
db.file.createIndex({ data_type: 1 });
db.file.createIndex({ assay_type: 1 });
db.file.createIndex({ analysis_type: 1 });
db.file.createIndex({ mime_type: 1 });
db.file.createIndex({ bundle_collection_id_namespace: 1 });
db.file.createIndex({ bundle_collection_local_id: 1 });
db.file.createIndex({ dbgap_study_id: 1 });
db.file.createIndex({ access_url: 1 });
db.file.createIndex({ submission: 1 });  // for sync operations
db.file.createIndex({ data_access_level: 1 });  // for access control

print("Creating indexes on 'dcc' collection...");
db.dcc.createIndex({ id: 1 });
db.dcc.createIndex({ dcc_name: 1 });
db.dcc.createIndex({ dcc_abbreviation: 1 });
db.dcc.createIndex({ dcc_description: 1 });
db.dcc.createIndex({ contact_email: 1 });
db.dcc.createIndex({ contact_name: 1 });
db.dcc.createIndex({ dcc_url: 1 });
db.dcc.createIndex({ project_id_namespace: 1 });
db.dcc.createIndex({ project_local_id: 1 });
db.dcc.createIndex({ submission: 1 });

print("Creating indexes on 'file_format' collection...");
db.file_format.createIndex({ id: 1 });
db.file_format.createIndex({ name: 1 });
db.file_format.createIndex({ description: 1 });
db.file_format.createIndex({ submission: 1, id: 1 }, { unique: true });  // unique per DCC

print("Creating indexes on 'data_type' collection...");
db.data_type.createIndex({ id: 1 });
db.data_type.createIndex({ name: 1 });
db.data_type.createIndex({ description: 1 });
db.data_type.createIndex({ submission: 1, id: 1 }, { unique: true });  // unique per DCC

print("Creating indexes on 'assay_type' collection...");
db.assay_type.createIndex({ id: 1 });
db.assay_type.createIndex({ name: 1 });
db.assay_type.createIndex({ description: 1 });
db.assay_type.createIndex({ submission: 1, id: 1 }, { unique: true });  // unique per DCC

print("Creating indexes on 'collection' collection...");
db.collection.createIndex({ id_namespace: 1 });
db.collection.createIndex({ local_id: 1 });
db.collection.createIndex({ accession_id: 1 });  // cross-DCC accession (case-folded)
db.collection.createIndex({ id_namespace: 1, local_id: 1 });  // composite key
db.collection.createIndex({ persistent_id: 1 });
db.collection.createIndex({ abbreviation: 1 });
db.collection.createIndex({ name: 1 });
db.collection.createIndex({ description: 1 });
db.collection.createIndex({ submission: 1 });

print("Creating indexes on 'biosample' collection...");
db.biosample.createIndex({ id_namespace: 1 });
db.biosample.createIndex({ local_id: 1 });
db.biosample.createIndex({ id_namespace: 1, local_id: 1 });  // composite key
db.biosample.createIndex({ project_id_namespace: 1 });
db.biosample.createIndex({ project_local_id: 1 });
db.biosample.createIndex({ persistent_id: 1 });
db.biosample.createIndex({ sample_prep_method: 1 });
db.biosample.createIndex({ anatomy: 1 });
db.biosample.createIndex({ biofluid: 1 });
db.biosample.createIndex({ submission: 1 });

print("Creating indexes on 'anatomy' collection...");
db.anatomy.createIndex({ id: 1 });
db.anatomy.createIndex({ name: 1 });
db.anatomy.createIndex({ description: 1 });
db.anatomy.createIndex({ submission: 1, id: 1 }, { unique: true });  // unique per DCC

print("Creating indexes on 'file_in_collection' collection...");
db.file_in_collection.createIndex({ file_id_namespace: 1 });
db.file_in_collection.createIndex({ file_local_id: 1 });
db.file_in_collection.createIndex({ file_id_namespace: 1, file_local_id: 1 });
db.file_in_collection.createIndex({ collection_id_namespace: 1 });
db.file_in_collection.createIndex({ collection_local_id: 1 });
db.file_in_collection.createIndex({ collection_id_namespace: 1, collection_local_id: 1 });
db.file_in_collection.createIndex({ submission: 1 });

print("Creating indexes on 'biosample_in_collection' collection...");
db.biosample_in_collection.createIndex({ biosample_id_namespace: 1 });
db.biosample_in_collection.createIndex({ biosample_local_id: 1 });
db.biosample_in_collection.createIndex({ biosample_id_namespace: 1, biosample_local_id: 1 });
db.biosample_in_collection.createIndex({ collection_id_namespace: 1 });
db.biosample_in_collection.createIndex({ collection_local_id: 1 });
db.biosample_in_collection.createIndex({ collection_id_namespace: 1, collection_local_id: 1 });
db.biosample_in_collection.createIndex({ submission: 1 });

print("Creating indexes on 'subject' collection...");
db.subject.createIndex({ id_namespace: 1 });
db.subject.createIndex({ local_id: 1 });
db.subject.createIndex({ id_namespace: 1, local_id: 1 });  // composite key
db.subject.createIndex({ project_id_namespace: 1 });
db.subject.createIndex({ project_local_id: 1 });
db.subject.createIndex({ persistent_id: 1 });
db.subject.createIndex({ granularity: 1 });
db.subject.createIndex({ sex: 1 });
db.subject.createIndex({ ethnicity: 1 });
db.subject.createIndex({ submission: 1 });

print("Creating indexes on 'biosample_from_subject' collection...");
db.biosample_from_subject.createIndex({ biosample_id_namespace: 1 });
db.biosample_from_subject.createIndex({ biosample_local_id: 1 });
db.biosample_from_subject.createIndex({ biosample_id_namespace: 1, biosample_local_id: 1 });
db.biosample_from_subject.createIndex({ subject_id_namespace: 1 });
db.biosample_from_subject.createIndex({ subject_local_id: 1 });
db.biosample_from_subject.createIndex({ subject_id_namespace: 1, subject_local_id: 1 });
db.biosample_from_subject.createIndex({ submission: 1 });

print("Creating indexes on 'subject_race' collection...");
db.subject_race.createIndex({ subject_id_namespace: 1 });
db.subject_race.createIndex({ subject_local_id: 1 });
db.subject_race.createIndex({ subject_id_namespace: 1, subject_local_id: 1 });
db.subject_race.createIndex({ race: 1 });
db.subject_race.createIndex({ submission: 1 });

print("Creating indexes on 'collection_anatomy' collection...");
db.collection_anatomy.createIndex({ collection_id_namespace: 1, collection_local_id: 1 });
db.collection_anatomy.createIndex({ anatomy: 1 });
db.collection_anatomy.createIndex({ submission: 1 });

print("Creating indexes on 'subject_in_collection' collection...");
db.subject_in_collection.createIndex({ collection_id_namespace: 1, collection_local_id: 1 });
db.subject_in_collection.createIndex({ subject_id_namespace: 1, subject_local_id: 1 });
db.subject_in_collection.createIndex({ submission: 1 });

print("Creating indexes on 'locks' collection...");
db.locks.createIndex({ active: 1 });

print("Creating indexes on 'jobs' collection...");
// Partial unique index on workflow_key enforces per-source mutex: only one
// active job (pending|running) may exist per source file. The filter keys
// on the boolean `active` discriminator (active == status in
// ACTIVE_STATUSES), which JobRecord.to_mongo stamps and cfdb.workflows.lock
// maintains. Terminal jobs have active=false so they fall outside the
// filter and fresh claims can succeed.
//
// Why `active` instead of `status: { $in: [...] }`: Amazon DocumentDB
// rejects the $in operator inside a partialFilterExpression (only $eq,
// $exists, $and, $gt/$gte/$lt/$lte are supported), so the predicate is
// expressed as implicit equality on `active`.
//
// IMPORTANT: this MUST stay in sync with operational_index_specs() in
// cfdb/indexes.py (the application source of truth); a lockstep test
// guards drift. Renaming/repredicating one side without the other breaks
// the mutex contract.
//
// Compatibility: requires MongoDB >= 3.2 / DocumentDB 5.0 (partial
// indexes). Re-running with a changed predicate raises
// IndexOptionsConflict; dropIndex first if the predicate ever changes.
ensureIndex(
    db.jobs,
    { workflow_key: 1 },
    {
        name: "workflow_key_active_unique",
        unique: true,
        partialFilterExpression: {
            active: true
        }
    }
);
ensureIndex(db.jobs, { job_id: 1 }, { name: "job_id_unique", unique: true });
ensureIndex(
    db.jobs,
    { status: 1, updated_at: 1 },
    { name: "status_updated_at" }
);

// Serves the durable retry scheduler's due-dispatch lease
// (workflows.lock.lease_due_dispatch): { status: "pending",
// next_dispatch_at: { $lte: now } } sorted by next_dispatch_at asc. The
// status equality prefix plus the next_dispatch_at range/sort are both
// index-served, so the per-tick poll is not a scan of all PENDING rows.
ensureIndex(
    db.jobs,
    { status: 1, next_dispatch_at: 1 },
    { name: "status_next_dispatch_at" }
);

// TTL index on terminal job rows. Without this, every stale-reclaim
// transition leaves a permanent ``failed`` document and the collection
// grows unbounded for any frequently-touched workflow_key. The partial
// filter excludes active rows (active=true) so the TTL never reaps an
// in-flight job. 7 days gives operators a window to investigate recent
// failures before the row is reclaimed. Filters on `active` (not a status
// $in list) for DocumentDB partialFilterExpression compatibility — see
// the workflow_key_active_unique note above.
ensureIndex(
    db.jobs,
    { updated_at: 1 },
    {
        name: "terminal_ttl",
        expireAfterSeconds: 60 * 60 * 24 * 7,
        partialFilterExpression: {
            active: false
        }
    }
);

print("Creating indexes on 'ncbi_taxonomy' collection...");
db.ncbi_taxonomy.createIndex({ id: 1 });
db.ncbi_taxonomy.createIndex({ name: 1 });
db.ncbi_taxonomy.createIndex({ clade: 1 });
db.ncbi_taxonomy.createIndex({ submission: 1, id: 1 }, { unique: true });

print("Creating indexes on 'subject_role_taxonomy' collection...");
db.subject_role_taxonomy.createIndex({ subject_id_namespace: 1 });
db.subject_role_taxonomy.createIndex({ subject_local_id: 1 });
db.subject_role_taxonomy.createIndex({ subject_id_namespace: 1, subject_local_id: 1 });
db.subject_role_taxonomy.createIndex({ taxonomy_id: 1 });
db.subject_role_taxonomy.createIndex({ submission: 1 });

print("Creating indexes on 'project' collection...");
db.project.createIndex({ id_namespace: 1 });
db.project.createIndex({ local_id: 1 });
db.project.createIndex({ id_namespace: 1, local_id: 1 });
db.project.createIndex({ name: 1 });
db.project.createIndex({ abbreviation: 1 });
db.project.createIndex({ submission: 1 });

print("All indexes created successfully");
