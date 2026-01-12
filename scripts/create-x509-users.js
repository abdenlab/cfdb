// Create X.509 authenticated users for MongoDB
// Run this script after MongoDB starts with TLS enabled

// Switch to $external database (required for X.509 authentication)
db = db.getSiblingDB('$external');

// Create API client user
// Subject DN must match exactly as MongoDB reads it from the certificate
// MongoDB reads the DN in RFC 2253 order (reversed): C, O, OU, CN
db.createUser({
    user: "C=US,O=Abdenlab-Clients,OU=Clients,CN=cfdb-api",
    roles: [
        { role: "readWrite", db: "cfdb" }
    ]
});
print("Created X.509 user for API client");

// Create Materializer client user
// Needs additional dbAdmin role for creating indexes
db.createUser({
    user: "C=US,O=Abdenlab-Clients,OU=Clients,CN=cfdb-materializer",
    roles: [
        { role: "readWrite", db: "cfdb" },
        { role: "dbAdmin", db: "cfdb" }
    ]
});
print("Created X.509 user for Materializer client");

print("X.509 user setup complete");
