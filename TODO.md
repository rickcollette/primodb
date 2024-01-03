# TODO

[x] In-memory key-value store

[x] Locks

[x] DB client and server

[x] Config

[x] WAL

[x] WAL recovery


[] WAL file lock
type FileMetadata struct {
    Filename       string   `json:"filename"`
    Owner          string   `json:"owner"`
    ReadonlyUsers  []string `json:"readonly_users"`
    ReadwriteUsers []string `json:"readwrite_users"`
    LockState      string   `json:"lockstate"`
    FileAttr       string   `json:"fileattr"`
    ShareAttr      string   `json:"shareattr"`
}

func SaveFileMetadata(fileID string, metadata FileMetadata) error {
    // Serialize metadata to JSON
    metadataJSON, err := json.Marshal(metadata)
    if err != nil {
        return err
    }

    // Save to KVStore
    _, err = store.Create(fileID, string(metadataJSON))
    return err
}

func GetFileMetadata(fileID string) (FileMetadata, error) {
    metadataJSON, err := store.Read(fileID)
    if err != nil {
        return FileMetadata{}, err
    }

    // Deserialize JSON back to struct
    var metadata FileMetadata
    err = json.Unmarshal([]byte(metadataJSON), &metadata)
    return metadata, err
}

// Additional functions to update lock state, modify access lists, etc.
